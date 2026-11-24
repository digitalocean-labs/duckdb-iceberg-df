#include "maintenance/orphan_files.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_file_credentials.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "iceberg_options.hpp"
#include "maintenance/maintenance_table_loader.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
namespace duckdb {
namespace {
constexpr const char *FUNCTION_NAME = "iceberg_remove_orphan_files";
struct ListedFile {
	string path;
	string normalized;
	idx_t size = 0;
	bool has_modified = false;
	timestamp_t modified;
	string version_tag;
};
struct References {
	string table_uuid;
	string location;
	string prefix;
	string metadata_location;
	unordered_set<string> referenced;
	unordered_set<string> live;
};
bool IsCancellation(const ErrorData &error) {
	return error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::OUT_OF_MEMORY;
}
bool IsLocalPath(const string &path) {
	auto lower = StringUtil::Lower(path);
	auto scheme_end = lower.find("://");
	if (scheme_end != string::npos && scheme_end > 0 && lower.substr(0, scheme_end).find('/') == string::npos) {
		return lower.substr(0, scheme_end) == "file";
	}
	return true;
}
void ValidateGCEnabled(const IcebergTableMetadata &metadata) {
	auto &properties = metadata.GetTableProperties();
	auto entry = properties.find("gc.enabled");
	if (entry == properties.end() || StringUtil::CIEquals(entry->second, "true")) {
		return;
	}
	if (StringUtil::CIEquals(entry->second, "false")) {
		throw InvalidInputException("%s: cannot run when table property 'gc.enabled' is false", FUNCTION_NAME);
	}
	throw InvalidInputException("%s: invalid value '%s' for table property 'gc.enabled'", FUNCTION_NAME, entry->second);
}
bool IsProtected(const string &normalized) {
	auto slash = normalized.find_last_of("/\\");
	auto name = slash == string::npos ? normalized : normalized.substr(slash + 1);
	return StringUtil::EndsWith(name, ".metadata.json") || StringUtil::EndsWith(name, ".metadata.json.gz") ||
	       name == "version-hint.text";
}
string NormalizeStoragePath(const string &path) {
	auto p = path;
	StringUtil::Trim(p);
	bool keep_leading_slash = !p.empty() && p.front() == '/';
	auto scheme_end = p.find("://");
	if (scheme_end != string::npos && p.substr(0, scheme_end).find('/') == string::npos) {
		auto scheme = StringUtil::Lower(p.substr(0, scheme_end));
		if (scheme != "s3" && scheme != "s3a" && scheme != "s3n" && scheme != "file") {
			throw NotImplementedException("%s: unsupported storage scheme '%s'", FUNCTION_NAME, scheme);
		}
		keep_leading_slash = scheme == "file";
		p = p.substr(scheme_end + 3);
	} else if (StringUtil::StartsWith(StringUtil::Lower(p), "file:/")) {
		keep_leading_slash = true;
		p = p.substr(5);
		if (!p.empty() && p.front() != '/') {
			p = "/" + p;
		}
	}
	string result;
	for (auto c : p) {
		if (c == '/' && (result.empty() || result.back() == '/')) {
			if (result.empty() && keep_leading_slash) {
				result = "/";
			}
			continue;
		}
		result += c;
	}
	while (result.size() > 1 && result.back() == '/') {
		result.pop_back();
	}
	for (auto &level : StringUtil::Split(result, "/")) {
		if (level == "." || level == "..") {
			throw InvalidInputException("%s: storage path '%s' contains '.' or '..'", FUNCTION_NAME, path);
		}
	}
	return result;
}
string NormalizeAbsolutePath(const string &path, const char *what, FileSystem &fs) {
	string trimmed = path;
	StringUtil::Trim(trimmed);
	auto lower = StringUtil::Lower(trimmed);
	auto scheme_end = trimmed.find("://");
	bool has_scheme =
	    scheme_end != string::npos && scheme_end > 0 && trimmed.substr(0, scheme_end).find('/') == string::npos;
	string scheme;
	bool absolute = true;
	if (has_scheme) {
		scheme = lower.substr(0, scheme_end);
		if (scheme == "s3" || scheme == "s3a" || scheme == "s3n") {
			absolute = scheme_end + 3 < trimmed.size() && trimmed[scheme_end + 3] != '/';
		} else if (scheme == "file") {
			absolute = StringUtil::StartsWith(lower, "file:///") ||
			           (StringUtil::StartsWith(lower, "file:/") && !StringUtil::StartsWith(lower, "file://"));
		} else {
			//! The normalizer reports unsupported schemes.
			return NormalizeStoragePath(trimmed);
		}
	} else {
		absolute = fs.IsPathAbsolute(trimmed);
	}
	if (!absolute) {
		throw InvalidInputException("%s: the %s '%s' is not an absolute location: refusing to touch files",
		                            FUNCTION_NAME, what, path);
	}
	auto normalized = NormalizeStoragePath(trimmed);
	if ((!has_scheme || scheme == "file") && !fs.IsPathAbsolute(normalized)) {
		throw InvalidInputException("%s: the %s '%s' does not resolve to an absolute location: refusing to touch files",
		                            FUNCTION_NAME, what, path);
	}
	return normalized;
}
string TablePrefix(const string &location) {
	auto key = NormalizeStoragePath(location);
	idx_t levels = 0;
	for (auto &level : StringUtil::Split(key, "/")) {
		levels += !level.empty();
	}
	if (key.empty() || levels < 3) {
		throw InvalidInputException("%s: table location '%s' is root-like", FUNCTION_NAME, location);
	}
	if (key.find_first_of("*?[") != string::npos) {
		throw InvalidInputException("%s: the table location '%s' contains glob characters", FUNCTION_NAME, location);
	}
	return key + "/";
}
string MetadataDirectory(const string &prefix) {
	return prefix + "metadata" + (prefix.back() == '\\' ? "\\" : "/");
}
bool IsDirectChild(const string &path, const string &directory) {
	if (!StringUtil::StartsWith(path, directory)) {
		return false;
	}
	auto name = path.substr(directory.size());
	return !name.empty() && name.find_first_of("/\\") == string::npos;
}
bool AgeAllowsDeletion(timestamp_t modified, int64_t cutoff) {
	return modified.value > 0 && modified.value < cutoff;
}
bool VersionMatches(const ListedFile &listed, const ListedFile &current) {
	if (listed.size != current.size) {
		return false;
	}
	if (!listed.version_tag.empty() && !current.version_tag.empty()) {
		auto strip_quotes = [](const string &tag) {
			if (tag.size() >= 2 && tag.front() == '"' && tag.back() == '"') {
				return tag.substr(1, tag.size() - 2);
			}
			return tag;
		};
		return strip_quotes(listed.version_tag) == strip_quotes(current.version_tag);
	}
	if (!listed.has_modified || !current.has_modified) {
		return false;
	}
	return listed.modified.value / Interval::MICROS_PER_SEC == current.modified.value / Interval::MICROS_PER_SEC;
}
void CheckReferences(const References &refs) {
	if (refs.table_uuid.empty() || refs.location.empty() || refs.prefix.empty()) {
		throw InvalidInputException("%s: incomplete table identity: refusing to delete anything", FUNCTION_NAME);
	}
	auto metadata_directory = MetadataDirectory(refs.prefix);
	if (!IsDirectChild(refs.metadata_location, metadata_directory)) {
		throw InvalidInputException("%s: current metadata is outside the metadata directory", FUNCTION_NAME);
	}
	idx_t outside = 0;
	for (auto &path : refs.live) {
		outside += !StringUtil::StartsWith(path, refs.prefix);
	}
	if (outside) {
		throw InvalidInputException("%s: live files are outside the table location", FUNCTION_NAME);
	}
}
void CheckListing(const References &refs, const unordered_set<std::string_view> &listed) {
	if (listed.empty()) {
		throw InvalidInputException("%s: listing '%s' is empty", FUNCTION_NAME, refs.location);
	}
	CheckReferences(refs);
	auto metadata_directory = MetadataDirectory(refs.prefix);
	for (auto &path : listed) {
		if (IsProtected(string(path)) && !IsDirectChild(string(path), metadata_directory)) {
			throw InvalidInputException("%s: nested metadata marker '%s' outside '%s': refusing to delete",
			                            FUNCTION_NAME, string(path), metadata_directory);
		}
	}
	if (!listed.count(refs.metadata_location)) {
		throw InvalidInputException("%s: current metadata.json is missing from the listing", FUNCTION_NAME);
	}
	idx_t missing = 0;
	for (auto &path : refs.live) {
		missing += !listed.count(path);
	}
	if (missing) {
		throw InvalidInputException("%s: required references are missing", FUNCTION_NAME);
	}
}
void CheckManifestTotals(const IcebergSnapshot &snapshot, const vector<IcebergManifestListEntry> &manifests) {
	if (snapshot.manifest_list.empty() && !snapshot.manifests.empty()) {
		if (manifests.size() != snapshot.manifests.size()) {
			throw InvalidConfigurationException("%s: inline manifest count mismatch", FUNCTION_NAME);
		}
		return;
	}
	idx_t data = 0;
	idx_t deletes = 0;
	for (auto &entry : manifests) {
		auto &file = entry.GetFile();
		if (!file.counts || !file.counts->FilesComplete()) {
			throw InvalidConfigurationException("%s: manifest has incomplete file counts", FUNCTION_NAME);
		}
		auto live = *file.counts->added_files_count + *file.counts->existing_files_count;
		(file.content == IcebergManifestContentType::DATA ? data : deletes) += live;
	}
	auto &metrics = snapshot.metrics.GetMetrics();
	auto data_metric = metrics.find(IcebergSnapshotMetricType::TOTAL_DATA_FILES);
	auto delete_metric = metrics.find(IcebergSnapshotMetricType::TOTAL_DELETE_FILES);
	if (data_metric == metrics.end() || delete_metric == metrics.end() ||
	    data_metric->second != NumericCast<int64_t>(data) || delete_metric->second != NumericCast<int64_t>(deletes)) {
		throw InvalidConfigurationException("%s: manifest counts do not match declared totals", FUNCTION_NAME);
	}
}
const vector<IcebergManifestEntry> &ValidatedEntries(IcebergManifestListEntry &manifest,
                                                     const IcebergSnapshot &snapshot) {
	auto &entries =
	    manifest.HasManifestEntries() ? manifest.GetManifestEntries() : manifest.GetOrCreateManifestEntries();
	auto &file = manifest.GetFile();
	auto complete = file.counts && file.counts->FilesComplete();
	if (entries.empty()) {
		if (!complete || !file.counts->HasNoLiveFiles()) {
			throw InvalidConfigurationException("%s: manifest has no readable entries", FUNCTION_NAME);
		}
		return entries;
	}
	if (!complete) {
		if (snapshot.manifest_list.empty() && !snapshot.manifests.empty()) {
			return entries;
		}
		throw InvalidConfigurationException("%s: manifest counts are incomplete", FUNCTION_NAME);
	}
	auto expected =
	    *file.counts->added_files_count + *file.counts->existing_files_count + *file.counts->deleted_files_count;
	if (entries.size() != expected) {
		throw InvalidConfigurationException("%s: manifest entries do not match declared counts", FUNCTION_NAME);
	}
	return entries;
}
References LoadReferences(ClientContext &context, const shared_ptr<IcebergTable> &table) {
	table->LoadCredentials(context);
	auto &metadata = table->GetMetadata();
	if (!table->initialization_source || !table->initialization_source->result.metadata_location ||
	    table->initialization_source->result.metadata_location->empty()) {
		throw InvalidInputException("%s: no metadata-location: refusing to delete anything", FUNCTION_NAME);
	}
	auto &fs = FileSystem::GetFileSystem(context);
	References refs;
	refs.table_uuid = metadata.table_uuid;
	refs.location = metadata.location;
	refs.prefix = TablePrefix(NormalizeAbsolutePath(refs.location, "table location", fs));
	refs.metadata_location =
	    NormalizeAbsolutePath(*table->initialization_source->result.metadata_location, "current metadata location", fs);
	refs.referenced.insert(refs.metadata_location);
	for (auto &item : metadata.metadata_log) {
		refs.referenced.insert(NormalizeAbsolutePath(item.metadata_file, "metadata-log entry", fs));
	}
	for (auto &file : metadata.statistics) {
		auto path = NormalizeAbsolutePath(file.path, "statistics file", fs);
		refs.referenced.insert(path);
		if (StringUtil::StartsWith(path, refs.prefix)) {
			refs.live.insert(std::move(path));
		}
	}
	IcebergFileSecret file_secret(IcebergFileCredentialsCallback::Get(context, *table),
	                              std::to_string(table->catalog.GetOid()));
	IcebergOptions options;
	unordered_set<string> scanned_manifests;
	for (auto &entry : metadata.snapshots) {
		auto &snapshot = entry.second;
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = &snapshot;
		try {
			snapshot_info.schema_id = snapshot.GetSchemaId();
		} catch (std::exception &) {
			snapshot_info.schema_id = metadata.GetCurrentSchemaId();
		}
		if (snapshot.manifest_list.empty() && snapshot.manifests.empty()) {
			throw InvalidInputException("%s: snapshot %lld has no manifest list or manifests", FUNCTION_NAME,
			                            snapshot.snapshot_id);
		}
		if (!snapshot.manifest_list.empty()) {
			auto path = NormalizeAbsolutePath(snapshot.manifest_list, "manifest list", fs);
			refs.referenced.insert(path);
			refs.live.insert(std::move(path));
		}
		vector<IcebergManifestListEntry> manifests;
		IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context, manifests, file_secret);
		CheckManifestTotals(snapshot, manifests);
		vector<IcebergManifestListEntry> unseen;
		for (auto &manifest : manifests) {
			auto path = NormalizeAbsolutePath(manifest.GetFile().manifest_path, "manifest", fs);
			refs.referenced.insert(path);
			refs.live.insert(path);
			if (scanned_manifests.insert(std::move(path)).second) {
				unseen.push_back(std::move(manifest));
			}
		}
		if (unseen.empty()) {
			continue;
		}
		auto scan = AvroScan::ScanManifest(snapshot_info, unseen, options, fs, metadata.location, metadata, context,
		                                   nullptr, {}, file_secret);
		manifest_file::ManifestReader reader(*scan);
		while (!reader.Finished()) {
			reader.Read();
		}
		scan->Complete();
		for (auto &manifest : unseen) {
			auto &entries = ValidatedEntries(manifest, snapshot);
			for (auto &manifest_entry : entries) {
				if (manifest_entry.status != IcebergManifestEntryStatusType::DELETED) {
					auto path = NormalizeAbsolutePath(manifest_entry.data_file.file_path, "data file", fs);
					refs.referenced.insert(path);
					refs.live.insert(std::move(path));
				}
			}
		}
	}
	return refs;
}
vector<ListedFile> ListTableLocation(ClientContext &context, const References &refs) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto pattern = refs.location;
	while (!pattern.empty() && pattern.back() == '/') {
		pattern.pop_back();
	}
	pattern += "/**";
	vector<OpenFileInfo> found;
	try {
		found = fs.Glob(pattern);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (IsCancellation(error)) {
			throw;
		}
		throw IOException("%s: listing '%s' failed, nothing was deleted (is LIST permission granted?): %s",
		                  FUNCTION_NAME, refs.location, error.RawMessage());
	}
	vector<ListedFile> result;
	result.reserve(found.size());
	for (auto &file : found) {
		if (StringUtil::EndsWith(file.path, "/")) {
			continue;
		}
		ListedFile listed;
		listed.path = file.path;
		listed.normalized = NormalizeStoragePath(file.path);
		if (!StringUtil::StartsWith(listed.normalized, refs.prefix)) {
			continue;
		}
		idx_t size = 0;
		timestamp_t modified;
		if (file.extended_info && file.extended_info->TryGetOption("file_size", size) &&
		    file.extended_info->TryGetOption("last_modified", modified)) {
			listed.size = size;
			listed.modified = modified;
			listed.has_modified = modified.value > 0;
			string version_tag;
			if (file.extended_info->TryGetOption("etag", version_tag)) {
				listed.version_tag = std::move(version_tag);
			}
		}
		result.push_back(std::move(listed));
	}
	return result;
}

enum class CandidateRecheck : uint8_t { UNCHANGED, CHANGED, ERROR };
CandidateRecheck RecheckCandidate(ClientContext &context, IcebergTable &table, const ListedFile &candidate,
                                  int64_t cutoff) {
	auto &fs = FileSystem::GetFileSystem(context);
	unique_ptr<FileHandle> handle;
	try {
		auto file = IcebergFileCredentialsCallback::GetFileInfo(context, table, candidate.path);
		handle =
		    fs.OpenFile(file, FileOpenFlags(FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS));
	} catch (std::exception &ex) {
		if (IsCancellation(ErrorData(ex))) {
			throw;
		}
		return CandidateRecheck::ERROR;
	}
	if (!handle) {
		return CandidateRecheck::ERROR;
	}
	try {
		auto size = fs.GetFileSize(*handle);
		if (size < 0) {
			return CandidateRecheck::CHANGED;
		}
		ListedFile current;
		current.size = NumericCast<idx_t>(size);
		current.version_tag = fs.GetVersionTag(*handle);
		current.modified = fs.GetLastModifiedTime(*handle);
		current.has_modified = current.modified.value > 0;
		if (!AgeAllowsDeletion(current.modified, cutoff)) {
			return CandidateRecheck::CHANGED;
		}
		ListedFile listed;
		listed.size = candidate.size;
		listed.has_modified = candidate.has_modified;
		listed.modified = candidate.modified;
		listed.version_tag = candidate.version_tag;
		if (!VersionMatches(listed, current)) {
			return CandidateRecheck::CHANGED;
		}
	} catch (std::exception &ex) {
		if (IsCancellation(ErrorData(ex))) {
			throw;
		}
		return CandidateRecheck::ERROR;
	}
	return CandidateRecheck::UNCHANGED;
}

} // namespace
OrphanFilesResult RemoveOrphanFiles(ClientContext &context, const QualifiedName &table_name, int64_t older_than_micros,
                                    bool dry_run) {
	if (!context.transaction.IsAutoCommit()) {
		throw InvalidInputException("%s cannot run inside an explicit transaction: it deletes files, which ROLLBACK "
		                            "cannot undo",
		                            FUNCTION_NAME);
	}
	auto table = ReloadIcebergTableShared(context, table_name, FUNCTION_NAME);
	References refs;
	try {
		refs = LoadReferences(context, table);
	} catch (std::exception &ex) {
		if (IsCancellation(ErrorData(ex))) {
			throw;
		}
		throw InvalidInputException("%s: could not read the retained file set: %s; refusing to delete anything",
		                            FUNCTION_NAME, ex.what());
	}
	ValidateGCEnabled(table->GetMetadata());
	if (!dry_run && IsLocalPath(refs.location) && !table->catalog.attach_options.allow_local_file_orphan_deletion) {
		throw InvalidInputException("%s: refusing local deletion; re-attach with "
		                            "ALLOW_LOCAL_FILE_ORPHAN_DELETION true to enable it",
		                            FUNCTION_NAME);
	}
	auto listing = ListTableLocation(context, refs);
	{
		//! Views point into listing's strings; this set dies before the deletion work.
		unordered_set<std::string_view> listed;
		for (auto &file : listing) {
			listed.insert(file.normalized);
		}
		CheckListing(refs, listed);
	}
	OrphanFilesResult result;
	result.listed_files = NumericCast<int64_t>(listing.size());
	auto cutoff = Timestamp::GetCurrentTimestamp().value - older_than_micros;
	auto metadata_directory = MetadataDirectory(refs.prefix);
	vector<const ListedFile *> candidates;
	for (auto &file : listing) {
		if (refs.referenced.count(file.normalized)) {
			result.referenced_present++;
		} else if (StringUtil::StartsWith(file.normalized, metadata_directory) &&
		           !StringUtil::EndsWith(file.normalized, ".avro")) {
			continue;
		} else if (!file.has_modified || !AgeAllowsDeletion(file.modified, cutoff)) {
			result.young_unreferenced++;
		} else if (!IsProtected(file.normalized)) {
			candidates.push_back(&file);
		}
	}
	for (auto candidate : candidates) {
		result.orphan_files++;
		result.orphan_bytes += NumericCast<int64_t>(candidate->size);
	}
	if (dry_run) {
		return result;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto candidate : candidates) {
		context.InterruptCheck();
		auto recheck = RecheckCandidate(context, *table, *candidate, cutoff);
		if (recheck != CandidateRecheck::UNCHANGED) {
			result.delete_errors++;
			continue;
		}
		try {
			fs.RemoveFile(candidate->path);
			result.deleted_files++;
			result.deleted_bytes += NumericCast<int64_t>(candidate->size);
		} catch (std::exception &ex) {
			if (IsCancellation(ErrorData(ex))) {
				throw;
			}
			result.delete_errors++;
		}
	}
	return result;
}
} // namespace duckdb
