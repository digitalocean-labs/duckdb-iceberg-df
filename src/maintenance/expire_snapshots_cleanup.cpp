#include "maintenance/expire_snapshots_cleanup.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "iceberg_logging.hpp"
#include "iceberg_options.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"

#include <algorithm>
#include <mutex>

namespace duckdb {

namespace {

constexpr const char *EXPIRE_SNAPSHOTS_NAME = "iceberg_expire_snapshots";
constexpr const char *CLEANUP_KEY = "iceberg_expire_snapshots_cleanup";

//! Only file paths are read, so a snapshot without a schema-id reads with the current schema.
IcebergSnapshotScanInfo ScanInfoFor(const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot) {
	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = &snapshot;
	snapshot_info.schema_id = snapshot.HasSchemaId() ? snapshot.GetSchemaId() : metadata.GetCurrentSchemaId();
	return snapshot_info;
}

string NormalizeStoragePath(const string &path) {
	auto p = path;
	StringUtil::Trim(p);
	bool keep_leading_slash = !p.empty() && p.front() == '/';
	auto scheme_end = p.find("://");
	if (scheme_end != string::npos && p.substr(0, scheme_end).find('/') == string::npos) {
		auto scheme = StringUtil::Lower(p.substr(0, scheme_end));
		if (scheme != "s3" && scheme != "s3a" && scheme != "s3n" && scheme != "file") {
			throw InvalidConfigurationException("%s: unsupported storage scheme '%s'", EXPIRE_SNAPSHOTS_NAME, scheme);
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
			throw InvalidConfigurationException("%s: storage path '%s' contains '.' or '..'", EXPIRE_SNAPSHOTS_NAME,
			                                    path);
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
	if (has_scheme) {
		scheme = lower.substr(0, scheme_end);
		bool absolute;
		if (scheme == "s3" || scheme == "s3a" || scheme == "s3n") {
			absolute = scheme_end + 3 < trimmed.size() && trimmed[scheme_end + 3] != '/';
		} else if (scheme == "file") {
			absolute = StringUtil::StartsWith(lower, "file:///") ||
			           (StringUtil::StartsWith(lower, "file:/") && !StringUtil::StartsWith(lower, "file://"));
		} else {
			return NormalizeStoragePath(trimmed);
		}
		if (!absolute) {
			throw InvalidConfigurationException("%s: the %s '%s' is not an absolute location", EXPIRE_SNAPSHOTS_NAME,
			                                    what, path);
		}
	} else if (!fs.IsPathAbsolute(trimmed)) {
		throw InvalidConfigurationException("%s: the %s '%s' is not an absolute location", EXPIRE_SNAPSHOTS_NAME, what,
		                                    path);
	}
	auto normalized = NormalizeStoragePath(trimmed);
	if ((!has_scheme || scheme == "file") && !fs.IsPathAbsolute(normalized)) {
		throw InvalidConfigurationException("%s: the %s '%s' does not resolve to an absolute location",
		                                    EXPIRE_SNAPSHOTS_NAME, what, path);
	}
	return normalized;
}

string TableLocationPrefix(const string &location) {
	auto key = NormalizeStoragePath(location);
	idx_t levels = 0;
	for (auto &level : StringUtil::Split(key, "/")) {
		if (!level.empty()) {
			levels++;
		}
	}
	if (levels < 3) {
		throw InvalidConfigurationException("%s: the table location '%s' is too shallow for deletion",
		                                    EXPIRE_SNAPSHOTS_NAME, location);
	}
	if (key.find_first_of("*?[") != string::npos) {
		throw InvalidConfigurationException("%s: the table location '%s' contains glob characters",
		                                    EXPIRE_SNAPSHOTS_NAME, location);
	}
	return key + "/";
}

bool IsLocalPath(const string &path) {
	auto lower = StringUtil::Lower(path);
	auto scheme_end = lower.find("://");
	if (scheme_end != string::npos && scheme_end > 0 && lower.substr(0, scheme_end).find('/') == string::npos) {
		return lower.substr(0, scheme_end) == "file";
	}
	return true;
}

bool IsMetadataFile(const string &path) {
	auto lower = StringUtil::Lower(path);
	if (StringUtil::EndsWith(lower, ".metadata.json") || StringUtil::EndsWith(lower, ".metadata.json.gz")) {
		return true;
	}
	auto slash = path.rfind('/');
	return path.substr(slash == string::npos ? 0 : slash + 1) == "version-hint.text";
}

string ValidateCandidatePath(FileSystem &fs, const string &prefix, const string &path) {
	if (path.empty() || path.find_first_of("*?[") != string::npos) {
		throw InvalidConfigurationException("%s: expired file '%s' is not an absolute glob-free path",
		                                    EXPIRE_SNAPSHOTS_NAME, path);
	}
	auto normalized = NormalizeAbsolutePath(path, "expired file", fs);
	if (!StringUtil::StartsWith(normalized, prefix)) {
		throw InvalidConfigurationException("%s: expired file '%s' is outside the table location",
		                                    EXPIRE_SNAPSHOTS_NAME, path);
	}
	if (IsMetadataFile(path)) {
		throw InvalidConfigurationException("%s: expired file '%s' is a current metadata file", EXPIRE_SNAPSHOTS_NAME,
		                                    path);
	}
	return normalized;
}

void ValidateLocalCandidates(FileSystem &fs, const string &location, const vector<string> &candidates) {
	bool has_local = false;
	for (auto &candidate : candidates) {
		has_local |= IsLocalPath(candidate);
	}
	if (!has_local) {
		return;
	}
	auto root = NormalizeAbsolutePath(location, "table location", fs);
	auto slash = root.find_last_of("/\\");
	if (slash == string::npos || slash + 1 >= root.size()) {
		throw InvalidConfigurationException("%s: the table location '%s' is not a directory", EXPIRE_SNAPSHOTS_NAME,
		                                    location);
	}
	auto parent = root.substr(0, slash);
	auto name = root.substr(slash + 1);
	string canonical_root;
	try {
		canonical_root = fs.JoinPath(fs.CanonicalizePath(parent), name);
		if (fs.CanonicalizePath(root) != canonical_root) {
			throw InvalidConfigurationException("%s: the table location '%s' is a symlink", EXPIRE_SNAPSHOTS_NAME,
			                                    location);
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::OUT_OF_MEMORY) {
			throw;
		}
		throw InvalidConfigurationException("%s: cannot prove the local table location '%s' is safe: %s",
		                                    EXPIRE_SNAPSHOTS_NAME, location, ex.what());
	}
	auto separator = fs.PathSeparator(canonical_root);
	for (auto &candidate : candidates) {
		if (!IsLocalPath(candidate)) {
			continue;
		}
		try {
			auto canonical = fs.CanonicalizePath(candidate);
			if (!StringUtil::StartsWith(canonical, canonical_root + separator)) {
				throw InvalidConfigurationException("%s: expired file '%s' is outside the local table location",
				                                    EXPIRE_SNAPSHOTS_NAME, candidate);
			}
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::OUT_OF_MEMORY) {
				throw;
			}
			throw InvalidConfigurationException("%s: cannot prove expired file '%s' is inside the local table "
			                                    "location: %s",
			                                    EXPIRE_SNAPSHOTS_NAME, candidate, ex.what());
		}
	}
}

void ValidateManifestList(const IcebergSnapshot &snapshot, const vector<IcebergManifestListEntry> &manifests) {
	if (snapshot.manifest_list.empty() && !snapshot.manifests.empty()) {
		if (manifests.size() != snapshot.manifests.size()) {
			throw InvalidConfigurationException("%s: snapshot %lld declares %llu inline manifests but %llu were loaded",
			                                    EXPIRE_SNAPSHOTS_NAME, snapshot.snapshot_id,
			                                    static_cast<uint64_t>(snapshot.manifests.size()),
			                                    static_cast<uint64_t>(manifests.size()));
		}
		return;
	}
	idx_t data_files = 0;
	idx_t delete_files = 0;
	for (auto &entry : manifests) {
		auto &file = entry.GetFile();
		if (!file.counts || !file.counts->FilesComplete()) {
			throw InvalidConfigurationException("%s: snapshot %lld manifest '%s' has incomplete file counts",
			                                    EXPIRE_SNAPSHOTS_NAME, snapshot.snapshot_id, file.manifest_path);
		}
		auto live_files = *file.counts->added_files_count + *file.counts->existing_files_count;
		if (file.content == IcebergManifestContentType::DATA) {
			data_files += live_files;
		} else {
			delete_files += live_files;
		}
	}
	auto &metrics = snapshot.metrics.GetMetrics();
	auto data_metric = metrics.find(IcebergSnapshotMetricType::TOTAL_DATA_FILES);
	auto delete_metric = metrics.find(IcebergSnapshotMetricType::TOTAL_DELETE_FILES);
	if (data_metric == metrics.end() || delete_metric == metrics.end() || data_metric->second < 0 ||
	    delete_metric->second < 0) {
		throw InvalidConfigurationException("%s: snapshot %lld omits its total data/delete file counts",
		                                    EXPIRE_SNAPSHOTS_NAME, snapshot.snapshot_id);
	}
	if (static_cast<idx_t>(data_metric->second) != data_files ||
	    static_cast<idx_t>(delete_metric->second) != delete_files) {
		throw InvalidConfigurationException("%s: snapshot %lld file counts do not match its manifest list",
		                                    EXPIRE_SNAPSHOTS_NAME, snapshot.snapshot_id);
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
			throw InvalidConfigurationException("%s: manifest '%s' produced no readable entries", EXPIRE_SNAPSHOTS_NAME,
			                                    file.manifest_path);
		}
		return entries;
	}
	if (!complete) {
		if (snapshot.manifest_list.empty() && !snapshot.manifests.empty()) {
			return entries;
		}
		throw InvalidConfigurationException("%s: manifest '%s' has incomplete file counts", EXPIRE_SNAPSHOTS_NAME,
		                                    file.manifest_path);
	}
	auto expected =
	    *file.counts->added_files_count + *file.counts->existing_files_count + *file.counts->deleted_files_count;
	if (entries.size() != expected) {
		throw InvalidConfigurationException("%s: manifest '%s' counts do not match: %llu entries vs %llu declared",
		                                    EXPIRE_SNAPSHOTS_NAME, file.manifest_path,
		                                    static_cast<uint64_t>(entries.size()), static_cast<uint64_t>(expected));
	}
	return entries;
}

void AddSnapshotFiles(ClientContext &context, const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot,
                      bool expired, unordered_set<string> &manifest_lists, unordered_set<string> &manifests,
                      unordered_set<string> &data_files) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (!snapshot.manifest_list.empty()) {
		manifest_lists.insert(snapshot.manifest_list);
	}
	vector<IcebergManifestListEntry> snapshot_manifests;
	try {
		IcebergManifestList::LoadManifestFiles(ScanInfoFor(metadata, snapshot), metadata, context, snapshot_manifests);
	} catch (std::exception &ex) {
		if (!expired) {
			throw;
		}
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::OUT_OF_MEMORY) {
			throw;
		}
		throw InvalidConfigurationException("%s: could not read expired snapshot metadata: %s", EXPIRE_SNAPSHOTS_NAME,
		                                    ex.what());
	}
	ValidateManifestList(snapshot, snapshot_manifests);
	for (auto &manifest : snapshot_manifests) {
		manifests.insert(manifest.GetFile().manifest_path);
		vector<IcebergManifestListEntry> single_manifest;
		single_manifest.push_back(manifest);
		single_manifest[0].ResetManifestEntries();
		IcebergOptions options;
		try {
			auto scan = AvroScan::ScanManifest(ScanInfoFor(metadata, snapshot), single_manifest, options, fs, "",
			                                   metadata, context);
			auto reader = make_uniq<manifest_file::ManifestReader>(*scan);
			while (!reader->Finished()) {
				reader->Read();
			}
			scan->Complete();
		} catch (std::exception &ex) {
			if (!expired) {
				throw;
			}
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::OUT_OF_MEMORY) {
				throw;
			}
			throw InvalidConfigurationException("%s: could not read expired snapshot metadata: %s",
			                                    EXPIRE_SNAPSHOTS_NAME, ex.what());
		}
		auto &entries = ValidatedEntries(single_manifest[0], snapshot);
		for (auto &entry : entries) {
			if (entry.status != IcebergManifestEntryStatusType::DELETED) {
				data_files.insert(entry.data_file.file_path);
			}
		}
	}
}

class ExpireSnapshotsCleanupState : public ClientContextState {
public:
	void Add(vector<string> files) {
		lock_guard<mutex> guard(lock);
		pending.insert(pending.end(), files.begin(), files.end());
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		vector<string> files;
		{
			lock_guard<mutex> guard(lock);
			files.swap(pending);
		}
		if (files.empty()) {
			return;
		}
		auto &fs = FileSystem::GetFileSystem(context);
		for (auto &path : files) {
			try {
				fs.TryRemoveFile(path);
			} catch (std::exception &ex) {
				DUCKDB_LOG(context, IcebergLogType, "Iceberg Snapshot Expiration, cleanup failed for '%s': %s", path,
				           ex.what());
			}
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		Clear();
	}

private:
	void Clear() {
		lock_guard<mutex> guard(lock);
		pending.clear();
	}

	mutex lock;
	vector<string> pending;
};

} // namespace

vector<string> PlanExpiredSnapshotFiles(ClientContext &context, const IcebergTableMetadata &metadata,
                                        const vector<int64_t> &expired_snapshot_ids) {
	vector<string> result;
	if (expired_snapshot_ids.empty()) {
		return result;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto prefix = TableLocationPrefix(NormalizeAbsolutePath(metadata.location, "table location", fs));

	unordered_set<int64_t> expired_ids(expired_snapshot_ids.begin(), expired_snapshot_ids.end());
	unordered_set<string> expired_lists;
	unordered_set<string> expired_manifests;
	unordered_set<string> expired_data;
	for (auto snapshot_id : expired_ids) {
		auto snapshot = metadata.FindSnapshotByIdInternal(snapshot_id);
		if (!snapshot) {
			throw InvalidConfigurationException("%s: expired snapshot %lld is missing from the table metadata",
			                                    EXPIRE_SNAPSHOTS_NAME, snapshot_id);
		}
		if (metadata.iceberg_version >= 2 && snapshot->manifest_list.empty()) {
			throw InvalidConfigurationException("%s: expired snapshot %lld has no manifest-list", EXPIRE_SNAPSHOTS_NAME,
			                                    snapshot_id);
		}
		if (!snapshot->manifest_list.empty()) {
			ValidateCandidatePath(fs, prefix, snapshot->manifest_list);
		}
		AddSnapshotFiles(context, metadata, *snapshot, true, expired_lists, expired_manifests, expired_data);
	}

	unordered_set<string> retained_lists;
	unordered_set<string> retained_manifests;
	unordered_set<string> retained_data;
	for (auto &entry : metadata.snapshots) {
		if (expired_ids.count(entry.first)) {
			continue;
		}
		AddSnapshotFiles(context, metadata, entry.second, false, retained_lists, retained_manifests, retained_data);
	}
	unordered_set<string> protected_paths;
	auto protect = [&](const unordered_set<string> &paths, const char *what) {
		for (auto &path : paths) {
			protected_paths.insert(NormalizeAbsolutePath(path, what, fs));
		}
	};
	protect(retained_lists, "retained manifest list");
	protect(retained_manifests, "retained manifest");
	protect(retained_data, "retained data file");

	unordered_set<string> planned;
	auto add_candidates = [&](const unordered_set<string> &paths) {
		for (auto &path : paths) {
			auto key = ValidateCandidatePath(fs, prefix, path);
			if (!protected_paths.count(key) && planned.insert(key).second) {
				result.push_back(path);
			}
		}
	};
	add_candidates(expired_lists);
	add_candidates(expired_manifests);
	add_candidates(expired_data);

	ValidateLocalCandidates(fs, metadata.location, result);
	std::sort(result.begin(), result.end());
	return result;
}

void ScheduleExpiredFileCleanup(ClientContext &context, vector<string> files) {
	if (files.empty()) {
		return;
	}
	auto state = context.registered_state->GetOrCreate<ExpireSnapshotsCleanupState>(CLEANUP_KEY);
	state->Add(std::move(files));
}

} // namespace duckdb
