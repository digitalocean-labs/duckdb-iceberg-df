#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "function/iceberg_functions.hpp"
#include "maintenance/orphan_files.hpp"

namespace duckdb {

namespace {

constexpr const char *FUNCTION_NAME = "iceberg_remove_orphan_files";
constexpr int64_t DEFAULT_OLDER_THAN_MICROS = 7LL * 24 * Interval::MICROS_PER_HOUR;
constexpr int64_t DEFAULT_MIN_DESTRUCTIVE_OLDER_THAN_MICROS = 3LL * 24 * Interval::MICROS_PER_HOUR;

struct RemoveOrphanFilesBindData : public TableFunctionData {
	QualifiedName table_name;
	int64_t older_than_micros = DEFAULT_OLDER_THAN_MICROS;
	int64_t min_age_micros = DEFAULT_MIN_DESTRUCTIVE_OLDER_THAN_MICROS;
	bool dry_run = true;
};

struct RemoveOrphanFilesGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<RemoveOrphanFilesGlobalState>();
	}

	bool finished = false;
};

QualifiedName ParseTableName(const Value &identifier_value) {
	if (identifier_value.IsNull()) {
		throw InvalidInputException("%s: table_name cannot be NULL", FUNCTION_NAME);
	}
	auto identifier = StringValue::Get(identifier_value);
	auto parts = QualifiedName::ParseComponents(identifier);
	if (parts.size() != 3) {
		throw InvalidInputException("%s: expected catalog.schema.table, got '%s'", FUNCTION_NAME, identifier);
	}
	for (auto &part : parts) {
		if (part.empty()) {
			throw InvalidInputException("%s: empty identifier component", FUNCTION_NAME);
		}
	}
	return QualifiedName {parts[0], parts[1], parts[2]};
}

unique_ptr<FunctionData> RemoveOrphanFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RemoveOrphanFilesBindData>();
	result->table_name = ParseTableName(input.inputs[0]);
	for (auto &kv : input.named_parameters) {
		auto name = StringUtil::Lower(kv.first.GetIdentifierName());
		if (kv.second.IsNull()) {
			throw InvalidInputException("%s: '%s' cannot be NULL", FUNCTION_NAME, name);
		}
		if (name == "older_than") {
			int64_t micros;
			if (!Interval::TryGetMicro(IntervalValue::Get(kv.second), micros)) {
				throw InvalidInputException("%s: 'older_than' is too large", FUNCTION_NAME);
			}
			result->older_than_micros = micros;
		} else if (name == "dry_run") {
			result->dry_run = BooleanValue::Get(kv.second);
		}
	}
	if (result->older_than_micros < 0) {
		throw InvalidInputException("%s: 'older_than' must not be negative", FUNCTION_NAME);
	}
	if (result->older_than_micros > Timestamp::GetCurrentTimestamp().value) {
		throw InvalidInputException("%s: 'older_than' is too large", FUNCTION_NAME);
	}
	interval_t min_age;
	if (context.TryGetCurrentSetting(ORPHAN_FILES_MIN_AGE_SETTING, min_age)) {
		result->min_age_micros = Interval::GetMicro(min_age);
	}
	if (!result->dry_run && result->older_than_micros < result->min_age_micros) {
		throw InvalidInputException("%s: 'older_than' must be at least %lld seconds for a destructive run",
		                            FUNCTION_NAME, result->min_age_micros / Interval::MICROS_PER_SEC);
	}
	for (auto column : {"listed_files", "referenced_present", "orphan_files", "orphan_bytes", "deleted_files",
	                    "deleted_bytes", "young_unreferenced", "delete_errors"}) {
		names.emplace_back(column);
		return_types.emplace_back(LogicalType::BIGINT);
	}
	return std::move(result);
}

void RemoveOrphanFilesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RemoveOrphanFilesBindData>();
	auto &state = data.global_state->Cast<RemoveOrphanFilesGlobalState>();
	if (state.finished) {
		output.SetChildCardinality(0);
		return;
	}
	state.finished = true;
	auto result = RemoveOrphanFiles(context, bind_data.table_name, bind_data.older_than_micros, bind_data.dry_run);
	int64_t values[] = {result.listed_files,       result.referenced_present, result.orphan_files,
	                    result.orphan_bytes,       result.deleted_files,      result.deleted_bytes,
	                    result.young_unreferenced, result.delete_errors};
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		FlatVector::GetDataMutable<int64_t>(output.data[i])[0] = values[i];
	}
	output.SetChildCardinality(1);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRemoveOrphanFilesFunction() {
	TableFunctionSet function_set(FUNCTION_NAME);
	TableFunction function(FUNCTION_NAME, FunctionSignature().AddPositionalOnly("table_name", LogicalType::VARCHAR),
	                       RemoveOrphanFilesFunction, RemoveOrphanFilesBind, RemoveOrphanFilesGlobalState::Init);
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("older_than", LogicalType::INTERVAL).Add("dry_run", LogicalType::BOOLEAN);
	});
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
