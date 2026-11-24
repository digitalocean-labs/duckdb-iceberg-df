#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

constexpr const char *ORPHAN_FILES_MIN_AGE_SETTING = "iceberg_remove_orphan_files_min_age";
struct OrphanFilesResult {
	int64_t listed_files = 0;
	int64_t referenced_present = 0;
	int64_t orphan_files = 0;
	int64_t orphan_bytes = 0;
	int64_t deleted_files = 0;
	int64_t deleted_bytes = 0;
	int64_t young_unreferenced = 0;
	int64_t delete_errors = 0;
};

OrphanFilesResult RemoveOrphanFiles(ClientContext &context, const QualifiedName &table_name, int64_t older_than_micros,
                                    bool dry_run);

} // namespace duckdb
