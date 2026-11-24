#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
struct IcebergTableMetadata;

//! The files only the snapshots in 'expired_snapshot_ids' reach, after subtracting every path a retained
//! snapshot or a current metadata file still reaches. Throws when the metadata cannot prove the live set.
vector<string> PlanExpiredSnapshotFiles(ClientContext &context, const IcebergTableMetadata &metadata,
                                        const vector<int64_t> &expired_snapshot_ids);

//! Queue files for deletion after the current transaction commits. A rollback or a failed commit deletes nothing.
void ScheduleExpiredFileCleanup(ClientContext &context, vector<string> files);

} // namespace duckdb
