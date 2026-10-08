//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_pruning.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Which mechanism excluded a row group, so skips can be attributed in the profiling metrics
enum class ParquetRowGroupPruneReason : uint8_t { NONE, STATISTICS, BLOOM_FILTER };

//! Which pruning mechanisms are enabled, read from the settings once per row group
struct ParquetPruningConfig {
	bool row_group_statistics = true;
	bool bloom_filter = true;
	bool page_statistics = true;
	bool dictionary = true;
};

//! Pruning counters, accumulated per thread and summed into the scan's global state
struct ParquetPruningCounters {
	//! Row groups excluded before any of their data was requested
	idx_t row_groups_pruned_stats = 0;
	idx_t row_groups_pruned_bloom = 0;
	//! Data pages skipped, by mechanism, with the compressed bytes left undecoded
	idx_t pages_pruned_stats = 0;
	idx_t pages_pruned_dictionary = 0;
	idx_t pages_pruned_bytes = 0;
	//! Pages pruned whose bytes the prefetcher had already fetched - decode saved, but no I/O
	idx_t pages_pruned_after_fetch = 0;
	idx_t pages_pruned_after_fetch_bytes = 0;

	void Add(const ParquetPruningCounters &other) {
		row_groups_pruned_stats += other.row_groups_pruned_stats;
		row_groups_pruned_bloom += other.row_groups_pruned_bloom;
		pages_pruned_stats += other.pages_pruned_stats;
		pages_pruned_dictionary += other.pages_pruned_dictionary;
		pages_pruned_bytes += other.pages_pruned_bytes;
		pages_pruned_after_fetch += other.pages_pruned_after_fetch;
		pages_pruned_after_fetch_bytes += other.pages_pruned_after_fetch_bytes;
	}

	void Reset() {
		*this = ParquetPruningCounters();
	}

	bool Any() const {
		return row_groups_pruned_stats || row_groups_pruned_bloom || pages_pruned_stats || pages_pruned_dictionary ||
		       pages_pruned_bytes || pages_pruned_after_fetch || pages_pruned_after_fetch_bytes;
	}
};

} // namespace duckdb
