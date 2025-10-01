//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/reorder_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/split_algorithm.hpp"
#define REORDER_DATACHUNK true

namespace duckdb {
class ReorderGet {
public:
	explicit ReorderGet(ClientContext &context) : context(context) {
	}
	~ReorderGet() = default;

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	std::deque<std::pair<idx_t, idx_t>> GetTableCardOrder() {
		return table_card_order_bak;
	}

	const bool NeedFilterPushDown() {
		return need_filter_push_down;
	}

	void Clear() {
		in_clause = false;
		need_filter_push_down = false;
	}

	//! Reorder the get and data chunk nodes of the first subquery (the one to be executed) when it has
	//!  CROSS_PRODUCT with the best relation order (smaller first)
	bool ReorderTables(subquery_queue &subqueries);

private:
	ClientContext &context;

	bool in_clause = false;
	bool need_filter_push_down = false;

	// from the biggest to the smallest
	std::deque<std::pair<idx_t, idx_t>> table_card_order_bak;
};
} // namespace duckdb
