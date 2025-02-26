//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/reorder_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#define REORDER_DATACHUNK			true

namespace duckdb {
class ReorderGet {
public:
	explicit ReorderGet(ClientContext &context) : context(context) {
	}
	~ReorderGet() = default;

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	std::deque<std::pair<idx_t, idx_t>> GetTableCardOrder() { return table_card_order_bak; }

private:
	ClientContext &context;

	bool in_clause = false;

	// from the biggest to the smallest
	std::deque<std::pair<idx_t, idx_t>> table_card_order_bak;
};
} // namespace duckdb