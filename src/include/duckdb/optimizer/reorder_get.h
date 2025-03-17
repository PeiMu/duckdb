//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/reorder_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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

private:
	ClientContext &context;

	bool in_clause = false;
	bool need_filter_push_down = false;

	// from the biggest to the smallest
	std::deque<std::pair<idx_t, idx_t>> table_card_order_bak;
};
} // namespace duckdb