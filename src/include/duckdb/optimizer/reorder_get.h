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

private:
	ClientContext &context;

	bool in_clause = false;
};
} // namespace duckdb