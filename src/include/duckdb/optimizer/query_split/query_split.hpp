//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/foreign_key_center.hpp"

namespace duckdb {

class QuerySplit {
public:
	explicit QuerySplit(ClientContext &context) : context(context) {};
	~QuerySplit() = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	ClientContext &context;
	std::queue<unique_ptr<LogicalOperator>> subqueries;
};

} // namespace duckdb
