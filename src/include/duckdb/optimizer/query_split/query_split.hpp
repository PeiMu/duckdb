//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/split_algo_factor.hpp"

namespace duckdb {

class QuerySplit {
public:
	explicit QuerySplit(ClientContext &context) : context(context) {};
	~QuerySplit() = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan, unique_ptr<DataChunk> previous_result,
	                                     bool &subquery_loop);

private:
	ClientContext &context;
	std::queue<unique_ptr<LogicalOperator>> subqueries;
	std::unique_ptr<SplitAlgorithm> query_splitter;
};

} // namespace duckdb
