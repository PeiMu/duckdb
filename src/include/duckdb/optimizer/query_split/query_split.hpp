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
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan);

public:
	std::stack<std::set<TableExpr>> GetTableExprStack() {
		if (nullptr == query_splitter)
			return std::stack<std::set<TableExpr>>();

		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		return top_down_splitter->GetTableExprStack();
	}

	void PopTableExprStack() {
		D_ASSERT(nullptr != query_splitter);
		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		top_down_splitter->GetTableExprStack().pop();
	}

	subquery_queue GetSubqueries() {
		if (nullptr == query_splitter)
			return subquery_queue();
		return std::move(query_splitter->subqueries);
	}

private:
	ClientContext &context;
	std::unique_ptr<SplitAlgorithm> query_splitter;
};

} // namespace duckdb
