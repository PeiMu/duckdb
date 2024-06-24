//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/split_algo_factor.hpp"

#define ENABLE_QUERY_SPLIT true
#define ENABLE_DEBUG_PRINT true

namespace duckdb {

class QuerySplit {
public:
	explicit QuerySplit(ClientContext &context) : context(context) {
		EnumSplitAlgorithm split_algorithm = top_down;
		if (nullptr == query_splitter)
			query_splitter = SplitAlgorithmFactor::CreateSplitter(context, split_algorithm);
	};
	~QuerySplit() = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan);
	void MergeSubquery(unique_ptr<LogicalOperator> &plan,
	                                          unique_ptr<LogicalOperator> subquery);
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> &plan, bool &needToSplit);

	unique_ptr<LogicalOperator> UnMergeSubquery(unique_ptr<LogicalOperator> &uniquePtr);

public:
	table_expr_info GetTableExprQueue() {
		if (nullptr == query_splitter)
			return table_expr_info();

		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		return top_down_splitter->GetTableExprQueue();
	}

	std::set<TableExpr> GetProjExpr() {
		if (nullptr == query_splitter)
			return std::set<TableExpr>();

		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		return top_down_splitter->GetProjExpr();
	}

	subquery_queue GetSubqueries() {
		if (nullptr == query_splitter)
			return subquery_queue();
		return std::move(query_splitter->subqueries);
	}

	std::queue<std::set<idx_t>> GetUsedTableQueue() {
		if (nullptr == query_splitter)
			return std::queue<std::set<idx_t>>();
		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		return top_down_splitter->GetUsedTableQueue();
	}

private:
	ClientContext &context;
	std::unique_ptr<SplitAlgorithm> query_splitter;
};

} // namespace duckdb
