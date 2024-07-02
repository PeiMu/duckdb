//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/split_algo_factor.hpp"

#define ENABLE_QUERY_SPLIT        true
#define ENABLE_PARALLEL_EXECUTION false
#define ENABLE_DEBUG_PRINT        true

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
	void MergeSubquery(unique_ptr<LogicalOperator> &plan, subquery_queue old_subqueries);
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> &plan, bool &needToSplit);

	void UnMergeSubquery(unique_ptr<LogicalOperator> &plan);

	void Clear() {
		if (nullptr != query_splitter) {
			auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
			top_down_splitter->Clear();
		}
	};

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

private:
	ClientContext &context;
	std::unique_ptr<SplitAlgorithm> query_splitter;
};

} // namespace duckdb
