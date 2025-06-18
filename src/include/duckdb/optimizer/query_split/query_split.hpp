//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/split_algo_factor.hpp"

// #define ENABLE_PARALLEL_EXECUTION		false
// #define ENABLE_DEBUG_PRINT				false
// #define ENABLE_MEASURE_EXE_TIME    		true
#define TIME_BREAK_DOWN            false
#define MANUAL_EXPLAIN_ANALYZE     false
#define WHOLE_PLAN_EXPLAIN_ANALYZE false
#define ALWAYS_SPLIT               true
#define ENABLE_REORDER_PLAN        false

#if ENABLE_MEASURE_EXE_TIME || ENABLE_MERGE_BACK_PLAN || ENABLE_DEBUG_PRINT
inline bool execute_plan = false;
#endif

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
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan, bool follow_pipeline_breaker);
	//! Reset variables and initialize
	unique_ptr<LogicalOperator> Clear(unique_ptr<LogicalOperator> plan);

public:
	table_expr_info GetTableExprQueue() {
		if (nullptr == query_splitter)
			return table_expr_info();

		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		return top_down_splitter->GetTableExprQueue();
	}

	std::vector<TableExpr> GetProjExpr() {
		if (nullptr == query_splitter)
			return std::vector<TableExpr>();

		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		return top_down_splitter->GetProjExpr();
	}

	subquery_queue GetSubqueries() {
		if (nullptr == query_splitter)
			return subquery_queue();
		return std::move(query_splitter->subqueries);
	}

	int GetSplitNumber() {
		if (nullptr == query_splitter)
			return -1;

		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		return top_down_splitter->GetSplitNumber();
	}

private:
	void VisitOperator(LogicalOperator &op);

private:
	ClientContext &context;
	std::unique_ptr<SplitAlgorithm> query_splitter;
};

} // namespace duckdb
