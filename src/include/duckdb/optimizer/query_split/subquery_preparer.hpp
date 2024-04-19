//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/subquery_preparer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/optimizer/query_split/top_down.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

//! Prepare the subquery, including
//! 1. merging the data chunk (temp table) to the logical plan,
//! 2. creating the projection head at the top of the logical plan,
//! 3. adapting the selection node to the query AST
class SubqueryPreparer {
public:
	SubqueryPreparer(Binder &binder, ClientContext &context) : binder(binder), context(context) {};
	~SubqueryPreparer() = default;

	//! Merge the data chunk (temp table) to the current subquery
	unique_ptr<LogicalOperator> MergeDataChunk(unique_ptr<LogicalOperator> subquery,
	                                           unique_ptr<DataChunk> previous_result,
	                                           const std::set<TableExpr> &table_expr_set);

	//! Generate the projection head node at the top of the current subquery
	unique_ptr<LogicalOperator> GenerateProjHead(const unique_ptr<LogicalOperator> &original_plan,
	                                             unique_ptr<LogicalOperator> subquery,
	                                             const std::stack<std::set<TableExpr>> &table_expr_stack);

	//! Adapt the selection node to the query AST
	shared_ptr<PreparedStatementData> AdaptSelect(shared_ptr<PreparedStatementData> original_stmt_data,
	                                              const unique_ptr<LogicalOperator> &subquery);

protected:
	Binder &binder;
	ClientContext &context;
};
} // namespace duckdb
