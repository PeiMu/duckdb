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
class SubqueryPreparer : public LogicalOperatorVisitor {
public:
	SubqueryPreparer(Binder &binder, ClientContext &context) : binder(binder), context(context) {};
	~SubqueryPreparer() = default;

	//! Merge the data chunk (temp table) to the current subquery
	unique_ptr<LogicalOperator> MergeDataChunk(unique_ptr<LogicalOperator> subquery,
						   unique_ptr<QueryResult> previous_result);

	//! Generate the projection head node at the top of the current subquery
	unique_ptr<LogicalOperator> GenerateProjHead(const unique_ptr<LogicalOperator> &original_plan,
	                                             unique_ptr<LogicalOperator> subquery,
	                                             const table_expr_info &table_expr_queue);

	//! Adapt the selection node to the query AST
	shared_ptr<PreparedStatementData> AdaptSelect(shared_ptr<PreparedStatementData> original_stmt_data,
	                                              const unique_ptr<LogicalOperator> &subquery);

	table_expr_info UpdateTableIndex(table_expr_info table_expr_queue);

private:
	//! 1. find the insert point and insert the `ColumnDataGet` node to the logical plan;
	//! 2. update the table_idx and column_idx
	void VisitOperator(LogicalOperator &op) override;
	//! Because the `chunk_scan` will create a new table index and contains the result of all tables (SEQ SCAN) of the
	//! current level, it is necessary to replace the index of the related expressions
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Binder &binder;
	ClientContext &context;
	// all columns needed of the current level are shown in the proj's expression
	std::set<TableExpr> proj_exprs;
	// so far we only execute the first child node and will miss the sibling info
	// todo: should be changed when supporting the multi-thread of sibling execution
	std::set<TableExpr> last_sibling_exprs;
	// a new chunk scan node with the last level's result, generated and merged in `MergeDataChunk`
	unique_ptr<LogicalColumnDataGet> chunk_scan;
	// `chunk_scan` will be moved, and we need one extra member to remember the new table index
	idx_t new_table_idx;
	// the collection of the old table indexes, to detect and be replaced to the new index by `UpdateTableIndex`
	std::vector<idx_t> old_table_idx;
};
} // namespace duckdb