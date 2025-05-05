//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/top_down.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/query_split/query_split_util.h"
#include "duckdb/optimizer/query_split/split_algorithm.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

#define SPLIT_FILTER true

//! Based on the DAG of the logical plan, we generate the subqueries bottom-up
class TopDownSplit : public SplitAlgorithm {
public:
	explicit TopDownSplit(ClientContext &context) : SplitAlgorithm(context) {};
	~TopDownSplit() override = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan, bool follow_pipeline_breaker) override;
	void Clear() {
#if SPLIT_FILTER
		filter_parent = false;
#endif
		top_most = true;
		while (!table_expr_queue.empty()) {
			table_expr_queue.pop();
		}
		while (!used_table_queue.empty()) {
			used_table_queue.pop();
		}
		sibling_used_table.clear();
		target_tables.clear();
		proj_expr.clear();
		query_split_index = 0;
		used_table_ids.clear();
	};

public:
	table_expr_info GetTableExprQueue() {
		return table_expr_queue;
	}

	std::vector<TableExpr> GetProjExpr() {
		return proj_expr;
	}

	int GetSplitNumber() {
		return query_split_index;
	}

protected:
	//! Extract the subquery in the top-down order, and insert
	//! the operations of the same level to `subqueries`
	// todo: maybe it can be a standalone class?
	void VisitOperator(LogicalOperator &op) override;

private:
	//! get the `proj_expr` by checking which column is used in the projection
	void GetProjTableExpr(const LogicalProjection &proj_op);
	//! get the `proj_expr` by checking which column is used in the aggregate
	void GetAggregateTableExpr(const LogicalAggregate &aggregate_op);
	//! get the `table_expr_queue` by checking which column is used in the join
	std::set<TableExpr> GetJoinTableExpr(const LogicalComparisonJoin &join_op);
	//! get the `table_expr_queue` by checking which column is used in the cross_product
	std::set<TableExpr> GetCrossProductTableExpr(const LogicalCrossProduct &product_op);
	//! get the `table_expr_queue` by checking which column is used in the filter
	std::set<TableExpr> GetFilterTableExpr(const LogicalFilter &filter_op);
	//! get the `table_expr_queue` by checking which column is used in the SEQ_SCAN
	std::set<TableExpr> GetSeqScanTableExpr(const LogicalGet &get_op);

	//! Collect all used tables into `target_tables`
	void GetTargetTables(LogicalOperator &op);
	void AddTableExprs(std::set<TableExpr> &table_exprs, const unique_ptr<Expression> &expr);
	void GetColRefExpr(const BoundColumnRefExpression &column_ref_expr);
	void AddFunctionExpr(std::set<TableExpr> &table_exprs, const BoundFunctionExpression &function_expr);
	void GetFunctionExpr(const BoundFunctionExpression &function_expr);
	void AddCastExpr(std::set<TableExpr> &table_exprs, const BoundCastExpression &cast_expr);
	void GetCastExpr(const BoundCastExpression &cast_expr);
	void AddComparisonExpr(std::set<TableExpr> &table_exprs, const BoundComparisonExpression &comparison_expr);

private:
#if SPLIT_FILTER
	bool filter_parent = false;
#endif
	// when following the pipeline breaker role, we need to split the top-most JOIN; sometimes the top-most operator is
	//  a filter node, which should also be splitted.
	bool top_most = true;
	std::set<TableExpr> last_level_table_exprs;

	// the collection of necessary table/column information in a top-down order, e.g. the lowest level is the last
	// element in the stack and will be got first. PS: we only modify it in `VisitOperator`
	table_expr_info table_expr_queue;
	// the collection of the used tables of the current level
	std::queue<std::set<idx_t>> used_table_queue;
	// todo: fix this when supporting parallel execution
	std::set<idx_t> sibling_used_table;
	// table index, table entry
	std::unordered_set<idx_t> target_tables;
	// expressions in the projection node
	std::vector<TableExpr> proj_expr;
	int query_split_index = 0;

	// we need to further check if all the CROSS_PRODUCT can be simplified in the subqueries
	// in a bottom-up order
	std::unordered_set<idx_t> used_table_ids;

	bool follow_pipeline_breaker_ = false;
};

} // namespace duckdb
