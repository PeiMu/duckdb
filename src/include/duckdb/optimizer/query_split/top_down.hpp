//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/top_down.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/query_split/split_algorithm.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

//! Based on the DAG of the logical plan, we generate the subqueries bottom-up
class TopDownSplit : public SplitAlgorithm {
public:
	explicit TopDownSplit(ClientContext &context) : SplitAlgorithm(context) {};
	~TopDownSplit() override = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan) override;
	// todo: extract to a stand-alone class?
	void MergeSubquery(unique_ptr<LogicalOperator> &plan, subquery_queue old_subqueries) override;
	void UnMergeSubquery(unique_ptr<LogicalOperator> &plan) override;
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> &plan, bool &needToSplit) override;
	void Clear() {
		filter_parent = false;
		while (!table_expr_queue.empty()) {
			table_expr_queue.pop();
		}
		while (!used_table_queue.empty()) {
			used_table_queue.pop();
		}
		sibling_used_table.clear();
		target_tables.clear();
		proj_expr.clear();
		op_levels = 0;
	};

public:
	table_expr_info GetTableExprQueue() {
		return table_expr_queue;
	}

	std::set<TableExpr> GetProjExpr() {
		return proj_expr;
	}

	std::queue<std::set<idx_t>> GetUsedTableQueue() {
		return used_table_queue;
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

	void CollectUsedTablePerLevel();
	void CollectUsedTable(const unique_ptr<LogicalOperator> &subquery, std::set<idx_t> &table_in_subquery);

	void InsertTableBlocks(unique_ptr<LogicalOperator> &op,
	                       unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks,
	                       std::queue<idx_t> &table_blocks_key_order);

private:
	bool filter_parent = false;

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
	std::set<TableExpr> proj_expr;

	//
	int op_levels = 0;
};

} // namespace duckdb