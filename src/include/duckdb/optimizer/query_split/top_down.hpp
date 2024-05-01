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

namespace duckdb {

//! Based on the DAG of the logical plan, we generate the subqueries bottom-up
class TopDownSplit : public SplitAlgorithm {
public:
	explicit TopDownSplit(ClientContext &context) : SplitAlgorithm(context) {};
	~TopDownSplit() override = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan) override;

public:
	table_expr_info GetTableExprQueue() {
		return table_expr_queue;
	}

	std::set<TableExpr> GetProjExpr() {
		return proj_expr;
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
	//! get the `table_expr_queue` by checking which column is used in the filter
	std::set<TableExpr> GetFilterTableExpr(const LogicalFilter &filter_op);

	//! Collect all used tables into `used_tables`
	void GetTargetTables(LogicalOperator &op);

private:
	bool filter_parent = false;

	// the collection of necessary table/column information in a top-down order, e.g. the lowest level is the last
	// element in the stack and will be got first. PS: we only modify it in `VisitOperator`
	table_expr_info table_expr_queue;
	// table index, table entry
	std::unordered_map<idx_t, LogicalGet *> used_tables;
	// expressions in the projection node
	std::set<TableExpr> proj_expr;
};

} // namespace duckdb