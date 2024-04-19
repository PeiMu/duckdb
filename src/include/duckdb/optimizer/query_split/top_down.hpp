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
	//! the collection of all levels of subqueries in a bottom-up order, e.g. the lowest level subquery is the first
	//! element in the queue and will be executed first
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan) override;

public:
	std::stack<std::set<TableExpr>> &GetTableExprStack() {
		return table_expr_stack;
	}

protected:
	//! Extract the subquery in the top-down order, and insert
	//! the operations of the same level to `subqueries`
	void VisitOperator(LogicalOperator &op) override;

private:
	//! get the <table_index, expression_index> pair by checking which column is used in the projection
	void GetProjTableExpr(const LogicalProjection &proj_op);
	//! get the <table_index, expression_index> pair by checking which column is used in the join
	void GetJoinTableExpr(const LogicalComparisonJoin &join_op, bool same_level);
	//! get the <table_index, expression_index> pair by checking which column is used in the filter
	void GetFilterTableExpr(const LogicalFilter &filter_op);

	//! Collect all used tables into `used_tables`
	void GetTargetTables(LogicalOperator &op);

private:
	bool filter_parent = false;

	// the collection of necessary table/column information in a top-down order, e.g. the lowest level is the last
	// element in the stack and will be got first
	std::stack<std::set<TableExpr>> table_expr_stack;
	// table index, table entry
	std::unordered_map<idx_t, LogicalGet *> used_tables;
};

} // namespace duckdb