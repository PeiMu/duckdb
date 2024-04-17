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

#include <queue>

namespace duckdb {

struct TableExpr {
	idx_t table_idx;
	idx_t column_idx;
	std::string column_name;
	LogicalType return_type;

	bool operator==(const TableExpr &other) const {
		return table_idx == other.table_idx && column_idx == other.column_idx && column_name == other.column_name;
	}
};

struct TableExprHash {
	size_t operator()(const TableExpr &table_expr) const {
		return std::hash<idx_t> {}(table_expr.table_idx) ^ std::hash<idx_t> {}(table_expr.column_idx) ^
		       std::hash<std::string> {}(table_expr.column_name);
	}
};

//! Based on the DAG of the logical plan, we generate the subqueries bottom-up
class TopDownSplit : public SplitAlgorithm {
public:
	explicit TopDownSplit(ClientContext &context) : SplitAlgorithm(context) {};
	~TopDownSplit() override = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan, bool &subquery_loop) override;

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

	//! Collect all used tables into `target_tables`
	void GetTargetTables(LogicalOperator &op);

private:
	bool filter_parent = false;
	std::queue<std::vector<unique_ptr<LogicalOperator>>> subqueries;
	std::stack<std::unordered_set<TableExpr, TableExprHash>> table_expr_stack;
	// table index, table entry
	std::unordered_map<idx_t, LogicalGet *> target_tables;
};

} // namespace duckdb