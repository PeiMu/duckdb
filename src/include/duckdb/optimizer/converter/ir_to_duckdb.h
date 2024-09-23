//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/ir_to_duckdb.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "read.hpp"
#include "simplest_ir.h"

namespace duckdb {
class IRConverter {
public:
	IRConverter() = default;
	~IRConverter() = default;

	unique_ptr<LogicalOperator> InjectPlan(unique_ptr<LogicalOperator> duckdb_plan);

private:
	std::unordered_map<std::string, unique_ptr<LogicalGet>> GetTableMap(unique_ptr<LogicalOperator> &duckdb_plan);
	unique_ptr<LogicalOperator> ConstructPlan(LogicalOperator *new_plan, SimplestStmt *postgres_plan_pointer,
	                                          unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
	                                          const unordered_map<int, int> &pg_duckdb_table_idx);
	ExpressionType ConvertCompType(SimplestComparisonType type);
	LogicalType ConvertVarType(SimplestVarType type);
	void SetAttrVecName(std::vector<unique_ptr<SimplestAttr>> &attr_vec, const std::deque<table_str> &table_col_names);
	void SetCompExprName(std::vector<unique_ptr<SimplestVarConstComparison>> &comp_vec,
	                     const std::deque<table_str> &table_col_names);
	void SetCompExprName(std::vector<unique_ptr<SimplestVarComparison>> &comp_vec,
	                     const std::deque<table_str> &table_col_names);

	//! table index mapping from postgres to duckdb
	unordered_map<int, int> MatchTableIndex(const unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
	                                        const std::deque<table_str> &table_col_names);
	// todo: Refactor as SimplestVisitor (like `LogicalOperatorVisitor`)
	void AddTableColumnName(unique_ptr<SimplestStmt> &postgres_plan, const std::deque<table_str> &table_col_names);
};

} // namespace duckdb