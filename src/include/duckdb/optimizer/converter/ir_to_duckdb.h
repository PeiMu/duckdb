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
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "read.hpp"
#include "simplest_ir.h"

#define INJECT_PLAN true

namespace duckdb {
class IRConverter {
public:
	IRConverter() = default;
	~IRConverter() = default;

	unique_ptr<LogicalOperator> InjectPlan(const char *postgres_plan_str,
	                                       unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
	                                       std::vector<unique_ptr<Expression>> &expr_vec);

	std::vector<unique_ptr<Expression>> CollectFilterExpressions(unique_ptr<LogicalOperator> &duckdb_plan);

	std::unordered_map<std::string, unique_ptr<LogicalGet>> GetTableMap(unique_ptr<LogicalOperator> &duckdb_plan);

private:
	unique_ptr<LogicalComparisonJoin> ConstructDuckdbJoin(SimplestJoin *pJoin, unique_ptr<LogicalOperator> left_child,
	                                                      unique_ptr<LogicalOperator> right_child,
	                                                      const unordered_map<int, int> &pg_duckdb_table_idx);
	bool CheckCondIndex(const unique_ptr<Expression> &expr, const unique_ptr<LogicalOperator> &child);
	unique_ptr<LogicalOperator> ConstructDuckdbPlan(SimplestStmt *postgres_plan_pointer,
	                                                unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
	                                                const unordered_map<int, int> &pg_duckdb_table_idx,
	                                                std::vector<unique_ptr<Expression>> &expr_vec);
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

	// <duckdb_table_index, duckdb's column_ids>
	// in duckdb's column_ids, it convert table entry's id to binding id
	std::unordered_map<uint64_t, vector<column_t>> column_idx_mapping;
};

} // namespace duckdb