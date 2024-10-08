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
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/optimizer/query_split/split_algorithm.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "read.hpp"
#include "simplest_ir.h"

#define INJECT_PLAN true

namespace duckdb {
class IRConverter {
public:
	IRConverter(Binder &binder, ClientContext &context) : binder(binder), context(context) {};
	~IRConverter() = default;

	std::vector<unique_ptr<Expression>> CollectFilterExpressions(unique_ptr<LogicalOperator> &duckdb_plan);

	std::unordered_map<std::string, unique_ptr<LogicalGet>> GetTableMap(unique_ptr<LogicalOperator> &duckdb_plan);

	unique_ptr<LogicalOperator> ConstructDuckdbPlan(SimplestStmt *postgres_plan_pointer,
	                                                unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
	                                                const unordered_map<int, int> &pg_duckdb_table_idx,
	                                                std::vector<unique_ptr<Expression>> &expr_vec,
	                                                unique_ptr<ColumnDataCollection> subquery_result,
	                                                idx_t result_chunk_idx);

	// todo: Refactor as SimplestVisitor (like `LogicalOperatorVisitor`)
	void AddTableColumnName(unique_ptr<SimplestStmt> &postgres_plan, const std::deque<table_str> &table_col_names);

	//! table index mapping from postgres to duckdb, <pg_idx, dd_idx>
	unordered_map<int, int> MatchTableIndex(const unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
	                                        const std::deque<table_str> &table_col_names, idx_t result_chunk_idx);

	unique_ptr<LogicalOperator> GenerateProjHead(const unique_ptr<LogicalOperator> &origin_dd_plan,
	                                             unique_ptr<LogicalOperator> new_dd_plan,
	                                             const unique_ptr<SimplestStmt> &pg_simplest_stmt,
	                                             const unordered_map<int, int> &pg_duckdb_table_idx);

	std::vector<TableExpr> GetTableExprFromTargetList(const std::vector<unique_ptr<SimplestAttr>> &pg_target_list,
	                                                  const unordered_map<int, int> &pg_duckdb_table_idx);

private:
	bool CheckCondIndex(const unique_ptr<Expression> &expr, const unique_ptr<LogicalOperator> &child);
	bool CheckExprExist(const unique_ptr<Expression> &expr, idx_t attr_table_idx);
	std::pair<idx_t, idx_t> ConvertTableColumnIndex(std::pair<unsigned int, unsigned int> postgres_table_column_pair,
	                                                const unordered_map<int, int> &pg_duckdb_table_idx);
	unique_ptr<LogicalComparisonJoin> ConstructDuckdbJoin(SimplestJoin *pJoin, unique_ptr<LogicalOperator> left_child,
	                                                      unique_ptr<LogicalOperator> right_child,
	                                                      const unordered_map<int, int> &pg_duckdb_table_idx);
	unique_ptr<LogicalOperator> DealWithQual(unique_ptr<LogicalGet> logical_get,
	                                         std::vector<unique_ptr<Expression>> &expr_vec,
	                                         const std::vector<unique_ptr<SimplestExpr>> &qual_vec,
	                                         const unordered_map<int, int> &pg_duckdb_table_idx);
	vector<unique_ptr<Expression>> GetFilter_exprs(std::vector<unique_ptr<Expression>> &expr_vec, idx_t table_idx);
	ExpressionType ConvertCompType(SimplestExprType type);
	LogicalType ConvertVarType(SimplestVarType type);
	OrderType ConvertOrderType(SimplestExprType type);
	void SetAttrName(unique_ptr<SimplestAttr> &attr, const std::deque<table_str> &table_col_names);
	void SetAttrVecName(std::vector<unique_ptr<SimplestAttr>> &attr_vec, const std::deque<table_str> &table_col_names);
	void SetExprName(unique_ptr<SimplestExpr> &expr, const std::deque<table_str> &table_col_names);
	void SetExprVecName(std::vector<unique_ptr<SimplestExpr>> &expr_vec, const std::deque<table_str> &table_col_names);
	void SetExprVecName(std::vector<unique_ptr<SimplestVarComparison>> &comp_vec,
	                    const std::deque<table_str> &table_col_names);

	// <duckdb_table_index, duckdb's column_ids>
	// in duckdb's column_ids, it convert table entry's id to binding id
	std::unordered_map<uint64_t, vector<column_t>> column_idx_mapping;

	Binder &binder;
	ClientContext &context;
};

} // namespace duckdb