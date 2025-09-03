//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split_util.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <chrono>
#include <fstream>

namespace duckdb {
timespec tic();

void toc(timespec *start_time, const char *prefix);

std::chrono::high_resolution_clock::time_point chrono_tic();

long chrono_toc(std::chrono::high_resolution_clock::time_point *start_time, const char *prefix, bool print = true);

void appendLineToFile(string filepath, string line);

struct TableExpr {
	idx_t table_idx;
	idx_t column_idx;
	std::string column_name;
	LogicalType return_type;

	bool operator==(const TableExpr &other) const {
		return table_idx == other.table_idx && column_idx == other.column_idx;
	}

	bool operator<(const TableExpr &other) const {
		return ((table_idx < other.table_idx) || (table_idx == other.table_idx && column_idx < other.column_idx));
	}
};

struct TableExprHash {
	size_t operator()(const TableExpr &table_expr) const {
		return std::hash<idx_t> {}(table_expr.table_idx) ^ std::hash<idx_t> {}(table_expr.column_idx);
	}
};

//! Get the const of TableExpr (which has only one binding)
const TableExpr GetConstTableExpr(const unique_ptr<Expression> &expr);

//! Get the reference of TableExpr (which has only one binding)
ColumnBinding &GetRefColumnBinding(unique_ptr<Expression> &expr);

//! Update the expr (which more than one binding) by `func`
// fixme: see if we can refactor this by `VisitReplace`
template <typename T>
void UpdateFunctionExpr(BoundFunctionExpression &function_expr, T &&func);
template <typename T>
void UpdateCaseExpr(BoundCaseExpression &case_expr, T &&func);
template <typename T>
void UpdateComparisonExpr(BoundComparisonExpression &comparison_expr, T &&func);
template <typename T>
void UpdateExprs(unique_ptr<Expression> &expr, T &&func);

//! Visit the expr (which more than one binding) by `func`
// fixme: see if we can refactor this by `VisitReplace`
template <typename T>
void VisitFunctionExpr(const BoundFunctionExpression &function_expr, T &&func);
template <typename T>
void VisitCaseExpr(const BoundCaseExpression &case_expr, T &&func);
template <typename T>
void VisitComparisonExpr(const BoundComparisonExpression &comparison_expr, T &&func);
template <typename T>
void VisitExprs(const unique_ptr<Expression> &expr, T &&func);

} // namespace duckdb
