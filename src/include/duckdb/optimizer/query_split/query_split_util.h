//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/timer_util.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/planner/column_binding.hpp"

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

const TableExpr GetTableExpr(const unique_ptr<Expression> &expr);
ColumnBinding &GetColumnBinding(unique_ptr<Expression> &expr);
} // namespace duckdb
