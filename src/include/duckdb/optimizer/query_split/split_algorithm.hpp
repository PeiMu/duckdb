//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/foreign_key_center.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <queue>

// debug
#include "duckdb/common/printer.hpp"

namespace duckdb {

struct TableExpr {
	idx_t table_idx;
	idx_t column_idx;
	std::string column_name;
	LogicalType return_type;

	bool operator==(const TableExpr &other) const {
		return table_idx == other.table_idx && column_idx == other.column_idx && column_name == other.column_name;
	}

	bool operator<(const TableExpr &other) const {
		return ((table_idx < other.table_idx) || (table_idx == other.table_idx && column_idx < other.column_idx));
	}
};

struct TableExprHash {
	size_t operator()(const TableExpr &table_expr) const {
		return std::hash<idx_t> {}(table_expr.table_idx) ^ std::hash<idx_t> {}(table_expr.column_idx) ^
		       std::hash<std::string> {}(table_expr.column_name);
	}
};

enum EnumSplitAlgorithm { foreign_key_center = 1, min_sub_query, top_down };

using subquery_queue = std::queue<std::vector<unique_ptr<LogicalOperator>>>;

class SplitAlgorithm : public LogicalOperatorVisitor {
public:
	explicit SplitAlgorithm(ClientContext &context) : context(context) {};
	~SplitAlgorithm() override = default;
	//! Perform Query Split
	virtual unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan) {
		return std::move(plan);
	};

public:
	//! the collection of all levels of subqueries in a bottom-up order, e.g. the lowest level subquery is the first
	//! element in the queue and will be executed first
	subquery_queue subqueries;

protected:
	ClientContext &context;
	//! record the parent node to replace it to the valid child node
};

} // namespace duckdb