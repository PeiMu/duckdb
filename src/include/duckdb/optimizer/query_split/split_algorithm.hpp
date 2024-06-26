//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/split_algorithm.hpp
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

enum EnumSplitAlgorithm { foreign_key_center = 1, min_sub_query, top_down };

using subquery_queue = std::deque<std::vector<unique_ptr<LogicalOperator>>>;
using table_expr_info = std::queue<std::vector<std::set<TableExpr>>>;

class SplitAlgorithm : public LogicalOperatorVisitor {
public:
	explicit SplitAlgorithm(ClientContext &context) : context(context) {};
	~SplitAlgorithm() override = default;
	//! Perform Query Split
	virtual unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan) {
		return std::move(plan);
	};

	virtual void MergeSubquery(unique_ptr<LogicalOperator> &plan, subquery_queue old_subqueries) {
	}

	virtual void UnMergeSubquery(unique_ptr<LogicalOperator> &plan) {
	}

	virtual unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> &plan, bool &needToSplit) {
		return nullptr;
	}

public:
	//! the collection of all levels of subqueries in a bottom-up order, e.g. the lowest level subquery is the first
	//! element in the queue and will be executed first
	subquery_queue subqueries;

protected:
	ClientContext &context;
	//! record the parent node to replace it to the valid child node
};

} // namespace duckdb