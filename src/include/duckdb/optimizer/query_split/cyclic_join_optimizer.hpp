//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split/cyclic_join_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/column_binding.hpp"
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

//! Represents an equality relationship between two columns
struct EqualityEdge {
	ColumnBinding left;
	ColumnBinding right;
	Expression *expression; // Pointer to the original expression for removal

	EqualityEdge(ColumnBinding left, ColumnBinding right, Expression *expr)
	    : left(left), right(right), expression(expr) {
	}

	bool operator==(const EqualityEdge &other) const {
		return (left == other.left && right == other.right) || (left == other.right && right == other.left);
	}

	struct Hash {
		std::size_t operator()(const EqualityEdge &edge) const {
			// Normalize ordering to ensure a=b and b=a have same hash
			// Compare bindings manually since ColumnBinding doesn't have operator<
			bool left_is_smaller = (edge.left.table_index < edge.right.table_index) ||
			                       (edge.left.table_index == edge.right.table_index &&
			                        edge.left.column_index < edge.right.column_index);
			auto min_binding = left_is_smaller ? edge.left : edge.right;
			auto max_binding = left_is_smaller ? edge.right : edge.left;
			return std::hash<idx_t>()(min_binding.table_index) ^
			       (std::hash<idx_t>()(min_binding.column_index) << 1) ^
			       (std::hash<idx_t>()(max_binding.table_index) << 2) ^
			       (std::hash<idx_t>()(max_binding.column_index) << 3);
		}
	};
};

//! Represents a cycle of join conditions
struct JoinCycle {
	vector<EqualityEdge> edges;

	JoinCycle() = default;
};

//! Helper class to detect and optimize cyclic join conditions
class CyclicJoinOptimizer {
public:
	CyclicJoinOptimizer() = default;

	//! Collect all equality join conditions from the plan
	void CollectJoinConditions(LogicalOperator &op);

	//! Detect cycles in the collected join conditions
	vector<JoinCycle> DetectCycles();

	//! Track which column pairs are guaranteed equal in a temp table
	//! based on which join edges were in the sub_plan
	void RecordTempTableEqualities(idx_t temp_table_idx, const LogicalOperator &sub_plan);

	//! Remove redundant filters from the plan that reference columns
	//! already guaranteed to be equal in temp tables
	bool RemoveRedundantFilters(LogicalOperator &op, const std::unordered_set<idx_t> &temp_table_indices);

	//! Get the equality guarantees for a specific temp table
	const vector<std::pair<idx_t, idx_t>> *GetTempTableEqualities(idx_t table_idx) const;

private:
	//! Helper to collect join conditions recursively
	void CollectJoinConditionsRecursive(LogicalOperator &op);

	//! Helper to detect cycles using DFS
	void DetectCyclesRecursive(idx_t current, idx_t start, vector<bool> &visited,
	                           vector<bool> &in_stack, vector<EqualityEdge> &path,
	                           vector<JoinCycle> &cycles);

	//! Helper to remove redundant filters recursively
	bool RemoveRedundantFiltersRecursive(LogicalOperator &op,
	                                     const std::unordered_set<idx_t> &temp_table_indices);

	//! Check if an expression is a redundant equality between columns from same temp table
	bool IsRedundantTempTableEquality(Expression &expr, const std::unordered_set<idx_t> &temp_table_indices);

	//! Extract column bindings from an expression
	void ExtractColumnBindings(Expression &expr, vector<ColumnBinding> &bindings);

private:
	//! All equality edges collected from the plan
	vector<EqualityEdge> all_edges;

	//! Map from table_index to list of other table_indices it's connected to
	std::unordered_map<idx_t, vector<idx_t>> adjacency_list;

	//! Map from temp_table_idx -> vector of (column_idx, column_idx) pairs that are guaranteed equal
	//! For example, if temp table contains columns from R and S where R.r = S.s,
	//! and R.r is at index 0 and S.s is at index 1, we store {temp_table_idx -> [(0, 1)]}
	std::unordered_map<idx_t, vector<std::pair<idx_t, idx_t>>> temp_table_equalities;

	//! Map from temp_table_idx -> set of original table indices it contains
	std::unordered_map<idx_t, std::unordered_set<idx_t>> temp_table_sources;
};

} // namespace duckdb
