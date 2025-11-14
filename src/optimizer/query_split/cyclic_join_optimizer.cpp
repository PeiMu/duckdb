#include "duckdb/optimizer/query_split/cyclic_join_optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

void CyclicJoinOptimizer::CollectJoinConditions(LogicalOperator &op) {
	all_edges.clear();
	adjacency_list.clear();
	CollectJoinConditionsRecursive(op);
}

void CyclicJoinOptimizer::CollectJoinConditionsRecursive(LogicalOperator &op) {
	// Check if this is a comparison join
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			// Only handle equality conditions
			if (condition.comparison == ExpressionType::COMPARE_EQUAL) {
				// Extract column bindings from left and right
				if (condition.left->type == ExpressionType::BOUND_COLUMN_REF &&
				    condition.right->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &left_col = condition.left->Cast<BoundColumnRefExpression>();
					auto &right_col = condition.right->Cast<BoundColumnRefExpression>();

					EqualityEdge edge(left_col.binding, right_col.binding, condition.left.get());
					all_edges.push_back(edge);

					// Build adjacency list for cycle detection
					adjacency_list[left_col.binding.table_index].push_back(right_col.binding.table_index);
					adjacency_list[right_col.binding.table_index].push_back(left_col.binding.table_index);
				}
			}
		}
	}

	// Also check filters for equality conditions
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			if (expr->type == ExpressionType::COMPARE_EQUAL) {
				auto &comp = expr->Cast<BoundComparisonExpression>();
				if (comp.left->type == ExpressionType::BOUND_COLUMN_REF &&
				    comp.right->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &left_col = comp.left->Cast<BoundColumnRefExpression>();
					auto &right_col = comp.right->Cast<BoundColumnRefExpression>();

					EqualityEdge edge(left_col.binding, right_col.binding, expr.get());
					all_edges.push_back(edge);

					adjacency_list[left_col.binding.table_index].push_back(right_col.binding.table_index);
					adjacency_list[right_col.binding.table_index].push_back(left_col.binding.table_index);
				}
			}
		}
	}

	// Recursively process children
	for (auto &child : op.children) {
		if (child) {
			CollectJoinConditionsRecursive(*child);
		}
	}
}

vector<JoinCycle> CyclicJoinOptimizer::DetectCycles() {
	vector<JoinCycle> cycles;
	vector<bool> visited(adjacency_list.size(), false);
	vector<bool> in_stack(adjacency_list.size(), false);
	vector<EqualityEdge> path;

	// Try DFS from each node
	for (auto &entry : adjacency_list) {
		idx_t start = entry.first;
		if (!visited[start]) {
			DetectCyclesRecursive(start, start, visited, in_stack, path, cycles);
		}
	}

	return cycles;
}

void CyclicJoinOptimizer::DetectCyclesRecursive(idx_t current, idx_t start,
                                                 vector<bool> &visited, vector<bool> &in_stack,
                                                 vector<EqualityEdge> &path, vector<JoinCycle> &cycles) {
	visited[current] = true;
	in_stack[current] = true;

	if (adjacency_list.find(current) != adjacency_list.end()) {
		for (idx_t neighbor : adjacency_list[current]) {
			// Find the edge connecting current to neighbor
			EqualityEdge *connecting_edge = nullptr;
			for (auto &edge : all_edges) {
				if ((edge.left.table_index == current && edge.right.table_index == neighbor) ||
				    (edge.left.table_index == neighbor && edge.right.table_index == current)) {
					connecting_edge = &edge;
					break;
				}
			}

			if (connecting_edge) {
				path.push_back(*connecting_edge);

				if (neighbor == start && path.size() >= 3) {
					// Found a cycle!
					JoinCycle cycle;
					cycle.edges = path;
					cycles.push_back(cycle);
				} else if (!visited[neighbor]) {
					DetectCyclesRecursive(neighbor, start, visited, in_stack, path, cycles);
				}

				path.pop_back();
			}
		}
	}

	in_stack[current] = false;
}

void CyclicJoinOptimizer::RecordTempTableEqualities(idx_t temp_table_idx, const LogicalOperator &sub_plan) {
	// Collect all join conditions in the sub_plan
	vector<std::pair<ColumnBinding, ColumnBinding>> sub_plan_equalities;
	std::unordered_set<idx_t> source_tables;

	std::function<void(const LogicalOperator &)> collect_from_subplan;
	collect_from_subplan = [&](const LogicalOperator &op) {
		// Track source tables
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			source_tables.insert(get.table_index);
		}

		// Collect join conditions
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			for (auto &condition : join.conditions) {
				if (condition.comparison == ExpressionType::COMPARE_EQUAL) {
					if (condition.left->type == ExpressionType::BOUND_COLUMN_REF &&
					    condition.right->type == ExpressionType::BOUND_COLUMN_REF) {
						auto &left_col = condition.left->Cast<BoundColumnRefExpression>();
						auto &right_col = condition.right->Cast<BoundColumnRefExpression>();
						sub_plan_equalities.emplace_back(left_col.binding, right_col.binding);
					}
				}
			}
		}

		// Recurse
		for (auto &child : op.children) {
			if (child) {
				collect_from_subplan(*child);
			}
		}
	};

	collect_from_subplan(sub_plan);

	// Now we need to map original column bindings to positions in the temp table
	// The temp table will have columns in the order they appear in the projection
	// For now, we'll store the equality relationships
	// This is a simplified version - in practice you'd need to track the exact column mapping

	vector<std::pair<idx_t, idx_t>> equality_pairs;
	for (auto &eq : sub_plan_equalities) {
		// Store as pairs - in a full implementation, you'd map these to actual column indices
		// in the temp table based on the projection
		equality_pairs.emplace_back(eq.first.column_index, eq.second.column_index);
	}

	if (!equality_pairs.empty()) {
		temp_table_equalities[temp_table_idx] = equality_pairs;
		temp_table_sources[temp_table_idx] = source_tables;
	}
}

bool CyclicJoinOptimizer::RemoveRedundantFilters(LogicalOperator &op,
                                                  const std::unordered_set<idx_t> &temp_table_indices) {
	return RemoveRedundantFiltersRecursive(op, temp_table_indices);
}

bool CyclicJoinOptimizer::RemoveRedundantFiltersRecursive(LogicalOperator &op,
                                                           const std::unordered_set<idx_t> &temp_table_indices) {
	bool modified = false;

	// Check if this is a filter operator
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		vector<unique_ptr<Expression>> new_expressions;

		for (auto &expr : filter.expressions) {
			// Check if this is a redundant equality
			if (!IsRedundantTempTableEquality(*expr, temp_table_indices)) {
				new_expressions.push_back(std::move(expr));
			} else {
				modified = true;
			}
		}

		filter.expressions = std::move(new_expressions);
	}

	// Also check join conditions
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		vector<JoinCondition> new_conditions;

		for (auto &condition : join.conditions) {
			// Check if both sides reference columns from the same temp table
			if (condition.comparison == ExpressionType::COMPARE_EQUAL &&
			    condition.left->type == ExpressionType::BOUND_COLUMN_REF &&
			    condition.right->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &left_col = condition.left->Cast<BoundColumnRefExpression>();
				auto &right_col = condition.right->Cast<BoundColumnRefExpression>();

				// Check if both columns are from the same temp table and guaranteed equal
				if (left_col.binding.table_index == right_col.binding.table_index &&
				    temp_table_indices.find(left_col.binding.table_index) != temp_table_indices.end()) {
					// Check if this equality is already guaranteed
					auto equalities = GetTempTableEqualities(left_col.binding.table_index);
					if (equalities) {
						bool is_redundant = false;
						for (auto &eq_pair : *equalities) {
							if ((eq_pair.first == left_col.binding.column_index &&
							     eq_pair.second == right_col.binding.column_index) ||
							    (eq_pair.first == right_col.binding.column_index &&
							     eq_pair.second == left_col.binding.column_index)) {
								is_redundant = true;
								break;
							}
						}
						if (!is_redundant) {
							new_conditions.push_back(std::move(condition));
						} else {
							modified = true;
						}
					} else {
						new_conditions.push_back(std::move(condition));
					}
				} else {
					new_conditions.push_back(std::move(condition));
				}
			} else {
				new_conditions.push_back(std::move(condition));
			}
		}

		join.conditions = std::move(new_conditions);
	}

	// Recursively process children
	for (auto &child : op.children) {
		if (child) {
			modified |= RemoveRedundantFiltersRecursive(*child, temp_table_indices);
		}
	}

	return modified;
}

bool CyclicJoinOptimizer::IsRedundantTempTableEquality(Expression &expr,
                                                        const std::unordered_set<idx_t> &temp_table_indices) {
	// Check if this is an equality comparison
	if (expr.type != ExpressionType::COMPARE_EQUAL) {
		return false;
	}

	auto &comp = expr.Cast<BoundComparisonExpression>();
	if (comp.left->type != ExpressionType::BOUND_COLUMN_REF ||
	    comp.right->type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}

	auto &left_col = comp.left->Cast<BoundColumnRefExpression>();
	auto &right_col = comp.right->Cast<BoundColumnRefExpression>();

	// Check if both columns are from the same temp table
	if (left_col.binding.table_index != right_col.binding.table_index) {
		return false;
	}

	idx_t table_idx = left_col.binding.table_index;
	if (temp_table_indices.find(table_idx) == temp_table_indices.end()) {
		return false;
	}

	// Check if this equality is guaranteed by the temp table
	auto equalities = GetTempTableEqualities(table_idx);
	if (!equalities) {
		return false;
	}

	for (auto &eq_pair : *equalities) {
		if ((eq_pair.first == left_col.binding.column_index &&
		     eq_pair.second == right_col.binding.column_index) ||
		    (eq_pair.first == right_col.binding.column_index &&
		     eq_pair.second == left_col.binding.column_index)) {
			return true;
		}
	}

	return false;
}

void CyclicJoinOptimizer::ExtractColumnBindings(Expression &expr, vector<ColumnBinding> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		bindings.push_back(col_ref.binding);
	}

	// Recursively check children
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		ExtractColumnBindings(child, bindings);
	});
}

const vector<std::pair<idx_t, idx_t>> *CyclicJoinOptimizer::GetTempTableEqualities(idx_t table_idx) const {
	auto it = temp_table_equalities.find(table_idx);
	if (it != temp_table_equalities.end()) {
		return &it->second;
	}
	return nullptr;
}

} // namespace duckdb
