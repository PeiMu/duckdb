#include "duckdb/optimizer/reorder_get.h"

namespace duckdb {

unique_ptr<LogicalOperator> ReorderGet::Optimize(unique_ptr<LogicalOperator> plan) {
	if (LogicalOperatorType::LOGICAL_PROJECTION != plan->type && LogicalOperatorType::LOGICAL_ORDER_BY != plan->type &&
	    LogicalOperatorType::LOGICAL_EXPLAIN != plan->type) {
		return std::move(plan);
	}

	// test
	// collect all tables
	std::map<std::pair<idx_t, idx_t>, JoinCondition> join_conds;
	std::map<idx_t, unique_ptr<LogicalOperator>> get_ops;
	// <table_index, card>
	std::deque<std::pair<idx_t, idx_t>> table_order;
	std::function<void(unique_ptr<LogicalOperator> & op)> collect_get_op;
	collect_get_op = [&collect_get_op, &join_conds, &get_ops, &table_order, this](unique_ptr<LogicalOperator> &op) {
		for (auto &child : op->children) {
			collect_get_op(child);
			if (LogicalOperatorType::LOGICAL_GET == child->type) {
				auto &get_op = child->Cast<LogicalGet>();
				auto temp_table_card = std::make_pair(get_op.table_index, get_op.EstimateCardinality(context));
				get_ops[get_op.table_index] = std::move(child);
				// sort the table index with card, from the biggest to the smallest
				for (size_t idx = 0; idx < table_order.size(); idx++) {
					if (table_order[idx].second < temp_table_card.second) {
						auto temp = table_order[idx];
						table_order[idx] = temp_table_card;
						temp_table_card = temp;
					}
				}
				table_order.push_back(temp_table_card);
			} else if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child->type) {
				auto &join_op = child->Cast<LogicalComparisonJoin>();
				for (auto &cond : join_op.conditions) {
					auto &left_table = cond.left;
#ifdef DEBUG
					D_ASSERT(left_table->type == ExpressionType::BOUND_COLUMN_REF);
#endif
					auto left_table_index = left_table->Cast<BoundColumnRefExpression>().binding.table_index;
					auto &right_table = cond.right;
#ifdef DEBUG
					D_ASSERT(right_table->type == ExpressionType::BOUND_COLUMN_REF);
#endif
					auto right_table_index = right_table->Cast<BoundColumnRefExpression>().binding.table_index;
					join_conds[std::make_pair(left_table_index, right_table_index)] = std::move(cond);
				}
			}
		}
	};
	collect_get_op(plan);

	auto plan_pointer = plan.get();
	// get the position before the first join
	while (!plan_pointer->children.empty() &&
	       LogicalOperatorType::LOGICAL_COMPARISON_JOIN != plan_pointer->children[0]->type) {
		plan_pointer = plan_pointer->children[0].get();
	}
	plan_pointer->children.clear();
	std::stack<vector<JoinCondition>> join_conditions_stack;
	std::stack<idx_t> joined_table_index;

	while (!table_order.empty()) {
		auto table_index = table_order.front().first;
		table_order.pop_front();
		vector<JoinCondition> join_conditions;
		for (auto it = join_conds.begin(); it != join_conds.end();) {
			bool find_in_left = false;
			if (it->first.first == table_index) {
#ifdef DEBUG
				D_ASSERT(it->second.comparison == ExpressionType::COMPARE_EQUAL);
#endif
				// swap the cond
				auto temp = std::move(it->second.left);
				it->second.left = std::move(it->second.right);
				it->second.right = std::move(temp);
				find_in_left = true;
			}
			if (find_in_left || it->first.second == table_index) {
				join_conditions.emplace_back(std::move(it->second));
				if (joined_table_index.empty() || (joined_table_index.top() != table_index))
					joined_table_index.push(table_index);
				it = join_conds.erase(it);
			} else {
				it++;
			}
		}
		if (!join_conditions.empty())
			join_conditions_stack.emplace(std::move(join_conditions));
		if (join_conds.empty())
			break;
	}
#ifdef DEBUG
	D_ASSERT(!table_order.empty());
#endif

	// get the current_plan or the rest of tables
	unique_ptr<LogicalOperator> current_plan = std::move(get_ops[table_order.back().first]);
	table_order.pop_back();
	while (!table_order.empty()) {
		current_plan =
		    LogicalCrossProduct::Create(std::move(current_plan), std::move(get_ops[table_order.back().first]));
		table_order.pop_back();
	}

	unique_ptr<LogicalOperator> tmp_comp_join;
	while (!join_conditions_stack.empty()) {
#ifdef DEBUG
		D_ASSERT(!joined_table_index.empty());
#endif
		tmp_comp_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		tmp_comp_join->children.push_back(std::move(current_plan));
		tmp_comp_join->children.push_back(std::move(get_ops[joined_table_index.top()]));
		joined_table_index.pop();
		tmp_comp_join->Cast<LogicalComparisonJoin>().conditions = std::move(join_conditions_stack.top());
		join_conditions_stack.pop();
		current_plan = std::move(tmp_comp_join);
	}

	plan_pointer->children.push_back(std::move(current_plan));

	return std::move(plan);
}
} // namespace duckdb