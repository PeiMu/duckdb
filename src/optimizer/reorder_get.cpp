#include "duckdb/optimizer/reorder_get.h"

namespace duckdb {

unique_ptr<LogicalOperator> ReorderGet::Optimize(unique_ptr<LogicalOperator> plan) {
	if (LogicalOperatorType::LOGICAL_PROJECTION != plan->type && LogicalOperatorType::LOGICAL_ORDER_BY != plan->type &&
	    LogicalOperatorType::LOGICAL_LIMIT != plan->type && LogicalOperatorType::LOGICAL_EXPLAIN != plan->type) {
		return std::move(plan);
	}
#if ENABLE_DEBUG_PRINT
	Printer::Print("before ReorderGet");
	plan->Print();
#endif

	// collect all tables
	std::map<std::pair<idx_t, idx_t>, std::vector<JoinCondition>> join_conds;
	std::map<idx_t, unique_ptr<LogicalOperator>> table_index_blocks;
	std::stack<unique_ptr<LogicalOperator>> filter_nodes;
	// <table_index, card>
	std::deque<std::pair<idx_t, idx_t>> table_card_order;
	std::function<void(unique_ptr<LogicalOperator> & op)> collect_block;
	collect_block = [&collect_block, &join_conds, &table_index_blocks, &table_card_order, &filter_nodes,
	                 this](unique_ptr<LogicalOperator> &op) {
		for (auto &child : op->children) {
			if (LogicalOperatorType::LOGICAL_GET == child->type) {
				auto &get_op = child->Cast<LogicalGet>();
				idx_t estimated_card = get_op.EstimateCardinality(context);
				auto temp_table_card = std::make_pair(get_op.table_index, estimated_card);
				table_index_blocks[get_op.table_index] = std::move(child);
				// sort the table index with card, from the biggest to the smallest
				for (size_t idx = 0; idx < table_card_order.size(); idx++) {
					if (table_card_order[idx].second < temp_table_card.second) {
						auto temp = table_card_order[idx];
						table_card_order[idx] = temp_table_card;
						temp_table_card = temp;
					}
				}
				table_card_order.push_back(temp_table_card);
				continue;
#if REORDER_DATACHUNK
			} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == child->type) {
				auto &chunk_get_op = child->Cast<LogicalColumnDataGet>();
				auto temp_table_card =
				    std::make_pair(chunk_get_op.table_index, chunk_get_op.EstimateCardinality(context));
				table_index_blocks[chunk_get_op.table_index] = std::move(child);
				// sort the table index with card, from the biggest to the smallest
				for (size_t idx = 0; idx < table_card_order.size(); idx++) {
					if (table_card_order[idx].second < temp_table_card.second) {
						auto temp = table_card_order[idx];
						table_card_order[idx] = temp_table_card;
						temp_table_card = temp;
					}
				}
				table_card_order.push_back(temp_table_card);
				continue;
#endif
			} else if (LogicalOperatorType::LOGICAL_FILTER == child->type) {
				// PS: we consider the FILTER+JOIN+[SCAN+CHUNK_GET] block as a whole
				std::function<void(unique_ptr<LogicalOperator> & op)> collect_filter;
				std::pair<idx_t, idx_t> temp_table_card;
				int table_index = -1;
				bool more_tables = false;
				collect_filter = [&collect_filter, &table_index, &temp_table_card, &more_tables, &filter_nodes,
				                  this](unique_ptr<LogicalOperator> &op) {
					for (auto &child : op->children) {
						if (-1 != table_index && !in_clause) {
							more_tables = true;
							break;
						}

						switch (child->type) {
						case LogicalOperatorType::LOGICAL_GET: {
							auto &get_op = child->Cast<LogicalGet>();
							temp_table_card = std::make_pair(get_op.table_index, get_op.EstimateCardinality(context));
							table_index = get_op.table_index;
							break;
						}
#if REORDER_DATACHUNK
						case LogicalOperatorType::LOGICAL_CHUNK_GET: {
							auto &chunk_get_op = child->Cast<LogicalColumnDataGet>();
							if (in_clause) {
								// todo: estimate the cardinality of IN clause
								in_clause = false;
							} else {
								temp_table_card =
								    std::make_pair(chunk_get_op.table_index, chunk_get_op.EstimateCardinality(context));
								table_index = chunk_get_op.table_index;
							}
							break;
						}
#endif
						case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
							auto &join_op = child->Cast<LogicalComparisonJoin>();
							if (JoinType::MARK == join_op.join_type || JoinType::SEMI == join_op.join_type) {
								// todo: estimate the cardinality of IN clause, after modifying STATISTICS_PROPAGATION
								in_clause = true;
							}
							collect_filter(child);
							// if it's an IN clause
							if (JoinType::MARK == join_op.join_type || JoinType::SEMI == join_op.join_type) {
								// todo: estimate the cardinality of IN clause, after modifying STATISTICS_PROPAGATION
							}
							break;
						}
						case LogicalOperatorType::LOGICAL_FILTER:
						case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
							collect_filter(child);
							break;
						case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
							// skip
							break;
						default:
							Printer::Print("Doesn't support " + LogicalOperatorToString(child->type) +
							               " in ReorderGet Opt yet!");
							D_ASSERT(false);
						}
					}
				};
				collect_filter(child);
				if (-1 == table_index) {
					// fixme: need to refactor
					// have operators like aggregate, projection, etc.
					collect_block(child);
					continue;
				}
				if (more_tables) {
					collect_block(child);
					filter_nodes.push(std::move(child));
					continue;
				}

				if (in_clause) {
					// todo: estimate the cardinality of IN clause
				}
				// sort the table index with card, from the biggest to the smallest
				for (size_t idx = 0; idx < table_card_order.size(); idx++) {
					if (table_card_order[idx].second < temp_table_card.second) {
						auto temp = table_card_order[idx];
						table_card_order[idx] = temp_table_card;
						temp_table_card = temp;
					}
				}
				table_card_order.push_back(temp_table_card);
				table_index_blocks[table_index] = std::move(child);
				continue;
			} else if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child->type) {
				auto &join_op = child->Cast<LogicalComparisonJoin>();
				for (auto &cond : join_op.conditions) {
					auto left_table_index = GetConstTableExpr(cond.left).table_idx;
					auto right_table_index = GetConstTableExpr(cond.right).table_idx;
					join_conds[std::make_pair(left_table_index, right_table_index)].emplace_back(std::move(cond));
				}
			}
			collect_block(child);
		}
	};
	collect_block(plan);

#if ENABLE_DEBUG_PRINT
	Printer::Print("table_card_order");
	for (const auto &ele : table_card_order) {
		Printer::Print("table " + std::to_string(ele.first) + " with card = " + std::to_string(ele.second));
	}
#endif
	table_card_order_bak = table_card_order;

	auto plan_pointer = plan.get();
	// get the position before the first join
	while (!plan_pointer->children.empty() && nullptr != plan_pointer->children[0] &&
	       LogicalOperatorType::LOGICAL_COMPARISON_JOIN != plan_pointer->children[0]->type) {
		plan_pointer = plan_pointer->children[0].get();
	}
	plan_pointer->children.clear();

	// insert the filter nodes
	while (!filter_nodes.empty()) {
		auto &filter = filter_nodes.top();
		filter->children.clear();
		plan_pointer->children.emplace_back(std::move(filter));
		plan_pointer = plan_pointer->children[0].get();
		filter_nodes.pop();
		need_filter_push_down = true;
	}

	std::stack<vector<JoinCondition>> join_conditions_stack;
	std::stack<idx_t> joined_table_index;

	std::deque<std::pair<idx_t, idx_t>> unused_table_card_order;
	while (!table_card_order.empty()) {
		auto table_index = table_card_order.front().first;
		vector<JoinCondition> join_conditions;
		bool used = false;
		for (auto it = join_conds.begin(); it != join_conds.end();) {
			bool find_in_left = false;
			if (it->first.first == table_index) {
				for (auto &cond : it->second) {
					// swap the cond
					auto temp = std::move(cond.left);
					cond.left = std::move(cond.right);
					cond.right = std::move(temp);
					// change the comparison symbol if necessary
					switch (cond.comparison) {
					case ExpressionType::COMPARE_EQUAL:
						break;
					case ExpressionType::COMPARE_NOTEQUAL:
						break;
					case ExpressionType::COMPARE_LESSTHAN:
						cond.comparison = ExpressionType::COMPARE_GREATERTHAN;
						break;
					case ExpressionType::COMPARE_GREATERTHAN:
						cond.comparison = ExpressionType::COMPARE_LESSTHAN;
						break;
					case ExpressionType::COMPARE_LESSTHANOREQUALTO:
						cond.comparison = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
						break;
					case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
						cond.comparison = ExpressionType::COMPARE_LESSTHANOREQUALTO;
						break;
					default:
						Printer::Print("Doesn't support " + ExpressionTypeToString(cond.comparison) +
						               " in ReorderGet Opt yet!");
						D_ASSERT(false);
					}
				}
				find_in_left = true;
			}
			if (find_in_left || it->first.second == table_index) {
				std::move(it->second.begin(), it->second.end(), std::back_inserter(join_conditions));
				if (joined_table_index.empty() || (joined_table_index.top() != table_index)) {
					used = true;
					joined_table_index.push(table_index);
				}
				it = join_conds.erase(it);
			} else {
				it++;
			}
		}
		if (!used)
			unused_table_card_order.push_back(table_card_order.front());
		table_card_order.pop_front();
		if (!join_conditions.empty())
			join_conditions_stack.emplace(std::move(join_conditions));
		if (join_conds.empty())
			break;
	}

	while (!table_card_order.empty()) {
		unused_table_card_order.push_back(table_card_order.front());
		table_card_order.pop_front();
	}
	// get the current_plan or the rest of tables, in a bottom up order
	unique_ptr<LogicalOperator> current_plan = std::move(table_index_blocks[unused_table_card_order.back().first]);
	unused_table_card_order.pop_back();
	while (!unused_table_card_order.empty()) {
		current_plan = LogicalCrossProduct::Create(std::move(current_plan),
		                                           std::move(table_index_blocks[unused_table_card_order.back().first]));
		unused_table_card_order.pop_back();
	}

	unique_ptr<LogicalOperator> tmp_comp_join;
	while (!join_conditions_stack.empty()) {
#ifdef DEBUG
		D_ASSERT(!joined_table_index.empty());
#endif
		tmp_comp_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		tmp_comp_join->children.push_back(std::move(current_plan));
		tmp_comp_join->children.push_back(std::move(table_index_blocks[joined_table_index.top()]));
		joined_table_index.pop();
		tmp_comp_join->Cast<LogicalComparisonJoin>().conditions = std::move(join_conditions_stack.top());
		join_conditions_stack.pop();
		current_plan = std::move(tmp_comp_join);
	}

	plan_pointer->children.push_back(std::move(current_plan));

#if ENABLE_DEBUG_PRINT
	Printer::Print("after ReorderGet");
	plan->Print();
#endif

	return std::move(plan);
}
} // namespace duckdb
