#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan) {
	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	unique_ptr<LogicalOperator> subquery;
	GetTargetTables(*plan);
	VisitOperator(*plan);
	return std::move(plan);
}

void TopDownSplit::VisitOperator(LogicalOperator &op) {
	std::vector<unique_ptr<LogicalOperator>> same_level_subqueries;
	std::vector<std::set<TableExpr>> same_level_table_exprs;

	// todo: fix this when supporting parallel execution
	// Since we don't split at CROSS_PRODUCT, we don't split its sibling
	bool cross_product_sibling = false;

	// todo: fix this when supporting parallel execution
	// Since we currently don't support parallel execution, we have the issue
	// e.g. comp_join --------
	//          |            |
	//      chunk_get      filter
	// for now we just ignore the sibling of chunk_get
	bool chunk_get_sibling = false;

	// For now, we only check logical_filter and logical_comparison_join.
	// Basically, the split point is based on logical_comparison_join,
	// but if it has a logical_filter parent, then we split at the
	// logical_filter node.
	for (auto &child : op.children) {
		std::set<TableExpr> table_exprs;
		if (cross_product_sibling || chunk_get_sibling)
			break;
		switch (child->type) {
		// if the other child node is not CROSS_PRODUCT, JOIN nor FILTER
		case LogicalOperatorType::LOGICAL_FILTER:
			// add filter's column usage
			table_exprs = GetFilterTableExpr(child->Cast<LogicalFilter>());
			// check continuous filter nodes, only split the first one
			if (!filter_parent) {
				query_split_index++;
				child->split_index = query_split_index;
				// inherit from the children until it is not a filter
				auto child_pointer = child.get();
				while (LogicalOperatorType::LOGICAL_FILTER == child_pointer->type) {
					auto child_exprs = GetFilterTableExpr(child_pointer->Cast<LogicalFilter>());
					table_exprs.insert(child_exprs.begin(), child_exprs.end());
					child_pointer = child_pointer->children[0].get();
				}
				if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child_pointer->type) {
					auto child_exprs = GetJoinTableExpr(child_pointer->Cast<LogicalComparisonJoin>());
					table_exprs.insert(child_exprs.begin(), child_exprs.end());
				}
			}
			filter_parent = true;
			break;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			// if comp_join is the child of filter, we split at the filter node,
			// and inherit the table_exprs by the filter node
			if (!filter_parent) {
				query_split_index++;
				child->split_index = query_split_index;
				table_exprs = GetJoinTableExpr(child->Cast<LogicalComparisonJoin>());
			}
			filter_parent = false;
			break;
		default:
			if (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == child->type)
				cross_product_sibling = true;
			else
				cross_product_sibling = false;
			if (LogicalOperatorType::LOGICAL_CHUNK_GET == child->type)
				chunk_get_sibling = true;
			else
				chunk_get_sibling = false;
			child->split_index = 0;
			filter_parent = false;
			break;
		}
		VisitOperator(*child);

		if (child->split_index) {
			same_level_subqueries.emplace_back(std::move(child));
		}
		if (!table_exprs.empty()) {
			same_level_table_exprs.emplace_back(table_exprs);
		}
	}

#ifdef DEBUG
	D_ASSERT(same_level_subqueries.size() <= 2);
#endif
	if (!same_level_subqueries.empty()) {
		subqueries.emplace_back(std::move(same_level_subqueries));
	}
#ifdef DEBUG
	D_ASSERT(same_level_table_exprs.size() <= 2);
#endif
	if (!same_level_table_exprs.empty()) {
		table_expr_queue.emplace(same_level_table_exprs);
	}

	// collect table_expr_queue from projection node
	if (LogicalOperatorType::LOGICAL_PROJECTION == op.type) {
		GetProjTableExpr(op.Cast<LogicalProjection>());
	}
}

void TopDownSplit::GetTargetTables(LogicalOperator &op) {
	if (LogicalOperatorType::LOGICAL_GET == op.type) {
		auto &get_op = op.Cast<LogicalGet>();
		auto current_table_index = get_op.table_index;
		target_tables.emplace(current_table_index);
	} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == op.type) {
		auto &chunk_op = op.Cast<LogicalColumnDataGet>();
		auto current_table_index = chunk_op.table_index;
		target_tables.emplace(current_table_index);
	}
	for (auto &child : op.children) {
		GetTargetTables(*child);
	}
}

std::set<TableExpr> TopDownSplit::GetJoinTableExpr(const LogicalComparisonJoin &join_op) {
	std::set<TableExpr> table_exprs;
	for (const auto &cond : join_op.conditions) {
#ifdef DEBUG
		D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
#endif
		TableExpr current_left_table;
		auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
		current_left_table.table_idx = left_expr.binding.table_index;
		// it's the temporary id, but not physical id, e.g. by get.column_ids
		current_left_table.column_idx = left_expr.binding.column_index;
		current_left_table.column_name = left_expr.alias;
		current_left_table.return_type = left_expr.return_type;
		if (target_tables.count(current_left_table.table_idx))
			table_exprs.emplace(current_left_table);

		TableExpr current_right_table;
		auto &right_expr = cond.right->Cast<BoundColumnRefExpression>();
		current_right_table.table_idx = right_expr.binding.table_index;
		current_right_table.column_idx = right_expr.binding.column_index;
		current_right_table.column_name = right_expr.alias;
		current_right_table.return_type = right_expr.return_type;
		if (target_tables.count(current_right_table.table_idx))
			table_exprs.emplace(current_right_table);
	}
	return table_exprs;
}

std::set<TableExpr> TopDownSplit::GetCrossProductTableExpr(const duckdb::LogicalCrossProduct &product_op) {
	std::set<TableExpr> table_exprs;
	TableExpr cross_product_table_expr;
	// cross_product_table_expr.cross_product = true;
	table_exprs.emplace(cross_product_table_expr);
	return table_exprs;
}

std::set<TableExpr> TopDownSplit::GetSeqScanTableExpr(const LogicalGet &get_op) {
	std::set<TableExpr> table_exprs;
	for (const auto &table_filter : get_op.table_filters.filters) {
		TableExpr table_filter_expr;
		table_filter_expr.table_idx = get_op.table_index;
		auto column_idx_it = std::find(get_op.column_ids.begin(), get_op.column_ids.end(), table_filter.first);
#ifdef DEBUG
		D_ASSERT(column_idx_it != get_op.column_ids.end());
#endif
		table_filter_expr.column_idx = column_idx_it - get_op.column_ids.begin();
		table_filter_expr.column_name = get_op.names[table_filter.first];
		table_filter_expr.return_type = get_op.returned_types[table_filter.first];
		table_exprs.emplace(table_filter_expr);
	}

	return table_exprs;
}

std::set<TableExpr> TopDownSplit::GetFilterTableExpr(const LogicalFilter &filter_op) {
	std::set<TableExpr> table_exprs;

	auto get_column_ref_expr = [&table_exprs, this](const BoundColumnRefExpression &column_ref_expr) {
		TableExpr table_expr;
		//		auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
		table_expr.table_idx = column_ref_expr.binding.table_index;
		table_expr.column_idx = column_ref_expr.binding.column_index;
		table_expr.column_name = column_ref_expr.alias;
		table_expr.return_type = column_ref_expr.return_type;
		if (target_tables.count(table_expr.table_idx)) {
			table_exprs.emplace(table_expr);
		}
	};

	auto get_function_expr = [&table_exprs, this, get_column_ref_expr](const BoundFunctionExpression &function_expr) {
		for (const auto &func_child : function_expr.children) {
			if (ExpressionType::BOUND_COLUMN_REF == func_child->type) {
				get_column_ref_expr(func_child->Cast<BoundColumnRefExpression>());
			} else if (ExpressionType::VALUE_CONSTANT == func_child->type) {
				// it's a constant value, skip it
			} else {
				Printer::Print(StringUtil::Format("Do not support yet, func_child->type:  %s",
				                                  ExpressionTypeToString(func_child->type)));
				D_ASSERT(false);
			}
		}
	};

	auto get_comparison_expr = [&table_exprs, this, get_column_ref_expr,
	                            get_function_expr](const BoundComparisonExpression &comparison_expr) {
		auto &left_expr = comparison_expr.left;
		if (ExpressionType::BOUND_COLUMN_REF == left_expr->type) {
			get_column_ref_expr(left_expr->Cast<BoundColumnRefExpression>());
		} else if (ExpressionType::BOUND_FUNCTION == left_expr->type) {
			get_function_expr(left_expr->Cast<BoundFunctionExpression>());
		} else if (ExpressionType::VALUE_CONSTANT == left_expr->type) {
			// it's a constant value, skip it
		} else {
			Printer::Print(StringUtil::Format("Do not support yet, left_expr->type:  %s",
			                                  ExpressionTypeToString(left_expr->type)));
			D_ASSERT(false);
		}

		auto &right_expr = comparison_expr.right;
		if (ExpressionType::BOUND_COLUMN_REF == right_expr->type) {
			get_column_ref_expr(right_expr->Cast<BoundColumnRefExpression>());
		} else if (ExpressionType::BOUND_FUNCTION == left_expr->type) {
			get_function_expr(left_expr->Cast<BoundFunctionExpression>());
		} else if (ExpressionType::VALUE_CONSTANT == right_expr->type) {
			// it's a constant value, skip it
		} else {
			Printer::Print(StringUtil::Format("Do not support yet, right_expr->type:  %s",
			                                  ExpressionTypeToString(right_expr->type)));
			D_ASSERT(false);
		}
	};

	std::function<void(const unique_ptr<Expression> &expr)> get_expr;
	get_expr = [&table_exprs, this, get_column_ref_expr, get_function_expr, get_comparison_expr,
	            &get_expr](const unique_ptr<Expression> &expr) {
		if (ExpressionType::BOUND_COLUMN_REF == expr->type) {
			get_column_ref_expr(expr->Cast<BoundColumnRefExpression>());
		} else if (ExpressionType::BOUND_FUNCTION == expr->type) {
			get_function_expr(expr->Cast<BoundFunctionExpression>());
		} else if (ExpressionType::COMPARE_NOTEQUAL == expr->type || ExpressionType::COMPARE_EQUAL == expr->type ||
		           ExpressionType::COMPARE_GREATERTHAN == expr->type ||
		           ExpressionType::COMPARE_LESSTHAN == expr->type ||
		           ExpressionType::COMPARE_GREATERTHANOREQUALTO == expr->type ||
		           ExpressionType::COMPARE_LESSTHANOREQUALTO == expr->type) {
			get_comparison_expr(expr->Cast<BoundComparisonExpression>());
		} else if (ExpressionType::CONJUNCTION_OR == expr->type || ExpressionType::CONJUNCTION_AND == expr->type) {
			auto &conjunction_expr = expr->Cast<BoundConjunctionExpression>();
			for (const auto &child_expr : conjunction_expr.children) {
				get_expr(child_expr);
			}
		} else if (ExpressionType::OPERATOR_IS_NULL == expr->type ||
		           ExpressionType::OPERATOR_IS_NOT_NULL == expr->type || ExpressionType::OPERATOR_NOT == expr->type) {
			auto &operator_expr = expr->Cast<BoundOperatorExpression>();
			for (const auto &child_expr : operator_expr.children) {
				get_expr(child_expr);
			}
		} else if (ExpressionType::VALUE_CONSTANT == expr->type) {
			// it's a constant value, skip it
		} else {
			Printer::Print(
			    StringUtil::Format("Do not support yet, expr->type:  %s", ExpressionTypeToString(expr->type)));
			D_ASSERT(false);
		}
	};

	for (const auto &expr : filter_op.expressions) {
		get_expr(expr);
	}
	return table_exprs;
}

void TopDownSplit::GetProjTableExpr(const LogicalProjection &proj_op) {
	// if it's children is `aggregate` or `group by`, we only check the child op
	if (LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY == proj_op.children[0]->type) {
		GetAggregateTableExpr(proj_op.children[0]->Cast<LogicalAggregate>());
	} else {
		for (const auto &expr : proj_op.expressions) {
#ifdef DEBUG
			D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
#endif
			TableExpr table_expr;
			auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
			table_expr.table_idx = column_ref_expr.binding.table_index;
			table_expr.column_idx = column_ref_expr.binding.column_index;
			table_expr.column_name = column_ref_expr.alias;
			table_expr.return_type = column_ref_expr.return_type;
			if (target_tables.count(table_expr.table_idx)) {
				proj_expr.emplace_back(table_expr);
			}
		}
	}
}

void TopDownSplit::GetAggregateTableExpr(const LogicalAggregate &aggregate_op) {
	for (const auto &agg_expr : aggregate_op.expressions) {
#ifdef DEBUG
		D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
#endif
		auto &aggregate_expr = agg_expr->Cast<BoundAggregateExpression>();
		for (const auto &expr : aggregate_expr.children) {
			TableExpr table_expr;
#ifdef DEBUG
			D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
#endif
			auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
			table_expr.table_idx = column_ref_expr.binding.table_index;
			table_expr.column_idx = column_ref_expr.binding.column_index;
			table_expr.column_name = column_ref_expr.alias;
			table_expr.return_type = column_ref_expr.return_type;
			if (target_tables.count(table_expr.table_idx)) {
				proj_expr.emplace_back(table_expr);
			}
		}
	}
}

void TopDownSplit::MergeSubquery(unique_ptr<LogicalOperator> &plan, subquery_queue old_subqueries) {
	// get the position to reorder
	auto new_plan = plan.get();
	while (true) {
		if (nullptr == new_plan->children[0]) {
			auto old_subquery_pair = std::move(old_subqueries.back());
			new_plan->children[0] = std::move(old_subquery_pair[0]);
			if (2 == old_subquery_pair.size()) {
#ifdef DEBUG
				D_ASSERT(nullptr == new_plan->children[1]);
#endif
				new_plan->children[1] = std::move(old_subquery_pair[1]);
			}
			old_subqueries.pop_back();
		}
		new_plan = new_plan->children[0].get();
		if (old_subqueries.empty())
			break;
	}
}

void TopDownSplit::UnMergeSubquery(unique_ptr<LogicalOperator> &plan) {
	std::function<void(unique_ptr<LogicalOperator> & op)> unMerge;
	subqueries.clear();
	unMerge = [&unMerge, this](unique_ptr<LogicalOperator> &op) {
		std::vector<unique_ptr<LogicalOperator>> same_level_subqueries;
		// todo: fix this when supporting parallel execution (see the `cross_product_sibling` issue in
		// `TopDownSplit::VisitOperator`)
		bool cross_product_sibling = false;
		// todo: fix this when supporting parallel execution (see the `chunk_get_sibling` issue in
		// `TopDownSplit::VisitOperator`)
		bool chunk_get_sibling = false;
		for (auto &child : op->children) {
			if (cross_product_sibling || chunk_get_sibling)
				break;
			if (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == child->type)
				cross_product_sibling = true;
			else
				cross_product_sibling = false;
			if (LogicalOperatorType::LOGICAL_CHUNK_GET == child->type)
				chunk_get_sibling = true;
			else
				chunk_get_sibling = false;
			unMerge(child);
			if (child->split_index) {
				same_level_subqueries.emplace_back(std::move(child));
			}
		}
#ifdef DEBUG
		D_ASSERT(same_level_subqueries.size() <= 2);
#endif
		if (!same_level_subqueries.empty()) {
			subqueries.emplace_back(std::move(same_level_subqueries));
		}
	};

	unMerge(plan);
}

bool TopDownSplit::Rewrite(unique_ptr<LogicalOperator> &plan) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		break;
	default:
		return false;
	}

	// 1. collect the condition of the last join
	auto op_child = plan.get();
	LogicalOperator *last_join_pointer;
	int op_level = 0;
	int last_join_level = 0;
	int last_cross_product_level = 0;
	while (!op_child->children.empty()) {
		if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == op_child->type) {
			last_join_pointer = op_child;
			last_join_level = op_level;
		} else if (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == op_child->type) {
			last_cross_product_level = op_level;
		}
		op_child = op_child->children[0].get();
		op_level++;
	}
	if (last_cross_product_level < last_join_level) {
		return false;
	}

	auto &last_join = last_join_pointer->Cast<LogicalComparisonJoin>();

	// select the needed tables
	std::unordered_set<idx_t> left_cond_table_index;
	for (const auto &cond : last_join.conditions) {
#ifdef DEBUG
		D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
#endif
		auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
		left_cond_table_index.emplace(left_expr.binding.table_index);
	}

	// 2. collect all tables below the last JOIN in the top-down order
	std::unordered_map<idx_t, unique_ptr<LogicalOperator>> table_blocks;
	std::deque<idx_t> table_blocks_key_order;

	auto last_cross_product = last_join_pointer;
	while (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == last_cross_product->children[0]->type) {
		last_cross_product = last_cross_product->children[0].get();
		auto &cross_product_op = last_cross_product->Cast<LogicalCrossProduct>();
		InsertTableBlocks(cross_product_op.children[1], table_blocks, table_blocks_key_order);
	}
	auto &last_block = last_cross_product->Cast<LogicalCrossProduct>().children[0];

	// if the last block is unused in the last join, swap it with its sibling,
	// aka the last element in the table_blocks_key_order
	bool used = BlockUsed(left_cond_table_index, last_block);
	if (!used) {
		auto last_sibling_index = table_blocks_key_order.back();
		table_blocks_key_order.pop_back();
#ifdef DEBUG
		D_ASSERT(left_cond_table_index.count(last_sibling_index));
#endif
		auto last_sibling = std::move(table_blocks[last_sibling_index]);
		table_blocks.erase(last_sibling_index);
		InsertTableBlocks(last_block, table_blocks, table_blocks_key_order);
		last_block = std::move(last_sibling);
	}

	std::queue<unique_ptr<LogicalOperator>> unused_blocks;
	for (auto &block : table_blocks) {
		if (!left_cond_table_index.count(block.first)) {
			unused_blocks.push(std::move(block.second));
		}
	}

	// if all tables below the last join are used
	if (unused_blocks.empty()) {
		// revert table blocks to plan
		auto revert_pointer = last_join_pointer;
		while (!table_blocks_key_order.empty() &&
		       LogicalOperatorType::LOGICAL_CROSS_PRODUCT == revert_pointer->children[0]->type) {
			revert_pointer = revert_pointer->children[0].get();
			auto &revert_op = revert_pointer->Cast<LogicalCrossProduct>();
			revert_op.children[1] = std::move(table_blocks[table_blocks_key_order.front()]);
			table_blocks_key_order.pop_front();
		}
#ifdef DEBUG
		D_ASSERT(table_blocks_key_order.empty());
#endif
		return false;
	}

	// 3. construct the new plan tree
	// add the cross_product with the used table
	unique_ptr<LogicalOperator> belowed_cross_product = std::move(last_block);
	for (auto &block : table_blocks) {
		if (left_cond_table_index.count(block.first)) {
			belowed_cross_product =
			    LogicalCrossProduct::Create(std::move(belowed_cross_product), std::move(block.second));
		}
	}

	// add the belowed_cross_product to the last_join
	last_join.children[0] = std::move(belowed_cross_product);

	// get the previous op of last_join by index since we cannot convert JOIN to LogicalOperator
	auto reordered_plan = plan.get();
	for (int level_id = 0; level_id < last_join_level - 1; level_id++) {
		reordered_plan = reordered_plan->children[0].get();
	}

	// add the unused blocks as aboved_cross_product, on top of the last_join
	unique_ptr<LogicalOperator> aboved_cross_product = std::move(reordered_plan->children[0]);
	while (!unused_blocks.empty()) {
		aboved_cross_product =
		    LogicalCrossProduct::Create(std::move(aboved_cross_product), std::move(unused_blocks.front()));
		unused_blocks.pop();
	}

	// get the position to reorder
	reordered_plan = plan.get();
	for (int level_id = 0; level_id < last_join_level - 1; level_id++) {
		reordered_plan = reordered_plan->children[0].get();
	}
	// add the aboved_cross_product
	reordered_plan->children[0] = std::move(aboved_cross_product);
	return true;
}

void TopDownSplit::InsertTableBlocks(unique_ptr<LogicalOperator> &op,
                                     unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks,
                                     std::deque<idx_t> &table_blocks_key_order) {
	if (LogicalOperatorType::LOGICAL_GET == op->type) {
		auto &get_op = op->Cast<LogicalGet>();
		table_blocks.emplace(get_op.table_index, std::move(op));
		table_blocks_key_order.emplace_back(get_op.table_index);
	} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == op->type) {
		auto &chunk_op = op->Cast<LogicalColumnDataGet>();
		table_blocks.emplace(chunk_op.table_index, std::move(op));
		table_blocks_key_order.emplace_back(chunk_op.table_index);
	} else if (LogicalOperatorType::LOGICAL_FILTER == op->type) {
		idx_t table_index;
		std::function<void(unique_ptr<LogicalOperator> & current_op)> find_get;
		find_get = [&find_get, &table_index](unique_ptr<LogicalOperator> &current_op) {
			for (auto &child_op : current_op->children) {
				if (LogicalOperatorType::LOGICAL_GET != child_op->type)
					find_get(child_op);
				else {
					auto &get_op = child_op->Cast<LogicalGet>();
					table_index = get_op.table_index;
				}
			}
		};
		find_get(op);
		table_blocks.emplace(table_index, std::move(op));
		table_blocks_key_order.emplace_back(table_index);
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, block_op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}
}

bool TopDownSplit::BlockUsed(const unordered_set<idx_t> &left_cond_table_index, const unique_ptr<LogicalOperator> &op) {
	idx_t table_index;
	if (LogicalOperatorType::LOGICAL_GET == op->type) {
		auto &get_op = op->Cast<LogicalGet>();
		table_index = get_op.table_index;
	} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == op->type) {
		auto &chunk_op = op->Cast<LogicalColumnDataGet>();
		table_index = chunk_op.table_index;
	} else if (LogicalOperatorType::LOGICAL_FILTER == op->type) {
		std::function<void(const unique_ptr<LogicalOperator> &current_op)> find_get;
		find_get = [&find_get, &table_index](const unique_ptr<LogicalOperator> &current_op) {
			for (auto &child_op : current_op->children) {
				if (LogicalOperatorType::LOGICAL_GET != child_op->type)
					find_get(child_op);
				else {
					auto &get_op = child_op->Cast<LogicalGet>();
					table_index = get_op.table_index;
				}
			}
		};
		find_get(op);
	} else if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == op->type) {
		// if it is a JOIN, all blocks should be used
		return true;
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, block_op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}

	return left_cond_table_index.count(table_index);
}

} // namespace duckdb
