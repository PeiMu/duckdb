#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan) {
	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	unique_ptr<LogicalOperator> subquery;
	GetTargetTables(*plan);
	VisitOperator(*plan);
	CollectUsedTablePerLevel();
	return std::move(plan);
}

void TopDownSplit::VisitOperator(LogicalOperator &op) {
	std::vector<unique_ptr<LogicalOperator>> same_level_subqueries;
	std::vector<std::set<TableExpr>> same_level_table_exprs;

	// Since we don't split at CROSS_PRODUCT, we don't split its sibling
	// todo: this should be fixed after supporting parallel execution
	bool cross_product_sibling = false;

	// For now, we only check logical_filter and logical_comparison_join.
	// Basically, the split point is based on logical_comparison_join,
	// but if it has a logical_filter parent, then we split at the
	// logical_filter node.
	for (auto &child : op.children) {
		std::set<TableExpr> table_exprs;
		if (cross_product_sibling)
			break;
		switch (child->type) {
		// todo: potentially we can fuse CROSS_PRODUCT+FILTER or JOIN+FILTER,
		// if the other child node is not CROSS_PRODUCT, JOIN nor FILTER
		case LogicalOperatorType::LOGICAL_FILTER:
			// add filter's column usage
			table_exprs = GetFilterTableExpr(child->Cast<LogicalFilter>());
			// check continuous filter nodes, only split the first one
			if (!filter_parent) {
				child->split_point = true;
				// inherit from the children until it is not a filter
				//				auto child_copy = child->Copy(context);
				auto child_pointer = child.get();
				while (LogicalOperatorType::LOGICAL_FILTER == child_pointer->type) {
					auto child_exprs = GetFilterTableExpr(child_pointer->Cast<LogicalFilter>());
					table_exprs.insert(child_exprs.begin(), child_exprs.end());
					//					child_copy = child_copy->children[0]->Copy(context);
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
				child->split_point = true;
				table_exprs = GetJoinTableExpr(child->Cast<LogicalComparisonJoin>());
			}
			filter_parent = false;
			break;
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
			cross_product_sibling = true;
			child->split_point = false;
			filter_parent = false;
			break;
		default:
			child->split_point = false;
			filter_parent = false;
			break;
		}
		VisitOperator(*child);

		if (child->split_point) {
			//			same_level_subqueries.emplace_back(child->Copy(context));
			same_level_subqueries.emplace_back(std::move(child));
		}
		if (!table_exprs.empty()) {
			same_level_table_exprs.emplace_back(table_exprs);
		}
	}

	D_ASSERT(same_level_subqueries.size() <= 2);
	if (!same_level_subqueries.empty()) {
		subqueries.emplace_back(std::move(same_level_subqueries));
	}
	D_ASSERT(same_level_table_exprs.size() <= 2);
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
		used_tables.emplace(current_table_index);
	} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == op.type) {
		auto &chunk_op = op.Cast<LogicalColumnDataGet>();
		auto current_table_index = chunk_op.table_index;
		used_tables.emplace(current_table_index);
	}
	for (auto &child : op.children) {
		GetTargetTables(*child);
	}
}

std::set<TableExpr> TopDownSplit::GetJoinTableExpr(const LogicalComparisonJoin &join_op) {
	std::set<TableExpr> table_exprs;
	for (const auto &cond : join_op.conditions) {
		D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
		TableExpr current_left_table;
		auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
		current_left_table.table_idx = left_expr.binding.table_index;
		// it's the temporary id, but not physical id, e.g. by get.column_ids
		current_left_table.column_idx = left_expr.binding.column_index;
		current_left_table.column_name = left_expr.alias;
		current_left_table.return_type = left_expr.return_type;
		if (used_tables.count(current_left_table.table_idx))
			table_exprs.emplace(current_left_table);

		TableExpr current_right_table;
		auto &right_expr = cond.right->Cast<BoundColumnRefExpression>();
		current_right_table.table_idx = right_expr.binding.table_index;
		current_right_table.column_idx = right_expr.binding.column_index;
		current_right_table.column_name = right_expr.alias;
		current_right_table.return_type = right_expr.return_type;
		if (used_tables.count(current_right_table.table_idx))
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
		D_ASSERT(column_idx_it != get_op.column_ids.end());
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
		if (used_tables.count(table_expr.table_idx)) {
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
			D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
			TableExpr table_expr;
			auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
			table_expr.table_idx = column_ref_expr.binding.table_index;
			table_expr.column_idx = column_ref_expr.binding.column_index;
			table_expr.column_name = column_ref_expr.alias;
			table_expr.return_type = column_ref_expr.return_type;
			if (used_tables.count(table_expr.table_idx)) {
				proj_expr.emplace(table_expr);
			}
		}
	}
}

void TopDownSplit::GetAggregateTableExpr(const LogicalAggregate &aggregate_op) {
	for (const auto &agg_expr : aggregate_op.expressions) {
		D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
		auto &aggregate_expr = agg_expr->Cast<BoundAggregateExpression>();
		for (const auto &expr : aggregate_expr.children) {
			TableExpr table_expr;
			D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
			auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
			table_expr.table_idx = column_ref_expr.binding.table_index;
			table_expr.column_idx = column_ref_expr.binding.column_index;
			table_expr.column_name = column_ref_expr.alias;
			table_expr.return_type = column_ref_expr.return_type;
			if (used_tables.count(table_expr.table_idx)) {
				proj_expr.emplace(table_expr);
			}
		}
	}
}

void TopDownSplit::CollectUsedTable(const unique_ptr<LogicalOperator> &subquery, std::set<idx_t> &table_in_subquery) {
	for (const auto &child : subquery->children) {
		if (nullptr == child) {
			continue;
		}
		if (LogicalOperatorType::LOGICAL_GET == child->type) {
			auto &get_op = child->Cast<LogicalGet>();
			table_in_subquery.emplace(get_op.table_index);
		} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == child->type) {
			auto &chunk_op = child->Cast<LogicalColumnDataGet>();
			table_in_subquery.emplace(chunk_op.table_index);
		}
		if (child->split_point)
			continue;
		CollectUsedTable(child, table_in_subquery);
	}
}

void TopDownSplit::CollectUsedTablePerLevel() {
	for (const auto &temp_subquery_vec : subqueries) {
		std::set<idx_t> table_in_current_level;
		// todo: fix this when supporting parallel execution
		CollectUsedTable(temp_subquery_vec[0], table_in_current_level);
		table_in_current_level.insert(sibling_used_table.begin(), sibling_used_table.end());
		used_table_queue.emplace(table_in_current_level);
		sibling_used_table.clear();
		if (2 == temp_subquery_vec.size())
			CollectUsedTable(temp_subquery_vec[1], sibling_used_table);
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
				D_ASSERT(nullptr == new_plan->children[1]);
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
	std::function<void (unique_ptr<LogicalOperator> &op)> unMerge;
	subqueries.clear();
	unMerge = [&unMerge, this](unique_ptr<LogicalOperator> &op) {
		std::vector<unique_ptr<LogicalOperator>> same_level_subqueries;
		for (auto &child : op->children) {
			unMerge(child);
			if (child->split_point) {
				same_level_subqueries.emplace_back(std::move(child));
			}
		}
		D_ASSERT(same_level_subqueries.size() <= 2);
		if (!same_level_subqueries.empty()) {
			subqueries.emplace_back(std::move(same_level_subqueries));
		}
	};

	unMerge(plan);
}

unique_ptr<LogicalOperator> TopDownSplit::Rewrite(unique_ptr<LogicalOperator> &plan, bool &needToSplit) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_PRAGMA: {
		needToSplit = false;
		return std::move(plan); // skip optimizing simple & often-occurring plans unaffected by rewrites
	}
	default:
		break;
	}

	// 1. collect the condition of the last join
	auto op_child = plan.get();
	op_levels = 0;
	while (!op_child->children.empty() && LogicalOperatorType::LOGICAL_CROSS_PRODUCT != op_child->children[0]->type) {
		op_child = op_child->children[0].get();
		op_levels++;
	}
	if (op_child->children.empty()) {
		needToSplit = false;
		return std::move(plan);
	}

	auto &last_join = op_child->Cast<LogicalComparisonJoin>();

	// select the needed tables
	std::unordered_set<idx_t> left_cond_table_index;
	for (const auto &cond : last_join.conditions) {
		D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
		auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
		left_cond_table_index.emplace(left_expr.binding.table_index);
	}

	// 2. collect all tables below the last JOIN
	std::unordered_map<idx_t, unique_ptr<LogicalOperator>> table_blocks;

	auto last_cross_product = op_child;
	while (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == last_cross_product->children[0]->type) {
		last_cross_product = last_cross_product->children[0].get();
		auto &cross_product_op = last_cross_product->Cast<LogicalCrossProduct>();
		InsertTableBlocks(cross_product_op.children[1], table_blocks);
	}
	auto &last_block = last_cross_product->Cast<LogicalCrossProduct>().children[0];

	std::queue<unique_ptr<LogicalOperator>> unused_blocks;
	for (auto &block : table_blocks) {
		if (!left_cond_table_index.count(block.first)) {
			unused_blocks.push(std::move(block.second));
		}
	}

	if (unused_blocks.empty()) {
		needToSplit = false;
		return std::move(plan);
	}

	unique_ptr<LogicalOperator> below_cross_product = std::move(last_block);
	for (auto &block : table_blocks) {
		if (left_cond_table_index.count(block.first)) {
			below_cross_product = LogicalCrossProduct::Create(std::move(below_cross_product), std::move(block.second));
		}
	}

	last_join.children[0] = std::move(below_cross_product);
	auto reordered_plan = plan.get();
	for (int level_id = 0; level_id < op_levels - 1; level_id++) {
		reordered_plan = reordered_plan->children[0].get();
	}
	unique_ptr<LogicalOperator> above_cross_product = std::move(reordered_plan->children[0]);

	while (!unused_blocks.empty()) {
		above_cross_product =
		    LogicalCrossProduct::Create(std::move(above_cross_product), std::move(unused_blocks.front()));
		unused_blocks.pop();
	}

	// get the position to reorder
	reordered_plan = plan.get();
	for (int level_id = 0; level_id < op_levels - 1; level_id++) {
		reordered_plan = reordered_plan->children[0].get();
	}
	reordered_plan->children[0] = std::move(above_cross_product);

	needToSplit = true;

	return std::move(plan);
}

void TopDownSplit::InsertTableBlocks(unique_ptr<LogicalOperator> &op,
                                     unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks) {
	if (LogicalOperatorType::LOGICAL_GET == op->type) {
		auto &get_op = op->Cast<LogicalGet>();
		table_blocks.emplace(get_op.table_index, get_op.Copy(context));
	} else if (LogicalOperatorType::LOGICAL_FILTER == op->type) {
		idx_t table_index;
		std::function<void(unique_ptr<LogicalOperator> & current_op)> find_get;
		find_get = [find_get, &table_index](unique_ptr<LogicalOperator> &current_op) {
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
		table_blocks.emplace(table_index, op->Copy(context));
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, block_op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}
}

} // namespace duckdb
