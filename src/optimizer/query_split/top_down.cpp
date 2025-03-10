#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan, bool follow_pipeline_breaker) {
	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	unique_ptr<LogicalOperator> subquery;
	follow_pipeline_breaker_ = follow_pipeline_breaker;
	GetTargetTables(*plan);
	VisitOperator(*plan);
	return std::move(plan);
}

void TopDownSplit::VisitOperator(LogicalOperator &op) {
	std::vector<unique_ptr<LogicalOperator>> same_level_subqueries;
	std::vector<std::set<TableExpr>> same_level_table_exprs;

	// TODO: This code is very ugly...
	// If we follow the pipeline breaker role, where we only split at the right child node of JOIN,
	// we need to check the right child first to fit the table_expr process - commit 1883b62
	// Else (we ENABLE_CROSS_PRODUCT_REWRITE), we want to get the deep-first tables,
	// to see if the CROSS_PRODUCTs of the subquery can be simplified
	for (int idx = op.children.size() - 1; idx > -1; idx--) {
		auto &child = op.children[idx];
		std::set<TableExpr> table_exprs;
		switch (child->type) {
			// if the other child node is not CROSS_PRODUCT, JOIN nor FILTER
#if SPLIT_FILTER
		case LogicalOperatorType::LOGICAL_FILTER: {
			if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child->children[0]->type) {
				auto &join_op = child->children[0]->Cast<LogicalComparisonJoin>();
				if (JoinType::SEMI != join_op.join_type && JoinType::MARK != join_op.join_type) {
					child->split_index = 0;
					break;
				}
			}
			// for easier implementation, we only split the Filter when it is a left child for non-parallel case
			if (ENABLE_PARALLEL_EXECUTION) {
				// todo
			} else {
#ifdef DEBUG
				D_ASSERT(LogicalOperatorType::LOGICAL_GET == child->children[0]->type ||
				         LogicalOperatorType::LOGICAL_CHUNK_GET == child->children[0]->type ||
				         LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child->children[0]->type);
#endif
				// add filter's column usage
				table_exprs = GetFilterTableExpr(child->Cast<LogicalFilter>());
				// check continuous filter nodes, only split the first one
				query_split_index++;
				child->split_index = query_split_index;
				// inherit from the children until it is not a filter
				auto child_pointer = child->children[0].get();
				if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child_pointer->type) {
					auto child_exprs = GetJoinTableExpr(child_pointer->Cast<LogicalComparisonJoin>());
					table_exprs.insert(child_exprs.begin(), child_exprs.end());
				}
				break;
			}
		}
#endif
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			// we skip the SEMI JOIN or MARK JOIN
			// fixme: may have bugs
			auto &join_op = child->Cast<LogicalComparisonJoin>();
			if (JoinType::SEMI == join_op.join_type || JoinType::MARK == join_op.join_type) {
				child->split_index = 0;
				break;
			}

			if (follow_pipeline_breaker_) {
				if (top_most || 1 == idx) {
					top_most = false;
					query_split_index++;
					child->split_index = query_split_index;
				}
			} else {
				query_split_index++;
				child->split_index = query_split_index;
			}

			table_exprs = GetJoinTableExpr(join_op);
			break;
		}
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
			if (0 == idx && nullptr == op.children[1]) {
				// fixme: add query_split_index when support ENABLE_PARALLEL_EXECUTION
//				query_split_index++;
				child->split_index = query_split_index;
			}
			break;
		}
		default:
			child->split_index = 0;
			break;
		}
		VisitOperator(*child);

		if (child->split_index) {
			same_level_subqueries.emplace_back(std::move(child));
		}

		if (!table_exprs.empty()) {
			// if the last level JOIN cannot be split, we merge the table exprs
			if (child && LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child->type && !child->split_index) {
				std::merge(last_level_table_exprs.begin(), last_level_table_exprs.end(), table_exprs.begin(),
				           table_exprs.end(),
				           std::inserter(last_level_table_exprs, std::begin(last_level_table_exprs)));
				table_exprs.clear();
			} else {
				same_level_table_exprs.emplace_back(table_exprs);
			}
		}
		if (!last_level_table_exprs.empty() && !child) {
			// if we have last_level_table_exprs, and the current JOIN can be split,
			// we need to merge the table exprs.
			// todo: check left (same_level_table_exprs[0]) or right (same_level_table_exprs[1])?
			same_level_table_exprs.rbegin()->insert(last_level_table_exprs.begin(), last_level_table_exprs.end());
			last_level_table_exprs.clear();
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
		GetColRefExpr(table_exprs, cond.left->Cast<BoundColumnRefExpression>());
		GetColRefExpr(table_exprs, cond.right->Cast<BoundColumnRefExpression>());
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

	std::function<void(const unique_ptr<Expression> &expr)> get_expr;
	get_expr = [&table_exprs, this, &get_expr](const unique_ptr<Expression> &expr) {
		if (ExpressionType::BOUND_COLUMN_REF == expr->type) {
			GetColRefExpr(table_exprs, expr->Cast<BoundColumnRefExpression>());
		} else if (ExpressionType::BOUND_FUNCTION == expr->type) {
			GetFunctionExpr(table_exprs, expr->Cast<BoundFunctionExpression>());
		} else if (ExpressionType::COMPARE_NOTEQUAL == expr->type || ExpressionType::COMPARE_EQUAL == expr->type ||
		           ExpressionType::COMPARE_GREATERTHAN == expr->type ||
		           ExpressionType::COMPARE_LESSTHAN == expr->type ||
		           ExpressionType::COMPARE_GREATERTHANOREQUALTO == expr->type ||
		           ExpressionType::COMPARE_LESSTHANOREQUALTO == expr->type) {
			GetComparisonExpr(table_exprs, expr->Cast<BoundComparisonExpression>());
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
		} else if (ExpressionType::COMPARE_BETWEEN == expr->type) {
			auto &bound_between_expr = expr->Cast<BoundBetweenExpression>();
			get_expr(bound_between_expr.input);
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
			GetColRefExpr(expr->Cast<BoundColumnRefExpression>());
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
			if (ExpressionType::BOUND_COLUMN_REF == expr->type) {
				GetColRefExpr(expr->Cast<BoundColumnRefExpression>());
			} else if (ExpressionType::OPERATOR_CAST == expr->type) {
				GetCastExpr(expr->Cast<BoundCastExpression>());
			} else {
				Printer::Print("Doesn't support " + ExpressionTypeToString(expr->type) + " yet!");
				D_ASSERT(false);
			}
		}
	}

	for (const auto &group_expr : aggregate_op.groups) {
		if (ExpressionType::BOUND_COLUMN_REF == group_expr->type) {
			GetColRefExpr(group_expr->Cast<BoundColumnRefExpression>());
		}
	}
}

void TopDownSplit::GetColRefExpr(set<TableExpr> &table_exprs, const BoundColumnRefExpression &column_ref_expr) {
	TableExpr table_expr;
	table_expr.table_idx = column_ref_expr.binding.table_index;
	table_expr.column_idx = column_ref_expr.binding.column_index;
	table_expr.column_name = column_ref_expr.alias;
	table_expr.return_type = column_ref_expr.return_type;
	if (target_tables.count(table_expr.table_idx)) {
		table_exprs.emplace(table_expr);
	}
}

void TopDownSplit::GetColRefExpr(const BoundColumnRefExpression &column_ref_expr) {
	TableExpr table_expr;
	table_expr.table_idx = column_ref_expr.binding.table_index;
	table_expr.column_idx = column_ref_expr.binding.column_index;
	table_expr.column_name = column_ref_expr.alias;
	table_expr.return_type = column_ref_expr.return_type;
	if (target_tables.count(table_expr.table_idx)) {
		proj_expr.emplace_back(table_expr);
	}
}

void TopDownSplit::GetFunctionExpr(set<TableExpr> &table_exprs, const BoundFunctionExpression &function_expr) {
	for (const auto &func_child : function_expr.children) {
		if (ExpressionType::BOUND_COLUMN_REF == func_child->type) {
			GetColRefExpr(table_exprs, func_child->Cast<BoundColumnRefExpression>());
		} else if (ExpressionType::VALUE_CONSTANT == func_child->type) {
			// it's a constant value, skip it
		} else {
			Printer::Print(StringUtil::Format("Do not support yet, func_child->type:  %s",
			                                  ExpressionTypeToString(func_child->type)));
			D_ASSERT(false);
		}
	}
}

void TopDownSplit::GetFunctionExpr(const BoundFunctionExpression &function_expr) {
	for (const auto &func_child : function_expr.children) {
		if (ExpressionType::BOUND_COLUMN_REF == func_child->type) {
			GetColRefExpr(func_child->Cast<BoundColumnRefExpression>());
		} else if (ExpressionType::VALUE_CONSTANT == func_child->type) {
			// it's a constant value, skip it
		} else if (ExpressionType::OPERATOR_CAST == func_child->type) {
			GetCastExpr(func_child->Cast<BoundCastExpression>());
		} else {
			Printer::Print(StringUtil::Format("Do not support yet, func_child->type:  %s",
			                                  ExpressionTypeToString(func_child->type)));
			D_ASSERT(false);
		}
	}
}

void TopDownSplit::GetCastExpr(const BoundCastExpression &cast_expr) {
	if (ExpressionType::BOUND_COLUMN_REF == cast_expr.child->type) {
		GetColRefExpr(cast_expr.child->Cast<BoundColumnRefExpression>());
	} else if (ExpressionType::BOUND_FUNCTION == cast_expr.child->type) {
		GetFunctionExpr(cast_expr.child->Cast<BoundFunctionExpression>());
	} else {
		Printer::Print("Doesn't support " + ExpressionTypeToString(cast_expr.child->type) + " yet!");
		D_ASSERT(false);
	}
}

void TopDownSplit::GetComparisonExpr(set<TableExpr> &table_exprs, const BoundComparisonExpression &comparison_expr) {
	auto &left_expr = comparison_expr.left;
	if (ExpressionType::BOUND_COLUMN_REF == left_expr->type) {
		GetColRefExpr(table_exprs, left_expr->Cast<BoundColumnRefExpression>());
	} else if (ExpressionType::BOUND_FUNCTION == left_expr->type) {
		GetFunctionExpr(table_exprs, left_expr->Cast<BoundFunctionExpression>());
	} else if (ExpressionType::VALUE_CONSTANT == left_expr->type) {
		// it's a constant value, skip it
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, left_expr->type:  %s", ExpressionTypeToString(left_expr->type)));
		D_ASSERT(false);
	}

	auto &right_expr = comparison_expr.right;
	if (ExpressionType::BOUND_COLUMN_REF == right_expr->type) {
		GetColRefExpr(table_exprs, right_expr->Cast<BoundColumnRefExpression>());
	} else if (ExpressionType::BOUND_FUNCTION == left_expr->type) {
		GetFunctionExpr(table_exprs, left_expr->Cast<BoundFunctionExpression>());
	} else if (ExpressionType::VALUE_CONSTANT == right_expr->type) {
		// it's a constant value, skip it
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, right_expr->type:  %s", ExpressionTypeToString(right_expr->type)));
		D_ASSERT(false);
	}
}

} // namespace duckdb
