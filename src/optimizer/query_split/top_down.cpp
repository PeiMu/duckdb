#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan, bool follow_pipeline_breaker) {
	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	unique_ptr<LogicalOperator> subquery;
	follow_pipeline_breaker_ = follow_pipeline_breaker;
	AddTargetTables(*plan);
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
		case LogicalOperatorType::LOGICAL_FILTER: {
			if (top_most && 0 == idx) {
				// if this is the top most operator, we only check the expr itself
				top_most = false;
				// add filter's column usage
				table_exprs = GetFilterTableExpr(child->Cast<LogicalFilter>());
				query_split_index++;
				child->split_index = query_split_index;
				break;
			}
#if SPLIT_FILTER
			// otherwise, it might have MARK join under it
			if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child->children[0]->type) {
				auto &join_op = child->children[0]->Cast<LogicalComparisonJoin>();
				if (JoinType::SEMI != join_op.join_type && JoinType::MARK != join_op.join_type) {
					child->split_index = 0;
					break;
				}
			}
			if (ENABLE_PARALLEL_EXECUTION) {
				// todo
			} else {
#ifdef DEBUG
				D_ASSERT(LogicalOperatorType::LOGICAL_GET == child->children[0]->type ||
				         LogicalOperatorType::LOGICAL_CHUNK_GET == child->children[0]->type ||
				         LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child->children[0]->type ||
				         LogicalOperatorType::LOGICAL_CROSS_PRODUCT == child->children[0]->type);
#endif
				// add filter's column usage
				table_exprs = GetFilterTableExpr(child->Cast<LogicalFilter>());
				// check continuous filter nodes, only split the first one
				query_split_index++;
				child->split_index = query_split_index;
				// add the SEMI or MARK join's column usage
				auto child_pointer = child->children[0].get();
				if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child_pointer->type) {
					auto &inner_join = child_pointer->Cast<LogicalComparisonJoin>();
#ifdef DEBUG
					D_ASSERT(JoinType::SEMI == inner_join.join_type || JoinType::MARK == inner_join.join_type);

#endif
					auto child_exprs = GetJoinTableExpr(inner_join);
					table_exprs.insert(child_exprs.begin(), child_exprs.end());
				}
				break;
			}
#endif
		}
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
					query_split_index++;
					child->split_index = query_split_index;
				}
			} else {
				query_split_index++;
				child->split_index = query_split_index;
			}

			table_exprs = GetJoinTableExpr(join_op);

			// we need to collect the FILTER table_exprs if we push the FILTER down to the JOIN
			if (LogicalOperatorType::LOGICAL_FILTER == child->children[0]->type) {
				auto &inner_filter = child->children[0]->Cast<LogicalFilter>();
				auto child_exprs = GetFilterTableExpr(inner_filter);
				table_exprs.insert(child_exprs.begin(), child_exprs.end());
			}
			top_most = false;
			break;
		}
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
			if (0 == idx && 2 == op.children.size() && nullptr == op.children[1]) {
				// we need to split it as a sibling node
				// fixme: add query_split_index when support ENABLE_PARALLEL_EXECUTION
				// query_split_index++;
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
		AddProjTableExpr(op.Cast<LogicalProjection>());
	}
}

void TopDownSplit::AddTargetTables(LogicalOperator &op) {
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
		AddTargetTables(*child);
	}
}

std::set<TableExpr> TopDownSplit::GetJoinTableExpr(const LogicalComparisonJoin &join_op) {
	std::set<TableExpr> table_exprs;
	for (const auto &cond : join_op.conditions) {
		// the JOIN nodes seems to only have one expr on each side of operator (e.g., '==')
		// so it's safe to use `AddTableExprs`
		// or we can use `VisitExprs` with `TableExprCollector` if we find it's necessary
		AddTableExprs(table_exprs, cond.left);
		AddTableExprs(table_exprs, cond.right);
	}
	return table_exprs;
}

std::set<TableExpr> TopDownSplit::GetCrossProductTableExpr(const LogicalCrossProduct &product_op) {
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

	for (const auto &expr : filter_op.expressions) {
		VisitExprs(expr, TableExprCollector {this, table_exprs});
	}
	return table_exprs;
}

void TopDownSplit::AddProjTableExpr(const LogicalProjection &proj_op) {
	// if it's children is `aggregate` or `group by`, we only check the child op
	if (nullptr != proj_op.children[0] &&
	    LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY == proj_op.children[0]->type) {
		AddAggregateTableExpr(proj_op.children[0]->Cast<LogicalAggregate>());
	} else {
		for (const auto &expr : proj_op.expressions) {
			VisitExprs(expr, HeaderExprCollector {this});
		}
	}
}

void TopDownSplit::AddAggregateTableExpr(const LogicalAggregate &aggregate_op) {
	// add table_expr of group by
	for (const auto &group_expr : aggregate_op.groups) {
		VisitExprs(group_expr, HeaderExprCollector {this});
	}

	// add table_expr of aggregate op expression
	for (const auto &agg_expr : aggregate_op.expressions) {
#ifdef DEBUG
		D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
#endif
		auto &aggregate_expr = agg_expr->Cast<BoundAggregateExpression>();
		for (const auto &expr : aggregate_expr.children) {
			VisitExprs(expr, HeaderExprCollector {this});
		}
	}
}

void TopDownSplit::AddTableExprs(std::set<TableExpr> &table_exprs, const unique_ptr<Expression> &expr) {
	TableExpr table_expr;
	auto expr_info = GetConstTableExpr(expr);
	table_expr.table_idx = expr_info.table_idx;
	table_expr.column_idx = expr_info.column_idx;
	table_expr.column_name = expr_info.column_name;
	table_expr.return_type = expr_info.return_type;
	if (target_tables.count(table_expr.table_idx)) {
		table_exprs.emplace(table_expr);
	}
}

void TopDownSplit::AddHeaderTableExprs(const unique_ptr<Expression> &expr) {
	TableExpr table_expr;
	auto expr_info = GetConstTableExpr(expr);
	table_expr.table_idx = expr_info.table_idx;
	table_expr.column_idx = expr_info.column_idx;
	table_expr.column_name = expr_info.column_name;
	table_expr.return_type = expr_info.return_type;
	if (target_tables.count(table_expr.table_idx)) {
		header_expr.emplace_back(table_expr);
	}
}
} // namespace duckdb
