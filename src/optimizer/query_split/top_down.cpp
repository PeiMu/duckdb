#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan, unique_ptr<DataChunk> previous_result,
                                                bool &subquery_loop) {
	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	unique_ptr<LogicalOperator> subquery;
	if (subqueries.empty()) {
#ifdef DEBUG
		// debug
		plan->Print();
#endif
		GetTargetTables(*plan);
		VisitOperator(*plan);

		if (subqueries.front().size() > 1) {
			// todo: execute in parallel
		}
		subquery = std::move(subqueries.front()[0]);
		subqueries.pop();
		if (0 == subqueries.size()) {
			subquery_loop = false;
		} else {
			subquery_loop = true;
		}

	} else {
		if (subqueries.front().size() > 1) {
			// todo: execute in parallel
		}
		subquery = std::move(subqueries.front()[0]);
		subqueries.pop();
		if (0 == subqueries.size()) {
			subquery_loop = false;
		} else {
			subquery_loop = true;
		}

		// todo: reorder the chunks with the column_ids
		DataChunk test_data_chunk;
		previous_result->Split(test_data_chunk, 1);
		test_data_chunk.Fuse(*previous_result);
		// todo: get types
		vector<LogicalType> types {LogicalType::VARCHAR, LogicalType::INTEGER};
		auto collection = make_uniq<ColumnDataCollection>(context, types);
		collection->Append(test_data_chunk);
		// todo: get table index
		idx_t table_idx = 1;

		auto chunk_scan = make_uniq<LogicalColumnDataGet>(table_idx, types, std::move(collection));
		chunk_scan->Print();
		// todo: find the insert point and insert the `ColumnDataGet` node to the logical plan
		std::function<void(LogicalOperator & op)> get_insert_point_test;
		get_insert_point_test = [&chunk_scan, &get_insert_point_test](LogicalOperator &op) {
			for (auto child_it = op.children.begin(); child_it != op.children.end(); child_it++) {
				if (LogicalOperatorType::LOGICAL_FILTER == (*child_it)->type) {
					op.children.erase(child_it);
					op.children.emplace_back(std::move(chunk_scan));
					return;
				} else if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == (*child_it)->type) {
					auto &join_op = (*child_it)->Cast<LogicalComparisonJoin>();

				} else {
					get_insert_point_test(*(*child_it));
				}
			}
		};
		get_insert_point_test(*subquery);
		subquery->Print();
	}

#ifdef DEBUG
	// debug: print subquery
	Printer::Print("Current subquery");
	subquery->Print();
#endif

	vector<unique_ptr<Expression>> new_exprs;
	auto expr_idx_pair = table_expr_stack.top();
	table_expr_stack.pop();
	// collect all columns with the same table
	auto temp_stack = table_expr_stack;
	while (!temp_stack.empty()) {
		auto temp = temp_stack.top();
		temp_stack.pop();
		std::unordered_set<TableExpr, TableExprHash> temp_set;
		for (const auto &current_pair : expr_idx_pair) {
			auto same_table_it = std::find_if(temp.begin(), temp.end(), [current_pair](TableExpr table_expr) {
				return table_expr.table_idx == current_pair.table_idx;
			});
			if (same_table_it != temp.end()) {
				temp_set.emplace(*same_table_it);
			}
		}
		// merge the temp_set to expr_idx_pair
		if (!temp_set.empty())
			expr_idx_pair.insert(temp_set.begin(), temp_set.end());
	}
	for (const auto &expr_pair : expr_idx_pair) {
		ColumnBinding binding = ColumnBinding(expr_pair.table_idx, expr_pair.column_idx);
		auto col_ref_select_expr =
		    make_uniq<BoundColumnRefExpression>(expr_pair.column_name, expr_pair.return_type, binding, 0);
		new_exprs.emplace_back(std::move(col_ref_select_expr));
	}

	plan->children.clear();
	plan->AddChild(std::move(subquery));
	plan->expressions.clear();
	plan->expressions = std::move(new_exprs);

#ifdef DEBUG
	// debug: print subquery
	Printer::Print("Current subquery with projection");
	plan->Print();
#endif
	return plan;
}

void TopDownSplit::VisitOperator(LogicalOperator &op) {
	std::vector<unique_ptr<LogicalOperator>> same_level_subqueries;

	// todo: collect table_expr_stack from projection node
	// if (op.type == LogicalOperatorType::LOGICAL_PROJECTION)
	// GetProjTableExpr(op.Cast<LogicalProjection>());

	// For now, we only check logical_filter and logical_comparison_join.
	// Basically, the split point is based on logical_comparison_join,
	// but if it has a logical_filter parent, then we split at the
	// logical_filter node.
	for (auto &child : op.children) {
		switch (child->type) {
		case LogicalOperatorType::LOGICAL_FILTER:
			// check if it's a filter node, otherwise set false
			filter_parent = true;
			same_level_subqueries.emplace_back(child->Copy(context));
			// todo: do we need to add filter's column usage?
			// GetFilterTableExpr(child->Cast<LogicalFilter>());
			break;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			GetJoinTableExpr(child->Cast<LogicalComparisonJoin>(), filter_parent);
			if (filter_parent) {
				filter_parent = false;
			} else {
				same_level_subqueries.emplace_back(child->Copy(context));
			}
			break;
		default:
			filter_parent = false;
			break;
		}
		VisitOperator(*child);
	}

	D_ASSERT(same_level_subqueries.size() <= 2);
	if (!same_level_subqueries.empty()) {
		subqueries.emplace(std::move(same_level_subqueries));
	}
}

void TopDownSplit::GetProjTableExpr(const LogicalProjection &proj_op) {
	std::unordered_set<TableExpr, TableExprHash> proj_pair;
	// check which column match in the projection's expression
	TableExpr current_pair;
	current_pair.table_idx = UINT64_MAX;
	current_pair.column_idx = UINT64_MAX;
	for (size_t expr_id = 0; expr_id < proj_op.expressions.size(); expr_id++) {
		auto expr_name = proj_op.expressions[expr_id]->GetName();
		// find the real column name inside the potential brackets
		auto last_left_bracket = expr_name.find_last_of('(');
		if (std::string::npos != last_left_bracket) {
			auto first_right_bracket = expr_name.find_first_of(')');
			D_ASSERT(first_right_bracket > last_left_bracket);
			expr_name = expr_name.substr(last_left_bracket + 1, first_right_bracket - last_left_bracket - 1);
		}
		current_pair.column_name = expr_name;
		current_pair.return_type = proj_op.expressions[expr_id]->return_type;

		for (const auto &table : target_tables) {
			auto column_idx = table.second->GetTable()->GetColumnIndex(expr_name, true);
			if (column_idx.IsValid()) {
				current_pair.column_idx = column_idx.index;
				current_pair.table_idx = table.first;
				break;
			}
		}
		D_ASSERT(UINT64_MAX != current_pair.table_idx);
		D_ASSERT(UINT64_MAX != current_pair.column_idx);
		proj_pair.emplace(current_pair);
	}

	if (!proj_pair.empty())
		table_expr_stack.emplace(proj_pair);
}

void TopDownSplit::GetTargetTables(LogicalOperator &op) {
	if (LogicalOperatorType::LOGICAL_GET == op.type) {
		auto &get_op = op.Cast<LogicalGet>();
		auto current_table_index = get_op.table_index;
		target_tables.emplace(current_table_index, &get_op);
	}
	for (auto &child : op.children) {
		GetTargetTables(*child);
	}
}

void TopDownSplit::GetJoinTableExpr(const LogicalComparisonJoin &join_op, bool same_level) {
	std::unordered_set<TableExpr, TableExprHash> table_exprs;
	for (const auto &cond : join_op.conditions) {
		D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
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
	if (!table_exprs.empty()) {
		//		if (same_level) {
		//			table_expr_stack.top().insert(table_exprs.begin(), table_exprs.end());
		//		} else {
		table_expr_stack.emplace(table_exprs);
		//		}
	}
}

void TopDownSplit::GetFilterTableExpr(const LogicalFilter &filter_op) {
	std::unordered_set<TableExpr, TableExprHash> table_exprs;

	auto get_column_ref_expr = [&table_exprs, this](const unique_ptr<Expression> &expr) {
		TableExpr table_expr;
		auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
		table_expr.table_idx = column_ref_expr.binding.table_index;
		table_expr.column_idx = target_tables[table_expr.table_idx]->column_ids[column_ref_expr.binding.column_index];
		table_expr.column_name = column_ref_expr.alias;
		table_expr.return_type = column_ref_expr.return_type;
		if (target_tables.count(table_expr.table_idx)) {
			table_exprs.emplace(table_expr);
		}
	};

	for (const auto &expr : filter_op.expressions) {
		if (ExpressionType::BOUND_COLUMN_REF == expr->type) {
			get_column_ref_expr(expr);
		} else if (ExpressionType::BOUND_FUNCTION == expr->type) {
			auto &function_expr = expr->Cast<BoundFunctionExpression>();
			for (const auto &func_child : function_expr.children) {
				if (ExpressionType::BOUND_COLUMN_REF == func_child->type) {
					get_column_ref_expr(func_child);
				} else if (ExpressionType::VALUE_CONSTANT == func_child->type) {
					// it's a constant value, skip it
				} else {
					Printer::Print("Do not support yet");
				}
			}
		} else {
			Printer::Print("Do not support yet");
		}
	}
	if (!table_exprs.empty())
		table_expr_stack.emplace(table_exprs);
}

} // namespace duckdb
