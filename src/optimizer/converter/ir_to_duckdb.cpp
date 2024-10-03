#include "duckdb/optimizer/converter/ir_to_duckdb.h"

namespace duckdb {
unique_ptr<LogicalOperator> IRConverter::InjectPlan(const char *postgres_plan_str,
                                                    unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
                                                    std::vector<unique_ptr<Expression>> &expr_vec) {
	PlanReader plan_reader;
	unique_ptr<SimplestNode> postgres_plan = plan_reader.StringToNode(postgres_plan_str);
	D_ASSERT(AggregateNode == postgres_plan->GetNodeType());
	unique_ptr<SimplestStmt> postgres_stmt = unique_ptr_cast<SimplestNode, SimplestStmt>(std::move(postgres_plan));
	// add table/column name from plan_reader.table_col_names
	AddTableColumnName(postgres_stmt, plan_reader.table_col_names);
#ifdef DEBUG
	postgres_stmt->Print();
#endif
	auto postgres_plan_pointer = postgres_stmt.get();

	// start from JoinNode
	while (JoinNode != postgres_plan_pointer->GetNodeType()) {
#ifdef DEBUG
		D_ASSERT(postgres_plan_pointer->children.size() >= 1);
#endif
		postgres_plan_pointer = postgres_plan_pointer->children[0].get();
	};

	std::unordered_map<int, int> pg_duckdb_table_idx = MatchTableIndex(table_map, plan_reader.table_col_names);

	// construct plan from postgres
	auto new_duckdb_plan = ConstructDuckdbPlan(postgres_plan_pointer, table_map, pg_duckdb_table_idx, expr_vec);

	return new_duckdb_plan;
}

unordered_map<std::string, unique_ptr<LogicalGet>> IRConverter::GetTableMap(unique_ptr<LogicalOperator> &duckdb_plan) {
	unordered_map<std::string, unique_ptr<LogicalGet>> table_map;

	std::function<void(unique_ptr<LogicalOperator> & duckdb_plan)> iterate_plan;
	iterate_plan = [&table_map, &iterate_plan](unique_ptr<LogicalOperator> &duckdb_plan) {
		for (auto &child : duckdb_plan->children) {
			if (LogicalOperatorType::LOGICAL_GET == child->type) {
				auto get = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(child));
				std::string table_name = get->function.to_string(get->bind_data.get());
				table_map.emplace(table_name, std::move(get));
			} else {
				iterate_plan(child);
			}
		}
	};

	iterate_plan(duckdb_plan);

	return table_map;
}

std::vector<unique_ptr<Expression>> IRConverter::CollectFilterExpressions(unique_ptr<LogicalOperator> &duckdb_plan) {
	std::vector<unique_ptr<Expression>> expr_vec;
	std::function<void(unique_ptr<LogicalOperator> & duckdb_plan)> iterate_plan;
	iterate_plan = [&expr_vec, &iterate_plan](unique_ptr<LogicalOperator> &duckdb_plan) {
		for (auto &child : duckdb_plan->children) {
			if (LogicalOperatorType::LOGICAL_FILTER == child->type) {
				auto &filter_node = child->Cast<LogicalFilter>();
				for (auto &expr : filter_node.expressions) {
					expr_vec.emplace_back(std::move(expr));
				}
			} else {
				iterate_plan(child);
			}
		}
	};

	iterate_plan(duckdb_plan);

	return expr_vec;
}

unique_ptr<LogicalComparisonJoin> IRConverter::ConstructDuckdbJoin(SimplestJoin *postgres_join,
                                                                   unique_ptr<LogicalOperator> left_child,
                                                                   unique_ptr<LogicalOperator> right_child,
                                                                   const unordered_map<int, int> &pg_duckdb_table_idx) {
	auto duckdb_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	duckdb_join->children.push_back(std::move(left_child));
	duckdb_join->children.push_back(std::move(right_child));
	JoinCondition cond;
	for (const auto &postgres_cond : postgres_join->join_conditions) {
		auto comp_op = postgres_cond->GetSimplestExprType();
		cond.comparison = ConvertCompType(comp_op);
		auto &left_pg_cond = postgres_cond->left_attr;
		LogicalType left_type = ConvertVarType(left_pg_cond->GetType());
		auto left_index_find = pg_duckdb_table_idx.find(left_pg_cond->GetTableIndex());
#ifdef DEBUG
		D_ASSERT(left_index_find != pg_duckdb_table_idx.end());
#endif
		auto left_table_index = left_index_find->second;
		auto find_col_idx = std::find(column_idx_mapping[left_table_index].begin(),
		                              column_idx_mapping[left_table_index].end(), left_pg_cond->GetColumnIndex() - 1);
#ifdef DEBUG
		D_ASSERT(find_col_idx != column_idx_mapping[left_table_index].end());
#endif
		auto left_column_index = find_col_idx - column_idx_mapping[left_table_index].begin();
		cond.left = make_uniq<BoundColumnRefExpression>(left_pg_cond->GetColumnName(), left_type,
		                                                ColumnBinding(left_table_index, left_column_index));

		auto &right_pg_cond = postgres_cond->right_attr;
		LogicalType right_type = ConvertVarType(right_pg_cond->GetType());
		auto right_index_find = pg_duckdb_table_idx.find(right_pg_cond->GetTableIndex());
#ifdef DEBUG
		D_ASSERT(right_index_find != pg_duckdb_table_idx.end());
#endif
		auto right_table_index = right_index_find->second;
		find_col_idx = std::find(column_idx_mapping[right_table_index].begin(),
		                         column_idx_mapping[right_table_index].end(), right_pg_cond->GetColumnIndex() - 1);
#ifdef DEBUG
		D_ASSERT(find_col_idx != column_idx_mapping[right_table_index].end());
#endif
		auto right_column_index = find_col_idx - column_idx_mapping[right_table_index].begin();
		cond.right = make_uniq<BoundColumnRefExpression>(right_pg_cond->GetColumnName(), right_type,
		                                                 ColumnBinding(right_table_index, right_column_index));

		// check if the cond match the children
		bool match_cond = CheckCondIndex(cond.left, duckdb_join->children[0]);
		// if not match, we need to swap the condition
		if (!match_cond) {
#ifdef DEBUG
			D_ASSERT(!CheckCondIndex(cond.right, duckdb_join->children[1]));
#endif
			unique_ptr<Expression> tmp = std::move(cond.left);
			cond.left = std::move(cond.right);
			cond.right = std::move(tmp);
		}

		duckdb_join->conditions.push_back(std::move(cond));
	}
	return duckdb_join;
}

bool IRConverter::CheckCondIndex(const unique_ptr<Expression> &expr, const unique_ptr<LogicalOperator> &child) {
#ifdef DEBUG
	// todo: check if all of the expr are bound_column_ref
	D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->GetExpressionType());
#endif
	auto &bound_col_ref = expr->Cast<BoundColumnRefExpression>();
	auto expr_table_idx = bound_col_ref.binding.table_index;
	bool match_index = false;

	std::function<void(const unique_ptr<LogicalOperator> &duckdb_plan)> iterate_plan;
	iterate_plan = [expr_table_idx, &match_index, &iterate_plan](const unique_ptr<LogicalOperator> &duckdb_plan) {
		if (match_index)
			return;

		for (auto &child : duckdb_plan->children) {
			iterate_plan(child);
		}

		if (LogicalOperatorType::LOGICAL_GET == duckdb_plan->type) {
			auto &get_node = duckdb_plan->Cast<LogicalGet>();
			match_index = get_node.table_index == expr_table_idx;
			if (match_index)
				return;
		}
	};

	iterate_plan(child);
	return match_index;
}

unique_ptr<LogicalOperator> IRConverter::ConstructDuckdbPlan(
    SimplestStmt *postgres_plan_pointer, unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
    const unordered_map<int, int> &pg_duckdb_table_idx, std::vector<unique_ptr<Expression>> &expr_vec) {
	std::function<unique_ptr<LogicalOperator>(SimplestStmt * postgres_plan_pointer)> iterate_plan;
	iterate_plan = [&iterate_plan, &table_map, pg_duckdb_table_idx, &expr_vec,
	                this](SimplestStmt *postgres_plan_pointer) -> unique_ptr<LogicalOperator> {
		unique_ptr<LogicalOperator> left_child, right_child;
		if (postgres_plan_pointer->children.size() > 0) {
			left_child = iterate_plan(postgres_plan_pointer->children[0].get());
			if (postgres_plan_pointer->children.size() == 2)
				right_child = iterate_plan(postgres_plan_pointer->children[1].get());
		}
		switch (postgres_plan_pointer->GetNodeType()) {
		case JoinNode: {
			// get info from postgres_join and construct duckdb_join
			auto postgres_join = dynamic_cast<SimplestJoin *>(postgres_plan_pointer);
			auto duckdb_join =
			    ConstructDuckdbJoin(postgres_join, std::move(left_child), std::move(right_child), pg_duckdb_table_idx);
			return unique_ptr_cast<LogicalComparisonJoin, LogicalOperator>(std::move(duckdb_join));
		}
		case FilterNode:
			Printer::Print("Doesn't support FilterNode yet!");
			return left_child;
		case HashNode:
			// todo: check if HashNode really doesn't have extra info
			return left_child;
		case ScanNode: {
			auto postgres_scan = dynamic_cast<SimplestScan *>(postgres_plan_pointer);
			// get scan node from table_map
			auto duckdb_scan = std::move(table_map[postgres_scan->GetTableName()]);
#ifdef DEBUG
			D_ASSERT(0 == column_idx_mapping.count(duckdb_scan->table_index));
#endif
			auto attr_table_idx = duckdb_scan->table_index;
			column_idx_mapping[attr_table_idx] = duckdb_scan->column_ids;
			unique_ptr<LogicalOperator> logical_get =
			    unique_ptr_cast<LogicalGet, LogicalOperator>(std::move(duckdb_scan));

			// check if it's necessary to add FILTER by the `qual_vec`
			vector<unique_ptr<Expression>> filter_expressions;
			for (const auto &qual : postgres_scan->qual_vec) {
				// currently we get filter expressions from duckdb plan
				// todo: construct filter expressions from postgres info
				// refactor to a standalone function
				for (auto it = expr_vec.begin(); it != expr_vec.end();) {
					auto &expr = *it;
					bool find_filter_expr = CheckExprExist(expr, attr_table_idx);
					if (find_filter_expr) {
						filter_expressions.emplace_back(std::move(expr));
						it = expr_vec.erase(it);
					} else {
						it++;
					}
				}
			}
			if (!filter_expressions.empty()) {
				auto scan_filter = make_uniq<LogicalFilter>();
				scan_filter->expressions = std::move(filter_expressions);
				scan_filter->AddChild(std::move(logical_get));
				return unique_ptr_cast<LogicalFilter, LogicalOperator>(std::move(scan_filter));
			} else {
				return logical_get;
			}
		}
		case SortNode: {
			auto postgres_sort = dynamic_cast<SimplestSort *>(postgres_plan_pointer);
			vector<BoundOrderByNode> orders;
			auto &target_list = postgres_sort->target_list;
			D_ASSERT(!target_list.empty());
			for (const auto &order_struct : postgres_sort->GetOrderStructVec()) {
				auto duckdb_logical_type = ConvertVarType(target_list[order_struct.sort_col_idx - 1]->GetType());
				auto expr = make_uniq<BoundConstantExpression>(Value(duckdb_logical_type));
				auto order_type = ConvertOrderType(order_struct.order_type);
				auto is_nulls_first =
				    order_struct.nulls_first ? OrderByNullType::NULLS_FIRST : OrderByNullType::NULLS_LAST;
				orders.emplace_back(BoundOrderByNode(order_type, is_nulls_first, std::move(expr)));
			}
			unique_ptr<LogicalOrder> duckdb_order = make_uniq<LogicalOrder>(std::move(orders));
			duckdb_order->children.push_back(std::move(left_child));
			return unique_ptr_cast<LogicalOrder, LogicalOperator>(std::move(duckdb_order));
		}
		default:
			return unique_ptr<LogicalOperator>();
		}
	};

	auto new_duckdb_plan = iterate_plan(postgres_plan_pointer);

	return new_duckdb_plan;
}

ExpressionType IRConverter::ConvertCompType(SimplestExprType type) {
	switch (type) {
	case Equal:
		return ExpressionType::COMPARE_EQUAL;
	case LessThan:
		return ExpressionType::COMPARE_LESSTHAN;
	case GreaterThan:
		return ExpressionType::COMPARE_GREATERTHAN;
	case LessEqual:
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	case GreaterEqual:
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	case NotEqual:
		return ExpressionType::COMPARE_NOTEQUAL;
	default:
		Printer::Print("Invalid postgres comparison type!");
		return ExpressionType::INVALID;
	}
}

LogicalType IRConverter::ConvertVarType(SimplestVarType type) {
	switch (type) {
	case IntVar:
		return LogicalType(LogicalTypeId::INTEGER);
	case FloatVar:
		return LogicalType(LogicalTypeId::FLOAT);
	case StringVar:
		return LogicalType(LogicalTypeId::STRING_LITERAL);
	default:
		Printer::Print("Invalid postgres var type!");
		return LogicalType(LogicalTypeId::INVALID);
	}
}

OrderType IRConverter::ConvertOrderType(SimplestExprType type) {
	switch (type) {
	case InvalidExprType:
		Printer::Print("Invalid Order Type!!!");
		return OrderType::INVALID;
	case LessThan:
		return OrderType::ASCENDING;
	case GreaterThan:
		return OrderType::DESCENDING;
	default:
		Printer::Print("Doesn't support order type " + std::to_string(type) + " yet!");
		exit(-1);
	}
}

void IRConverter::SetAttrName(unique_ptr<SimplestAttr> &attr, const std::deque<table_str> &table_col_names) {
	auto col_index = attr->GetColumnIndex();
#ifdef DEBUG
	D_ASSERT(table_col_names[attr->GetTableIndex() - 1].size() == 1);
#endif
	auto col_name = table_col_names[attr->GetTableIndex() - 1].begin()->second[col_index - 1]->GetLiteralValue();
	attr->SetColumnName(col_name);
}

void IRConverter::SetAttrVecName(std::vector<unique_ptr<SimplestAttr>> &attr_vec,
                                 const std::deque<table_str> &table_col_names) {
	for (auto &attr_var_node : attr_vec) {
		SetAttrName(attr_var_node, table_col_names);
	}
}

void IRConverter::SetExprName(unique_ptr<SimplestExpr> &expr, const std::deque<table_str> &table_col_names) {
	if (VarConstComparisonNode == expr->GetNodeType()) {
		auto &var_const_comp = expr->Cast<SimplestVarConstComparison>();
		auto &expr_attr = var_const_comp.attr;
		SetAttrName(expr_attr, table_col_names);
	} else if (IsNullExprNode == expr->GetNodeType()) {
		auto &is_null_expr = expr->Cast<SimplestIsNullExpr>();
		auto &expr_attr = is_null_expr.attr;
		SetAttrName(expr_attr, table_col_names);
	} else if (LogicalExprNode == expr->GetNodeType()) {
		auto &logical_expr = expr->Cast<SimplestLogicalExpr>();
		auto &left_expr = logical_expr.left_expr;
		SetExprName(left_expr, table_col_names);
		auto &right_expr = logical_expr.right_expr;
		SetExprName(right_expr, table_col_names);
	} else {
		Printer::Print("Doesn't support " + std::to_string((int)expr->GetNodeType()) + " yet!");
		exit(-1);
	}
}

void IRConverter::SetExprVecName(std::vector<unique_ptr<SimplestExpr>> &expr_vec,
                                 const std::deque<table_str> &table_col_names) {

	for (auto &expr : expr_vec) {
		SetExprName(expr, table_col_names);
	}
}

void IRConverter::SetExprVecName(std::vector<unique_ptr<SimplestVarComparison>> &comp_vec,
                                 const std::deque<table_str> &table_col_names) {
	for (auto &comp : comp_vec) {
		auto &left_attr = comp->left_attr;
		SetAttrName(left_attr, table_col_names);

		auto &right_attr = comp->right_attr;
		SetAttrName(right_attr, table_col_names);
	}
}

unordered_map<int, int>
IRConverter::MatchTableIndex(const unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
                             const std::deque<table_str> &table_col_names) {
	unordered_map<int, int> pg_duckdb_table_mapping;
	for (size_t i = 0; i < table_col_names.size(); i++) {
		auto &table_col = std::move(table_col_names[i]);
#ifdef DEBUG
		D_ASSERT(table_col.size() == 1);
#endif
		auto find_table = table_map.find(table_col.begin()->first);
		if (find_table != table_map.end()) {
			pg_duckdb_table_mapping[i + 1] = find_table->second->table_index;
		} else {
			Printer::Print("Error! Couldn't find table \"" + table_col.begin()->first + "\" in duckdb plan");
		}
	}

	return pg_duckdb_table_mapping;
}

void IRConverter::AddTableColumnName(unique_ptr<SimplestStmt> &postgres_plan,
                                     const std::deque<table_str> &table_col_names) {
	std::function<void(unique_ptr<SimplestStmt> & postgres_plan)> iterate_plan;
	iterate_plan = [&table_col_names, &iterate_plan, this](unique_ptr<SimplestStmt> &postgres_plan) {
		// set col attr names in `target_list`
		SetAttrVecName(postgres_plan->target_list, table_col_names);
		SetExprVecName(postgres_plan->qual_vec, table_col_names);

		if (ScanNode == postgres_plan->GetNodeType()) {
			// set scan_node's table_name
			auto &scan_node = postgres_plan->Cast<SimplestScan>();
#ifdef DEBUG
			D_ASSERT(table_col_names[scan_node.GetTableIndex() - 1].size() == 1);
#endif
			auto table_name = table_col_names[scan_node.GetTableIndex() - 1].begin()->first;
			scan_node.SetTableName(table_name);
		} else if (HashNode == postgres_plan->GetNodeType()) {
			// hash_node's `hash_keys` has attr
			auto &hash_node = postgres_plan->Cast<SimplestHash>();
			SetAttrVecName(hash_node.hash_keys, table_col_names);
		} else if (JoinNode == postgres_plan->GetNodeType()) {
			// join condition has attr
			auto &join_node = postgres_plan->Cast<SimplestJoin>();
			SetExprVecName(join_node.join_conditions, table_col_names);
		} else if (FilterNode == postgres_plan->GetNodeType()) {
			// filter condition has attr
			auto &filter_node = postgres_plan->Cast<SimplestFilter>();
			SetExprVecName(filter_node.filter_conditions, table_col_names);
		}

		for (auto &child : postgres_plan->children) {
			iterate_plan(child);
		}
	};

	iterate_plan(postgres_plan);
}
bool IRConverter::CheckExprExist(const unique_ptr<Expression> &expr, idx_t attr_table_idx) {
	bool find_filter_expr = false;
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
	case ExpressionClass::BOUND_CONSTANT:
		break;
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &input_ref = expr->Cast<BoundColumnRefExpression>();
		if (input_ref.binding.table_index == attr_table_idx) {
			find_filter_expr = true;
		}
		break;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &bound_between_expr = expr->Cast<BoundBetweenExpression>();
		auto &expr_input = bound_between_expr.input;
		find_filter_expr = CheckExprExist(expr_input, attr_table_idx);
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &bound_func_expr = expr->Cast<BoundFunctionExpression>();
		for (const auto &child : bound_func_expr.children) {
			find_filter_expr = CheckExprExist(child, attr_table_idx);
			if (find_filter_expr)
				break;
		}
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &bound_comp_expr = expr->Cast<BoundComparisonExpression>();
		auto &left_expr = bound_comp_expr.left;
		find_filter_expr = CheckExprExist(left_expr, attr_table_idx);
		D_ASSERT(ExpressionType::VALUE_CONSTANT == bound_comp_expr.right->GetExpressionType());
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &bound_conjection_expr = expr->Cast<BoundConjunctionExpression>();
		for (const auto &child : bound_conjection_expr.children) {
			find_filter_expr = CheckExprExist(child, attr_table_idx);
			if (find_filter_expr)
				break;
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &bound_op_expr = expr->Cast<BoundOperatorExpression>();
		for (const auto &child : bound_op_expr.children) {
			find_filter_expr = CheckExprExist(child, attr_table_idx);
			if (find_filter_expr)
				break;
		}
		break;
	}
	default:
		Printer::Print("Doesn't support " + std::to_string((int)expr->GetExpressionClass()) + " yet!");
		exit(-1);
	}

	return find_filter_expr;
}
} // namespace duckdb