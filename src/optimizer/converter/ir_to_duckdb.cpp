#include "duckdb/optimizer/converter/ir_to_duckdb.h"

namespace duckdb {
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

bool IRConverter::CheckCondIndex(const unique_ptr<Expression> &expr, const unique_ptr<LogicalOperator> &child) {
#ifdef DEBUG
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
		} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == duckdb_plan->type) {
			auto &column_get_node = duckdb_plan->Cast<LogicalColumnDataGet>();
			match_index = column_get_node.table_index == expr_table_idx;
			if (match_index)
				return;
		}
	};

	iterate_plan(child);
	return match_index;
}

std::pair<idx_t, idx_t>
IRConverter::ConvertTableColumnIndex(std::pair<unsigned int, unsigned int> postgres_table_column_pair,
                                     const unordered_map<int, int> &pg_duckdb_table_idx) {
	auto index_find = pg_duckdb_table_idx.find(postgres_table_column_pair.first);
#ifdef DEBUG
	D_ASSERT(index_find != pg_duckdb_table_idx.end());
#endif
	auto table_index = index_find->second;
	auto find_col_idx = std::find(column_idx_mapping[table_index].begin(), column_idx_mapping[table_index].end(),
	                              postgres_table_column_pair.second - 1);
#ifdef DEBUG
	D_ASSERT(find_col_idx != column_idx_mapping[table_index].end());
#endif
	auto column_index = find_col_idx - column_idx_mapping[table_index].begin();

	return std::make_pair(table_index, column_index);
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
		auto left_table_column_index = ConvertTableColumnIndex(
		    std::make_pair(left_pg_cond->GetTableIndex(), left_pg_cond->GetColumnIndex()), pg_duckdb_table_idx);
		cond.left = make_uniq<BoundColumnRefExpression>(
		    left_pg_cond->GetColumnName(), left_type,
		    ColumnBinding(left_table_column_index.first, left_table_column_index.second));

		auto &right_pg_cond = postgres_cond->right_attr;
		LogicalType right_type = ConvertVarType(right_pg_cond->GetType());
		auto right_table_column_index = ConvertTableColumnIndex(
		    std::make_pair(right_pg_cond->GetTableIndex(), right_pg_cond->GetColumnIndex()), pg_duckdb_table_idx);
		cond.right = make_uniq<BoundColumnRefExpression>(
		    right_pg_cond->GetColumnName(), right_type,
		    ColumnBinding(right_table_column_index.first, right_table_column_index.second));

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

vector<unique_ptr<Expression>> IRConverter::GetFilter_exprs(std::vector<unique_ptr<Expression>> &expr_vec,
                                                            idx_t table_idx) {
	vector<unique_ptr<Expression>> filter_expressions;

	for (auto it = expr_vec.begin(); it != expr_vec.end();) {
		auto &expr = *it;
		bool find_filter_expr = CheckExprExist(expr, table_idx);
		if (find_filter_expr) {
			filter_expressions.emplace_back(std::move(expr));
			it = expr_vec.erase(it);
		} else {
			it++;
		}
	}

	return filter_expressions;
}

unique_ptr<LogicalOperator> IRConverter::DealWithQual(unique_ptr<LogicalGet> logical_get,
                                                      std::vector<unique_ptr<Expression>> &expr_vec,
                                                      const std::vector<unique_ptr<SimplestExpr>> &qual_vec,
                                                      const unordered_map<int, int> &pg_duckdb_table_idx) {
	// check if it's necessary to add FILTER by the `qual_vec`
	vector<unique_ptr<Expression>> filter_expressions;
	auto attr_table_idx = logical_get->table_index;
	unique_ptr<LogicalOperator> ret;
	// todo: currently we get filter expressions from duckdb plan
	//  so some filters are generated by duckdb
	auto temp_filter_expressions = GetFilter_exprs(expr_vec, attr_table_idx);
	for (auto &temp_expr : temp_filter_expressions) {
		filter_expressions.emplace_back(std::move(temp_expr));
	}
	for (const auto &qual : qual_vec) {
		// todo: construct filter expressions from postgres info, aka generate unique_ptr<Expression>
	}
	if (!filter_expressions.empty()) {
		auto scan_filter = make_uniq<LogicalFilter>();
		scan_filter->expressions = std::move(filter_expressions);
		scan_filter->AddChild(std::move(logical_get));
		ret = unique_ptr_cast<LogicalFilter, LogicalOperator>(std::move(scan_filter));
	} else {
		ret = unique_ptr_cast<LogicalGet, LogicalOperator>(std::move(logical_get));
	}

	for (const auto &qual : qual_vec) {
		// check if it's a `IN` clause
		if (VarConstComparisonNode == qual->GetNodeType()) {
			auto &comp_node = qual->Cast<SimplestVarConstComparison>();
			if (StringVarArr == comp_node.const_var->GetType()) {
				auto str_vec = comp_node.const_var->GetStringVecValue();
				vector<LogicalType> return_types {LogicalType::VARCHAR};
				auto collection = make_uniq<ColumnDataCollection>(context, return_types);
				ColumnDataAppendState append_state;
				collection->InitializeAppend(append_state);
				DataChunk chunk;
				chunk.Initialize(context, return_types);
				// from in_clause_rewriter.cpp, we only generate data chunk for more than 6 strings
				if (str_vec.size() < 6)
					continue;
				for (idx_t column_idx = 0; column_idx < str_vec.size(); column_idx++) {
					Value value = Value(str_vec[column_idx]);
					idx_t index = chunk.size();
					chunk.SetCardinality(chunk.size() + 1);
					chunk.SetValue(0, index, value);
					if (chunk.size() == STANDARD_VECTOR_SIZE || column_idx + 1 == str_vec.size()) {
						// chunk full: append to chunk collection
						collection->Append(append_state, chunk);
						chunk.Reset();
					}
				}
				auto chunk_index = binder.GenerateTableIndex();
				auto data_chunk_node =
				    make_uniq<LogicalColumnDataGet>(chunk_index, return_types, std::move(collection));
				// construct JOIN with the SCAN node
				auto &attr_var = comp_node.attr;
				auto data_chunk_join = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
				data_chunk_join->mark_index = chunk_index;
				data_chunk_join->children.push_back(std::move(ret));
				data_chunk_join->children.push_back(std::move(data_chunk_node));
				JoinCondition cond;
				cond.comparison = ConvertCompType(qual->GetSimplestExprType());
				auto left_table_column_index = ConvertTableColumnIndex(
				    std::make_pair(attr_var->GetTableIndex(), attr_var->GetColumnIndex()), pg_duckdb_table_idx);
				cond.left = make_uniq<BoundColumnRefExpression>(
				    attr_var->GetColumnName(), ConvertVarType(attr_var->GetType()),
				    ColumnBinding(left_table_column_index.first, left_table_column_index.second));
				cond.right = make_uniq<BoundColumnRefExpression>("data_chunk", LogicalType::VARCHAR,
				                                                 ColumnBinding(chunk_index, 0));
				data_chunk_join->conditions.push_back(std::move(cond));
				// construct FILTER to select
				unique_ptr<Expression> filter_expr = make_uniq<BoundColumnRefExpression>(
				    "IN (...)", LogicalType::BOOLEAN, ColumnBinding(chunk_index, 0));
				auto data_chunk_filter = make_uniq<LogicalFilter>();
				filter_expressions.clear();
				filter_expressions.emplace_back(std::move(filter_expr));
				data_chunk_filter->expressions = std::move(filter_expressions);
				data_chunk_filter->AddChild(std::move(data_chunk_join));
				ret = unique_ptr_cast<LogicalFilter, LogicalOperator>(std::move(data_chunk_filter));
			}
			// todo: maybe have other types of DATA CHUNK?
		}
	}

	return ret;
}

unique_ptr<LogicalOperator> IRConverter::ConstructDuckdbPlan(
    SimplestStmt *postgres_plan_pointer, unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
    const unordered_map<int, int> &pg_duckdb_table_idx, std::vector<unique_ptr<Expression>> &expr_vec,
    unique_ptr<ColumnDataCollection> subquery_result, idx_t result_chunk_idx) {
	std::function<unique_ptr<LogicalOperator>(SimplestStmt * postgres_plan_pointer)> iterate_plan;
	iterate_plan = [&iterate_plan, &table_map, pg_duckdb_table_idx, &expr_vec, &subquery_result, result_chunk_idx,
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
			// it can be a `temp%` table generated by QuerySplit, skip it
			if (postgres_scan->GetTableName().find("temp") != std::string::npos) {
				int64_t chunk_size = subquery_result->Count();
				auto chunk_scan = make_uniq<LogicalColumnDataGet>(result_chunk_idx, subquery_result->Types(),
				                                                  std::move(subquery_result));
				chunk_scan->estimated_cardinality = chunk_size;
				chunk_scan->has_estimated_cardinality = true;
				// update column_ids
				for (size_t idx = 0; idx < postgres_scan->target_list.size(); idx++) {
					column_idx_mapping[result_chunk_idx].emplace_back(idx);
				}
				return unique_ptr_cast<LogicalColumnDataGet, LogicalOperator>(std::move(chunk_scan));
			}

			auto find_table = table_map.find(postgres_scan->GetTableName());
			if (find_table == table_map.end()) {
				Printer::Print("Error! Couldn't find table \"" + postgres_scan->GetTableName() + "\" in duckdb plan");
			}
			unique_ptr<LogicalGet> duckdb_scan = std::move(find_table->second);
#ifdef DEBUG
			D_ASSERT(0 == column_idx_mapping.count(duckdb_scan->table_index));
#endif
			auto attr_table_idx = duckdb_scan->table_index;
			column_idx_mapping[attr_table_idx] = duckdb_scan->column_ids;
			unique_ptr<LogicalOperator> logical_op_ret =
			    DealWithQual(std::move(duckdb_scan), expr_vec, postgres_scan->qual_vec, pg_duckdb_table_idx);
			return logical_op_ret;
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
	case TextLike:
		return ExpressionType::COMPARE_IN;
	case TEXT_Not_LIKE:
		return ExpressionType::COMPARE_NOT_IN;
	default:
		Printer::Print("Invalid postgres comparison type!");
		return ExpressionType::INVALID;
	}
}

LogicalType IRConverter::ConvertVarType(SimplestVarType type) {
	switch (type) {
	case BoolVar:
		return LogicalType(LogicalTypeId::BOOLEAN);
	case IntVar:
		return LogicalType(LogicalTypeId::INTEGER);
	case FloatVar:
		return LogicalType(LogicalTypeId::FLOAT);
	case StringVar:
		return LogicalType(LogicalTypeId::VARCHAR);
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
	} else if (VarComparisonNode == expr->GetNodeType()) {
		auto &var_comp = expr->Cast<SimplestVarComparison>();
		auto &left_attr = var_comp.left_attr;
		SetAttrName(left_attr, table_col_names);
		auto &right_attr = var_comp.right_attr;
		SetAttrName(right_attr, table_col_names);
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
                             const std::deque<table_str> &table_col_names, idx_t result_chunk_idx) {
	unordered_map<int, int> pg_duckdb_table_mapping;
	for (size_t i = 0; i < table_col_names.size(); i++) {
		auto &table_col = std::move(table_col_names[i]);
#ifdef DEBUG
		D_ASSERT(table_col.size() == 1);
#endif
		// it can be a `temp%` table generated by QuerySplit
		if (table_col.begin()->first.find("temp") != std::string::npos) {
			pg_duckdb_table_mapping[i + 1] = result_chunk_idx;
			continue;
		}

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
		bool find_filter_expr_in_left = CheckExprExist(left_expr, attr_table_idx);
		auto &right_expr = bound_comp_expr.right;
		bool find_filter_expr_in_right = CheckExprExist(right_expr, attr_table_idx);
		find_filter_expr = find_filter_expr_in_left || find_filter_expr_in_right;
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

// todo: refactor and reuse `SubqueryPreparer::GenerateProjHead`
unique_ptr<LogicalOperator> IRConverter::GenerateProjHead(const unique_ptr<LogicalOperator> &origin_dd_plan,
                                                          unique_ptr<LogicalOperator> new_dd_plan,
                                                          const unique_ptr<SimplestStmt> &pg_simplest_stmt,
                                                          const unordered_map<int, int> &pg_duckdb_table_idx) {
	// get target_list from `new_dd_plan`
	auto &target_list = pg_simplest_stmt->target_list;

	auto new_plan = origin_dd_plan->Copy(context);
	new_plan->children.clear();
	new_plan->AddChild(std::move(new_dd_plan));
	new_plan->expressions.clear();

	// generate expressions from target_list
	vector<unique_ptr<Expression>> new_exprs;
	for (const auto &target : target_list) {
		auto table_column_index = ConvertTableColumnIndex(
		    std::make_pair(target->GetTableIndex(), target->GetColumnIndex()), pg_duckdb_table_idx);
		auto table_index = table_column_index.first;
		auto column_index = table_column_index.second;
		auto column_name = target->GetColumnName();
		auto return_type = ConvertVarType(target->GetType());
		ColumnBinding binding = ColumnBinding(table_index, column_index);
		auto col_ref_select_expr = make_uniq<BoundColumnRefExpression>(column_name, return_type, binding, 0);
		new_exprs.emplace_back(std::move(col_ref_select_expr));
	}

	new_plan->expressions = std::move(new_exprs);
	return new_plan;
}

// todo: move this to subquery_preparer
std::vector<TableExpr>
IRConverter::GetTableExprFromTargetList(const std::vector<unique_ptr<SimplestAttr>> &pg_target_list,
                                        const unordered_map<int, int> &pg_duckdb_table_idx) {
	std::vector<TableExpr> table_expr_vec;

	// if duckdb find two expr are the same, here it will merge to one
	// e.g. Proj {[7.0], [7.1], [7.1]}
	//      Agg {min(1.1), min(4.1)}
	std::unordered_set<TableExpr, TableExprHash> check_repeat;
	for (const auto &target : pg_target_list) {
		auto table_column_index = ConvertTableColumnIndex(
		    std::make_pair(target->GetTableIndex(), target->GetColumnIndex()), pg_duckdb_table_idx);
		auto table_index = table_column_index.first;
		auto column_index = table_column_index.second;
		auto column_name = target->GetColumnName();
		auto return_type = ConvertVarType(target->GetType());

		TableExpr table_expr {table_index, column_index, column_name, return_type};
		if (!check_repeat.count(table_expr)) {
			table_expr_vec.emplace_back(table_expr);
			check_repeat.emplace(table_expr);
		}
	}

	return table_expr_vec;
}
} // namespace duckdb