#include "duckdb/optimizer/converter/ir_to_sql.h"

namespace duckdb {

std::string IRToSQLConverter::LogicalPlanToSQL(const unique_ptr<SimplestStmt> &plan) {
	std::string sql_code;
#ifdef DEBUG
	D_ASSERT(nullptr != plan);
#endif

	GenerateSQL(plan);

	sql_code = "SELECT ";
	for (auto select : select_field) {
		select += ", ";
		sql_code += select;
	}
	if (!select_field.empty())
		sql_code.erase(sql_code.size() - 2);

	sql_code += "\nFROM ";
	for (auto table_name : table_names) {
		table_name.second += ", ";
		sql_code += table_name.second;
	}
	if (!table_names.empty())
		sql_code.erase(sql_code.size() - 2);

	sql_code += "\nWHERE ";
	for (auto filter : filter_field) {
		filter += " AND ";
		sql_code += filter;
	}
	if (!filter_field.empty())
		sql_code.erase(sql_code.size() - 5);
	for (auto join : join_field) {
		join += " AND ";
		sql_code += join;
	}
	if (!join_field.empty())
		sql_code.erase(sql_code.size() - 5);

	sql_code += ";";
#ifdef DEBUG
	Printer::Print(StringUtil::Format("current SQL code is:\n%s", sql_code));
#endif
	return sql_code;
}

void IRToSQLConverter::GenerateSQL(const unique_ptr<SimplestStmt> &op) {
	std::string sql_code;
	if (op->children.size() > 0) {
		GenerateSQL(op->children[0]);
		if (op->children.size() == 2)
			GenerateSQL(op->children[1]);
	}

	switch (op->GetNodeType()) {
	case SimplestNodeType::ProjectionNode: {
		auto &proj_op = op->Cast<SimplestProjection>();
#ifdef DEBUG
		D_ASSERT(!proj_op.target_list.empty());
#endif
		// `SELECT table_name.$target_list`
		for (size_t idx = 0; idx < proj_op.target_list.size(); idx++) {
			auto &target = proj_op.target_list[idx];
			auto target_table_index = target->GetTableIndex();
			// for DuckDB with agg
			if (table_names.find(target_table_index) == table_names.end()) {
				auto &child_op = proj_op.children[0];
				if (SimplestNodeType::AggregateNode == child_op->GetNodeType()) {
					auto &agg_op = child_op->Cast<SimplestAggregate>();
					if (target_table_index == agg_op.GetAggIndex()) {
						std::string agg_fn_type = TranslateSimplestAggFnType(agg_op.agg_fns[idx].second);
						auto table_name = table_names[agg_op.agg_fns[idx].first->GetTableIndex()];
						std::string select_str = table_name + "." + agg_op.agg_fns[idx].first->GetColumnName();
						select_str = agg_fn_type + "(" + select_str + ")";
						select_field.emplace_back(select_str);
					} else {
						// todo
						Printer::Print("TODO!");
						D_ASSERT(false);
					}
				} else {
					Printer::Print(
					    StringUtil::Format("Do not support yet, child_op node type:  %s", child_op->GetNodeType()));
					D_ASSERT(false);
				}
			} else {
				// for the others
				auto table_name = table_names[target_table_index];
				std::string select_str = table_name + "." + target->GetColumnName();
				auto find_select_str = agg_field.find(agg_field_key(target_table_index, target->GetColumnIndex()));
				if (find_select_str != agg_field.end()) {
					select_str = find_select_str->second + "(" + select_str + ")";
				}
				select_field.emplace_back(select_str);
			}
		}
		break;
	}
	case SimplestNodeType::AggregateNode: {
		auto &agg_op = op->Cast<SimplestAggregate>();
		for (const auto &agg_fn : agg_op.agg_fns) {
			agg_field.emplace(
			    std::make_pair(agg_field_key(agg_fn.first->GetTableIndex(), agg_fn.first->GetColumnIndex()),
			                   TranslateSimplestAggFnType(agg_fn.second)));
		}
		break;
	}
	case SimplestNodeType::FilterNode: {
		auto &filter_op = op->Cast<SimplestFilter>();
#ifdef DEBUG
		D_ASSERT(!filter_op.qual_vec.empty());
#endif
		// `WHERE `
		for (const auto &qual : filter_op.qual_vec) {
			auto &simplest_expr = qual->Cast<SimplestExpr>();
			auto expr_type = simplest_expr.GetSimplestExprType();

			switch (expr_type) {
			case TextLike: {
#ifdef DEBUG
				D_ASSERT(SimplestNodeType::VarConstComparisonNode == qual->GetNodeType());
#endif
				auto &var_const_comp = qual->Cast<SimplestVarConstComparison>();
				auto &var_attr = var_const_comp.attr;
				auto table_name = table_names[var_attr->GetTableIndex()];
				auto filter_str = table_name + "." + var_attr->GetColumnName();
				filter_str += " LIKE '";
				auto &const_attr = var_const_comp.const_var;
				// todo: determine type
				filter_str += const_attr->GetStringValue();
				filter_str += "'";
				filter_field.emplace_back(filter_str);
				break;
			}
			case SingleAttr: {
				// the `filter_field` should be collected in the child MARK join node
#ifdef DEBUG
				D_ASSERT(SimplestNodeType::JoinNode == filter_op.children[0]->GetNodeType());
				auto &join_child_op = filter_op.children[0]->Cast<SimplestJoin>();
				D_ASSERT(SimplestJoinType::Mark == join_child_op.GetSimplestJoinType());
#endif
				break;
			}
			case InvalidExprType:
				Printer::Print("Invalid expression type!");
				D_ASSERT(false);
				break;
			default:
				Printer::Print(StringUtil::Format("Do not support yet, expr->type:  %d", expr_type));
				D_ASSERT(false);
			}
		}
		break;
	}
	case SimplestNodeType::JoinNode: {
		auto &join_op = op->Cast<SimplestJoin>();
		auto &left_child = join_op.children[0];
		auto &right_child = join_op.children[1];
		auto &conditions = join_op.join_conditions;
		auto join_type = join_op.GetSimplestJoinType();
		switch (join_type) {
		case Inner: {
			for (const auto &cond : conditions) {
				auto &var_comp = cond->Cast<SimplestVarComparison>();
				auto &left_var_attr = var_comp.left_attr;
				auto left_table_name = table_names[left_var_attr->GetTableIndex()];
				auto join_str = left_table_name + "." + left_var_attr->GetColumnName();
				join_str += " = ";
				auto &right_var_attr = var_comp.right_attr;
				auto right_table_name = table_names[right_var_attr->GetTableIndex()];
				join_str += right_table_name + "." + right_var_attr->GetColumnName();
				join_field.emplace_back(join_str);
			}
			break;
		}
		case Mark: {
			// todo: check if it is always be a `IN` claude
			for (const auto &cond : conditions) {
				auto &var_comp = cond->Cast<SimplestVarComparison>();
				auto &left_var_attr = var_comp.left_attr;
				auto table_name = table_names[left_var_attr->GetTableIndex()];
				auto filter_str = table_name + "." + left_var_attr->GetColumnName();
				filter_str += " IN ";
				filter_str += "(";
				auto &right_var_attr = var_comp.right_attr;
				auto chunk_contents_str = chunk_contents[right_var_attr->GetTableIndex()];
				for (const auto &content : chunk_contents_str) {
					std::string content_str = "'" + content + "', ";
					filter_str += content_str;
				}
				filter_str.erase(filter_str.size() - 2);
				filter_str += ")";
				filter_field.emplace_back(filter_str);
			}
			break;
		}
		default:
			Printer::Print(StringUtil::Format("Do not support yet, join_type:  %d", join_type));
			D_ASSERT(false);
		}
		break;
	}
	case SimplestNodeType::ScanNode: {
		auto &scan_op = op->Cast<SimplestScan>();
		table_names.emplace(scan_op.GetTableIndex(), scan_op.GetTableName());
		for (const auto &qual : scan_op.qual_vec) {
			CollectScanFilter(qual);
		}
		break;
	}
	case SimplestNodeType::ChunkNode: {
		auto &chunk_op = op->Cast<SimplestChunk>();
		chunk_contents[chunk_op.GetTableIndex()] = chunk_op.GetContents();
		break;
	}
	case SimplestNodeType::HashNode:
	case SimplestNodeType::CrossProductNode:
		break;
	default:
		Printer::Print(StringUtil::Format("Do not support yet, op->type:  %d", op->GetNodeType()));
		D_ASSERT(false);
	}
}

std::string IRToSQLConverter::TranslateSimplestAggFnType(SimplestAggFnType agg_fn_type) {
	std::string agg_fn_type_str;
	switch (agg_fn_type) {
	case SimplestAggFnType::InvalidAggType:
		Printer::Print("Invalid expression type!");
		D_ASSERT(false);
		break;
	case SimplestAggFnType::Min:
		agg_fn_type_str = "min";
		break;
	case SimplestAggFnType::Max:
		agg_fn_type_str = "max";
		break;
	case SimplestAggFnType::Sum:
		agg_fn_type_str = "sum";
		break;
	case SimplestAggFnType::Average:
		agg_fn_type_str = "avg";
		break;
	}

	return agg_fn_type_str;
}

std::string IRToSQLConverter::CollectScanFilter(const unique_ptr<SimplestExpr> &qual_expr) {
	std::string ret_str;
	switch (qual_expr->GetNodeType()) {
	case SimplestNodeType::VarConstComparisonNode: {
		auto &var_const_comp = qual_expr->Cast<SimplestVarConstComparison>();
		auto &var_attr = var_const_comp.attr;
		auto table_name = table_names[var_attr->GetTableIndex()];
		ret_str = table_name + "." + var_attr->GetColumnName();
		std::string appendix_str = "";
		switch (var_const_comp.GetSimplestExprType()) {
		case SimplestExprType::LessThan:
			ret_str += " < ";
			break;
		case SimplestExprType::LessEqual:
			ret_str += " <= ";
			break;
		case SimplestExprType::GreaterThan:
			ret_str += " > ";
			break;
		case SimplestExprType::GreaterEqual:
			ret_str += " >= ";
			break;
		case SimplestExprType::TextLike:
			ret_str += " LIKE '";
			appendix_str = "'";
			break;
		case SimplestExprType::TEXT_Not_LIKE:
			ret_str += " NOT LIKE '";
			appendix_str = "'";
			break;
		default:
			Printer::Print(StringUtil::Format("Do not support yet, var_const_comp->type:  %d",
			                                  var_const_comp.GetSimplestExprType()));
			D_ASSERT(false);
		}
		auto &const_attr = var_const_comp.const_var;
		// todo: determine type
		ret_str += const_attr->GetStringValue();
		ret_str += appendix_str;
		return ret_str;
	}
	case SimplestNodeType::LogicalExprNode: {
		auto &logical_expr = qual_expr->Cast<SimplestLogicalExpr>();
		std::string left_expr_str, right_expr_str;
		if (SimplestLogicalOp::LogicalNot != logical_expr.GetLogicalOp()) {
			auto &left_expr = logical_expr.left_expr;
			left_expr_str = CollectScanFilter(left_expr);
		}
		auto &right_expr = logical_expr.right_expr;
		right_expr_str = CollectScanFilter(right_expr);
		ret_str = "(";
		switch (logical_expr.GetLogicalOp()) {
		case SimplestLogicalOp::InvalidLogicalOp:
			Printer::Print("Invalid logical expr!");
			D_ASSERT(false);
			break;
		case SimplestLogicalOp::LogicalAnd:
			ret_str += left_expr_str;
			ret_str += " AND ";
			ret_str += right_expr_str;
			ret_str += ")";
			return ret_str;
		case SimplestLogicalOp::LogicalOr:
			ret_str += left_expr_str;
			ret_str += " OR ";
			ret_str += right_expr_str;
			ret_str += ")";
			return ret_str;
		case SimplestLogicalOp::LogicalNot:
			// todo
			Printer::Print("Unimplemented LogicalNot yet!");
			D_ASSERT(false);
			break;
		default:
			Printer::Print(
			    StringUtil::Format("Do not support yet, logical_expr->type:  %d", logical_expr.GetLogicalOp()));
			D_ASSERT(false);
			break;
		}
		break;
	}
	case SimplestNodeType::IsNullExprNode: {
		auto &is_null_expr = qual_expr->Cast<SimplestIsNullExpr>();
		auto &var_attr = is_null_expr.attr;
		auto table_name = table_names[var_attr->GetTableIndex()];
		ret_str = table_name + "." + var_attr->GetColumnName();
		switch (is_null_expr.GetSimplestExprType()) {
		case SimplestExprType::InvalidExprType:
			Printer::Print("Invalid logical expr!");
			D_ASSERT(false);
			break;
		case SimplestExprType::NullType:
			ret_str += " IS NULL";
			return ret_str;
		case SimplestExprType::NonNullType:
			ret_str += " IS NOT NULL";
			return ret_str;
		default:
			Printer::Print(
			    StringUtil::Format("Do not support yet, is_null_expr->type:  %d", is_null_expr.GetSimplestExprType()));
			D_ASSERT(false);
			break;
		}
		break;
	}
	default:
		Printer::Print(StringUtil::Format("Do not support yet, qual_expr->type:  %d", qual_expr->GetNodeType()));
		D_ASSERT(false);
	}
	return ret_str;
}
} // namespace duckdb