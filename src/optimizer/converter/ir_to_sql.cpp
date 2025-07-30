#include "duckdb/optimizer/converter/ir_to_sql.h"

namespace duckdb {

std::string IRToSQLConverter::LogicalPlanToSQL(const unique_ptr<SimplestStmt> &plan) {
	std::string sql_code;
#ifdef DEBUG
	D_ASSERT(nullptr != plan);
#endif

	std::string prefix_string;
	GenerateSQL(plan);

	sql_code = "SELECT ";
	for (auto select : select_field) {
		select += ", ";
		sql_code += select;
	}
	sql_code.erase(sql_code.size() - 2);

	sql_code += "\nFROM ";
	for (auto table_name : table_names) {
		table_name.second += ", ";
		sql_code += table_name.second;
	}
	sql_code.erase(sql_code.size() - 2);

	sql_code += "\nWHERE ";
	for (auto filter : filter_field) {
		filter += " AND ";
		sql_code += filter;
	}
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
		for (const auto &target : proj_op.target_list) {
			auto table_name = table_names[target->GetTableIndex()];
			select_field.emplace_back(table_name + "." + target->GetColumnName());
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
#ifdef DEBUG
			D_ASSERT(SimplestNodeType::VarConstComparisonNode == qual->GetNodeType());
#endif
			auto &var_const_comp = qual->Cast<SimplestVarConstComparison>();
			auto expr_type = var_const_comp.GetSimplestExprType();
			switch (expr_type) {
			case TextLike: {
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
	case SimplestNodeType::ScanNode: {
		auto &scan_op = op->Cast<SimplestScan>();
		table_names.emplace(scan_op.GetTableIndex(), scan_op.GetTableName());
		break;
	}
	default:
		Printer::Print(StringUtil::Format("Do not support yet, op->type:  %d", op->GetNodeType()));
		D_ASSERT(false);
	}
}
} // namespace duckdb