#include "duckdb/optimizer/query_split/query_split_util.h"

namespace duckdb {
typedef struct timespec timespec;
timespec diff(timespec start, timespec end) {
	timespec temp;
	if ((end.tv_nsec - start.tv_nsec) < 0) {
		temp.tv_sec = end.tv_sec - start.tv_sec - 1;
		temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec - start.tv_sec;
		temp.tv_nsec = end.tv_nsec - start.tv_nsec;
	}
	return temp;
}

timespec sum(timespec t1, timespec t2) {
	timespec temp;
	if (t1.tv_nsec + t2.tv_nsec >= 1000000000) {
		temp.tv_sec = t1.tv_sec + t2.tv_sec + 1;
		temp.tv_nsec = t1.tv_nsec + t2.tv_nsec - 1000000000;
	} else {
		temp.tv_sec = t1.tv_sec + t2.tv_sec;
		temp.tv_nsec = t1.tv_nsec + t2.tv_nsec;
	}
	return temp;
}

void printTimeSpec(timespec t, const char *prefix) {
	std::string str = prefix + std::to_string(t.tv_sec) + "." + std::to_string(t.tv_nsec) + " s";
	Printer::Print(str);
	//    printf("%s: %d.%09d\n", prefix, (int)t.tv_sec, (int)t.tv_nsec);
}

timespec tic() {
	timespec start_time;
	if (-1 == clock_gettime(CLOCK_REALTIME, &start_time)) {
		Printer::Print("Could not get clock time!");
		D_ASSERT(false);
	}
	return start_time;
}

void toc(timespec *start_time, const char *prefix) {
	timespec current_time;
	if (-1 == clock_gettime(CLOCK_REALTIME, &current_time)) {
		Printer::Print("Could not get clock time!");
		D_ASSERT(false);
	}
	printTimeSpec(diff(*start_time, current_time), prefix);
	*start_time = current_time;
}

std::chrono::high_resolution_clock::time_point chrono_tic() {
	return std::chrono::high_resolution_clock::now();
}

long chrono_toc(std::chrono::high_resolution_clock::time_point *start_time, const char *prefix, bool print) {
	auto current_time = std::chrono::high_resolution_clock::now();
	auto time_diff = std::chrono::duration_cast<std::chrono::microseconds>(current_time - *start_time).count();
	std::string str = prefix + std::to_string(time_diff) + " us";
	if (print)
		Printer::Print(str);
	*start_time = current_time;
	return time_diff;
}

void appendLineToFile(string filepath, string line) {
	std::ofstream file;
	// can't enable exception now because of gcc bug that raises ios_base::failure with useless message
	// file.exceptions(file.exceptions() | std::ios::failbit);
	file.open(filepath, std::ios::out | std::ios::app);
	if (file.fail())
		throw std::ios_base::failure(std::strerror(errno));

	// make sure write fails with exception if something is wrong
	file.exceptions(file.exceptions() | std::ios::failbit | std::ifstream::badbit);

	file << line << std::endl;
}

const TableExpr GetConstTableExpr(const unique_ptr<Expression> &expr) {
	switch (expr->type) {
	case ExpressionType::BOUND_COLUMN_REF: {
		auto &bound_col_ref_expr = expr->Cast<BoundColumnRefExpression>();
		return TableExpr {bound_col_ref_expr.binding.table_index, bound_col_ref_expr.binding.column_index,
		                  bound_col_ref_expr.alias, bound_col_ref_expr.return_type};
	}
	case ExpressionType::OPERATOR_CAST:
		return GetConstTableExpr(expr->Cast<BoundCastExpression>().child);
	case ExpressionType::BOUND_FUNCTION: {
		auto &bound_func_expr = expr->Cast<BoundFunctionExpression>();
#ifdef DEBUG
		D_ASSERT(2 == bound_func_expr.children.size());
#endif
		if (ExpressionType::VALUE_CONSTANT == bound_func_expr.children[0]->type) {
			return GetConstTableExpr(bound_func_expr.children[1]);
		} else if (ExpressionType::VALUE_CONSTANT == bound_func_expr.children[1]->type) {
			return GetConstTableExpr(bound_func_expr.children[0]);
		} else {
			auto left_idx = GetConstTableExpr(bound_func_expr.children[0]);
			auto right_idx = GetConstTableExpr(bound_func_expr.children[1]);
#ifdef DEBUG
			D_ASSERT(left_idx == right_idx);
#endif
			return left_idx;
		}
	}
	case ExpressionType::COMPARE_BETWEEN:
	case ExpressionType::COMPARE_NOT_BETWEEN: {
		auto &compare_expr = expr->Cast<BoundBetweenExpression>();
		return GetConstTableExpr(compare_expr.input);
	}
	default:
		Printer::Print("Doesn't support " + ExpressionTypeToString(expr->type) + " in GetConstTableExpr yet!");
		D_ASSERT(false);
	}
	D_ASSERT(false);
	return TableExpr {DConstants::INVALID_INDEX, DConstants::INVALID_INDEX, "", LogicalType::INVALID};
}

ColumnBinding &GetRefColumnBinding(unique_ptr<Expression> &expr) {
	switch (expr->type) {
	case ExpressionType::BOUND_COLUMN_REF:
		return expr->Cast<BoundColumnRefExpression>().binding;
	case ExpressionType::OPERATOR_CAST:
		return GetRefColumnBinding(expr->Cast<BoundCastExpression>().child);
	case ExpressionType::BOUND_FUNCTION: {
		auto &bound_func_expr = expr->Cast<BoundFunctionExpression>();
#ifdef DEBUG
		D_ASSERT(2 == bound_func_expr.children.size());
#endif
		if (ExpressionType::VALUE_CONSTANT == bound_func_expr.children[0]->type) {
			return GetRefColumnBinding(bound_func_expr.children[1]);
		} else if (ExpressionType::VALUE_CONSTANT == bound_func_expr.children[1]->type) {
			return GetRefColumnBinding(bound_func_expr.children[0]);
		} else {
			auto &left_idx = GetRefColumnBinding(bound_func_expr.children[0]);
			auto &right_idx = GetRefColumnBinding(bound_func_expr.children[1]);
#ifdef DEBUG
			D_ASSERT(left_idx == right_idx);
#endif
			return left_idx;
		}
	}
	case ExpressionType::COMPARE_BETWEEN:
	case ExpressionType::COMPARE_NOT_BETWEEN: {
		auto &compare_expr = expr->Cast<BoundBetweenExpression>();
		return GetRefColumnBinding(compare_expr.input);
	}
	default:
		Printer::Print("Doesn't support " + ExpressionTypeToString(expr->type) + " in GetColumnBinding yet!");
		D_ASSERT(false);
	}
	D_ASSERT(false);
}

template <typename T>
void UpdateFunctionExpr(BoundFunctionExpression &function_expr, T &&func) {
	for (auto &func_child : function_expr.children) {
		UpdateExprs(func_child, func);
	}
}

template <typename T>
void UpdateCaseExpr(BoundCaseExpression &case_expr, T &&func) {
	for (auto &case_check : case_expr.case_checks) {
		UpdateExprs(case_check.when_expr, func);
		UpdateExprs(case_check.then_expr, func);
	}

	UpdateExprs(case_expr.else_expr, func);
}

template <typename T>
void UpdateComparisonExpr(BoundComparisonExpression &comparison_expr, T &&func) {
	auto &left_expr = comparison_expr.left;
	UpdateExprs(left_expr, func);

	auto &right_expr = comparison_expr.right;
	UpdateExprs(right_expr, func);
}

template <typename T>
void UpdateExprs(unique_ptr<Expression> &expr, T &&func) {
	switch (expr->type) {
	case ExpressionType::VALUE_CONSTANT:
		return;
	case ExpressionType::BOUND_FUNCTION:
		UpdateFunctionExpr(expr->Cast<BoundFunctionExpression>(), func);
		return;
	case ExpressionType::CASE_EXPR:
		UpdateCaseExpr(expr->Cast<BoundCaseExpression>(), func);
		return;
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		UpdateComparisonExpr(expr->Cast<BoundComparisonExpression>(), func);
		return;
	case ExpressionType::CONJUNCTION_OR:
	case ExpressionType::CONJUNCTION_AND: {
		auto &conjunction_expr = expr->Cast<BoundConjunctionExpression>();
		for (auto &child_expr : conjunction_expr.children) {
			UpdateExprs(child_expr, func);
		}
		return;
	}
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
	case ExpressionType::OPERATOR_NOT:
	case ExpressionType::OPERATOR_COALESCE: {
		auto &operator_expr = expr->Cast<BoundOperatorExpression>();
		for (auto &child_expr : operator_expr.children) {
			UpdateExprs(child_expr, func);
		}
		return;
	}
	default:
		break;
	}

	func(expr);
}

template <typename T>
void VisitFunctionExpr(const BoundFunctionExpression &function_expr, T &&func) {
	for (const auto &func_child : function_expr.children) {
		VisitExprs(func_child, func);
	}
}

template <typename T>
void VisitCaseExpr(const BoundCaseExpression &case_expr, T &&func) {
	for (const auto &case_check : case_expr.case_checks) {
		VisitExprs(case_check.when_expr, func);
		VisitExprs(case_check.then_expr, func);
	}

	VisitExprs(case_expr.else_expr, func);
}

template <typename T>
void VisitComparisonExpr(const BoundComparisonExpression &comparison_expr, T &&func) {
	auto &left_expr = comparison_expr.left;
	VisitExprs(left_expr, func);

	auto &right_expr = comparison_expr.right;
	VisitExprs(right_expr, func);
}

template <typename T>
void VisitExprs(const unique_ptr<Expression> &expr, T &&func) {
	switch (expr->type) {
	case ExpressionType::VALUE_CONSTANT:
		return;
	case ExpressionType::BOUND_FUNCTION:
		VisitFunctionExpr(expr->Cast<BoundFunctionExpression>(), func);
		return;
	case ExpressionType::CASE_EXPR:
		VisitCaseExpr(expr->Cast<BoundCaseExpression>(), func);
		return;
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VisitComparisonExpr(expr->Cast<BoundComparisonExpression>(), func);
		return;
	case ExpressionType::CONJUNCTION_OR:
	case ExpressionType::CONJUNCTION_AND: {
		auto &conjunction_expr = expr->Cast<BoundConjunctionExpression>();
		for (const auto &child_expr : conjunction_expr.children) {
			VisitExprs(child_expr, func);
		}
		return;
	}
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
	case ExpressionType::OPERATOR_NOT:
	case ExpressionType::OPERATOR_COALESCE: {
		auto &operator_expr = expr->Cast<BoundOperatorExpression>();
		for (const auto &child_expr : operator_expr.children) {
			VisitExprs(child_expr, func);
		}
		return;
	}
	default:
		break;
	}

	func(expr);
}

bool HasNullptr(const unique_ptr<LogicalOperator> &plan) {
	bool has_nullptr = false;
	for (size_t idx = 0; idx < plan->children.size(); idx++) {
		if (nullptr == plan->children[idx])
			return true;
		else
			has_nullptr |= HasNullptr(plan->children[idx]);
	}
	return has_nullptr;
}
} // namespace duckdb
