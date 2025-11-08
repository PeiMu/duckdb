#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownGet(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = op->Cast<LogicalGet>();

	bool is_temp_table = false;
	auto table_name = get.function.to_string(get.bind_data.get());
	if ("temp" == table_name.substr(0, 4)) {
		is_temp_table = true;
	}

	if (get.function.pushdown_complex_filter || get.function.filter_pushdown) {
		// this scan supports some form of filter push-down
		// check if there are any parameters
		// if there are, invalidate them to force a re-bind on execution
		for (auto &filter : filters) {
			if (filter->filter->HasParameter()) {
				// there is a parameter in the filters! invalidate it
				BoundParameterExpression::InvalidateRecursive(*filter->filter);
			}
		}
	}
	if (get.function.pushdown_complex_filter) {
		// for the remaining filters, check if we can push any of them into the scan as well
		vector<unique_ptr<Expression>> expressions;
		expressions.reserve(filters.size());
		for (auto &filter : filters) {
			auto &expr = filter->filter;
			if (ExpressionClass::BOUND_COMPARISON == expr->GetExpressionClass() && is_temp_table) {
				auto &bound_comp_expr = expr->Cast<BoundComparisonExpression>();
				auto &left_expr = bound_comp_expr.left;
				auto &right_expr = bound_comp_expr.right;
				if (left_expr->type == ExpressionType::BOUND_COLUMN_REF &&
				    right_expr->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &left_bound_col = left_expr->Cast<BoundColumnRefExpression>();
					auto &right_bound_col = left_expr->Cast<BoundColumnRefExpression>();
					if (left_bound_col.binding.table_index == right_bound_col.binding.table_index)
						continue;
				}
			}
			expressions.push_back(std::move(expr));
		}
		filters.clear();

		get.function.pushdown_complex_filter(optimizer.context, get, get.bind_data.get(), expressions);

		if (expressions.empty()) {
			return op;
		}
		// re-generate the filters
		for (auto &expr : expressions) {
			auto f = make_uniq<Filter>();
			f->filter = std::move(expr);
			f->ExtractBindings();
			filters.push_back(std::move(f));
		}
	}

	if (!get.table_filters.filters.empty() || !get.function.filter_pushdown) {
		// the table function does not support filter pushdown: push a LogicalFilter on top
		return FinishPushdown(std::move(op));
	}
	PushFilters();

	//! We generate the table filters that will be executed during the table scan
	//! Right now this only executes simple AND filters
	get.table_filters = combiner.GenerateTableScanFilters(get.column_ids);

	// //! For more complex filters if all filters to a column are constants we generate a min max boundary used to
	// check
	// //! the zonemaps.
	// auto zonemap_checks = combiner.GenerateZonemapChecks(get.column_ids, get.table_filters);

	// for (auto &f : get.table_filters) {
	// 	f.column_index = get.column_ids[f.column_index];
	// }

	// //! Use zonemap checks as table filters for pre-processing
	// for (auto &zonemap_check : zonemap_checks) {
	// 	if (zonemap_check.column_index != COLUMN_IDENTIFIER_ROW_ID) {
	// 		get.table_filters.push_back(zonemap_check);
	// 	}
	// }

	GenerateFilters();

	//! Now we try to pushdown the remaining filters to perform zonemap checking
	return FinishPushdown(std::move(op));
}

} // namespace duckdb
