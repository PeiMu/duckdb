#include "duckdb/optimizer/converter/duckdb_to_ir.h"

namespace duckdb {

unique_ptr<SimplestStmt> duckdb::DuckToIRConverter::ConstructSimplestStmt(
    LogicalOperator *duckdb_plan_pointer, const std::unordered_map<unsigned int, std::string> &intermediate_table_map) {
	std::function<unique_ptr<SimplestStmt>(LogicalOperator * duckdb_plan_pointer)> iterate_plan;
	iterate_plan = [&iterate_plan, intermediate_table_map,
	                this](LogicalOperator *duckdb_plan_pointer) -> unique_ptr<SimplestStmt> {
		unique_ptr<SimplestStmt> left_child, right_child;
		if (duckdb_plan_pointer->children.size() > 0) {
			left_child = iterate_plan(duckdb_plan_pointer->children[0].get());
			if (duckdb_plan_pointer->children.size() == 2)
				right_child = iterate_plan(duckdb_plan_pointer->children[1].get());
		}
		switch (duckdb_plan_pointer->type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &proj_op = duckdb_plan_pointer->Cast<LogicalProjection>();
			auto simplest_proj = ConstructSimplestProj(proj_op, std::move(left_child));
			return unique_ptr_cast<SimplestProjection, SimplestStmt>(std::move(simplest_proj));
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &agg_group_op = duckdb_plan_pointer->Cast<LogicalAggregate>();
			auto simplest_agg_group = ConstructSimplestAggGroup(agg_group_op, std::move(left_child));
			return unique_ptr_cast<SimplestAggregate, SimplestStmt>(std::move(simplest_agg_group));
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter_op = duckdb_plan_pointer->Cast<LogicalFilter>();
			auto simplest_filter = ConstructSimplestFilter(filter_op, std::move(left_child));
			return unique_ptr_cast<SimplestFilter, SimplestStmt>(std::move(simplest_filter));
		}
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
			auto &cross_product_op = duckdb_plan_pointer->Cast<LogicalCrossProduct>();
			auto simplest_cross_product =
			    ConstructSimplestCrossProduct(cross_product_op, std::move(left_child), std::move(right_child));
			return unique_ptr_cast<SimplestCrossProduct, SimplestStmt>(std::move(simplest_cross_product));
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join_op = duckdb_plan_pointer->Cast<LogicalComparisonJoin>();
			auto simplest_join = ConstructSimplestJoin(join_op, std::move(left_child), std::move(right_child));
			return unique_ptr_cast<SimplestJoin, SimplestStmt>(std::move(simplest_join));
		}
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get_op = duckdb_plan_pointer->Cast<LogicalGet>();
			auto simplest_scan = ConstructSimplestScan(get_op);
			return unique_ptr_cast<SimplestScan, SimplestStmt>(std::move(simplest_scan));
		}
		case LogicalOperatorType::LOGICAL_CHUNK_GET: {
			auto &column_data_get_op = duckdb_plan_pointer->Cast<LogicalColumnDataGet>();
			auto find_intermediate_table = intermediate_table_map.find(column_data_get_op.table_index);
			if (find_intermediate_table != intermediate_table_map.end()) {
				auto simplest_scan = ConstructSimplestScan(column_data_get_op, find_intermediate_table->second);
				return unique_ptr_cast<SimplestScan, SimplestStmt>(std::move(simplest_scan));
			} else {
				// it might be an `IN` clause
				auto simplest_chunk = ConstructSimplestChunk(column_data_get_op);
				return unique_ptr_cast<SimplestChunk, SimplestStmt>(std::move(simplest_chunk));
			}
		}
		default:
			Printer::Print(StringUtil::Format("Do not support yet, op->type:  %s",
			                                  LogicalOperatorToString(duckdb_plan_pointer->type)));
			D_ASSERT(false);
			return nullptr;
		}
	};

	auto simplest_stmt = iterate_plan(duckdb_plan_pointer);

#ifdef DEBUG
	Printer::Print("constructed simplest stmt");
	simplest_stmt->Print();
#endif

	return simplest_stmt;
}

unique_ptr<SimplestProjection> DuckToIRConverter::ConstructSimplestProj(LogicalProjection &proj_op,
                                                                        unique_ptr<SimplestStmt> child) {
	auto table_index = proj_op.table_index;

	std::vector<unique_ptr<SimplestStmt>> children;
	children.emplace_back(std::move(child));

	std::vector<unique_ptr<SimplestAttr>> target_list;
	for (const auto &expr : proj_op.expressions) {
		auto table_expr = GetConstTableExpr(expr);
		auto simplest_target = make_uniq<SimplestAttr>(ConvertVarType(table_expr.return_type), table_expr.table_idx,
		                                               table_expr.column_idx, table_expr.column_name);
		target_list.emplace_back(std::move(simplest_target));
	}
	auto base_stmt =
	    make_uniq<SimplestStmt>(std::move(children), std::move(target_list), SimplestNodeType::ProjectionNode);

	auto simplest_projection = make_uniq<SimplestProjection>(std::move(base_stmt), table_index);

	return simplest_projection;
}

unique_ptr<SimplestAggregate> DuckToIRConverter::ConstructSimplestAggGroup(LogicalAggregate &agg_group_op,
                                                                           unique_ptr<SimplestStmt> child) {
	std::vector<unique_ptr<SimplestStmt>> children;
	children.emplace_back(std::move(child));

	// todo: add target list
	std::vector<unique_ptr<SimplestAttr>> target_list;

	// add table_expr of group by
	for (const auto &group_expr : agg_group_op.groups) {
		// todo
	}

	// set agg_index and group_index
	unsigned int agg_index = agg_group_op.aggregate_index;
	unsigned int group_index = agg_group_op.group_index;

	agg_fn_pair agg_fns;

	// add table_expr of aggregate op expression
	for (const auto &agg_expr : agg_group_op.expressions) {
#ifdef DEBUG
		D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
#endif
		auto &aggregate_expr = agg_expr->Cast<BoundAggregateExpression>();
		std::string agg_fn_type = aggregate_expr.function.name;
		unique_ptr<SimplestAttr> simplest_attr;
		for (const auto &expr : aggregate_expr.children) {
#ifdef DEBUG
			D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
#endif
			auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
			simplest_attr = make_uniq<SimplestAttr>(ConvertVarType(column_ref_expr.return_type),
			                                        column_ref_expr.binding.table_index,
			                                        column_ref_expr.binding.column_index, column_ref_expr.alias);
			agg_fns.emplace_back(std::make_pair(std::move(simplest_attr), ConvertAggFnType(agg_fn_type)));
		}
	}

	auto base_stmt =
	    make_uniq<SimplestStmt>(std::move(children), std::move(target_list), SimplestNodeType::AggregateNode);

	auto simplest_aggregate =
	    make_uniq<SimplestAggregate>(std::move(base_stmt), std::move(agg_fns), agg_index, group_index);
	return simplest_aggregate;
}

unique_ptr<SimplestCrossProduct> DuckToIRConverter::ConstructSimplestCrossProduct(
    LogicalCrossProduct &cross_product_op, unique_ptr<SimplestStmt> left_child, unique_ptr<SimplestStmt> right_child) {
	std::vector<unique_ptr<SimplestStmt>> children;
	children.emplace_back(std::move(left_child));
	children.emplace_back(std::move(right_child));
	auto base_stmt = make_uniq<SimplestStmt>(std::move(children), SimplestNodeType::CrossProductNode);
	auto simplest_cross_product = make_uniq<SimplestCrossProduct>(std::move(base_stmt));

	return simplest_cross_product;
}

unique_ptr<SimplestJoin> DuckToIRConverter::ConstructSimplestJoin(LogicalComparisonJoin &join_op,
                                                                  unique_ptr<SimplestStmt> left_child,
                                                                  unique_ptr<SimplestStmt> right_child) {
	SimplestJoinType join_type;
	switch (join_op.join_type) {
	case JoinType::INVALID:
		Printer::Print("Invalid join type!");
		join_type = SimplestJoinType::InvalidJoinType;
		D_ASSERT(false);
		break;
	case JoinType::LEFT:
		join_type = SimplestJoinType::Left;
		break;
	case JoinType::RIGHT:
		join_type = SimplestJoinType::Right;
		break;
	case JoinType::INNER:
		join_type = SimplestJoinType::Inner;
		break;
	case JoinType::MARK:
		join_type = SimplestJoinType::Mark;
		break;
	default:
		Printer::Print(StringUtil::Format("Do not support yet, join_type:  %s", join_op.join_type));
		join_type = SimplestJoinType::InvalidJoinType;
		D_ASSERT(false);
	}

	std::vector<unique_ptr<SimplestStmt>> children;
	children.emplace_back(std::move(left_child));
	children.emplace_back(std::move(right_child));
	auto base_stmt = make_uniq<SimplestStmt>(std::move(children), SimplestNodeType::JoinNode);

	std::vector<unique_ptr<SimplestVarComparison>> join_conditions;
	for (const auto &cond : join_op.conditions) {
		auto comp_type = ConvertCompType(cond.comparison);
		const auto &left_cond = cond.left;
		auto left_type = ConvertVarType(left_cond->return_type);
		auto left_expr_info = GetConstTableExpr(left_cond);
		auto left_simplest_cond = make_uniq<SimplestAttr>(left_type, left_expr_info.table_idx,
		                                                  left_expr_info.column_idx, left_expr_info.column_name);
		const auto &right_cond = cond.right;
		auto right_type = ConvertVarType(right_cond->return_type);
		auto right_expr_info = GetConstTableExpr(right_cond);
		auto right_simplest_cond = make_uniq<SimplestAttr>(right_type, right_expr_info.table_idx,
		                                                   right_expr_info.column_idx, right_expr_info.column_name);
#ifdef DEBUG
		D_ASSERT(left_type == right_type);
#endif
		auto simplest_cond =
		    make_uniq<SimplestVarComparison>(comp_type, std::move(left_simplest_cond), std::move(right_simplest_cond));
		join_conditions.emplace_back(std::move(simplest_cond));
	}

	auto simplest_join = make_uniq<SimplestJoin>(std::move(base_stmt), std::move(join_conditions), join_type);

	return simplest_join;
}

unique_ptr<SimplestFilter> DuckToIRConverter::ConstructSimplestFilter(LogicalFilter &filter_op,
                                                                      unique_ptr<SimplestStmt> child) {
	std::vector<unique_ptr<SimplestStmt>> children;
	children.emplace_back(std::move(child));
	// todo: add target list
	std::vector<unique_ptr<SimplestAttr>> target_list;
	// add qual vec
	std::vector<unique_ptr<SimplestExpr>> qual_vec = CollectQualVecExprs(filter_op.expressions);

	auto base_stmt = make_uniq<SimplestStmt>(std::move(children), std::move(target_list), std::move(qual_vec),
	                                         SimplestNodeType::FilterNode);

	auto simplest_filter = make_uniq<SimplestFilter>(std::move(base_stmt));

	return simplest_filter;
}

unique_ptr<SimplestScan> DuckToIRConverter::ConstructSimplestScan(LogicalGet &get_op) {
	auto table_index = get_op.table_index;

	// add target list
	std::vector<unique_ptr<SimplestAttr>> target_list;
#ifdef DEBUG
	get_op.names.size() == get_op.returned_types.size();
#endif
	for (size_t column_idx = 0; column_idx < get_op.names.size(); column_idx++) {
		unique_ptr<SimplestAttr> simplest_attr = make_uniq<SimplestAttr>(
		    ConvertVarType(get_op.returned_types[column_idx]), table_index, column_idx, get_op.names[column_idx]);
		target_list.emplace_back(std::move(simplest_attr));
	}

	// add qual vec
	std::vector<unique_ptr<SimplestExpr>> qual_vec;
	for (const auto &filter : get_op.table_filters.filters) {
		auto column_index = filter.first;
		auto &filter_cond = filter.second;
		auto simplest_var_attr = make_uniq<SimplestAttr>(ConvertVarType(get_op.returned_types[column_index]),
		                                                 table_index, column_index, get_op.names[column_index]);
		auto simplest_scan_filter_expr = CollectScanFilter(filter_cond, std::move(simplest_var_attr));
		qual_vec.emplace_back(std::move(simplest_scan_filter_expr));
	}

	auto base_stmt = make_uniq<SimplestStmt>(std::move(target_list), std::move(qual_vec), SimplestNodeType::ScanNode);

	auto table_name = get_op.function.to_string(get_op.bind_data.get());
	auto simplest_scan = make_uniq<SimplestScan>(std::move(base_stmt), table_index, table_name);
	return simplest_scan;
}

unique_ptr<SimplestScan> DuckToIRConverter::ConstructSimplestScan(LogicalColumnDataGet &get_op,
                                                                  std::string intermediate_table_name) {
	// todo: add target list
	std::vector<unique_ptr<SimplestAttr>> target_list;
	// todo: add qual vec
	std::vector<unique_ptr<SimplestExpr>> qual_vec;

	auto base_stmt = make_uniq<SimplestStmt>(std::move(target_list), std::move(qual_vec), SimplestNodeType::ScanNode);

	auto table_index = get_op.table_index;
	auto simplest_scan = make_uniq<SimplestScan>(std::move(base_stmt), table_index, intermediate_table_name);
	return simplest_scan;
}

unique_ptr<SimplestChunk> DuckToIRConverter::ConstructSimplestChunk(LogicalColumnDataGet &column_data_get_op) {
	// fixme: might have other types
	std::vector<std::string> chunk_contents;
	DataChunk chunk;

	column_data_get_op.collection->InitializeScanChunk(chunk);
	ColumnDataScanState scan_state;
	column_data_get_op.collection->InitializeScan(scan_state);
	while (column_data_get_op.collection->Scan(scan_state, chunk)) {
		for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
			for (idx_t j = 0; j < chunk.size(); j++) {
				chunk_contents.emplace_back(chunk.data[i].GetValue(j).ToString());
			}
		}
	}

	// todo: add target list
	std::vector<unique_ptr<SimplestAttr>> target_list;
	// todo: add qual vec
	std::vector<unique_ptr<SimplestExpr>> qual_vec;

	auto base_stmt = make_uniq<SimplestStmt>(std::move(target_list), std::move(qual_vec), SimplestNodeType::ScanNode);
	auto simplest_chunk =
	    make_uniq<SimplestChunk>(std::move(base_stmt), column_data_get_op.table_index, chunk_contents);
	return simplest_chunk;
}

SimplestExprType DuckToIRConverter::ConvertCompType(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return SimplestExprType::Equal;
	case ExpressionType::COMPARE_LESSTHAN:
		return SimplestExprType::LessThan;
	case ExpressionType::COMPARE_GREATERTHAN:
		return SimplestExprType::GreaterThan;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return SimplestExprType::LessEqual;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return SimplestExprType::GreaterEqual;
	case ExpressionType::COMPARE_NOTEQUAL:
		return SimplestExprType::NotEqual;
	case ExpressionType::COMPARE_IN:
		return SimplestExprType::TextLike;
	case ExpressionType::COMPARE_NOT_IN:
		return SimplestExprType::TEXT_Not_LIKE;
	default:
		Printer::Print("Invalid comparison type!");
		return SimplestExprType::InvalidExprType;
	}
}

SimplestVarType DuckToIRConverter::ConvertVarType(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return SimplestVarType::BoolVar;
	case LogicalTypeId::INTEGER:
		return SimplestVarType::IntVar;
	case LogicalTypeId::FLOAT:
		return SimplestVarType::FloatVar;
	case LogicalTypeId::VARCHAR:
		return SimplestVarType::StringVar;
	default:
		Printer::Print("Invalid postgres var type!");
		return SimplestVarType::InvalidVarType;
	}
}

SimplestAggFnType DuckToIRConverter::ConvertAggFnType(std::string agg_fn_type) {
	if (agg_fn_type == "min")
		return SimplestAggFnType::Min;
	else if (agg_fn_type == "max")
		return SimplestAggFnType::Max;
	else if (agg_fn_type == "sum")
		return SimplestAggFnType::Sum;
	else if (agg_fn_type == "avg")
		return SimplestAggFnType::Average;
	else
		return SimplestAggFnType::InvalidAggType;
}

std::vector<unique_ptr<SimplestExpr>>
DuckToIRConverter::CollectQualVecExprs(const vector<unique_ptr<Expression>> &exprs) {
	std::vector<unique_ptr<SimplestExpr>> qual_vec;
	for (const auto &expr : exprs) {
		// fixme: refactor by visitor in query_split_util.h
		switch (expr->type) {
		case ExpressionType::BOUND_FUNCTION: {
			auto &bound_func = expr->Cast<BoundFunctionExpression>();
			// todo: determine the simplest_expr_type
			auto simplest_expr_type = SimplestExprType::TextLike;
			auto &left_expr = bound_func.children[0];
			TableExpr left_table_expr = GetConstTableExpr(left_expr);
			auto left_simplest_attr =
			    make_uniq<SimplestAttr>(ConvertVarType(left_table_expr.return_type), left_table_expr.table_idx,
			                            left_table_expr.column_idx, left_table_expr.column_name);
			auto &right_expr = bound_func.children[1]->Cast<BoundConstantExpression>();
			// todo: determine the type
			auto right_simplest_attr = make_uniq<SimplestConstVar>(right_expr.value.ToString());
			auto simplest_var_const_comp = make_uniq<SimplestVarConstComparison>(
			    simplest_expr_type, std::move(left_simplest_attr), std::move(right_simplest_attr));

			qual_vec.emplace_back(std::move(simplest_var_const_comp));

			break;
		}
		case ExpressionType::BOUND_COLUMN_REF: {
			TableExpr table_expr = GetConstTableExpr(expr);
			auto simplest_attr = make_uniq<SimplestAttr>(ConvertVarType(table_expr.return_type), table_expr.table_idx,
			                                             table_expr.column_idx, table_expr.column_name);
			auto simplest_attr_expr = make_uniq<SimplestSingleAttrExpr>(std::move(simplest_attr));
			qual_vec.emplace_back(std::move(simplest_attr_expr));
			break;
		}
		default:
			Printer::Print(
			    StringUtil::Format("Do not support yet, expr->type:  %s", ExpressionTypeToString(expr->type)));
			D_ASSERT(false);
		}
	}

	return qual_vec;
}

unique_ptr<SimplestExpr> DuckToIRConverter::CollectScanFilter(const unique_ptr<TableFilter> &filter_cond,
                                                              unique_ptr<SimplestAttr> var_attr) {
	switch (filter_cond->filter_type) {
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = filter_cond->Cast<ConjunctionAndFilter>();
#ifdef DEBUG
		D_ASSERT(2 == conjunction_and.child_filters.size());
#endif
		auto left_filter = CollectScanFilter(conjunction_and.child_filters[0], make_uniq<SimplestAttr>(*var_attr));
		auto right_filter = CollectScanFilter(conjunction_and.child_filters[1], make_uniq<SimplestAttr>(*var_attr));
		auto simplest_conjunction_and = make_uniq<SimplestLogicalExpr>(SimplestLogicalOp::LogicalAnd,
		                                                               std::move(left_filter), std::move(right_filter));
		return unique_ptr_cast<SimplestLogicalExpr, SimplestExpr>(std::move(simplest_conjunction_and));
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_or = filter_cond->Cast<ConjunctionOrFilter>();
#ifdef DEBUG
		D_ASSERT(2 == conjunction_or.child_filters.size());
#endif
		auto left_filter = CollectScanFilter(conjunction_or.child_filters[0], make_uniq<SimplestAttr>(*var_attr));
		auto right_filter = CollectScanFilter(conjunction_or.child_filters[1], make_uniq<SimplestAttr>(*var_attr));
		auto simplest_conjunction_or = make_uniq<SimplestLogicalExpr>(SimplestLogicalOp::LogicalOr,
		                                                              std::move(left_filter), std::move(right_filter));
		return unique_ptr_cast<SimplestLogicalExpr, SimplestExpr>(std::move(simplest_conjunction_or));
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter_cond->Cast<ConstantFilter>();
		std::string constant_str = constant_filter.constant.ToString();
		auto simplest_comp_type = ConvertCompType(constant_filter.comparison_type);
		auto simplest_const_var = make_uniq<SimplestConstVar>(constant_str);
		auto simplest_constant_comp = make_uniq<SimplestVarConstComparison>(
		    simplest_comp_type, make_uniq<SimplestAttr>(*var_attr), std::move(simplest_const_var));
		return unique_ptr_cast<SimplestVarConstComparison, SimplestExpr>(std::move(simplest_constant_comp));
	}
	case TableFilterType::IS_NOT_NULL: {
		auto simplest_is_not_null =
		    make_uniq<SimplestIsNullExpr>(SimplestExprType::NonNullType, make_uniq<SimplestAttr>(*var_attr));
		return unique_ptr_cast<SimplestIsNullExpr, SimplestExpr>(std::move(simplest_is_not_null));
	}
	case TableFilterType::IS_NULL: {
		auto simplest_is_null =
		    make_uniq<SimplestIsNullExpr>(SimplestExprType::NullType, make_uniq<SimplestAttr>(*var_attr));
		return unique_ptr_cast<SimplestIsNullExpr, SimplestExpr>(std::move(simplest_is_null));
	}
	case TableFilterType::STRUCT_EXTRACT: {
		Printer::Print("Do not support yet: TableFilterType::STRUCT_EXTRACT");
		D_ASSERT(false);
	}
	}
}
} // namespace duckdb
