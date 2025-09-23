//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/optimizer/query_split/query_split.hpp"
#include "duckdb/optimizer/reorder_get.h"
#include "duckdb/execution/column_binding_resolver.hpp"

#include <functional>

namespace duckdb {
class Binder;

class Optimizer {
public:
	Optimizer(Binder &binder, ClientContext &context);

	//! Optimize a plan by running specialized optimizers
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);
	//! Optimize a plan by running specialized optimizers before join order optimization
	unique_ptr<LogicalOperator> PreOptimize(unique_ptr<LogicalOperator> plan_p);
	unique_ptr<LogicalOperator> MiddleOptimize(unique_ptr<LogicalOperator> plan_p);
	//! Optimize a plan by running specialized optimizers when enable split_jop config
	unique_ptr<LogicalOperator> ReorderGetOptimize(unique_ptr<LogicalOperator> plan_p);
	//! Optimize a plan by running specialized optimizers after join order optimization
	unique_ptr<LogicalOperator> PostOptimize(unique_ptr<LogicalOperator> plan);
	//! Return a reference to the client context of this optimizer
	ClientContext &GetContext();
	//! Whether the specific optimizer is disabled
	bool OptimizerDisabled(OptimizerType type);
	static bool OptimizerDisabled(ClientContext &context, OptimizerType type);

public:
	ClientContext &context;
	Binder &binder;
	ExpressionRewriter rewriter;

private:
	void RunBuiltInOptimizers();
	void RunBuiltInPreOptimizers();
	void RunBuiltInMiddleOptimizers();
	void RunBuiltInPostOptimizers();
	void RunOptimizer(OptimizerType type, const std::function<void()> &callback);
	void Verify(LogicalOperator &op);

public:
	// helper functions
	unique_ptr<Expression> BindScalarFunction(const string &name, unique_ptr<Expression> c1);
	unique_ptr<Expression> BindScalarFunction(const string &name, unique_ptr<Expression> c1, unique_ptr<Expression> c2);

private:
	unique_ptr<LogicalOperator> plan;
	column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map;

private:
	unique_ptr<Expression> BindScalarFunction(const string &name, vector<unique_ptr<Expression>> children);
};

} // namespace duckdb
