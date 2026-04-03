#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/aqp_jit.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"
namespace duckdb {

PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::FILTER, std::move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	} else {
		expression = std::move(select_list[0]);
	}
}

class FilterState : public CachingOperatorState {
public:
	explicit FilterState(ExecutionContext &context, Expression &expr)
	    : executor(context.client, expr), sel(STANDARD_VECTOR_SIZE) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FilterState>(context, *expression);
}

OperatorResultType PhysicalFilter::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FilterState>();

	// AQP JIT dispatch: if a compiled expression is registered for this
	// operator, call it directly instead of the interpreted ExpressionExecutor.
	idx_t result_count;
	auto *jit = context.client.aqp_jit_context.get();
	bool used_compiled = false;
	if (jit && (jit->flags & AQPJIT_EXPR)) {
		uint64_t eid = ExpressionID(*this);
		// Poll any pending background compilation (zero-cost: wait_for(0s))
		if (auto pit = jit->pending_exprs.find(eid); pit != jit->pending_exprs.end()) {
			if (pit->second.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
				jit->expr_fns[eid] = pit->second.get();
				jit->pending_exprs.erase(pit);
			}
		}
		if (auto fit = jit->expr_fns.find(eid); fit != jit->expr_fns.end()) {
			AQPChunkView cv = MakeChunkView(input);
			AQPSelView   sv = MakeSelView(state.sel);
			result_count     = fit->second(&cv, &sv);
			used_compiled    = true;
			if (jit->dispatch_count == 0)
				fprintf(stderr, "[AQP-JIT] first dispatch eid=0x%016lx\n",
				        (unsigned long)eid);
			jit->dispatch_count++;
		}
	}
	if (!used_compiled) {
		result_count = state.executor.SelectExpression(input, state.sel);
		if (jit) jit->fallback_count++;
	}

	if (result_count == input.size()) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, state.sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

InsertionOrderPreservingMap<string> PhysicalFilter::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["__expression__"] = expression->GetName();
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
