#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/aqp_jit.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"
namespace duckdb {

PhysicalFilter::PhysicalFilter(PhysicalPlan &physical_plan, vector<LogicalType> types,
                               vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality)
    : CachingPhysicalOperator(physical_plan, PhysicalOperatorType::FILTER, std::move(types), estimated_cardinality) {
	D_ASSERT(!select_list.empty());
	if (select_list.size() == 1) {
		expression = std::move(select_list[0]);
		return;
	}

	// Create a conjunction from the select list.
	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &expr : select_list) {
		conjunction->children.push_back(std::move(expr));
	}
	expression = std::move(conjunction);
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
	uint64_t eid = ExpressionID(*this);

	// Scan+Filter fusion: the TABLE_SCAN already applied this filter.
	if (jit && jit->fused_scan_filter_eids.count(eid)) {
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

#ifdef DEBUG
  	if (nullptr != jit && jit->flags) {
		Printer::Print(
		    StringUtil::Format("[AQP-JIT-TRACE] PhysicalFilter::Execute eid=0x%016lx, jit=%p, flags=%u, expr_fns=%zu",
		                       (unsigned long)eid, (void *)jit, jit ? jit->flags : 0u, jit ? jit->expr_fns.size() : 0u));
	}
#endif
	if (jit && (jit->flags & AQPJIT_EXPR)) {
		// Poll any pending background compilation (zero-cost: wait_for(0s))
		if (auto pit = jit->pending_exprs.find(eid); pit != jit->pending_exprs.end()) {
			if (pit->second.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
				jit->expr_fns[eid] = pit->second.get();
				jit->pending_exprs.erase(pit);
			}
		}
		if (auto fit = jit->expr_fns.find(eid); fit != jit->expr_fns.end()) {
#ifdef DEBUG
			Printer::Print(StringUtil::Format("[AQP-JIT] dispatch JIT fn=%p, eid=0x%016lx, nrows=%zu",
			                                  (void *)fit->second, (unsigned long)eid, (size_t)input.size()));
#endif
			AQPChunkView cv = MakeChunkView(input);
			AQPSelView sv = MakeSelView(state.sel);
			result_count = fit->second(&cv, &sv);
			used_compiled = true;
#ifdef DEBUG
			Printer::Print(StringUtil::Format("[AQP-JIT] dispatch #%lu eid=0x%016lx, nrows=%zu → selected=%zu",
			                                  (unsigned long)jit->dispatch_count, (unsigned long)eid,
			                                  (size_t)input.size(), (size_t)result_count));
#endif
			jit->dispatch_count++;
		} else {
#ifdef DEBUG
			Printer::Print(StringUtil::Format(
			    "[AQP-JIT-TRACE] eid=0x%016lx not in expr_fns, (skipped filter) → interpreter", (unsigned long)eid));
#endif
		}
	}
	if (!used_compiled) {
		// No compiled function for this filter — either JIT is disabled or
		// this filter was intentionally skipped (e.g. VARCHAR).
		// Fall back to the DuckDB interpreter.
		result_count = state.executor.SelectExpression(input, state.sel);
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
