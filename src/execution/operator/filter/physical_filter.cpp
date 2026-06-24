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
	uint64_t eid = ExpressionID(*this);

	// Scan+Filter fusion: the TABLE_SCAN already applied this filter.
	if (jit && jit->fused_scan_filter_eids.count(eid)) {
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Pipeline-level JIT: a compiled filter function that reads the input
	// chunk and writes matching rows directly into the output chunk.
	if (jit && (jit->flags & AQPJIT_PIPELINE)) {
		auto pit = jit->pipeline_fns.find(eid);
		if (pit != jit->pipeline_fns.end()) {
			input.Flatten();
			chunk.Reset();
			chunk.Flatten();
			AQPChunkView in_cv  = MakeChunkViewAt(input, 0);
			AQPChunkView out_cv = MakeChunkViewAt(chunk, input.ColumnCount());
			void *pipe_state = nullptr;
			auto sit = jit->pipeline_states.find(eid);
			if (sit != jit->pipeline_states.end() && sit->second) {
				pipe_state = sit->second;
			}
			if (!pipe_state) {
				thread_local std::vector<void *> col_vec_ptrs;
				thread_local AQPPipelineFilterState pf_state;
				col_vec_ptrs.resize(chunk.ColumnCount());
				for (idx_t ci = 0; ci < chunk.ColumnCount(); ci++) {
					col_vec_ptrs[ci] = &chunk.data[ci];
				}
				pf_state.col_vectors = col_vec_ptrs.data();
				pf_state.num_cols = chunk.ColumnCount();
				pf_state.copy_str = AQPCopyStringImpl;
				pipe_state = &pf_state;
			}
			if (auto ppit = jit->pipeline_params.find(eid); ppit != jit->pipeline_params.end())
				aqp_jit_set_params(ppit->second.data());
			int64_t out_rows = pit->second(&in_cv, &out_cv, pipe_state);
			aqp_jit_set_params(nullptr);
			if (out_rows >= 0) {
				chunk.SetCardinality(static_cast<idx_t>(out_rows));
				jit->dispatch_count++;
				return OperatorResultType::NEED_MORE_INPUT;
			}
		}
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
			if (auto pit = jit->expr_params.find(eid); pit != jit->expr_params.end())
				aqp_jit_set_params(pit->second.data());
			AQPChunkView cv = MakeChunkView(input);
			AQPSelView sv = MakeSelView(state.sel);
			result_count = fit->second(&cv, &sv);
			aqp_jit_set_params(nullptr);
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
