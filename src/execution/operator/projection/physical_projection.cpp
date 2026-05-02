#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/aqp_jit.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class ProjectionState : public OperatorState {
public:
	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
	    : executor(context.client, expressions) {
	}

	ExpressionExecutor executor;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

PhysicalProjection::PhysicalProjection(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                       vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::PROJECTION, std::move(types), estimated_cardinality),
      select_list(std::move(select_list)) {
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	// AQP JIT Level 2: Use pre-computed column mapping for zero-copy projection.
	// Vector::Reference() aliases the input vector's data pointer — no memcpy.
	// The compiled AQPOperatorFn (memcpy-based) is stored in op_fns for Level 3
	// pipeline fusion and for engines without zero-copy aliasing.
	auto *jit = context.client.aqp_jit_context.get();
	if (jit && (jit->flags & AQPJIT_OPERATOR)) {
		uint64_t eid = ExpressionID(*this);
		auto mit = jit->proj_col_maps.find(eid);
		if (mit != jit->proj_col_maps.end()) {
			auto &mapping = mit->second;
			chunk.SetCardinality(input.size());
			for (idx_t i = 0; i < mapping.size() && i < chunk.ColumnCount(); i++) {
				if (mapping[i] >= 0 && (idx_t)mapping[i] < input.ColumnCount()) {
					chunk.data[i].Reference(input.data[mapping[i]]);
				}
			}
			jit->dispatch_count++;
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}

	auto &state = state_p.Cast<ProjectionState>();
	state.executor.Execute(input, chunk);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<ProjectionState>(context, select_list);
}

InsertionOrderPreservingMap<string> PhysicalProjection::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string projections;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			projections += "\n";
		}
		auto &expr = select_list[i];
		projections += expr->GetName();
	}
	result["__projections__"] = projections;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
