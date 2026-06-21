#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/execution/aqp_jit.hpp"

#include <cstring>
#include <functional>
#include <limits>

namespace duckdb {

PhysicalUngroupedAggregate::PhysicalUngroupedAggregate(vector<LogicalType> types,
                                                       vector<unique_ptr<Expression>> expressions,
                                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNGROUPED_AGGREGATE, std::move(types), estimated_cardinality),
      aggregates(std::move(expressions)) {

	distinct_collection_info = DistinctAggregateCollectionInfo::Create(aggregates);
	if (!distinct_collection_info) {
		return;
	}
	distinct_data = make_uniq<DistinctAggregateData>(*distinct_collection_info);
}

//===--------------------------------------------------------------------===//
// Ungrouped Aggregate State
//===--------------------------------------------------------------------===//
UngroupedAggregateState::UngroupedAggregateState(const vector<unique_ptr<Expression>> &aggregate_expressions)
    : aggregate_expressions(aggregate_expressions) {
	counts = make_uniq_array<atomic<idx_t>>(aggregate_expressions.size());
	for (idx_t i = 0; i < aggregate_expressions.size(); i++) {
		auto &aggregate = aggregate_expressions[i];
		D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		auto state = make_unsafe_uniq_array_uninitialized<data_t>(aggr.function.state_size(aggr.function));
		aggr.function.initialize(aggr.function, state.get());
		aggregate_data.push_back(std::move(state));
		bind_data.push_back(aggr.bind_info.get());
		destructors.push_back(aggr.function.destructor);
#ifdef DEBUG
		counts[i] = 0;
#endif
	}
}
UngroupedAggregateState::~UngroupedAggregateState() {
	D_ASSERT(destructors.size() == aggregate_data.size());
	for (idx_t i = 0; i < destructors.size(); i++) {
		if (!destructors[i]) {
			continue;
		}
		Vector state_vector(Value::POINTER(CastPointerToValue(aggregate_data[i].get())));
		state_vector.SetVectorType(VectorType::FLAT_VECTOR);

		ArenaAllocator allocator(Allocator::DefaultAllocator());
		AggregateInputData aggr_input_data(bind_data[i], allocator);
		destructors[i](state_vector, aggr_input_data, 1);
	}
}

void UngroupedAggregateState::Move(UngroupedAggregateState &other) {
	other.aggregate_data = std::move(aggregate_data);
	other.destructors = std::move(destructors);
}

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//
class UngroupedAggregateGlobalSinkState : public GlobalSinkState {
public:
	UngroupedAggregateGlobalSinkState(const PhysicalUngroupedAggregate &op, ClientContext &client)
	    : state(BufferAllocator::Get(client), op.aggregates), finished(false) {
		if (op.distinct_data) {
			distinct_state = make_uniq<DistinctAggregateState>(*op.distinct_data, client);
		}
	}

	//! The global aggregate state
	GlobalUngroupedAggregateState state;
	//! Whether or not the aggregate is finished
	bool finished;
	//! The data related to the distinct aggregates (if there are any)
	unique_ptr<DistinctAggregateState> distinct_state;
};

ArenaAllocator &GlobalUngroupedAggregateState::CreateAllocator() const {
	lock_guard<mutex> glock(lock);
	stored_allocators.emplace_back(make_uniq<ArenaAllocator>(client_allocator));
	return *stored_allocators.back();
}

void GlobalUngroupedAggregateState::Combine(LocalUngroupedAggregateState &other) {
	lock_guard<mutex> glock(lock);
	for (idx_t aggr_idx = 0; aggr_idx < state.aggregate_expressions.size(); aggr_idx++) {
		auto &aggregate = state.aggregate_expressions[aggr_idx]->Cast<BoundAggregateExpression>();

		if (aggregate.IsDistinct()) {
			continue;
		}

		Vector source_state(Value::POINTER(CastPointerToValue(other.state.aggregate_data[aggr_idx].get())));
		Vector dest_state(Value::POINTER(CastPointerToValue(state.aggregate_data[aggr_idx].get())));

		AggregateInputData aggr_input_data(aggregate.bind_info.get(), allocator,
		                                   AggregateCombineType::ALLOW_DESTRUCTIVE);
		aggregate.function.combine(source_state, dest_state, aggr_input_data, 1);
#ifdef DEBUG
		state.counts[aggr_idx] += other.state.counts[aggr_idx];
#endif
	}
}

void GlobalUngroupedAggregateState::CombineDistinct(LocalUngroupedAggregateState &other,
                                                    DistinctAggregateData &distinct_data) {
	lock_guard<mutex> glock(lock);
	for (idx_t aggr_idx = 0; aggr_idx < state.aggregate_expressions.size(); aggr_idx++) {
		if (!distinct_data.IsDistinct(aggr_idx)) {
			continue;
		}

		auto &aggregate = state.aggregate_expressions[aggr_idx]->Cast<BoundAggregateExpression>();
		AggregateInputData aggr_input_data(aggregate.bind_info.get(), allocator,
		                                   AggregateCombineType::ALLOW_DESTRUCTIVE);

		Vector state_vec(Value::POINTER(CastPointerToValue(other.state.aggregate_data[aggr_idx].get())));
		Vector combined_vec(Value::POINTER(CastPointerToValue(state.aggregate_data[aggr_idx].get())));
		aggregate.function.combine(state_vec, combined_vec, aggr_input_data, 1);
#ifdef DEBUG
		state.counts[aggr_idx] += other.state.counts[aggr_idx];
#endif
	}
}

//===--------------------------------------------------------------------===//
// Ungrouped Aggregate Execute State
//===--------------------------------------------------------------------===//
UngroupedAggregateExecuteState::UngroupedAggregateExecuteState(ClientContext &context,
                                                               const vector<unique_ptr<Expression>> &aggregates,
                                                               const vector<LogicalType> &child_types)
    : aggregates(aggregates), child_executor(context), aggregate_input_chunk(), filter_set() {
	vector<LogicalType> payload_types;
	vector<AggregateObject> aggregate_objects;
	auto &allocator = BufferAllocator::Get(context);
	for (auto &aggregate : aggregates) {
		D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		// initialize the payload chunk
		for (auto &child : aggr.children) {
			payload_types.push_back(child->return_type);
			child_executor.AddExpression(*child);
		}
		aggregate_objects.emplace_back(&aggr);
	}
	if (!payload_types.empty()) { // for select count(*) from t; there is no payload at all
		aggregate_input_chunk.Initialize(allocator, payload_types);
	}
	filter_set.Initialize(context, aggregate_objects, child_types);
}

void UngroupedAggregateExecuteState::Reset() {
	aggregate_input_chunk.Reset();
}

void UngroupedAggregateExecuteState::Sink(LocalUngroupedAggregateState &state, DataChunk &input) {
	DataChunk &payload_chunk = aggregate_input_chunk;

	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;

	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();

		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		if (aggregate.IsDistinct()) {
			continue;
		}

		idx_t payload_cnt = 0;
		// resolve the filter (if any)
		if (aggregate.filter) {
			auto &filtered_data = filter_set.GetFilterData(aggr_idx);
			auto count = filtered_data.ApplyFilter(input);

			child_executor.SetChunk(filtered_data.filtered_payload);
			payload_chunk.SetCardinality(count);
		} else {
			child_executor.SetChunk(input);
			payload_chunk.SetCardinality(input);
		}

		// resolve the child expressions of the aggregate (if any)
		for (idx_t i = 0; i < aggregate.children.size(); ++i) {
			child_executor.ExecuteExpression(payload_idx + payload_cnt, payload_chunk.data[payload_idx + payload_cnt]);
			payload_cnt++;
		}

		state.Sink(payload_chunk, payload_idx, aggr_idx);
	}
}

//===--------------------------------------------------------------------===//
// Local State
//===--------------------------------------------------------------------===//
LocalUngroupedAggregateState::LocalUngroupedAggregateState(GlobalUngroupedAggregateState &gstate)
    : allocator(gstate.CreateAllocator()), state(gstate.state.aggregate_expressions) {
}

class UngroupedAggregateLocalSinkState : public LocalSinkState {
public:
	UngroupedAggregateLocalSinkState(const PhysicalUngroupedAggregate &op, const vector<LogicalType> &child_types,
	                                 UngroupedAggregateGlobalSinkState &gstate_p, ExecutionContext &context)
	    : state(gstate_p.state), execute_state(context.client, op.aggregates, child_types) {
		auto &gstate = gstate_p.Cast<UngroupedAggregateGlobalSinkState>();
		InitializeDistinctAggregates(op, gstate, context);
		InitializeJITAggregate(op, context);
	}

	//! The local aggregate state
	LocalUngroupedAggregateState state;
	//! The ungrouped aggregate execute state
	UngroupedAggregateExecuteState execute_state;
	//! The local sink states of the distinct aggregates hash tables
	vector<unique_ptr<LocalSinkState>> radix_states;

	//! AQP JIT aggregate state
	bool use_jit_agg = false;
	AQPJITContext::AQPAggUpdateFn jit_agg_fn = nullptr;
	vector<uint8_t> jit_agg_state;
	vector<AQPJITContext::AQPAggMeta> jit_agg_meta;

public:
	void InitializeDistinctAggregates(const PhysicalUngroupedAggregate &op,
	                                  const UngroupedAggregateGlobalSinkState &gstate, ExecutionContext &context) {

		if (!op.distinct_data) {
			return;
		}
		auto &data = *op.distinct_data;
		auto &state = *gstate.distinct_state;
		D_ASSERT(!data.radix_tables.empty());

		const idx_t aggregate_count = state.radix_states.size();
		radix_states.resize(aggregate_count);

		auto &distinct_info = *op.distinct_collection_info;

		for (auto &idx : distinct_info.indices) {
			idx_t table_idx = distinct_info.table_map[idx];
			if (data.radix_tables[table_idx] == nullptr) {
				// This aggregate has identical input as another aggregate, so no table is created for it
				continue;
			}
			auto &radix_table = *data.radix_tables[table_idx];
			radix_states[table_idx] = radix_table.GetLocalSinkState(context);
		}
	}

	void InitializeJITAggregate(const PhysicalUngroupedAggregate &op, ExecutionContext &context) {
		auto *jit = context.client.aqp_jit_context.get();
		if (!jit || !(jit->flags & AQPJIT_OPERATOR)) {
			return;
		}
		if (op.distinct_data) {
			return;
		}
		uint64_t eid = ExpressionID(op);
		auto ait = jit->agg_fns.find(eid);
		if (ait == jit->agg_fns.end()) {
			return;
		}
		auto sit = jit->agg_state_sizes.find(eid);
		uint32_t sz = (sit != jit->agg_state_sizes.end()) ? sit->second : 0;
		if (sz == 0) {
			return;
		}
		auto mit = jit->agg_meta.find(eid);
		if (mit == jit->agg_meta.end() || mit->second.size() != op.aggregates.size()) {
			return;
		}

		jit_agg_fn = ait->second;
		jit_agg_meta = mit->second;
		jit_agg_state.resize(sz);

		// Initialize each accumulator to the identity value for its agg type
		for (auto &m : jit_agg_meta) {
			uint8_t *slot = jit_agg_state.data() + m.state_offset;
			switch (m.agg_type) {
			case 1: { // Min — init to max representable value
				if (m.dtype == AQP_DTYPE_INT32 || m.dtype == AQP_DTYPE_DATE) {
					int64_t v = std::numeric_limits<int32_t>::max();
					memcpy(slot, &v, 8);
				} else if (m.dtype == AQP_DTYPE_INT64) {
					int64_t v = std::numeric_limits<int64_t>::max();
					memcpy(slot, &v, 8);
				} else if (m.dtype == AQP_DTYPE_INT16) {
					int64_t v = std::numeric_limits<int16_t>::max();
					memcpy(slot, &v, 8);
				} else if (m.dtype == AQP_DTYPE_INT8) {
					int64_t v = std::numeric_limits<int8_t>::max();
					memcpy(slot, &v, 8);
				} else if (m.dtype == AQP_DTYPE_DOUBLE || m.dtype == AQP_DTYPE_FLOAT) {
					double v = std::numeric_limits<double>::max();
					memcpy(slot, &v, 8);
				} else {
					memset(slot, 0, 8);
				}
				break;
			}
			case 2: { // Max — init to min representable value
				if (m.dtype == AQP_DTYPE_INT32 || m.dtype == AQP_DTYPE_DATE) {
					int64_t v = std::numeric_limits<int32_t>::lowest();
					memcpy(slot, &v, 8);
				} else if (m.dtype == AQP_DTYPE_INT64) {
					int64_t v = std::numeric_limits<int64_t>::lowest();
					memcpy(slot, &v, 8);
				} else if (m.dtype == AQP_DTYPE_INT16) {
					int64_t v = std::numeric_limits<int16_t>::lowest();
					memcpy(slot, &v, 8);
				} else if (m.dtype == AQP_DTYPE_INT8) {
					int64_t v = std::numeric_limits<int8_t>::lowest();
					memcpy(slot, &v, 8);
				} else if (m.dtype == AQP_DTYPE_DOUBLE || m.dtype == AQP_DTYPE_FLOAT) {
					double v = std::numeric_limits<double>::lowest();
					memcpy(slot, &v, 8);
				} else {
					memset(slot, 0, 8);
				}
				break;
			}
			default: // Sum, Count, CountStar, Average — zero is correct identity
				memset(slot, 0, m.state_bytes);
				break;
			}
		}
		use_jit_agg = true;
	}
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
bool PhysicalUngroupedAggregate::SinkOrderDependent() const {
	for (auto &expr : aggregates) {
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		if (aggr.function.order_dependent == AggregateOrderDependent::ORDER_DEPENDENT) {
			return true;
		}
	}
	return false;
}

unique_ptr<GlobalSinkState> PhysicalUngroupedAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<UngroupedAggregateGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalUngroupedAggregate::GetLocalSinkState(ExecutionContext &context) const {
	D_ASSERT(sink_state);
	auto &gstate = sink_state->Cast<UngroupedAggregateGlobalSinkState>();
	return make_uniq<UngroupedAggregateLocalSinkState>(*this, children[0].get().GetTypes(), gstate, context);
}

void PhysicalUngroupedAggregate::SinkDistinct(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSinkInput &input) const {
	auto &sink = input.local_state.Cast<UngroupedAggregateLocalSinkState>();
	auto &global_sink = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(distinct_data);
	auto &distinct_state = *global_sink.distinct_state;
	auto &distinct_info = *distinct_collection_info;
	auto &distinct_indices = distinct_info.Indices();

	DataChunk empty_chunk;

	auto &distinct_filter = distinct_info.Indices();

	for (auto &idx : distinct_indices) {
		auto &aggregate = aggregates[idx]->Cast<BoundAggregateExpression>();

		idx_t table_idx = distinct_info.table_map[idx];
		if (!distinct_data->radix_tables[table_idx]) {
			// This distinct aggregate shares its data with another
			continue;
		}
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state.radix_states[table_idx];
		auto &radix_local_sink = *sink.radix_states[table_idx];
		OperatorSinkInput sink_input {radix_global_sink, radix_local_sink, input.interrupt_state};

		if (aggregate.filter) {
			// The hashtable can apply a filter, but only on the payload
			// And in our case, we need to filter the groups (the distinct aggr children)

			// Apply the filter before inserting into the hashtable
			auto &filtered_data = sink.execute_state.filter_set.GetFilterData(idx);
			idx_t count = filtered_data.ApplyFilter(chunk);
			filtered_data.filtered_payload.SetCardinality(count);

			radix_table.Sink(context, filtered_data.filtered_payload, sink_input, empty_chunk, distinct_filter);
		} else {
			radix_table.Sink(context, chunk, sink_input, empty_chunk, distinct_filter);
		}
	}
}

SinkResultType PhysicalUngroupedAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSinkInput &input) const {
	auto &sink = input.local_state.Cast<UngroupedAggregateLocalSinkState>();

	// perform the aggregation inside the local state
	sink.execute_state.Reset();

	if (distinct_data) {
		SinkDistinct(context, chunk, input);
	}

	// AQP JIT Level 2: compiled aggregate update
	if (sink.use_jit_agg) {
		chunk.Flatten();
		AQPChunkView cv = MakeChunkView(chunk);
		sink.jit_agg_fn(&cv, sink.jit_agg_state.data());
		auto *jit = context.client.aqp_jit_context.get();
		if (jit) {
			jit->dispatch_count++;
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	sink.execute_state.Sink(sink.state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

void LocalUngroupedAggregateState::Sink(DataChunk &payload_chunk, idx_t payload_idx, idx_t aggr_idx) {
#ifdef DEBUG
	state.counts[aggr_idx] += payload_chunk.size();
#endif
	auto &aggregate = state.aggregate_expressions[aggr_idx]->Cast<BoundAggregateExpression>();
	idx_t payload_cnt = aggregate.children.size();
	D_ASSERT(payload_idx + payload_cnt <= payload_chunk.data.size());
	auto start_of_input = payload_cnt == 0 ? nullptr : &payload_chunk.data[payload_idx];
	AggregateInputData aggr_input_data(state.bind_data[aggr_idx], allocator);
	aggregate.function.simple_update(start_of_input, aggr_input_data, payload_cnt, state.aggregate_data[aggr_idx].get(),
	                                 payload_chunk.size());
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalUngroupedAggregate::CombineDistinct(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<UngroupedAggregateLocalSinkState>();

	if (!distinct_data) {
		return;
	}
	auto &distinct_state = gstate.distinct_state;
	auto table_count = distinct_data->radix_tables.size();
	for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state->radix_states[table_idx];
		auto &radix_local_sink = *lstate.radix_states[table_idx];

		radix_table.Combine(context, radix_global_sink, radix_local_sink);
	}
}

SinkCombineResultType PhysicalUngroupedAggregate::Combine(ExecutionContext &context,
                                                          OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<UngroupedAggregateLocalSinkState>();
	D_ASSERT(!gstate.finished);

	// finalize: combine the local state into the global state
	// all aggregates are combinable: we might be doing a parallel aggregate
	// use the combine method to combine the partial aggregates
	OperatorSinkCombineInput distinct_input {gstate, lstate, input.interrupt_state};
	CombineDistinct(context, distinct_input);

	// AQP JIT: convert the flat JIT state into DuckDB native aggregate states
	// before the standard Combine path. We feed each JIT accumulator as a
	// one-row vector through the native SimpleUpdate callback, which handles
	// all type/state-layout specifics (works for MIN, MAX, SUM, COUNT, AVG).
	if (lstate.use_jit_agg) {
		for (idx_t aggr_idx = 0; aggr_idx < aggregates.size() && aggr_idx < lstate.jit_agg_meta.size(); aggr_idx++) {
			auto &aggregate = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();
			auto &m = lstate.jit_agg_meta[aggr_idx];
			uint8_t *slot = lstate.jit_agg_state.data() + m.state_offset;

			DataChunk one_row;
			one_row.Initialize(Allocator::DefaultAllocator(), {aggregate.return_type});
			one_row.SetCardinality(1);
			auto &vec = one_row.data[0];
			auto pt = aggregate.return_type.InternalType();

			if (m.agg_type == 6) {
				// CountStar: result is int64 count → BIGINT
				int64_t cnt;
				memcpy(&cnt, slot, 8);
				FlatVector::GetData<int64_t>(vec)[0] = cnt;
			} else if (m.agg_type == 4) {
				// Average: state is {sum:8, count:8}, emit sum/count as double
				double sum_val;
				int64_t cnt;
				memcpy(&sum_val, slot, 8);
				memcpy(&cnt, slot + 8, 8);
				double avg = cnt > 0 ? sum_val / static_cast<double>(cnt) : 0.0;
				if (pt == PhysicalType::DOUBLE) {
					FlatVector::GetData<double>(vec)[0] = avg;
				} else if (pt == PhysicalType::FLOAT) {
					FlatVector::GetData<float>(vec)[0] = static_cast<float>(avg);
				} else {
					FlatVector::GetData<double>(vec)[0] = avg;
				}
			} else {
				// Min, Max, Sum, Count — stored as int64 or double
				bool is_float = (m.dtype == AQP_DTYPE_DOUBLE || m.dtype == AQP_DTYPE_FLOAT);
				switch (pt) {
				case PhysicalType::INT8: {
					int64_t v; memcpy(&v, slot, 8);
					FlatVector::GetData<int8_t>(vec)[0] = static_cast<int8_t>(v);
					break;
				}
				case PhysicalType::INT16: {
					int64_t v; memcpy(&v, slot, 8);
					FlatVector::GetData<int16_t>(vec)[0] = static_cast<int16_t>(v);
					break;
				}
				case PhysicalType::INT32: {
					int64_t v; memcpy(&v, slot, 8);
					FlatVector::GetData<int32_t>(vec)[0] = static_cast<int32_t>(v);
					break;
				}
				case PhysicalType::INT64: {
					int64_t v; memcpy(&v, slot, 8);
					FlatVector::GetData<int64_t>(vec)[0] = v;
					break;
				}
				case PhysicalType::FLOAT: {
					if (is_float) {
						double v; memcpy(&v, slot, 8);
						FlatVector::GetData<float>(vec)[0] = static_cast<float>(v);
					} else {
						int64_t v; memcpy(&v, slot, 8);
						FlatVector::GetData<float>(vec)[0] = static_cast<float>(v);
					}
					break;
				}
				case PhysicalType::DOUBLE: {
					if (is_float) {
						double v; memcpy(&v, slot, 8);
						FlatVector::GetData<double>(vec)[0] = v;
					} else {
						int64_t v; memcpy(&v, slot, 8);
						FlatVector::GetData<double>(vec)[0] = static_cast<double>(v);
					}
					break;
				}
				default: {
					int64_t v; memcpy(&v, slot, 8);
					FlatVector::GetData<int64_t>(vec)[0] = v;
					break;
				}
				}
			}

			Vector *input_vec = &vec;
			auto *native_state = lstate.state.state.aggregate_data[aggr_idx].get();
			AggregateInputData aggr_input_data(lstate.state.state.bind_data[aggr_idx], lstate.state.allocator);
			idx_t payload_cnt = (m.agg_type == 6) ? 0 : 1;
			Vector *start_of_input = (payload_cnt == 0) ? nullptr : input_vec;
			aggregate.function.simple_update(start_of_input, aggr_input_data, payload_cnt,
			                                 native_state, 1);
		}
	}

	gstate.state.Combine(lstate.state);

	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class UngroupedDistinctAggregateFinalizeEvent : public BasePipelineEvent {
public:
	UngroupedDistinctAggregateFinalizeEvent(ClientContext &context, const PhysicalUngroupedAggregate &op_p,
	                                        UngroupedAggregateGlobalSinkState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), context(context), op(op_p), gstate(gstate_p), tasks_scheduled(0),
	      tasks_done(0) {
	}

public:
	void Schedule() override;
	void FinalizeTask() {
		lock_guard<mutex> finalize(lock);
		D_ASSERT(!gstate.finished);
		D_ASSERT(tasks_done < tasks_scheduled);
		if (++tasks_done == tasks_scheduled) {
			gstate.finished = true;
		}
	}

private:
	ClientContext &context;

	const PhysicalUngroupedAggregate &op;
	UngroupedAggregateGlobalSinkState &gstate;

	mutex lock;
	idx_t tasks_scheduled;
	idx_t tasks_done;

public:
	vector<unique_ptr<GlobalSourceState>> global_source_states;
};

class UngroupedDistinctAggregateFinalizeTask : public ExecutorTask {
public:
	UngroupedDistinctAggregateFinalizeTask(Executor &executor, shared_ptr<Event> event_p,
	                                       const PhysicalUngroupedAggregate &op,
	                                       UngroupedAggregateGlobalSinkState &state_p)
	    : ExecutorTask(executor, std::move(event_p)), op(op), gstate(state_p), aggregate_state(gstate.state) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

	string TaskType() const override {
		return "UngroupedDistinctAggregateFinalizeTask";
	}

private:
	TaskExecutionResult AggregateDistinct();

private:
	const PhysicalUngroupedAggregate &op;
	UngroupedAggregateGlobalSinkState &gstate;

	// Distinct aggregation state
	LocalUngroupedAggregateState aggregate_state;
	idx_t aggregation_idx = 0;
	unique_ptr<LocalSourceState> radix_table_lstate;
	bool blocked = false;
};

void UngroupedDistinctAggregateFinalizeEvent::Schedule() {
	D_ASSERT(gstate.distinct_state);
	auto &aggregates = op.aggregates;
	auto &distinct_data = *op.distinct_data;

	idx_t n_tasks = 0;
	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		// Forward the payload idx
		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			global_source_states.push_back(nullptr);
			continue;
		}
		D_ASSERT(distinct_data.info.table_map.count(agg_idx));

		// Create global state for scanning
		auto table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table_p = *distinct_data.radix_tables[table_idx];
		n_tasks += radix_table_p.MaxThreads(*gstate.distinct_state->radix_states[table_idx]);
		global_source_states.push_back(radix_table_p.GetGlobalSourceState(context));
	}
	n_tasks = MaxValue<idx_t>(n_tasks, 1);
	n_tasks = MinValue<idx_t>(n_tasks, NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()));

	vector<shared_ptr<Task>> tasks;
	for (idx_t i = 0; i < n_tasks; i++) {
		tasks.push_back(
		    make_uniq<UngroupedDistinctAggregateFinalizeTask>(pipeline->executor, shared_from_this(), op, gstate));
		tasks_scheduled++;
	}
	SetTasks(std::move(tasks));
}

TaskExecutionResult UngroupedDistinctAggregateFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	auto res = AggregateDistinct();
	if (res == TaskExecutionResult::TASK_BLOCKED) {
		return res;
	}
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

TaskExecutionResult UngroupedDistinctAggregateFinalizeTask::AggregateDistinct() {
	D_ASSERT(gstate.distinct_state);
	auto &distinct_state = *gstate.distinct_state;
	auto &distinct_data = *op.distinct_data;

	auto &aggregates = op.aggregates;
	auto &state = aggregate_state;

	// Thread-local contexts
	ThreadContext thread_context(executor.context);
	ExecutionContext execution_context(executor.context, thread_context, nullptr);

	auto &finalize_event = event->Cast<UngroupedDistinctAggregateFinalizeEvent>();

	// Now loop through the distinct aggregates, scanning the distinct HTs

	// This needs to be preserved in case the radix_table.GetData blocks
	auto &agg_idx = aggregation_idx;

	for (; agg_idx < aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			continue;
		}

		const auto table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table = *distinct_data.radix_tables[table_idx];
		if (!blocked) {
			// Because we can block, we need to make sure we preserve this state
			radix_table_lstate = radix_table.GetLocalSourceState(execution_context);
		}
		auto &lstate = *radix_table_lstate;

		auto &sink = *distinct_state.radix_states[table_idx];
		InterruptState interrupt_state(shared_from_this());
		OperatorSourceInput source_input {*finalize_event.global_source_states[agg_idx], lstate, interrupt_state};

		DataChunk output_chunk;
		output_chunk.Initialize(executor.context, distinct_state.distinct_output_chunks[table_idx]->GetTypes());

		DataChunk payload_chunk;
		payload_chunk.InitializeEmpty(distinct_data.grouped_aggregate_data[table_idx]->group_types);
		payload_chunk.SetCardinality(0);

		while (true) {
			output_chunk.Reset();

			auto res = radix_table.GetData(execution_context, output_chunk, sink, source_input);
			if (res == SourceResultType::FINISHED) {
				D_ASSERT(output_chunk.size() == 0);
				break;
			} else if (res == SourceResultType::BLOCKED) {
				blocked = true;
				return TaskExecutionResult::TASK_BLOCKED;
			}

			// We dont need to resolve the filter, we already did this in Sink
			idx_t payload_cnt = aggregate.children.size();
			for (idx_t i = 0; i < payload_cnt; i++) {
				payload_chunk.data[i].Reference(output_chunk.data[i]);
			}
			payload_chunk.SetCardinality(output_chunk);

			// Update the aggregate state
			state.Sink(payload_chunk, 0, agg_idx);
		}
		blocked = false;
	}

	// After scanning the distinct HTs, we can combine the thread-local agg states with the thread-global
	gstate.state.CombineDistinct(state, distinct_data);
	finalize_event.FinalizeTask();
	return TaskExecutionResult::TASK_FINISHED;
}

SinkFinalizeType PhysicalUngroupedAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                              GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(distinct_data);
	auto &distinct_state = *gstate.distinct_state;

	for (idx_t table_idx = 0; table_idx < distinct_data->radix_tables.size(); table_idx++) {
		auto &radix_table_p = distinct_data->radix_tables[table_idx];
		auto &radix_state = *distinct_state.radix_states[table_idx];
		radix_table_p->Finalize(context, radix_state);
	}
	auto new_event = make_shared_ptr<UngroupedDistinctAggregateFinalizeEvent>(context, *this, gstate, pipeline);
	event.InsertEvent(std::move(new_event));
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalUngroupedAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();

	if (distinct_data) {
		return FinalizeDistinct(pipeline, event, context, input.global_state);
	}

	D_ASSERT(!gstate.finished);
	gstate.finished = true;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
void VerifyNullHandling(DataChunk &chunk, UngroupedAggregateState &state,
                        const vector<unique_ptr<Expression>> &aggregates) {
#ifdef DEBUG
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggr = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();
		if (state.counts[aggr_idx] == 0 && aggr.function.null_handling == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
			// Default is when 0 values go in, NULL comes out
			UnifiedVectorFormat vdata;
			chunk.data[aggr_idx].ToUnifiedFormat(1, vdata);
			D_ASSERT(!vdata.validity.RowIsValid(vdata.sel->get_index(0)));
		}
	}
#endif
}

void GlobalUngroupedAggregateState::Finalize(DataChunk &result, idx_t column_offset) {
	result.SetCardinality(1);
	for (idx_t aggr_idx = 0; aggr_idx < state.aggregate_expressions.size(); aggr_idx++) {
		auto &aggregate = state.aggregate_expressions[aggr_idx]->Cast<BoundAggregateExpression>();

		Vector state_vector(Value::POINTER(CastPointerToValue(state.aggregate_data[aggr_idx].get())));
		AggregateInputData aggr_input_data(aggregate.bind_info.get(), allocator);
		aggregate.function.finalize(state_vector, aggr_input_data, result.data[column_offset + aggr_idx], 1, 0);
	}
}

SourceResultType PhysicalUngroupedAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(gstate.finished);

	// initialize the result chunk with the aggregate values
	gstate.state.Finalize(chunk);
	VerifyNullHandling(chunk, gstate.state.state, aggregates);

	return SourceResultType::FINISHED;
}

InsertionOrderPreservingMap<string> PhysicalUngroupedAggregate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string aggregate_info;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (i > 0) {
			aggregate_info += "\n";
		}
		aggregate_info += aggregates[i]->GetName();
		if (aggregate.filter) {
			aggregate_info += " Filter: " + aggregate.filter->GetName();
		}
	}
	result["Aggregates"] = aggregate_info;
	return result;
}

} // namespace duckdb
