#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/execution/aqp_jit.hpp"
#include "duckdb/main/client_context.hpp"

#include <utility>

namespace duckdb {

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p,
                                     vector<ColumnIndex> column_ids_p, vector<idx_t> projection_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality, ExtraOperatorInfo extra_info,
                                     vector<Value> parameters_p, virtual_column_map_t virtual_columns_p,
                                     idx_t logical_table_index_p)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)),
      column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)), names(std::move(names_p)),
      table_filters(std::move(table_filters_p)), extra_info(std::move(extra_info)), parameters(std::move(parameters_p)),
      virtual_columns(std::move(virtual_columns_p)), logical_table_index(logical_table_index_p) {
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		if (op.dynamic_filters && op.dynamic_filters->HasFilters()) {
			table_filters = op.dynamic_filters->GetFinalTableFilters(op, op.table_filters.get());
		}

		if (op.function.init_global) {
			auto filters = table_filters ? *table_filters : GetTableFilters(op);
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, filters,
			                             op.extra_info.sample_options);

			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
		if (op.function.in_out_function) {
			// this is an in-out function, we need to setup the input chunk
			vector<LogicalType> input_types;
			for (auto &param : op.parameters) {
				input_types.push_back(param.type());
			}
			input_chunk.Initialize(context, input_types);
			for (idx_t c = 0; c < op.parameters.size(); c++) {
				input_chunk.data[c].Reference(op.parameters[c]);
			}
			input_chunk.SetCardinality(1);
		}
	}

	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;
	bool in_out_final = false;
	DataChunk input_chunk;
	//! Combined table filters, if we have dynamic filters
	unique_ptr<TableFilterSet> table_filters;

	optional_ptr<TableFilterSet> GetTableFilters(const PhysicalTableScan &op) const {
		return table_filters ? table_filters.get() : op.table_filters.get();
	}
	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalTableScan &op) {
		if (op.function.init_local) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids,
			                             gstate.GetTableFilters(op), op.extra_info.sample_options);
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
	}

	unique_ptr<LocalTableFunctionState> local_state;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TableScanGlobalSourceState>(context, *this);
}

SourceResultType PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	D_ASSERT(!column_ids.empty());
	auto &g_state = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &l_state = input.local_state.Cast<TableScanLocalSourceState>();

	TableFunctionInput data(bind_data.get(), l_state.local_state.get(), g_state.global_state.get());

	if (function.function) {
		function.function(context.client, data, chunk);

		// Remember whether the scan itself produced data before JIT filtering.
		// A scan that returns 0 rows signals that the table is exhausted;
		// JIT filtering can also reduce the chunk to 0 rows even though
		// more data remains in the table.
		bool scan_had_data = chunk.size() > 0;

		// AQP JIT: Scan+Filter fusion — apply compiled filter at scan level.
		// Produces a pre-filtered chunk, avoiding a separate PhysicalFilter operator call.
		if (chunk.size() > 0) {
			auto *jit = context.client.aqp_jit_context.get();
			if (jit && !jit->scan_filter_fns.empty()) {
				uint64_t scan_eid = ExpressionID(*this);
				auto fit = jit->scan_filter_fns.find(scan_eid);
				if (fit != jit->scan_filter_fns.end()) {
					chunk.Flatten();
					AQPChunkView cv = MakeChunkView(chunk);
					SelectionVector sel(STANDARD_VECTOR_SIZE);
					AQPSelView sv = MakeSelView(sel);
					idx_t result_count = fit->second(&cv, &sv);
					if (result_count == 0) {
						chunk.SetCardinality(0);
					} else if (result_count < chunk.size()) {
						chunk.Slice(sel, result_count);
					}
					jit->dispatch_count++;
				}
			}
		}

		// AQP Bloom filter push-down
		if (chunk.size() > 0) {
			auto *jit = context.client.aqp_jit_context.get();
			if (jit && !jit->bloom_scan_filters.empty()) {
				uint64_t scan_eid = ExpressionID(*this);
				auto bit = jit->bloom_scan_filters.find(scan_eid);
				if (bit != jit->bloom_scan_filters.end()) {
					for (auto &bf_ptr : bit->second) {
						if (chunk.size() == 0) break;
						auto &bf = *bf_ptr;
						if (bf.col_idx >= chunk.ColumnCount()) continue;
						chunk.Flatten();
						auto &vec = chunk.data[bf.col_idx];
						auto expected_pt = (bf.dtype == AQP_DTYPE_INT32) ? PhysicalType::INT32 : PhysicalType::INT64;
						if (vec.GetType().InternalType() != expected_pt) continue;
						auto &validity = FlatVector::Validity(vec);
						SelectionVector sel(STANDARD_VECTOR_SIZE);
						idx_t result_count = 0;
						idx_t n = chunk.size();
						if (bf.dtype == AQP_DTYPE_INT32) {
							auto *data_ptr = FlatVector::GetData<int32_t>(vec);
							for (idx_t i = 0; i < n; i++) {
								if (validity.RowIsValid(i)) {
									uint64_t h = duckdb::Hash<int32_t>(data_ptr[i]);
									if (bf.LookupOne(h)) { sel.set_index(result_count++, i); }
								}
							}
						} else {
							auto *data_ptr = FlatVector::GetData<int64_t>(vec);
							for (idx_t i = 0; i < n; i++) {
								if (validity.RowIsValid(i)) {
									uint64_t h = duckdb::Hash<int64_t>(data_ptr[i]);
									if (bf.LookupOne(h)) { sel.set_index(result_count++, i); }
								}
							}
						}
						if (result_count == 0) {
							chunk.SetCardinality(0);
						} else if (result_count < n) {
							chunk.Slice(sel, result_count);
						}
					}
				}
			}
		}

		// Use scan_had_data to determine the return value: if the scan
		// produced rows but JIT filtering removed them all, there may
		// still be more data in the table — return HAVE_MORE_OUTPUT.
		if (!scan_had_data) {
			return SourceResultType::FINISHED;
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	if (g_state.in_out_final) {
		function.in_out_function_final(context, data, chunk);
	}
	switch (function.in_out_function(context, data, g_state.input_chunk, chunk)) {
	case OperatorResultType::BLOCKED: {
		auto guard = g_state.Lock();
		return g_state.BlockSource(guard, input.interrupt_state);
	}
	default:
		// FIXME: Handling for other cases (such as NEED_MORE_INPUT) breaks current functionality and extensions that
		// might be relying on current behaviour. Needs a rework that is not in scope
		break;
	}

	if (chunk.size() == 0 && function.in_out_function_final) {
		function.in_out_function_final(context, data, chunk);
		g_state.in_out_final = true;
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

ProgressData PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	ProgressData res;
	if (function.table_scan_progress) {
		double table_progress = function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
		if (table_progress < 0.0) {
			res.SetInvalid();
		} else {
			res.done = table_progress;
			res.total = 100.0;
			// Assume cardinality is always 1e3
			res.Normalize(1e3);
		}
	} else {
		// if table_scan_progress is not implemented we don't support this function yet in the progress bar
		res.SetInvalid();
	}
	return res;
}

bool PhysicalTableScan::SupportsPartitioning(const OperatorPartitionInfo &partition_info) const {
	if (!function.get_partition_data) {
		return false;
	}
	// FIXME: actually check if partition info is supported
	return true;
}

OperatorPartitionData PhysicalTableScan::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                          GlobalSourceState &gstate_p, LocalSourceState &lstate,
                                                          const OperatorPartitionInfo &partition_info) const {
	D_ASSERT(SupportsPartitioning(partition_info));
	D_ASSERT(function.get_partition_data);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	TableFunctionGetPartitionInput input(bind_data.get(), state.local_state.get(), gstate.global_state.get(),
	                                     partition_info);
	return function.get_partition_data(context.client, input);
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name + " " + function.extra_info);
}

void AddProjectionNames(const ColumnIndex &index, const string &name, const LogicalType &type, string &result) {
	if (!index.HasChildren()) {
		// base case - no children projected out
		if (!result.empty()) {
			result += "\n";
		}
		result += name;
		return;
	}
	auto &child_types = StructType::GetChildTypes(type);
	for (auto &child_index : index.GetChildIndexes()) {
		auto &ele = child_types[child_index.GetPrimaryIndex()];
		AddProjectionNames(child_index, name + "." + ele.first, ele.second, result);
	}
}

InsertionOrderPreservingMap<string> PhysicalTableScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (function.to_string) {
		TableFunctionToStringInput input(function, bind_data.get());
		auto to_string_result = function.to_string(input);
		for (const auto &it : to_string_result) {
			result[it.first] = it.second;
		}
	} else {
		result["Function"] = StringUtil::Upper(function.name);
	}
	if (function.projection_pushdown) {
		string projections;
		idx_t projected_column_count = function.filter_prune ? projection_ids.size() : column_ids.size();
		for (idx_t i = 0; i < projected_column_count; i++) {
			auto base_index = function.filter_prune ? projection_ids[i] : i;
			auto &column_index = column_ids[base_index];
			auto column_id = column_index.GetPrimaryIndex();
			if (column_id >= names.size()) {
				continue;
			}
			AddProjectionNames(column_index, names[column_id], returned_types[column_id], projections);
		}
		result["Projections"] = projections;
	}
	if (function.filter_pushdown && table_filters) {
		string filters_info;
		bool first_item = true;
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				if (!first_item) {
					filters_info += "\n";
				}
				first_item = false;

				const auto col_id = column_ids[column_index].GetPrimaryIndex();
				if (IsVirtualColumn(col_id)) {
					auto entry = virtual_columns.find(col_id);
					if (entry == virtual_columns.end()) {
						throw InternalException("Virtual column not found");
					}
					filters_info += filter->ToString(entry->second.name);
				} else {
					filters_info += filter->ToString(names[col_id]);
				}
			}
		}
		result["Filters"] = filters_info;
	}
	if (extra_info.sample_options) {
		result["Sample Method"] = "System: " + extra_info.sample_options->sample_size.ToString() + "%";
	}
	if (!extra_info.file_filters.empty()) {
		result["File Filters"] = extra_info.file_filters;
		if (extra_info.filtered_files.IsValid() && extra_info.total_files.IsValid()) {
			result["Scanning Files"] = StringUtil::Format("%llu/%llu", extra_info.filtered_files.GetIndex(),
			                                              extra_info.total_files.GetIndex());
		}
	}

	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = other_p.Cast<PhysicalTableScan>();
	if (function.function != other.function.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

bool PhysicalTableScan::ParallelSource() const {
	if (!function.function) {
		// table in-out functions cannot be executed in parallel as part of a PhysicalTableScan
		// since they have only a single input row
		return false;
	}
	return true;
}

InsertionOrderPreservingMap<string> PhysicalTableScan::ExtraSourceParams(GlobalSourceState &gstate_p,
                                                                         LocalSourceState &lstate) const {
	if (!function.dynamic_to_string) {
		return InsertionOrderPreservingMap<string>();
	}
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	TableFunctionDynamicToStringInput input(function, bind_data.get(), state.local_state.get(),
	                                        gstate.global_state.get());
	return function.dynamic_to_string(input);
}

} // namespace duckdb
