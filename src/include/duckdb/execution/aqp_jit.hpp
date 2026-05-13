//===----------------------------------------------------------------------===//
//                         AQP JIT Receiver for DuckDB
//
// This header defines the JIT context and ABI types that allow the AQP
// middleware to register LLVM-compiled expression functions with DuckDB's
// execution engine. The middleware compiles filter expressions from the AQP IR
// using LLVM, then stores compiled function pointers here. DuckDB's
// PhysicalFilter checks this context and dispatches to compiled code instead
// of the interpreted ExpressionExecutor.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"

#include <future>
#include <mutex>

namespace duckdb {

// Forward declaration — full type included only in aqp_jit.cpp
class PhysicalOperator;

// ---------------------------------------------------------------------------
// ABI types — stable C-compatible layout shared with the AQP middleware.
// The middleware LLVM compiler generates code against these structs.
// sel_t is uint32_t in DuckDB (typedefs.hpp).
// validity_t is uint64_t (1 bit per row, packed; nullptr = all rows valid).
// ---------------------------------------------------------------------------

struct AQPColView {
	void     *data;      // flat element array; cast to int32_t*, int64_t*, etc.
	uint64_t *validity;  // nullptr = all valid; else 1 bit per row in 64-bit words
	int32_t   vtype;     // 0=FLAT, 1=CONSTANT, 2=DICTIONARY
	int32_t   dtype;     // AQP_DTYPE_* constants below
};

struct AQPChunkView {
	AQPColView *cols;
	uint64_t    nrows;  // current chunk row count (≤ STANDARD_VECTOR_SIZE)
	uint64_t    ncols;
};

struct AQPSelView {
	sel_t   *indices;  // selected row indices (sel_t = uint32_t)
	uint32_t count;
};

// dtype constants — must match include/jit/aqp_jit_abi.h in AQP middleware
static constexpr int32_t AQP_DTYPE_BOOL    = 0;
static constexpr int32_t AQP_DTYPE_INT8    = 1;
static constexpr int32_t AQP_DTYPE_INT16   = 2;
static constexpr int32_t AQP_DTYPE_INT32   = 3;
static constexpr int32_t AQP_DTYPE_INT64   = 4;
static constexpr int32_t AQP_DTYPE_FLOAT   = 5;
static constexpr int32_t AQP_DTYPE_DOUBLE  = 6;
static constexpr int32_t AQP_DTYPE_VARCHAR = 7;
static constexpr int32_t AQP_DTYPE_DATE    = 8;
static constexpr int32_t AQP_DTYPE_OTHER   = 99;

// ---------------------------------------------------------------------------
// Compiled function types
// ---------------------------------------------------------------------------

// Expression-level: evaluates WHERE clause; fills sel->indices[0..ret-1].
// Returns count of selected rows. Matches ExpressionExecutor::SelectExpression.
using AQPExprFn = idx_t (*)(AQPChunkView *, AQPSelView *);

// Operator-level: transforms input chunk to output chunk (filter, projection).
// Returns OperatorResultType cast to int32_t to avoid DuckDB enum dependency.
using AQPOperatorFn = int32_t (*)(AQPChunkView *in, AQPChunkView *out);

// Pipeline-level: processes one chunk from source through fused operators to sink.
// Returns count of output rows, or negative on error.
using AQPPipelineFn = int64_t (*)(AQPChunkView *source_chunk,
                                  AQPChunkView *sink_chunk,
                                  void *pipeline_state);

// Callback for deep-copying a non-inline string_t into a Vector's string heap.
using AQPCopyStringFn = void (*)(const void *src_string,
                                 void *dst_string,
                                 void *dst_vector);

// Hash-join view: exposed to JIT'd probe code so it can touch DuckDB's
// JoinHashTable directly (no vtable, no AQPHashTable shim).
// Filled in PhysicalHashJoin::ExecuteInternal at probe time.
// MUST stay byte-compatible with AQPJoinHTView in aqp_jit_abi.h.
struct AQPJoinHTView {
	void           *entries;        // ht_entry_t *
	uint64_t        bitmask;        // capacity - 1
	uint64_t        use_salt;       // 1 if capacity > USE_SALT_THRESHOLD (8192)
	void           *layout_ptr;     // opaque shared_ptr<TupleDataLayout>* (set to layout_ptr.get())
	uint32_t        tuple_size;     // total row width in bytes
	uint32_t        pointer_offset; // offset of next_pointer inside row
	const uint64_t *data_offsets;   // layout->GetOffsets().data() — per-col offsets within row
};

// State for pipeline filter functions (not fusions) — provides Vector pointers.
struct AQPPipelineFilterState {
	void **col_vectors;       // col_vectors[i] = &chunk.data[i]
	uint64_t num_cols;
	AQPCopyStringFn copy_str; // deep string copy callback
};

// Sub-plan coordinator: orchestrates multiple compiled pipelines.
using AQPSubPlanFn = int32_t (*)(void *subplan_ctx);

// ---------------------------------------------------------------------------
// JIT flags — indicate what was compiled and at what optimization level
// ---------------------------------------------------------------------------
enum AQPJITFlags : uint32_t {
	AQPJIT_NONE     = 0,
	AQPJIT_EXPR     = 1u << 0,  // Level 1: individual expression compilation
	AQPJIT_OPERATOR = 1u << 1,  // Level 2: full operator compilation
	AQPJIT_PIPELINE = 1u << 2,  // Level 3: fused pipeline compilation
	AQPJIT_OPT3     = 1u << 3,  // Use LLVM O3 optimization
	AQPJIT_SQL      = 1u << 4,  // Level 4: SQL / sub-SQL compilation
	AQPJIT_SUBPLAN  = AQPJIT_SQL,  // Legacy alias
	AQPJIT_SIMD     = 1u << 5,  // Enable explicit SIMD vectorization
};

// ---------------------------------------------------------------------------
// Per-query JIT context — stored in ClientContext::aqp_jit_context.
// Written once by the AQP middleware before execution begins (for synchronous
// compilation). The pending_exprs map supports future background compilation.
// ---------------------------------------------------------------------------
struct AQPJITContext {
	uint32_t flags = AQPJIT_NONE;

	// expr_id → compiled function.  Key computed by ExpressionID() below.
	unordered_map<uint64_t, AQPExprFn>     expr_fns;      // Level 1 + Level 2 filter
	// Level 2 operators (+ Level 3). Hash build/probe are stored here but NOT
	// dispatched at Level 2 — AQPHashTable is incompatible with DuckDB's
	// JoinHashTable. They are consumed only by Level 3/4 pipeline fusion.
	unordered_map<uint64_t, AQPOperatorFn> op_fns;
	unordered_map<uint64_t, AQPPipelineFn> pipeline_fns;  // Level 3 fused pipelines (probe side)
	unordered_map<uint64_t, AQPPipelineFn> build_pipeline_fns;  // Level 3 fused build-side pipelines

	// Projection column mappings: eid → {out_col_i -> in_col_i}
	// DuckDB dispatches these via zero-copy Vector::Reference() at Level 2.
	// Other engines use the memcpy-based AQPOperatorFn in op_fns instead.
	unordered_map<uint64_t, vector<int>>   proj_col_maps;

	// Aggregate update functions: void fn(AQPChunkView*, void* agg_state)
	// Dispatched at Level 2 for ungrouped aggregates (tight compiled loop).
	using AQPAggUpdateFn = void (*)(AQPChunkView *, void *);
	unordered_map<uint64_t, AQPAggUpdateFn> agg_fns;
	// Aggregate state size in bytes per eid
	unordered_map<uint64_t, uint32_t>       agg_state_sizes;

	// Per-aggregate metadata for JIT state initialization and finalize conversion.
	// agg_type: 1=Min, 2=Max, 3=Sum, 4=Avg, 5=Count, 6=CountStar
	struct AQPAggMeta {
		int32_t  agg_type;
		int32_t  dtype;
		uint32_t state_offset;
		uint32_t state_bytes;
	};
	unordered_map<uint64_t, vector<AQPAggMeta>> agg_meta;

	// Scan+Filter fusion: filter applied at scan level, producing pre-filtered chunks.
	// Key = TABLE_SCAN operator eid, Value = compiled filter function.
	unordered_map<uint64_t, AQPExprFn> scan_filter_fns;

	// PhysicalFilter eids whose work is already done by scan+filter fusion.
	// PhysicalFilter checks this set and becomes a pass-through when present.
	unordered_set<uint64_t> fused_scan_filter_eids;

	// Per-pipeline opaque state (e.g., AQP hash table pointer for fused build/probe).
	unordered_map<uint64_t, void*> pipeline_states;

	// Pipeline-JIT hash-join: view of DuckDB's JoinHashTable shared with JIT'd
	// probe code. Owned by the context; populated at probe time from
	// sink.hash_table fields. Keyed by HASH_JOIN operator eid.
	unordered_map<uint64_t, unique_ptr<AQPJoinHTView>> join_ht_views;

	// Sub-plan coordinator: one per sub-plan execution
	AQPSubPlanFn subplan_fn = nullptr;

	// Background compilation: futures that resolve to compiled fns.
	// Polled at chunk boundaries; swapped into active maps when ready.
	unordered_map<uint64_t, std::future<AQPExprFn>> pending_exprs;

	// Mutex for thread-safe AQP hash table build (Sink is called from multiple threads).
	std::mutex build_mutex;

	// Diagnostic counters — incremented by PhysicalFilter on each dispatch.
	uint64_t dispatch_count = 0;   // chunks routed through compiled path
	uint64_t fallback_count = 0;   // chunks routed through interpreted path
};

// ---------------------------------------------------------------------------
// Helper functions (implemented in aqp_jit.cpp)
// ---------------------------------------------------------------------------

// Convert a DuckDB DataChunk to an AQPChunkView.
// Cost: O(ncols) pointer assignments — no data copy.
AQPChunkView MakeChunkView(DataChunk &chunk);

// Same as MakeChunkView but uses col_buf[buf_offset..] to avoid collisions
// when building separate input and output views simultaneously.
AQPChunkView MakeChunkViewAt(DataChunk &chunk, idx_t buf_offset);

// Wrap an existing SelectionVector as an AQPSelView.
AQPSelView MakeSelView(SelectionVector &sel);

// Map DuckDB PhysicalType to AQP dtype constant.
int32_t ToDtype(PhysicalType pt);

// Compute a stable expression ID for a filter operator.
// Uses the operator's heap address XOR-hashed with a constant — stable within
// a single query execution (the DuckDB plan does not move after creation).
uint64_t ExpressionID(const PhysicalOperator &op);

// Deep-copy a string_t into a Vector's string heap (safe for non-inline strings).
void AQPCopyStringImpl(const void *src_string, void *dst_string, void *dst_vector);

} // namespace duckdb
