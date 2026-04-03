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
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"

#include <future>

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

// ---------------------------------------------------------------------------
// JIT flags — indicate what was compiled and at what optimization level
// ---------------------------------------------------------------------------
enum AQPJITFlags : uint32_t {
	AQPJIT_NONE     = 0,
	AQPJIT_EXPR     = 1u << 0,  // expression-level JIT active
	AQPJIT_OPERATOR = 1u << 1,  // operator-level JIT active
	AQPJIT_OPT3     = 1u << 3,  // compiled with O3 (else O0)
};

// ---------------------------------------------------------------------------
// Per-query JIT context — stored in ClientContext::aqp_jit_context.
// Written once by the AQP middleware before execution begins (for synchronous
// compilation). The pending_exprs map supports future background compilation.
// ---------------------------------------------------------------------------
struct AQPJITContext {
	uint32_t flags = AQPJIT_NONE;

	// expr_id → compiled function.  Key computed by ExpressionID() below.
	unordered_map<uint64_t, AQPExprFn>     expr_fns;
	unordered_map<uint64_t, AQPOperatorFn> op_fns;

	// Background compilation (phase 2): futures that resolve to compiled fns.
	// Polled at chunk boundaries; swapped into expr_fns when ready.
	unordered_map<uint64_t, std::future<AQPExprFn>> pending_exprs;

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

// Wrap an existing SelectionVector as an AQPSelView.
AQPSelView MakeSelView(SelectionVector &sel);

// Map DuckDB PhysicalType to AQP dtype constant.
int32_t ToDtype(PhysicalType pt);

// Compute a stable expression ID for a filter operator.
// Uses the operator's heap address XOR-hashed with a constant — stable within
// a single query execution (the DuckDB plan does not move after creation).
uint64_t ExpressionID(const PhysicalOperator &op);

} // namespace duckdb
