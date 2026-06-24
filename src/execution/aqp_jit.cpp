#include "duckdb/execution/aqp_jit.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

int32_t ToDtype(PhysicalType pt) {
	switch (pt) {
	case PhysicalType::BOOL:   return AQP_DTYPE_BOOL;
	case PhysicalType::INT8:   return AQP_DTYPE_INT8;
	case PhysicalType::INT16:  return AQP_DTYPE_INT16;
	case PhysicalType::INT32:  return AQP_DTYPE_INT32;
	case PhysicalType::INT64:  return AQP_DTYPE_INT64;
	case PhysicalType::FLOAT:  return AQP_DTYPE_FLOAT;
	case PhysicalType::DOUBLE: return AQP_DTYPE_DOUBLE;
	case PhysicalType::VARCHAR:return AQP_DTYPE_VARCHAR;
	default:                   return AQP_DTYPE_OTHER;
	}
}

AQPChunkView MakeChunkView(DataChunk &chunk) {
	return MakeChunkViewAt(chunk, 0);
}

AQPChunkView MakeChunkViewAt(DataChunk &chunk, idx_t buf_offset, bool writable_validity) {
	// col_views is a thread-local scratch buffer; we rebuild it per call.
	// buf_offset allows multiple non-overlapping regions (e.g., input at 0,
	// output at input.ColumnCount()) for operator-level JIT that needs
	// separate input and output AQPChunkViews simultaneously.
	static thread_local AQPColView col_buf[4096]; // max columns per chunk

	// Flatten ensures all vectors are FLAT (no CONSTANT/DICTIONARY wrappers).
	// Required so FlatVector accessors and raw data pointers are valid.
	chunk.Flatten();

	idx_t ncols = chunk.ColumnCount();
	for (idx_t i = 0; i < ncols; i++) {
		Vector &vec = chunk.data[i];
		auto &vmask = FlatVector::Validity(vec);
		if (writable_validity) {
			// Fresh all-valid buffer: JIT'd emit code clears bits for NULL
			// outputs via raw word stores, so the mask must be owned and
			// not shared with another vector.
			vmask.Initialize(STANDARD_VECTOR_SIZE);
		}
		col_buf[buf_offset + i].data     = vec.GetData();
		col_buf[buf_offset + i].validity = vmask.AllValid() ? nullptr : reinterpret_cast<uint64_t *>(vmask.GetData());
		col_buf[buf_offset + i].vtype    = static_cast<int32_t>(vec.GetVectorType());
		col_buf[buf_offset + i].dtype    = ToDtype(vec.GetType().InternalType());
	}

	AQPChunkView cv;
	cv.cols  = &col_buf[buf_offset];
	cv.nrows = static_cast<uint64_t>(chunk.size());
	cv.ncols = static_cast<uint64_t>(ncols);
	return cv;
}

AQPSelView MakeSelView(SelectionVector &sel) {
	AQPSelView sv;
	sv.indices = sel.data();
	sv.count   = 0; // caller fills count after expression evaluation
	return sv;
}

uint64_t ExpressionID(const PhysicalOperator &op) {
	// Use the operator's heap address as a stable per-execution ID.
	// The DuckDB physical plan is immutable after creation, so addresses
	// are stable for the lifetime of a query execution.
	return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&op));
}

void AQPCopyStringImpl(const void *src_string, void *dst_string, void *dst_vector) {
	auto &vec = *reinterpret_cast<Vector *>(dst_vector);
	auto src = *reinterpret_cast<const string_t *>(src_string);
	string_t result = StringVector::AddStringOrBlob(vec, src);
	memcpy(dst_string, &result, sizeof(string_t));
}

} // namespace duckdb

// §7.3 template cache: thread-local params for parameterized compilation.
// Defined here (in libduckdb.so) so both the middleware (linker) and the
// LLJIT runtime symbol table can resolve the same address.
namespace aqp_jit {

thread_local const uint8_t *g_jit_params = nullptr;

const uint8_t *aqp_jit_get_params() { return g_jit_params; }
void aqp_jit_set_params(const uint8_t *p) { g_jit_params = p; }

} // namespace aqp_jit

extern "C" {

void aqp_copy_string(void *dst_data, void *src_data,
                     uint64_t dst_row, uint64_t src_row,
                     void *state_ptr, uint32_t col_idx) {
	auto *state = reinterpret_cast<duckdb::AQPPipelineFilterState *>(state_ptr);
	const uint8_t *src = reinterpret_cast<const uint8_t *>(src_data) + src_row * 16;
	uint8_t *dst = reinterpret_cast<uint8_t *>(dst_data) + dst_row * 16;
	uint32_t len;
	memcpy(&len, src, sizeof(uint32_t));
	if (len <= 12) {
		memcpy(dst, src, 16);
	} else {
		state->copy_str(src, dst, state->col_vectors[col_idx]);
	}
}

}
