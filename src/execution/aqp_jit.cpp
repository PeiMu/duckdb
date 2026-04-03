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
	// col_views is a thread-local scratch buffer; we rebuild it per call.
	// Cost: O(ncols) — negligible compared to processing 2048 rows.
	static thread_local AQPColView col_buf[4096]; // max columns per chunk

	// Flatten ensures all vectors are FLAT (no CONSTANT/DICTIONARY wrappers).
	// Required so FlatVector accessors and raw data pointers are valid.
	chunk.Flatten();

	idx_t ncols = chunk.ColumnCount();
	for (idx_t i = 0; i < ncols; i++) {
		Vector &vec = chunk.data[i];
		auto &vmask = FlatVector::Validity(vec);
		col_buf[i].data     = vec.GetData();
		col_buf[i].validity = vmask.AllValid() ? nullptr : reinterpret_cast<uint64_t *>(vmask.GetData());
		col_buf[i].vtype    = static_cast<int32_t>(vec.GetVectorType());
		col_buf[i].dtype    = ToDtype(vec.GetType().InternalType());
	}

	AQPChunkView cv;
	cv.cols  = col_buf;
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

} // namespace duckdb
