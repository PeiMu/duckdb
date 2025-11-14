# Step-by-Step Integration Guide for client_context.cpp

This guide shows exactly where to add code in `client_context.cpp` to integrate the cyclic join optimizer.

## Patch 1: Add Include Statement

**Location**: Top of file (around line 36)

**After this line**:
```cpp
#include "duckdb/optimizer/query_split/subquery_preparer.hpp"
```

**Add**:
```cpp
#include "duckdb/optimizer/query_split/cyclic_join_optimizer.hpp"
```

---

## Patch 2: Initialize Optimizer

**Location**: In `CreatePreparedStatementInternal` function, around line 490

**After this line**:
```cpp
SubqueryPreparer subquery_preparer(*planner.binder, *this);
```

**Add**:
```cpp
// Initialize cyclic join optimizer to handle redundant join conditions
CyclicJoinOptimizer cyclic_optimizer;
std::unordered_set<idx_t> all_temp_table_indices;

// Collect all join conditions from the initial plan
if (plan) {
	cyclic_optimizer.CollectJoinConditions(*plan);

	#if ENABLE_DEBUG_PRINT
	if (execute_plan) {
		auto cycles = cyclic_optimizer.DetectCycles();
		if (!cycles.empty()) {
			Printer::Print("Detected " + std::to_string(cycles.size()) + " cyclic join pattern(s)");
		}
	}
	#endif
}
```

---

## Patch 3: Track Temp Table Equalities

**Location**: In the query splitting loop, around line 647-652

**After these lines**:
```cpp
subquery_preparer.ClearOldTableIndex();
subquery_preparer.AddOldTableIndex(subqueries.front()[0]);
auto sub_plan = subquery_preparer.GenerateProjHead(plan, std::move(subqueries.front()[0]),
                                                   table_expr_queue, proj_expr, merge_sibling_expr);
```

**Add**:
```cpp
// Track which equalities are guaranteed in the temp table
idx_t temp_table_idx = DConstants::INVALID_INDEX;
if (sub_plan) {
	// Get the new table index that will be assigned to the temp table
	// Note: You may need to adjust this based on how your code tracks temp table indices
	// Option 1: If SubqueryPreparer tracks it
	temp_table_idx = subquery_preparer.GetNewTableIndex();

	// Option 2: If you track it elsewhere, get it from your tracking mechanism
	// temp_table_idx = ...; // your code to get the temp table index

	if (temp_table_idx != DConstants::INVALID_INDEX) {
		cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
		all_temp_table_indices.insert(temp_table_idx);

		#if ENABLE_DEBUG_PRINT
		if (execute_plan) {
			Printer::Print("Recorded equalities for temp table " + std::to_string(temp_table_idx));
		}
		#endif
	}
}
```

**Important Note**: You need to determine the correct way to get `temp_table_idx`. This depends on your implementation. Some options:

1. If `SubqueryPreparer` has a method to get the new table index, use it
2. If the temp table index is determined later when executing the sub-plan, you may need to record it at that point
3. If you create temp tables with a specific naming/indexing pattern, extract it from there

---

## Patch 4: Remove Redundant Filters

**Location**: Still in the query splitting loop, AFTER sub_plan execution but BEFORE merging

This should be placed after the sub_plan is executed and the temp table is created, but before you call `MergeSubquery()` or similar merging logic.

**Find a location like this** (the exact line number depends on your code):
```cpp
// ... code that executes sub_plan and creates temp table ...

// Before merging back into the main plan
```

**Add**:
```cpp
// Remove redundant cyclic join filters from the remaining plan
if (plan && !all_temp_table_indices.empty()) {
	bool filters_removed = cyclic_optimizer.RemoveRedundantFilters(*plan, all_temp_table_indices);

	#if ENABLE_DEBUG_PRINT
	if (execute_plan && filters_removed) {
		Printer::Print("Removed redundant cyclic join filters from remaining plan");
		Printer::Print("Plan after removing redundant filters:");
		plan->Print();
	}
	#endif
}
```

---

## Complete Code Block Example

Here's what the complete section should look like (around lines 490-670):

```cpp
SubqueryPreparer subquery_preparer(*planner.binder, *this);

// Initialize cyclic join optimizer to handle redundant join conditions
CyclicJoinOptimizer cyclic_optimizer;
std::unordered_set<idx_t> all_temp_table_indices;

// Collect all join conditions from the initial plan
if (plan) {
	cyclic_optimizer.CollectJoinConditions(*plan);

	#if ENABLE_DEBUG_PRINT
	if (execute_plan) {
		auto cycles = cyclic_optimizer.DetectCycles();
		if (!cycles.empty()) {
			Printer::Print("Detected " + std::to_string(cycles.size()) + " cyclic join pattern(s)");
		}
	}
	#endif
}

bool needToSplit = config.enable_dbshaker_query_split;
// ... rest of variable declarations ...

while (config.enable_dbshaker_query_split && !config.convert_ir_to_duckdb && execute_plan) {
	// ... existing code ...

	subquery_preparer.ClearOldTableIndex();
	subquery_preparer.AddOldTableIndex(subqueries.front()[0]);
	auto sub_plan = subquery_preparer.GenerateProjHead(plan, std::move(subqueries.front()[0]),
	                                                   table_expr_queue, proj_expr, merge_sibling_expr);

	// Track which equalities are guaranteed in the temp table
	idx_t temp_table_idx = DConstants::INVALID_INDEX;
	if (sub_plan) {
		temp_table_idx = subquery_preparer.GetNewTableIndex(); // Adjust as needed

		if (temp_table_idx != DConstants::INVALID_INDEX) {
			cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
			all_temp_table_indices.insert(temp_table_idx);

			#if ENABLE_DEBUG_PRINT
			if (execute_plan) {
				Printer::Print("Recorded equalities for temp table " + std::to_string(temp_table_idx));
			}
			#endif
		}
	}

	subqueries.pop_front();
	table_expr_queue.pop_front();

	// ... execute sub_plan and create temp table ...

	// Remove redundant cyclic join filters from the remaining plan
	if (plan && !all_temp_table_indices.empty()) {
		bool filters_removed = cyclic_optimizer.RemoveRedundantFilters(*plan, all_temp_table_indices);

		#if ENABLE_DEBUG_PRINT
		if (execute_plan && filters_removed) {
			Printer::Print("Removed redundant cyclic join filters from remaining plan");
			Printer::Print("Plan after removing redundant filters:");
			plan->Print();
		}
		#endif
	}

	// ... continue with merge logic ...
}
```

---

## Key Decision Point: Getting temp_table_idx

The **most critical part** is determining `temp_table_idx`. Here are your options:

### Option A: Extend SubqueryPreparer

Add a public method to `SubqueryPreparer`:

In `subquery_preparer.hpp`:
```cpp
public:
	idx_t GetNewTableIndex() const {
		return new_table_idx;
	}
```

Then use:
```cpp
temp_table_idx = subquery_preparer.GetNewTableIndex();
```

### Option B: Track It During Temp Table Creation

If the temp table is created later in your code, track the index at that point:

```cpp
// When you create the temp table:
auto temp_table = ...; // your temp table creation code
idx_t temp_table_idx = temp_table->table_index; // or however you get the index

// Then call:
cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
all_temp_table_indices.insert(temp_table_idx);
```

### Option C: Use MergeDataChunk Return Value

Looking at your code, `MergeDataChunk` might return the cardinality. You may need to check if it also provides or sets the table index.

---

## Verification Checklist

After integration, verify:

1. ✅ Code compiles without errors
2. ✅ `ENABLE_DEBUG_PRINT` shows detection messages for cyclic joins
3. ✅ Debug output shows "Removed redundant cyclic join filters" when applicable
4. ✅ Query results are still correct (no semantic changes)
5. ✅ Performance improves (fewer redundant filter operations)

---

## Troubleshooting

### Compile Error: "GetNewTableIndex not found"

- Add the method to `SubqueryPreparer` (see Option A above)
- Or track the index differently (see Option B or C)

### No Filters Removed

- Check that `temp_table_idx` is being set correctly
- Verify that `RecordTempTableEqualities` is being called
- Enable debug printing to see what's happening
- Check that the remaining plan actually has redundant filters

### Wrong Filters Removed

- Verify that the column index mapping is correct
- Check that `temp_table_idx` matches the actual temp table created
- Review the logic in `RecordTempTableEqualities` to ensure it's tracking the right columns

---

## Testing Recommendations

Create a test query with a clear cyclic join:

```sql
SELECT *
FROM R, S, T
WHERE R.r = S.s
  AND S.s = T.t
  AND T.t = R.r;
```

With query splitting:
1. First sub-plan should execute `R JOIN S ON R.r = S.s`
2. Remaining plan should have `temp JOIN T ON temp.s = T.t`
3. The redundant condition `temp.r = temp.s` should be removed

Verify by checking the debug output and the final execution plan.
