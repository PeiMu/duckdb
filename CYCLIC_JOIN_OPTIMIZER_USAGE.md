# Cyclic Join Optimizer - Usage Guide

## Overview

The `CyclicJoinOptimizer` helps eliminate redundant join conditions that arise when query splitting creates cyclic join patterns.

### The Problem

When you have a cyclic join pattern like:
```sql
R.r = S.s AND S.s = T.t AND T.t = R.r
```

And you split the query into:
1. **Sub-plan**: `R JOIN S ON R.r = S.s` → creates `temp1(r, s)` where `r = s` is guaranteed
2. **Remaining plan**: `temp1 JOIN T ON temp1.s = T.t AND T.t = temp1.r`

The problem is that the remaining plan might add a redundant filter `WHERE temp1.r = temp1.s`, which is unnecessary because this equality is already guaranteed by the sub-plan's join.

### The Solution

The `CyclicJoinOptimizer` class:
1. **Detects** cyclic join conditions in the original plan
2. **Tracks** which column equalities are guaranteed in each temp table created from sub-plans
3. **Removes** redundant filters from the remaining plan that check equalities already guaranteed in temp tables

---

## API Reference

### Class: `CyclicJoinOptimizer`

Located in: `src/include/duckdb/optimizer/query_split/cyclic_join_optimizer.hpp`

#### Methods

##### 1. `void CollectJoinConditions(LogicalOperator &op)`

Collects all equality join conditions from the logical plan.

**When to call**: Before query splitting begins, on the initial plan after `PreOptimize()`.

**Example**:
```cpp
CyclicJoinOptimizer cyclic_optimizer;
cyclic_optimizer.CollectJoinConditions(*plan);
```

---

##### 2. `vector<JoinCycle> DetectCycles()`

Detects cycles in the collected join conditions.

**When to call**: After `CollectJoinConditions()`, for debugging or logging.

**Returns**: A vector of `JoinCycle` objects, each representing a detected cycle.

**Example**:
```cpp
auto cycles = cyclic_optimizer.DetectCycles();
if (!cycles.empty()) {
    Printer::Print("Found " + std::to_string(cycles.size()) + " cyclic join patterns");
}
```

---

##### 3. `void RecordTempTableEqualities(idx_t temp_table_idx, const LogicalOperator &sub_plan)`

Records which column pairs are guaranteed equal in a temp table based on the joins in the sub-plan.

**When to call**: After creating a sub-plan but before executing it.

**Parameters**:
- `temp_table_idx`: The table index that will be assigned to the temp table
- `sub_plan`: The logical plan that will be executed to create the temp table

**Example**:
```cpp
auto sub_plan = subquery_preparer.GenerateProjHead(...);
idx_t temp_table_idx = subquery_preparer.GetNewTableIndex();
cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
```

---

##### 4. `bool RemoveRedundantFilters(LogicalOperator &op, const std::unordered_set<idx_t> &temp_table_indices)`

Removes redundant filters from the plan that check equalities already guaranteed in temp tables.

**When to call**: After executing a sub-plan and creating a temp table, but **before** merging it back into the remaining plan.

**Parameters**:
- `op`: The remaining logical plan to clean up
- `temp_table_indices`: Set of all temp table indices created so far

**Returns**: `true` if any filters were removed, `false` otherwise

**Example**:
```cpp
std::unordered_set<idx_t> temp_tables = {temp_table_idx};
bool modified = cyclic_optimizer.RemoveRedundantFilters(*plan, temp_tables);
```

---

##### 5. `const vector<std::pair<idx_t, idx_t>> *GetTempTableEqualities(idx_t table_idx) const`

Gets the equality guarantees for a specific temp table.

**Returns**: Pointer to a vector of column index pairs that are guaranteed equal, or `nullptr` if none.

**Example**:
```cpp
auto equalities = cyclic_optimizer.GetTempTableEqualities(temp_table_idx);
if (equalities) {
    for (auto &eq : *equalities) {
        // eq.first and eq.second are column indices that are guaranteed equal
    }
}
```

---

## Integration Guide

### Step 1: Add Include

At the top of `client_context.cpp`:

```cpp
#include "duckdb/optimizer/query_split/cyclic_join_optimizer.hpp"
```

### Step 2: Initialize Optimizer

In `CreatePreparedStatementInternal`, after `PreOptimize()` but before the query splitting loop:

```cpp
// Around line 489-490 in client_context.cpp
SubqueryPreparer subquery_preparer(*planner.binder, *this);

// Add this:
CyclicJoinOptimizer cyclic_optimizer;
if (plan) {
    cyclic_optimizer.CollectJoinConditions(*plan);
}
```

### Step 3: Track Temp Table Equalities

In the query splitting loop, after creating `sub_plan`:

```cpp
// Around line 647-648
auto sub_plan = subquery_preparer.GenerateProjHead(plan, std::move(subqueries.front()[0]),
                                                   table_expr_queue, proj_expr, merge_sibling_expr);

// Add this:
idx_t temp_table_idx = subquery_preparer.GetNewTableIndex(); // Adjust based on your implementation
if (sub_plan) {
    cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
}
```

### Step 4: Remove Redundant Filters

After executing the sub_plan and creating the temp table, but before merging:

```cpp
// After executing sub_plan but before MergeSubquery
std::unordered_set<idx_t> temp_table_indices = {temp_table_idx};
if (plan) {
    cyclic_optimizer.RemoveRedundantFilters(*plan, temp_table_indices);
}
```

---

## Complete Example

```cpp
// In CreatePreparedStatementInternal function:

// After PreOptimize (line ~466):
plan = optimizer.PreOptimize(std::move(plan));

// Initialize cyclic join optimizer
CyclicJoinOptimizer cyclic_optimizer;
std::unordered_set<idx_t> all_temp_tables;

if (plan) {
    cyclic_optimizer.CollectJoinConditions(*plan);
}

// In the query splitting loop (around line 549):
while (config.enable_dbshaker_query_split && !config.convert_ir_to_duckdb && execute_plan) {
    // ... existing code ...

    // After creating sub_plan:
    auto sub_plan = subquery_preparer.GenerateProjHead(
        plan, std::move(subqueries.front()[0]),
        table_expr_queue, proj_expr, merge_sibling_expr
    );

    // Get temp table index (you need to determine how to get this)
    idx_t temp_table_idx = subquery_preparer.GetNewTableIndex();

    // Record equalities in this sub_plan
    if (sub_plan) {
        cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
        all_temp_tables.insert(temp_table_idx);
    }

    // ... execute sub_plan and create temp table ...

    // Remove redundant filters from remaining plan BEFORE merging
    if (plan) {
        bool modified = cyclic_optimizer.RemoveRedundantFilters(*plan, all_temp_tables);

        #if ENABLE_DEBUG_PRINT
        if (execute_plan && modified) {
            Printer::Print("Removed redundant cyclic join filters");
            plan->Print();
        }
        #endif
    }

    // ... continue with merge logic ...
}
```

---

## Which Edge to Remove?

For a cycle like `R.r = S.s AND S.s = T.t AND T.t = R.r`:

**Remove the edge that closes the cycle** - typically `T.t = R.r`.

### Rationale:

1. **Preserves join order flexibility**: Keeping `R.r = S.s` and `S.s = T.t` allows the optimizer to choose:
   - Join R and S first, then T
   - Join S and T first, then R

2. **Removes redundant closure**: The first two edges already establish the transitive relationship.

3. **Chain structure**: `R → S → T` is easier for the join order optimizer than a triangle.

### The Optimizer Handles This Automatically

The `RemoveRedundantFilters()` method will automatically detect and remove the redundant edge based on which equalities are guaranteed in temp tables. You don't need to manually decide which edge to remove.

---

## Important Notes

1. **Temp Table Index Tracking**: You must correctly track which table index will be assigned to temp tables. This may require extending `SubqueryPreparer` with a `GetNewTableIndex()` method.

2. **Column Index Mapping**: The current implementation uses a simplified mapping. For production use, you may need to track the exact column positions in temp tables based on the projection.

3. **Timing**: Call `RemoveRedundantFilters()` **after** the temp table is created but **before** merging it back into the remaining plan.

4. **Multiple Temp Tables**: Maintain a set of all temp table indices created throughout query splitting, and pass them all to `RemoveRedundantFilters()`.

---

## Testing

To verify the optimizer is working:

1. Enable debug printing:
   ```cpp
   #define ENABLE_DEBUG_PRINT 1
   ```

2. Run a query with a cyclic join pattern

3. Check the output:
   - You should see "Detected N cyclic join pattern(s)" after `CollectJoinConditions()`
   - You should see "Removed redundant cyclic join filters" after `RemoveRedundantFilters()`
   - The printed plan should show fewer filter operations

---

## Files Modified

1. **New files**:
   - `src/include/duckdb/optimizer/query_split/cyclic_join_optimizer.hpp`
   - `src/optimizer/query_split/cyclic_join_optimizer.cpp`

2. **Modified files**:
   - `src/optimizer/query_split/CMakeLists.txt` (added `cyclic_join_optimizer.cpp`)
   - `src/main/client_context.cpp` (integration code)

---

## Future Improvements

1. **Better Column Mapping**: Track exact column positions in temp tables based on projections
2. **Cost-Based Selection**: Instead of always removing the closing edge, estimate selectivity and remove the least selective edge
3. **Multi-Cycle Support**: Handle queries with multiple independent cycles
4. **Nested Cycle Detection**: Handle cycles that span across multiple query split levels
