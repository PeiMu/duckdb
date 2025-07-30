add_library_unity(
  duckdb_plan_converter
  OBJECT
        read.cpp
        ir_to_duckdb.cpp
        duckdb_to_ir.cpp
        ir_to_sql.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_plan_converter>
    PARENT_SCOPE)
