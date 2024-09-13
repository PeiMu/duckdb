add_library_unity(
  duckdb_plan_converter
  OBJECT
        read.cpp
        readfuncs.cpp
        ir_to_duckdb.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_plan_converter>
    PARENT_SCOPE)
