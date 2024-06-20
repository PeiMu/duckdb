add_library_unity(
  duckdb_optimizer_join_push_down
  OBJECT
        join_push_down.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer_join_push_down>
    PARENT_SCOPE)
