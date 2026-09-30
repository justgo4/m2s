cmake_minimum_required(VERSION 3.20)
project(mysql_arrow_reader C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include(FetchContent)
FetchContent_Declare(
  nanoarrow
  GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
  GIT_TAG ec8a58cae18beaa241c7fea7cb26816ac27c280f
  GIT_SHALLOW FALSE
)
set(NANOARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(NANOARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(NANOARROW_IPC ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(nanoarrow)

add_executable(mysql_arrow_reader mysql_arrow_reader.c)
target_link_libraries(mysql_arrow_reader PRIVATE nanoarrow_static nanoarrow_ipc_static)
target_compile_options(mysql_arrow_reader PRIVATE -O3 -Wall -Wextra -Werror)

add_library(j4_native SHARED j4_native.c)
target_compile_options(j4_native PRIVATE
  -O3 -Wall -Wextra -Werror
  -march=x86-64 -mtune=generic
  -fvisibility=hidden -fno-stack-protector -fno-builtin
  -fno-tree-loop-distribute-patterns
)
target_link_options(j4_native PRIVATE
  -nostdlib -nodefaultlibs
  -Wl,--no-undefined -Wl,-z,relro,-z,now
)
set_target_properties(j4_native PROPERTIES
  OUTPUT_NAME j4_native
  VERSION 3.0.0
  SOVERSION 3
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)

install(TARGETS mysql_arrow_reader RUNTIME DESTINATION bin)
install(TARGETS j4_native LIBRARY DESTINATION lib)
