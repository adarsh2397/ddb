cmake_minimum_required(VERSION 3.18)

project(GcsGrpcExtension)

include(${CMAKE_CURRENT_SOURCE_DIR}/gcs_grpc_proto.cmake)

include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/httplib)

set(GCS_GRPC_EXTENSION_FILES gcs_grpc_extension.cpp gcs_grpc_file_system.cpp
                             gcs_grpc_reactor.cpp gcs_grpc_auth.cpp)

build_static_extension(gcs_grpc ${GCS_GRPC_EXTENSION_FILES})
target_link_libraries(gcs_grpc_extension gcs_storage_protos)
# The ported reactor uses C++17; DuckDB core stays on its default standard.
set_target_properties(gcs_grpc_extension PROPERTIES CXX_STANDARD 17)

install(
  TARGETS gcs_grpc_extension gcs_storage_protos
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
