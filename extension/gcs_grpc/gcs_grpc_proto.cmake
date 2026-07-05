# Generates C++ + gRPC stubs for the Cloud Storage gRPC API
# (google.storage.v2) used by the gcs_grpc extension. Produces a static library
# target `gcs_storage_protos` carrying the generated sources, linked against
# the gRPC and protobuf runtimes.
#
# storage.proto has a large transitive import graph (google/api/*,
# google/rpc/*, google/type/*, google/iam/v1/*, plus protobuf well-known
# types). We fetch the googleapis repo at a pinned commit and run protoc over
# the curated subset of protos that storage.proto pulls in. If protoc reports
# an unresolved import, add the named .proto to GCS_GRPC_PROTO_FILES below.

include(FetchContent)

# Protobuf MUST be found in CONFIG mode, and BEFORE gRPC. The legacy
# FindProtobuf module defines only a PARTIAL protobuf::* imported-target set;
# modern gRPCConfig then runs find_dependency(protobuf CONFIG), whose
# protobuf-targets.cmake sees that partial set and aborts with
#   "Some (but not all) targets in this export set were already defined."
# MODULE_COMPATIBLE keeps the legacy Protobuf_* variables available.
set(protobuf_MODULE_COMPATIBLE ON)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

if(gRPC_VERSION AND gRPC_VERSION VERSION_LESS "1.62")
  message(
    FATAL_ERROR
      "gcs_grpc requires gRPC >= 1.62 (RLS-based Rapid Storage redirect routing); found ${gRPC_VERSION}"
  )
endif()

# protoc + grpc_cpp_plugin come from the same toolchain as the runtime libs
# (conda-forge libgrpc / vcpkg grpc). Prefer the imported targets when present.
if(TARGET protobuf::protoc)
  set(_GCS_PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
  find_program(_GCS_PROTOC protoc REQUIRED)
endif()
if(TARGET gRPC::grpc_cpp_plugin)
  set(_GCS_GRPC_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  find_program(_GCS_GRPC_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

# Pinned googleapis snapshot. Bump deliberately; the storage.proto surface
# (ReadObject / BidiReadObject) is stable but field numbers must match runtime.
FetchContent_Declare(
  googleapis
  GIT_REPOSITORY https://github.com/googleapis/googleapis.git
  GIT_TAG ba4573adabf42e4e0bb9877f11de8c8f1005aa0a
  GIT_SHALLOW FALSE)
FetchContent_MakeAvailable(googleapis)

set(_GCS_PROTO_ROOT ${googleapis_SOURCE_DIR})
set(_GCS_PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/gcs_grpc_gen)
file(MAKE_DIRECTORY ${_GCS_PROTO_OUT})

# storage.proto annotates several bytes fields (notably ChecksummedData.content)
# with `[ctype = CORD]`. Protobuf builds without Cord support (the common
# conda-forge / vcpkg case) generate the std::string content() accessor as
# PRIVATE — making the field unreadable. Strip just the `ctype = CORD`
# directive in-place before protoc runs so the generated accessor is a plain
# public `const std::string& content()`. The four passes cover sole / first /
# middle / last positions in the field-option list.
file(READ ${_GCS_PROTO_ROOT}/google/storage/v2/storage.proto _gcs_storage_proto_src)
string(REGEX REPLACE "\\[ *ctype *= *CORD *\\]" ""  _gcs_storage_proto_src "${_gcs_storage_proto_src}")
string(REGEX REPLACE ", *ctype *= *CORD *,"      "," _gcs_storage_proto_src "${_gcs_storage_proto_src}")
string(REGEX REPLACE "\\[ *ctype *= *CORD *, *"  "[" _gcs_storage_proto_src "${_gcs_storage_proto_src}")
string(REGEX REPLACE ", *ctype *= *CORD *\\]"    "]" _gcs_storage_proto_src "${_gcs_storage_proto_src}")
file(WRITE ${_GCS_PROTO_ROOT}/google/storage/v2/storage.proto "${_gcs_storage_proto_src}")

# Curated subset of protos to generate. storage.proto is the only one needing
# the grpc plugin (it defines the Storage service); the rest are message-only
# dependencies.
set(GCS_GRPC_PROTO_FILES
    google/storage/v2/storage.proto
    google/api/annotations.proto
    google/api/client.proto
    google/api/field_behavior.proto
    google/api/launch_stage.proto
    google/api/http.proto
    google/api/resource.proto
    google/api/routing.proto
    google/rpc/status.proto
    google/type/date.proto
    google/type/expr.proto
    google/iam/v1/iam_policy.proto
    google/iam/v1/policy.proto
    google/iam/v1/options.proto)

set(_GCS_GEN_SRCS "")
foreach(_proto ${GCS_GRPC_PROTO_FILES})
  get_filename_component(_dir ${_proto} DIRECTORY)
  get_filename_component(_name ${_proto} NAME_WE)
  set(_pb_cc ${_GCS_PROTO_OUT}/${_dir}/${_name}.pb.cc)
  set(_pb_h ${_GCS_PROTO_OUT}/${_dir}/${_name}.pb.h)
  list(APPEND _GCS_GEN_SRCS ${_pb_cc})

  # Only storage.proto needs the gRPC service stub.
  set(_grpc_args "")
  set(_grpc_out "")
  if(_proto STREQUAL "google/storage/v2/storage.proto")
    set(_grpc_cc ${_GCS_PROTO_OUT}/${_dir}/${_name}.grpc.pb.cc)
    list(APPEND _GCS_GEN_SRCS ${_grpc_cc})
    set(_grpc_args --grpc_out=${_GCS_PROTO_OUT}
                   --plugin=protoc-gen-grpc=${_GCS_GRPC_PLUGIN})
    set(_grpc_out ${_grpc_cc})
  endif()

  add_custom_command(
    OUTPUT ${_pb_cc} ${_pb_h} ${_grpc_out}
    COMMAND ${_GCS_PROTOC} --proto_path=${_GCS_PROTO_ROOT}
            --cpp_out=${_GCS_PROTO_OUT} ${_grpc_args} ${_proto}
    DEPENDS ${_GCS_PROTO_ROOT}/${_proto}
    COMMENT "protoc (gcs grpc): ${_proto}"
    VERBATIM)
endforeach()

add_library(gcs_storage_protos STATIC ${_GCS_GEN_SRCS})
# Wrap the generated-headers dir in $<BUILD_INTERFACE:> — gcs_storage_protos is
# in DuckDB's export set and CMake forbids exporting a raw build-directory path
# on a target's include interface. The headers are only consumed at build time.
target_include_directories(gcs_storage_protos PUBLIC $<BUILD_INTERFACE:${_GCS_PROTO_OUT}>)
target_link_libraries(gcs_storage_protos PUBLIC gRPC::grpc++ protobuf::libprotobuf)
set_target_properties(gcs_storage_protos PROPERTIES CXX_STANDARD 17 POSITION_INDEPENDENT_CODE ON)
# Generated protobuf code triggers these under -Werror; quiet them locally.
target_compile_options(gcs_storage_protos PRIVATE -Wno-unused-parameter)
