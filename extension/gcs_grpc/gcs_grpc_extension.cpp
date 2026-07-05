#include "gcs_grpc_extension.hpp"

#include "gcs_grpc_file_system.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("gcs_grpc_enabled",
	                          "Handle gs:// paths with the gRPC transport (false falls back to httpfs)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("gcs_grpc_transport",
	                          "GCS gRPC transport: 'directpath' (google-c2p resolver + ALTS, GCE only) or "
	                          "'cloudpath' (TLS + OAuth2 bearer token)",
	                          LogicalType::VARCHAR, Value("directpath"));
	config.AddExtensionOption("gcs_grpc_endpoint", "GCS gRPC endpoint host", LogicalType::VARCHAR,
	                          Value("storage.googleapis.com"));
	config.AddExtensionOption("gcs_grpc_num_channels",
	                          "Number of gRPC channels (each with its own TCP connection + worker thread)",
	                          LogicalType::UBIGINT, Value::UBIGINT(4));
	config.AddExtensionOption("gcs_grpc_max_streams", "Total concurrent streams in flight (split across channels)",
	                          LogicalType::UBIGINT, Value::UBIGINT(16));
	config.AddExtensionOption("gcs_grpc_bidi_reads",
	                          "BidiReadObject (Rapid Storage fast path) policy: 'off', 'on' or 'auto'",
	                          LogicalType::VARCHAR, Value("auto"));
	config.AddExtensionOption("gcs_grpc_target_read_bytes",
	                          "Split large reads into sub-reads of this size for cross-stream parallelism",
	                          LogicalType::UBIGINT, Value::UBIGINT(16ULL << 20));
	config.AddExtensionOption("gcs_grpc_bearer_token",
	                          "Static OAuth2 bearer token override (default: GCE metadata server)",
	                          LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption("gcs_grpc_verbose", "Emit gcs_grpc transport logs to stderr", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false));

	db.GetFileSystem().RegisterSubSystem(make_uniq<GcsGrpcFileSystem>(db));
}

void GcsGrpcExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GcsGrpcExtension::Name() {
	return "gcs_grpc";
}

std::string GcsGrpcExtension::Version() const {
#ifdef EXT_VERSION_GCS_GRPC
	return EXT_VERSION_GCS_GRPC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(gcs_grpc, loader) {
	duckdb::LoadInternal(loader);
}
}
