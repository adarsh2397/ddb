#include "gcs_grpc_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>
#include <condition_variable>

namespace duckdb {

namespace {

bool HasGcsPrefix(const string &path) {
	return StringUtil::StartsWith(path, "gs://") || StringUtil::StartsWith(path, "gcs://");
}

//! Read a setting via the opener when present, falling back to the database.
Value GetSetting(optional_ptr<FileOpener> opener, DatabaseInstance &db, const string &name, Value default_value) {
	Value result;
	if (opener && FileOpener::TryGetCurrentSetting(opener, name, result)) {
		return result;
	}
	if (db.TryGetCurrentSetting(name, result)) {
		return result;
	}
	return default_value;
}

gcs_grpc::gcs_bidi_mode ParseBidiMode(const string &mode) {
	auto lower = StringUtil::Lower(mode);
	if (lower == "off") {
		return gcs_grpc::gcs_bidi_mode::off;
	}
	if (lower == "on") {
		return gcs_grpc::gcs_bidi_mode::on;
	}
	if (lower == "auto") {
		return gcs_grpc::gcs_bidi_mode::automatic;
	}
	throw InvalidInputException("gcs_grpc_bidi_reads must be one of 'off', 'on', 'auto' (got '%s')", mode);
}

} // namespace

//===--------------------------------------------------------------------===//
// GcsGrpcFileHandle
//===--------------------------------------------------------------------===//

GcsGrpcFileHandle::GcsGrpcFileHandle(FileSystem &fs, string path_p, FileOpenFlags flags,
                                     gcs_grpc::gcs_grpc_native_handle state_p)
    : FileHandle(fs, std::move(path_p), flags), state(std::move(state_p)) {
}

GcsGrpcFileHandle::~GcsGrpcFileHandle() {
}

//===--------------------------------------------------------------------===//
// GcsGrpcFileSystem
//===--------------------------------------------------------------------===//

GcsGrpcFileSystem::GcsGrpcFileSystem(DatabaseInstance &db_p) : db(db_p) {
}

GcsGrpcFileSystem::~GcsGrpcFileSystem() {
	if (reactor) {
		reactor->shutdown();
	}
}

bool GcsGrpcFileSystem::CanHandleFile(const string &fpath) {
	if (!HasGcsPrefix(fpath)) {
		return false;
	}
	// Disabled -> fall through to httpfs autoload.
	Value enabled;
	if (db.TryGetCurrentSetting("gcs_grpc_enabled", enabled)) {
		return enabled.GetValue<bool>();
	}
	return true;
}

void GcsGrpcFileSystem::ParsePath(const string &path, string &bucket, string &key) {
	idx_t proto_len;
	if (StringUtil::StartsWith(path, "gs://")) {
		proto_len = 5;
	} else if (StringUtil::StartsWith(path, "gcs://")) {
		proto_len = 6;
	} else {
		throw IOException("gcs_grpc: not a GCS path: '%s'", path);
	}
	auto slash = path.find('/', proto_len);
	if (slash == string::npos || slash == proto_len || slash + 1 >= path.size()) {
		throw IOException("gcs_grpc: expected gs://<bucket>/<object>, got '%s'", path);
	}
	bucket = path.substr(proto_len, slash - proto_len);
	key = path.substr(slash + 1);
}

std::shared_ptr<gcs_grpc::gcs_grpc_reactor> GcsGrpcFileSystem::GetOrCreateReactor(optional_ptr<FileOpener> opener) {
	std::lock_guard<std::mutex> guard(reactor_lock);
	if (reactor) {
		return reactor;
	}

	gcs_grpc::gcs_grpc_reactor::config cfg;
	auto transport = GetSetting(opener, db, "gcs_grpc_transport", Value("directpath")).GetValue<string>();
	auto transport_lower = StringUtil::Lower(transport);
	if (transport_lower == "directpath") {
		cfg.directpath = true;
	} else if (transport_lower == "cloudpath") {
		cfg.directpath = false;
	} else {
		throw InvalidInputException("gcs_grpc_transport must be 'directpath' or 'cloudpath' (got '%s')", transport);
	}
	cfg.endpoint = GetSetting(opener, db, "gcs_grpc_endpoint", Value("storage.googleapis.com")).GetValue<string>();
	cfg.num_channels = GetSetting(opener, db, "gcs_grpc_num_channels", Value::UBIGINT(4)).GetValue<uint64_t>();
	cfg.max_streams = GetSetting(opener, db, "gcs_grpc_max_streams", Value::UBIGINT(16)).GetValue<uint64_t>();
	cfg.bidi_reads = ParseBidiMode(GetSetting(opener, db, "gcs_grpc_bidi_reads", Value("auto")).GetValue<string>());
	cfg.target_read_bytes =
	    GetSetting(opener, db, "gcs_grpc_target_read_bytes", Value::UBIGINT(16ULL << 20)).GetValue<uint64_t>();
	cfg.verbose = GetSetting(opener, db, "gcs_grpc_verbose", Value::BOOLEAN(false)).GetValue<bool>();

	auto bearer = GetSetting(opener, db, "gcs_grpc_bearer_token", Value("")).GetValue<string>();
	if (!bearer.empty()) {
		cfg.creds = std::make_shared<gcs_grpc::StaticTokenProvider>(bearer);
	} else {
		cfg.creds = std::make_shared<gcs_grpc::MetadataServerTokenProvider>();
	}

	reactor = std::make_shared<gcs_grpc::gcs_grpc_reactor>(cfg);
	return reactor;
}

unique_ptr<FileHandle> GcsGrpcFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                   optional_ptr<FileOpener> opener) {
	if (flags.OpenForWriting() || flags.OpenForAppending()) {
		throw NotImplementedException("gcs_grpc: writing to GCS is not supported; "
		                              "SET gcs_grpc_enabled=false to use httpfs");
	}
	string bucket, key;
	ParsePath(path, bucket, key);

	auto r = GetOrCreateReactor(opener);
	gcs_grpc::gcs_grpc_object_state state;
	try {
		state = r->stat_object(bucket, key);
	} catch (std::exception &ex) {
		throw IOException("gcs_grpc: failed to open '%s': %s", path, ex.what());
	}

	auto native = std::make_shared<const gcs_grpc::gcs_grpc_object_state>(std::move(state));
	auto handle = make_uniq<GcsGrpcFileHandle>(*this, path, flags, std::move(native));
	if (opener) {
		handle->TryAddLogger(*opener);
	}
	return std::move(handle);
}

void GcsGrpcFileSystem::ReadRange(GcsGrpcFileHandle &handle, void *buffer, idx_t nr_bytes, idx_t location) {
	if (nr_bytes == 0) {
		return;
	}
	auto size = handle.state->object_size;
	if (location + nr_bytes > size) {
		throw IOException("gcs_grpc: read of %llu bytes at offset %llu is out of bounds for '%s' (%llu bytes)",
		                  nr_bytes, location, handle.path, size);
	}

	// Bridge the async reactor to DuckDB's synchronous Read: submit one range
	// and block on its completion. Concurrent reads from other threads keep the
	// lanes/bidi streams saturated; the reactor splits large ranges internally.
	std::mutex m;
	std::condition_variable cv;
	bool done = false;
	std::exception_ptr error;

	gcs_grpc::host_read_req req;
	req.handle = handle.state;
	req.offset = location;
	req.size = nr_bytes;
	req.dst = static_cast<uint8_t *>(buffer);
	req.ctx = gcs_grpc::request_context::create(1, nr_bytes, [&](size_t, std::exception_ptr ep) {
		std::lock_guard<std::mutex> guard(m);
		error = ep;
		done = true;
		cv.notify_one();
	});

	auto start = std::chrono::steady_clock::now();
	{
		std::lock_guard<std::mutex> guard(reactor_lock);
		if (!reactor) {
			throw IOException("gcs_grpc: reactor is not initialized");
		}
	}
	reactor->host_read_async(std::move(req));

	std::unique_lock<std::mutex> lk(m);
	cv.wait(lk, [&] { return done; });
	if (error) {
		std::rethrow_exception(error);
	}

	auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start);
	handle.bytes_read.fetch_add(nr_bytes, std::memory_order_relaxed);
	handle.read_nanos.fetch_add(static_cast<uint64_t>(nanos.count()), std::memory_order_relaxed);
}

void GcsGrpcFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &gh = handle.Cast<GcsGrpcFileHandle>();
	ReadRange(gh, buffer, static_cast<idx_t>(nr_bytes), location);
}

int64_t GcsGrpcFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &gh = handle.Cast<GcsGrpcFileHandle>();
	auto size = gh.state->object_size;
	if (gh.file_offset >= size) {
		return 0;
	}
	auto to_read = MinValue<idx_t>(static_cast<idx_t>(nr_bytes), size - gh.file_offset);
	ReadRange(gh, buffer, to_read, gh.file_offset);
	gh.file_offset += to_read;
	return static_cast<int64_t>(to_read);
}

int64_t GcsGrpcFileSystem::GetFileSize(FileHandle &handle) {
	return static_cast<int64_t>(handle.Cast<GcsGrpcFileHandle>().state->object_size);
}

FileType GcsGrpcFileSystem::GetFileType(FileHandle &handle) {
	return FileType::FILE_TYPE_REGULAR;
}

bool GcsGrpcFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	string bucket, key;
	try {
		ParsePath(filename, bucket, key);
		auto r = GetOrCreateReactor(opener);
		r->stat_object(bucket, key);
		return true;
	} catch (...) {
		return false;
	}
}

vector<OpenFileInfo> GcsGrpcFileSystem::Glob(const string &path, FileOpener *opener) {
	if (!HasGlob(path)) {
		// Exact remote path: return as-is (existence surfaces at open).
		return {OpenFileInfo(path)};
	}
	throw NotImplementedException("gcs_grpc: glob patterns are not supported yet; use exact paths or "
	                              "SET gcs_grpc_enabled=false to glob via httpfs");
	// Follow-up: google.storage.v2.Storage.ListObjects supports a match_glob
	// field for server-side globbing.
}

void GcsGrpcFileSystem::Seek(FileHandle &handle, idx_t location) {
	handle.Cast<GcsGrpcFileHandle>().file_offset = location;
}

void GcsGrpcFileSystem::Reset(FileHandle &handle) {
	handle.Cast<GcsGrpcFileHandle>().file_offset = 0;
}

idx_t GcsGrpcFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<GcsGrpcFileHandle>().file_offset;
}

bool GcsGrpcFileSystem::TryGetNetworkThroughput(FileHandle &handle, NetworkThroughputEstimate &result) {
	auto &gh = handle.Cast<GcsGrpcFileHandle>();
	auto bytes = gh.bytes_read.load(std::memory_order_relaxed);
	auto nanos = gh.read_nanos.load(std::memory_order_relaxed);
	// Require a minimum sample before reporting an estimate.
	if (bytes < 8ULL << 20 || nanos == 0) {
		return false;
	}
	result.latency_seconds = 0.001; // sub-ms within a zone; nominal 1ms
	result.bandwidth_bytes_per_s = static_cast<double>(bytes) / (static_cast<double>(nanos) / 1e9);
	return true;
}

} // namespace duckdb
