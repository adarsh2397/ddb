//===----------------------------------------------------------------------===//
//                         DuckDB
//
// gcs_grpc_file_system.hpp
//
// FileSystem adapter exposing the GCS gRPC reactor to DuckDB. Handles
// gs:// and gcs:// paths (read-only) when gcs_grpc_enabled is set.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "gcs_grpc_reactor.hpp"

#include <mutex>

namespace duckdb {

class GcsGrpcFileSystem;

class GcsGrpcFileHandle : public FileHandle {
public:
	GcsGrpcFileHandle(FileSystem &fs, string path, FileOpenFlags flags,
	                  gcs_grpc::gcs_grpc_native_handle state);
	~GcsGrpcFileHandle() override;

	void Close() override {
	}

public:
	//! Immutable object descriptor (bucket, key, size, generation pinned at open).
	gcs_grpc::gcs_grpc_native_handle state;
	//! Cursor for sequential (non-positional) reads.
	idx_t file_offset = 0;
	//! Throughput bookkeeping for TryGetNetworkThroughput.
	std::atomic<uint64_t> bytes_read {0};
	std::atomic<uint64_t> read_nanos {0};
};

class GcsGrpcFileSystem : public FileSystem {
public:
	explicit GcsGrpcFileSystem(DatabaseInstance &db);
	~GcsGrpcFileSystem() override;

	bool CanHandleFile(const string &fpath) override;
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool TryGetNetworkThroughput(FileHandle &handle, NetworkThroughputEstimate &result) override;

	std::string GetName() const override {
		return "GcsGrpcFileSystem";
	}

private:
	//! Lazily construct the shared reactor from the current settings.
	std::shared_ptr<gcs_grpc::gcs_grpc_reactor> GetOrCreateReactor(optional_ptr<FileOpener> opener);
	//! Parse gs://bucket/key or gcs://bucket/key.
	static void ParsePath(const string &path, string &bucket, string &key);
	//! Blocking read of one contiguous range through the async reactor.
	void ReadRange(GcsGrpcFileHandle &handle, void *buffer, idx_t nr_bytes, idx_t location);

private:
	DatabaseInstance &db;
	std::mutex reactor_lock;
	std::shared_ptr<gcs_grpc::gcs_grpc_reactor> reactor;
};

} // namespace duckdb
