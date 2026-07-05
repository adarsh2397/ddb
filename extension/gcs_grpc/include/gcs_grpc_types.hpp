//===----------------------------------------------------------------------===//
//                         DuckDB
//
// gcs_grpc_types.hpp
//
// Minimal request/completion types for the GCS gRPC reactor, ported from the
// Sirius io layer (sirius/src/include/io/types.hpp) with the CUDA/cudf parts
// removed.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace duckdb {
namespace gcs_grpc {

/// Completion handler for async I/O.
/// @param bytes_transferred  Total bytes read on success.
/// @param ep                 Non-null on failure.
using io_completion_handler = std::function<void(size_t bytes_transferred, std::exception_ptr ep)>;

/// Immutable per-object descriptor carried (by shared_ptr) in every read
/// request so the object outlives the async transfer.
struct gcs_grpc_object_state {
	std::string bucket; ///< bare bucket name (resource path built at call time)
	std::string key;    ///< object name
	std::size_t object_size {0};
	std::int64_t generation {0}; ///< pinned read generation (0 = latest)
};

using gcs_grpc_native_handle = std::shared_ptr<const gcs_grpc_object_state>;

/// A destination fragment for scatter-gather reads.
struct host_span {
	std::byte *ptr {nullptr};
	std::size_t len {0};

	std::byte *data() const noexcept {
		return ptr;
	}
	std::size_t size() const noexcept {
		return len;
	}
};

/**
 * Shared completion state for one logical read call. A single read may be
 * split into multiple sub-requests; all sub-requests decrement `pending` and
 * the last one resolves the handler exactly once (CAS-guarded, with a
 * destructor safety net so a dropped sub-request fails loudly instead of
 * deadlocking the caller).
 */
struct request_context {
private:
	struct create_passkey {};

public:
	explicit request_context(create_passkey) noexcept {
	}

	request_context(request_context const &) = delete;
	request_context &operator=(request_context const &) = delete;

	[[nodiscard]] static std::shared_ptr<request_context> create(size_t n_chunks, size_t total_bytes,
	                                                             io_completion_handler handler) {
		if (n_chunks == 0) {
			if (handler) {
				try {
					handler(0, nullptr);
				} catch (...) { // NOLINT: zero-work path, nowhere to propagate
				}
			}
			return nullptr;
		}
		if (!handler) {
			throw std::invalid_argument("request_context::create: handler is null but n_chunks > 0");
		}
		auto ctx = std::make_shared<request_context>(create_passkey {});
		ctx->handler = std::move(handler);
		ctx->total_bytes = total_bytes;
		ctx->pending.store(n_chunks, std::memory_order_relaxed);
		return ctx;
	}

	~request_context() noexcept {
		// Safety net: fire the handler with an error if the normal path never did.
		bool expected = false;
		if (handler_fired.compare_exchange_strong(expected, true, std::memory_order_acq_rel) && handler) {
			try {
				handler(0, std::make_exception_ptr(std::runtime_error(
				               "request_context destructed before all chunks completed")));
			} catch (...) { // NOLINT: keep destructor noexcept
			}
		}
	}

	void chunk_done() {
		if (pending.fetch_sub(1, std::memory_order_acq_rel) != 1) {
			return;
		}
		bool expected = false;
		if (!handler_fired.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
			return;
		}
		if (failed.load(std::memory_order_relaxed)) {
			handler(0, exc);
		} else {
			handler(total_bytes, nullptr);
		}
	}

	void chunk_failed(std::exception_ptr e) {
		bool expected = false;
		if (failed.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
			exc = std::move(e);
		}
		chunk_done();
	}

	io_completion_handler handler;
	std::atomic<size_t> pending {0};
	size_t total_bytes {0};
	std::atomic<bool> failed {false};
	std::exception_ptr exc;
	std::atomic<bool> handler_fired {false};
};

/// Descriptor for one buffered host read pushed to the reactor.
struct host_read_req {
	gcs_grpc_native_handle handle {};
	size_t offset {0};
	size_t size {0};
	uint8_t *dst {nullptr};
	std::shared_ptr<request_context> ctx;
};

/// Descriptor for one contiguous FILE range delivered into a scatter-gather
/// list of host destination segments (in file order).
struct host_read_sg_req {
	gcs_grpc_native_handle handle {};
	size_t offset {0}; ///< file offset of the range
	size_t size {0};   ///< total bytes == sum of segment sizes
	std::vector<host_span> segments;
	std::shared_ptr<request_context> ctx;
};

/// Write cursor over a scatter-gather segment list. Appends stream-ordered
/// payload bytes across segment boundaries.
struct sg_write_cursor {
	const std::vector<host_span> *segments {nullptr};
	size_t seg_idx {0};
	size_t seg_off {0};
	size_t written {0};

	/// Copy up to n bytes from src into the segments; returns bytes copied
	/// (less than n only when the segment list is exhausted).
	size_t append(void const *src, size_t n) {
		auto const *p = static_cast<std::byte const *>(src);
		size_t copied = 0;
		while (copied < n && segments && seg_idx < segments->size()) {
			auto seg = (*segments)[seg_idx];
			size_t avail = seg.size() - seg_off;
			size_t take = std::min(n - copied, avail);
			std::memcpy(seg.data() + seg_off, p + copied, take);
			copied += take;
			seg_off += take;
			if (seg_off == seg.size()) {
				++seg_idx;
				seg_off = 0;
			}
		}
		written += copied;
		return copied;
	}
};

} // namespace gcs_grpc
} // namespace duckdb
