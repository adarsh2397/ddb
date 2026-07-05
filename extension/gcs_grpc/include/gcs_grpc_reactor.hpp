//===----------------------------------------------------------------------===//
//                         DuckDB
//
// gcs_grpc_reactor.hpp
//
// Native GCS gRPC reactor (google.storage.v2 ReadObject/BidiReadObject),
// ported from Sirius (sirius/src/io/gcs/gcs_grpc_reactor.*) with the
// CUDA/device path removed.
//
// Transport architecture (per instance):
//   - A pool of num_channels gRPC channels ("lanes"), each with its own
//     CompletionQueue + dedicated worker thread. Distinct channel args force
//     separate TCP connections, removing both the per-channel HTTP/2
//     concurrent-stream cap and the single-worker deserialize/memcpy ceiling.
//   - Reads are submitted to lanes round-robin; each lane runs a bounded
//     in-flight window of max_streams / num_channels concurrent streams.
//   - When bidi_reads permits, ranges for the same object multiplex over a
//     persistent BidiReadObject stream per (lane, object) — the Rapid
//     Storage (zonal bucket) fast path. Otherwise each range is a
//     server-streaming ReadObject.
//   - Transient failures (UNAVAILABLE, DEADLINE_EXCEEDED, ABORTED,
//     RESOURCE_EXHAUSTED, INTERNAL) retry with exponential backoff + jitter,
//     resuming at the last delivered byte.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "gcs_grpc_auth.hpp"
#include "gcs_grpc_types.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace duckdb {
namespace gcs_grpc {

//! BidiReadObject usage policy (Rapid Storage multi-range fast path).
enum class gcs_bidi_mode {
	off,      //!< never use BidiReadObject
	on,       //!< always use it; errors surface to the caller
	automatic //!< probe per bucket; fall back to unary ReadObject when unsupported
};

class gcs_grpc_reactor {
public:
	struct config {
		std::shared_ptr<GcsTokenProvider> creds;
		std::string endpoint {"storage.googleapis.com"}; //!< gRPC target (DirectPath: google-c2p:///)
		//! When true, build channels for DirectPath: target the c2p resolver
		//! ("google-c2p:///<host>") with GoogleDefaultCredentials (ALTS handshake +
		//! compute-SA auth), bypassing the GFE on a co-located GCE VM. Auth in this
		//! mode is handled by GoogleDefaultCredentials, so creds is not used for
		//! the bearer header. Off-GCE this must be false.
		bool directpath {false};
		long request_timeout_s {60};
		std::size_t max_streams {16}; //!< TOTAL concurrent streams in flight (split across lanes)
		std::size_t num_channels {4}; //!< gRPC channels, each with its own CQ + worker thread
		gcs_bidi_mode bidi_reads {gcs_bidi_mode::automatic};
		//! Large scatter-gather reads are split into sub-reads of this size so
		//! they parallelize across streams/lanes (bidi ranges also obey it).
		std::size_t target_read_bytes {16UL << 20};
		std::size_t max_retry_attempts {4};
		std::chrono::milliseconds retry_backoff_base {50};
		std::chrono::milliseconds retry_jitter {20};
		bool verbose {false}; //!< emit INFO-level transport logs to stderr
	};

	//! Cumulative activity counters — one snapshot per reactor.
	struct stats_snapshot {
		std::uint64_t bytes_read {0};
		std::uint64_t ranges_completed {0}; //!< logical ranges delivered (unary + bidi)
		std::uint64_t sg_reads {0};         //!< scatter-gather requests accepted
		std::uint64_t unary_streams {0};    //!< ReadObject streams started
		std::uint64_t bidi_sessions {0};    //!< BidiReadObject streams opened
		std::uint64_t bidi_ranges {0};      //!< ranges submitted over bidi sessions
		std::uint64_t bidi_fallbacks {0};   //!< ranges rerouted bidi -> unary
		std::uint64_t bidi_redirects {0};   //!< Rapid Storage routing_token redirects followed
		std::uint64_t retries {0};          //!< retry attempts scheduled
		std::uint64_t retry_exhausted {0};  //!< ranges failed after max retries
	};

	explicit gcs_grpc_reactor(config cfg);
	~gcs_grpc_reactor();

	gcs_grpc_reactor(gcs_grpc_reactor const &) = delete;
	gcs_grpc_reactor &operator=(gcs_grpc_reactor const &) = delete;

	//! Synchronous helper (small sync callers): plain blocking server-streaming
	//! ReadObject on lane 0's stub, no CQ involvement. Bypasses bidi — do not
	//! use as the main read primitive.
	std::size_t host_read(gcs_grpc_native_handle handle, std::size_t offset, std::size_t size, std::uint8_t *dst);

	//! Async single-destination read (wraps host_read_sg_async).
	void host_read_async(host_read_req req);

	//! Scatter-gather read: ONE contiguous file range delivered into multiple
	//! destination segments. This is the bidi-capable hot path.
	void host_read_sg_async(host_read_sg_req req);

	void shutdown();

	//! GetObject metadata (size + generation) — the HEAD equivalent.
	gcs_grpc_object_state stat_object(const std::string &bucket, const std::string &key);

	std::uint64_t bytes_read_total() const noexcept {
		return _bytes_read_total.load(std::memory_order_relaxed);
	}

	stats_snapshot stats() const noexcept;

private:
	struct impl;                 // hides grpc/proto headers from this TU
	std::unique_ptr<impl> _impl; // channel lanes, CQ workers, bidi sessions
	config _cfg;
	std::atomic<std::uint64_t> _bytes_read_total {0};
};

} // namespace gcs_grpc
} // namespace duckdb
