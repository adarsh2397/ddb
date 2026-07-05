// Ported from sirius/src/io/gcs/gcs_grpc_reactor.cpp (CUDA/device path removed).

#include "gcs_grpc_reactor.hpp"

#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>

#include "google/rpc/status.pb.h"
#include "google/storage/v2/storage.grpc.pb.h"
#include "google/storage/v2/storage.pb.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace duckdb {
namespace gcs_grpc {

namespace {

namespace v2 = google::storage::v2;

/// Flow-control constants for BidiReadObject sessions (internal; the
/// externally tunable knobs are config.max_streams / num_channels /
/// target_read_bytes).
constexpr std::size_t kBidiMaxOutstandingPerSession = 128;
constexpr std::size_t kMaxRangesPerWrite = 64;
constexpr std::size_t kMaxSessionsPerLane = 64;
constexpr auto kStatsLogInterval = std::chrono::seconds(10);
/// Rapid Storage may redirect a bidi stream (routing_token handshake) more than
/// once as tokens refresh; cap distinct from hard-failure retries so a genuine
/// "unreachable location" loop still terminates.
constexpr std::size_t kMaxBidiRedirects = 5;

/// Build the gRPC resource path the v2 API expects for a bucket.
std::string bucket_resource(const std::string &bucket) {
	return "projects/_/buckets/" + bucket;
}

/// Build the `x-goog-request-params` routing header value the GCS gRPC backend
/// requires for ReadObject/BidiReadObject/GetObject. Without this header the
/// server rejects the RPC with INVALID_ARGUMENT. The value is
/// `bucket=<resource path>` with reserved characters percent-encoded.
std::string percent_encode(const std::string &in) {
	static constexpr char kHex[] = "0123456789ABCDEF";
	std::string out;
	out.reserve(in.size());
	for (char c : in) {
		auto uc = static_cast<unsigned char>(c);
		if (std::isalnum(uc) || c == '-' || c == '_' || c == '.' || c == '~') {
			out += c;
		} else {
			out += '%';
			out += kHex[uc >> 4];
			out += kHex[uc & 0x0F];
		}
	}
	return out;
}

std::string routing_params(const std::string &bucket) {
	return "bucket=" + percent_encode(bucket_resource(bucket));
}

/// gRPC call-credentials plugin that pulls an OAuth2 bearer token from the
/// GcsTokenProvider and injects it as the "authorization" metadata header on
/// every RPC. The provider caches + refreshes the token, so this is cheap.
class authorizer_call_credentials final : public grpc::MetadataCredentialsPlugin {
public:
	explicit authorizer_call_credentials(std::shared_ptr<GcsTokenProvider> creds) : _creds(std::move(creds)) {
	}

	grpc::Status GetMetadata(grpc::string_ref /*service_url*/, grpc::string_ref /*method_name*/,
	                         const grpc::AuthContext & /*channel_auth_context*/,
	                         std::multimap<grpc::string, grpc::string> *metadata) override {
		try {
			metadata->insert({"authorization", "Bearer " + _creds->GetToken()});
			return grpc::Status::OK;
		} catch (std::exception &ex) {
			return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
			                    std::string("gcs_grpc: token provider failed: ") + ex.what());
		}
	}

private:
	std::shared_ptr<GcsTokenProvider> _creds;
};

std::exception_ptr to_exception(grpc::Status const &s, const char *what) {
	return std::make_exception_ptr(std::runtime_error("gcs_grpc: " + std::string(what) +
	                                                  " failed: " + std::to_string(static_cast<int>(s.error_code())) +
	                                                  " " + s.error_message()));
}

/// Transient statuses worth retrying per GCS gRPC guidance. INTERNAL is
/// included because GFE/DirectPath emit it for transient stream resets.
bool is_retriable(grpc::StatusCode code) {
	switch (code) {
	case grpc::StatusCode::UNAVAILABLE:
	case grpc::StatusCode::DEADLINE_EXCEEDED:
	case grpc::StatusCode::RESOURCE_EXHAUSTED:
	case grpc::StatusCode::ABORTED:
	case grpc::StatusCode::INTERNAL:
		return true;
	default:
		return false;
	}
}

/// Statuses that mean "BidiReadObject is not served here" when they arrive
/// before any data on a fresh session (auto-mode probe failure).
bool is_bidi_unsupported(grpc::StatusCode code) {
	switch (code) {
	case grpc::StatusCode::UNIMPLEMENTED:
	case grpc::StatusCode::INVALID_ARGUMENT:
	case grpc::StatusCode::FAILED_PRECONDITION:
		return true;
	default:
		return false;
	}
}

/// Rapid Storage redirect handshake: a zonal bucket aborts a fresh
/// BidiReadObject stream and returns a BidiReadObjectRedirectedError detail
/// carrying a routing_token (and usually a read_handle) that the client must
/// echo back when reopening the stream so it is routed to the right location.
/// The detail rides in the `grpc-status-details-bin` trailer, which gRPC
/// surfaces as the serialized google.rpc.Status via error_details().
std::optional<v2::BidiReadObjectRedirectedError> extract_bidi_redirect(grpc::Status const &status) {
	auto const &details = status.error_details();
	if (details.empty()) {
		return std::nullopt;
	}
	google::rpc::Status rpc_status;
	if (!rpc_status.ParseFromString(details)) {
		return std::nullopt;
	}
	for (auto const &any : rpc_status.details()) {
		v2::BidiReadObjectRedirectedError redirect;
		if (any.UnpackTo(&redirect)) {
			return redirect;
		}
	}
	return std::nullopt;
}

std::chrono::milliseconds backoff_for_attempt(std::size_t attempt, std::chrono::milliseconds base,
                                              std::chrono::milliseconds jitter) {
	thread_local std::mt19937 rng {std::random_device {}()};
	auto exp = base.count() << std::min<std::size_t>(attempt, 6); // cap 64x
	auto jit = jitter.count() > 0 ? std::uniform_int_distribution<long long>(0, jitter.count())(rng) : 0;
	return std::chrono::milliseconds(exp + jit);
}

const char *bidi_mode_name(gcs_bidi_mode m) {
	switch (m) {
	case gcs_bidi_mode::off:
		return "off";
	case gcs_bidi_mode::on:
		return "on";
	default:
		return "auto";
	}
}

// ---------------------------------------------------------------------------
// cq_event — common envelope for every CompletionQueue tag. Each lane worker
// does `static_cast<cq_event*>(tag)->on_complete(ok)`, so unary ops, bidi
// session sub-events, backoff alarms and submit kicks all share one loop.
// ---------------------------------------------------------------------------
struct cq_event {
	virtual ~cq_event() = default;
	virtual void on_complete(bool ok) = 0;
};

// ---------------------------------------------------------------------------
// range_state — one logical range read: destination scatter-gather segments
// plus a persistent write cursor. Survives transport retries (unary
// re-attempts and bidi session restarts resume at offset + written) and
// transport switches (bidi -> unary fallback).
// ---------------------------------------------------------------------------
struct range_state {
	gcs_grpc_native_handle handle;
	std::size_t offset {0}; ///< original file offset
	std::size_t total {0};  ///< total bytes to deliver
	std::vector<host_span> segments;
	std::shared_ptr<request_context> cctx;
	std::size_t attempts {0};

	std::size_t written {0};
	std::size_t seg_idx {0};
	std::size_t seg_off {0};

	std::size_t remaining() const {
		return total - written;
	}
	std::size_t resume_offset() const {
		return offset + written;
	}

	/// Append stream-ordered payload across segment boundaries; ignores bytes
	/// past `total` (defensive against over-delivery).
	std::size_t append(void const *src, std::size_t n) {
		n = std::min(n, remaining());
		auto const *p = static_cast<std::byte const *>(src);
		std::size_t copied = 0;
		while (copied < n && seg_idx < segments.size()) {
			auto seg = segments[seg_idx];
			std::size_t avail = seg.size() - seg_off;
			std::size_t take = std::min(n - copied, avail);
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

} // namespace

// ---------------------------------------------------------------------------
// impl — pool of channel "lanes". Each lane owns one gRPC channel, one
// CompletionQueue and one worker thread; submissions round-robin across lanes.
// All stream operations are initiated on the lane's worker thread (submits
// arrive via a zero-delay alarm "kick"), which makes the bidi Write
// sequencing single-writer by construction.
// ---------------------------------------------------------------------------
struct gcs_grpc_reactor::impl {
	struct lane;

	// -- logging (stderr; INFO gated on cfg.verbose) --------------------------
	bool verbose() const {
		return cfg.verbose;
	}
#define GCS_GRPC_LOG_INFO(im, ...)                                                                                    \
	do {                                                                                                              \
		if ((im)->verbose()) {                                                                                        \
			fprintf(stderr, "[gcs_grpc INFO] " __VA_ARGS__);                                                          \
			fprintf(stderr, "\n");                                                                                    \
		}                                                                                                             \
	} while (0)
#define GCS_GRPC_LOG_WARN(...)                                                                                        \
	do {                                                                                                              \
		fprintf(stderr, "[gcs_grpc WARN] " __VA_ARGS__);                                                              \
		fprintf(stderr, "\n");                                                                                        \
	} while (0)
#define GCS_GRPC_LOG_ERROR(...)                                                                                       \
	do {                                                                                                              \
		fprintf(stderr, "[gcs_grpc ERROR] " __VA_ARGS__);                                                             \
		fprintf(stderr, "\n");                                                                                        \
	} while (0)

	// -- unary ReadObject op --------------------------------------------------
	struct read_op final : cq_event {
		lane *ln {nullptr};
		std::unique_ptr<range_state> rs;
		std::unique_ptr<grpc::ClientContext> ctx; // fresh per attempt
		v2::ReadObjectRequest grpc_req;
		v2::ReadObjectResponse resp;
		std::unique_ptr<grpc::ClientAsyncReader<v2::ReadObjectResponse>> reader;
		grpc::Alarm alarm;
		enum class phase { start, reading, finish, backoff } state {phase::start};
		grpc::Status status;

		void on_complete(bool ok) override;
	};

	// -- bidi session ----------------------------------------------------------
	struct bidi_session;

	struct bidi_tag final : cq_event {
		enum kind_t { START, READ, WRITE, FINISH };
		bidi_session *s {nullptr};
		kind_t kind {START};
		void on_complete(bool ok) override;
	};

	struct bidi_session {
		lane *ln {nullptr};
		gcs_grpc_native_handle handle;
		std::string map_key;

		std::unique_ptr<grpc::ClientContext> ctx;
		std::unique_ptr<grpc::ClientAsyncReaderWriter<v2::BidiReadObjectRequest, v2::BidiReadObjectResponse>> rw;
		v2::BidiReadObjectResponse resp;
		grpc::Status status;

		enum class phase { idle, starting, ready, closing, finishing } state {phase::idle};
		bool read_inflight {false};
		bool write_inflight {false};
		bool counted {false};      ///< occupies an in-flight window slot
		bool saw_response {false}; ///< any response arrived on the CURRENT stream
		std::size_t attempts {0};  ///< stream (re)starts after transient failures
		std::size_t redirects {0}; ///< Rapid Storage routing_token redirects followed

		// Rapid Storage redirect state, echoed on the next stream open so the
		// server routes to the correct location / resumes without revalidation.
		std::string routing_token;              ///< from BidiReadObjectRedirectedError
		std::optional<std::string> read_handle; ///< bytes; opaque server handle

		std::int64_t next_read_id {1};
		std::deque<std::unique_ptr<range_state>> pending;
		std::unordered_map<std::int64_t, std::unique_ptr<range_state>> outstanding;

		bidi_tag start_tag;
		bidi_tag read_tag;
		bidi_tag write_tag;
		bidi_tag finish_tag;

		bool has_work() const {
			return !pending.empty() || !outstanding.empty();
		}
	};

	// -- submit kick -----------------------------------------------------------
	struct kick_tag final : cq_event {
		lane *ln {nullptr};
		void on_complete(bool ok) override;
	};

	// -- lane ------------------------------------------------------------------
	struct lane {
		impl *owner {nullptr};
		std::size_t index {0};
		std::shared_ptr<grpc::Channel> channel;
		std::unique_ptr<v2::Storage::Stub> stub;
		grpc::CompletionQueue cq;
		std::thread worker;

		// Guards incoming/kick_armed (cross-thread submits) and the live-call
		// registries (so shutdown() can cancel from another thread).
		std::mutex mtx;
		std::deque<std::unique_ptr<range_state>> incoming;
		bool kick_armed {false};
		grpc::Alarm kick_alarm;
		kick_tag kick;

		// Worker-thread state; registry containers additionally guarded by mtx
		// at mutation points (see route_bidi / start_unary / reap paths).
		std::size_t inflight {0}; ///< unary streams + active bidi sessions
		std::size_t window {16};
		std::deque<read_op *> pending_ops;           ///< unary ops awaiting a slot
		std::deque<bidi_session *> sessions_waiting; ///< sessions with work awaiting a slot
		std::unordered_map<std::string, std::unique_ptr<bidi_session>> sessions;
		std::unordered_map<read_op *, grpc::ClientContext *> live_op_ctxs;
		std::unordered_set<read_op *> backoff_ops; ///< ops with a pending retry alarm

		void worker_loop() {
			void *tag = nullptr;
			bool ok = false;
			while (cq.Next(&tag, &ok)) {
				static_cast<cq_event *>(tag)->on_complete(ok);
			}
		}

		/// True once every started RPC has been reaped (call under mtx).
		bool drained() const {
			if (!live_op_ctxs.empty() || !backoff_ops.empty() || !incoming.empty()) {
				return false;
			}
			for (auto const &kv : sessions) {
				if (kv.second->rw) {
					return false;
				}
			}
			return true;
		}
	};

	// -- data ------------------------------------------------------------------
	config cfg;
	std::atomic<std::uint64_t> *bytes_counter {nullptr};

	std::vector<std::unique_ptr<lane>> lanes;
	std::atomic<std::size_t> next_lane {0};
	std::atomic<bool> stopping {false};

	// Bidi availability per bucket: absent = unknown (auto probes), value = known.
	std::mutex bidi_mtx;
	std::unordered_map<std::string, bool> bucket_bidi_ok;

	// Stats (cumulative; snapshot exposed via reactor::stats()).
	std::atomic<std::uint64_t> ranges_completed {0};
	std::atomic<std::uint64_t> sg_reads {0};
	std::atomic<std::uint64_t> unary_streams {0};
	std::atomic<std::uint64_t> bidi_sessions_opened {0};
	std::atomic<std::uint64_t> bidi_ranges {0};
	std::atomic<std::uint64_t> bidi_fallbacks {0};
	std::atomic<std::uint64_t> bidi_redirects {0};
	std::atomic<std::uint64_t> retries {0};
	std::atomic<std::uint64_t> retry_exhausted {0};

	// One-shot feature logs (so a single query run shows which paths are live).
	std::atomic<bool> logged_first_unary {false};
	std::atomic<bool> logged_first_sg {false};
	std::atomic<bool> logged_first_redirect {false};
	std::atomic<bool> logged_bidi_needs_directpath {false};

	/// The gRPC target the channels dial ("google-c2p:///..." or host:443);
	/// echoed in routing-failure logs so they self-diagnose CFE-vs-DirectPath.
	std::string channel_target;

	// Periodic throughput log.
	std::thread stats_thread;
	std::mutex stats_mtx;
	std::condition_variable stats_cv;

	bool is_stopping() const {
		return stopping.load(std::memory_order_relaxed);
	}

	// ---------------------------------------------------------------------------
	// Submission (any thread)
	// ---------------------------------------------------------------------------

	lane &pick_lane() {
		return *lanes[next_lane.fetch_add(1, std::memory_order_relaxed) % lanes.size()];
	}

	/// Enqueue a range onto ln and kick its worker via a zero-delay alarm.
	/// The Set happens under the lane mutex so shutdown() (which flips
	/// `stopping` and then acquires each lane mutex) can never observe a Set
	/// racing past its CQ shutdown.
	void enqueue_on(lane &ln, std::unique_ptr<range_state> rs) {
		std::unique_lock<std::mutex> lk(ln.mtx);
		if (is_stopping()) {
			lk.unlock();
			fail_range(std::move(rs), std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor stopping")));
			return;
		}
		ln.incoming.push_back(std::move(rs));
		if (!ln.kick_armed) {
			ln.kick_armed = true;
			ln.kick_alarm.Set(&ln.cq, std::chrono::system_clock::now(), &ln.kick);
		}
	}

	void submit_range(std::unique_ptr<range_state> rs) {
		if (rs->total == 0) {
			// Guard: read_offset/read_limit of 0 means "to end of object" in the
			// GCS proto — never send a zero-size range to the wire.
			complete_range(std::move(rs));
			return;
		}
		if (is_stopping()) {
			fail_range(std::move(rs), std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor stopping")));
			return;
		}
		enqueue_on(pick_lane(), std::move(rs));
	}

	// ---------------------------------------------------------------------------
	// Worker-side routing (lane worker thread only)
	// ---------------------------------------------------------------------------

	void drain_incoming(lane &ln) {
		std::deque<std::unique_ptr<range_state>> work;
		{
			std::lock_guard<std::mutex> lk(ln.mtx);
			work.swap(ln.incoming);
			ln.kick_armed = false;
		}
		for (auto &rs : work) {
			route(ln, std::move(rs));
		}
		pump(ln);
	}

	/// True when this range should go over a bidi session on this lane.
	bool want_bidi(std::string const &bucket) {
		if (cfg.bidi_reads == gcs_bidi_mode::off) {
			return false;
		}
		if (cfg.bidi_reads == gcs_bidi_mode::on) {
			return true;
		}
		// auto mode: BidiReadObject on zonal buckets is only reachable over
		// DirectPath — the redirect routing_token is consumed by the client-side
		// c2p/RLS routing stack, which only exists on a DirectPath channel. Via
		// the CFE the reopened stream just re-redirects forever (while unary reads
		// are proxied fine). Don't even probe without DirectPath.
		if (!cfg.directpath) {
			if (!logged_bidi_needs_directpath.exchange(true)) {
				GCS_GRPC_LOG_WARN(
				    "gcs_grpc: bidi_reads=auto but gcs_grpc_transport!=directpath — using unary ReadObject. "
				    "BidiReadObject (Rapid fast path) requires DirectPath on a GCE VM co-located with the "
				    "zonal bucket.");
			}
			return false;
		}
		std::lock_guard<std::mutex> lk(bidi_mtx);
		auto it = bucket_bidi_ok.find(bucket);
		return it == bucket_bidi_ok.end() || it->second; // unknown -> probe
	}

	void route(lane &ln, std::unique_ptr<range_state> rs) {
		if (is_stopping()) {
			fail_range(std::move(rs), std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor stopping")));
			return;
		}
		if (want_bidi(rs->handle->bucket)) {
			route_bidi(ln, std::move(rs));
		} else {
			route_unary(ln, std::move(rs));
		}
	}

	void route_unary(lane &ln, std::unique_ptr<range_state> rs) {
		auto *op = new read_op();
		op->ln = &ln;
		op->rs = std::move(rs);
		ln.pending_ops.push_back(op);
	}

	void route_bidi(lane &ln, std::unique_ptr<range_state> rs) {
		auto key = rs->handle->bucket + "/" + rs->handle->key + "@" + std::to_string(rs->handle->generation);
		auto it = ln.sessions.find(key);
		if (it == ln.sessions.end()) {
			if (ln.sessions.size() >= kMaxSessionsPerLane) {
				evict_idle_session(ln);
			}
			auto s = std::make_unique<bidi_session>();
			s->ln = &ln;
			s->handle = rs->handle;
			s->map_key = key;
			s->start_tag.s = s.get();
			s->start_tag.kind = bidi_tag::START;
			s->read_tag.s = s.get();
			s->read_tag.kind = bidi_tag::READ;
			s->write_tag.s = s.get();
			s->write_tag.kind = bidi_tag::WRITE;
			s->finish_tag.s = s.get();
			s->finish_tag.kind = bidi_tag::FINISH;
			{
				std::lock_guard<std::mutex> lk(ln.mtx);
				it = ln.sessions.emplace(key, std::move(s)).first;
			}
		}
		auto *s = it->second.get();
		s->pending.push_back(std::move(rs));
		bidi_ranges.fetch_add(1, std::memory_order_relaxed);
		if (!s->counted && (s->state == bidi_session::phase::idle || s->state == bidi_session::phase::ready)) {
			ln.sessions_waiting.push_back(s);
		}
	}

	void evict_idle_session(lane &ln) {
		for (auto &kv : ln.sessions) {
			auto &s = kv.second;
			if (!s->counted && !s->has_work() && s->state == bidi_session::phase::ready) {
				// Cancel; the posted Read fails, FINISH reaps and erases the session.
				s->state = bidi_session::phase::closing;
				s->ctx->TryCancel();
				return;
			}
		}
	}

	/// Start queued work while the lane window has room. Sessions get slots
	/// before unary ops (they carry many ranges per slot). No new stream work
	/// is initiated once shutdown began.
	void pump(lane &ln) {
		if (is_stopping()) {
			return;
		}
		while (ln.inflight < ln.window && (!ln.sessions_waiting.empty() || !ln.pending_ops.empty())) {
			if (!ln.sessions_waiting.empty()) {
				auto *s = ln.sessions_waiting.front();
				ln.sessions_waiting.pop_front();
				if (s->counted || !s->has_work()) {
					continue;
				}
				s->counted = true;
				++ln.inflight;
				if (s->state == bidi_session::phase::idle) {
					start_session(*s);
				} else if (s->state == bidi_session::phase::ready) {
					pump_session_writes(*s);
				}
				continue;
			}
			auto *op = ln.pending_ops.front();
			ln.pending_ops.pop_front();
			++ln.inflight;
			start_unary(op);
		}
	}

	void release_slot(lane &ln) {
		--ln.inflight;
		pump(ln);
	}

	// ---------------------------------------------------------------------------
	// Unary ReadObject state machine
	// ---------------------------------------------------------------------------

	void start_unary(read_op *op) {
		auto &rs = *op->rs;
		op->ctx = std::make_unique<grpc::ClientContext>();
		op->ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(cfg.request_timeout_s));
		op->ctx->AddMetadata("x-goog-request-params", routing_params(rs.handle->bucket));

		op->grpc_req.Clear();
		op->grpc_req.set_bucket(bucket_resource(rs.handle->bucket));
		op->grpc_req.set_object(rs.handle->key);
		if (rs.handle->generation != 0) {
			op->grpc_req.set_generation(rs.handle->generation);
		}
		op->grpc_req.set_read_offset(static_cast<int64_t>(rs.resume_offset()));
		op->grpc_req.set_read_limit(static_cast<int64_t>(rs.remaining()));

		{
			std::lock_guard<std::mutex> lk(op->ln->mtx);
			op->ln->live_op_ctxs[op] = op->ctx.get();
		}

		unary_streams.fetch_add(1, std::memory_order_relaxed);
		if (!logged_first_unary.exchange(true)) {
			GCS_GRPC_LOG_INFO(this, "unary ReadObject path active (first stream: %zu bytes of gs://%s/%s)",
			                  rs.remaining(), rs.handle->bucket.c_str(), rs.handle->key.c_str());
		}

		op->state = read_op::phase::start;
		op->reader = op->ln->stub->AsyncReadObject(op->ctx.get(), op->grpc_req, &op->ln->cq, op);
	}

	void handle_unary_event(read_op *op, bool ok) {
		auto &ln = *op->ln;
		switch (op->state) {
		case read_op::phase::start:
			if (!ok) { // call failed to start -> reap status
				op->state = read_op::phase::finish;
				op->reader->Finish(&op->status, op);
				break;
			}
			op->state = read_op::phase::reading;
			op->reader->Read(&op->resp, op);
			break;

		case read_op::phase::reading:
			if (ok) {
				if (op->resp.has_checksummed_data()) {
					auto const &content = op->resp.checksummed_data().content();
					op->rs->append(content.data(), content.size());
				}
				op->reader->Read(&op->resp, op); // pull the next message
			} else {                             // stream end -> finish
				op->state = read_op::phase::finish;
				op->reader->Finish(&op->status, op);
			}
			break;

		case read_op::phase::finish: {
			{
				std::lock_guard<std::mutex> lk(ln.mtx);
				ln.live_op_ctxs.erase(op);
			}
			bool const stop = is_stopping();
			if (op->status.ok() || (op->rs->remaining() == 0 && op->rs->written > 0)) {
				complete_range(std::move(op->rs));
				delete op;
				release_slot(ln);
			} else if (!stop && is_retriable(op->status.error_code()) && op->rs->attempts < cfg.max_retry_attempts) {
				++op->rs->attempts;
				retries.fetch_add(1, std::memory_order_relaxed);
				auto delay = backoff_for_attempt(op->rs->attempts, cfg.retry_backoff_base, cfg.retry_jitter);
				GCS_GRPC_LOG_WARN("gcs_grpc: retrying ReadObject gs://%s/%s (attempt %zu/%zu, status=%d %s, resume at "
				                  "+%zu of %zu bytes, backoff %lldms)",
				                  op->rs->handle->bucket.c_str(), op->rs->handle->key.c_str(), op->rs->attempts,
				                  cfg.max_retry_attempts, static_cast<int>(op->status.error_code()),
				                  op->status.error_message().c_str(), op->rs->written, op->rs->total,
				                  static_cast<long long>(delay.count()));
				op->state = read_op::phase::backoff;
				op->reader.reset();
				op->ctx.reset();
				{
					std::lock_guard<std::mutex> lk(ln.mtx);
					ln.backoff_ops.insert(op);
					op->alarm.Set(&ln.cq, std::chrono::system_clock::now() + delay, op);
				}
				// Release the stream slot during backoff so healthy work proceeds.
				release_slot(ln);
			} else {
				if (!op->status.ok() && op->rs->attempts >= cfg.max_retry_attempts) {
					retry_exhausted.fetch_add(1, std::memory_order_relaxed);
					GCS_GRPC_LOG_ERROR("gcs_grpc: ReadObject gs://%s/%s failed after %zu attempts: %d %s",
					                   op->rs->handle->bucket.c_str(), op->rs->handle->key.c_str(), op->rs->attempts,
					                   static_cast<int>(op->status.error_code()), op->status.error_message().c_str());
				}
				fail_range(std::move(op->rs), to_exception(op->status, "ReadObject"));
				delete op;
				release_slot(ln);
			}
			break;
		}

		case read_op::phase::backoff: {
			{
				std::lock_guard<std::mutex> lk(ln.mtx);
				ln.backoff_ops.erase(op);
			}
			if (!ok || is_stopping()) { // alarm canceled / shutdown
				fail_range(std::move(op->rs), std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor shut down")));
				delete op;
				break;
			}
			// Re-enter the lane queue; pump() assigns a fresh slot.
			ln.pending_ops.push_front(op);
			pump(ln);
			break;
		}
		}
	}

	// ---------------------------------------------------------------------------
	// BidiReadObject session state machine (Rapid Storage fast path)
	// ---------------------------------------------------------------------------

	void start_session(bidi_session &s) {
		s.ctx = std::make_unique<grpc::ClientContext>();
		// Long-lived multi-range stream: no per-call deadline (channel keepalive
		// detects dead peers); ranges themselves are retried on stream failure.
		//
		// Rapid Storage redirect: a pending routing_token must be echoed in BOTH
		// the x-goog-request-params header AND BidiReadObjectSpec.routing_token
		// (set on the first write, see pump_session_writes). Spec-only does NOT
		// work: without the token in the routing header the DirectPath/c2p layer
		// keeps routing the reopened stream to the same backend, which redirects
		// again.
		//
		// CRITICAL: build x-goog-request-params EXACTLY like google-cloud-cpp's
		// OpenObject RequestParams() (storage/internal/async/open_object.cc): BOTH
		// the bucket and the routing_token are RAW — NOT percent-encoded, i.e.
		// `bucket=projects/_/buckets/<name>&routing_token=<token>` with literal
		// slashes. Percent-encoding the token (base64 '+' '/' '=') or the bucket
		// makes the redirect loop forever. Do NOT switch this to routing_params().
		std::string params = "bucket=" + bucket_resource(s.handle->bucket);
		if (!s.routing_token.empty()) {
			params += "&routing_token=" + s.routing_token;
		}
		if (!s.routing_token.empty()) {
			GCS_GRPC_LOG_INFO(this,
			                  "bidi (re)open gs://%s/%s redirect#%zu x-goog-request-params=[%s] "
			                  "(routing_token %zu bytes raw, read_handle %s)",
			                  s.handle->bucket.c_str(), s.handle->key.c_str(), s.redirects, params.c_str(),
			                  s.routing_token.size(), s.read_handle ? "set" : "unset");
		}
		s.ctx->AddMetadata("x-goog-request-params", params);
		s.saw_response = false;
		s.state = bidi_session::phase::starting;
		s.rw = s.ln->stub->PrepareAsyncBidiReadObject(s.ctx.get(), &s.ln->cq);
		s.rw->StartCall(&s.start_tag);
		bidi_sessions_opened.fetch_add(1, std::memory_order_relaxed);
	}

	/// Move pending ranges into a BidiReadObjectRequest (respecting flow
	/// control) and issue the Write. First write on a stream carries the
	/// read_object_spec.
	void pump_session_writes(bidi_session &s) {
		if (is_stopping()) {
			return;
		}
		if (s.state != bidi_session::phase::ready || s.write_inflight) {
			return;
		}
		if (s.pending.empty() || s.outstanding.size() >= kBidiMaxOutstandingPerSession) {
			return;
		}

		v2::BidiReadObjectRequest req;
		if (s.next_read_id == 1) { // first write on this stream
			auto *spec = req.mutable_read_object_spec();
			spec->set_bucket(bucket_resource(s.handle->bucket));
			spec->set_object(s.handle->key);
			if (s.handle->generation != 0) {
				spec->set_generation(s.handle->generation);
			}
			// Echo redirect state so the server routes to the correct location and
			// resumes the read without re-validating (Rapid Storage handshake).
			if (!s.routing_token.empty()) {
				spec->set_routing_token(s.routing_token);
			}
			if (s.read_handle) {
				spec->mutable_read_handle()->set_handle(*s.read_handle);
			}
		}
		std::size_t n = 0;
		while (!s.pending.empty() && n < kMaxRangesPerWrite && s.outstanding.size() < kBidiMaxOutstandingPerSession) {
			auto rs = std::move(s.pending.front());
			s.pending.pop_front();
			auto rid = s.next_read_id++;
			auto *rr = req.add_read_ranges();
			rr->set_read_offset(static_cast<int64_t>(rs->resume_offset()));
			rr->set_read_length(static_cast<int64_t>(rs->remaining()));
			rr->set_read_id(rid);
			s.outstanding.emplace(rid, std::move(rs));
			++n;
		}
		if (n == 0) {
			return;
		}
		s.write_inflight = true;
		s.rw->Write(req, &s.write_tag);
	}

	/// Issue Finish once no read/write is outstanding on a broken stream.
	void maybe_finish(bidi_session &s) {
		if (s.state == bidi_session::phase::closing && !s.read_inflight && !s.write_inflight) {
			s.state = bidi_session::phase::finishing;
			s.rw->Finish(&s.status, &s.finish_tag);
		}
	}

	void handle_bidi_event(bidi_session &s, bidi_tag::kind_t kind, bool ok) {
		auto &ln = *s.ln;
		switch (kind) {
		case bidi_tag::START:
			if (!ok || is_stopping()) {
				s.state = bidi_session::phase::finishing;
				s.rw->Finish(&s.status, &s.finish_tag);
				return;
			}
			s.state = bidi_session::phase::ready;
			// Keep exactly one Read outstanding for the stream's lifetime.
			s.read_inflight = true;
			s.rw->Read(&s.resp, &s.read_tag);
			pump_session_writes(s);
			return;

		case bidi_tag::WRITE:
			s.write_inflight = false;
			if (!ok) {
				// Stream broken; the outstanding Read fails next, then FINISH reaps.
				s.state = bidi_session::phase::closing;
				maybe_finish(s);
				return;
			}
			pump_session_writes(s);
			return;

		case bidi_tag::READ:
			if (!ok) {
				s.read_inflight = false;
				if (s.state != bidi_session::phase::finishing) {
					s.state = bidi_session::phase::closing;
				}
				maybe_finish(s);
				return;
			}
			if (!s.saw_response) {
				s.saw_response = true;
				if (cfg.bidi_reads == gcs_bidi_mode::automatic) {
					mark_bidi_supported(s.handle->bucket);
				}
			}
			for (auto const &rd : s.resp.object_data_ranges()) {
				if (!rd.has_read_range()) {
					continue;
				}
				auto rid = rd.read_range().read_id();
				auto it = s.outstanding.find(rid);
				if (it == s.outstanding.end()) {
					continue; // stale/duplicated range data
				}
				if (rd.has_checksummed_data()) {
					auto const &content = rd.checksummed_data().content();
					it->second->append(content.data(), content.size());
				}
				if (rd.range_end() || it->second->remaining() == 0) {
					complete_range(std::move(it->second));
					s.outstanding.erase(it);
				}
			}
			s.resp.Clear();
			if (is_stopping()) {
				s.read_inflight = false;
				s.state = bidi_session::phase::closing;
				maybe_finish(s);
				return;
			}
			s.rw->Read(&s.resp, &s.read_tag);
			pump_session_writes(s);
			// All ranges served and nothing queued: give the window slot back but
			// keep the stream open for the next task hitting this object.
			if (s.counted && !s.has_work()) {
				s.counted = false;
				release_slot(ln);
			}
			return;

		case bidi_tag::FINISH:
			// Finish is only ever posted with no read/write outstanding, so this
			// is terminal for the current stream.
			reap_session(s); // may destroy s — nothing after this
			return;
		}
	}

	/// Tear down the current stream so the session can be reopened (retry or
	/// redirect). Leaves pending/outstanding untouched — the caller requeues.
	void reset_session_stream(bidi_session &s) {
		s.rw.reset();
		s.ctx.reset();
		s.state = bidi_session::phase::idle;
		s.next_read_id = 1;
		s.read_inflight = s.write_inflight = false;
	}

	/// Terminal handling once Finish completed: follow a Rapid Storage redirect,
	/// retry, fall back or fail all ranges the session still holds; erase the
	/// session unless it restarts.
	void reap_session(bidi_session &s) {
		auto &ln = *s.ln;
		bool const stop = is_stopping();

		// Collect every incomplete range (outstanding resume mid-way).
		std::deque<std::unique_ptr<range_state>> leftovers;
		for (auto &kv : s.outstanding) {
			leftovers.push_back(std::move(kv.second));
		}
		s.outstanding.clear();
		while (!s.pending.empty()) {
			leftovers.push_back(std::move(s.pending.front()));
			s.pending.pop_front();
		}

		if (s.counted) {
			s.counted = false;
			--ln.inflight; // release without pumping yet; pump at the end
		}

		bool const probe_failed =
		    !s.saw_response && cfg.bidi_reads == gcs_bidi_mode::automatic && is_bidi_unsupported(s.status.error_code());

		// Rapid Storage redirect handshake — check before the generic
		// retriable-restart branch (ABORTED is retriable, but restarting WITHOUT
		// the token just re-aborts).
		std::optional<v2::BidiReadObjectRedirectedError> redirect;
		if (!stop && !leftovers.empty()) {
			redirect = extract_bidi_redirect(s.status);
		}
		bool const follow_redirect = redirect.has_value() &&
		                             (!redirect->routing_token().empty() || redirect->has_read_handle()) &&
		                             s.redirects < kMaxBidiRedirects;

		// Decisive diagnostic for the "not available from this location" case.
		if (!s.status.ok()) {
			GCS_GRPC_LOG_INFO(this,
			                  "bidi stream ended gs://%s/%s status=%d \"%s\" saw_response=%d detail_bytes=%zu "
			                  "redirect_parsed=%d routing_token_len=%zu read_handle_present=%d redirects_so_far=%zu",
			                  s.handle->bucket.c_str(), s.handle->key.c_str(), static_cast<int>(s.status.error_code()),
			                  s.status.error_message().c_str(), static_cast<int>(s.saw_response),
			                  s.status.error_details().size(), static_cast<int>(redirect.has_value()),
			                  redirect.has_value() ? redirect->routing_token().size() : 0,
			                  static_cast<int>(redirect.has_value() && redirect->has_read_handle()), s.redirects);
		}

		if (stop) {
			for (auto &rs : leftovers) {
				fail_range(std::move(rs), std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor shut down")));
			}
		} else if (follow_redirect) {
			++s.redirects;
			bidi_redirects.fetch_add(1, std::memory_order_relaxed);
			if (!redirect->routing_token().empty()) {
				s.routing_token = redirect->routing_token();
			}
			if (redirect->has_read_handle()) {
				s.read_handle = redirect->read_handle().handle();
			}
			if (!logged_first_redirect.exchange(true)) {
				GCS_GRPC_LOG_INFO(this,
				                  "Rapid Storage redirect handshake active for bucket=%s — reopening BidiReadObject "
				                  "with routing_token (this is expected for zonal buckets)",
				                  s.handle->bucket.c_str());
			}
			GCS_GRPC_LOG_INFO(this,
			                  "following BidiReadObject redirect for gs://%s/%s (redirect %zu/%zu, routing_token=%s, "
			                  "read_handle=%s, %zu ranges resuming)",
			                  s.handle->bucket.c_str(), s.handle->key.c_str(), s.redirects, kMaxBidiRedirects,
			                  s.routing_token.empty() ? "<none>" : "<set>", s.read_handle ? "<set>" : "<none>",
			                  leftovers.size());
			reset_session_stream(s);
			for (auto &rs : leftovers) {
				s.pending.push_back(std::move(rs)); // resume offsets preserved; not a failure
			}
			ln.sessions_waiting.push_back(&s);
			pump(ln);
			return; // session stays in the map
		} else if (probe_failed) {
			{
				std::lock_guard<std::mutex> lk(bidi_mtx);
				bucket_bidi_ok[s.handle->bucket] = false;
			}
			GCS_GRPC_LOG_WARN("gcs_grpc: BidiReadObject unavailable for bucket=%s (status=%d %s); falling back to "
			                  "unary ReadObject (%zu ranges rerouted)",
			                  s.handle->bucket.c_str(), static_cast<int>(s.status.error_code()),
			                  s.status.error_message().c_str(), leftovers.size());
			bidi_fallbacks.fetch_add(leftovers.size(), std::memory_order_relaxed);
			for (auto &rs : leftovers) {
				route_unary(ln, std::move(rs));
			}
		} else if (!leftovers.empty() && (s.status.ok() || is_retriable(s.status.error_code())) &&
		           s.attempts < cfg.max_retry_attempts) {
			// Stream ended (server rotation or transient error) with ranges still
			// in flight: restart the stream and resume the ranges.
			++s.attempts;
			if (!s.status.ok()) {
				retries.fetch_add(1, std::memory_order_relaxed);
				GCS_GRPC_LOG_WARN("gcs_grpc: bidi session gs://%s/%s broke (status=%d %s); restart %zu/%zu with "
				                  "%zu ranges",
				                  s.handle->bucket.c_str(), s.handle->key.c_str(),
				                  static_cast<int>(s.status.error_code()), s.status.error_message().c_str(),
				                  s.attempts, cfg.max_retry_attempts, leftovers.size());
			}
			reset_session_stream(s);
			for (auto &rs : leftovers) {
				if (!s.status.ok()) {
					++rs->attempts;
				}
				s.pending.push_back(std::move(rs));
			}
			ln.sessions_waiting.push_back(&s);
			pump(ln);
			return; // session stays in the map
		} else if (!leftovers.empty()) {
			// A redirect we couldn't resolve (loop hit the cap, or no token) means
			// the reopened stream never lands on a machine that can serve the zonal
			// object — usually the DirectPath c2p/RLS routing stack is not engaging
			// in this process.
			bool const location_issue =
			    redirect.has_value() || s.redirects > 0 || s.status.error_code() == grpc::StatusCode::ABORTED;
			retry_exhausted.fetch_add(leftovers.size(), std::memory_order_relaxed);
			if (location_issue) {
				GCS_GRPC_LOG_ERROR(
				    "gcs_grpc: BidiReadObject gs://%s/%s unresolvable redirect loop (status=%d %s, "
				    "redirects_followed=%zu, directpath=%d, target=%s). Verify with "
				    "GRPC_TRACE=google_c2p_resolver,rls_lb,xds_client GRPC_VERBOSITY=DEBUG, or SET "
				    "gcs_grpc_bidi_reads='off'. (%zu ranges failed)",
				    s.handle->bucket.c_str(), s.handle->key.c_str(), static_cast<int>(s.status.error_code()),
				    s.status.error_message().c_str(), s.redirects, static_cast<int>(cfg.directpath),
				    channel_target.c_str(), leftovers.size());
			} else {
				GCS_GRPC_LOG_ERROR("gcs_grpc: bidi session gs://%s/%s failed terminally: %d %s (%zu ranges)",
				                   s.handle->bucket.c_str(), s.handle->key.c_str(),
				                   static_cast<int>(s.status.error_code()), s.status.error_message().c_str(),
				                   leftovers.size());
			}
			for (auto &rs : leftovers) {
				fail_range(std::move(rs), to_exception(s.status, "BidiReadObject"));
			}
		}

		// Erase the session (destroys `s` — done last, nothing may touch it
		// after). Drop any stale wait-queue entries first so pump() never
		// dereferences the dead session.
		ln.sessions_waiting.erase(std::remove(ln.sessions_waiting.begin(), ln.sessions_waiting.end(), &s),
		                          ln.sessions_waiting.end());
		auto key = s.map_key;
		{
			std::lock_guard<std::mutex> lk(ln.mtx);
			ln.sessions.erase(key);
		}
		pump(ln);
	}

	void mark_bidi_supported(std::string const &bucket) {
		bool log_it = false;
		{
			std::lock_guard<std::mutex> lk(bidi_mtx);
			auto res = bucket_bidi_ok.emplace(bucket, true);
			log_it = res.second || !res.first->second;
			res.first->second = true;
		}
		if (log_it) {
			GCS_GRPC_LOG_INFO(this, "BidiReadObject ACTIVE for bucket=%s (Rapid Storage multi-range fast path)",
			                  bucket.c_str());
		}
	}

	// ---------------------------------------------------------------------------
	// Completion + failure
	// ---------------------------------------------------------------------------

	void complete_range(std::unique_ptr<range_state> rs) {
		if (bytes_counter) {
			bytes_counter->fetch_add(rs->written, std::memory_order_relaxed);
		}
		ranges_completed.fetch_add(1, std::memory_order_relaxed);
		if (rs->cctx) {
			rs->cctx->chunk_done();
		}
	}

	void fail_range(std::unique_ptr<range_state> rs, std::exception_ptr ep) {
		if (rs && rs->cctx) {
			rs->cctx->chunk_failed(std::move(ep));
		}
	}

	// ---------------------------------------------------------------------------
	// Periodic stats log — makes transport behaviour verifiable per query run.
	// ---------------------------------------------------------------------------

	void stats_loop() {
		std::uint64_t last_bytes = 0;
		auto last_time = std::chrono::steady_clock::now();
		std::unique_lock<std::mutex> lk(stats_mtx);
		while (!is_stopping()) {
			stats_cv.wait_for(lk, kStatsLogInterval);
			if (is_stopping()) {
				break;
			}
			auto now = std::chrono::steady_clock::now();
			auto bytes = bytes_counter ? bytes_counter->load(std::memory_order_relaxed) : 0;
			if (bytes == last_bytes) {
				last_time = now;
				continue; // idle — don't spam the log
			}
			auto secs = std::chrono::duration<double>(now - last_time).count();
			auto mib = static_cast<double>(bytes - last_bytes) / (1024.0 * 1024.0);
			GCS_GRPC_LOG_INFO(this,
			                  "stats: +%.1f MiB (%.1f MiB/s) | totals: bytes=%llu ranges=%llu sg_reads=%llu "
			                  "unary_streams=%llu bidi_sessions=%llu bidi_ranges=%llu bidi_fallbacks=%llu "
			                  "bidi_redirects=%llu retries=%llu retry_exhausted=%llu",
			                  mib, secs > 0 ? mib / secs : 0.0, static_cast<unsigned long long>(bytes),
			                  static_cast<unsigned long long>(ranges_completed.load(std::memory_order_relaxed)),
			                  static_cast<unsigned long long>(sg_reads.load(std::memory_order_relaxed)),
			                  static_cast<unsigned long long>(unary_streams.load(std::memory_order_relaxed)),
			                  static_cast<unsigned long long>(bidi_sessions_opened.load(std::memory_order_relaxed)),
			                  static_cast<unsigned long long>(bidi_ranges.load(std::memory_order_relaxed)),
			                  static_cast<unsigned long long>(bidi_fallbacks.load(std::memory_order_relaxed)),
			                  static_cast<unsigned long long>(bidi_redirects.load(std::memory_order_relaxed)),
			                  static_cast<unsigned long long>(retries.load(std::memory_order_relaxed)),
			                  static_cast<unsigned long long>(retry_exhausted.load(std::memory_order_relaxed)));
			last_bytes = bytes;
			last_time = now;
		}
	}
};

// ---------------------------------------------------------------------------
// cq_event dispatch shims
// ---------------------------------------------------------------------------

void gcs_grpc_reactor::impl::read_op::on_complete(bool ok) {
	ln->owner->handle_unary_event(this, ok);
}

void gcs_grpc_reactor::impl::bidi_tag::on_complete(bool ok) {
	s->ln->owner->handle_bidi_event(*s, kind, ok);
}

void gcs_grpc_reactor::impl::kick_tag::on_complete(bool /*ok*/) {
	// Runs on the lane worker for both normal kicks and shutdown-flushed alarms;
	// drain_incoming()'s route() fails queued ranges when stopping.
	ln->owner->drain_incoming(*ln);
}

// ---------------------------------------------------------------------------
// Reactor construction / teardown
// ---------------------------------------------------------------------------

gcs_grpc_reactor::gcs_grpc_reactor(config cfg) : _cfg(std::move(cfg)) {
	if (!_cfg.directpath && !_cfg.creds) {
		throw std::invalid_argument("gcs_grpc_reactor: a token provider is required on the non-DirectPath path");
	}

	_impl = std::make_unique<impl>();
	_impl->cfg = _cfg;
	_impl->bytes_counter = &_bytes_read_total;

	auto const n_lanes = std::max<std::size_t>(_cfg.num_channels, 1);
	auto const window = std::max<std::size_t>(_cfg.max_streams / n_lanes, 1);

	std::string target;
	std::shared_ptr<grpc::ChannelCredentials> channel_creds;

	if (_cfg.directpath) {
		// DirectPath: the c2p resolver triggers DirectPath negotiation, and
		// GoogleDefaultCredentials carries the ALTS transport creds + compute-SA
		// auth (so the bearer-plugin is not used in this mode). On a co-located GCE
		// VM this bypasses the GFE; gRPC auto-falls back to CFE/TLS otherwise.
		// endpoint is a bare host here (strip any scheme/port defensively).
		auto host = _cfg.endpoint;
		auto p = host.find("://");
		if (p != std::string::npos) {
			host = host.substr(p + 3);
		}
		auto c = host.find(':');
		if (c != std::string::npos) {
			host = host.substr(0, c);
		}
		target = "google-c2p:///" + host;
		channel_creds = grpc::GoogleDefaultCredentials();
	} else {
		// Portable path: TLS transport + per-call bearer token from the provider.
		auto call_creds =
		    grpc::MetadataCredentialsFromPlugin(std::make_unique<authorizer_call_credentials>(_cfg.creds));
		channel_creds =
		    grpc::CompositeChannelCredentials(grpc::SslCredentials(grpc::SslCredentialsOptions {}), call_creds);

		target = _cfg.endpoint;
		if (target.find("://") == std::string::npos && target.find(':') == std::string::npos) {
			target += ":443"; // default gRPC TLS port
		}
	}

	_impl->channel_target = target;

	_impl->lanes.reserve(n_lanes);
	for (std::size_t i = 0; i < n_lanes; ++i) {
		auto ln = std::make_unique<impl::lane>();
		ln->owner = _impl.get();
		ln->index = i;
		ln->window = window;
		ln->kick.ln = ln.get();

		grpc::ChannelArguments args;
		// Distinct channel args force a distinct subchannel (its own TCP/ALTS
		// connection) per lane — otherwise gRPC dedupes them into one connection.
		args.SetInt("grpc.duckdb_gcs_lane", static_cast<int>(i));
		args.SetMaxReceiveMessageSize(-1);
		args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
		args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);

		ln->channel = grpc::CreateCustomChannel(target, channel_creds, args);
		ln->stub = v2::Storage::NewStub(ln->channel);
		_impl->lanes.push_back(std::move(ln));
	}

	// Start workers once channels + stubs are live.
	for (auto &ln : _impl->lanes) {
		ln->worker = std::thread([l = ln.get()]() { l->worker_loop(); });
	}
	_impl->stats_thread = std::thread([im = _impl.get()]() { im->stats_loop(); });

	GCS_GRPC_LOG_INFO(_impl.get(),
	                  "reactor up | channels=%zu max_streams=%zu (per-lane window %zu) bidi_reads=%s directpath=%d "
	                  "target=%s target_read_bytes=%zu retry(max=%zu base=%lldms jitter=%lldms) timeout=%lds",
	                  n_lanes, _cfg.max_streams, window, bidi_mode_name(_cfg.bidi_reads),
	                  static_cast<int>(_cfg.directpath), target.c_str(), _cfg.target_read_bytes,
	                  _cfg.max_retry_attempts, static_cast<long long>(_cfg.retry_backoff_base.count()),
	                  static_cast<long long>(_cfg.retry_jitter.count()), _cfg.request_timeout_s);
}

gcs_grpc_reactor::~gcs_grpc_reactor() {
	shutdown();
}

void gcs_grpc_reactor::shutdown() {
	if (!_impl) {
		return;
	}
	bool expected = false;
	if (!_impl->stopping.compare_exchange_strong(expected, true)) {
		return;
	}

	// Wake + stop the stats logger first (it only reads counters).
	_impl->stats_cv.notify_all();
	if (_impl->stats_thread.joinable()) {
		_impl->stats_thread.join();
	}

	// 1) Cancel every live RPC and pending retry alarm. The workers keep
	//    running and reap the resulting completions: handlers observe
	//    `stopping`, fail their ranges and never start new stream work.
	for (auto &ln : _impl->lanes) {
		std::lock_guard<std::mutex> lk(ln->mtx);
		for (auto &kv : ln->live_op_ctxs) {
			kv.second->TryCancel();
		}
		for (auto *op : ln->backoff_ops) {
			op->alarm.Cancel();
		}
		for (auto &kv : ln->sessions) {
			if (kv.second->ctx) {
				kv.second->ctx->TryCancel();
			}
		}
	}

	// 2) Wait for every started RPC to be reaped — a CompletionQueue may only
	//    be Shutdown() once no further work will be added, and reaping (Finish)
	//    counts as work. Bounded by the request timeout as a hard cap.
	auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(_cfg.request_timeout_s + 5);
	for (auto &ln : _impl->lanes) {
		for (;;) {
			{
				std::lock_guard<std::mutex> lk(ln->mtx);
				if (ln->drained()) {
					break;
				}
			}
			if (std::chrono::steady_clock::now() > deadline) {
				GCS_GRPC_LOG_ERROR("gcs_grpc: lane %zu did not drain before shutdown deadline; forcing CQ shutdown",
				                   ln->index);
				break;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
	}

	// 3) Now the CQs are quiescent: shut them down and join the workers.
	for (auto &ln : _impl->lanes) {
		ln->cq.Shutdown();
	}
	for (auto &ln : _impl->lanes) {
		if (ln->worker.joinable()) {
			ln->worker.join();
		}
	}

	// 4) Fail anything that never reached a stream: queued submissions, unary
	//    ops awaiting a slot, and ranges still held by (never-started) sessions.
	auto fail_ep = std::make_exception_ptr(std::runtime_error("gcs_grpc: reactor shut down"));
	for (auto &ln : _impl->lanes) {
		for (auto &rs : ln->incoming) {
			_impl->fail_range(std::move(rs), fail_ep);
		}
		ln->incoming.clear();
		for (auto *op : ln->pending_ops) {
			_impl->fail_range(std::move(op->rs), fail_ep);
			delete op;
		}
		ln->pending_ops.clear();
		for (auto &kv : ln->sessions) {
			auto &s = kv.second;
			for (auto &orange : s->outstanding) {
				_impl->fail_range(std::move(orange.second), fail_ep);
			}
			s->outstanding.clear();
			while (!s->pending.empty()) {
				_impl->fail_range(std::move(s->pending.front()), fail_ep);
				s->pending.pop_front();
			}
		}
		ln->sessions.clear();
	}

	auto st = stats();
	GCS_GRPC_LOG_INFO(_impl.get(),
	                  "reactor shutdown | totals: bytes=%llu ranges=%llu sg_reads=%llu unary_streams=%llu "
	                  "bidi_sessions=%llu bidi_ranges=%llu bidi_fallbacks=%llu bidi_redirects=%llu retries=%llu "
	                  "retry_exhausted=%llu",
	                  static_cast<unsigned long long>(st.bytes_read),
	                  static_cast<unsigned long long>(st.ranges_completed),
	                  static_cast<unsigned long long>(st.sg_reads),
	                  static_cast<unsigned long long>(st.unary_streams),
	                  static_cast<unsigned long long>(st.bidi_sessions),
	                  static_cast<unsigned long long>(st.bidi_ranges),
	                  static_cast<unsigned long long>(st.bidi_fallbacks),
	                  static_cast<unsigned long long>(st.bidi_redirects),
	                  static_cast<unsigned long long>(st.retries),
	                  static_cast<unsigned long long>(st.retry_exhausted));
}

gcs_grpc_reactor::stats_snapshot gcs_grpc_reactor::stats() const noexcept {
	stats_snapshot s;
	if (!_impl) {
		return s;
	}
	s.bytes_read = _bytes_read_total.load(std::memory_order_relaxed);
	s.ranges_completed = _impl->ranges_completed.load(std::memory_order_relaxed);
	s.sg_reads = _impl->sg_reads.load(std::memory_order_relaxed);
	s.unary_streams = _impl->unary_streams.load(std::memory_order_relaxed);
	s.bidi_sessions = _impl->bidi_sessions_opened.load(std::memory_order_relaxed);
	s.bidi_ranges = _impl->bidi_ranges.load(std::memory_order_relaxed);
	s.bidi_fallbacks = _impl->bidi_fallbacks.load(std::memory_order_relaxed);
	s.bidi_redirects = _impl->bidi_redirects.load(std::memory_order_relaxed);
	s.retries = _impl->retries.load(std::memory_order_relaxed);
	s.retry_exhausted = _impl->retry_exhausted.load(std::memory_order_relaxed);
	return s;
}

// ---------------------------------------------------------------------------
// stat + read entry points
// ---------------------------------------------------------------------------

gcs_grpc_object_state gcs_grpc_reactor::stat_object(const std::string &bucket, const std::string &key) {
	v2::GetObjectRequest req;
	req.set_bucket(bucket_resource(bucket));
	req.set_object(key);

	grpc::ClientContext ctx;
	ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(_cfg.request_timeout_s));
	ctx.AddMetadata("x-goog-request-params", routing_params(bucket));

	v2::Object obj;
	auto status = _impl->lanes.front()->stub->GetObject(&ctx, req, &obj);
	if (!status.ok()) {
		std::rethrow_exception(to_exception(status, "GetObject"));
	}

	gcs_grpc_object_state st;
	st.bucket = bucket;
	st.key = key;
	st.object_size = static_cast<std::size_t>(obj.size());
	st.generation = obj.generation();
	return st;
}

std::size_t gcs_grpc_reactor::host_read(gcs_grpc_native_handle handle, std::size_t offset, std::size_t size,
                                        std::uint8_t *dst) {
	// Synchronous helper: plain blocking server-streaming ReadObject on lane
	// 0's stub, no CQ involvement.
	v2::ReadObjectRequest req;
	req.set_bucket(bucket_resource(handle->bucket));
	req.set_object(handle->key);
	if (handle->generation != 0) {
		req.set_generation(handle->generation);
	}
	req.set_read_offset(static_cast<int64_t>(offset));
	req.set_read_limit(static_cast<int64_t>(size));

	grpc::ClientContext ctx;
	ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(_cfg.request_timeout_s));
	ctx.AddMetadata("x-goog-request-params", routing_params(handle->bucket));

	auto reader = _impl->lanes.front()->stub->ReadObject(&ctx, req);

	std::size_t written = 0;
	v2::ReadObjectResponse resp;
	while (reader->Read(&resp)) {
		if (resp.has_checksummed_data()) {
			// ChecksummedData.content is `bytes content = 1 [ctype = CORD]` in the
			// GCS v2 proto. gcs_grpc_proto.cmake strips the annotation before protoc
			// runs, so the generated accessor is a plain public string.
			auto const &content = resp.checksummed_data().content();
			auto const n = std::min<std::size_t>(content.size(), size - written);
			if (n > 0) {
				std::memcpy(dst + written, content.data(), n);
				written += n;
			}
			if (written >= size) {
				break;
			}
		}
	}
	auto status = reader->Finish();
	if (!status.ok()) {
		std::rethrow_exception(to_exception(status, "ReadObject"));
	}

	_bytes_read_total.fetch_add(written, std::memory_order_relaxed);
	return written;
}

void gcs_grpc_reactor::host_read_async(host_read_req req) {
	host_read_sg_req sg;
	sg.handle = std::move(req.handle);
	sg.offset = req.offset;
	sg.size = req.size;
	sg.segments.push_back(host_span {reinterpret_cast<std::byte *>(req.dst), req.size});
	sg.ctx = std::move(req.ctx);
	host_read_sg_async(std::move(sg));
}

void gcs_grpc_reactor::host_read_sg_async(host_read_sg_req req) {
	_impl->sg_reads.fetch_add(1, std::memory_order_relaxed);
	if (!_impl->logged_first_sg.exchange(true)) {
		GCS_GRPC_LOG_INFO(_impl.get(),
		                  "scatter-gather read path active (first request: %zu bytes across %zu segments, split "
		                  "target %zu bytes)",
		                  req.size, req.segments.size(), _cfg.target_read_bytes);
	}

	auto const target = std::max<std::size_t>(_cfg.target_read_bytes, 1UL << 20);
	auto const n_sub = req.size == 0 ? 0 : (req.size + target - 1) / target;

	if (n_sub <= 1) {
		auto rs = std::make_unique<range_state>();
		rs->handle = std::move(req.handle);
		rs->offset = req.offset;
		rs->total = req.size;
		rs->segments = std::move(req.segments);
		rs->cctx = std::move(req.ctx);
		_impl->submit_range(std::move(rs));
		return;
	}

	// Split into sub-ranges of ~target bytes so they parallelize across
	// streams/lanes. The outer request_context (pending == 1) is resolved via an
	// inner context counting the sub-ranges.
	auto outer = std::move(req.ctx);
	auto inner = request_context::create(n_sub, req.size, [outer](std::size_t, std::exception_ptr ep) {
		if (!outer) {
			return;
		}
		if (ep) {
			outer->chunk_failed(std::move(ep));
		} else {
			outer->chunk_done();
		}
	});

	std::size_t seg_idx = 0;
	std::size_t seg_off = 0;
	std::size_t cur_off = req.offset;
	std::size_t left = req.size;
	while (left > 0) {
		auto sub_size = std::min(target, left);
		auto rs = std::make_unique<range_state>();
		rs->handle = req.handle;
		rs->offset = cur_off;
		rs->total = sub_size;
		rs->cctx = inner;

		// Carve `sub_size` bytes of destination segments (subspans at the edges).
		std::size_t need = sub_size;
		while (need > 0 && seg_idx < req.segments.size()) {
			auto seg = req.segments[seg_idx];
			std::size_t avail = seg.size() - seg_off;
			std::size_t take = std::min(need, avail);
			rs->segments.push_back(host_span {seg.data() + seg_off, take});
			seg_off += take;
			need -= take;
			if (seg_off == seg.size()) {
				++seg_idx;
				seg_off = 0;
			}
		}
		_impl->submit_range(std::move(rs));
		cur_off += sub_size;
		left -= sub_size;
	}
}

} // namespace gcs_grpc
} // namespace duckdb
