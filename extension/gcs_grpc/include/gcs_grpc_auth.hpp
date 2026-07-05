//===----------------------------------------------------------------------===//
//                         DuckDB
//
// gcs_grpc_auth.hpp
//
// Bearer-token providers for the GCS gRPC transport. On the portable
// (non-DirectPath) path the token is attached to each RPC as gRPC call
// credentials; DirectPath uses GoogleDefaultCredentials (ALTS) instead.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>
#include <mutex>
#include <string>

namespace duckdb {
namespace gcs_grpc {

struct GcsTokenProvider {
	virtual ~GcsTokenProvider() = default;
	//! Returns a valid OAuth2 access token (never empty; throws on failure).
	virtual std::string GetToken() = 0;
};

//! Fixed token supplied via the gcs_grpc_bearer_token setting (debug/testing;
//! GCE tokens expire after ~1h).
class StaticTokenProvider : public GcsTokenProvider {
public:
	explicit StaticTokenProvider(std::string token);
	std::string GetToken() override;

private:
	std::string token;
};

//! Fetches OAuth2 access tokens from the GCE metadata server and caches them
//! until shortly before expiry. Only usable on a GCE VM.
class MetadataServerTokenProvider : public GcsTokenProvider {
public:
	explicit MetadataServerTokenProvider(std::string service_account = "default");
	std::string GetToken() override;

private:
	void RefreshTokenLocked();

private:
	//! Refresh this many seconds before the advertised expiry.
	static constexpr long REFRESH_LEAD_SECONDS = 60;
	std::string service_account;
	std::mutex lock;
	std::string token;
	std::chrono::steady_clock::time_point token_expiry;
};

} // namespace gcs_grpc
} // namespace duckdb
