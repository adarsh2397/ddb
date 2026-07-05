#include "gcs_grpc_auth.hpp"

#include "duckdb/common/exception.hpp"

#include "httplib.hpp"

namespace duckdb {
namespace gcs_grpc {

namespace {

//! Minimal JSON field extractor — pulls the string value for key from a flat
//! (non-nested) JSON object. The metadata server response is a well-defined,
//! non-nested object so a simple scan is safe.
std::string ExtractJsonString(const std::string &json, const std::string &key) {
	std::string needle = "\"" + key + "\"";
	auto pos = json.find(needle);
	if (pos == std::string::npos) {
		return {};
	}
	pos += needle.size();
	while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':')) {
		++pos;
	}
	if (pos >= json.size() || json[pos] != '"') {
		return {};
	}
	++pos;
	std::string value;
	while (pos < json.size() && json[pos] != '"') {
		if (json[pos] == '\\' && pos + 1 < json.size()) {
			++pos;
		}
		value.push_back(json[pos++]);
	}
	return value;
}

//! Extract a JSON number field (e.g. expires_in). Returns -1 if not found.
long ExtractJsonNumber(const std::string &json, const std::string &key) {
	std::string needle = "\"" + key + "\"";
	auto pos = json.find(needle);
	if (pos == std::string::npos) {
		return -1;
	}
	pos += needle.size();
	while (pos < json.size() && (json[pos] == ' ' || json[pos] == ':')) {
		++pos;
	}
	long val = 0;
	bool digits = false;
	while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
		val = val * 10 + (json[pos++] - '0');
		digits = true;
	}
	return digits ? val : -1;
}

} // namespace

StaticTokenProvider::StaticTokenProvider(std::string token_p) : token(std::move(token_p)) {
	if (token.empty()) {
		throw InvalidInputException("gcs_grpc: static bearer token must be non-empty");
	}
}

std::string StaticTokenProvider::GetToken() {
	return token;
}

MetadataServerTokenProvider::MetadataServerTokenProvider(std::string service_account_p)
    : service_account(std::move(service_account_p)) {
	if (service_account.empty()) {
		service_account = "default";
	}
}

void MetadataServerTokenProvider::RefreshTokenLocked() {
	// GCE metadata server; local to the VM, so a short timeout suffices.
	duckdb_httplib::Client client("http://metadata.google.internal");
	client.set_connection_timeout(5);
	client.set_read_timeout(10);

	std::string path = "/computeMetadata/v1/instance/service-accounts/" + service_account + "/token";
	duckdb_httplib::Headers headers {{"Metadata-Flavor", "Google"}};
	auto res = client.Get(path, headers);
	if (!res) {
		throw IOException("gcs_grpc: metadata server request failed: %s (are you running on GCE?)",
		                  duckdb_httplib::to_string(res.error()));
	}
	if (res->status < 200 || res->status >= 300) {
		throw IOException("gcs_grpc: metadata server returned HTTP %d: %s", res->status, res->body);
	}

	// Response JSON: {"access_token":"...","expires_in":3599,"token_type":"Bearer"}
	auto new_token = ExtractJsonString(res->body, "access_token");
	auto expires_in = ExtractJsonNumber(res->body, "expires_in");
	if (new_token.empty()) {
		throw IOException("gcs_grpc: could not parse access_token from metadata response");
	}
	if (expires_in <= 0) {
		expires_in = 3600;
	}

	token = std::move(new_token);
	token_expiry = std::chrono::steady_clock::now() + std::chrono::seconds(expires_in - REFRESH_LEAD_SECONDS);
}

std::string MetadataServerTokenProvider::GetToken() {
	std::lock_guard<std::mutex> guard(lock);
	if (token.empty() || std::chrono::steady_clock::now() >= token_expiry) {
		RefreshTokenLocked();
	}
	return token;
}

} // namespace gcs_grpc
} // namespace duckdb
