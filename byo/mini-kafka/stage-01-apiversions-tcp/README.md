# Stage 1 — ApiVersions over TCP

Every Kafka client's first move is an **ApiVersions** request: "which APIs and
versions do you speak?" Your broker must answer it correctly or no real client
will proceed.

The starter already length-frames requests/responses and prints
`LISTENING <host>:<port>`. Implement `dispatch(request_bytes)`:

- parse the request header (api_key, api_version, correlation_id, client_id),
- for ApiVersions (api_key **18**), return a response the official
  `kafka-broker-api-versions.sh` and librdkafka accept.

Advertise at least ApiVersions(18), Metadata(3), Produce(0), Fetch(1) — you will
implement those in later stages, but clients negotiate against this table now.

Tests: a raw-socket decode of your ApiVersions v3 response, and the official CLI
tool connecting to you.
