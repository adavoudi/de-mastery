# Stage 7 — Consumer-group offsets (OffsetCommit / OffsetFetch)

Let consumers checkpoint their progress.

- **FindCoordinator** (api 10): you are your own group coordinator.
- **OffsetCommit** (api 8): store an offset per `(group, topic, partition)`.
- **OffsetFetch** (api 9): read it back; return −1 when there is no commit.

Mind the request shapes: OffsetFetch v8+ uses a `groups` array and v9 adds
`member_id` + `member_epoch` before the topics.

Tests: commit then fetch in the same group (fresh consumer); unknown group has
no offset; two groups keep independent offsets.
