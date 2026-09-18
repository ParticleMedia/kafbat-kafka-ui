# Spec: Safe Reassignment Operations

## Objective

Extend Cluster Operations with three production-safe capabilities:

1. Execute a validated multi-topic partition reassignment plan.
2. Select and cancel active partition reassignments.
3. Optionally throttle reassignment traffic and automatically restore the exact
   broker and topic configuration that existed before execution.

The implementation targets a single Kafbat UI instance with a mounted PVC. It
must recover an unfinished operation after process restart without deleting or
overwriting unrelated operator changes.

## Capability Map

| Module | Responsibility | Depends on |
|---|---|---|
| `operation-journal` | Persist operation intent, snapshots, and lifecycle state on the PVC | — |
| `throttle-lifecycle` | Apply replication throttles and conditionally restore snapshots | `operation-journal` |
| `batch-execute` | Idempotently submit a validated batch plan | `operation-journal`, `throttle-lifecycle` |
| `cancel-reassignment` | Cancel selected active partitions and reconcile owned throttles | `operation-journal`, `throttle-lifecycle` |
| `operations-ui` | Confirmation, throttle input, selection, cancellation, and status feedback | All API modules |

Build order: `operation-journal` → `throttle-lifecycle` → `batch-execute` and
`cancel-reassignment` → `operations-ui`.

## Architecture and Contracts

- Extend the existing execution request additively with `operationId` and
  optional positive `throttleBytesPerSecond`.
- Return `operationId`, `acceptedPartitions`, and lifecycle status from execute.
- Add `POST /api/clusters/{clusterName}/partition-reassignments/cancel` with an
  `operationId` and a non-empty list of topic/partition keys. Repeating a cancel
  for a partition that is no longer active reports it as skipped.
- Include persisted operation status with the active-reassignment response so
  the UI can show cleanup progress or conflicts without another polling API.
- Persist one unresolved throttled operation per cluster as JSON beneath the
  configured journal directory. Use an exclusive process lock and atomic
  replace writes. Blocking file I/O runs on Reactor's bounded-elastic scheduler.
- Persist the snapshot before changing Kafka. Lifecycle states are `PREPARED`,
  `THROTTLE_APPLIED`, `REASSIGNMENT_SUBMITTED`, `CLEANUP_PENDING`, `COMPLETED`,
  `CANCELLED`, `FAILED`, and `CLEANUP_CONFLICT`.
- Apply broker `leader.replication.throttled.rate` and
  `follower.replication.throttled.rate` plus topic
  `leader.replication.throttled.replicas` and
  `follower.replication.throttled.replicas`.
- Cleanup is compare-and-restore: restore a field only when Kafka still contains
  the value written by this operation. A different current value is preserved
  and recorded as a cleanup conflict. Kafka's Admin API does not provide a
  compare-and-set precondition, so an operator write racing between the final
  read and restore cannot be detected atomically.
- A scheduled reconciler resumes journaled operations after restart and cleans
  up only after every partition owned by the operation is no longer active.

## User Interface

- A valid batch plan exposes `Execute plan`.
- The confirmation dialog summarizes the affected topics, partitions, and
  replica moves. Throttling is disabled by default; enabling it requires a
  positive integer bytes-per-second value.
- The Active Reassignments table supports row selection and a
  `Cancel selected` confirmation dialog.
- Success and error messages distinguish accepted, cancelled, skipped, cleanup
  pending, and cleanup-conflict outcomes.

## Security and Failure Boundaries

- Execute and cancel require the existing `REASSIGN_PARTITIONS` permission,
  execution feature flag, and non-read-only cluster.
- Existing request-size, topic, partition, and replicas-per-partition limits
  remain enforced. New operation IDs and throttle values are validated at the
  HTTP boundary.
- The journal contains no credentials. Cluster-derived file names cannot escape
  the configured journal root.
- A reused `operationId` with an identical request returns the recorded result;
  reuse with a different request returns a conflict.
- Failure before Kafka mutation leaves a recoverable or removable `PREPARED`
  record. Failure after throttle application triggers rollback. Unknown Kafka
  outcomes remain journaled for reconciliation rather than being retried blindly.
- Execute, cancel, throttle application, cleanup, and conflict paths use the
  existing audit mechanism.

## Commands and Testing

- Backend focused tests: `./gradlew :api:test --tests '*PartitionReassignment*'`
- Contract generation/check: `./gradlew :contract-typespec:build`
- Frontend focused tests: use the repository Vitest command for the reassignment
  hook and component specs.
- Full verification: backend tests, frontend tests, typecheck, lint, production
  build, Docker image build, and browser runtime verification.

Tests cover journal recovery and path safety, idempotency, throttle calculation,
snapshot restoration, external-change conflicts, partial cancellation, permission
and read-only enforcement, UI confirmation, validation, and polling refresh.

## PVC Deployment

- Run exactly one Kafbat UI replica for a journal PVC. The process takes an
  exclusive lock and intentionally fails closed when another replica owns it.
- Mount a `ReadWriteOnce` PVC at the configured
  `kafka.cluster-operations.operation-journal-path` (the development compose
  example uses `/var/lib/kafbat-ui/reassignment-operations`).
- Make the mount writable by the image runtime user (`uid=100`, `gid=101`). In
  Kubernetes, set pod `securityContext.fsGroup: 101` (and preferably
  `fsGroupChangePolicy: OnRootMismatch`), or use an init container to `chown`
  the mount as the compose example does.
- Use a `Recreate` deployment strategy, or otherwise ensure the old pod fully
  releases the PVC and journal lock before the replacement starts.
- Keep the same PVC across restarts. Deleting it discards idempotency records,
  throttle snapshots, and automatic recovery state.

## Success Criteria

- A user can execute a valid multi-topic plan from Cluster Operations after an
  explicit confirmation.
- A user can select and cancel active reassignment rows after confirmation.
- An optional throttle is applied before submission and the pre-existing Kafka
  configuration is restored after completion or full cancellation.
- Restarting the application with the same PVC resumes reconciliation.
- External configuration values observed before cleanup are preserved when
  they differ from the operation-owned value; the Kafka Admin API's
  read-then-write race remains an explicit platform limitation.
- Existing non-throttled execution remains backward compatible.

## Boundaries

- Always: preserve existing API fields, validate server-side, write intent before
  side effects, and test every state transition.
- Ask first: multi-instance coordination, external databases, or automatic
  cleanup of throttles not owned by Kafbat UI.
- Never: persist credentials, follow cluster names as file paths, blindly delete
  throttle configs, or retry an execution whose Kafka outcome is unknown.
