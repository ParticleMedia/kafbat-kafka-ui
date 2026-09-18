# Safe Reassignment Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add persistent batch execution, selected cancellation, and crash-safe reassignment throttling to Cluster Operations.

**Architecture:** A PVC-backed atomic JSON journal records intent and configuration snapshots before Kafka mutation. A per-cluster state machine serializes execute/cancel and a scheduled reconciler conditionally restores only values still owned by Kafbat UI.

**Tech Stack:** Java 25, Spring WebFlux, Kafka AdminClient, TypeSpec/OpenAPI, React, TypeScript, TanStack Query, Vitest/Testing Library.

**Spec:** `docs/superpowers/specs/2026-09-18-reassignment-operations.md`

## Global Constraints

- Single application instance owns the PVC through an exclusive lock.
- Existing API consumers remain compatible; new request fields are optional.
- Throttling is opt-in and disabled by default.
- Never restore a Kafka config value changed by another operator.
- All state-changing requests require `REASSIGN_PARTITIONS` and explicit UI confirmation.

---

### Task 1: Contract and journal foundation

**Files:**
- Modify: `contract-typespec/api/partition-reassignments.tsp`
- Modify: `api/src/main/java/io/kafbat/ui/config/ClusterOperationsProperties.java`
- Create: `api/src/main/java/io/kafbat/ui/service/reassign/PartitionReassignmentOperation.java`
- Create: `api/src/main/java/io/kafbat/ui/service/reassign/PartitionReassignmentOperationJournal.java`
- Test: `api/src/test/java/io/kafbat/ui/service/reassign/PartitionReassignmentOperationJournalTest.java`

**Interfaces:**
- Produces journal `create`, `find`, `update`, and `listUnresolved` operations keyed by cluster and operation ID.
- Produces additive execute, cancel, and operation-status DTOs through generated contract classes.

- [ ] Write journal tests for atomic round-trip, duplicate IDs, path-safe cluster names, unresolved recovery, and lock failure.
- [ ] Run the focused journal test and confirm RED because journal types do not exist.
- [ ] Implement the records, file store, configuration, and TypeSpec additions.
- [ ] Generate contract classes and run the focused tests until GREEN.

### Task 2: Throttle lifecycle and recovery

**Files:**
- Modify: `api/src/main/java/io/kafbat/ui/service/ReactiveAdminClient.java`
- Create: `api/src/main/java/io/kafbat/ui/service/reassign/PartitionReassignmentThrottleService.java`
- Create: `api/src/main/java/io/kafbat/ui/service/reassign/PartitionReassignmentReconciler.java`
- Test: `api/src/test/java/io/kafbat/ui/service/reassign/PartitionReassignmentThrottleServiceTest.java`
- Test: `api/src/test/java/io/kafbat/ui/service/reassign/PartitionReassignmentReconcilerTest.java`

**Interfaces:**
- Consumes the Task 1 operation snapshot and journal.
- Produces `apply`, `rollback`, and `reconcile` methods used by execution and cancellation.

- [ ] Write failing tests for broker/topic throttle values, snapshot-before-mutation, rollback, compare-and-restore conflict, and restart reconciliation.
- [ ] Add generic describe/alter config wrappers required by the lifecycle service.
- [ ] Implement apply, rollback, conditional cleanup, and scheduled recovery.
- [ ] Run focused lifecycle tests and backend checkstyle until GREEN.

### Task 3: Idempotent batch execute API

**Files:**
- Modify: `api/src/main/java/io/kafbat/ui/service/reassign/PartitionReassignmentService.java`
- Modify: `api/src/main/java/io/kafbat/ui/controller/PartitionReassignmentsController.java`
- Test: `api/src/test/java/io/kafbat/ui/service/reassign/PartitionReassignmentServiceTest.java`
- Test: `api/src/test/java/io/kafbat/ui/controller/PartitionReassignmentsControllerTest.java`

**Interfaces:**
- Consumes `operationId`, optional `throttleBytesPerSecond`, journal, and throttle lifecycle.
- Produces execute response with accepted count and operation lifecycle status.

- [ ] Add failing tests for backward-compatible execution, idempotent replay, payload mismatch conflict, throttled execution order, rollback, and unknown outcomes.
- [ ] Implement execute orchestration behind existing permission, feature flag, and read-only checks.
- [ ] Run focused service and controller tests until GREEN.

### Task 4: Selected cancellation API

**Files:**
- Modify: `api/src/main/java/io/kafbat/ui/service/reassign/PartitionReassignmentService.java`
- Modify: `api/src/main/java/io/kafbat/ui/controller/PartitionReassignmentsController.java`
- Test: `api/src/test/java/io/kafbat/ui/service/reassign/PartitionReassignmentServiceTest.java`
- Test: `api/src/test/java/io/kafbat/ui/controller/PartitionReassignmentsControllerTest.java`

**Interfaces:**
- Consumes a cancellation operation ID and non-empty topic/partition selection.
- Produces cancelled and skipped partition counts; triggers cleanup only when no owned partitions remain active.

- [ ] Add failing tests for selected cancellation, already-finished skips, retry safety, partial cancellation, and full cancellation cleanup.
- [ ] Implement cancellation using Kafka `Optional.empty()` assignments.
- [ ] Run focused service and controller tests until GREEN.

### Task 5: Execute and cancel UI

**Files:**
- Modify: `frontend/src/lib/hooks/api/partitionReassignments.ts`
- Modify: `frontend/src/components/ClusterOperations/PartitionReassignment/PartitionReassignment.tsx`
- Test: `frontend/src/lib/hooks/api/__tests__/partitionReassignments.spec.ts`
- Test: `frontend/src/components/ClusterOperations/PartitionReassignment/__tests__/PartitionReassignment.spec.tsx`

**Interfaces:**
- Consumes generated execute/cancel types and operation status returned by polling.
- Produces explicit confirmation dialogs, optional throttle validation, row selection, and refresh feedback.

- [ ] Add failing hook and component tests for execute confirmation, throttle validation, cancel selection, and mutation results.
- [ ] Implement hooks and accessible UI controls with throttling off by default.
- [ ] Run focused frontend tests, typecheck, and lint until GREEN.

### Task 6: Deployment and end-to-end verification

**Files:**
- Modify: `.dev/external-clusters-application.yml`
- Modify: `.dev/external-clusters.yaml`
- Modify: `documentation/` only if an existing deployment page fits the PVC setting.

**Interfaces:**
- Mounts a persistent journal directory and configures it for the running Docker container.

- [ ] Run contract generation and inspect the generated diff.
- [ ] Run all backend and frontend tests, typecheck, lint, and production build.
- [ ] Build/restart Docker with a named volume and verify health.
- [ ] Use the browser to verify Execute and Cancel confirmation flows, validation, status polling, and a clean console.
- [ ] Review the final diff for unrelated edits, secrets, skipped tests, and weakened limits.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Crash between Kafka mutation and response | High | Persist intent first and reconcile unknown outcomes |
| Operator edits throttle during reassignment | High | Compare-and-restore; preserve mismatches as conflicts |
| Two application instances mount the PVC | High | Exclusive lock; fail closed for state-changing operations |
| Partial Kafka config mutation | High | Record per-resource state and rollback from snapshots |
| Duplicate HTTP retry | High | Stable operation ID plus request fingerprint |

## Open Questions

None. The user approved the single-instance PVC design on 2026-09-18.
