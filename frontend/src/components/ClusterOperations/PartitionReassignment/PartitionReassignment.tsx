import React from 'react';
import {
  PartitionReassignmentOperationState,
  PartitionReassignmentPlanRequest,
} from 'generated-sources';
import ResourcePageHeading from 'components/common/ResourcePageHeading/ResourcePageHeading';
import { Button } from 'components/common/Button/Button';
import { Modal } from 'components/common/Modal';
import ProgressBar from 'components/common/ProgressBar/ProgressBar';
import useAppParams from 'lib/hooks/useAppParams';
import { ClusterNameRoute } from 'lib/paths';
import {
  useActivePartitionReassignments,
  useCancelPartitionReassignments,
  useCreatePartitionReassignmentPlan,
  useExecutePartitionReassignmentPlan,
  usePartitionReassignmentCapabilities,
  useValidatePartitionReassignmentPlan,
} from 'lib/hooks/api/partitionReassignments';

import * as S from './PartitionReassignment.styled';

const INITIAL_REQUEST = JSON.stringify(
  {
    assignments: [{ topic: 'orders', partition: 0, replicas: [1, 2] }],
  },
  null,
  2
);

const errorMessage = (error: unknown) => {
  if (error && typeof error === 'object' && 'message' in error) {
    return String(error.message);
  }
  return 'Unable to process the reassignment plan';
};

const createOperationId = () =>
  globalThis.crypto?.randomUUID?.() ??
  `${Date.now()}-${Math.random().toString(16).slice(2)}`;

const PartitionReassignment: React.FC = () => {
  const { clusterName } = useAppParams<ClusterNameRoute>();
  const createPlan = useCreatePartitionReassignmentPlan(clusterName);
  const validatePlan = useValidatePartitionReassignmentPlan(clusterName);
  const executePlan = useExecutePartitionReassignmentPlan(clusterName);
  const cancelReassignments = useCancelPartitionReassignments(clusterName);
  const activeReassignments = useActivePartitionReassignments(clusterName);
  const capabilities = usePartitionReassignmentCapabilities(clusterName);
  const [requestJson, setRequestJson] = React.useState(INITIAL_REQUEST);
  const [plan, setPlan] = React.useState<
    Awaited<ReturnType<typeof createPlan.mutateAsync>> | undefined
  >();
  const [error, setError] = React.useState<string>();
  const [message, setMessage] = React.useState<string>();
  const [isValid, setIsValid] = React.useState(false);
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [isExecuteOpen, setIsExecuteOpen] = React.useState(false);
  const [isCancelOpen, setIsCancelOpen] = React.useState(false);
  const [operationId, setOperationId] = React.useState('');
  const [isThrottleEnabled, setIsThrottleEnabled] = React.useState(false);
  const [throttle, setThrottle] = React.useState('');

  const handleGenerate = async () => {
    setError(undefined);
    setMessage(undefined);
    setIsValid(false);
    try {
      const request = JSON.parse(
        requestJson
      ) as PartitionReassignmentPlanRequest;
      if (!Array.isArray(request.assignments)) {
        throw new Error('Enter valid target assignments JSON');
      }
      setPlan(await createPlan.mutateAsync(request));
    } catch (e) {
      setPlan(undefined);
      setError(
        e instanceof SyntaxError
          ? 'Enter valid target assignments JSON'
          : errorMessage(e)
      );
    }
  };

  const handleValidate = async () => {
    if (!plan) return;
    setError(undefined);
    setIsValid(false);
    try {
      const result = await validatePlan.mutateAsync({
        changes: plan.changes,
        planHash: plan.planHash,
      });
      setIsValid(result.valid);
    } catch (e) {
      setError(errorMessage(e));
    }
  };

  const openExecute = () => {
    setError(undefined);
    setOperationId(createOperationId());
    setIsThrottleEnabled(false);
    setThrottle('');
    setIsExecuteOpen(true);
  };

  const handleExecute = async () => {
    if (!plan || !isValid) return;
    const throttleBytesPerSecond = Number(throttle);
    if (
      isThrottleEnabled &&
      (!Number.isSafeInteger(throttleBytesPerSecond) ||
        throttleBytesPerSecond <= 0)
    ) {
      setError('Throttle must be a positive whole number of bytes per second');
      return;
    }
    setError(undefined);
    try {
      const result = await executePlan.mutateAsync({
        changes: plan.changes,
        planHash: plan.planHash,
        operationId,
        ...(isThrottleEnabled ? { throttleBytesPerSecond } : {}),
      });
      setIsExecuteOpen(false);
      setIsValid(false);
      setMessage(
        result.status ===
          PartitionReassignmentOperationState.REASSIGNMENT_SUBMITTED
          ? `Reassignment submitted for ${result.acceptedPartitions} ${
              result.acceptedPartitions === 1 ? 'partition' : 'partitions'
            }`
          : `Operation ${result.operationId} is ${result.status
              .toLowerCase()
              .replaceAll(
                '_',
                ' '
              )}. Reconciliation will continue automatically.`
      );
    } catch (caughtError) {
      setError(errorMessage(caughtError));
    }
  };

  const toggleSelection = (topic: string, partition: number) => {
    const key = `${topic}:${partition}`;
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const openCancel = () => {
    setError(undefined);
    setOperationId(createOperationId());
    setIsCancelOpen(true);
  };

  const handleCancel = async () => {
    const partitions = (activeReassignments.data?.reassignments ?? [])
      .filter((reassignment) =>
        selected.has(`${reassignment.topic}:${reassignment.partition}`)
      )
      .map(({ topic, partition }) => ({ topic, partition }));
    if (partitions.length === 0) return;
    setError(undefined);
    try {
      const result = await cancelReassignments.mutateAsync({
        operationId,
        partitions,
      });
      setIsCancelOpen(false);
      setSelected(new Set());
      setMessage(
        `Cancelled ${result.cancelledPartitions} ${
          result.cancelledPartitions === 1 ? 'partition' : 'partitions'
        }; skipped ${result.skippedPartitions}`
      );
    } catch (caughtError) {
      setError(errorMessage(caughtError));
    }
  };

  return (
    <S.Container>
      <ResourcePageHeading text="Partition Reassignment" />
      <S.Content>
        <S.Panel>
          <h2>Active reassignments</h2>
          <S.Description>
            Automatically refreshes every 5 seconds. Progress is the percentage
            of target replicas currently in the in-sync replica set (ISR).
          </S.Description>
          {capabilities.data && !capabilities.data.executionEnabled && (
            <S.WarningMessage role="status">
              {capabilities.data.reason ??
                'Reassignment execution is disabled.'}
            </S.WarningMessage>
          )}
          {activeReassignments.data?.operation && (
            <S.OperationStatus role="status">
              Operation {activeReassignments.data.operation.operationId}:{' '}
              {activeReassignments.data.operation.status}
              {activeReassignments.data.operation.throttleBytesPerSecond
                ? ` at ${activeReassignments.data.operation.throttleBytesPerSecond.toLocaleString()} bytes/sec`
                : ''}
            </S.OperationStatus>
          )}
          {activeReassignments.data?.operation?.cleanupConflicts.map(
            (conflict) => (
              <S.ErrorMessage role="alert" key={conflict}>
                Cleanup conflict: {conflict}
              </S.ErrorMessage>
            )
          )}
          {activeReassignments.isLoading && (
            <S.Description>Loading active reassignments…</S.Description>
          )}
          {activeReassignments.error && (
            <S.ErrorMessage role="alert">
              Unable to load active partition reassignments
            </S.ErrorMessage>
          )}
          {!activeReassignments.isLoading &&
            !activeReassignments.error &&
            activeReassignments.data?.reassignments.length === 0 && (
              <S.EmptyState>No active partition reassignments</S.EmptyState>
            )}
          {activeReassignments.data &&
            activeReassignments.data.reassignments.length > 0 && (
              <S.Table aria-label="Active partition reassignments">
                <thead>
                  <tr>
                    <th aria-label="Selection" />
                    <th>Topic</th>
                    <th>Partition</th>
                    <th>Current replicas</th>
                    <th>Target replicas</th>
                    <th>Replica movement</th>
                    <th>Target replicas in sync</th>
                  </tr>
                </thead>
                <tbody>
                  {activeReassignments.data.reassignments.map(
                    (reassignment) => (
                      <tr
                        key={`${reassignment.topic}-${reassignment.partition}`}
                      >
                        <td>
                          <input
                            type="checkbox"
                            aria-label={`Select ${reassignment.topic} partition ${reassignment.partition}`}
                            checked={selected.has(
                              `${reassignment.topic}:${reassignment.partition}`
                            )}
                            disabled={!capabilities.data?.executionEnabled}
                            onChange={() =>
                              toggleSelection(
                                reassignment.topic,
                                reassignment.partition
                              )
                            }
                          />
                        </td>
                        <td>{reassignment.topic}</td>
                        <td>{reassignment.partition}</td>
                        <td>{reassignment.currentReplicas.join(', ')}</td>
                        <td>{reassignment.targetReplicas.join(', ')}</td>
                        <td>
                          {[
                            ...reassignment.addingReplicas.map(
                              (brokerId) => `+${brokerId}`
                            ),
                            ...reassignment.removingReplicas.map(
                              (brokerId) => `−${brokerId}`
                            ),
                          ].join(' ') || '—'}
                        </td>
                        <td>
                          <S.Progress>
                            <ProgressBar
                              completed={reassignment.progressPercent}
                            />
                            <span>{reassignment.progressPercent}%</span>
                          </S.Progress>
                        </td>
                      </tr>
                    )
                  )}
                </tbody>
              </S.Table>
            )}
          {activeReassignments.data &&
            activeReassignments.data.reassignments.length > 0 && (
              <S.Actions>
                <Button
                  buttonType="danger"
                  buttonSize="M"
                  disabled={
                    selected.size === 0 || !capabilities.data?.executionEnabled
                  }
                  onClick={openCancel}
                >
                  Cancel selected
                </Button>
              </S.Actions>
            )}
        </S.Panel>
        <S.Panel>
          <h2>Target assignments</h2>
          <S.Description>
            Preview current and target replicas. Execution requires validation
            and explicit confirmation.
          </S.Description>
          <label htmlFor="partition-reassignment-json">
            Target assignments JSON
          </label>
          <S.JsonInput
            id="partition-reassignment-json"
            value={requestJson}
            onChange={(event) => setRequestJson(event.target.value)}
            spellCheck={false}
          />
          <S.Actions>
            <Button
              buttonType="primary"
              buttonSize="M"
              inProgress={createPlan.isPending}
              onClick={handleGenerate}
            >
              Generate plan
            </Button>
          </S.Actions>
          {error && <S.ErrorMessage role="alert">{error}</S.ErrorMessage>}
        </S.Panel>

        {plan && (
          <S.Panel>
            <h2>Plan preview</h2>
            <div>
              <strong>Plan hash: </strong>
              <S.Hash>{plan.planHash}</S.Hash>
            </div>
            <S.Table>
              <thead>
                <tr>
                  <th>Topic</th>
                  <th>Partition</th>
                  <th>Current replicas</th>
                  <th>Target replicas</th>
                </tr>
              </thead>
              <tbody>
                {plan.changes.map((change) => (
                  <tr key={`${change.topic}-${change.partition}`}>
                    <td>{change.topic}</td>
                    <td>{change.partition}</td>
                    <td>{change.currentReplicas.join(', ')}</td>
                    <td>{change.targetReplicas.join(', ')}</td>
                  </tr>
                ))}
              </tbody>
            </S.Table>
            <S.Actions>
              <Button
                buttonType="secondary"
                buttonSize="M"
                inProgress={validatePlan.isPending}
                onClick={handleValidate}
              >
                Revalidate plan
              </Button>
              {isValid && capabilities.data?.executionEnabled && (
                <Button
                  buttonType="danger"
                  buttonSize="M"
                  onClick={openExecute}
                >
                  Execute plan
                </Button>
              )}
            </S.Actions>
            {isValid && <S.SuccessMessage>Plan is valid</S.SuccessMessage>}
          </S.Panel>
        )}
        {message && <S.SuccessMessage>{message}</S.SuccessMessage>}
      </S.Content>
      <Modal
        isOpen={isExecuteOpen}
        onClose={() => !executePlan.isPending && setIsExecuteOpen(false)}
        title="Execute reassignment plan"
        maxWidth="560px"
        footer={
          <S.ModalActions>
            <Button
              buttonType="secondary"
              buttonSize="M"
              disabled={executePlan.isPending}
              onClick={() => setIsExecuteOpen(false)}
            >
              Cancel
            </Button>
            <Button
              buttonType="danger"
              buttonSize="M"
              inProgress={executePlan.isPending}
              onClick={handleExecute}
            >
              Execute reassignment
            </Button>
          </S.ModalActions>
        }
      >
        <S.ModalBody>
          <S.WarningMessage>
            This submits {plan?.changes.length ?? 0} partition changes to Kafka
            immediately. Completion is tracked under Active reassignments.
          </S.WarningMessage>
          <S.ExecutionSummary aria-label="Reassignment execution summary">
            {plan?.changes.map((change) => (
              <li key={`${change.topic}:${change.partition}`}>
                <strong>
                  {change.topic}-{change.partition}
                </strong>
                <span>
                  {change.currentReplicas.join(', ')} →{' '}
                  {change.targetReplicas.join(', ')}
                </span>
              </li>
            ))}
          </S.ExecutionSummary>
          <label>
            <input
              type="checkbox"
              checked={isThrottleEnabled}
              onChange={(event) => setIsThrottleEnabled(event.target.checked)}
            />{' '}
            Throttle replication
          </label>
          {isThrottleEnabled && (
            <S.Field>
              <label htmlFor="reassignment-throttle">
                Throttle bytes per second
              </label>
              <S.NumberInput
                id="reassignment-throttle"
                type="number"
                min="1"
                step="1"
                value={throttle}
                onChange={(event) => setThrottle(event.target.value)}
              />
            </S.Field>
          )}
        </S.ModalBody>
      </Modal>
      <Modal
        isOpen={isCancelOpen}
        onClose={() => !cancelReassignments.isPending && setIsCancelOpen(false)}
        title="Cancel active reassignments"
        maxWidth="520px"
        footer={
          <S.ModalActions>
            <Button
              buttonType="secondary"
              buttonSize="M"
              disabled={cancelReassignments.isPending}
              onClick={() => setIsCancelOpen(false)}
            >
              Keep running
            </Button>
            <Button
              buttonType="danger"
              buttonSize="M"
              inProgress={cancelReassignments.isPending}
              onClick={handleCancel}
            >
              Cancel reassignments
            </Button>
          </S.ModalActions>
        }
      >
        <S.ModalBody>
          <S.WarningMessage>
            Cancel {selected.size} selected active{' '}
            {selected.size === 1 ? 'partition' : 'partitions'}? Kafka will
            revert each selected reassignment to its previous replica set.
          </S.WarningMessage>
        </S.ModalBody>
      </Modal>
    </S.Container>
  );
};

export default PartitionReassignment;
