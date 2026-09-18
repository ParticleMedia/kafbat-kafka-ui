import React from 'react';
import type {
  PartitionReassignmentPlan,
  PartitionReassignmentTarget,
  Topic,
} from 'generated-sources';
import type { Option } from 'react-multi-select-component';
import MultiSelect from 'components/common/MultiSelect/MultiSelect.styled';
import { Button } from 'components/common/Button/Button';
import Heading from 'components/common/heading/Heading.styled';
import { useBrokers } from 'lib/hooks/api/brokers';
import {
  useCreatePartitionReassignmentPlan,
  useExecutePartitionReassignmentPlan,
  usePartitionReassignmentCapabilities,
  useValidatePartitionReassignmentPlan,
} from 'lib/hooks/api/partitionReassignments';

import {
  generateBalancedAssignments,
  hasAssignmentChanges,
} from './assignmentGenerator';
import ExecuteReassignmentModal from './ExecuteReassignmentModal';
import ManualAssignmentEditor from './ManualAssignmentEditor';
import PlanPreview from './PlanPreview';
import * as S from './AssignmentPlanner.styled';

interface Props {
  clusterName: string;
  topic: Topic;
}

type AssignmentMode = 'balanced' | 'manual';

const errorMessage = (error: unknown) => {
  if (error && typeof error === 'object' && 'message' in error) {
    return String(error.message);
  }
  return 'Unable to process the partition reassignment';
};

const brokerValueRenderer = (selected: Option[], options: Option[]) => {
  if (selected.length === 0) return 'Select brokers';
  if (selected.length === options.length) {
    return `All brokers (${options.length})`;
  }
  if (selected.length <= 2) {
    return selected.map((broker) => `Broker ${broker.value}`).join(', ');
  }
  return `${selected.length} brokers selected`;
};

const replicaBrokerIds = (
  replicas: Array<{ broker?: number }> | undefined
): number[] =>
  (replicas ?? []).flatMap((replica) =>
    replica.broker === undefined ? [] : [replica.broker]
  );

const AssignmentPlanner: React.FC<Props> = ({ clusterName, topic }) => {
  const {
    data: brokers = [],
    isLoading,
    error: brokersError,
  } = useBrokers(clusterName);
  const createPlan = useCreatePartitionReassignmentPlan(clusterName);
  const validatePlan = useValidatePartitionReassignmentPlan(clusterName);
  const executePlan = useExecutePartitionReassignmentPlan(clusterName);
  const executionCapabilities =
    usePartitionReassignmentCapabilities(clusterName);
  const [mode, setMode] = React.useState<AssignmentMode>('balanced');
  const [selectedBrokers, setSelectedBrokers] = React.useState<Option[]>([]);
  const partitionOptions = React.useMemo<Option[]>(
    () =>
      [...(topic.partitions ?? [])]
        .sort((left, right) => left.partition - right.partition)
        .map((partition) => ({
          value: partition.partition,
          label: `Partition ${partition.partition}`,
        })),
    [topic.partitions]
  );
  const [selectedPartitions, setSelectedPartitions] =
    React.useState<Option[]>(partitionOptions);
  const [replicationFactor, setReplicationFactor] = React.useState(
    topic.replicationFactor ?? 1
  );
  const [manualAssignments, setManualAssignments] = React.useState<
    Record<number, number[]>
  >(() =>
    Object.fromEntries(
      (topic.partitions ?? []).map((partition) => [
        partition.partition,
        replicaBrokerIds(partition.replicas),
      ])
    )
  );
  const [plan, setPlan] = React.useState<PartitionReassignmentPlan>();
  const [assignments, setAssignments] = React.useState<
    PartitionReassignmentTarget[]
  >([]);
  const [error, setError] = React.useState<string>();
  const [message, setMessage] = React.useState<string>();
  const [isValid, setIsValid] = React.useState(false);
  const [isConfirmationOpen, setIsConfirmationOpen] = React.useState(false);
  const initializedBrokers = React.useRef(false);
  const previewVersion = React.useRef(0);

  const brokerOptions = React.useMemo<Option[]>(
    () =>
      [...brokers]
        .sort((left, right) => left.id - right.id)
        .map((broker) => ({
          value: broker.id,
          label: `Broker ${broker.id}${
            broker.host ? ` (${broker.host}:${broker.port ?? ''})` : ''
          }`,
        })),
    [brokers]
  );

  React.useEffect(() => {
    if (!initializedBrokers.current && brokerOptions.length > 0) {
      setSelectedBrokers(brokerOptions);
      initializedBrokers.current = true;
    }
  }, [brokerOptions]);

  const clearPreview = () => {
    previewVersion.current += 1;
    setPlan(undefined);
    setAssignments([]);
    setError(undefined);
    setMessage(undefined);
    setIsValid(false);
  };

  const handleBrokerChange = (options: Option[]) => {
    clearPreview();
    setSelectedBrokers(options);
  };

  const handleReplicationFactorChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    clearPreview();
    setReplicationFactor(Number(event.target.value));
  };

  const handlePartitionChange = (options: Option[]) => {
    clearPreview();
    setSelectedPartitions(options);
  };

  const handleModeChange = (nextMode: AssignmentMode) => {
    clearPreview();
    setMode(nextMode);
  };

  const handleManualAssignmentChange = (partition: number, value: number[]) => {
    clearPreview();
    setManualAssignments((current) => ({ ...current, [partition]: value }));
  };

  const buildManualAssignments = (): PartitionReassignmentTarget[] =>
    selectedPartitions.map((selectedPartition) => {
      const partition = Number(selectedPartition.value);
      const replicas = manualAssignments[partition] ?? [];
      if (replicas.length === 0) {
        throw new Error(`Partition ${partition} requires a target broker`);
      }
      if (new Set(replicas).size !== replicas.length) {
        throw new Error(
          `Partition ${partition} target replicas must not contain duplicate brokers`
        );
      }
      return { topic: topic.name, partition, replicas };
    });

  const handleGenerate = async () => {
    clearPreview();
    const requestVersion = previewVersion.current;
    try {
      const targets =
        mode === 'manual'
          ? buildManualAssignments()
          : generateBalancedAssignments(
              topic,
              selectedBrokers.map((broker) => Number(broker.value)),
              replicationFactor,
              selectedPartitions.map((partition) => Number(partition.value))
            );
      if (!hasAssignmentChanges(topic, targets)) {
        setMessage('The topic already matches this target assignment');
        return;
      }
      const generatedPlan = await createPlan.mutateAsync({
        assignments: targets,
      });
      if (previewVersion.current !== requestVersion) return;
      setAssignments(targets);
      setPlan(generatedPlan);
    } catch (caughtError) {
      if (previewVersion.current === requestVersion) {
        setError(errorMessage(caughtError));
      }
    }
  };

  const handleValidate = async () => {
    if (!plan) return;
    const requestVersion = previewVersion.current;
    setError(undefined);
    setIsValid(false);
    try {
      const result = await validatePlan.mutateAsync({
        changes: plan.changes,
        planHash: plan.planHash,
      });
      if (previewVersion.current === requestVersion) {
        setIsValid(result.valid);
      }
    } catch (caughtError) {
      if (previewVersion.current === requestVersion) {
        setError(errorMessage(caughtError));
      }
    }
  };

  const handleExecute = async () => {
    if (!plan || !isValid || !executionCapabilities.data?.executionEnabled)
      return;
    setError(undefined);
    try {
      const result = await executePlan.mutateAsync({
        changes: plan.changes,
        planHash: plan.planHash,
      });
      setIsConfirmationOpen(false);
      setIsValid(false);
      setMessage(
        `Reassignment submitted for ${result.acceptedPartitions} ${
          result.acceptedPartitions === 1 ? 'partition' : 'partitions'
        }. Monitor cluster operations for progress.`
      );
    } catch (caughtError) {
      setIsConfirmationOpen(false);
      setError(errorMessage(caughtError));
    }
  };

  return (
    <S.Section>
      <Heading level={3}>Assignment planner</Heading>
      <S.Panel>
        <S.Description>
          Generate or manually define a target assignment and preview its
          impact. Preview is dry-run only; execution requires confirmation.
        </S.Description>
        <S.ModeSelector>
          <legend>Assignment method</legend>
          <label>
            <input
              type="radio"
              name="assignment-method"
              checked={mode === 'balanced'}
              onChange={() => handleModeChange('balanced')}
            />
            Balanced
          </label>
          <label>
            <input
              type="radio"
              name="assignment-method"
              checked={mode === 'manual'}
              onChange={() => handleModeChange('manual')}
            />
            Manual
          </label>
        </S.ModeSelector>
        <S.Controls>
          {mode === 'balanced' && (
            <S.Field>
              <S.Label id="target-brokers-label">Target brokers</S.Label>
              <MultiSelect
                labelledBy="target-brokers-label"
                options={brokerOptions}
                value={selectedBrokers}
                onChange={handleBrokerChange}
                valueRenderer={brokerValueRenderer}
                disabled={isLoading}
                hasSelectAll
                overrideStrings={{ selectSomeItems: 'Select brokers' }}
                minWidth="280px"
              />
              <S.Hint>{selectedBrokers.length} brokers selected</S.Hint>
            </S.Field>
          )}
          <S.Field>
            <S.Label id="target-partitions-label">Partitions</S.Label>
            <MultiSelect
              labelledBy="target-partitions-label"
              options={partitionOptions}
              value={selectedPartitions}
              onChange={handlePartitionChange}
              hasSelectAll
              overrideStrings={{ selectSomeItems: 'Select partitions' }}
              minWidth="240px"
            />
            <S.Hint>{selectedPartitions.length} partitions selected</S.Hint>
          </S.Field>
          {mode === 'balanced' && (
            <S.Field>
              <S.Label htmlFor="assignment-replication-factor">
                Replication factor
              </S.Label>
              <S.NumberInput
                id="assignment-replication-factor"
                aria-label="Replication factor"
                type="number"
                min={1}
                max={selectedBrokers.length || 1}
                step={1}
                value={replicationFactor}
                onChange={handleReplicationFactorChange}
              />
              <S.Hint>Maximum {selectedBrokers.length || 0}</S.Hint>
            </S.Field>
          )}
        </S.Controls>

        {mode === 'manual' && (
          <ManualAssignmentEditor
            assignments={[...(topic.partitions ?? [])]
              .filter((partition) =>
                selectedPartitions.some(
                  (selected) => Number(selected.value) === partition.partition
                )
              )
              .sort((left, right) => left.partition - right.partition)
              .map((partition) => ({
                partition: partition.partition,
                currentReplicas: replicaBrokerIds(partition.replicas),
              }))}
            brokers={brokers}
            values={manualAssignments}
            onChange={handleManualAssignmentChange}
          />
        )}

        <S.Actions>
          <Button
            buttonType="primary"
            buttonSize="M"
            inProgress={createPlan.isPending}
            disabled={
              isLoading ||
              Boolean(brokersError) ||
              selectedPartitions.length === 0
            }
            onClick={handleGenerate}
          >
            Generate preview
          </Button>
        </S.Actions>

        {brokersError && (
          <S.ErrorMessage role="alert">
            Unable to load available brokers
          </S.ErrorMessage>
        )}
        {error && <S.ErrorMessage role="alert">{error}</S.ErrorMessage>}
        {message && (
          <S.SuccessMessage role="status">{message}</S.SuccessMessage>
        )}

        {plan && (
          <PlanPreview
            assignments={assignments}
            clusterBrokerCount={brokers.length}
            isValid={isValid}
            isValidating={validatePlan.isPending}
            isExecutionEnabled={
              executionCapabilities.data?.executionEnabled === true
            }
            executionDisabledReason={
              executionCapabilities.isLoading
                ? 'Checking whether reassignment execution is available.'
                : (executionCapabilities.data?.reason ??
                  (executionCapabilities.error
                    ? 'Unable to determine whether reassignment execution is available.'
                    : undefined))
            }
            onExecute={() => setIsConfirmationOpen(true)}
            onValidate={handleValidate}
            plan={plan}
            selectedBrokerCount={
              mode === 'manual'
                ? new Set(assignments.flatMap((target) => target.replicas)).size
                : selectedBrokers.length
            }
            topic={topic}
          />
        )}
        {plan && executionCapabilities.data?.executionEnabled && (
          <ExecuteReassignmentModal
            clusterName={clusterName}
            isExecuting={executePlan.isPending}
            isOpen={isConfirmationOpen}
            onCancel={() => setIsConfirmationOpen(false)}
            onConfirm={handleExecute}
            plan={plan}
            topicName={topic.name}
          />
        )}
      </S.Panel>
    </S.Section>
  );
};

export default AssignmentPlanner;
