import React from 'react';
import type { PartitionReassignmentPlan } from 'generated-sources';
import { Button } from 'components/common/Button/Button';
import { Modal } from 'components/common/Modal';

import * as S from './AssignmentPlanner.styled';

interface Props {
  clusterName: string;
  isExecuting: boolean;
  isOpen: boolean;
  onCancel(): void;
  onConfirm(): void;
  plan: PartitionReassignmentPlan;
  topicName: string;
}

const ExecuteReassignmentModal: React.FC<Props> = ({
  clusterName,
  isExecuting,
  isOpen,
  onCancel,
  onConfirm,
  plan,
  topicName,
}) => {
  const [confirmation, setConfirmation] = React.useState('');
  React.useEffect(() => {
    if (!isOpen) setConfirmation('');
  }, [isOpen]);
  const replicaMoves = plan.changes.reduce(
    (total, change) =>
      total +
      change.targetReplicas.filter(
        (brokerId) => !change.currentReplicas.includes(brokerId)
      ).length,
    0
  );
  const close = () => {
    if (!isExecuting) {
      setConfirmation('');
      onCancel();
    }
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={close}
      title="Execute partition reassignment"
      maxWidth="560px"
      footer={
        <S.ModalActions>
          <Button
            buttonType="secondary"
            buttonSize="M"
            disabled={isExecuting}
            onClick={close}
          >
            Cancel
          </Button>
          <Button
            buttonType="danger"
            buttonSize="M"
            disabled={confirmation !== topicName}
            inProgress={isExecuting}
            onClick={onConfirm}
          >
            Execute reassignment
          </Button>
        </S.ModalActions>
      }
    >
      <S.ConfirmationBody>
        <S.Warning>
          This submits changes to Kafka immediately. Submission does not mean
          the reassignment has finished.
        </S.Warning>
        <S.ConfirmationSummary>
          <div>
            <dt>Cluster</dt>
            <dd>{clusterName}</dd>
          </div>
          <div>
            <dt>Topic</dt>
            <dd>{topicName}</dd>
          </div>
          <div>
            <dt>Changed partitions</dt>
            <dd>
              {plan.changes.length}{' '}
              {plan.changes.length === 1 ? 'partition' : 'partitions'}
            </dd>
          </div>
          <div>
            <dt>Replica moves</dt>
            <dd>
              {replicaMoves}{' '}
              {replicaMoves === 1 ? 'replica move' : 'replica moves'}
            </dd>
          </div>
        </S.ConfirmationSummary>
        <S.Label htmlFor="reassignment-confirmation">
          Type {topicName} to confirm
        </S.Label>
        <S.ConfirmInput
          id="reassignment-confirmation"
          aria-label={`Type ${topicName} to confirm`}
          autoComplete="off"
          autoFocus
          value={confirmation}
          onChange={(event) => setConfirmation(event.target.value)}
        />
      </S.ConfirmationBody>
    </Modal>
  );
};

export default ExecuteReassignmentModal;
