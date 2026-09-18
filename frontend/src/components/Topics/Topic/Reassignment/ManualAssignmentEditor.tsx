import React from 'react';
import type { Broker } from 'generated-sources';

import * as S from './AssignmentPlanner.styled';

interface PartitionAssignment {
  partition: number;
  currentReplicas: number[];
}

interface Props {
  assignments: PartitionAssignment[];
  brokers: Broker[];
  values: Record<number, number[]>;
  onChange(partition: number, value: number[]): void;
}

const brokerLabel = ({ host, id, port }: Broker) => {
  if (!host) return `Broker ${id}`;
  return `Broker ${id} — ${host}${port === undefined ? '' : `:${port}`}`;
};

const ManualAssignmentEditor: React.FC<Props> = ({
  assignments,
  brokers,
  values,
  onChange,
}) => {
  const sortedBrokers = React.useMemo(
    () => [...brokers].sort((left, right) => left.id - right.id),
    [brokers]
  );

  return (
    <S.ManualEditor>
      <S.Hint>
        Choose brokers in replica order. The first broker is the preferred
        leader.
      </S.Hint>
      <S.Table aria-label="Manual partition assignments">
        <thead>
          <tr>
            <th>Partition</th>
            <th>Current replicas</th>
            <th>Target replicas</th>
          </tr>
        </thead>
        <tbody>
          {assignments.map(({ partition, currentReplicas }) => {
            const replicas = values[partition] ?? [];
            const nextBroker = sortedBrokers.find(
              (broker) => !replicas.includes(broker.id)
            );
            return (
              <tr key={partition}>
                <td>{partition}</td>
                <td>{currentReplicas.join(', ')}</td>
                <td>
                  <S.ReplicaControls>
                    {replicas.map((brokerId, index) => (
                      <S.ReplicaRow key={`${partition}-${brokerId}`}>
                        <S.ReplicaSelect
                          aria-label={`Target replica ${index + 1} for partition ${partition}${
                            index === 0 ? ' (preferred leader)' : ''
                          }`}
                          value={brokerId}
                          onChange={(event) => {
                            const nextReplicas = [...replicas];
                            nextReplicas[index] = Number(event.target.value);
                            onChange(partition, nextReplicas);
                          }}
                        >
                          {sortedBrokers.map((broker) => (
                            <option
                              key={broker.id}
                              value={broker.id}
                              disabled={replicas.some(
                                (selectedBrokerId, selectedIndex) =>
                                  selectedIndex !== index &&
                                  selectedBrokerId === broker.id
                              )}
                            >
                              {brokerLabel(broker)}
                            </option>
                          ))}
                        </S.ReplicaSelect>
                        {replicas.length > 1 && (
                          <S.InlineAction
                            type="button"
                            aria-label={`Remove target replica ${index + 1} from partition ${partition}`}
                            onClick={() =>
                              onChange(
                                partition,
                                replicas.filter(
                                  (_, replicaIndex) => replicaIndex !== index
                                )
                              )
                            }
                          >
                            Remove
                          </S.InlineAction>
                        )}
                      </S.ReplicaRow>
                    ))}
                    <S.InlineAction
                      type="button"
                      aria-label={`Add target replica to partition ${partition}`}
                      disabled={!nextBroker}
                      onClick={() => {
                        if (nextBroker) {
                          onChange(partition, [...replicas, nextBroker.id]);
                        }
                      }}
                    >
                      + Add replica
                    </S.InlineAction>
                  </S.ReplicaControls>
                </td>
              </tr>
            );
          })}
        </tbody>
      </S.Table>
    </S.ManualEditor>
  );
};

export default ManualAssignmentEditor;
