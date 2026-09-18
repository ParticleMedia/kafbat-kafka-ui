import React from 'react';
import { act, fireEvent, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render } from 'lib/testHelpers';
import { useBrokers } from 'lib/hooks/api/brokers';
import {
  useCreatePartitionReassignmentPlan,
  useExecutePartitionReassignmentPlan,
  usePartitionReassignmentCapabilities,
  useValidatePartitionReassignmentPlan,
} from 'lib/hooks/api/partitionReassignments';
import AssignmentPlanner from 'components/Topics/Topic/Reassignment/AssignmentPlanner';
import type { Topic } from 'generated-sources';

jest.mock('lib/hooks/api/brokers');
jest.mock('lib/hooks/api/partitionReassignments');

const createPlan = jest.fn();
const validatePlan = jest.fn();
const executePlan = jest.fn();

const topic: Topic = {
  name: 'orders',
  partitionCount: 3,
  replicationFactor: 2,
  underReplicatedPartitions: 0,
  partitions: [0, 1, 2].map((partition) => ({
    partition,
    leader: 1,
    offsetMin: 0,
    offsetMax: 10,
    replicas: [
      { broker: 1, leader: true, inSync: true },
      { broker: 2, leader: false, inSync: true },
    ],
  })),
};

describe('AssignmentPlanner', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (useBrokers as jest.Mock).mockReturnValue({
      data: [{ id: 1 }, { id: 2 }, { id: 3 }],
      isLoading: false,
      error: undefined,
    });
    (useCreatePartitionReassignmentPlan as jest.Mock).mockReturnValue({
      mutateAsync: createPlan,
      isPending: false,
    });
    (useValidatePartitionReassignmentPlan as jest.Mock).mockReturnValue({
      mutateAsync: validatePlan,
      isPending: false,
    });
    (useExecutePartitionReassignmentPlan as jest.Mock).mockReturnValue({
      mutateAsync: executePlan,
      isPending: false,
    });
    (usePartitionReassignmentCapabilities as jest.Mock).mockReturnValue({
      data: { executionEnabled: true },
      isLoading: false,
    });
  });

  it('generates a dry-run preview from broker and replication selections', async () => {
    createPlan.mockResolvedValue({
      clusterName: 'dev',
      changes: [
        {
          topic: 'orders',
          partition: 1,
          currentReplicas: [1, 2],
          targetReplicas: [2, 3],
        },
        {
          topic: 'orders',
          partition: 2,
          currentReplicas: [1, 2],
          targetReplicas: [3, 1],
        },
      ],
      planHash: 'abc123',
    });
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);

    expect(
      screen.getByRole('heading', { name: 'Assignment planner' })
    ).toBeInTheDocument();
    expect(screen.getByText(/Dry-run only/i)).toBeInTheDocument();
    expect(screen.getByText('Partitions')).toBeInTheDocument();
    expect(screen.getByText('3 partitions selected')).toBeInTheDocument();
    const brokerSelect = screen.getByLabelText('Target brokers');
    expect(brokerSelect).toHaveTextContent('All brokers (3)');
    expect(brokerSelect).not.toHaveTextContent('All items are selected.');

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate preview' }))
    );

    expect(createPlan).toHaveBeenCalledWith({
      assignments: [
        { topic: 'orders', partition: 0, replicas: [1, 2] },
        { topic: 'orders', partition: 1, replicas: [2, 3] },
        { topic: 'orders', partition: 2, replicas: [3, 1] },
      ],
    });
    expect(
      await screen.findByRole('heading', { name: 'Plan preview' })
    ).toBeInTheDocument();
    expect(screen.getByText('Current')).toBeInTheDocument();
    expect(screen.getByText('Projected')).toBeInTheDocument();
    expect(
      screen.getByText(/assumes target replicas are in sync/i)
    ).toBeInTheDocument();
    expect(screen.getByRole('row', { name: /orders 1/ })).toHaveTextContent(
      '1, 2'
    );
    expect(screen.getByRole('row', { name: /orders 1/ })).toHaveTextContent(
      '2, 3'
    );
    expect(screen.getByLabelText('Plan summary')).toHaveTextContent(
      'Replica moves2'
    );
    expect(
      screen.getByRole('table', { name: 'Broker distribution changes' })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('row', { name: 'Broker 3 0 2 +2 0 1' })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('row', { name: /orders 1 1, 2 2, 3 1 2/ })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: 'Execute reassignment' })
    ).toBeDisabled();
  });

  it('shows compact broker ids when only some brokers are selected', async () => {
    (useBrokers as jest.Mock).mockReturnValue({
      data: [1, 2, 3].map((id) => ({
        id,
        host: `node-${id}.example.com`,
        port: 9092,
      })),
      isLoading: false,
      error: undefined,
    });
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);

    const brokerSelect = screen.getByLabelText('Target brokers');
    fireEvent.click(
      brokerSelect.querySelector('.dropdown-heading') as HTMLElement
    );
    await act(() =>
      userEvent.click(screen.getByRole('option', { name: /Broker 1/ }))
    );
    fireEvent.click(
      brokerSelect.querySelector('.dropdown-heading') as HTMLElement
    );

    expect(brokerSelect).toHaveTextContent('Broker 2, Broker 3');
    expect(brokerSelect).not.toHaveTextContent('node-2.example.com');
    expect(brokerSelect).not.toHaveTextContent('All items are selected.');
  });

  it('preserves broker order selected from manual assignment dropdowns', async () => {
    (useBrokers as jest.Mock).mockReturnValue({
      data: [1, 2, 3].map((id) => ({
        id,
        host: `10.0.0.${id}`,
        port: 9092,
      })),
      isLoading: false,
      error: undefined,
    });
    createPlan.mockResolvedValue({
      clusterName: 'dev',
      changes: [
        {
          topic: 'orders',
          partition: 0,
          currentReplicas: [1, 2],
          targetReplicas: [3, 2],
        },
      ],
      planHash: 'manual-plan',
    });
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);

    await act(() =>
      userEvent.click(screen.getByRole('radio', { name: 'Manual' }))
    );
    const preferredLeader = screen.getByRole('combobox', {
      name: 'Target replica 1 for partition 0 (preferred leader)',
    });
    expect(preferredLeader).toHaveValue('1');
    expect(
      within(preferredLeader).getByRole('option', {
        name: 'Broker 3 — 10.0.0.3:9092',
      })
    ).toBeInTheDocument();

    await act(() => userEvent.selectOptions(preferredLeader, '3'));
    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate preview' }))
    );

    expect(createPlan).toHaveBeenCalledWith({
      assignments: [
        { topic: 'orders', partition: 0, replicas: [3, 2] },
        { topic: 'orders', partition: 1, replicas: [1, 2] },
        { topic: 'orders', partition: 2, replicas: [1, 2] },
      ],
    });
    expect(
      await screen.findByRole('heading', { name: 'Plan preview' })
    ).toBeInTheDocument();
  });

  it('prevents duplicate brokers and supports adding a replica slot', async () => {
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);

    await act(() =>
      userEvent.click(screen.getByRole('radio', { name: 'Manual' }))
    );
    const preferredLeader = screen.getByRole('combobox', {
      name: 'Target replica 1 for partition 0 (preferred leader)',
    });
    expect(
      within(preferredLeader).getByRole('option', { name: 'Broker 2' })
    ).toBeDisabled();

    await act(() =>
      userEvent.click(
        screen.getByRole('button', {
          name: 'Add target replica to partition 0',
        })
      )
    );
    const thirdReplica = screen.getByRole('combobox', {
      name: 'Target replica 3 for partition 0',
    });
    expect(thirdReplica).toHaveValue('3');
    expect(
      screen.getByRole('button', {
        name: 'Add target replica to partition 0',
      })
    ).toBeDisabled();

    await act(() =>
      userEvent.click(
        screen.getByRole('button', {
          name: 'Remove target replica 2 from partition 0',
        })
      )
    );

    expect(
      screen.getByRole('combobox', {
        name: 'Target replica 2 for partition 0',
      })
    ).toHaveValue('3');
  });

  it('validates replication factor before calling the API', async () => {
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);
    fireEvent.change(screen.getByLabelText('Replication factor'), {
      target: { value: '4' },
    });

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate preview' }))
    );

    expect(screen.getByRole('alert')).toHaveTextContent(
      'Replication factor cannot exceed selected brokers'
    );
    expect(createPlan).not.toHaveBeenCalled();
  });

  it('revalidates the generated plan without exposing execution', async () => {
    createPlan.mockResolvedValue({
      clusterName: 'dev',
      changes: [
        {
          topic: 'orders',
          partition: 1,
          currentReplicas: [1, 2],
          targetReplicas: [2, 3],
        },
      ],
      planHash: 'abc123',
    });
    validatePlan.mockResolvedValue({ valid: true });
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate preview' }))
    );
    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Revalidate plan' }))
    );

    expect(validatePlan).toHaveBeenCalledWith({
      changes: expect.any(Array),
      planHash: 'abc123',
    });
    expect(await screen.findByText('Plan is valid')).toBeInTheDocument();
  });

  it('requires typed confirmation before executing a revalidated plan', async () => {
    createPlan.mockResolvedValue({
      clusterName: 'dev',
      changes: [
        {
          topic: 'orders',
          partition: 0,
          currentReplicas: [1, 2],
          targetReplicas: [2, 3],
        },
      ],
      planHash: 'abc123',
    });
    validatePlan.mockResolvedValue({ valid: true });
    executePlan.mockResolvedValue({ acceptedPartitions: 1 });
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate preview' }))
    );
    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Revalidate plan' }))
    );
    await act(() =>
      userEvent.click(
        screen.getByRole('button', { name: 'Execute reassignment' })
      )
    );

    const dialog = screen.getByRole('dialog', {
      name: 'Execute partition reassignment',
    });
    expect(dialog).toHaveTextContent('1 partition');
    expect(dialog).toHaveTextContent('1 replica move');
    const confirmButton = within(dialog).getByRole('button', {
      name: 'Execute reassignment',
    });
    expect(confirmButton).toBeDisabled();

    await act(() =>
      userEvent.type(
        within(dialog).getByRole('textbox', { name: 'Type orders to confirm' }),
        'orders'
      )
    );
    await act(() => userEvent.click(confirmButton));

    expect(executePlan).toHaveBeenCalledWith({
      changes: expect.any(Array),
      planHash: 'abc123',
    });
    expect(
      await screen.findByText(
        'Reassignment submitted for 1 partition. Monitor cluster operations for progress.'
      )
    ).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('explains why execution is disabled before opening confirmation', async () => {
    (usePartitionReassignmentCapabilities as jest.Mock).mockReturnValue({
      data: {
        executionEnabled: false,
        reason:
          'Partition reassignment execution is disabled by server configuration.',
      },
      isLoading: false,
    });
    createPlan.mockResolvedValue({
      clusterName: 'dev',
      changes: [
        {
          topic: 'orders',
          partition: 0,
          currentReplicas: [1, 2],
          targetReplicas: [2, 3],
        },
      ],
      planHash: 'abc123',
    });
    validatePlan.mockResolvedValue({ valid: true });
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate preview' }))
    );
    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Revalidate plan' }))
    );

    expect(
      screen.getByRole('button', { name: 'Execute reassignment' })
    ).toBeDisabled();
    expect(
      screen.getByText(/disabled by server configuration/i)
    ).toBeInTheDocument();
  });

  it('discards an in-flight preview when planner inputs change', async () => {
    let resolvePlan: (value: {
      clusterName: string;
      changes: never[];
      planHash: string;
    }) => void = () => undefined;
    createPlan.mockReturnValue(
      new Promise((resolve) => {
        resolvePlan = resolve;
      })
    );
    render(<AssignmentPlanner clusterName="dev" topic={topic} />);

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate preview' }))
    );
    act(() => {
      fireEvent.change(screen.getByLabelText('Replication factor'), {
        target: { value: '1' },
      });
    });
    await act(async () => {
      resolvePlan({ clusterName: 'dev', changes: [], planHash: 'stale' });
      await Promise.resolve();
    });

    expect(
      screen.queryByRole('heading', { name: 'Plan preview' })
    ).not.toBeInTheDocument();
  });
});
