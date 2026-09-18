import React from 'react';
import { act, fireEvent, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render, WithRoute } from 'lib/testHelpers';
import { clusterPartitionReassignmentPath } from 'lib/paths';
import {
  useActivePartitionReassignments,
  useCancelPartitionReassignments,
  useCreatePartitionReassignmentPlan,
  useExecutePartitionReassignmentPlan,
  usePartitionReassignmentCapabilities,
  useValidatePartitionReassignmentPlan,
} from 'lib/hooks/api/partitionReassignments';
import PartitionReassignment from 'components/ClusterOperations/PartitionReassignment/PartitionReassignment';

jest.mock('lib/hooks/api/partitionReassignments');

const createPlan = jest.fn();
const validatePlan = jest.fn();
const executePlan = jest.fn();
const cancelReassignments = jest.fn();

describe('PartitionReassignment', () => {
  beforeEach(() => {
    jest.clearAllMocks();
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
    (useCancelPartitionReassignments as jest.Mock).mockReturnValue({
      mutateAsync: cancelReassignments,
      isPending: false,
    });
    (useActivePartitionReassignments as jest.Mock).mockReturnValue({
      data: { reassignments: [] },
      isLoading: false,
      error: null,
    });
    (usePartitionReassignmentCapabilities as jest.Mock).mockReturnValue({
      data: {
        executionEnabled: false,
        reason:
          'Partition reassignment execution is disabled by server configuration.',
      },
      isLoading: false,
    });
  });

  const renderComponent = () =>
    render(
      <WithRoute path={clusterPartitionReassignmentPath()}>
        <PartitionReassignment />
      </WithRoute>,
      { initialEntries: [clusterPartitionReassignmentPath('dev')] }
    );

  it('generates and renders a read-only plan', async () => {
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
    renderComponent();
    fireEvent.change(screen.getByLabelText('Target assignments JSON'), {
      target: {
        value: JSON.stringify({
          assignments: [{ topic: 'orders', partition: 0, replicas: [2, 3] }],
        }),
      },
    });

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate plan' }))
    );

    expect(await screen.findByText('abc123')).toBeInTheDocument();
    expect(screen.getByText('orders')).toBeInTheDocument();
    expect(screen.getByText('1, 2')).toBeInTheDocument();
    expect(screen.getByText('2, 3')).toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /execute/i })
    ).not.toBeInTheDocument();
  });

  it('shows active reassignment progress and replica movement', () => {
    (useActivePartitionReassignments as jest.Mock).mockReturnValue({
      data: {
        reassignments: [
          {
            topic: 'orders',
            partition: 0,
            currentReplicas: [1, 2],
            targetReplicas: [2, 3],
            addingReplicas: [3],
            removingReplicas: [1],
            progressPercent: 50,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    renderComponent();

    expect(
      screen.getByRole('heading', { name: 'Active reassignments' })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('row', { name: /orders 0 1, 2 2, 3/ })
    ).toHaveTextContent('+3');
    expect(
      screen.getByRole('row', { name: /orders 0 1, 2 2, 3/ })
    ).toHaveTextContent('−1');
    expect(screen.getByText('50%')).toBeInTheDocument();
  });

  it('shows an empty state when Kafka has no active reassignments', () => {
    renderComponent();

    expect(
      screen.getByText('No active partition reassignments')
    ).toBeInTheDocument();
    expect(
      screen.getByText(/disabled by server configuration/i)
    ).toBeInTheDocument();
  });

  it('rejects invalid JSON before calling the API', async () => {
    renderComponent();
    fireEvent.change(screen.getByLabelText('Target assignments JSON'), {
      target: { value: '{not-json' },
    });

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate plan' }))
    );

    expect(screen.getByRole('alert')).toHaveTextContent(
      'Enter valid target assignments JSON'
    );
    expect(createPlan).not.toHaveBeenCalled();
  });

  it('revalidates the generated plan', async () => {
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
    renderComponent();

    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Generate plan' }))
    );
    await act(() =>
      userEvent.click(screen.getByRole('button', { name: 'Revalidate plan' }))
    );

    expect(await screen.findByText('Plan is valid')).toBeInTheDocument();
    expect(validatePlan).toHaveBeenCalledWith({
      changes: expect.any(Array),
      planHash: 'abc123',
    });
  });

  it('confirms and executes a valid batch plan with an optional throttle', async () => {
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
    executePlan.mockResolvedValue({
      operationId: 'execute-1',
      acceptedPartitions: 1,
      status: 'REASSIGNMENT_SUBMITTED',
    });
    (usePartitionReassignmentCapabilities as jest.Mock).mockReturnValue({
      data: { executionEnabled: true },
      isLoading: false,
    });
    renderComponent();

    await userEvent.click(
      screen.getByRole('button', { name: 'Generate plan' })
    );
    await userEvent.click(
      screen.getByRole('button', { name: 'Revalidate plan' })
    );
    await userEvent.click(screen.getByRole('button', { name: 'Execute plan' }));
    const dialog = screen.getByRole('dialog', {
      name: 'Execute reassignment plan',
    });
    expect(dialog).toBeInTheDocument();
    const summary = within(dialog).getByRole('list', {
      name: 'Reassignment execution summary',
    });
    expect(summary).toHaveTextContent('orders-0');
    expect(summary).toHaveTextContent('1, 2 → 2, 3');
    await userEvent.click(screen.getByLabelText('Throttle replication'));
    await userEvent.type(
      screen.getByLabelText('Throttle bytes per second'),
      '1000000'
    );
    await userEvent.click(
      screen.getByRole('button', { name: 'Execute reassignment' })
    );

    expect(executePlan).toHaveBeenCalledWith({
      changes: expect.any(Array),
      planHash: 'abc123',
      operationId: expect.any(String),
      throttleBytesPerSecond: 1000000,
    });
    expect(
      await screen.findByText('Reassignment submitted for 1 partition')
    ).toBeInTheDocument();
  });

  it('reports an idempotent execution that still needs reconciliation', async () => {
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
    executePlan.mockResolvedValue({
      operationId: 'execute-1',
      acceptedPartitions: 0,
      status: 'THROTTLE_APPLIED',
    });
    (usePartitionReassignmentCapabilities as jest.Mock).mockReturnValue({
      data: { executionEnabled: true },
      isLoading: false,
    });
    renderComponent();

    await userEvent.click(
      screen.getByRole('button', { name: 'Generate plan' })
    );
    await userEvent.click(
      screen.getByRole('button', { name: 'Revalidate plan' })
    );
    await userEvent.click(screen.getByRole('button', { name: 'Execute plan' }));
    await userEvent.click(
      screen.getByRole('button', { name: 'Execute reassignment' })
    );

    expect(
      await screen.findByText(
        'Operation execute-1 is throttle applied. Reconciliation will continue automatically.'
      )
    ).toBeInTheDocument();
  });

  it('selects and confirms cancellation of active reassignments', async () => {
    (usePartitionReassignmentCapabilities as jest.Mock).mockReturnValue({
      data: { executionEnabled: true },
      isLoading: false,
    });
    (useActivePartitionReassignments as jest.Mock).mockReturnValue({
      data: {
        reassignments: [
          {
            topic: 'orders',
            partition: 0,
            currentReplicas: [1, 2],
            targetReplicas: [2, 3],
            addingReplicas: [3],
            removingReplicas: [1],
            progressPercent: 50,
          },
        ],
      },
      isLoading: false,
      error: null,
    });
    cancelReassignments.mockResolvedValue({
      operationId: 'cancel-1',
      cancelledPartitions: 1,
      skippedPartitions: 0,
    });
    renderComponent();

    await userEvent.click(
      screen.getByRole('checkbox', { name: 'Select orders partition 0' })
    );
    await userEvent.click(
      screen.getByRole('button', { name: 'Cancel selected' })
    );
    expect(
      screen.getByRole('dialog', { name: 'Cancel active reassignments' })
    ).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole('button', { name: 'Cancel reassignments' })
    );

    expect(cancelReassignments).toHaveBeenCalledWith({
      operationId: expect.any(String),
      partitions: [{ topic: 'orders', partition: 0 }],
    });
    expect(
      await screen.findByText('Cancelled 1 partition; skipped 0')
    ).toBeInTheDocument();
  });
});
