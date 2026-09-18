import { act, renderHook, waitFor } from '@testing-library/react';
import fetchMock from 'fetch-mock';
import { TestQueryClientProvider } from 'lib/testHelpers';
import {
  useActivePartitionReassignments,
  useCancelPartitionReassignments,
  useCreatePartitionReassignmentPlan,
  useExecutePartitionReassignmentPlan,
  usePartitionReassignmentCapabilities,
  useValidatePartitionReassignmentPlan,
} from 'lib/hooks/api/partitionReassignments';

const clusterName = 'test-cluster';
const basePath = `/api/clusters/${clusterName}/partition-reassignments`;

describe('partition reassignment hooks', () => {
  beforeEach(() => fetchMock.restore());

  it('loads effective execution capabilities', async () => {
    const mock = fetchMock.getOnce(`${basePath}/capabilities`, {
      executionEnabled: false,
      reason: 'Execution is disabled.',
    });
    const { result } = renderHook(
      () => usePartitionReassignmentCapabilities(clusterName),
      { wrapper: TestQueryClientProvider }
    );

    await waitFor(() => expect(result.current.isSuccess).toBeTruthy());
    expect(result.current.data).toEqual({
      executionEnabled: false,
      reason: 'Execution is disabled.',
    });
    expect(mock.calls()).toHaveLength(1);
  });

  it('polls active partition reassignments', async () => {
    const response = {
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
    };
    const mock = fetchMock.get(`${basePath}`, response);
    const { result } = renderHook(
      () => useActivePartitionReassignments(clusterName),
      { wrapper: TestQueryClientProvider }
    );

    await waitFor(() => expect(result.current.data).toEqual(response));
    expect(mock.calls()).toHaveLength(1);
  });

  it('creates a read-only reassignment plan', async () => {
    const mock = fetchMock.postOnce(`${basePath}/plan`, {
      clusterName,
      changes: [],
      planHash: 'hash',
    });
    const { result } = renderHook(
      () => useCreatePartitionReassignmentPlan(clusterName),
      { wrapper: TestQueryClientProvider }
    );

    await act(() =>
      result.current.mutateAsync({
        assignments: [{ topic: 'orders', partition: 0, replicas: [2, 3] }],
      })
    );

    await waitFor(() => expect(result.current.isSuccess).toBeTruthy());
    expect(mock.calls()).toHaveLength(1);
  });

  it('revalidates an existing plan', async () => {
    const mock = fetchMock.postOnce(`${basePath}/validate`, { valid: true });
    const { result } = renderHook(
      () => useValidatePartitionReassignmentPlan(clusterName),
      { wrapper: TestQueryClientProvider }
    );

    await act(() =>
      result.current.mutateAsync({
        changes: [
          {
            topic: 'orders',
            partition: 0,
            currentReplicas: [1, 2],
            targetReplicas: [2, 3],
          },
        ],
        planHash: 'hash',
      })
    );

    await waitFor(() => expect(result.current.isSuccess).toBeTruthy());
    expect(mock.calls()).toHaveLength(1);
  });

  it('submits a validated plan for execution', async () => {
    const mock = fetchMock.postOnce(`${basePath}/execute`, {
      acceptedPartitions: 1,
    });
    const { result } = renderHook(
      () => useExecutePartitionReassignmentPlan(clusterName),
      { wrapper: TestQueryClientProvider }
    );

    await act(() =>
      result.current.mutateAsync({
        changes: [
          {
            topic: 'orders',
            partition: 0,
            currentReplicas: [1, 2],
            targetReplicas: [2, 3],
          },
        ],
        planHash: 'hash',
      })
    );

    await waitFor(() => expect(result.current.isSuccess).toBeTruthy());
    expect(mock.calls()).toHaveLength(1);
  });

  it('cancels selected active partition reassignments', async () => {
    const mock = fetchMock.postOnce(`${basePath}/cancel`, {
      operationId: 'cancel-1',
      cancelledPartitions: 1,
      skippedPartitions: 1,
    });
    const { result } = renderHook(
      () => useCancelPartitionReassignments(clusterName),
      { wrapper: TestQueryClientProvider }
    );

    await act(() =>
      result.current.mutateAsync({
        operationId: 'cancel-1',
        partitions: [
          { topic: 'orders', partition: 0 },
          { topic: 'orders', partition: 1 },
        ],
      })
    );

    await waitFor(() => expect(result.current.isSuccess).toBeTruthy());
    expect(mock.calls()).toHaveLength(1);
  });
});
