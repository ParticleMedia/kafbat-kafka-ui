import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  PartitionReassignmentCancellationRequest,
  PartitionReassignmentExecutionRequest,
  PartitionReassignmentPlanRequest,
  PartitionReassignmentValidationRequest,
} from 'generated-sources';
import { partitionReassignmentsApiClient as api } from 'lib/api';
import { apiFetch } from 'lib/errorHandling';
import { ClusterName } from 'lib/interfaces/cluster';

export const usePartitionReassignmentCapabilities = (
  clusterName: ClusterName
) =>
  useQuery({
    queryKey: [
      'clusters',
      clusterName,
      'partition-reassignments',
      'capabilities',
    ],
    queryFn: () =>
      apiFetch(() => api.getPartitionReassignmentCapabilities({ clusterName })),
  });

export const useActivePartitionReassignments = (clusterName: ClusterName) =>
  useQuery({
    queryKey: ['clusters', clusterName, 'partition-reassignments', 'active'],
    queryFn: () =>
      apiFetch(() => api.listActivePartitionReassignments({ clusterName })),
    refetchInterval: 5000,
  });

export const useCreatePartitionReassignmentPlan = (clusterName: ClusterName) =>
  useMutation({
    mutationFn: (request: PartitionReassignmentPlanRequest) =>
      apiFetch(() =>
        api.createPartitionReassignmentPlan({
          clusterName,
          partitionReassignmentPlanRequest: request,
        })
      ),
  });

export const useValidatePartitionReassignmentPlan = (
  clusterName: ClusterName
) =>
  useMutation({
    mutationFn: (request: PartitionReassignmentValidationRequest) =>
      apiFetch(() =>
        api.validatePartitionReassignmentPlan({
          clusterName,
          partitionReassignmentValidationRequest: request,
        })
      ),
  });

export const useExecutePartitionReassignmentPlan = (
  clusterName: ClusterName
) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (request: PartitionReassignmentExecutionRequest) =>
      apiFetch(() =>
        api.executePartitionReassignmentPlan({
          clusterName,
          partitionReassignmentExecutionRequest: request,
        })
      ),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: [
          'clusters',
          clusterName,
          'partition-reassignments',
          'active',
        ],
      }),
  });
};

export const useCancelPartitionReassignments = (clusterName: ClusterName) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (request: PartitionReassignmentCancellationRequest) =>
      apiFetch(() =>
        api.cancelPartitionReassignments({
          clusterName,
          partitionReassignmentCancellationRequest: request,
        })
      ),
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: [
          'clusters',
          clusterName,
          'partition-reassignments',
          'active',
        ],
      }),
  });
};
