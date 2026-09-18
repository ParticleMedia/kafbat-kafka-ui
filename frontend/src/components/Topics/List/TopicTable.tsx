import React from 'react';
import { GetTopicsRequest, Topic, TopicColumnsToSort } from 'generated-sources';
import { ColumnDef } from '@tanstack/react-table';
import Table, { SizeCell } from 'components/common/NewTable';
import { useSearchParams } from 'react-router-dom';
import ClusterContext from 'components/contexts/ClusterContext';
import { useTopics } from 'lib/hooks/api/topics';
import { PER_PAGE } from 'lib/constants';
import { useLocalStoragePersister } from 'components/common/NewTable/ColumnResizer/lib';
import { formatBytes } from 'components/common/BytesFormatted/utils';
import PageLoader from 'components/common/PageLoader/PageLoader';
import ErrorPage from 'components/ErrorPage/ErrorPage';
import ColoredCell from 'components/common/NewTable/ColoredCell';
import { useClusterStats } from 'lib/hooks/api/clusters';
import { getTopicDistributionMetrics } from 'components/Topics/lib/topicDistributionMetrics';

import { TopicTitleCell } from './TopicTitleCell';
import ActionsCell from './ActionsCell';
import BatchActionsbar from './BatchActionsBar';

const BrokerSpreadHeader = () => (
  <span
    aria-label="Broker Spread"
    title="Percentage of cluster brokers hosting in-sync replicas for this topic"
  >
    Broker Spread
  </span>
);

const ReplicaSkewHeader = () => (
  <span
    aria-label="Replica Skew"
    title="Percentage of topic brokers hosting more replicas than the average"
  >
    Replica Skew
  </span>
);

const LeaderSkewHeader = () => (
  <span
    aria-label="Leader Skew"
    title="Percentage of topic brokers hosting more leaders than the average"
  >
    Leader Skew
  </span>
);

const PreferredLeadersHeader = () => (
  <span
    aria-label="Preferred Leaders"
    title="Percentage of partitions whose leader is the first replica"
  >
    Preferred Leaders
  </span>
);

const UnderReplicatedHeader = () => (
  <span
    aria-label="Under Replicated"
    title="Percentage of partitions with missing in-sync replicas"
  >
    Under Replicated
  </span>
);

const TopicTable: React.FC<{ params: GetTopicsRequest }> = ({ params }) => {
  const [searchParams] = useSearchParams();
  const { isReadOnly } = React.useContext(ClusterContext);

  const { data, error, refetch, isLoading, isRefetching } = useTopics({
    ...params,
    page: Number(searchParams.get('page') || 1),
    perPage: Number(searchParams.get('perPage') || PER_PAGE),
  });
  const { data: clusterStats } = useClusterStats(params.clusterName);

  const topics = data?.topics || [];
  const pageCount = data?.pageCount || 0;
  const brokerCount = clusterStats.brokerCount ?? 0;

  const renderHealthPercentage = (value: number, healthyWhenHigh = false) => (
    <ColoredCell
      value={`${value}%`}
      warn={
        healthyWhenHigh ? value > 50 && value <= 75 : value > 0 && value <= 33
      }
      attention={healthyWhenHigh ? value <= 50 : value >= 34}
    />
  );

  const columns = React.useMemo<ColumnDef<Topic>[]>(
    () => [
      {
        id: TopicColumnsToSort.NAME,
        header: 'Topic Name',
        accessorKey: 'name',
        cell: TopicTitleCell,
        size: 400,
        meta: {
          width: '100%',
        },
      },
      {
        id: TopicColumnsToSort.TOTAL_PARTITIONS,
        header: 'Partitions',
        accessorKey: 'partitionCount',
        size: 100,
      },
      {
        id: 'topicBrokers',
        header: 'Brokers',
        enableSorting: false,
        cell: ({ row }) =>
          getTopicDistributionMetrics(row.original, brokerCount).topicBrokers,
        size: 90,
      },
      {
        id: 'brokerSpreadPercentage',
        header: BrokerSpreadHeader,
        enableSorting: false,
        cell: ({ row }) =>
          renderHealthPercentage(
            getTopicDistributionMetrics(row.original, brokerCount)
              .brokerSpreadPercentage,
            true
          ),
        size: 120,
      },
      {
        id: 'brokerSkewPercentage',
        header: ReplicaSkewHeader,
        enableSorting: false,
        cell: ({ row }) =>
          renderHealthPercentage(
            getTopicDistributionMetrics(row.original, brokerCount)
              .brokerSkewPercentage
          ),
        size: 115,
      },
      {
        id: 'brokerLeaderSkewPercentage',
        header: LeaderSkewHeader,
        enableSorting: false,
        cell: ({ row }) =>
          renderHealthPercentage(
            getTopicDistributionMetrics(row.original, brokerCount)
              .brokerLeaderSkewPercentage
          ),
        size: 110,
      },
      {
        id: 'preferredReplicasPercentage',
        header: PreferredLeadersHeader,
        enableSorting: false,
        cell: ({ row }) =>
          renderHealthPercentage(
            getTopicDistributionMetrics(row.original, brokerCount)
              .preferredReplicasPercentage,
            true
          ),
        size: 140,
      },
      {
        id: 'underReplicatedPercentage',
        header: UnderReplicatedHeader,
        enableSorting: false,
        cell: ({ row }) =>
          renderHealthPercentage(
            getTopicDistributionMetrics(row.original, brokerCount)
              .underReplicatedPercentage
          ),
        size: 135,
      },
      {
        id: TopicColumnsToSort.OUT_OF_SYNC_REPLICAS,
        header: 'Out of sync replicas',
        accessorKey: 'partitions',
        size: 154,
        cell: ({ getValue }) => {
          const partitions = getValue<Topic['partitions']>();
          if (partitions === undefined || partitions.length === 0) {
            return 0;
          }
          return partitions.reduce((memo, { replicas }) => {
            const outOfSync = replicas?.filter(({ inSync }) => !inSync);
            return memo + (outOfSync?.length || 0);
          }, 0);
        },
      },
      {
        id: TopicColumnsToSort.REPLICATION_FACTOR,
        header: 'Replication Factor',
        accessorKey: 'replicationFactor',
        size: 148,
        maxSize: 148,
      },
      {
        id: TopicColumnsToSort.MESSAGES_COUNT,
        header: 'Number of messages',
        accessorKey: 'messagesCount',
        cell: (args) => {
          return args.getValue() ?? 'N/A';
        },
        size: 146,
      },
      {
        id: 'summedTopicOffsets',
        header: 'Recent Offsets',
        enableSorting: false,
        cell: ({ row }) =>
          getTopicDistributionMetrics(
            row.original,
            brokerCount
          ).summedTopicOffsets.toLocaleString(),
        size: 125,
      },
      {
        id: 'bytesInPerSec',
        header: 'Bytes In / sec',
        accessorKey: 'bytesInPerSec',
        enableSorting: false,
        cell: ({ getValue }) => {
          const value = getValue<number | undefined>();
          return value === undefined ? 'N/A' : `${formatBytes(value, 2)}/s`;
        },
        size: 120,
      },
      {
        id: 'bytesOutPerSec',
        header: 'Bytes Out / sec',
        accessorKey: 'bytesOutPerSec',
        enableSorting: false,
        cell: ({ getValue }) => {
          const value = getValue<number | undefined>();
          return value === undefined ? 'N/A' : `${formatBytes(value, 2)}/s`;
        },
        size: 125,
      },
      {
        id: TopicColumnsToSort.SIZE,
        header: 'Size',
        accessorKey: 'segmentSize',
        size: 100,
        cell: SizeCell,
        meta: {
          csvFn: (row: Topic) => formatBytes(row.segmentSize, 0),
        },
      },
      {
        id: 'actions',
        header: '',
        cell: ActionsCell,
        size: 60,
      },
    ],
    [brokerCount]
  );

  const columnSizingPersister = useLocalStoragePersister('Topics');

  if (isLoading || isRefetching) {
    return <PageLoader />;
  }

  if (error) {
    return <ErrorPage offsetY={201} status={error.status} onClick={refetch} />;
  }

  return (
    <Table
      data={topics}
      pageCount={pageCount}
      columns={columns}
      enableSorting
      serverSideProcessing
      batchActionsBar={BatchActionsbar}
      enableRowSelection={
        !isReadOnly ? (row) => !row.original.internal : undefined
      }
      enableColumnResizing
      columnSizingPersister={columnSizingPersister}
      emptyMessage="No topics found"
    />
  );
};

export default TopicTable;
