import React from 'react';
import type { Partition, Replica } from 'generated-sources';
import BytesFormatted from 'components/common/BytesFormatted/BytesFormatted';
import Table from 'components/common/NewTable';
import * as Metrics from 'components/common/Metrics';
import { Tag } from 'components/common/Tag/Tag.styled';
import { clusterBrokerPath, RouteParamsClusterTopic } from 'lib/paths';
import useAppParams from 'lib/hooks/useAppParams';
import { useTopicDetails } from 'lib/hooks/api/topics';
import { ColumnDef } from '@tanstack/react-table';
import { useClusterStats } from 'lib/hooks/api/clusters';
import Heading from 'components/common/heading/Heading.styled';
import { NavLink } from 'react-router-dom';

import * as S from './Overview.styled';
import ActionsCell from './ActionsCell';
import DistributionMetrics from './DistributionMetrics';
import BrokerDistributionTable from './BrokerDistributionTable';

const getPartitionColumns = (clusterName: string): ColumnDef<Partition>[] => [
  {
    header: 'Partition ID',
    enableSorting: false,
    accessorKey: 'partition',
  },
  {
    header: 'Leader',
    enableSorting: false,
    accessorKey: 'leader',
    cell: ({ getValue }) => {
      const brokerId = getValue<number | undefined>();
      return brokerId === undefined ? (
        'N/A'
      ) : (
        <NavLink to={clusterBrokerPath(clusterName, brokerId)}>
          {brokerId}
        </NavLink>
      );
    },
  },
  {
    header: 'Replicas',
    enableSorting: false,
    accessorKey: 'replicas',
    cell: ({ getValue }) => {
      const replicas = getValue<Partition['replicas']>();
      if (replicas === undefined || replicas.length === 0) {
        return 0;
      }
      return replicas.map(({ broker, leader, inSync }: Replica) => (
        <S.Replica
          leader={leader}
          outOfSync={!inSync}
          key={broker}
          title={leader ? 'Leader' : ''}
        >
          {broker}
        </S.Replica>
      ));
    },
  },
  {
    header: 'In Sync Replicas',
    enableSorting: false,
    accessorKey: 'inSyncReplicas',
    cell: ({ row }) =>
      row.original.replicas
        ?.filter(({ inSync }) => inSync)
        .map(({ broker }) => broker)
        .join(', ') || 'None',
  },
  {
    header: 'Preferred Leader',
    enableSorting: false,
    accessorKey: 'preferredLeader',
    cell: ({ row }) => {
      const preferred =
        row.original.replicas?.[0]?.broker === row.original.leader;
      return (
        <Tag color={preferred ? 'green' : 'yellow'}>
          {preferred ? 'Preferred' : 'Not preferred'}
        </Tag>
      );
    },
  },
  {
    header: 'Replication Health',
    enableSorting: false,
    accessorKey: 'replicationHealth',
    cell: ({ row }) => {
      const replicas = row.original.replicas ?? [];
      const underReplicated =
        replicas.filter(({ inSync }) => inSync).length < replicas.length;
      return (
        <Tag color={underReplicated ? 'red' : 'green'}>
          {underReplicated ? 'Under replicated' : 'Healthy'}
        </Tag>
      );
    },
  },
  {
    header: 'First Offset',
    enableSorting: false,
    accessorKey: 'offsetMin',
  },
  { header: 'Next Offset', enableSorting: false, accessorKey: 'offsetMax' },
  {
    header: 'Message Count',
    enableSorting: false,
    accessorKey: `messageCount`,
  },
  {
    header: '',
    enableSorting: false,
    accessorKey: 'actions',
    cell: ActionsCell,
  },
];

const Overview: React.FC = () => {
  const { clusterName, topicName } = useAppParams<RouteParamsClusterTopic>();
  const { data } = useTopicDetails({ clusterName, topicName });
  const { data: clusterStats } = useClusterStats(clusterName);

  const messageCount = React.useMemo(
    () =>
      (data?.partitions || []).reduce((memo, partition) => {
        return memo + partition.offsetMax - partition.offsetMin;
      }, 0),
    [data]
  );
  const newData = React.useMemo(() => {
    if (!data?.partitions) return [];

    return data.partitions.map((items: Partition) => {
      return {
        ...items,
        messageCount: items.offsetMax - items.offsetMin,
      };
    });
  }, [data?.partitions]);

  const columns = React.useMemo(
    () => getPartitionColumns(clusterName),
    [clusterName]
  );
  return (
    <>
      <Metrics.Wrapper>
        <Metrics.Section>
          <Metrics.Indicator label="Partitions">
            {data?.partitionCount}
          </Metrics.Indicator>
          <Metrics.Indicator label="Replication Factor">
            {data?.replicationFactor}
          </Metrics.Indicator>
          <Metrics.Indicator
            label="URP"
            title="Under replicated partitions"
            isAlert
            alertType={
              data?.underReplicatedPartitions === 0 ? 'success' : 'error'
            }
          >
            {data?.underReplicatedPartitions === 0 ? (
              <Metrics.LightText>
                {data?.underReplicatedPartitions}
              </Metrics.LightText>
            ) : (
              <Metrics.RedText>
                {data?.underReplicatedPartitions}
              </Metrics.RedText>
            )}
          </Metrics.Indicator>
          <Metrics.Indicator
            label="In Sync Replicas"
            isAlert
            alertType={
              data?.inSyncReplicas === data?.replicas ? 'success' : 'error'
            }
          >
            {data?.inSyncReplicas &&
            data?.replicas &&
            data?.inSyncReplicas < data?.replicas ? (
              <Metrics.RedText>{data?.inSyncReplicas}</Metrics.RedText>
            ) : (
              data?.inSyncReplicas
            )}
            <Metrics.LightText> of {data?.replicas}</Metrics.LightText>
          </Metrics.Indicator>
          <Metrics.Indicator label="Type">
            <Tag color="gray">{data?.internal ? 'Internal' : 'External'}</Tag>
          </Metrics.Indicator>
          <Metrics.Indicator label="Segment Size" title="">
            <BytesFormatted value={data?.segmentSize} />
          </Metrics.Indicator>
          <Metrics.Indicator label="Segment Count">
            {data?.segmentCount}
          </Metrics.Indicator>
          <Metrics.Indicator label="Clean Up Policy">
            <Tag color="gray">{data?.cleanUpPolicy || 'Unknown'}</Tag>
          </Metrics.Indicator>
          <Metrics.Indicator label="Message Count">
            {messageCount}
          </Metrics.Indicator>
        </Metrics.Section>
        {data && (
          <DistributionMetrics
            topic={data}
            clusterBrokerCount={clusterStats.brokerCount ?? 0}
          />
        )}
      </Metrics.Wrapper>
      {data && (
        <BrokerDistributionTable clusterName={clusterName} topic={data} />
      )}
      <S.DataSection>
        <Heading level={3}>Partition Information</Heading>
        <Table
          columns={columns}
          data={newData}
          enableSorting
          emptyMessage="No Partitions found "
        />
      </S.DataSection>
    </>
  );
};

export default Overview;
