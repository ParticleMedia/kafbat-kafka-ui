import React from 'react';
import type { Topic } from 'generated-sources';
import { ColumnDef } from '@tanstack/react-table';
import Table from 'components/common/NewTable';
import Heading from 'components/common/heading/Heading.styled';
import { Tag } from 'components/common/Tag/Tag.styled';
import { NavLink } from 'react-router-dom';
import { clusterBrokerPath } from 'lib/paths';
import {
  BrokerTopicDistribution,
  getBrokerTopicDistribution,
} from 'components/Topics/lib/topicDistributionMetrics';

import * as S from './Overview.styled';

interface Props {
  clusterName: string;
  topic: Topic;
}

const BalanceCell: React.FC<{ skewed: boolean }> = ({ skewed }) => (
  <Tag color={skewed ? 'yellow' : 'green'}>
    {skewed ? 'Skewed' : 'Balanced'}
  </Tag>
);

const getColumns = (
  clusterName: string
): ColumnDef<BrokerTopicDistribution>[] => [
  {
    header: 'Broker',
    accessorKey: 'brokerId',
    cell: ({ getValue }) => {
      const brokerId = getValue<number>();
      return (
        <NavLink to={clusterBrokerPath(clusterName, brokerId)}>
          Broker {brokerId}
        </NavLink>
      );
    },
  },
  {
    header: 'Replicas',
    accessorFn: (row) => row.partitions.length,
  },
  {
    header: 'Leaders',
    accessorFn: (row) => row.leaders.length,
  },
  {
    header: 'Partitions',
    accessorFn: (row) => row.partitions.join(', '),
  },
  {
    header: 'Replica Skew',
    accessorKey: 'skewed',
    cell: ({ getValue }) => <BalanceCell skewed={getValue<boolean>()} />,
  },
  {
    header: 'Leader Skew',
    accessorKey: 'leaderSkewed',
    cell: ({ getValue }) => <BalanceCell skewed={getValue<boolean>()} />,
  },
];

const BrokerDistributionTable: React.FC<Props> = ({ clusterName, topic }) => {
  const data = React.useMemo(() => getBrokerTopicDistribution(topic), [topic]);
  const columns = React.useMemo(() => getColumns(clusterName), [clusterName]);

  return (
    <S.DataSection>
      <Heading level={3}>Partitions by Broker</Heading>
      <Table
        columns={columns}
        data={data}
        enableSorting
        emptyMessage="No broker placement found"
      />
    </S.DataSection>
  );
};

export default BrokerDistributionTable;
