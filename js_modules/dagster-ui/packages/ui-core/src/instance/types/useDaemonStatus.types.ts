// Generated GraphQL types, do not edit manually.

import * as Types from '../../graphql/types';

export type InstanceWarningQueryVariables = Types.Exact<{[key: string]: never}>;

export type InstanceWarningQuery = {
  __typename: 'Query';
  instance: {
    __typename: 'Instance';
    id: string;
    hasInfo: boolean;
    daemonHealth: {
      __typename: 'DaemonHealth';
      id: string;
      allDaemonStatuses: Array<{
        __typename: 'DaemonStatus';
        id: string;
        daemonType: string;
        required: boolean;
        healthy: boolean | null;
        lastHeartbeatTime: number | null;
        lastHeartbeatErrors: Array<{
          __typename: 'PythonError';
          message: string;
          stack: Array<string>;
          errorChain: Array<{
            __typename: 'ErrorChainLink';
            isExplicitLink: boolean;
            error: {__typename: 'PythonError'; message: string; stack: Array<string>};
          }>;
        }>;
      }>;
    };
  };
  partitionBackfillsOrError:
    | {
        __typename: 'PartitionBackfills';
        results: Array<{__typename: 'PartitionBackfill'; id: string}>;
      }
    | {__typename: 'PythonError'};
};

export const InstanceWarningQueryVersion = '7ead177b08f678cb85bfebac63b1b25fff6c60c50c862a3c3d20d8d701463823';
