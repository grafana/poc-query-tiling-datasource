import { getBackendSrv, isFetchError } from '@grafana/runtime';
import {
  CoreApp,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  LoadingState,
  DataFrameSchema,
  FieldType,
  StreamingDataFrame,
  StreamingFrameAction,
} from '@grafana/data';

import { MyQuery, MyDataSourceOptions, DEFAULT_QUERY, DataSourceResponse } from './types';
import { Observable, lastValueFrom, merge } from 'rxjs';
import _, { shuffle } from 'lodash';
import {
  generateTimeSeriesData,
  generateRandomTimeSeriesConfig,
  divideIntoChunks,
  amendTable,
  Table,
  generateRandomNumber,
} from 'utils';

export class DataSource extends DataSourceApi<MyQuery, MyDataSourceOptions> {
  baseUrl: string;

  constructor(instanceSettings: DataSourceInstanceSettings<MyDataSourceOptions>) {
    super(instanceSettings);
    this.baseUrl = instanceSettings.url!;
  }

  getDefaultQuery(_: CoreApp): Partial<MyQuery> {
    return DEFAULT_QUERY;
  }

  filterQuery(query: MyQuery): boolean {
    // if no query has been provided, prevent the query from being executed
    return !!query.queryText;
  }

  getQueryStream(
    from: number,
    to: number,
    target: MyQuery,
    panelId: number,
    interval: number,
    maxDataPoints: number
  ): Observable<DataQueryResponse> {
    const streamId = `stream-${panelId}-${target.refId}`;

    // Get some random time series data
    const timeSeriesData = generateTimeSeriesData(from, to, interval, generateRandomTimeSeriesConfig());

    // Divide time series data into chunks and chuffle them to simulate random order of data arrival
    const numberOfChunks = Math.ceil(generateRandomNumber(2, 10));
    const chunks = divideIntoChunks(timeSeriesData, numberOfChunks);

    const shuffledChunks = shuffle(Array.from({ length: numberOfChunks }, (_, i) => i));

    const stream = new Observable<DataQueryResponse>((subscriber) => {
      let result: Table = [[], []];
      let timeoutId: ReturnType<typeof setTimeout>;
      // Keep track of the current chunk index to indicate query state
      let currentIndex = 0;

      // Describe the schema of the streaming data frame
      const schema: DataFrameSchema = {
        refId: target.refId,
        fields: [
          { name: 'time', type: FieldType.time, config: { interval } },
          {
            name: target.refId,
            type: FieldType.number,
          },
        ],
      };

      // Create a new streaming data frame based on the provided schema
      const frame = StreamingDataFrame.fromDataFrameJSON(
        { schema },
        { maxLength: maxDataPoints, action: StreamingFrameAction.Replace }
      );

      const publishNextChunk = () => {
        const currentChunk = chunks[shuffledChunks[currentIndex]];
        const nextData: [number[], number[]] = [[], []];
        currentChunk.forEach((d) => {
          nextData[0].push(d[0]);
          nextData[1].push(d[1]);
        });

        // Make sure new results are correctly combined with the previous ones
        result = amendTable(result, nextData);

        frame.push({
          data: {
            values: result,
          },
        });
        currentIndex++;
      };

      const pushNextEvent = () => {
        publishNextChunk();
        subscriber.next({
          data: [frame],
          key: streamId,
          // Set the loading state to `Done` when all chunks have been published
          state: currentIndex === numberOfChunks ? LoadingState.Done : LoadingState.Streaming,
        });

        // Schedule the next event if there are more chunks to publish
        if (currentIndex < numberOfChunks) {
          timeoutId = setTimeout(pushNextEvent, Math.ceil(generateRandomNumber(500, 2000)));
        }
      };

      // Simulate async data publishing by using setTimeout
      setTimeout(pushNextEvent, Math.ceil(generateRandomNumber(500, 2000)));

      return () => {
        clearTimeout(timeoutId);
      };
    });

    return stream;
  }

  query(options: DataQueryRequest<MyQuery>): Observable<DataQueryResponse> {
    const { range, targets, intervalMs, maxDataPoints, panelId } = options;
    const from = range!.from.valueOf();
    const to = range!.to.valueOf();
    const streams: Array<Observable<DataQueryResponse>> = [];

    for (let target of targets) {
      streams.push(this.getQueryStream(from, to, target, panelId || 1, intervalMs, maxDataPoints || 100));
    }

    return merge(...streams);
  }

  async request(url: string, params?: string) {
    const response = getBackendSrv().fetch<DataSourceResponse>({
      url: `${this.baseUrl}${url}${params?.length ? `?${params}` : ''}`,
    });
    return lastValueFrom(response);
  }

  /**
   * Checks whether we can connect to the API.
   */
  async testDatasource() {
    const defaultErrorMessage = 'Cannot connect to API';

    try {
      const response = await this.request('/health');
      if (response.status === 200) {
        return {
          status: 'success',
          message: 'Success',
        };
      } else {
        return {
          status: 'error',
          message: response.statusText ? response.statusText : defaultErrorMessage,
        };
      }
    } catch (err) {
      let message = '';
      if (_.isString(err)) {
        message = err;
      } else if (isFetchError(err)) {
        message = 'Fetch error: ' + (err.statusText ? err.statusText : defaultErrorMessage);
        if (err.data && err.data.error && err.data.error.code) {
          message += ': ' + err.data.error.code + '. ' + err.data.error.message;
        }
      }
      return {
        status: 'error',
        message,
      };
    }
  }
}
