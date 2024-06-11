import { closestIdx } from '@grafana/data';

export function generateTimeSeriesData(
  startEpoch: number,
  endEpoch: number,
  interval: number,
  config: TimeSeriesConfig = {}
): TimeSeriesData[] {
  if (startEpoch >= endEpoch) {
    throw new Error('Start time must be less than end time.');
  }
  if (interval <= 0) {
    throw new Error('Interval must be greater than zero.');
  }

  const amplitude = config.amplitude ?? 10; // Example amplitude of temperature variation
  const mean = config.mean ?? 20; // Example mean temperature
  const frequency = config.frequency ?? 1; // Frequency of the sine wave

  const timeSeriesData: TimeSeriesData[] = [];

  for (let timestamp = startEpoch; timestamp <= endEpoch; timestamp += interval) {
    const timeInDay = (timestamp % 86400) / 86400; // Time of the day in fraction (0 to 1)
    const value = mean + amplitude * Math.sin(2 * Math.PI * frequency * timeInDay - Math.PI / 2);

    timeSeriesData.push([timestamp, value]);
  }

  return timeSeriesData;
}

export function divideIntoChunks<T>(array: T[], numberOfChunks: number): T[][] {
  const chunkSize = Math.ceil(array.length / numberOfChunks);
  const chunks: T[][] = [];

  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }

  return chunks;
}

type TimeSeriesConfig = {
  amplitude?: number;
  mean?: number;
  frequency?: number;
};
type TimeSeriesData = [number, number];

export function shuffle<T>(array: T[]): T[] {
  let currentIndex = array.length,
    temporaryValue,
    randomIndex;

  // While there remain elements to shuffle...
  while (0 !== currentIndex) {
    // Pick a remaining element...
    randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex -= 1;

    // And swap it with the current element.
    temporaryValue = array[currentIndex];
    array[currentIndex] = array[randomIndex];
    array[randomIndex] = temporaryValue;
  }

  return array;
}

export function generateRandomNumber(min: number, max: number): number {
  return Math.random() * (max - min) + min;
}

export function generateRandomTimeSeriesConfig(): TimeSeriesConfig {
  const amplitude = generateRandomNumber(5, 20); // Random amplitude between 5 and 20
  const mean = generateRandomNumber(15, 30); // Random mean between 15 and 30
  const frequency = generateRandomNumber(0.1, 2); // Random frequency between 0.1 and 2

  return { amplitude, mean, frequency };
}

export type Table = [times: number[], ...values: any[][]];

// prevTable and nextTable are assumed sorted ASC on reference [0] arrays
// nextTable is assumed to be contiguous, only edges are checked for overlap
// ...so prev: [1,2,5] + next: [3,4,6] -> [1,2,3,4,6]
export function amendTable(prevTable: Table, nextTable: Table): Table {
  //   console.log('prevTable', prevTable);
  //   console.log('nextTable', nextTable);
  let [prevTimes] = prevTable;
  let [nextTimes] = nextTable;

  let pLen = prevTimes.length;
  let pStart = prevTimes[0];
  let pEnd = prevTimes[pLen - 1];

  let nLen = nextTimes.length;
  let nStart = nextTimes[0];
  let nEnd = nextTimes[nLen - 1];

  let outTable: Table;

  if (pLen) {
    if (nLen) {
      // append, no overlap
      if (nStart > pEnd) {
        outTable = prevTable.map((_, i) => prevTable[i].concat(nextTable[i])) as Table;
      }
      // prepend, no overlap
      else if (nEnd < pStart) {
        outTable = nextTable.map((_, i) => nextTable[i].concat(prevTable[i])) as Table;
      }
      // full replace
      else if (nStart <= pStart && nEnd >= pEnd) {
        outTable = nextTable;
      }
      // partial replace
      else if (nStart > pStart && nEnd < pEnd) {
        // partial replace
        let startIdx = closestIdx(nStart, prevTimes);
        startIdx = prevTimes[startIdx] < nStart ? startIdx + 1 : startIdx;
        let endIdx = closestIdx(nEnd, prevTimes);
        endIdx = prevTimes[endIdx] > nEnd ? endIdx - 1 : endIdx;

        outTable = prevTable.map((_, i) =>
          prevTable[i]
            .slice(0, startIdx)
            .concat(nextTable[i])
            .concat(prevTable[i].slice(endIdx + 1))
        ) as Table;
      }
      // append, with overlap
      else if (nStart >= pStart) {
        let idx = closestIdx(nStart, prevTimes);
        idx = prevTimes[idx] < nStart ? idx - 1 : idx;
        outTable = prevTable.map((_, i) => prevTable[i].slice(0, idx).concat(nextTable[i])) as Table;
      }
      // prepend, with overlap
      else if (nEnd >= pStart) {
        let idx = closestIdx(nEnd, prevTimes);
        idx = prevTimes[idx] < nEnd ? idx : idx + 1;
        outTable = nextTable.map((_, i) => nextTable[i].concat(prevTable[i].slice(idx))) as Table;
      }
    } else {
      outTable = prevTable;
    }
  } else {
    if (nLen) {
      outTable = nextTable;
    } else {
      outTable = [[]];
    }
  }

  return outTable!;
}

export function trimTable(table: Table, fromTime: number, toTime: number): Table {
  let [times, ...vals] = table;
  let fromIdx: number | undefined;
  let toIdx: number | undefined;

  // trim to bounds
  if (times[0] < fromTime) {
    fromIdx = closestIdx(fromTime, times);

    if (times[fromIdx] < fromTime) {
      fromIdx++;
    }
  }

  if (times[times.length - 1] > toTime) {
    toIdx = closestIdx(toTime, times);

    if (times[toIdx] > toTime) {
      toIdx--;
    }
  }

  if (fromIdx != null || toIdx != null) {
    times = times.slice(fromIdx ?? 0, toIdx);
    vals = vals.map((vals2) => vals2.slice(fromIdx ?? 0, toIdx));
  }

  return [times, ...vals];
}
