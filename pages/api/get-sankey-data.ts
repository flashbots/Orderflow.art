import { NextApiRequest, NextApiResponse } from "next";
import { entityColumns, sankeyFrontendColors, tableName } from "@/utils/constants";
import { getSankeyDataResponse } from "@/utils/types";
import { client } from "@/utils/clickhouse";
import { getExpirationTimestamp, queryArray } from "@/utils/helpers";
import { Redis } from "ioredis";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<getSankeyDataResponse>,
) {
  const redis = new Redis(process.env.REDIS_URL!);

  try {
    const {
      isOrderflow,
      frontend,
      metaAggregator,
      solver,
      mempool,
      ofa,
      builder,
      columns,
    } = req.query;
    const entitiesArray = [
      queryArray(frontend),
      queryArray(metaAggregator),
      queryArray(solver),
      queryArray(mempool),
      queryArray(ofa),
      queryArray(builder),
    ];
    const columnsArray = queryArray(columns);

    let isOf = false;
    if (isOrderflow === "true") {
      isOf = true;
    }

    const table = isOf ? tableName.orderflow : tableName.liquidity;

    let entColumns = isOf ? entityColumns.orderflow : entityColumns.liquidity;

    const entities: Record<string, string[]> = {};

    for (let i = 0; i < entColumns.length; i++) {
      entities[entColumns[i]] = entitiesArray[i];
    }

    // Filter out removed columns
    if (columnsArray.length > 0) {
      entColumns = entColumns.filter(function (col) {
        return !columnsArray.includes(col);
      });
    }

    let filter = "";
    let entityFilter = "";

    // Create filter string for entities
    if (Object.values(entities).flat(1).length > 0) {
      const filterStrings: string[] = [];

      for (const [entityType, ents] of Object.entries(entities)) {
        if (ents.length > 0) {
          let entityTypeFilter = "(";
          for (let i = 0; i < ents.length; i++) {
            entityTypeFilter += `${entityType} = '${ents[i]}'`;

            if (i !== ents.length - 1) {
              entityTypeFilter += " OR ";
            }
          }
          entityTypeFilter += ")";

          filterStrings.push(entityTypeFilter);
        }
      }

      if (filterStrings.length) {
        entityFilter = "(";

        for (let i = 0; i < filterStrings.length; i++) {
          if (i !== 0) {
            entityFilter += ` AND `;
          }
          entityFilter += filterStrings[i];
        }
        entityFilter += ")";
      }
    }

    if (entityFilter) {
      filter += ` AND ${entityFilter}`;
    }

    let baseQueries: string[][] = [];
    let labelQueries: Record<string, string> = {};

    for (let source = 0; source < entColumns.length; source++) {
      const labelQueryString = `
      SELECT
        DISTINCT ${entColumns[source]}
      FROM ${table}
      WHERE ${entColumns[source]} != ''
      AND
          total_volume != 0
      ${filter}`.replace(/\s+/g, " ");

      labelQueries[entColumns[source]] = labelQueryString;
      for (let target = source + 1; target < entColumns.length; target++) {
        let extraFilter = "";
        if (target > source + 1) {
          for (let i = source + 1; i < target; i++) {
            extraFilter += ` AND ${entColumns[i]} = ''`;
          }
        }

        let queryString = `
          SELECT
            ${entColumns[source]} as source,
            ${entColumns[target]} as target,
            SUM(total_volume) as value
          FROM
            ${table}
          WHERE
            ${entColumns[source]} != ''
          AND
            ${entColumns[target]} != ''
          AND
            total_volume != 0
          ${extraFilter}
          ${filter}
          GROUP BY
            source,
            target
        `;

        baseQueries.push([
          entColumns[source],
          entColumns[target],
          queryString.replace(/\s+/g, " "),
        ]);
      }
    }

    const labels: Record<string, string[]> = {};
    const labelsArray: string[] = [];
    const expirationTimestamp = getExpirationTimestamp();

    const sendLabels = async (column: string, query: string) => {
      try {
        const labelsCache: string | null = await redis.get("sql:" + query);

        if (labelsCache !== null) {
          labels[column] = JSON.parse(labelsCache);
          labelsArray.push(...JSON.parse(labelsCache));
        } else {
          const findLabels = await client.query({
            query: query,
            format: "JSONCompactEachRow",
          });

          const json: string[][] = await findLabels.json();
          const parsedJson = json.flat(1);

          await redis.set("sql:" + query, JSON.stringify(parsedJson), "EXAT", expirationTimestamp);

          labels[column] = parsedJson;
          labelsArray.push(...parsedJson);
        }
      } catch (err) {
        console.log(err);
        setTimeout(() => {
          sendLabels(column, query);
        }, 100);
      }
    };

    const getLabels = async () => {
      const requests: Promise<void>[] = [];

      for (const [column, query] of Object.entries(labelQueries)) {
        requests.push(sendLabels(column, query));
      }

      await Promise.allSettled(requests);
      return;
    };

    await getLabels();

    // Label indicies
    const indicies: Record<string, Record<string, number>> = {};
    let index = 0;
    for (const [column, values] of Object.entries(labels)) {
      indicies[column] = {};
      for (const value of values) {
        indicies[column][value] = index;
        index++;
      }
    }

    let source: number[] = [],
      target: number[] = [],
      value: number[] = [];

    const sendData = async (sourceColumn: string, targetColumn: string, query: string) => {
      try {
        let dataArray: Record<string, any>[] = [];
        const dataCache: string | null = await redis.get("sql:" + query);

        if (dataCache !== null) {
          dataArray = JSON.parse(dataCache);
        } else {
          const data = await client.query({
            query: query,
            format: "JSONEachRow",
          });
          const dataJson: Record<string, any>[] = await data.json();

          await redis.set("sql:" + query, JSON.stringify(dataJson), "EXAT", expirationTimestamp);

          dataArray = dataJson;
        }

        for (const record of dataArray) {
          source.push(indicies[sourceColumn][record.source]);
          target.push(indicies[targetColumn][record.target]);
          value.push(record.value);
        }
      } catch (err) {
        console.log(err);
        setTimeout(() => {
          sendData(sourceColumn, targetColumn, query);
        }, 100);
      }
    };

    const getRequests = async () => {
      const requests: Promise<void>[] = [];

      for (const [sourceColumn, targetColumn, query] of baseQueries) {
        requests.push(sendData(sourceColumn, targetColumn, query));
      }

      await Promise.allSettled(requests);
      return;
    };

    await getRequests();

    // Group into Top 10 + Other for each entity column
    const TOP_N = 20;

    // Calculate total volume for each label in each column
    const volumeByColumn: Record<string, Record<string, number>> = {};
    for (const column of entColumns) {
      volumeByColumn[column] = {};
    }

    // Sum up volumes for each label
    for (let i = 0; i < source.length; i++) {
      const sourceLabel = labelsArray[source[i]];
      const targetLabel = labelsArray[target[i]];
      const vol = value[i];

      // Find which column each label belongs to
      for (const [column, columnLabels] of Object.entries(labels)) {
        if (columnLabels.includes(sourceLabel)) {
          volumeByColumn[column][sourceLabel] = (volumeByColumn[column][sourceLabel] || 0) + vol;
        }
        if (columnLabels.includes(targetLabel)) {
          volumeByColumn[column][targetLabel] = (volumeByColumn[column][targetLabel] || 0) + vol;
        }
      }
    }

    // Determine top N for each column
    const topLabels: Record<string, Set<string>> = {};
    for (const [column, volumes] of Object.entries(volumeByColumn)) {
      const sorted = Object.entries(volumes)
        .sort((a, b) => b[1] - a[1])
        .slice(0, TOP_N)
        .map(([label]) => label);
      topLabels[column] = new Set(sorted);
    }

    // Create mapping from old label to new label (top N or "Other")
    const labelMapping: Record<string, string> = {};
    for (const [column, columnLabels] of Object.entries(labels)) {
      for (const label of columnLabels) {
        if (topLabels[column].has(label)) {
          labelMapping[label] = label;
        } else {
          labelMapping[label] = `Other (${column})`;
        }
      }
    }

    // Rebuild labels structure with grouped data
    const newLabels: Record<string, string[]> = {};
    const newLabelsArray: string[] = [];

    for (const [column, columnLabels] of Object.entries(labels)) {
      const topN = Array.from(topLabels[column]);
      const hasOther = columnLabels.length > TOP_N;

      newLabels[column] = hasOther ? [...topN, `Other (${column})`] : topN;
      newLabelsArray.push(...newLabels[column]);
    }

    // Create new indices
    const newIndicies: Record<string, Record<string, number>> = {};
    let newIndex = 0;
    for (const [column, columnLabels] of Object.entries(newLabels)) {
      newIndicies[column] = {};
      for (const label of columnLabels) {
        newIndicies[column][label] = newIndex;
        newIndex++;
      }
    }

    // Aggregate links using the new grouping
    const linkMap = new Map<string, number>();

    for (let i = 0; i < source.length; i++) {
      const sourceLabel = labelsArray[source[i]];
      const targetLabel = labelsArray[target[i]];
      const vol = value[i];

      const newSourceLabel = labelMapping[sourceLabel];
      const newTargetLabel = labelMapping[targetLabel];

      // Find which columns these belong to
      let sourceColumn = "";
      let targetColumn = "";
      for (const [column, columnLabels] of Object.entries(labels)) {
        if (columnLabels.includes(sourceLabel)) sourceColumn = column;
        if (columnLabels.includes(targetLabel)) targetColumn = column;
      }

      const newSourceIdx = newIndicies[sourceColumn][newSourceLabel];
      const newTargetIdx = newIndicies[targetColumn][newTargetLabel];

      const key = `${newSourceIdx}-${newTargetIdx}`;
      linkMap.set(key, (linkMap.get(key) || 0) + vol);
    }

    // Convert aggregated links back to arrays
    const newSource: number[] = [];
    const newTarget: number[] = [];
    const newValue: number[] = [];

    for (const [key, vol] of linkMap.entries()) {
      const [src, tgt] = key.split("-").map(Number);
      newSource.push(src);
      newTarget.push(tgt);
      newValue.push(vol);
    }

    const colors = [];

    const randomHexColor = () => {
      return "#" + Math.floor(Math.random() * 16777215).toString(16);
    };

    for (const label of newLabelsArray) {
      // Use gray color for "Other" groups
      if (label.startsWith("Other (")) {
        colors.push("#999999");
      } else {
        colors.push(sankeyFrontendColors[label] ? sankeyFrontendColors[label] : randomHexColor());
      }
    }

    return res.status(200).send({
      data: {
        entityFilter,
        links: { source: newSource, target: newTarget, value: newValue },
        labels: newLabelsArray,
        colors,
        range: null, // No time range data in aggregated tables
      },
    });
  } catch (error) {
    let message = "Unknown Error Occurred";
    if (error instanceof Error) message = error.message;
    console.log(message);
    return res.status(400).send({ error: message });
  } finally {
    redis.disconnect();
  }
}
