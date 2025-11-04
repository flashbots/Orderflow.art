import { NextApiRequest, NextApiResponse } from "next";
import { SelectItem, getEntitiesResponse } from "@/utils/types";
import { client } from "@/utils/clickhouse";
import { getExpirationTimestamp } from "@/utils/helpers";
import { entityColumns, tableName } from "@/utils/constants";
import { Redis } from "ioredis";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<getEntitiesResponse>,
) {
  const redis = new Redis(process.env.REDIS_URL!);

  try {
    const { isOrderflow, timeframe } = req.query;

    let isOf = false;

    if (isOrderflow === "true") {
      isOf = true;
    }

    const table = isOf ? tableName.orderflow : tableName.liquidity;
    const entColumns = isOf ? entityColumns.orderflow : entityColumns.liquidity;
    const entColumnNames = ["frontend", "metaAggregator", "solver", "mempool", "ofa", "builder"];

    let entities: Record<string, SelectItem[]> = {};

    for (let i = 0; i < entColumns.length; i++) {
      const baseQuery = `
      SELECT
        ${entColumns[i]},
        SUM(total_volume) as total_trade_usd
      FROM
        ${table}
      WHERE total_volume != 0
      GROUP BY
        ${entColumns[i]}
      ORDER BY
        total_trade_usd DESC`.replace(/\s+/g, " ");

      const entityCache: string | null = await redis.get("sql:" + baseQuery);

      if (entityCache !== null) {
        entities[entColumnNames[i]] = JSON.parse(entityCache);
      } else {
        const data = await client.query({
          query: baseQuery,
          format: "JSONCompactEachRow",
        });

        const json: Array<[string, number]> = await data.json();
        const parsedEntityJson = json
          .filter((item) => item[0] !== "")
          .map((item) => ({
            label: item[0],
            value: item[0],
          }));
        entities[entColumnNames[i]] = parsedEntityJson;

        await redis.set(
          "sql:" + baseQuery,
          JSON.stringify(parsedEntityJson),
          "EXAT",
          getExpirationTimestamp(),
        );
      }
    }

    return res.status(200).send({
      entities: entities as getEntitiesResponse["entities"],
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
