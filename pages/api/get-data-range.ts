import { NextApiRequest, NextApiResponse } from "next";
import { tableName } from "@/utils/constants";
import { getDataRangeResponse } from "@/utils/types";
import { client } from "@/utils/clickhouse";
import { Redis } from "ioredis";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<getDataRangeResponse>,
) {
  const redis = new Redis(process.env.REDIS_URL!);

  try {
    // Aggregated data doesn't contain block_time or block_number
    // Return null for range
    return res.status(200).send({
      data: {
        range: null,
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
