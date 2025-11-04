import { NextApiRequest, NextApiResponse } from "next";
import { getFilteredPairsResponse } from "@/utils/types";
import { Redis } from "ioredis";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<getFilteredPairsResponse>,
) {
  const redis = new Redis(process.env.REDIS_URL!);

  try {
    // Aggregated data doesn't contain trading pair information
    // Return empty arrays for all pair categories
    return res.status(200).send({
      pairs: {
        ethbtcPairs: [],
        stableswapPairs: [],
        longtailPairs: [],
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
