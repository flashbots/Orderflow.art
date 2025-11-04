import { NextApiRequest, NextApiResponse } from "next";
import { getHashesResponse } from "@/utils/types";
import { Redis } from "ioredis";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<getHashesResponse>,
) {
  const redis = new Redis(process.env.REDIS_URL!);

  try {
    // Aggregated data doesn't contain transaction hashes
    // Return empty array
    return res.status(200).send({
      hashes: [],
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
