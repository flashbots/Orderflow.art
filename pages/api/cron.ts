import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@clickhouse/client-web";
import axios from "axios";

const DUNE_API_KEY = process.env.DUNE_API_KEY!;
const QUERIES = {
  orderflow: "6090649",
  liquidity: "6090754",
} as const;

const BATCH_SIZE = 2000;
const FETCH_LIMIT = 1000;
const DELAY_MS = 2000;
const MAX_ROWS = parseInt(process.env.MAX_ROWS || "50000");

interface OrderflowRow {
  block_number: number;
  block_time: string;
  builder: string;
  frontend: string;
  hash: string;
  mempool: string;
  metaaggregator: string;
  ofa: string;
  solver: string;
  trade_pair: string;
  trade_usd: number;
  user: string;
}

interface LiquidityRow {
  aggregator: string;
  amount_usd: number;
  block_number: number;
  block_time: string;
  frontend: string;
  hash: string;
  liquidity_src: string;
  metaaggregator: string;
  pmm: string;
  solver: string;
  token_pair: string;
  trade_usd: number;
}

const clickhouse = createClient({
  host: process.env.CLICKHOUSE_HOST!,
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD!,
  database: "orderflow",
});

function parseDate(dateStr: string): string {
  return dateStr.replace(".000 UTC", "");
}

async function fetchDuneQuery(queryId: string, queryName: string): Promise<any[]> {
  console.log(`[${queryName}] Starting fetch`);

  let allRows: any[] = [];
  let offset = 0;
  let hasMore = true;
  let requestCount = 0;

  try {
    while (hasMore && allRows.length < MAX_ROWS) {
      requestCount++;

      const response = await axios.get(
        `https://api.dune.com/api/v1/query/${queryId}/results`,
        {
          headers: { "X-Dune-API-Key": DUNE_API_KEY },
          params: { limit: FETCH_LIMIT, offset },
        }
      );

      if (response.data?.result?.rows) {
        const rows = response.data.result.rows;
        allRows = allRows.concat(rows);
        console.log(`[${queryName}] Fetched ${rows.length} rows (total: ${allRows.length})`);

        if (rows.length < FETCH_LIMIT || allRows.length >= MAX_ROWS) {
          hasMore = false;
        } else {
          offset += FETCH_LIMIT;
          await new Promise(resolve => setTimeout(resolve, DELAY_MS));
        }
      } else {
        hasMore = false;
      }
    }

    console.log(`[${queryName}] Completed: ${allRows.length} rows in ${requestCount} requests`);
    return allRows;
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 402 && allRows.length > 0) {
      console.log(`[${queryName}] Rate limited, returning ${allRows.length} rows`);
      return allRows;
    }
    throw error;
  }
}

async function uploadToClickHouse(table: string, rows: any[], queryName: string): Promise<void> {
  console.log(`[${queryName}] Uploading ${rows.length} rows to ${table}`);

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    await clickhouse.insert({
      table,
      values: batch,
      format: "JSONEachRow",
    });
  }

  console.log(`[${queryName}] Upload complete`);
}

function transformOrderflowData(rows: any[]): OrderflowRow[] {
  return rows.map((row) => ({
    block_number: row.block_number || 0,
    block_time: parseDate(row.block_time || ""),
    builder: row.builder || "",
    frontend: row.frontend || "",
    hash: row.hash || "",
    mempool: row.mempool || "",
    metaaggregator: row.metaaggregator || "",
    ofa: row.ofa || "",
    solver: row.solver || "",
    trade_pair: row.trade_pair || "",
    trade_usd: row.trade_usd || 0,
    user: row.user || "",
  }));
}

function transformLiquidityData(rows: any[]): LiquidityRow[] {
  return rows.map((row) => ({
    aggregator: row.aggregator || "",
    amount_usd: row.amount_usd || 0,
    block_number: row.block_number || 0,
    block_time: parseDate(row.block_time || ""),
    frontend: row.frontend || "",
    hash: row.hash || "",
    liquidity_src: row.liquidity_src || "",
    metaaggregator: row.metaaggregator || "",
    pmm: row.pmm || "",
    solver: row.solver || "",
    token_pair: row.token_pair || "",
    trade_usd: row.trade_usd || 0,
  }));
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  // Check authorization
  const authHeader = req.headers.authorization;
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    console.error("Unauthorized cron request");
    return res.status(401).json({ error: "Unauthorized" });
  }

  console.log(`Starting cron job at ${new Date().toISOString()}`);

  try {
    // Test ClickHouse connection
    await clickhouse.ping();
    console.log("Connected to ClickHouse");

    // Sync orderflow data
    const ofRows = await fetchDuneQuery(QUERIES.orderflow, "orderflow");
    if (ofRows.length > 0) {
      const transformed = transformOrderflowData(ofRows);
      await uploadToClickHouse("orderflow.prodof", transformed, "orderflow");
    }

    // Sync liquidity data
    const lqRows = await fetchDuneQuery(QUERIES.liquidity, "liquidity");
    if (lqRows.length > 0) {
      const transformed = transformLiquidityData(lqRows);
      await uploadToClickHouse("orderflow.prodlq", transformed, "liquidity");
    }

    // Get final counts
    const ofResult = await clickhouse.query({
      query: "SELECT COUNT(*) as count FROM orderflow.prodof",
      format: "JSONEachRow",
    });
    const ofData = (await ofResult.json()) as Array<{ count: string }>;

    const lqResult = await clickhouse.query({
      query: "SELECT COUNT(*) as count FROM orderflow.prodlq",
      format: "JSONEachRow",
    });
    const lqData = (await lqResult.json()) as Array<{ count: string }>;

    const result = {
      success: true,
      timestamp: new Date().toISOString(),
      orderflow: {
        fetched: ofRows.length,
        total: ofData[0].count,
      },
      liquidity: {
        fetched: lqRows.length,
        total: lqData[0].count,
      },
    };

    console.log("Cron job completed:", result);
    return res.status(200).json(result);
  } catch (error) {
    console.error("Cron job failed:", error);
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : "Unknown error",
    });
  } finally {
    await clickhouse.close();
  }
}
