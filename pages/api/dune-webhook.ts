import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@clickhouse/client-web";
import axios from "axios";

const DUNE_API_KEY = process.env.DUNE_API_KEY!;
const BATCH_SIZE = 2000;

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

interface DuneWebhookPayload {
  message: string;
  query_result: {
    execution_id: string;
    query_id: number;
    state: string;
    submitted_at: string;
    execution_ended_at: string;
    result_metadata: {
      column_names: string[];
      row_count: number;
      result_set_bytes: number;
      total_result_set_bytes: number;
      datapoint_count: number;
      pending_time_millis: number;
      execution_time_millis: number;
    };
    data_uri: string;
  };
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

async function fetchExecutionResult(dataUri: string): Promise<any[]> {
  console.log(`Fetching from data_uri: ${dataUri}`);

  try {
    const response = await axios.get(dataUri, {
      headers: {
        "X-Dune-API-Key": DUNE_API_KEY,
      },
    });

    if (response.data?.result?.rows) {
      const rows = response.data.result.rows;
      console.log(`Fetched ${rows.length} rows from execution result`);
      return rows;
    }

    return [];
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error("Error fetching execution result:", error.response?.data || error.message);
    }
    throw error;
  }
}

async function uploadToClickHouse(table: string, rows: any[], queryName: string): Promise<void> {
  console.log(`Uploading ${rows.length} rows to ${table}`);

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    await clickhouse.insert({
      table,
      values: batch,
      format: "JSONEachRow",
    });
  }

  console.log(`Upload complete for ${queryName}`);
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
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  console.log(`Webhook received at ${new Date().toISOString()}`);

  const payload = req.body as DuneWebhookPayload;

  // Log the webhook payload
  console.log("Webhook payload:", {
    execution_id: payload.query_result?.execution_id,
    query_id: payload.query_result?.query_id,
    state: payload.query_result?.state,
    row_count: payload.query_result?.result_metadata?.row_count,
  });

  // Verify query completed successfully
  if (payload.query_result?.state !== "QUERY_STATE_COMPLETED") {
    console.log(`Query not completed, state: ${payload.query_result?.state}`);
    return res.status(200).json({
      success: false,
      message: "Query not completed"
    });
  }

  const queryId = payload.query_result.query_id;
  const dataUri = payload.query_result.data_uri;
  const executionId = payload.query_result.execution_id;

  // Determine which query this is (orderflow or liquidity)
  const QUERY_IDS = {
    orderflow: 6090649,
    liquidity: 6090754,
  };

  let queryType: "orderflow" | "liquidity" | null = null;
  let tableName = "";

  if (queryId === QUERY_IDS.orderflow) {
    queryType = "orderflow";
    tableName = "orderflow.prodof";
  } else if (queryId === QUERY_IDS.liquidity) {
    queryType = "liquidity";
    tableName = "orderflow.prodlq";
  } else {
    console.log(`Unknown query_id: ${queryId}`);
    return res.status(200).json({
      success: false,
      message: "Unknown query ID"
    });
  }

  try {
    // Connect to ClickHouse
    await clickhouse.ping();
    console.log("Connected to ClickHouse");

    // Fetch data from Dune execution result
    const rows = await fetchExecutionResult(dataUri);

    if (rows.length === 0) {
      console.log("No rows to upload");
      return res.status(200).json({
        success: true,
        message: "No rows to upload",
        queryType,
        executionId,
      });
    }

    // Transform and upload to ClickHouse
    let transformed: any[];
    if (queryType === "orderflow") {
      transformed = transformOrderflowData(rows);
    } else {
      transformed = transformLiquidityData(rows);
    }

    await uploadToClickHouse(tableName, transformed, queryType);

    // Get final count
    const countResult = await clickhouse.query({
      query: `SELECT COUNT(*) as count FROM ${tableName}`,
      format: "JSONEachRow",
    });
    const countData = (await countResult.json()) as Array<{ count: string }>;

    const result = {
      success: true,
      timestamp: new Date().toISOString(),
      queryType,
      executionId,
      rowsUploaded: rows.length,
      totalRows: countData[0].count,
    };

    console.log("Webhook processing complete:", result);
    return res.status(200).json(result);
  } catch (error) {
    console.error("Webhook processing failed:", error);
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : "Unknown error",
      queryType,
      executionId,
    });
  } finally {
    await clickhouse.close();
  }
}
