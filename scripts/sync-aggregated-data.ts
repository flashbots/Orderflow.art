#!/usr/bin/env bun
import { createClient } from "@clickhouse/client-web";
import axios from "axios";

// Configuration
const DUNE_API_KEY = process.env.DUNE_API_KEY!;
const QUERIES = {
  orderflow_aggregated: "6137978",
  liquidity_aggregated: "6137946",
} as const;

const BATCH_SIZE = 2000; // Insert in batches of 2000 rows
const FETCH_LIMIT = 1000; // Fetch only 1000 rows at a time (smaller to avoid rate limits)
const DELAY_MS = 2000; // Wait 2 seconds between API calls
const MAX_ROWS = parseInt(process.env.MAX_ROWS || "100000"); // Limit total rows fetched

type QueryType = keyof typeof QUERIES;

interface OrderflowAggregatedRow {
  frontend: string;
  metaaggregator: string;
  solver: string;
  mempool: string;
  ofa: string;
  builder: string;
  total_volume: number;
}

interface LiquidityAggregatedRow {
  frontend: string;
  metaaggregator: string;
  solver: string;
  aggregator: string;
  liquidity_src: string;
  pmm: string;
  total_volume: number;
}

// Initialize ClickHouse client
const clickhouse = createClient({
  host: process.env.CLICKHOUSE_HOST!,
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD!,
  database: "orderflow",
});

/**
 * Fetch query results from Dune Analytics with pagination and rate limiting
 */
async function fetchDuneQuery(
  queryId: string,
  queryName: string
): Promise<any[]> {
  console.log(`[${queryName}] Starting fetch (${FETCH_LIMIT} rows per request, ${DELAY_MS}ms delay)`);
  console.log(`[${queryName}] Maximum rows to fetch: ${MAX_ROWS}`);

  let allRows: any[] = [];
  let offset = 0;
  let hasMore = true;
  let requestCount = 0;

  try {
    while (hasMore && allRows.length < MAX_ROWS) {
      requestCount++;
      const startTime = Date.now();

      console.log(`[${queryName}] Request #${requestCount}: Fetching rows ${offset} to ${offset + FETCH_LIMIT}...`);

      const response = await axios.get(
        `https://api.dune.com/api/v1/query/${queryId}/results`,
        {
          headers: {
            "X-Dune-API-Key": DUNE_API_KEY,
          },
          params: {
            limit: FETCH_LIMIT,
            offset,
          },
        }
      );

      if (response.data?.result?.rows) {
        const rows = response.data.result.rows;
        allRows = allRows.concat(rows);

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`[${queryName}] ✓ Fetched ${rows.length} rows in ${elapsed}s (total: ${allRows.length})`);

        // If we got fewer rows than the limit, we've reached the end
        if (rows.length < FETCH_LIMIT) {
          hasMore = false;
          console.log(`[${queryName}] Reached end of data`);
        } else if (allRows.length >= MAX_ROWS) {
          hasMore = false;
          console.log(`[${queryName}] Reached maximum row limit (${MAX_ROWS})`);
        } else {
          offset += FETCH_LIMIT;
          // Delay before next request to avoid rate limiting
          console.log(`[${queryName}] Waiting ${DELAY_MS}ms before next request...`);
          await new Promise(resolve => setTimeout(resolve, DELAY_MS));
        }
      } else {
        hasMore = false;
        console.log(`[${queryName}] No more results`);
      }
    }

    console.log(`[${queryName}] ✓ Completed: ${allRows.length} rows in ${requestCount} requests`);
    return allRows;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`[${queryName}] Error after ${requestCount} requests:`, error.response?.data || error.message);

      // Return what we have so far if we hit rate limits
      if (error.response?.status === 402 && allRows.length > 0) {
        console.log(`[${queryName}] Rate limited, but returning ${allRows.length} rows fetched so far`);
        return allRows;
      }
    } else {
      console.error(`[${queryName}] Error:`, error);
    }
    throw error;
  }
}

/**
 * Upload data to ClickHouse in batches
 */
async function uploadToClickHouse(
  table: string,
  rows: any[],
  queryName: string
): Promise<void> {
  console.log(`[${queryName}] Uploading ${rows.length} rows to ClickHouse table: ${table}`);

  // Upload in batches
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(rows.length / BATCH_SIZE);

    console.log(`[${queryName}] Inserting batch ${batchNum}/${totalBatches} (${batch.length} rows)...`);

    try {
      await clickhouse.insert({
        table,
        values: batch,
        format: "JSONEachRow",
      });

      console.log(`[${queryName}] ✓ Batch ${batchNum}/${totalBatches} inserted`);
    } catch (error) {
      console.error(`[${queryName}] Error inserting batch ${batchNum}:`, error);
      throw error;
    }
  }

  console.log(`[${queryName}] ✓ All ${rows.length} rows uploaded`);
}

/**
 * Transform orderflow aggregated data for ClickHouse
 */
function transformOrderflowAggregatedData(rows: any[]): OrderflowAggregatedRow[] {
  return rows.map((row) => ({
    frontend: row.frontend || "",
    metaaggregator: row.metaaggregator || "",
    solver: row.solver || "",
    mempool: row.mempool || "",
    ofa: row.ofa || "",
    builder: row.builder || "",
    total_volume: row.total_volume || 0,
  }));
}

/**
 * Transform liquidity aggregated data for ClickHouse
 */
function transformLiquidityAggregatedData(rows: any[]): LiquidityAggregatedRow[] {
  return rows.map((row) => ({
    frontend: row.frontend || "",
    metaaggregator: row.metaaggregator || "",
    solver: row.solver || "",
    aggregator: row.aggregator || "",
    liquidity_src: row.liquidity_src || "",
    pmm: row.pmm || "",
    total_volume: row.total_volume || 0,
  }));
}

/**
 * Sync orderflow aggregated data
 */
async function syncOrderflowAggregated(): Promise<void> {
  console.log("\n=== Syncing Orderflow Aggregated Data ===\n");

  const rows = await fetchDuneQuery(QUERIES.orderflow_aggregated, "orderflow_aggregated");
  const transformed = transformOrderflowAggregatedData(rows);
  await uploadToClickHouse("orderflow.prodof_aggregated", transformed, "orderflow_aggregated");
}

/**
 * Sync liquidity aggregated data
 */
async function syncLiquidityAggregated(): Promise<void> {
  console.log("\n=== Syncing Liquidity Aggregated Data ===\n");

  const rows = await fetchDuneQuery(QUERIES.liquidity_aggregated, "liquidity_aggregated");
  const transformed = transformLiquidityAggregatedData(rows);
  await uploadToClickHouse("orderflow.prodlq_aggregated", transformed, "liquidity_aggregated");
}

/**
 * Verify data in ClickHouse
 */
async function verifyData(): Promise<void> {
  console.log("\n=== Verifying Data ===\n");

  try {
    // Check orderflow aggregated table
    const ofResult = await clickhouse.query({
      query: "SELECT COUNT(*) as count, SUM(total_volume) as total FROM orderflow.prodof_aggregated",
      format: "JSONEachRow",
    });
    const ofData = (await ofResult.json()) as Array<{ count: string; total: number }>;
    console.log(`✓ Orderflow aggregated table: ${ofData[0].count} rows, $${ofData[0].total.toLocaleString()} total volume`);

    // Check liquidity aggregated table
    const lqResult = await clickhouse.query({
      query: "SELECT COUNT(*) as count, SUM(total_volume) as total FROM orderflow.prodlq_aggregated",
      format: "JSONEachRow",
    });
    const lqData = (await lqResult.json()) as Array<{ count: string; total: number }>;
    console.log(`✓ Liquidity aggregated table: ${lqData[0].count} rows, $${lqData[0].total.toLocaleString()} total volume`);
  } catch (error) {
    console.error("Error verifying data:", error);
    throw error;
  }
}

/**
 * Main function
 */
async function main(): Promise<void> {
  const startTime = Date.now();
  console.log(`Starting aggregated Dune -> ClickHouse sync at ${new Date().toISOString()}`);
  console.log(`Configuration:`);
  console.log(`  - Fetch limit: ${FETCH_LIMIT} rows per request`);
  console.log(`  - Delay between requests: ${DELAY_MS}ms`);
  console.log(`  - Max rows to fetch: ${MAX_ROWS}`);
  console.log(`  - Batch insert size: ${BATCH_SIZE} rows\n`);

  try {
    // Test ClickHouse connection
    console.log("Testing ClickHouse connection...");
    await clickhouse.ping();
    console.log("✓ Connected to ClickHouse\n");

    // Sync both aggregated datasets
    await syncOrderflowAggregated();
    await syncLiquidityAggregated();

    // Verify the data
    await verifyData();

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`\n✓ Sync completed successfully in ${duration}s`);
    process.exit(0);
  } catch (error) {
    console.error("\n✗ Sync failed:", error);
    process.exit(1);
  } finally {
    await clickhouse.close();
  }
}

// Run main function
main();
