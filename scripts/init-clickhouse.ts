#!/usr/bin/env bun
import { createClient } from "@clickhouse/client-web";

const clickhouse = createClient({
  host: process.env.CLICKHOUSE_HOST!,
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD!,
});

const schemas = {
  prodof: `
    CREATE TABLE IF NOT EXISTS orderflow.prodof (
      block_number UInt64,
      block_time DateTime,
      builder String,
      frontend String,
      hash String,
      mempool String,
      metaaggregator String,
      ofa String,
      solver String,
      trade_pair String,
      trade_usd Float64,
      user String
    ) ENGINE = MergeTree()
    ORDER BY (block_time, block_number)
  `,
  prodlq: `
    CREATE TABLE IF NOT EXISTS orderflow.prodlq (
      aggregator String,
      amount_usd Float64,
      block_number UInt64,
      block_time DateTime,
      frontend String,
      hash String,
      liquidity_src String,
      metaaggregator String,
      pmm String,
      solver String,
      token_pair String,
      trade_usd Float64
    ) ENGINE = MergeTree()
    ORDER BY (block_time, block_number)
  `,
  prodof_aggregated: `
    CREATE TABLE IF NOT EXISTS orderflow.prodof_aggregated (
      frontend String,
      metaaggregator String,
      solver String,
      mempool String,
      ofa String,
      builder String,
      total_volume Float64
    ) ENGINE = SummingMergeTree(total_volume)
    ORDER BY (frontend, metaaggregator, solver, mempool, ofa, builder)
  `,
  prodlq_aggregated: `
    CREATE TABLE IF NOT EXISTS orderflow.prodlq_aggregated (
      frontend String,
      metaaggregator String,
      solver String,
      aggregator String,
      liquidity_src String,
      pmm String,
      total_volume Float64
    ) ENGINE = SummingMergeTree(total_volume)
    ORDER BY (frontend, metaaggregator, solver, aggregator, liquidity_src, pmm)
  `,
};

async function main() {
  console.log("Initializing ClickHouse database and tables...\n");

  try {
    // Test connection
    console.log("Testing connection...");
    await clickhouse.ping();
    console.log("✓ Connected to ClickHouse\n");

    // Create database
    console.log("Creating database 'orderflow' if it doesn't exist...");
    await clickhouse.exec({
      query: "CREATE DATABASE IF NOT EXISTS orderflow",
    });
    console.log("✓ Database ready\n");

    // Create prodof table
    console.log("Creating table 'orderflow.prodof'...");
    await clickhouse.exec({
      query: schemas.prodof,
    });
    console.log("✓ Table 'orderflow.prodof' ready\n");

    // Create prodlq table
    console.log("Creating table 'orderflow.prodlq'...");
    await clickhouse.exec({
      query: schemas.prodlq,
    });
    console.log("✓ Table 'orderflow.prodlq' ready\n");

    // Create prodof_aggregated table
    console.log("Creating table 'orderflow.prodof_aggregated'...");
    await clickhouse.exec({
      query: schemas.prodof_aggregated,
    });
    console.log("✓ Table 'orderflow.prodof_aggregated' ready\n");

    // Create prodlq_aggregated table
    console.log("Creating table 'orderflow.prodlq_aggregated'...");
    await clickhouse.exec({
      query: schemas.prodlq_aggregated,
    });
    console.log("✓ Table 'orderflow.prodlq_aggregated' ready\n");

    // Show table info
    console.log("=== Table Information ===\n");

    const ofDescribe = await clickhouse.query({
      query: "DESCRIBE TABLE orderflow.prodof",
      format: "JSONEachRow",
    });
    const ofCols = (await ofDescribe.json()) as Array<{ name: string; type: string }>;
    console.log("orderflow.prodof columns:");
    ofCols.forEach((col) => {
      console.log(`  - ${col.name}: ${col.type}`);
    });

    console.log("");

    const lqDescribe = await clickhouse.query({
      query: "DESCRIBE TABLE orderflow.prodlq",
      format: "JSONEachRow",
    });
    const lqCols = (await lqDescribe.json()) as Array<{ name: string; type: string }>;
    console.log("orderflow.prodlq columns:");
    lqCols.forEach((col) => {
      console.log(`  - ${col.name}: ${col.type}`);
    });

    console.log("");

    const ofAggDescribe = await clickhouse.query({
      query: "DESCRIBE TABLE orderflow.prodof_aggregated",
      format: "JSONEachRow",
    });
    const ofAggCols = (await ofAggDescribe.json()) as Array<{ name: string; type: string }>;
    console.log("orderflow.prodof_aggregated columns:");
    ofAggCols.forEach((col) => {
      console.log(`  - ${col.name}: ${col.type}`);
    });

    console.log("");

    const lqAggDescribe = await clickhouse.query({
      query: "DESCRIBE TABLE orderflow.prodlq_aggregated",
      format: "JSONEachRow",
    });
    const lqAggCols = (await lqAggDescribe.json()) as Array<{ name: string; type: string }>;
    console.log("orderflow.prodlq_aggregated columns:");
    lqAggCols.forEach((col) => {
      console.log(`  - ${col.name}: ${col.type}`);
    });

    console.log("\n✓ Database initialization complete!");
    process.exit(0);
  } catch (error) {
    console.error("\n✗ Error:", error);
    process.exit(1);
  } finally {
    await clickhouse.close();
  }
}

// Run main function
main();
