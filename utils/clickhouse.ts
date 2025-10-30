import { createClient } from "@clickhouse/client-web";

export const client = createClient({
  host: process.env.CLICKHOUSE_HOST ?? "http://localhost:8123",
  username: process.env.CLICKHOUSE_USER ?? "default",
  password: process.env.CLICKHOUSE_PASSWORD ?? "",
  database: "orderflow",
});
