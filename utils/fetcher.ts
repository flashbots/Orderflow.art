import axios from "axios";

export default async function fetcher(uri: any, queryParam: any) {
  const server =
    process.env.NODE_ENV || "http://localhost:3000";
  const response = await axios.get(server + uri + queryParam);
  return response.data;
}
