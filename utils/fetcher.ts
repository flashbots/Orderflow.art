import axios from "axios";

export default async function fetcher(uri: any, queryParam: any) {
  const url = process.env.SERVER_URL ? process.env.SERVER_URL + uri + queryParam : uri + queryParam;
  const response = await axios.get(url);
  return response.data;
}
