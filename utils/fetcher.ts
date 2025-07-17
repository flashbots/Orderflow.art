import axios from "axios";

export default async function fetcher(uri: any, queryParam: any) {
  const response = await axios.get(uri + queryParam);
  return response.data;
}
