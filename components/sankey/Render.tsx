import { useState } from "react";
import useSWR from "swr";
import fetcher from "@/utils/fetcher";
import {
  Sankey,
  SelectItem,
  getDataRangeResponse,
  getEntitiesResponse,
  getSankeyDataResponse,
} from "@/utils/types";
import SankeyFilter from "./SankeyFilter";
import SankeyGraph from "./SankeyGraph";
import { SingleValue } from "react-select";
import { useWindowSize } from "usehooks-ts";

export default function SankeyRender() {
  const { height: windowHeight } = useWindowSize();
  const [timeframe, setTimeframe] = useState<SingleValue<SelectItem>>({
    label: "7d",
    value: "7d",
  });
  const [txHash, setTxHash] = useState<SelectItem[]>([]);
  const [graphType, setGraphType] = useState<Sankey>(Sankey.Orderflow);
  const isOrderflow: boolean = graphType === Sankey.Orderflow;
  const [queryParam, setQueryParam] = useState<string>(`?isOrderflow=${isOrderflow}`);

  const {
    data: sankeyData,
    error: sankeyError,
    isLoading: sankeyLoading,
  } = useSWR<getSankeyDataResponse>(
    ["/api/get-sankey-data", queryParam],
    ([url, queryParam]) => fetcher(url, queryParam),
    {
      revalidateOnFocus: false,
    },
  );

  const { data: rangeData } = useSWR<getDataRangeResponse>(
    ["/api/get-data-range", `?isOrderflow=${isOrderflow}`],
    ([url, queryParam]) => fetcher(url, queryParam),
    {
      revalidateOnFocus: false,
    },
  );

  const { data: entitiesData } = useSWR<getEntitiesResponse>(
    [
      "/api/get-entities",
      `?isOrderflow=${isOrderflow}${
        timeframe && timeframe.value !== "7d" ? "&timeframe=" + timeframe.value : ""
      }`,
    ],
    ([url, queryParam]) => fetcher(url, queryParam),
    {
      revalidateOnFocus: false,
    },
  );

  return (
    <>
      <div className="border border-b-0 border-dune-300 bg-dune-200 px-4 py-2 text-center">


      </div>
      <div className="border border-dune-300">
        <SankeyFilter
          entityData={entitiesData?.entities}
          entityFilter={sankeyData?.data?.entityFilter ?? ""}
          graphType={graphType}
          setGraphType={setGraphType}
          isOrderflow={isOrderflow}
          setQueryParam={setQueryParam}
          txHash={txHash}
          setTxHash={setTxHash}
          timeframe={timeframe}
          setTimeframe={setTimeframe}
        />
        <SankeyGraph
          height={Math.min(Math.max(windowHeight - 72, 600), 2000)}
          txHash={txHash}
          isLoading={sankeyLoading}
          data={sankeyData?.data}
          rangeData={rangeData?.data}
          error={sankeyError}
        />
      </div>
    </>
  );
}
