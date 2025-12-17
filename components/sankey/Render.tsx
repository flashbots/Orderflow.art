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
import { getDateString } from "@/utils/helpers";

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
      <div className="border border-dune-300 bg-dune-200 px-6 py-6 rounded-md text-center space-y-6">

  {/* Headline */}


  {/* Centered date paragraph */}
  {rangeData?.data?.range && (
    <div className="text-left mx-auto text-sm">
      The data displayed on this site covers the period from{" "}
      <strong>{getDateString(rangeData.data.range.startTime).split(" ")[0]} ({rangeData.data.range.startBlock})</strong>{" "}
      to{" "}
      <strong>{getDateString(rangeData.data.range.endTime).split(" ")[0]} ({rangeData.data.range.endBlock})</strong>{" "}.

        <p className="">
    One of the top solvers in the game continues to publish daily order flow data for the community. Check them out at         <a
          href="https://orderflow.barterswap.xyz"
          target="_blank"
          rel="noopener noreferrer"
          className="text-dune-600 underline hover:text-dune-800"
        >
          orderflow.barterswap.xyz !
        </a>
  </p>
    </div>
  )}


</div>


    </>
  );
}
