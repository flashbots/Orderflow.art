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
<div className="border border-dune-300 bg-dune-200 px-6 py-6 rounded-md text-center space-y-6">

  {/* Headline */}
  <p className="font-semibold text-lg">
    Orderflow.art was a proof of concept.
  </p>

  {/* Centered date paragraph */}
  {rangeData?.data?.range && (
    <div className="text-left max-w-xl mx-auto text-sm">
      The data displayed on this site covers the period from{" "}
      <strong>{getDateString(rangeData.data.range.startTime).split(" ")[0]}</strong>{" "}
      to{" "}
      <strong>{getDateString(rangeData.data.range.endTime).split(" ")[0]}</strong>{" "}
      and is no longer being updated.
    </div>
  )}

  {/* LEFT-ALIGNED block */}
<div className="text-left max-w-xl mx-auto">
  <p className="text-sm mb-4">
    If you are interested in current data, here are the available options:
  </p>

  <ul className="text-sm space-y-4 list-none pl-0">
    <li>
      <span className="font-semibold">Allium</span>
      <div>
        <a
          href="https://dexanalytics.org/metrics/orderflow"
          target="_blank"
          rel="noopener noreferrer"
          className="text-dune-600 underline hover:text-dune-800"
        >
          dexanalytics.org/metrics/orderflow
        </a>
      </div>
      <div className="text-xs text-gray-700">
        Available chains: Ethereum, Arbitrum, Base, Unichain
      </div>
    </li>

    <li>
      <span className="font-semibold">Barter</span>
      <div>
        <a
          href="https://orderflow.barterswap.xyz"
          target="_blank"
          rel="noopener noreferrer"
          className="text-dune-600 underline hover:text-dune-800"
        >
          orderflow.barterswap.xyz
        </a>
      </div>
      <div className="text-xs text-gray-700">
        Available chains: Ethereum
      </div>
    </li>
  </ul>
</div>
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
