import { FC } from "react";
import { SelectItem, getDataRangeResponse, getSankeyDataResponse } from "@/utils/types";
import Loader from "@/components/primitives/Loader";
import dynamic from "next/dynamic";
import { useMediaQuery } from "usehooks-ts";
import DataRange from "../primitives/DataRange";
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

type Props = {
  height: number;
  txHash: SelectItem[];
  isLoading: boolean;
  data?: getSankeyDataResponse["data"];
  rangeData?: getDataRangeResponse["data"];
  error?: getSankeyDataResponse["error"];
};

const SankeyGraph: FC<Props> = ({ height, txHash, isLoading, data, rangeData, error }) => {
  const isSmallDevice = useMediaQuery("(max-width: 1000px)");
  const isPortrait = useMediaQuery("(max-width: 600px)");

  if (error || (data && data.links.source.length === 0)) {
    return (
      <div className="flex h-full w-full flex-col items-center justify-center">
        <div className={`flex items-center justify-center`} style={{ height: `${height}px` }}>
          <h1 className="text-xl font-semibold">No data found for selected filters</h1>
        </div>
        <div className="flex w-full flex-row justify-end bg-barter-isabeline p-1 font-mono">
          <DataRange rangeData={rangeData} isPortrait={isPortrait} />
        </div>
      </div>
    );
  } else if (isPortrait) {
    return (
      <div className="flex h-full w-full flex-col items-center justify-center">
        {data && !isLoading ? (
          <div className="flex w-full items-center overflow-x-scroll" style={{ height: "90vh" }}>
            <Plot
              style={{ height: "100%" }}
              data={[
                {
                  type: "sankey",
                  orientation: "h",
                  valueformat: "$,.8r",
                  arrangement: "perpendicular",
                  node: {
                    pad: 8,
                    thickness: 10,
                    label: data.labels,
                    color: data.colors,
                  },
                  link: data.links,
                },
              ]}
              layout={{
                width: 1200,
                margin: {
                  t: 25,
                  b: 25,
                  l: 25,
                  r: 25,
                },
                font: {
                  size: 10,
                },
              }}
              config={{ displaylogo: false, displayModeBar: false }}
            />
          </div>
        ) : (
          <div
            className="flex w-full items-center justify-center overflow-x-scroll"
            style={{ height: "90vh" }}
          >
            <Loader title="Loading Sankey Diagram..." />
          </div>
        )}
        <div className="flex w-full flex-row justify-end bg-barter-isabeline p-1 font-mono">
          <DataRange rangeData={rangeData} isPortrait={isPortrait} />
        </div>
      </div>
    );
  } else if (isSmallDevice) {
    return (
      <div className={`flex h-[90vh] min-h-[600px] w-full flex-col items-center justify-center`}>
        {data && !isLoading ? (
          <div className="flex h-full w-full items-center overflow-x-scroll">
            <Plot
              className="h-full"
              data={[
                {
                  type: "sankey",
                  orientation: "h",
                  valueformat: "$,.8r",
                  arrangement: "perpendicular",
                  node: {
                    pad: 8,
                    thickness: 10,
                    label: data.labels,
                    color: data.colors,
                  },
                  link: data.links,
                },
              ]}
              layout={{
                width: 1200,
                margin: {
                  t: 25,
                  b: 25,
                  l: 25,
                  r: 25,
                },
                font: {
                  size: 9,
                },
              }}
              config={{
                displaylogo: false,
                displayModeBar: false,
              }}
            />
          </div>
        ) : (
          <div className={`flex h-full w-full items-center justify-center overflow-x-scroll`}>
            <Loader title="Loading Sankey Diagram..." />
          </div>
        )}
        <div className="flex w-full flex-row justify-end bg-barter-isabeline p-1 font-mono">
          <DataRange rangeData={rangeData} isPortrait={isPortrait} />
        </div>
      </div>
    );
  } else {
    return (
      <div
        className={`flex w-full flex-col items-center justify-center`}
        style={{ height: `${height}px` }}
      >
        {data && !isLoading ? (
          <div className={`flex w-full items-center`} style={{ height: `${height - 56}px` }}>
            <Plot
              style={{
                height: `${txHash.length === 1 ? "50%" : "100%"}`,
                width: "100%",
              }}
              data={[
                {
                  type: "sankey",
                  orientation: "h",
                  valueformat: "$,.8r",
                  arrangement: "perpendicular",
                  node: {
                    pad: 12,
                    thickness: 30,
                    label: data.labels,
                    color: data.colors,
                  },
                  link: data.links,
                },
              ]}
              layout={{
                autosize: true,
                margin: {
                  t: 20,
                  b: 20,
                  l: 20,
                  r: 20,
                },
                font: {
                  size: data.labels.length > 50 ? 12 : 16,
                },
              }}
              config={{ displaylogo: false }}
              useResizeHandler
              onRelayout={(event) => {
                console.log(event);
              }}
            />
          </div>
        ) : (
          <div
            className={`flex items-center justify-center`}
            style={{ height: `${height - 56}px` }}
          >
            <Loader title="Loading Sankey Diagram..." />
          </div>
        )}
        <div className="mt-4 flex w-full flex-row justify-end border-t border-t-barter-blue bg-barter-isabeline p-1 font-mono">
          <DataRange rangeData={rangeData} isPortrait={isPortrait} />
        </div>
      </div>
    );
  }
};

export default SankeyGraph;
