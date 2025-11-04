import { getDateString } from "@/utils/helpers";
import { getDataRangeResponse } from "@/utils/types";

export default function DataRange({
  rangeData,
  isPortrait,
}: {
  rangeData: getDataRangeResponse["data"];
  isPortrait: boolean;
}) {
  const toFormattedBlockNumber = (blockNumber: string) =>
    Number(blockNumber).toLocaleString("us-en", { maximumFractionDigits: 0 });

  // Handle aggregated data (no time range available)
  if (!rangeData || !rangeData.range) {
    return (
      <div className={`flex flex-col p-1 ${isPortrait ? "text-center" : "items-end"}`}>
        <span className={isPortrait ? "break-word text-[10px]" : "text-xs"}>
          Aggregated data (all time)
        </span>
      </div>
    );
  }

  return (
    <div className={`flex flex-col p-1 ${isPortrait ? "text-center" : "items-end"}`}>
      <span className={isPortrait ? "break-word text-[10px]" : "text-xs"}>
        {`First tracked block: ${toFormattedBlockNumber(
          rangeData.range.startBlock,
        )} (${getDateString(rangeData.range.startTime)})`}
      </span>
      <span className={isPortrait ? "break-word text-[10px]" : "text-xs"}>
        {`Last known block: ${toFormattedBlockNumber(rangeData.range.endBlock)} (${getDateString(
          rangeData.range.endTime,
        )})`}
      </span>
    </div>
  );
}
