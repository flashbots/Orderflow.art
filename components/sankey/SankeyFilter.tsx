import { Dispatch, FC, SetStateAction, useEffect, useRef, useState } from "react";
import {
  Entities,
  EntityName,
  Sankey,
  SelectItem,
  SelectItemEntities,
  getEntitiesResponse,
  getHashesResponse,
} from "@/utils/types";
import PairsStylizedMultiSelect from "../primitives/PairsSelect";
import { SingleValue } from "react-select";
import { changeQueryParam, queryArray } from "@/utils/helpers";
import StylizedTxHashSelect from "../primitives/TxHashSelect";
import useSWR from "swr";
import fetcher from "@/utils/fetcher";
import StylizedSingleSelect from "../primitives/SingleSelect";
import { timeframeList } from "@/utils/constants";
import { useRouter } from "next/router";
import { parse, stringify } from "querystring";
import EntitySelect from "../primitives/EntitySelect";

type Props = {
  entityData: getEntitiesResponse["entities"];
  entityFilter: string;
  graphType: Sankey;
  setGraphType: Dispatch<SetStateAction<Sankey>>;
  isOrderflow: boolean;
  setQueryParam: Dispatch<SetStateAction<string>>;
  txHash: SelectItem[];
  setTxHash: Dispatch<SetStateAction<SelectItem[]>>;
  timeframe: SingleValue<SelectItem>;
  setTimeframe: Dispatch<SetStateAction<SingleValue<SelectItem>>>;
};

const SankeyFilter: FC<Props> = ({
  entityData,
  entityFilter,
  graphType,
  setGraphType,
  isOrderflow,
  setQueryParam,
  txHash,
  setTxHash,
  timeframe,
  setTimeframe,
}) => {
  // Filter states
  const [frontend, setFrontend] = useState<SelectItem[]>([]);
  const [metaAggregator, setMetaAggregator] = useState<SelectItem[]>([]);
  const [solver, setSolver] = useState<SelectItem[]>([]);
  const [mempool, setMempool] = useState<SelectItem[]>([]);
  const [ofa, setOfa] = useState<SelectItem[]>([]);
  const [builder, setBuilder] = useState<SelectItem[]>([]);
  const [pairs, setPairs] = useState<SelectItem[]>([]);
  const [mempoolEnabled, setMempoolEnabled] = useState<boolean>(true);
  const [startTime, setStartTime] = useState<string>("");
  const [endTime, setEndTime] = useState<string>("");
  const [deeplink, setDeeplink] = useState<string>("");
  const router = useRouter();

  const containerRef = useRef<HTMLDivElement>(null);

  const { data: txHashData } = useSWR<getHashesResponse>(
    [
      "/api/get-hashes",
      `?isOrderflow=${isOrderflow}${
        timeframe && timeframe.value !== "7d" ? "&timeframe=" + timeframe.value : ""
      }`,
    ],
    ([url, queryParam]) => fetcher(url, queryParam),
    {
      revalidateOnFocus: false,
    },
  );

  useEffect(() => {
    const query = parse(router.asPath.slice(2));
    if (Object.keys(query).length && deeplink !== stringify(query)) {
      setDeeplink(stringify(query));
      const urlParams = new URLSearchParams();

      if (query.isOrderflow) {
        if (query.isOrderflow === "true") {
          urlParams.append("isOrderflow", query.isOrderflow);
          setGraphType(Sankey.Orderflow);
        } else if (query.isOrderflow === "false") {
          urlParams.append("isOrderflow", query.isOrderflow);
          setGraphType(Sankey.Liquidity);
        }
      }

      const columns: EntityName[] = [
        "frontend",
        "metaAggregator",
        "solver",
        "mempool",
        "ofa",
        "builder",
      ];

      const columnUpdates = {
        frontend: setFrontend,
        metaAggregator: setMetaAggregator,
        solver: setSolver,
        mempool: setMempool,
        ofa: setOfa,
        builder: setBuilder,
      };

      for (const column of columns) {
        const columnEntities = queryArray(query[column]);
        if (columnEntities) {
          const colArr = columnEntities as string[];
          for (const entity of colArr) {
            urlParams.append(column, entity);
          }

          const selectArray = colArr.map((ent) => {
            return { label: ent, value: ent };
          });

          columnUpdates[column](selectArray);
        }
      }

      if (query.timeframe && query.timeframe !== "7d") {
        const timeframe = Array.isArray(query.timeframe) ? query.timeframe[0] : query.timeframe;
        urlParams.append("timeframe", timeframe);
        setTimeframe({ label: timeframe, value: timeframe });
      }

      if (query.txHash) {
        const txHash = queryArray(query.txHash);

        for (const hash of txHash) {
          urlParams.append("txHash", hash);
        }

        const selectArray = txHash.map((hash) => {
          return { label: hash, value: hash };
        });
        setTxHash(selectArray);
      }

      if (query.pairs) {
        const pairs = queryArray(query.pairs);

        for (const pair of pairs) {
          urlParams.append("pairs", pair);
        }

        const selectArray = pairs.map((pair) => {
          return { label: pair, value: pair };
        });
        setPairs(selectArray);
      }

      if (isOrderflow && query.columns && query.columns === "mempool") {
        urlParams.append("columns", "mempool");
        setMempoolEnabled(false);
      } else if (!mempoolEnabled) {
        setMempoolEnabled(true);
      }

      if (query.startTime) {
        urlParams.append("startTime", query.startTime as string);
        setStartTime(query.startTime as string);
      }

      if (query.endTime) {
        urlParams.append("endTime", query.endTime as string);
        setEndTime(query.endTime as string);
      }
      const queryString = `${urlParams.toString()}`;
      setQueryParam("?" + queryString);
    }
  }, [router]);

  useEffect(() => {
    changeQueryParam(
      isOrderflow,
      timeframe,
      txHash,
      pairs,
      mempoolEnabled,
      setMempoolEnabled,
      { frontend, metaAggregator, solver, mempool, ofa, builder },
      setQueryParam,
      router,
      setDeeplink,
      startTime,
      endTime,
    );
  }, [
    frontend,
    metaAggregator,
    solver,
    mempool,
    ofa,
    builder,
    timeframe,
    txHash,
    pairs,
    mempoolEnabled,
  ]);

  useEffect(() => {
    if (entityData) {
      const currentEntities: Entities = {
        frontend: frontend.map((ent) => ent.value),
        metaAggregator: metaAggregator.map((ent) => ent.value),
        solver: solver.map((ent) => ent.value),
        mempool: mempool.map((ent) => ent.value),
        ofa: ofa.map((ent) => ent.value),
        builder: builder.map((ent) => ent.value),
      };

      const newEntities: SelectItemEntities = {
        frontend: [],
        metaAggregator: [],
        solver: [],
        mempool: [],
        ofa: [],
        builder: [],
      };
      const validEntities: Entities = {
        frontend: [],
        metaAggregator: [],
        solver: [],
        mempool: [],
        ofa: [],
        builder: [],
      };

      for (const [column, data] of Object.entries(entityData)) {
        validEntities[column as keyof typeof validEntities] = data.map((ele) => {
          return ele.value;
        });
      }

      for (const column of Object.keys(validEntities)) {
        const columnName = column as keyof typeof validEntities;
        for (const entity of currentEntities[columnName]) {
          if (validEntities[columnName].includes(entity)) {
            newEntities[columnName].push({ label: entity, value: entity });
          }
        }
      }

      setFrontend(newEntities["frontend"]);
      setMetaAggregator(newEntities["metaAggregator"]);
      setSolver(newEntities["solver"]);
      setMempool(newEntities["mempool"]);
      setOfa(newEntities["ofa"]);
      setBuilder(newEntities["builder"]);

      changeQueryParam(
        isOrderflow,
        timeframe,
        txHash,
        pairs,
        mempoolEnabled,
        setMempoolEnabled,
        newEntities,
        setQueryParam,
        router,
        setDeeplink,
        startTime,
        endTime,
      );
    }
  }, [entityData]);

  return (
    <div>
      <div className="flex flex-col items-center border-b border-b-barter-purple">
        <div className="relative flex w-full flex-row">
          {[
            { name: "Retail Trade Volume", value: Sankey.Orderflow },
            { name: "Retail Liquidity Impact", value: Sankey.Liquidity },
          ].map((b, i) => (
            <button
              key={i}
              onClick={() => setGraphType(b.value)}
              disabled={graphType === b.value}
              className={`text-md flex-1 bg-white px-4 py-2.5 font-bold shadow-none hover:bg-barter-isabeline disabled:cursor-not-allowed disabled:rounded-b-none disabled:bg-barter-yellow disabled:text-white disabled:hover:opacity-100 md:text-lg ${
                i == 0 ? "border-r border-r-barter-purple" : ""
              }`}
            >
              {b.name}
            </button>
          ))}
        </div>
      </div>
      <div ref={containerRef}>
        {/* Filters */}
        <div>
          <div className="text-sm">
            <div className="flex flex-col xl:flex-row">
              <div className="box-content flex-1">
                <PairsStylizedMultiSelect
                  isOrderflow={isOrderflow}
                  pairs={pairs}
                  setPairs={setPairs}
                  entityFilter={entityFilter}
                  timeframe={timeframe}
                />
              </div>
            </div>

            <div className="flex flex-col xl:flex-row">
              <div className="flex h-[39px] flex-1">
                <div className="flex h-[39px] items-center border-b border-r border-b-barter-blue border-r-barter-blue bg-barter-isabeline px-4">
                  <span className="text-sm font-bold">{"Tx Hash"}</span>
                </div>
                <div className="flex-1">
                  <StylizedTxHashSelect
                    value={txHash}
                    setValue={setTxHash}
                    options={txHashData?.hashes}
                  />
                </div>
              </div>
              <div className="min-w-[250px] flex-shrink xl:border-l xl:border-l-barter-blue">
                <StylizedSingleSelect
                  title={"Timeframe"}
                  value={timeframe}
                  options={timeframeList}
                  setValue={setTimeframe}
                />
              </div>
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-6">
              <EntitySelect
                title={"Frontend"}
                value={frontend}
                setValue={setFrontend}
                options={entityData?.frontend}
                placeholder={"Filter by frontend"}
                firstChild={true}
              />

              <EntitySelect
                title={"Meta Aggregator"}
                value={metaAggregator}
                setValue={setMetaAggregator}
                options={entityData?.metaAggregator}
                placeholder={"Filter by meta aggregator"}
              />

              <EntitySelect
                title={"Solver"}
                value={solver}
                setValue={setSolver}
                options={entityData?.solver}
                placeholder={"Filter by solver"}
              />

              <EntitySelect
                title={isOrderflow ? "Mempool" : "Aggregator"}
                value={mempool}
                setValue={setMempool}
                options={entityData?.mempool}
                placeholder={`Filter by ${isOrderflow ? "mempool" : "aggregator"}`}
                mempool={true}
                isOrderflow={isOrderflow}
                mempoolEnabled={mempoolEnabled}
                setMempoolEnabled={setMempoolEnabled}
              />

              <EntitySelect
                title={isOrderflow ? "Order Flow Auction" : "Liquidity Source"}
                value={ofa}
                setValue={setOfa}
                options={entityData?.ofa}
                placeholder={`Filter by ${isOrderflow ? "order flow auction" : "liquidity source"}`}
              />

              <EntitySelect
                title={isOrderflow ? "Builder" : "PMM"}
                value={builder}
                setValue={setBuilder}
                options={entityData?.builder}
                placeholder={`Filter by ${isOrderflow ? "builder" : "PMM"}`}
                lastChild={true}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SankeyFilter;
