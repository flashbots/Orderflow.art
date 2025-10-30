import Select, { FormatOptionLabelMeta, SingleValue, StylesConfig } from "react-select";
import { Dispatch, SetStateAction, useEffect, useRef, useState } from "react";
import { GroupedOption, SelectItem, SeparatedPairs, getFilteredPairsResponse } from "@/utils/types";
import useSWR from "swr";
import fetcher from "@/utils/fetcher";
import { pairGroupLabels } from "@/utils/constants";

export default function PairsStylizedMultiSelect({
  isOrderflow,
  pairs,
  setPairs,
  entityFilter,
  timeframe,
}: {
  isOrderflow: boolean;
  pairs: SelectItem[];
  setPairs: Dispatch<SetStateAction<SelectItem[]>>;
  entityFilter: string;
  timeframe: SingleValue<SelectItem>;
}) {
  const [input, setInput] = useState<string>("");
  const [groupSelected, setGroupSelected] = useState<{
    ethbtcPairs: boolean;
    stableswapPairs: boolean;
    longtailPairs: boolean;
  }>({ ethbtcPairs: false, stableswapPairs: false, longtailPairs: false });
  const [queryParam, setQueryParam] = useState<string>(`?isOrderflow=${isOrderflow}`);
  const [groupedOptions, setGroupedOptions] = useState<
    | {
        label: string;
        options: SelectItem[];
      }[]
    | undefined
  >(undefined);
  const containerRef = useRef<HTMLDivElement>(null);

  const { data } = useSWR<getFilteredPairsResponse>(
    ["/api/get-pairs", queryParam],
    ([url, queryParam]) => fetcher(url, queryParam),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    },
  );

  const formatGroupLabel = (data: GroupedOption) => (
    <div
      className="flex items-center justify-between py-2"
      onClick={() => {
        let newPairs = [];
        const newGroupSelected = groupSelected;
        const label = data.label as keyof typeof pairGroupLabels;
        const name = pairGroupLabels[label];

        if (groupSelected[name]) {
          newPairs = pairs.filter((pair) => {
            if (!data.options.map((item) => item.value).includes(pair.value)) {
              return pair;
            }
          });
          newGroupSelected[name] = false;
        } else {
          newPairs = data.options.filter((pair) => {
            if (!pairs.includes(pair)) {
              return pair;
            }
          });
          newPairs.push(...pairs);
          newGroupSelected[name] = true;
        }

        setGroupSelected(newGroupSelected);
        setPairs(newPairs);
      }}
    >
      <span>{data.label}</span>
      <input
        className="cursor-pointer"
        type="checkbox"
        readOnly
        checked={
          groupSelected[pairGroupLabels[data.label as keyof typeof pairGroupLabels]] ?? undefined
        }
      />
    </div>
  );

  const formatOptionLabel = (data: SelectItem, meta: FormatOptionLabelMeta<SelectItem>) => {
    const metaValues = meta.selectValue.map((pair) => pair.value);

    return (
      <div className="flex items-center justify-between" title={data.label}>
        <span className="overflow-hidden text-ellipsis whitespace-nowrap">{data.label}</span>
        <input type="checkbox" readOnly checked={metaValues.includes(data.value)} />
      </div>
    );
  };

  useEffect(() => {
    const urlParams = new URLSearchParams({
      isOrderflow: isOrderflow.toString(),
    });

    if (entityFilter) {
      urlParams.append("entityFilter", entityFilter);
    }

    if (timeframe && timeframe?.value !== "7d") {
      urlParams.append("timeframe", timeframe.value);
    }

    if (input) {
      urlParams.append("input", input);
    }

    setQueryParam(`?${urlParams.toString()}`);
  }, [isOrderflow, entityFilter, timeframe, input]);

  useEffect(() => {
    if (data?.pairs) {
      const groupedPairs: SeparatedPairs = {
        ethbtcPairs: { selected: [], unSelected: [], allSelected: false },
        stableswapPairs: { selected: [], unSelected: [], allSelected: false },
        longtailPairs: { selected: [], unSelected: [], allSelected: false },
      };

      const selectedValues = pairs.map((pair) => pair.value);

      for (const [group, pairs] of Object.entries(data.pairs)) {
        const groupName = group as keyof SeparatedPairs;
        const pairValues = pairs.map((pair) => pair.value);
        for (const pair of pairValues) {
          if (selectedValues.includes(pair)) {
            groupedPairs[groupName].selected.push({ label: pair, value: pair });
          } else {
            groupedPairs[groupName].unSelected.push({ label: pair, value: pair });
          }
        }

        if (pairs.length === groupedPairs[groupName].selected.length) {
          groupedPairs[groupName].allSelected = true;
        } else {
          groupedPairs[groupName].allSelected = false;
        }
      }

      setGroupedOptions([
        {
          label: "Select all ETH/BTC token pairs",
          options: [...groupedPairs.ethbtcPairs.selected, ...groupedPairs.ethbtcPairs.unSelected],
        },
        {
          label: "Select all stableswap token pairs",
          options: [
            ...groupedPairs.stableswapPairs.selected,
            ...groupedPairs.stableswapPairs.unSelected,
          ],
        },
        {
          label: "Select top 100 long-tail token pairs",
          options: [
            ...groupedPairs.longtailPairs.selected,
            ...groupedPairs.longtailPairs.unSelected,
          ],
        },
      ]);

      setGroupSelected({
        ethbtcPairs: groupedPairs.ethbtcPairs.allSelected,
        stableswapPairs: groupedPairs.stableswapPairs.allSelected,
        longtailPairs: groupedPairs.longtailPairs.allSelected,
      });
    }
  }, [data, pairs]);

  return (
    <div className="flex h-full flex-1 border-b border-b-barter-blue">
      <div
        ref={containerRef}
        className="flex flex-shrink-0 items-center border-r border-r-barter-blue bg-barter-isabeline px-4"
      >
        <span className="text-ellipsis text-sm font-bold">
          {isOrderflow ? "Trade pair" : "Token pair"}
        </span>
      </div>
      <div
        className="flex-1"
        style={{
          maxWidth: containerRef?.current?.offsetWidth
            ? `calc(100% - ${containerRef?.current?.offsetWidth}px)`
            : "100%",
        }}
      >
        <Select<SelectItem, true, GroupedOption>
          value={pairs}
          onChange={(newPairs) => setPairs([...newPairs])}
          options={groupedOptions}
          classNames={{
            container: () => "border-none",
            control: () => "cursor-pointer border-0 rounded-none",
            option: () => "bg-white text-black cursor-pointer hover:bg-dblue-200 active:bg-white",
            group: () => "py-0 [&:first-child>div:first-child]:border-t-0",
            groupHeading: () =>
              "text-base bg-barter-isabeline pt-0 mb-0 normal-case hover:bg-barter-blue/20 border-y border-solid border-barter-blue cursor-pointer",
            menuList: () => "pt-0 rounded",
            multiValue: () => "[&>div>div>input]:hidden",
          }}
          isMulti={true}
          inputValue={input}
          onInputChange={setInput}
          isDisabled={groupedOptions ? false : true}
          hideSelectedOptions={false}
          closeMenuOnSelect={false}
          formatGroupLabel={formatGroupLabel}
          formatOptionLabel={formatOptionLabel}
          placeholder={
            pairs.length
              ? `Filter by ${isOrderflow ? "trade pair" : "token pair"} (${pairs.length})`
              : `Filter by ${isOrderflow ? "trade pair" : "token pair"}`
          }
        />
      </div>
    </div>
  );
}
