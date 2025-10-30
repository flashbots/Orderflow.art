import Select, { FormatOptionLabelMeta, StylesConfig } from "react-select";
import { Dispatch, SetStateAction, useEffect, useState } from "react";
import { SelectItem } from "@/utils/types";
import StylizedMultiSelect from "./Select";
import StylizedToggle from "./Toggle";

export default function EntitySelect({
  title,
  value,
  setValue,
  options,
  placeholder,
  firstChild = false,
  lastChild = false,
  mempool = false,
  isOrderflow,
  mempoolEnabled,
  setMempoolEnabled,
}: {
  title: string;
  value: SelectItem[];
  setValue: Dispatch<SetStateAction<SelectItem[]>>;
  options: SelectItem[] | undefined;
  placeholder: string;
  firstChild?: boolean;
  lastChild?: boolean;
  mempool?: boolean;
  isOrderflow?: boolean;
  mempoolEnabled?: boolean;
  setMempoolEnabled?: Dispatch<SetStateAction<boolean>>;
}) {
  return (
    <div>
      <div className="flex flex-1 flex-row xl:flex-col">
        {mempool && mempoolEnabled !== undefined && setMempoolEnabled !== undefined ? (
          <div
            className={`flex flex-1 flex-row items-center justify-between border-b border-b-barter-blue bg-barter-isabeline px-4 ${
              !firstChild && "xl:border-l xl:border-l-barter-blue"
            }`}
          >
            <div className="flex flex-1 justify-center py-2">
              <span className={`${isOrderflow ? "pl-[44px]" : ""} text-sm font-bold`}>{title}</span>
            </div>
            {isOrderflow && (
              <div>
                <StylizedToggle checked={mempoolEnabled} setChecked={setMempoolEnabled} />
              </div>
            )}
          </div>
        ) : (
          <div
            className={`flex flex-1 justify-center text-ellipsis border-b border-b-barter-blue bg-barter-isabeline px-4 py-2 ${
              !firstChild && "xl:border-l xl:border-l-barter-blue"
            }`}
          >
            <span className="text-ellipsis whitespace-nowrap text-sm font-bold">{title}</span>
          </div>
        )}
        <div
          className={`flex-1 border-b border-l border-b-barter-blue border-l-barter-blue ${
            firstChild && "xl:border-l-0 xl:border-l-transparent"
          }`}
        >
          <StylizedMultiSelect
            value={value}
            setValue={setValue}
            options={options}
            placeholder={placeholder}
            firstChild={firstChild}
            lastChild={lastChild}
          />
        </div>
      </div>
    </div>
  );
}
