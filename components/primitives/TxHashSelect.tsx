import { FormatOptionLabelMeta, SingleValue, StylesConfig } from "react-select";
import Creatable from "react-select/creatable";
import { Dispatch, SetStateAction, useEffect, useState } from "react";
import { SelectItem } from "@/utils/types";

export default function StylizedTxHashSelect({
  value,
  options,
  setValue,
}: {
  value: SelectItem[];
  options: SelectItem[] | undefined;
  setValue: Dispatch<SetStateAction<SelectItem[]>>;
}) {
  const [sortedOptions, setSortedOptions] = useState<SelectItem[]>();

  const formatOptionLabel = (data: SelectItem, meta: FormatOptionLabelMeta<SelectItem>) => {
    const metaValues = meta.selectValue.map((pair) => pair.value);

    return (
      <div className="flex items-center justify-between" title={data.label}>
        <span className="overflow-hidden text-ellipsis whitespace-nowrap">{data.label}</span>
        <input
          className="cursor-pointer"
          type="checkbox"
          readOnly
          checked={metaValues.includes(data.value)}
        />
      </div>
    );
  };

  useEffect(() => {
    if (options) {
      const selectedOptions = [];
      const unselectedOptions = [];

      for (const option of options) {
        if (value.includes(option)) {
          selectedOptions.push(option);
        } else {
          unselectedOptions.push(option);
        }
      }

      for (const selected of value) {
        if (!selectedOptions.includes(selected)) {
          selectedOptions.push(selected);
        }
      }

      setSortedOptions([...selectedOptions, ...unselectedOptions]);
    }
  }, [options, value]);

  return (
    <div className="flex flex-1 border-b border-b-barter-blue">
      {/* Select dropdown */}
      <div className="flex flex-1">
        <Creatable
          value={value}
          onChange={(newValue) => {
            setValue([...newValue]);
          }}
          options={sortedOptions}
          classNames={{
            container: () => "w-full border-none",
            control: () => "border-0 h-full rounded-none cursor-pointer",
            option: () => "bg-white text-black cursor-pointer hover:bg-dblue-200 active:bg-white",
            menuList: () => "rounded",
          }}
          isClearable={true}
          formatCreateLabel={(inputValue) => {
            return <span>{inputValue}</span>;
          }}
          isMulti={true}
          controlShouldRenderValue={false}
          hideSelectedOptions={false}
          closeMenuOnSelect={false}
          placeholder={value.length ? `Filter by tx hash (${value.length})` : `Filter by tx hash`}
          formatOptionLabel={formatOptionLabel}
        />
      </div>
    </div>
  );
}
