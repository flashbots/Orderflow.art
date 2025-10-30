import Select, { SingleValue } from "react-select";
import { Dispatch, SetStateAction } from "react";
import { SelectItem } from "@/utils/types";

export default function StylizedSingleSelect({
  title,
  value,
  options,
  setValue,
}: {
  title: string;
  value: SingleValue<SelectItem>;
  options: SelectItem[];
  setValue: Dispatch<SetStateAction<SingleValue<SelectItem>>>;
}) {
  return (
    <div className="flex flex-1 border-b border-b-barter-blue">
      <div className="flex items-center border-r border-r-barter-blue bg-barter-isabeline px-4">
        <span className="text-sm font-bold">{title}</span>
      </div>

      <div className="flex flex-1">
        <Select
          value={value}
          onChange={(newValue) => setValue(newValue)}
          options={options}
          classNames={{
            container: () => "w-full border-none",
            control: () => "border-0 h-full rounded-none",
          }}
        />
      </div>
    </div>
  );
}
