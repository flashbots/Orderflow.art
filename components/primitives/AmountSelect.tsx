import Creatable from "react-select/creatable";
import { Dispatch, SetStateAction, useState } from "react";
import { SelectItem } from "@/utils/types";

export default function StylizedCreatableSelect({
  value,
  options,
  setValue,
}: {
  value: string;
  options: SelectItem[];
  setValue: Dispatch<SetStateAction<string>>;
}) {
  const [inputValue, setInputValue] = useState("");

  return (
    <div className="flex flex-1 border-b border-b-barter-blue">
      <div className="flex flex-1">
        <Creatable
          value={value ? { label: value, value } : {}}
          onChange={(newValue) => {
            if (newValue?.value) {
              setValue(newValue.value);
            } else {
              setValue("");
            }
          }}
          options={options}
          classNames={{
            container: () => "w-full border-none",
            control: () => "border-0 h-full rounded-none",
          }}
          isClearable={true}
          formatCreateLabel={(inputValue) => {
            return <span>{inputValue}</span>;
          }}
          inputValue={inputValue}
          onInputChange={(newValue) => {
            if (newValue === "" || (!isNaN(newValue as any) && !isNaN(parseFloat(newValue)))) {
              setInputValue(newValue);
            }
          }}
        />
      </div>
    </div>
  );
}
