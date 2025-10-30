import Image from "next/image";
import { FC } from "react";

type Props = {
  title?: string;
  description?: string;
  url: string;
  square?: boolean;
  libmev?: boolean;
};

const Embed: FC<Props> = ({ title, description, url, square, libmev }) => {
  return (
    <div className={`w-full border border-barter-blue ${square ? "aspect-square" : ""}`}>
      {/* About */}
      {title && description && (
        <div className="border-b border-b-barter-blue bg-barter-isabeline px-4 py-2">
          {title && <h3 className="font-semibold">{title}</h3>}
          {description && <p className="text-sm">{description}</p>}
        </div>
      )}

      {/* Embed */}
      <div className="relative h-full w-full">
        <embed src={url} width="100%" height={square ? "100%" : "600px"} />
        {libmev && (
          <Image
            className="absolute left-2 top-8"
            src={"/vectors/libmev-logo.svg"}
            alt="libmev logo"
            width={80}
            height={80}
          />
        )}
      </div>
    </div>
  );
};

export default Embed;
