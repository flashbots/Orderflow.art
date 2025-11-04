import { Avatar } from "@/utils/types";
import Image from "next/image";
import { FC } from "react";

type Props = {
  avatars: Avatar[];
};

const Avatars: FC<Props> = ({ avatars }) => {
  return (
    <div className="flex justify-center -space-x-1 overflow-hidden">
      {avatars.map((avatar: Avatar, i) => (
        <a
          className={`transition-opacity hover:opacity-70 ${avatar.spaceBefore ? "ml-6" : ""}`}
          href={avatar.twitter}
          target="_blank"
          rel="noopener noreferrer"
          key={i}
        >
          <Image
            className="h-10 w-10 rounded-full ring-2 ring-white"
            src={avatar.path}
            alt={avatar.path}
            width={40}
            height={40}
          />
        </a>
      ))}
    </div>
  );
};

export default Avatars;
