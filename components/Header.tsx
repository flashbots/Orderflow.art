import Link from "next/link";
import Image from "next/image";
import Avatars from "@/components/Avatars";

export default function Header() {
  return (
    <header className="flex flex-col text-center">
      <section>
        {/* Title */}
        <div className="flex w-full flex-col bg-barter-isabeline px-5 py-8 text-center">
          {/* General */}
          <div className="flex flex-col items-center">
            <div className="flex flex-row items-center gap-3 pb-3">
              <a
                href="https://flashbots.net"
                target="_blank"
                rel="noopener noreferrer"
                className="transition-opacity hover:opacity-70"
              >
                <Image
                  src="/vectors/flashbots-logo.svg"
                  alt="Flashbots logo"
                  height={30}
                  width={119}
                />
              </a>
              <span className="pt-[5px] font-serif text-gray-700">X</span>
              <a
                href="https://barterswap.xyz"
                target="_blank"
                rel="noopener noreferrer"
                className="transition-opacity hover:opacity-70"
              >
                <Image
                  src="/barter-horizontal-logo.png"
                  alt="Barter logo"
                  height={30}
                  width={119}
                />
              </a>
            </div>
            <p className="text-sm text-gray-600 pb-3">
              Started by Flashbots, continued by  Barter
            </p>

            <Link href="/" className="transition-opacity hover:opacity-70">
              <h1 className="float-left text-4xl font-semibold sm:text-4xl">🎨 Orderflow.art</h1>
            </Link>
            <p className="text-md mx-auto max-w-[500px] pt-4 text-center text-stone-700">
              Illuminating Ethereum&apos;s order flow landscape. Empowering users with tools to
              visualize power and profit in the MEV supply network.
            </p>
            <div className="mt-6">
              <Link
                href="/methodology"
                className="rounded-full bg-barter-purple px-6 py-3 text-sm text-white transition-opacity hover:opacity-70"
              >
                Read our methodology →
              </Link>
            </div>
            <div className="mb-3 mt-5">
              <a
                href="https://github.com/flashbots/Orderflow.art"
                target="_blank"
                rel="noopener noreferrer"
                className="p-1 text-sm text-gray-700 underline transition-opacity hover:opacity-70"
              >
                This website is open-source!
              </a>
            </div>
          </div>

          {/* Maintainers */}
          <div>
            <Avatars
              avatars={[
                {
                  path: "/avatars/angela.jpg",
                  twitter: "https://twitter.com/0xangelfish",
                },
                {
                  path: "/avatars/danning.png",
                  twitter: "https://twitter.com/sui414",
                },
                {
                  path: "/avatars/jaden.jpg",
                  twitter: "https://twitter.com/JadenDurnford",
                },
                {
                  path: "/avatars/barter.png",
                  twitter: "https://x.com/BarterDeFi",
                  spaceBefore: true,
                },
                {
                  path: "/avatars/Alex_Khailuk.jpeg",
                  twitter: "https://x.com/Alex_Khailuk",
                },
                {
                  path: "/avatars/real_obbwd.jpeg",
                  twitter: "https://x.com/real_obbwd",
                },
                {
                  path: "/avatars/grass_the_touch.jpeg",
                  twitter: "https://x.com/grass_the_touch",
                },
              ]}
            />
          </div>
        </div>
      </section>
    </header>
  );
}
