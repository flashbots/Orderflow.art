import Link from "next/link";
import { useRouter } from "next/router";

// Individual page
type Page = {
  name: string;
  path: string;
};

// Orderflow.art pages
const PAGES: Page[] = [
  { name: "Sankey", path: "/" },
  { name: "Frontends", path: "/frontends" },
  { name: "Order Flow Auctions", path: "/ofa" },
];

export default function Navigation() {
  // Collect current path
  const { route }: { route: string } = useRouter();

  return (
    <section className="z-10000 sticky top-0 flex flex-col overflow-x-auto border-b border-t border-barter-purple bg-barter-isabeline px-5 py-2 text-center">
      <ul className="mx-auto flex min-w-max max-w-[800px] flex-row space-x-10">
        {PAGES.map((page: Page, i: number) => {
          const isActivePath: boolean = route === page.path;

          return (
            // Render each page
            <li key={i}>
              <Link href={page.path} className="cursor-pointer">
                {isActivePath ? (
                  <span className="cursor-pointer text-[15px] text-gray-800 underline transition-opacity hover:opacity-70">
                    {page.name}
                  </span>
                ) : (
                  <span className="cursor-pointer text-[15px] text-gray-500 transition-opacity hover:underline hover:opacity-70">
                    {page.name}
                  </span>
                )}
              </Link>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
