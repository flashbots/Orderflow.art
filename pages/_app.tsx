import "@/globals.css";
import type { AppProps } from "next/app";
import NextNProgress from "nextjs-progressbar";
import "react-notion-x/src/styles.css";
import "prismjs/themes/prism-tomorrow.css";
import "katex/dist/katex.min.css";

export default function Orderflowin({ Component, pageProps }: AppProps) {
  return (
    <>
      {/* Add global progressbar */}
      <NextNProgress color="#9486BD" options={{ showSpinner: false }} showOnShallow={true} />
      <Component {...pageProps} />
    </>
  );
}
