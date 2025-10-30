import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: false,
  transpilePackages: ['react-notion-x'],
  serverExternalPackages: ['keyv', 'cacheable-request', 'got', 'notion-client'],
  turbopack: {},
};

export default nextConfig;
