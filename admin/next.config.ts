import type { NextConfig } from "next";

const QUEUE_API_URL = process.env.QUEUE_API_URL || "http://localhost:8080";

const nextConfig: NextConfig = {
  output: "standalone",
  async rewrites() {
    return [
      {
        source: "/api/backend/:path*",
        destination: `${QUEUE_API_URL}/:path*`,
      },
    ];
  },
};

export default nextConfig;
