import { createFileRoute } from "@tanstack/react-router";

import { MastraPage } from "../components/mastra/MastraPage";

export const Route = createFileRoute("/_chat/mastra")({
  component: MastraPage,
});
