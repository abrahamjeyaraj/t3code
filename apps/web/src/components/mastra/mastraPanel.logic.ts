export type MastraStatusTone = "success" | "danger" | "running" | "muted";

export function mastraStatusTone(status: string): MastraStatusTone {
  switch (status) {
    case "success":
      return "success";
    case "failed":
    case "tripwire":
      return "danger";
    case "running":
    case "pending":
    case "waiting":
      return "running";
    default:
      return "muted";
  }
}

export function mastraStatusLabel(status: string): string {
  return status === "success" ? "done" : status;
}

/** "3m 12s" between start and end (or `now` while still running); null when unparseable. */
export function formatMastraElapsed(
  startedAt: string,
  endedAt: string | null,
  now: number,
): string | null {
  const start = Date.parse(startedAt);
  const end = endedAt === null ? now : Date.parse(endedAt);
  if (Number.isNaN(start) || Number.isNaN(end)) return null;
  const total = Math.max(0, Math.round((end - start) / 1000));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

export function formatMastraStarted(startedAt: string): string {
  const date = new Date(startedAt);
  return Number.isNaN(date.getTime())
    ? startedAt
    : date.toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      });
}
