import { describe, expect, it } from "vite-plus/test";
import { formatMastraElapsed, mastraStatusLabel, mastraStatusTone } from "./mastraPanel.logic";

describe("mastra panel logic", () => {
  it("maps statuses to tones", () => {
    expect(mastraStatusTone("success")).toBe("success");
    expect(mastraStatusTone("failed")).toBe("danger");
    expect(mastraStatusTone("running")).toBe("running");
    expect(mastraStatusTone("canceled")).toBe("muted");
    expect(mastraStatusLabel("success")).toBe("done");
  });
  it("formats elapsed time for finished and running runs", () => {
    const start = "2026-11-07T10:00:00.000Z";
    expect(formatMastraElapsed(start, "2026-11-07T10:03:12.000Z", 0)).toBe("3m 12s");
    expect(formatMastraElapsed(start, null, Date.parse("2026-11-07T10:00:09.000Z"))).toBe("9s");
    expect(formatMastraElapsed(start, "2026-11-07T12:30:00.000Z", 0)).toBe("2h 30m");
    expect(formatMastraElapsed("nope", null, 0)).toBeNull();
  });
});
