import { describe, expect, test } from "vitest";
import { commands } from "vitest/browser";

describe("React Router RSC browser actions", () => {
  test("adds the detail product to the cart without rendering an app error", async () => {
    const result = await commands.runRscAddToCartFlow();

    expect(result.url).toContain("/projects/runtime");
    expect(result.bodyText).toContain("1 item selected.");
    expect(result.bodyText).not.toContain("Unexpected Application Error");
    expect(result.consoleErrors.filter(isActionableConsoleError)).toEqual([]);
    expect(result.pageErrors).toEqual([]);
  });
});

function isActionableConsoleError(message: string) {
  return !message.startsWith("Failed to fetch manifest patches ");
}
