import { describe, expect, test } from "vitest";
import { commands } from "vitest/browser";

describe("React Router browser forms", () => {
  test("adds products without failing route discovery or redirecting to data URLs", async () => {
    const result = await commands.runReactRouterAddToCartFlows();

    expect(result.homeUrl).toMatch(/\/$/);
    expect(result.detailUrl).toContain("/projects/runtime");
    expect(result.detailUrl).not.toContain(".data");
    expect(result.bodyText).toContain("2 items selected.");
    expect(result.bodyText).not.toContain("Application Error");
    expect(result.homeScrollBeforeAdd).toBeGreaterThan(0);
    expect(result.homeScrollAfterAdd).toBeGreaterThan(0);
    expect(result.detailScrollBeforeAdd).toBeGreaterThan(0);
    expect(result.detailScrollAfterAdd).toBeGreaterThan(0);
    expect(Math.abs(result.detailButtonTopAfterAdd - result.detailButtonTopBeforeAdd)).toBeLessThan(80);
    expect(result.manifestStatuses).toContain(200);
    expect(result.manifestStatuses).not.toContain(404);
    expect(result.consoleErrors).toEqual([]);
    expect(result.pageErrors).toEqual([]);
  });
});
