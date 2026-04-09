import { describe, expect, test } from "vitest";
import { collectMediaPathsFromString, collectMediaPathsFromUnknown } from "./media.js";

/** Resolved `/tmp/...` may become `D:\\tmp\\...` on Windows; compare by suffix. */
function expectEndsWithTmp(paths: string[], ...suffixes: string[]): void {
  expect(paths).toHaveLength(suffixes.length);
  for (let i = 0; i < suffixes.length; i++) {
    const n = paths[i].replace(/\\/g, "/");
    const s = suffixes[i].replace(/\\/g, "/");
    expect(n.endsWith(s)).toBe(true);
  }
}

describe("media path extraction", () => {
  test("does not collect direct local path values without an explicit marker", () => {
    const target = new Set<string>();
    collectMediaPathsFromString("/tmp/image.png", target);
    expect(target.size).toBe(0);
  });

  test("collects media: local path references", () => {
    const target = new Set<string>();
    collectMediaPathsFromString("preview media:/tmp/image.png", target);
    expectEndsWithTmp([...target], "/tmp/image.png");
  });

  test("collects file:// local path references", () => {
    const target = new Set<string>();
    collectMediaPathsFromString("open file:///tmp/image.png", target);
    expectEndsWithTmp([...target], "/tmp/image.png");
  });

  test("collects markdown local media links", () => {
    const target = new Set<string>();
    collectMediaPathsFromString("![preview](/tmp/image.png)", target);
    expectEndsWithTmp([...target], "/tmp/image.png");
  });

  test("does not collect incidental local paths in plain text", () => {
    const target = new Set<string>();
    collectMediaPathsFromString("debug: attempted /tmp/image.png from prior run", target);
    expect(target.size).toBe(0);
  });

  test("collects local media paths from nested objects", () => {
    const target = new Set<string>();
    collectMediaPathsFromUnknown(
      {
        images: [
          { src: "file:///tmp/image.png" },
          { ref: "media:/tmp/other.jpg" },
        ],
      },
      target,
    );
    const sorted = [...target].sort();
    expectEndsWithTmp(sorted, "/tmp/image.png", "/tmp/other.jpg");
  });
});
