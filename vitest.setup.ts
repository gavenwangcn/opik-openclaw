import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "opik-openclaw-test-"));
const dbPath = join(dir, "test.duckdb");
writeFileSync(dbPath, "");
process.env.OPIK_DUCKDB_PATH = dbPath;
