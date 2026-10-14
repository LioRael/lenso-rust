import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const source = process.argv[2];
const destination = process.argv[3];
if (!source || !destination) throw new Error("Pass native source and owned staging roots.");

async function syncFile(from: string, to: string) {
  const bytes = await readFile(from);
  const previous = await readFile(to).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return undefined;
    throw error;
  });
  if (!previous?.equals(bytes)) {
    // Changed content must be newer than Cargo's last fingerprint, even within one second.
    await writeFile(to, bytes);
  }
}

async function syncDirectory(from: string, to: string) {
  await mkdir(to, { recursive: true });
  const entries = await readdir(from, { withFileTypes: true });
  const names = new Set(entries.map((entry) => entry.name));
  for (const previous of await readdir(to))
    if (!names.has(previous)) await rm(join(to, previous), { recursive: true, force: true });
  for (const entry of entries) {
    const input = join(from, entry.name);
    const output = join(to, entry.name);
    if (entry.isDirectory()) await syncDirectory(input, output);
    else if (entry.isFile()) await syncFile(input, output);
    else throw new Error(`Native source staging requires regular files/directories: ${input}`);
  }
}

const root = resolve(source);
const stage = resolve(destination);
await syncFile(join(root, "Cargo.toml"), join(stage, "Cargo.toml"));
await syncDirectory(join(root, "crates"), join(stage, "crates"));
await syncDirectory(join(root, "examples/batch"), join(stage, "examples/batch"));
