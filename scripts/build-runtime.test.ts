import { expect, test } from "bun:test";
import { cp, mkdir, mkdtemp, rm, stat, symlink, utimes } from "node:fs/promises";
import { resolve } from "node:path";

test.skipIf(!process.env.LENSO_RUNTIME_ROOT)(
  "failed generation remains invalid on retry and a restored contract recovers without touching source locks",
  async () => {
    const runtimeRoot = resolve(process.env.LENSO_RUNTIME_ROOT!);
    const rustRoot = resolve(import.meta.dir, "..");
    await mkdir(resolve(rustRoot, "target"), { recursive: true });
    const fixture = await mkdtemp(resolve(rustRoot, "target/builder-test-"));
    try {
      const app = resolve(fixture, "app");
      const runtime = resolve(fixture, "runtime");
      await mkdir(app);
      await mkdir(runtime);
      for (const path of ["Cargo.toml", "Cargo.lock", "crates", "examples", "scripts"])
        await cp(resolve(rustRoot, path), resolve(app, path), {
          recursive: true,
          preserveTimestamps: true,
        });
      for (const path of ["Cargo.toml", "crates", "sdk", "tools"])
        await symlink(resolve(runtimeRoot, path), resolve(runtime, path));
      await mkdir(resolve(runtime, "examples/contracts"), { recursive: true });
      const canonical = resolve(runtimeRoot, "examples/contracts/logs.json");
      const contract = resolve(runtime, "examples/contracts/logs.json");
      await cp(canonical, contract);
      const lock = await Bun.file(resolve(app, "Cargo.lock")).text();
      const build = async () => {
        const child = Bun.spawn(
          [
            "sh",
            "scripts/build-runtime.sh",
            "--runtime-root",
            runtime,
            "--out",
            "target/debug/logs-batch",
          ],
          { cwd: app, stdout: "pipe", stderr: "pipe" },
        );
        const [code, stdout, stderr] = await Promise.all([
          child.exited,
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ]);
        return { code, output: stdout + stderr };
      };
      expect((await build()).code).toBe(0);
      const binary = resolve(app, "target/debug/logs-batch");
      const hash = async () =>
        new Bun.CryptoHasher("sha256").update(await Bun.file(binary).arrayBuffer()).digest("hex");
      const before = await hash();
      const invalid = await Bun.file(contract).json();
      invalid.methods.aggregate.input.format = "unsupported";
      await Bun.write(contract, JSON.stringify(invalid));
      for (let attempt = 0; attempt < 2; attempt++) {
        const result = await build();
        expect(result.code).not.toBe(0);
        expect(result.output).toContain("unsupported schema keyword: format");
        expect(await hash()).toBe(before);
      }
      await cp(canonical, contract);
      expect((await build()).code).toBe(0);
      const source = resolve(app, "examples/batch/src/main.rs");
      const original = await Bun.file(source).text();
      const metadata = await stat(source);
      const recovered = await hash();
      expect(original).toContain("async fn main");
      await Bun.write(source, original.replace("async fn main", "xxxxx fn main"));
      // Same-size content with a restored mtime must still invalidate Cargo.
      await utimes(source, metadata.atime, metadata.mtime);
      const failed = await build();
      expect(failed.code).not.toBe(0);
      expect(await hash()).toBe(recovered);
      await Bun.write(source, original);
      await utimes(source, metadata.atime, metadata.mtime);
      expect((await build()).code).toBe(0);
      expect(await Bun.file(resolve(app, "Cargo.lock")).text()).toBe(lock);
    } finally {
      await rm(fixture, { recursive: true, force: true });
    }
  },
  60000,
);
