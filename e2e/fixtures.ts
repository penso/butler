import { spawn, type ChildProcess } from "node:child_process";
import path from "node:path";
import { test as base } from "@playwright/test";

/// Built by `npm run server` (cargo build -p butler-e2e-server).
const binary =
  process.env.BUTLER_E2E_SERVER ??
  path.resolve(__dirname, "../target/debug/butler-e2e-server");

export type ServerOptions = {
  /// Where the dashboard is mounted, e.g. "/admin/jobs".
  base?: string;
  /// "user:password" for HTTP basic auth.
  auth?: string;
  /// A fixed port, to restart on the same one.
  port?: number;
};

export class Server {
  /// Origin plus base path, without a trailing slash.
  url = "";
  port = 0;
  private process?: ChildProcess;

  constructor(private options: ServerOptions) {}

  /// The dashboard's own page: the bare base when nested (axum doesn't serve
  /// "/admin/jobs/"), "/" at the root.
  get home(): string {
    return this.options.base ? this.url : this.url + "/";
  }

  async start(): Promise<void> {
    const args: string[] = ["--port", String(this.port || this.options.port || 0)];
    if (this.options.base) args.push("--base", this.options.base);
    if (this.options.auth) args.push("--auth", this.options.auth);
    const child = spawn(binary, args, { stdio: ["ignore", "pipe", "inherit"] });
    this.process = child;
    const origin = await new Promise<string>((resolve, reject) => {
      let output = "";
      child.stdout!.on("data", (chunk: Buffer) => {
        output += chunk.toString();
        const match = output.match(/listening on (http:\/\/[^\s]+)/);
        if (match) resolve(match[1]);
      });
      child.once("error", reject);
      child.once("exit", (code) => reject(new Error(`e2e server exited with ${code}`)));
    });
    this.port = Number(new URL(origin).port);
    this.url = origin + (this.options.base ?? "");
  }

  async stop(): Promise<void> {
    const child = this.process;
    // Killed by a signal, a process has a signalCode and no exitCode.
    if (!child || child.exitCode !== null || child.signalCode !== null) return;
    const exited = new Promise((resolve) => child.once("exit", resolve));
    child.kill("SIGKILL");
    await exited;
  }

  /// Stops, then starts again on the same port, with a fresh queue.
  async restart(): Promise<void> {
    await this.stop();
    await this.start();
  }
}

export const test = base.extend<{ startServer: (options?: ServerOptions) => Promise<Server> }>({
  startServer: async ({}, use) => {
    const servers: Server[] = [];
    await use(async (options = {}) => {
      const server = new Server(options);
      servers.push(server);
      await server.start();
      return server;
    });
    await Promise.all(servers.map((server) => server.stop()));
  },
});

export { expect } from "@playwright/test";
