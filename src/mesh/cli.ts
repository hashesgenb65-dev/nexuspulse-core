/**
 * Command-line entry point: runs one mesh member from a JSON config file.
 *
 *   node-runner <config.json>
 *
 * Exits with 2 on a bad config or missing argument, and with 1 if the node cannot start.
 * Otherwise it runs until SIGINT or SIGTERM, then stops cleanly and exits with 0.
 */

import { loadNodeConfig, MeshConfigError } from './node_config.js';
import { startMeshNode } from './node_runner.js';

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export async function runMeshNodeCli(argv: readonly string[]): Promise<number> {
  const file = argv[0];
  if (file === undefined) {
    process.stderr.write('usage: node-runner <config.json>\n');
    return 2;
  }

  let config;
  try {
    config = loadNodeConfig(file);
  } catch (error) {
    if (error instanceof MeshConfigError) {
      for (const issue of error.issues) process.stderr.write(`config: ${issue.path} ${issue.message}\n`);
      return 2;
    }
    throw error;
  }

  let running;
  try {
    running = await startMeshNode(config, {
      onError: (error, where) => process.stderr.write(`[${where}] ${describe(error)}\n`),
    });
  } catch (error) {
    process.stderr.write(`cannot start ${config.nodeId}: ${describe(error)}\n`);
    return 1;
  }
  process.stdout.write(`${config.nodeId} peers on :${running.rpcPort}, HTTP on :${running.httpPort}\n`);

  const shutdown = (): void => {
    void running.stop().then(() => process.exit(0));
  };
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
  return 0;
}

// Starts the CLI when this file is the entry point. Do not import this file from other modules.
void runMeshNodeCli(process.argv.slice(2)).then((code) => {
  if (code !== 0) process.exitCode = code;
});
