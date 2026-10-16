/**
 * NexusPulse process entry point. Reads settings from the environment, starts
 * the application, and shuts it down cleanly on SIGINT or SIGTERM.
 *
 * Exit codes: 0 clean shutdown, 2 invalid settings, 1 start-up or runtime failure.
 */

import { NexusPulseApp } from './app/server.js';
import { parseSettings, SettingsError } from './app/settings.js';

async function main(): Promise<number> {
  let settings;
  try {
    settings = parseSettings(process.env);
  } catch (err) {
    if (err instanceof SettingsError) {
      for (const issue of err.issues) {
        process.stderr.write(`nexuspulse: ${issue.variable} ${issue.message}\n`);
      }
      return 2;
    }
    throw err;
  }

  const app = new NexusPulseApp({ settings });
  try {
    await app.start();
  } catch (err) {
    process.stderr.write(`nexuspulse: ${err instanceof Error ? err.message : String(err)}\n`);
    return 1;
  }

  return await new Promise<number>((resolve) => {
    let stopping = false;
    const shutdown = (signal: NodeJS.Signals): void => {
      if (stopping) return;
      stopping = true;
      process.stderr.write(`nexuspulse: ${signal} received, shutting down\n`);
      app
        .stop()
        .then(() => resolve(0))
        .catch((err: unknown) => {
          process.stderr.write(`nexuspulse: shutdown failed: ${err instanceof Error ? err.message : String(err)}\n`);
          resolve(1);
        });
    };
    process.once('SIGINT', shutdown);
    process.once('SIGTERM', shutdown);
  });
}

main().then(
  (code) => {
    process.exitCode = code;
  },
  (err: unknown) => {
    process.stderr.write(`nexuspulse: fatal: ${err instanceof Error ? err.stack ?? err.message : String(err)}\n`);
    process.exitCode = 1;
  },
);
