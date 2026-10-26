/**
 * Entry point for the API key helper. Run it with `npm run apikey -- <command>`.
 *
 * Writes output to stdout and errors to stderr, then exits with the code that the command returns.
 * The command logic lives in api_key_cli.ts and is tested there.
 */

import { runApiKeyCli, type ApiKeyCliIo } from './api_key_cli.js';

const io: ApiKeyCliIo = {
  write: (line: string): void => {
    process.stdout.write(`${line}\n`);
  },
  error: (line: string): void => {
    process.stderr.write(`${line}\n`);
  },
};

process.exitCode = runApiKeyCli(process.argv.slice(2), io);
