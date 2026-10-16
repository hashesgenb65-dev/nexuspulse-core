/**
 * Entry point for the token helper. Run it with `npm run token -- <command>`.
 *
 * Writes output to stdout and errors to stderr, then exits with the code that
 * the command returns. The command logic lives in token_cli.ts and is tested there.
 */

import { runTokenCli, type CliIo } from './token_cli.js';

const io: CliIo = {
  write: (line: string): void => {
    process.stdout.write(`${line}\n`);
  },
  error: (line: string): void => {
    process.stderr.write(`${line}\n`);
  },
};

process.exitCode = runTokenCli(process.argv.slice(2), io);
