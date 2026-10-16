/**
 * Command-line helper for NexusPulse token files.
 *
 *   token new <file> <name>=<role> [<name>=<role> ...]   create a 0600 token file
 *   token check <file>                                   validate a token file
 *   token help                                           show usage
 *
 * Secrets are never printed. `new` writes them only into the file, and `check`
 * reports names and roles.
 *
 * Exit codes: 0 success, 1 the token file is missing, unreadable or invalid,
 * 2 bad usage.
 */

import { ROLE_NAMES, type Role } from '../api/auth.js';
import { loadTokenFile, newTokenSpec, TokenFileError, writeTokenFile } from '../app/token_file.js';

export interface CliIo {
  write(line: string): void;
  error(line: string): void;
}

export const EXIT_OK = 0;
export const EXIT_FAILURE = 1;
export const EXIT_USAGE = 2;

export const USAGE = [
  'usage:',
  '  token new <file> <name>=<role> [<name>=<role> ...]   create a token file (mode 0600)',
  '  token check <file>                                   validate a token file',
  '  token help',
  `roles: ${ROLE_NAMES.join(', ')}`,
].join('\n');

interface ParsedTokenArg {
  readonly name: string;
  readonly role: Role;
}

/** Parses `name=role`. Returns an error message instead of throwing, so the CLI can report it. */
export function parseTokenArg(arg: string): ParsedTokenArg | string {
  const eq = arg.indexOf('=');
  if (eq <= 0 || eq === arg.length - 1) return `expected name=role, got "${arg}"`;
  const name = arg.slice(0, eq);
  const role = arg.slice(eq + 1);
  if (!(ROLE_NAMES as readonly string[]).includes(role)) {
    return `role must be one of ${ROLE_NAMES.join(', ')}, got "${role}"`;
  }
  return { name, role: role as Role };
}

function isParsed(value: ParsedTokenArg | string): value is ParsedTokenArg {
  return typeof value !== 'string';
}

function commandNew(args: readonly string[], io: CliIo): number {
  const [file, ...tokenArgs] = args;
  if (file === undefined || tokenArgs.length === 0) {
    io.error('new needs a file and at least one name=role');
    io.error(USAGE);
    return EXIT_USAGE;
  }
  const specs = [];
  for (const arg of tokenArgs) {
    const parsed = parseTokenArg(arg);
    if (!isParsed(parsed)) {
      io.error(parsed);
      return EXIT_USAGE;
    }
    specs.push(newTokenSpec(parsed.name, parsed.role));
  }
  try {
    writeTokenFile(file, specs);
  } catch (err) {
    if (err instanceof TokenFileError) {
      io.error(err.message);
      for (const issue of err.issues) io.error(`  ${issue}`);
      return EXIT_FAILURE;
    }
    throw err;
  }
  io.write(`wrote ${specs.length} token${specs.length === 1 ? '' : 's'} to ${file} (mode 0600)`);
  return EXIT_OK;
}

function commandCheck(args: readonly string[], io: CliIo): number {
  const [file] = args;
  if (file === undefined || args.length !== 1) {
    io.error('check needs exactly one file');
    io.error(USAGE);
    return EXIT_USAGE;
  }
  try {
    const auth = loadTokenFile(file);
    io.write(`ok: ${auth.size} token${auth.size === 1 ? '' : 's'} in ${file}`);
    for (const entry of auth.describe()) io.write(`  ${entry.name} (${entry.role})`);
    return EXIT_OK;
  } catch (err) {
    if (err instanceof TokenFileError) {
      io.error(err.message);
      return EXIT_FAILURE;
    }
    throw err;
  }
}

/**
 * Runs one CLI command. `argv` excludes the program name.
 *
 * @returns The process exit code.
 */
export function runTokenCli(argv: readonly string[], io: CliIo): number {
  const [command, ...rest] = argv;
  switch (command) {
    case 'new':
      return commandNew(rest, io);
    case 'check':
      return commandCheck(rest, io);
    case 'help':
    case '--help':
    case '-h':
      io.write(USAGE);
      return EXIT_OK;
    case undefined:
      io.error(USAGE);
      return EXIT_USAGE;
    default:
      io.error(`unknown command "${command}"`);
      io.error(USAGE);
      return EXIT_USAGE;
  }
}
