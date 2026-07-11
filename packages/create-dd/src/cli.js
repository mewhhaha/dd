#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline/promises';
import process from 'node:process';
import { pathToFileURL } from 'node:url';
import { TEMPLATE_IDS, installCommand, packageManagerFromUserAgent, scaffold } from './core.js';

const usage = `Usage: create-dd [directory] [options]

Create a new dd application.

Options:
  -t, --template <name>          react-router, react-router-rsc, or hono
      --install / --no-install   Install dependencies (default: install)
      --package-manager <name>   pnpm, npm, yarn, or bun
  -h, --help                     Show this help`;

export function shellEscapePath(target, platform = process.platform) {
  if (platform === 'win32') return JSON.stringify(target);
  return `'${target.replaceAll("'", "'\"'\"'")}'`;
}

export function directoryInstructions(directory, command, platform = process.platform) {
  if (platform === 'win32') return `Project directory: ${JSON.stringify(directory)}\nRun this from that directory:\n  ${command}`;
  return `cd ${shellEscapePath(directory, platform)}\n  ${command}`;
}

export function parseArgs(argv) {
  const result = { install: true, packageManager: undefined, template: undefined, target: undefined };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '-h' || arg === '--help') return { help: true };
    if (arg === '--no-install') result.install = false;
    else if (arg === '--install') result.install = true;
    else if (arg === '-t' || arg === '--template') {
      const value = argv[++i];
      if (!value || value.startsWith('-')) throw new Error('Missing value for --template');
      result.template = value;
    }
    else if (arg.startsWith('--template=')) result.template = arg.slice('--template='.length);
    else if (arg === '--package-manager') {
      const value = argv[++i];
      if (!value || value.startsWith('-')) throw new Error('Missing value for --package-manager');
      result.packageManager = value;
    }
    else if (arg.startsWith('--package-manager=')) result.packageManager = arg.slice('--package-manager='.length);
    else if (arg.startsWith('-')) throw new Error(`Unknown option: ${arg}`);
    else if (!result.target) result.target = arg;
    else throw new Error(`Unexpected argument: ${arg}`);
  }
  if ((argv.includes('-t') || argv.includes('--template')) && !result.template) throw new Error('Missing value for --template');
  if (argv.includes('--package-manager') && !result.packageManager) throw new Error('Missing value for --package-manager');
  return result;
}

async function promptMissing(options) {
  if (options.target && options.template) return options;
  if (!process.stdin.isTTY || !process.stdout.isTTY) throw new Error('Directory and --template are required when not running interactively.');
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  try {
    if (!options.target) options.target = (await rl.question('Project directory: ')).trim();
    if (!options.template) {
      console.log('\nChoose a template:');
      TEMPLATE_IDS.forEach((id, index) => console.log(`  ${index + 1}) ${id}`));
      const choice = (await rl.question('Template number: ')).trim();
      options.template = TEMPLATE_IDS[Number(choice) - 1];
    }
  } finally { rl.close(); }
  if (!options.target) throw new Error('Project directory is required.');
  return options;
}

export async function run(argv = process.argv.slice(2)) {
  let options = parseArgs(argv);
  if (options.help) { console.log(usage); return; }
  options = await promptMissing(options);
  if (!TEMPLATE_IDS.includes(options.template)) throw new Error(`Unknown template "${options.template}". Choose: ${TEMPLATE_IDS.join(', ')}`);
  const packageManager = options.packageManager ?? packageManagerFromUserAgent();
  // Validate even when installation is skipped, so option errors never leave a scaffold behind.
  installCommand(packageManager);
  const created = await scaffold({ target: options.target, template: options.template });
  console.log(`\nCreated ${created.name} using ${options.template}.`);
  if (options.install) {
    const { command, args } = installCommand(packageManager);
    console.log(`Installing dependencies with ${command}…`);
    const code = await new Promise((resolve, reject) => {
      const child = spawn(command, args, { cwd: created.destination, stdio: 'inherit', shell: process.platform === 'win32' });
      child.on('error', reject); child.on('close', resolve);
    });
    if (code !== 0) {
      console.error(`Dependency installation failed (exit ${code}). Your project was kept at ${created.destination}.\nRetry:\n  ${directoryInstructions(created.destination, `${command} ${args.join(' ')}`)}`);
      process.exitCode = 1;
    }
  }
  console.log(`\nNext steps:\n  ${directoryInstructions(created.destination, `${packageManager} run dev`)}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  run().catch(error => { console.error(`create-dd: ${error.message}`); process.exitCode = 1; });
}
