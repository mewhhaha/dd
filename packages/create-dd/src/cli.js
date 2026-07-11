#!/usr/bin/env node
import * as clack from '@clack/prompts';
import cac from 'cac';
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import process from 'node:process';
import { pathToFileURL } from 'node:url';
import { TEMPLATE_IDS, installCommand, packageManagerFromUserAgent, scaffold } from './core.js';

const packageVersion = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
).version;

const templateOptions = [
  { value: 'react-router', label: 'React Router', hint: 'full-stack SSR' },
  { value: 'react-router-rsc', label: 'React Router RSC', hint: 'server components' },
  { value: 'hono', label: 'Hono', hint: 'lightweight web app' },
];

export function shellEscapePath(target, platform = process.platform) {
  if (platform === 'win32') return JSON.stringify(target);
  return `'${target.replaceAll("'", "'\"'\"'")}'`;
}

export function directoryInstructions(directory, command, platform = process.platform) {
  if (platform === 'win32') {
    return `Project directory: ${JSON.stringify(directory)}\nRun this from that directory:\n${command}`;
  }
  return `cd ${shellEscapePath(directory, platform)}\n${command}`;
}

function createParser() {
  const cli = cac('create-dd');
  const command = cli
    .command('[directory]', 'Create a new dd application')
    .option('-t, --template <name>', 'Starter template')
    .option('--install, --no-install', 'Install dependencies', { default: true })
    .option('--package-manager <name>', 'pnpm, npm, yarn, or bun')
    .example('create-dd my-app --template hono')
    .example('create-dd . --template react-router-rsc --no-install');

  cli
    .version(packageVersion)
    .help();

  return { cli, command };
}

function lastOptionValue(argv, names) {
  let value;
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index];
    if (argument === '--') break;
    if (names.includes(argument)) {
      const next = argv[++index];
      if (!next || next.startsWith('-')) {
        throw new Error(`option \`${argument}\` value is missing`);
      }
      value = next;
      continue;
    }
    for (const name of names) {
      if (name.startsWith('--') && argument.startsWith(`${name}=`)) {
        const next = argument.slice(name.length + 1);
        if (!next) throw new Error(`option \`${name}\` value is missing`);
        value = next;
      }
    }
  }
  return value;
}

export function parseArgs(argv) {
  const { cli, command } = createParser();
  const parsed = cli.parse(['node', 'create-dd', ...argv], { run: false });
  if (parsed.options.help || parsed.options.version) {
    return { help: Boolean(parsed.options.help), version: Boolean(parsed.options.version) };
  }

  command.checkUnknownOptions();
  command.checkOptionValue();
  command.checkRequiredArgs();
  command.checkUnusedArgs();
  if (parsed.options['--'].length) {
    throw new Error(`Unused args: ${parsed.options['--'].map(value => `\`${value}\``).join(', ')}`);
  }

  const installValues = parsed.options.install;
  const install = Array.isArray(installValues) ? installValues.at(-1) : installValues;
  return {
    install: install !== false,
    packageManager: lastOptionValue(argv, ['--package-manager']),
    target: parsed.args[0],
    template: lastOptionValue(argv, ['-t', '--template']),
  };
}

function cancelPrompts(prompts) {
  prompts.cancel('Operation cancelled.');
  return undefined;
}

export async function promptMissing(
  options,
  prompts = clack,
  interactive = Boolean(process.stdin.isTTY && process.stdout.isTTY),
) {
  if (options.target && options.template) return options;
  if (!interactive) {
    throw new Error('Directory and --template are required when not running interactively.');
  }

  if (!options.target) {
    const target = await prompts.text({
      message: 'Where should we create your project?',
      placeholder: 'my-dd-app',
      defaultValue: 'my-dd-app',
      validate(value) {
        if (!value.trim()) return 'Project directory is required.';
      },
    });
    if (prompts.isCancel(target)) return cancelPrompts(prompts);
    options.target = target.trim();
  }

  if (!options.template) {
    const template = await prompts.select({
      message: 'Choose a template',
      options: templateOptions,
      initialValue: TEMPLATE_IDS[0],
    });
    if (prompts.isCancel(template)) return cancelPrompts(prompts);
    options.template = template;
  }

  return options;
}

function spawnInstall(command, args, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      shell: process.platform === 'win32',
      stdio: 'inherit',
    });
    child.on('error', reject);
    child.on('close', resolve);
  });
}

export async function run(argv = process.argv.slice(2), prompts = clack) {
  let options = parseArgs(argv);
  if (options.help || options.version) return;

  prompts.intro('create-dd');
  options = await promptMissing(options, prompts);
  if (!options) return;
  if (!TEMPLATE_IDS.includes(options.template)) {
    throw new Error(`Unknown template "${options.template}". Choose: ${TEMPLATE_IDS.join(', ')}`);
  }

  const packageManager = options.packageManager ?? packageManagerFromUserAgent();
  installCommand(packageManager);
  const created = await scaffold({ target: options.target, template: options.template });
  prompts.log.success(`Created ${created.name} with ${options.template}.`);

  if (options.install) {
    const { command, args } = installCommand(packageManager);
    prompts.log.step(`Installing dependencies with ${command}`);
    let code;
    try {
      code = await spawnInstall(command, args, created.destination);
    } catch (error) {
      prompts.log.error(`Could not start ${command}: ${error.message}`);
      code = 1;
    }
    if (code !== 0) {
      prompts.log.error(`Dependency installation failed (exit ${code}).`);
      prompts.note(
        directoryInstructions(created.destination, `${command} ${args.join(' ')}`),
        'Retry installation',
      );
      prompts.outro(`Project kept at ${created.destination}.`);
      process.exitCode = 1;
      return;
    }
  }

  prompts.note(
    directoryInstructions(created.destination, `${packageManager} run dev`),
    'Next steps',
  );
  prompts.outro('Your dd app is ready.');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  run().catch(error => {
    clack.log.error(error.message);
    process.exitCode = 1;
  });
}
