import assert from 'node:assert/strict';
import { execFile, spawnSync } from 'node:child_process';
import { access, mkdtemp, mkdir, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { x as extractTar } from 'tar';
import { TEMPLATE_IDS, assertUsableTarget, packageManagerFromUserAgent, scaffold } from '../src/core.js';
import { directoryInstructions, parseArgs, shellEscapePath } from '../src/cli.js';

function runPnpm(args, cwd) {
  const command = process.env.npm_execpath ? process.execPath : process.platform === 'win32' ? 'pnpm.cmd' : 'pnpm';
  const commandArgs = process.env.npm_execpath ? [process.env.npm_execpath, ...args] : args;
  return new Promise((resolve, reject) => execFile(command, commandArgs, { cwd }, error => error ? reject(error) : resolve()));
}

async function fixtureTemplates() {
  const root = await mkdtemp(path.join(tmpdir(), 'create-dd-templates-'));
  for (const id of TEMPLATE_IDS) {
    const dir = path.join(root, id);
    await mkdir(dir);
    await writeFile(path.join(dir, 'package.json'), JSON.stringify({ name: 'template', dependencies: { '@mewhhaha/dd': '1.0.0' } }));
    await writeFile(path.join(dir, 'dd.json'), JSON.stringify({ name: 'template' }));
    await writeFile(path.join(dir, '_gitignore'), 'node_modules\n');
  }
  return root;
}

test('scaffolds each template into a path with spaces and preserves dotfiles', async () => {
  const templatesDir = await fixtureTemplates();
  const root = await mkdtemp(path.join(tmpdir(), 'create dd '));
  for (const id of TEMPLATE_IDS) {
    const target = path.join(root, `${id} app`);
    await scaffold({ target, template: id, templatesDir });
    assert.equal(JSON.parse(await readFile(path.join(target, 'package.json'))).name, `${id}-app`);
    assert.equal(JSON.parse(await readFile(path.join(target, 'dd.json'))).name, `${id}-app`);
    assert.equal(await readFile(path.join(target, '.gitignore'), 'utf8'), 'node_modules\n');
  }
});

test('staging failures leave no partial new project and preserve an empty target', async () => {
  const templatesDir = await fixtureTemplates();
  await writeFile(path.join(templatesDir, 'hono', 'package.json'), '{ invalid json');
  const root = await mkdtemp(path.join(tmpdir(), 'create-dd-stage-'));
  const fresh = path.join(root, 'fresh');
  await assert.rejects(scaffold({ target: fresh, template: 'hono', templatesDir }));
  await assert.rejects(access(fresh));
  const existing = path.join(root, 'existing'); await mkdir(existing);
  await assert.rejects(scaffold({ target: existing, template: 'hono', templatesDir }));
  assert.deepEqual(await readdir(existing), []);
});

test('a concurrent scaffold cannot replace a locked existing empty target', async () => {
  const templatesDir = await fixtureTemplates();
  const root = await mkdtemp(path.join(tmpdir(), 'create-dd-concurrent-'));
  const target = path.join(root, 'existing'); await mkdir(target);
  let release;
  let locked;
  const lockedPromise = new Promise(resolve => { locked = resolve; });
  const first = scaffold({ target, template: 'hono', templatesDir, onLocked: async () => {
    locked(); await new Promise(resolve => { release = resolve; });
  } });
  await lockedPromise;
  await assert.rejects(scaffold({ target, template: 'hono', templatesDir }), /not empty|already using/);
  await access(path.join(target, '.create-dd.lock'));
  release();
  await first;
  assert.equal(JSON.parse(await readFile(path.join(target, 'package.json'))).name, 'existing');
});

test('a colliding external write remains intact when existing-target materialization fails', async () => {
  const templatesDir = await fixtureTemplates();
  const root = await mkdtemp(path.join(tmpdir(), 'create-dd-external-write-'));
  const target = path.join(root, 'existing'); await mkdir(target);
  const external = '{"external":true}\n';
  await assert.rejects(scaffold({ target, template: 'hono', templatesDir, onLocked: async () => {
    await writeFile(path.join(target, 'package.json'), external);
  } }), /exist|EEXIST/i);
  assert.equal(await readFile(path.join(target, 'package.json'), 'utf8'), external);
  await assert.rejects(access(path.join(target, '.create-dd.lock')));
});

test('concurrent nonexistent targets keep the process that won atomic rename', async () => {
  const templatesDir = await fixtureTemplates();
  const root = await mkdtemp(path.join(tmpdir(), 'create-dd-atomic-'));
  const target = path.join(root, 'new-project');
  let release;
  let staged;
  const stagedPromise = new Promise(resolve => { staged = resolve; });
  const delayed = scaffold({ target, template: 'hono', templatesDir, onStaged: async () => {
    staged(); await new Promise(resolve => { release = resolve; });
  } });
  await stagedPromise;
  await scaffold({ target, template: 'hono', templatesDir });
  release();
  await assert.rejects(delayed, /EEXIST|ENOTEMPTY|not empty/);
  assert.equal(JSON.parse(await readFile(path.join(target, 'package.json'))).name, 'new-project');
});

test('refuses nonempty, file, and symbolic-link targets', async () => {
  const root = await mkdtemp(path.join(tmpdir(), 'create-dd-target-'));
  const nonempty = path.join(root, 'nonempty'); await mkdir(nonempty); await writeFile(path.join(nonempty, 'keep'), 'x');
  await assert.rejects(assertUsableTarget(nonempty), /not empty/);
  const file = path.join(root, 'file'); await writeFile(file, 'x');
  await assert.rejects(assertUsableTarget(file), /not a directory/);
  const link = path.join(root, 'link'); await symlink(nonempty, link);
  await assert.rejects(assertUsableTarget(link), /symbolic-link/);
});

test('detects package manager from npm user agent', () => {
  assert.equal(packageManagerFromUserAgent('pnpm/11.0.0 npm/? node/v24'), 'pnpm');
  assert.equal(packageManagerFromUserAgent('yarn/4.0.0 npm/? node/v24'), 'yarn');
  assert.equal(packageManagerFromUserAgent('bun/1.2.0 npm/? node/v24'), 'bun');
  assert.equal(packageManagerFromUserAgent('npm/11.0.0 node/v24'), 'npm');
});

test('rejects missing option values before treating options as values', () => {
  assert.throws(() => parseArgs(['app', '--template', '--no-install']), /Missing value for --template/);
  assert.throws(() => parseArgs(['app', '--package-manager', '-t', 'hono']), /Missing value for --package-manager/);
});

test('shell-escapes literal target paths', () => {
  for (const target of ["plain", "has spaces", "$(not-a-command)", "~glob*?[x]", "quote'and space"]) {
    const escaped = shellEscapePath(target);
    if (process.platform !== 'win32') assert.equal(escaped, `'${target.replaceAll("'", "'\"'\"'")}'`);
  }
  const windows = directoryInstructions('C:\\work\\$(not-a-command) *', 'pnpm run dev', 'win32');
  assert.doesNotMatch(windows, /\bcd\b/i);
  assert.match(windows, /Project directory: /);
  assert.match(windows, /pnpm run dev/);
});

test('pnpm-packed dd-vite manifest has publishable optional runtime dependency', async () => {
  const root = path.resolve(import.meta.dirname, '../../..');
  const temporary = await mkdtemp(path.join(tmpdir(), 'create-dd-vite-pack-'));
  try {
    await runPnpm(['pack', '--pack-destination', temporary], path.join(root, 'packages/dd-vite'));
    const archive = (await readdir(temporary)).find(file => file.endsWith('.tgz'));
    assert.ok(archive, 'pnpm pack should produce one archive');
    const extracted = path.join(temporary, 'extracted'); await mkdir(extracted);
    await extractTar({ file: path.join(temporary, archive), cwd: extracted });
    const manifest = JSON.parse(await readFile(path.join(extracted, 'package/package.json'), 'utf8'));
    assert.doesNotMatch(JSON.stringify(manifest), /workspace:/);
    assert.equal(manifest.optionalDependencies?.['@mewhhaha/dd'], '0.1.0');
  } finally { await rm(temporary, { recursive: true, force: true }); }
});

test('built template assets contain no local protocols or generated artifacts', async () => {
  const templates = path.resolve(import.meta.dirname, '../dist/templates');
  for (const id of TEMPLATE_IDS) {
    const manifest = await readFile(path.join(templates, id, 'package.json'), 'utf8');
    assert.doesNotMatch(manifest, /(?:workspace:|link:|file:)/);
    await access(path.join(templates, id, '_gitignore'));
    await assert.rejects(access(path.join(templates, id, '.gitignore')));
    for (const generated of ['dist', 'node_modules', '.react-router', 'dd-env.d.ts']) {
      await assert.rejects(access(path.join(templates, id, generated)));
    }
  }
});

test('CLI subprocess scaffolds actual built assets without installing', async () => {
  const root = await mkdtemp(path.join(tmpdir(), 'create-dd-cli-'));
  const target = path.join(root, "~a project $(not-a-command) ' *");
  const result = spawnSync(process.execPath, ['dist/cli.js', target, '--template', 'hono', '--no-install'], {
    cwd: path.resolve(import.meta.dirname, '..'), encoding: 'utf8'
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /Created a-project-not-a-command/);
  assert.match(result.stdout, new RegExp(`cd ${shellEscapePath(path.resolve(target)).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`));
  assert.equal(JSON.parse(await readFile(path.join(target, 'package.json'))).name, 'a-project-not-a-command');
  await access(path.join(target, '.gitignore'));
  await assert.rejects(access(path.join(target, '_gitignore')));
});
