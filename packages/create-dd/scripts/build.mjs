import { execFile } from 'node:child_process';
import { chmod, cp, lstat, mkdir, mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { x as extractTar } from 'tar';

const here = path.resolve(import.meta.dirname, '..');
const root = path.resolve(here, '../..');
const dist = path.join(here, 'dist');
const templateIds = ['react-router', 'react-router-rsc', 'hono'];

function run(command, args, cwd) {
  return new Promise((resolve, reject) => {
    const child = execFile(command, args, { cwd }, error => error ? reject(error) : resolve());
    child.stderr?.pipe(process.stderr);
  });
}

function pnpmCommand(args) {
  // `pnpm run build` exposes its JS entrypoint. Running it via Node avoids the
  // Windows .cmd launcher path entirely while retaining an argument array.
  if (process.env.npm_execpath) return { command: process.execPath, args: [process.env.npm_execpath, ...args] };
  return { command: process.platform === 'win32' ? 'pnpm.cmd' : 'pnpm', args };
}

async function walk(dir) {
  const files = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) files.push(...await walk(file));
    else if (entry.isFile()) files.push(file);
  }
  return files;
}

async function validateExpandedTemplate(dir) {
  for (const generated of ['dist', 'node_modules', '.react-router', 'dd-env.d.ts']) {
    try {
      await lstat(path.join(dir, generated));
      throw new Error(`${dir} contains generated top-level artifact: ${generated}`);
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
  }
  for (const file of await walk(dir)) {
    if (path.basename(file) !== 'package.json' && path.basename(file) !== 'dd.json') continue;
    const value = JSON.parse(await readFile(file, 'utf8'));
    const serialized = JSON.stringify(value);
    if (/(?:workspace:|link:|file:)/.test(serialized)) throw new Error(`${file} contains a local dependency protocol`);
    if (typeof value.$schema === 'string' && /^(?:\.\.\/|\.\/)?schema\//.test(value.$schema)) throw new Error(`${file} contains a monorepo-relative schema path`);
  }
}

async function stageTemplate(id, temporary) {
  const source = path.join(root, 'templates', id);
  try { await lstat(source); } catch { throw new Error(`Missing canonical template: ${source}`); }
  const packed = path.join(temporary, 'packed', id);
  await mkdir(packed, { recursive: true });
  const invocation = pnpmCommand(['pack', '--pack-destination', packed]);
  await run(invocation.command, invocation.args, source);
  const archives = (await readdir(packed)).filter(file => file.endsWith('.tgz'));
  if (archives.length !== 1) throw new Error(`Expected one archive from ${id}, found ${archives.length}`);
  const output = path.join(dist, 'templates', id);
  await mkdir(output, { recursive: true });
  await extractTar({ file: path.join(packed, archives[0]), cwd: output, strip: 1 });
  await validateExpandedTemplate(output);
}

await rm(dist, { recursive: true, force: true });
await mkdir(path.join(dist, 'templates'), { recursive: true });
await cp(path.join(here, 'src'), dist, { recursive: true });
await chmod(path.join(dist, 'cli.js'), 0o755);
const temporary = await mkdtemp(path.join(tmpdir(), 'create-dd-'));
try { for (const id of templateIds) await stageTemplate(id, temporary); }
finally { await rm(temporary, { recursive: true, force: true }); }
