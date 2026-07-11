import { existsSync } from 'node:fs';
import { cp, lstat, mkdir, mkdtemp, open, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

export const TEMPLATE_IDS = ['react-router', 'react-router-rsc', 'hono'];

export function packageManagerFromUserAgent(userAgent = process.env.npm_config_user_agent ?? '') {
  const agent = userAgent.toLowerCase();
  if (/(^|\s)pnpm\//.test(agent)) return 'pnpm';
  if (/(^|\s)yarn\//.test(agent)) return 'yarn';
  if (/(^|\s)bun\//.test(agent)) return 'bun';
  return 'npm';
}

export function packageNameFor(projectName) {
  const name = projectName.trim().toLowerCase().replace(/[^a-z0-9._-]+/g, '-').replace(/^[._-]+|[._-]+$/g, '');
  return name || 'dd-app';
}

export async function assertUsableTarget(target) {
  try {
    const stat = await lstat(target);
    if (stat.isSymbolicLink()) throw new Error(`Refusing symbolic-link target: ${target}`);
    if (!stat.isDirectory()) throw new Error(`Target exists and is not a directory: ${target}`);
    if ((await readdir(target)).length) throw new Error(`Target directory is not empty: ${target}`);
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
  }
}

async function targetExists(target) {
  try { await lstat(target); return true; }
  catch (error) { if (error?.code === 'ENOENT') return false; throw error; }
}

async function copyTemplate(source, target) {
  for (const entry of await readdir(source)) {
    await cp(path.join(source, entry), path.join(target, entry), { recursive: true, force: false, errorOnExist: true });
  }
}

async function materializeTemplate(source, target) {
  for (const entry of await readdir(source)) {
    // Do not roll back destination entries on failure: another writer can race
    // this copy, and ownership of a colliding path cannot be established.
    await cp(path.join(source, entry), path.join(target, entry), { recursive: true, force: false, errorOnExist: true });
  }
}

async function rewriteJsonName(file, name) {
  if (!existsSync(file)) return;
  const json = JSON.parse(await readFile(file, 'utf8'));
  json.name = name;
  await writeFile(file, `${JSON.stringify(json, null, 2)}\n`);
}

export async function scaffold({ target, template, templatesDir = process.env.CREATE_DD_TEMPLATES_DIR ?? new URL('./templates/', import.meta.url), projectName = path.basename(path.resolve(target)), onLocked, onStaged }) {
  if (!TEMPLATE_IDS.includes(template)) throw new Error(`Unknown template "${template}". Choose: ${TEMPLATE_IDS.join(', ')}`);
  const source = path.resolve(templatesDir instanceof URL ? fileURLToPath(templatesDir) : templatesDir, template);
  if (!existsSync(source)) throw new Error(`Template assets are unavailable: ${template}`);
  const destination = path.resolve(target);
  await assertUsableTarget(destination);
  const destinationExisted = await targetExists(destination);
  const parent = path.dirname(destination);
  await mkdir(parent, { recursive: true });
  const staging = await mkdtemp(path.join(parent, `.${path.basename(destination)}.create-dd-`));
  const name = packageNameFor(projectName);
  let lock;
  let ownsLock = false;
  try {
    await copyTemplate(source, staging);
    await renameGitignore(staging);
    await rewriteJsonName(path.join(staging, 'package.json'), name);
    await rewriteJsonName(path.join(staging, 'dd.json'), name);
    await onStaged?.();

    if (!destinationExisted) {
      // POSIX rename refuses to replace a non-empty directory, so a contender
      // that wins the destination cannot be deleted by a later contender.
      await rename(staging, destination);
    } else {
      await assertUsableTarget(destination);
      try { lock = await open(path.join(destination, '.create-dd.lock'), 'wx'); ownsLock = true; }
      catch (error) {
        if (error?.code === 'EEXIST') throw new Error(`Another create-dd process is already using target: ${destination}`);
        throw error;
      }
      await lock.close(); lock = undefined;
      await onLocked?.();
      await materializeTemplate(staging, destination);
      await rm(path.join(destination, '.create-dd.lock'), { force: true }); ownsLock = false;
    }
  } catch (error) {
    await lock?.close();
    if (ownsLock) await rm(path.join(destination, '.create-dd.lock'), { force: true });
    await rm(staging, { recursive: true, force: true });
    throw error;
  }
  await rm(staging, { recursive: true, force: true });
  return { destination, name };
}

export function installCommand(packageManager) {
  if (!['pnpm', 'npm', 'yarn', 'bun'].includes(packageManager)) throw new Error(`Unsupported package manager: ${packageManager}`);
  return { command: packageManager, args: ['install'] };
}

export async function renameGitignore(root) {
  const source = path.join(root, '_gitignore');
  if (existsSync(source)) await rename(source, path.join(root, '.gitignore'));
}
