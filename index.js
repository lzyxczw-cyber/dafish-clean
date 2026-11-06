/**
 * Mole panel · Host half
 *
 * Owns the local HTTP surface the browser half talks to and runs the local
 * Mole CLI (`mo` / `mole`) as a child process. Every route is same-origin and,
 * for anything that can change the machine, additionally requires the
 * `x-dsh-mole` request header — a header a cross-site page cannot set without
 * a preflight this server never answers.
 *
 * Only two classes of operation exist here:
 *   - reads: status / history / app inventory / disk analysis
 *   - previews: every `--dry-run` surface Mole offers
 * Executing a removal (`clean`, `optimize`, `purge`) is a separate route the
 * panel only calls from an explicit confirmation, and anything that needs an
 * interactive selection list is handed to the user's own Terminal through a
 * generated `.command` file instead of being faked here.
 */
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { chmodSync, existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { isAbsolute, join } from 'node:path'
import { fileURLToPath } from 'node:url'

export const inject = ['webServer']
export const name = 'dsh-mole'

const ROUTE_PREFIX = '/dsh-mole'
const ASSETS_DIR = fileURLToPath(new URL('./assets/', import.meta.url))
const ASSET_NAMES = new Set(['planet-earth.png', 'planet-jupiter.png', 'planet-sun.png', 'mascot.png', 'mascot-line.png'])
const assetCache = new Map()
const HOME = homedir()
const MAX_BODY = 256 * 1024
const MAX_CAPTURE = 4 * 1024 * 1024

const PREVIEWS = {
  clean: ['clean', '--dry-run'],
  optimize: ['optimize', '--dry-run'],
  purge: ['purge', '--dry-run'],
  installer: ['installer', '--dry-run'],
  slim: ['slim', '--dry-run'],
}

const RUNS = {
  clean: ['clean'],
  optimize: ['optimize'],
  purge: ['purge', '--yes'],
}

const TERMINAL_COMMANDS = {
  clean: () => ['mole clean'],
  optimize: () => ['mole optimize'],
  purge: () => ['mole purge'],
  installer: () => ['mole installer'],
  slim: () => ['mole slim'],
  'clean-whitelist': () => ['mole clean --whitelist'],
  'optimize-whitelist': () => ['mole optimize --whitelist'],
  'purge-paths': () => ['mole purge --paths'],
  history: () => ['mole history'],
  'install-mole': () => ['brew install mole'],
  uninstall: (name) => [`mole uninstall ${shellQuote(name)}`],
  'uninstall-data': (name) => [`mole uninstall --with-data ${shellQuote(name)}`],
  'uninstall-dry': (name) => [`mole uninstall --dry-run ${shellQuote(name)}`],
}

let binaryCache
let versionCache
let statusCache = { at: 0, payload: null }
let appsCache = { at: 0, payload: null }
let busyKinds = new Set()

function binaryCandidates() {
  const list = []
  if (process.env.MOLE_BIN) list.push(process.env.MOLE_BIN)
  list.push(
    '/opt/homebrew/bin/mole',
    '/usr/local/bin/mole',
    join(HOME, '.local/bin/mole'),
    join(HOME, '.local/bin/mo'),
    '/opt/homebrew/bin/mo',
    '/usr/local/bin/mo',
  )
  const pathEntries = (process.env.PATH ?? '').split(':').filter(Boolean)
  for (const dir of pathEntries) {
    list.push(join(dir, 'mole'), join(dir, 'mo'))
  }
  return list
}

function resolveMole() {
  if (binaryCache !== undefined && existsSync(binaryCache)) return binaryCache
  binaryCache = undefined
  const seen = new Set()
  for (const candidate of binaryCandidates()) {
    if (!candidate || seen.has(candidate)) continue
    seen.add(candidate)
    try {
      if (existsSync(candidate) && statSync(candidate).isFile()) {
        binaryCache = candidate
        return candidate
      }
    } catch {
      // ignore an unreadable candidate and keep looking
    }
  }
  return undefined
}

function childEnvironment() {
  const parts = ['/opt/homebrew/bin', '/usr/local/bin', join(HOME, '.local/bin')]
  if (process.env.PATH) parts.push(process.env.PATH)
  else parts.push('/usr/bin:/bin:/usr/sbin:/sbin')
  return { ...process.env, PATH: parts.join(':'), TERM: 'dumb', NO_COLOR: '1', CLICOLOR: '0' }
}

function spawnMoleStream(args, options = {}) {
  const timeoutMs = options.timeoutMs ?? 120_000
  const onLine = typeof options.onLine === 'function' ? options.onLine : null
  const binary = resolveMole()
  if (binary === undefined) {
    return Promise.resolve({
      code: 'mole-missing',
      ok: false,
      exitCode: null,
      stdout: '',
      stderr: '',
      timedOut: false,
      durationMs: 0,
      command: `mole ${args.join(' ')}`,
    })
  }
  const startedAt = Date.now()
  return new Promise((resolve) => {
    let stdout = ''
    let stderr = ''
    let pendingOut = ''
    let pendingErr = ''
    let timedOut = false
    let settled = false
    const emitLine = (line, stream) => {
      if (!onLine) return
      const clean = line.replace(/\r$/, '')
      if (!clean.trim()) return
      try {
        onLine(clean, stream)
      } catch {
        // a progress consumer must never break the command
      }
    }
    const flush = (buffer, stream) => {
      let index = buffer.indexOf('\n')
      let rest = buffer
      while (index >= 0) {
        emitLine(rest.slice(0, index), stream)
        rest = rest.slice(index + 1)
        index = rest.indexOf('\n')
      }
      return rest
    }
    const finish = (exitCode) => {
      if (settled) return
      settled = true
      if (pendingOut) emitLine(pendingOut, 'stdout')
      if (pendingErr) emitLine(pendingErr, 'stderr')
      resolve({
        ok: exitCode === 0 && !timedOut,
        code: timedOut ? 'timeout' : exitCode === 0 ? 'ok' : 'failed',
        exitCode,
        stdout: truncate(stdout),
        stderr: truncate(stderr),
        timedOut,
        durationMs: Date.now() - startedAt,
        command: `mole ${args.join(' ')}`,
      })
    }
    let child
    try {
      child = spawn(binary, args, {
        cwd: HOME,
        env: childEnvironment(),
        stdio: ['ignore', 'pipe', 'pipe'],
      })
    } catch (error) {
      stderr += String(error?.message ?? error)
      finish(1)
      return
    }
    const timer = setTimeout(() => {
      timedOut = true
      try {
        child.kill('SIGKILL')
      } catch {
        // the process already exited
      }
    }, timeoutMs)
    child.stdout?.on('data', (chunk) => {
      const text = chunk.toString('utf8')
      if (stdout.length < MAX_CAPTURE) stdout += text
      if (onLine) pendingOut = flush(pendingOut + text, 'stdout')
    })
    child.stderr?.on('data', (chunk) => {
      const text = chunk.toString('utf8')
      if (stderr.length < MAX_CAPTURE) stderr += text
      if (onLine) pendingErr = flush(pendingErr + text, 'stderr')
    })
    child.on('error', (error) => {
      clearTimeout(timer)
      stderr += String(error?.message ?? error)
      finish(1)
    })
    child.on('close', (code) => {
      clearTimeout(timer)
      finish(typeof code === 'number' ? code : 1)
    })
  })
}

function runMole(args, options = {}) {
  return spawnMoleStream(args, options)
}

function spawnBrewStream(args, options = {}) {
  const timeoutMs = options.timeoutMs ?? 900_000
  const onLine = typeof options.onLine === 'function' ? options.onLine : null
  const binary = detectBrew()
  if (binary === null) {
    return Promise.resolve({
      code: 'brew-missing',
      ok: false,
      exitCode: null,
      stdout: '',
      stderr: '',
      timedOut: false,
      durationMs: 0,
      command: `brew ${args.join(' ')}`,
    })
  }
  const startedAt = Date.now()
  return new Promise((resolve) => {
    let stdout = ''
    let stderr = ''
    let pendingOut = ''
    let pendingErr = ''
    let timedOut = false
    let settled = false
    const emitLine = (line, stream) => {
      if (!onLine) return
      const clean = line.replace(/\r$/, '')
      if (!clean.trim()) return
      try {
        onLine(clean, stream)
      } catch {
        // a progress consumer must never break the command
      }
    }
    const flush = (buffer, stream) => {
      let index = buffer.indexOf('\n')
      let rest = buffer
      while (index >= 0) {
        emitLine(rest.slice(0, index), stream)
        rest = rest.slice(index + 1)
        index = rest.indexOf('\n')
      }
      return rest
    }
    const finish = (exitCode) => {
      if (settled) return
      settled = true
      if (pendingOut) emitLine(pendingOut, 'stdout')
      if (pendingErr) emitLine(pendingErr, 'stderr')
      resolve({
        ok: exitCode === 0 && !timedOut,
        code: timedOut ? 'timeout' : exitCode === 0 ? 'ok' : 'failed',
        exitCode,
        stdout: truncate(stdout),
        stderr: truncate(stderr),
        timedOut,
        durationMs: Date.now() - startedAt,
        command: `brew ${args.join(' ')}`,
      })
    }
    let child
    try {
      child = spawn(binary, args, { cwd: HOME, env: childEnvironment(), stdio: ['ignore', 'pipe', 'pipe'] })
    } catch (error) {
      stderr += String(error?.message ?? error)
      finish(1)
      return
    }
    const timer = setTimeout(() => {
      timedOut = true
      try {
        child.kill('SIGKILL')
      } catch {
        // already exited
      }
    }, timeoutMs)
    child.stdout?.on('data', (chunk) => {
      const text = chunk.toString('utf8')
      if (stdout.length < MAX_CAPTURE) stdout += text
      if (onLine) pendingOut = flush(pendingOut + text, 'stdout')
    })
    child.stderr?.on('data', (chunk) => {
      const text = chunk.toString('utf8')
      if (stderr.length < MAX_CAPTURE) stderr += text
      if (onLine) pendingErr = flush(pendingErr + text, 'stderr')
    })
    child.on('error', (error) => {
      clearTimeout(timer)
      stderr += String(error?.message ?? error)
      finish(1)
    })
    child.on('close', (code) => {
      clearTimeout(timer)
      finish(typeof code === 'number' ? code : 1)
    })
  })
}

// ---------------------------------------------------------------------
// Background jobs with live progress
// ---------------------------------------------------------------------
const jobs = new Map()
const MAX_JOBS = 24
const JOB_TTL_MS = 10 * 60 * 1000
const SIZE_UNITS = { B: 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3, TB: 1024 ** 4 }
const PROGRESS_SIZE_RE = /(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)\b/i

function createPhase() {
  return { group: '', item: '', meta: '', groups: 0, items: 0, bytes: 0, line: '' }
}

/** Turn one line of Mole output into a coarse but continuously useful progress state. */
function applyProgressLine(line, phase) {
  const trimmed = String(line ?? '').trim()
  if (!trimmed) return
  phase.line = trimmed.slice(0, 320)
  const group = /^[➤>]\s*(.*)$/.exec(trimmed)
  if (group) {
    phase.group = group[1].slice(0, 160)
    phase.item = ''
    phase.groups += 1
    return
  }
  const item = /^([○✓◎⊙•])\s*(.*)$/.exec(trimmed)
  if (item) {
    phase.items += 1
    phase.item = item[2].split(' · ')[0].slice(0, 160)
    const size = PROGRESS_SIZE_RE.exec(item[2])
    if (size) phase.bytes += Number(size[1]) * (SIZE_UNITS[size[2].toUpperCase()] ?? 1)
    return
  }
  if (/^⚙/.test(trimmed)) {
    phase.meta = trimmed.replace(/^⚙\s*/, '').slice(0, 200)
    return
  }
  if (phase.item === '') phase.item = trimmed.slice(0, 160)
}

function pruneJobs(now = Date.now()) {
  for (const [id, job] of jobs) {
    const ended = job.endedMs ?? job.startedMs
    if (now - ended > JOB_TTL_MS) jobs.delete(id)
  }
  if (jobs.size <= MAX_JOBS) return
  const ordered = [...jobs.values()].sort((left, right) => left.startedMs - right.startedMs)
  while (ordered.length > MAX_JOBS) {
    const victim = ordered.shift()
    jobs.delete(victim.id)
  }
}

function jobErrorText(out) {
  if (out.code === 'mole-missing') return '未找到 mole 命令，请先安装 / mole is not installed (brew install mole)'
  if (out.timedOut) return '命令超时 / the command timed out'
  return `命令退出码 ${out.exitCode} / command exited with ${out.exitCode}`
}

async function finalizeJob(job, out) {
  const base = {
    ok: out.ok,
    code: out.code,
    exitCode: out.exitCode,
    output: out.stdout + (out.stderr ? `\n${out.stderr}` : ''),
    stdout: out.stdout,
    stderr: out.stderr,
    durationMs: out.durationMs,
  }
  if (job.action === 'preview') {
    return {
      ...base,
      kind: job.kind,
      cleanList: job.kind === 'clean' ? readCleanList() : null,
      version: await moleVersion(),
    }
  }
  if (job.action === 'install-mole') {
    if (out.ok) {
      versionCache = undefined
      statusCache = { at: 0, payload: null }
    }
    return { ...base, kind: 'install-mole', mole: await moleVersion(true) }
  }
  if (job.action === 'run') {
    const historyResult = await runMole(['history', '--json', '--limit', '10'], { timeoutMs: 30_000 })
    const history = jsonOrRaw(historyResult.stdout).parsed
    return { ...base, kind: job.kind, history, version: await moleVersion() }
  }
  const parsed = jsonOrRaw(out.stdout).parsed
  return {
    ...base,
    report: parsed,
    path: job.target,
    raw: parsed === null ? truncate(out.stdout).slice(0, 20_000) : undefined,
    reason: parsed === null ? 'unparsable' : undefined,
    stderr: out.stderr.trim() || undefined,
  }
}

function planJob(request) {
  const action = String(request.action ?? '')
  if (action === 'preview') {
    const kind = String(request.kind ?? '')
    const args = PREVIEWS[kind]
    if (args === undefined) throw Object.assign(new Error(`未知预览类型 / unknown preview kind: ${kind}`), { status: 400 })
    return { action, kind, args, timeoutMs: 600_000, busyKey: `preview:${kind}` }
  }
  if (action === 'run') {
    const kind = String(request.kind ?? '')
    const args = RUNS[kind]
    if (args === undefined) {
      throw Object.assign(new Error('这个操作需要用户在终端里完成 / this action needs the user terminal'), { status: 400 })
    }
    if (request.confirm !== 'Mole') throw Object.assign(new Error('缺少确认 / explicit confirmation is required'), { status: 400 })
    return { action, kind, args, timeoutMs: 900_000, busyKey: 'run' }
  }
  if (action === 'install-mole') {
    if (detectBrew() === null) {
      throw Object.assign(new Error('未检测到 Homebrew，请按页面提示在终端安装 / Homebrew was not found'), { status: 400 })
    }
    return { action, kind: 'install-mole', args: ['install', 'mole'], timeoutMs: 900_000, busyKey: 'install-mole', executor: 'brew' }
  }
  if (action === 'analyze') {
    const requested = String(request.path ?? '')
    const target = requested && isAbsolute(requested) ? requested : HOME
    let isDirectory = false
    try {
      isDirectory = statSync(target).isDirectory()
    } catch {
      isDirectory = false
    }
    if (!isDirectory) throw Object.assign(new Error(`路径不存在 / path does not exist: ${target}`), { status: 400 })
    return { action, kind: 'analyze', target, args: ['analyze', '--json', target], timeoutMs: 600_000, busyKey: 'analyze' }
  }
  throw Object.assign(new Error(`未知动作 / unknown action: ${action}`), { status: 400 })
}

function startJob(request) {
  const plan = planJob(request)
  if (busyKinds.has(plan.busyKey)) {
    throw Object.assign(new Error('已有同类任务在运行，请稍候 / a similar job is already running'), { status: 409 })
  }
  busyKinds.add(plan.busyKey)
  const job = {
    id: randomUUID(),
    action: plan.action,
    kind: plan.kind,
    target: plan.target ?? null,
    state: 'running',
    startedAt: new Date().toISOString(),
    startedMs: Date.now(),
    endedMs: null,
    phase: createPhase(),
    lines: [],
    error: null,
    result: null,
  }
  jobs.set(job.id, job)
  pruneJobs()
  const pushLine = (line) => {
    applyProgressLine(line, job.phase)
    job.lines.push(line.slice(0, 500))
    if (job.lines.length > 240) job.lines.splice(0, job.lines.length - 240)
  }
  const runner = plan.executor === 'brew' ? spawnBrewStream(plan.args, { timeoutMs: plan.timeoutMs, onLine: pushLine }) : spawnMoleStream(plan.args, { timeoutMs: plan.timeoutMs, onLine: pushLine })
  runner
    .then(async (out) => {
      let result = null
      let failure = null
      try {
        result = await finalizeJob(job, out)
      } catch (error) {
        failure = String(error?.message ?? error)
      }
      const benignPreview = job.action === 'preview' || job.action === 'analyze'
      const failed = out.code === 'mole-missing' || out.timedOut || (!benignPreview && out.exitCode !== 0) || (job.action === 'analyze' && result !== null && result.report === null && out.exitCode !== 0)
      job.endedMs = Date.now()
      job.result = result
      if (failed) {
        job.state = 'error'
        job.error = jobErrorText(out)
        if (failure) job.error = failure
      } else {
        job.state = 'done'
        job.phase.line = '完成'
        job.phase.item = ''
      }
      busyKinds.delete(plan.busyKey)
      if (job.action === 'run') {
        statusCache = { at: 0, payload: null }
        appsCache = { at: 0, payload: null }
      }
    })
    .catch((error) => {
      job.endedMs = Date.now()
      job.state = 'error'
      job.error = String(error?.message ?? error)
      busyKinds.delete(plan.busyKey)
    })
  return job
}

function jobView(job, includeResult = true) {
  return {
    id: job.id,
    action: job.action,
    kind: job.kind,
    target: job.target,
    state: job.state,
    startedAt: job.startedAt,
    startedMs: job.startedMs,
    endedMs: job.endedMs,
    durationMs: (job.endedMs ?? Date.now()) - job.startedMs,
    phase: job.phase,
    lines: job.lines.slice(-120),
    error: job.error,
    result: includeResult && job.state !== 'running' ? job.result : undefined,
  }
}

function stripAnsi(text) {
  // OSC and CSI sequences; the CLI disables color when piped, but never trust it.
  return String(text ?? '')
    .replace(/\u001b\][^\u0007\u001b]*(?:\u0007|\u001b\\)/g, '')
    .replace(/\u001b\[[0-9;?]*[ -/]*[@-~]/g, '')
    .replace(/\r/g, '')
}

function truncate(text) {
  if (typeof text !== 'string') return ''
  const clean = stripAnsi(text)
  if (clean.length <= MAX_CAPTURE) return clean
  return `${clean.slice(0, MAX_CAPTURE)}\n… output truncated (${clean.length} chars)`
}

function shellQuote(value) {
  const text = String(value ?? '')
  if (!/^[\w@%+=:,./ -]+$/.test(text)) return `'${text.replace(/'/g, `'\\''`)}'`
  return `"${text.replace(/(["\\$`])/g, '\\$1')}"`
}

function sendJson(response, status, payload) {
  response.writeHead(status, {
    'cache-control': 'no-store',
    'content-type': 'application/json; charset=utf-8',
  })
  response.end(JSON.stringify(payload))
}

function failed(response, status, code, message) {
  sendJson(response, status, { ok: false, error: { code, message } })
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    let size = 0
    const chunks = []
    request.on('data', (chunk) => {
      size += chunk.length
      if (size > MAX_BODY) {
        reject(new Error('请求体过大 / request body too large'))
        request.destroy()
        return
      }
      chunks.push(chunk)
    })
    request.on('end', () => {
      const text = Buffer.concat(chunks).toString('utf8')
      if (!text.trim()) return resolve({})
      try {
        const parsed = JSON.parse(text)
        resolve(parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {})
      } catch {
        reject(new Error('请求体不是合法 JSON / request body is not valid JSON'))
      }
    })
    request.on('error', reject)
  })
}

function isLoopbackHostname(hostname) {
  const value = String(hostname ?? '').toLowerCase()
  return value === 'localhost' || value === '127.0.0.1' || value === '::1' || value === '[::1]' || value.endsWith('.localhost')
}

function authorityHostname(authority) {
  const text = String(authority ?? '').toLowerCase()
  if (text.startsWith('[')) {
    const end = text.indexOf(']')
    return end === -1 ? '' : text.slice(1, end)
  }
  const at = text.lastIndexOf(':')
  if (at > 0 && /^\d+$/.test(text.slice(at + 1))) return text.slice(0, at)
  return text
}

/**
 * Cross-site pages cannot read the response. A present HTTP(S) Origin must be
 * loopback; an absent one means the caller is not a page (the Desktop proxy
 * strips it), and a custom-scheme Origin comes from the app shell. Mutating
 * routes are additionally gated on a header a cross-site page cannot set.
 */
function sameOrigin(request) {
  if (request.headers['sec-fetch-site'] === 'cross-site') return false
  const host = request.headers.host
  if (host !== undefined && !isLoopbackHostname(authorityHostname(host))) return false
  const origin = request.headers.origin
  if (origin === undefined || origin === 'null') return true
  try {
    const originUrl = new URL(origin)
    if (originUrl.protocol !== 'http:' && originUrl.protocol !== 'https:') return true
    return isLoopbackHostname(originUrl.hostname)
  } catch {
    return true
  }
}

function assertPost(request, response) {
  if (request.method !== 'POST') {
    failed(response, 405, 'method', '只支持 POST / POST only')
    return false
  }
  if (!sameOrigin(request)) {
    failed(response, 403, 'origin', '请求来源不可信 / untrusted request origin')
    return false
  }
  if (request.headers['x-dsh-mole'] !== '1') {
    failed(response, 403, 'header', '缺少面板请求头 / missing panel request header')
    return false
  }
  return true
}

function jsonOrRaw(text) {
  const trimmed = (text ?? '').trim()
  if (!trimmed) return { parsed: null }
  try {
    return { parsed: JSON.parse(trimmed) }
  } catch {
    const firstBrace = trimmed.indexOf('{')
    const firstBracket = trimmed.indexOf('[')
    const start = [firstBrace, firstBracket].filter((index) => index >= 0).sort((a, b) => a - b)[0]
    if (start !== undefined) {
      try {
        return { parsed: JSON.parse(trimmed.slice(start)) }
      } catch {
        // fall through to the raw text
      }
    }
    return { parsed: null }
  }
}

function detectBrew() {
  for (const candidate of ['/opt/homebrew/bin/brew', '/usr/local/bin/brew', join(HOME, '.local/bin/brew')]) {
    try {
      if (existsSync(candidate)) return candidate
    } catch {
      // keep looking
    }
  }
  return null
}

async function moleVersion(refresh = false) {
  if (!refresh && versionCache !== undefined) return versionCache
  const result = await runMole(['--version'], { timeoutMs: 20_000 })
  const platform = process.platform
  const brew = detectBrew()
  const install = {
    brew: 'brew install mole',
    script: 'curl -fsSL https://raw.githubusercontent.com/tw93/Mole/main/install.sh | bash',
    docs: 'https://github.com/tw93/Mole#quick-start',
  }
  if (result.code === 'mole-missing') {
    versionCache = { available: false, path: null, version: null, detail: '未找到 mole 命令', platform, brew, install }
    return versionCache
  }
  const match = /Mole version ([^\s]+)/.exec(result.stdout)
  versionCache = {
    available: true,
    path: resolveMole() ?? null,
    version: match?.[1] ?? null,
    detail: result.stdout.trim().split('\n')[0] ?? '',
    platform,
    brew,
    install,
  }
  return versionCache
}

function cleanListPath() {
  return join(HOME, '.config/mole/clean-list.txt')
}

function readCleanList() {
  try {
    const file = cleanListPath()
    if (!existsSync(file)) return null
    const text = readFileSync(file, 'utf8')
    return { path: file, text: text.slice(0, 400_000), lines: text.split('\n').length }
  } catch {
    return null
  }
}

function writeTerminalScript(lines) {
  const dir = join(tmpdir(), 'dsh-mole')
  mkdirSync(dir, { recursive: true })
  const file = join(dir, `mole-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.command`)
  const script = [
    '#!/bin/zsh',
    'clear',
    'echo "Mole · 由 DeepSeek Harness 启动 / started from DeepSeek Harness"',
    'echo',
    ...lines,
    'status=$?',
    'echo',
    'printf "按回车关闭此窗口 / Press Return to close: "',
    'read -r _',
    'exit $status',
    '',
  ].join('\n')
  writeFileSync(file, script, { mode: 0o755 })
  chmodSync(file, 0o755)
  return file
}

function openInTerminal(lines) {
  return new Promise((resolve) => {
    const file = writeTerminalScript(lines)
    const child = spawn('open', ['-a', 'Terminal', file], { stdio: 'ignore', detached: true })
    child.on('error', (error) => resolve({ ok: false, error: String(error?.message ?? error), file }))
    child.on('spawn', () => {
      child.unref()
      resolve({ ok: true, file })
    })
  })
}

function openInFinder(target) {
  return new Promise((resolve) => {
    const child = spawn('open', ['-R', target], { stdio: 'ignore', detached: true })
    child.on('error', (error) => resolve({ ok: false, error: String(error?.message ?? error) }))
    child.on('spawn', () => {
      child.unref()
      resolve({ ok: true })
    })
  })
}

async function handleStatus(response) {
  const now = Date.now()
  if (statusCache.payload && now - statusCache.at < 1_000) {
    sendJson(response, 200, statusCache.payload)
    return
  }
  const [version, result] = await Promise.all([moleVersion(), runMole(['status', '--json'], { timeoutMs: 45_000 })])
  if (result.code === 'mole-missing') {
    sendJson(response, 200, { ok: true, mole: version, status: null, reason: 'mole-missing' })
    return
  }
  const { parsed } = jsonOrRaw(result.stdout)
  const payload = {
    ok: true,
    mole: version,
    status: parsed,
    collectedAt: new Date().toISOString(),
    durationMs: result.durationMs,
    stderr: result.stderr.trim() || undefined,
    reason: parsed === null ? 'unparsable' : undefined,
  }
  statusCache = { at: now, payload }
  sendJson(response, 200, payload)
}

async function handleApps(response, force) {
  const now = Date.now()
  if (!force && appsCache.payload && now - appsCache.at < 60_000) {
    sendJson(response, 200, appsCache.payload)
    return
  }
  const result = await runMole(['uninstall', '--list'], { timeoutMs: 180_000 })
  if (result.code === 'mole-missing') {
    sendJson(response, 200, { ok: true, apps: [], reason: 'mole-missing' })
    return
  }
  const { parsed } = jsonOrRaw(result.stdout)
  const payload = {
    ok: true,
    apps: Array.isArray(parsed) ? parsed : [],
    reason: Array.isArray(parsed) ? undefined : 'unparsable',
    raw: Array.isArray(parsed) ? undefined : truncate(result.stdout).slice(0, 20_000),
    durationMs: result.durationMs,
  }
  appsCache = { at: now, payload }
  sendJson(response, 200, payload)
}

async function handleHistory(response, url) {
  const limitParam = Number(url.searchParams.get('limit') ?? '40')
  const limit = Number.isFinite(limitParam) ? Math.min(200, Math.max(1, Math.round(limitParam))) : 40
  const result = await runMole(['history', '--json', '--limit', String(limit)], { timeoutMs: 30_000 })
  const { parsed } = jsonOrRaw(result.stdout)
  sendJson(response, 200, {
    ok: true,
    history: parsed,
    raw: parsed === null ? truncate(result.stdout).slice(0, 20_000) : undefined,
    durationMs: result.durationMs,
  })
}

async function handleAnalyze(response, url) {
  const requested = url.searchParams.get('path')
  const target = requested && isAbsolute(requested) ? requested : HOME
  try {
    if (!statSync(target).isDirectory()) {
      failed(response, 400, 'not-a-directory', `不是文件夹 / not a directory: ${target}`)
      return
    }
  } catch {
    failed(response, 400, 'missing-path', `路径不存在 / path does not exist: ${target}`)
    return
  }
  const result = await runMole(['analyze', '--json', target], { timeoutMs: 600_000 })
  if (result.code === 'mole-missing') {
    sendJson(response, 200, { ok: true, report: null, reason: 'mole-missing' })
    return
  }
  const { parsed } = jsonOrRaw(result.stdout)
  sendJson(response, 200, {
    ok: true,
    report: parsed,
    path: target,
    durationMs: result.durationMs,
    raw: parsed === null ? truncate(result.stdout).slice(0, 20_000) : undefined,
    stderr: result.stderr.trim() || undefined,
  })
}

async function handlePreview(request, response) {
  let body
  try {
    body = await readBody(request)
  } catch (error) {
    failed(response, 400, 'body', String(error?.message ?? error))
    return
  }
  const kind = String(body.kind ?? '')
  const args = PREVIEWS[kind]
  if (args === undefined) {
    failed(response, 400, 'unknown-kind', `未知预览类型 / unknown preview kind: ${kind}`)
    return
  }
  if (busyKinds.has('run')) {
    failed(response, 409, 'busy', '清理正在执行，请稍候 / a cleanup is running')
    return
  }
  if (busyKinds.has(`preview:${kind}`)) {
    failed(response, 409, 'busy', '这个预览已经在扫描 / this preview is already running')
    return
  }
  busyKinds.add(`preview:${kind}`)
  try {
    const result = await runMole(args, { timeoutMs: 600_000 })
    sendJson(response, 200, {
      ok: result.ok,
      kind,
      code: result.code,
      exitCode: result.exitCode,
      output: result.stdout + (result.stderr ? `\n${result.stderr}` : ''),
      stdout: result.stdout,
      stderr: result.stderr,
      durationMs: result.durationMs,
      cleanList: kind === 'clean' ? readCleanList() : null,
      version: await moleVersion(),
    })
  } finally {
    busyKinds.delete(`preview:${kind}`)
  }
}

async function handleRun(request, response) {
  let body
  try {
    body = await readBody(request)
  } catch (error) {
    failed(response, 400, 'body', String(error?.message ?? error))
    return
  }
  const kind = String(body.kind ?? '')
  const args = RUNS[kind]
  if (args === undefined) {
    failed(response, 400, 'unknown-kind', `这个操作需要用户在终端里完成 / this action needs the user's terminal: ${kind}`)
    return
  }
  if (body.confirm !== 'Mole') {
    failed(response, 400, 'confirm', '缺少确认 / explicit confirmation is required')
    return
  }
  if (busyKinds.has('run')) {
    failed(response, 409, 'busy', '已有清理正在执行 / another cleanup is already running')
    return
  }
  busyKinds.add('run')
  try {
    const result = await runMole(args, { timeoutMs: 900_000 })
    statusCache = { at: 0, payload: null }
    appsCache = { at: 0, payload: null }
    const history = await runMole(['history', '--json', '--limit', '10'], { timeoutMs: 30_000 })
    const historyJson = jsonOrRaw(history.stdout).parsed
    sendJson(response, 200, {
      ok: result.ok,
      kind,
      code: result.code,
      exitCode: result.exitCode,
      output: result.stdout + (result.stderr ? `\n${result.stderr}` : ''),
      stdout: result.stdout,
      stderr: result.stderr,
      durationMs: result.durationMs,
      history: historyJson,
      version: await moleVersion(),
    })
  } finally {
    busyKinds.delete('run')
  }
}

async function handleUninstallPlan(request, response) {
  let body
  try {
    body = await readBody(request)
  } catch (error) {
    failed(response, 400, 'body', String(error?.message ?? error))
    return
  }
  const name = String(body.name ?? '').trim()
  if (!name || name.length > 200) {
    failed(response, 400, 'bad-name', '应用名无效 / invalid app name')
    return
  }
  const result = await runMole(['uninstall', '--dry-run', name], { timeoutMs: 180_000 })
  sendJson(response, 200, {
    ok: result.ok,
    name,
    code: result.code,
    output: result.stdout + (result.stderr ? `\n${result.stderr}` : ''),
    stdout: result.stdout,
    stderr: result.stderr,
    durationMs: result.durationMs,
  })
}

async function handleOpenTerminal(request, response) {
  let body
  try {
    body = await readBody(request)
  } catch (error) {
    failed(response, 400, 'body', String(error?.message ?? error))
    return
  }
  const action = String(body.action ?? '')
  const builder = TERMINAL_COMMANDS[action]
  if (builder === undefined) {
    failed(response, 400, 'unknown-action', `未知动作 / unknown action: ${action}`)
    return
  }
  const name = String(body.name ?? '').trim()
  if ((action === 'uninstall' || action === 'uninstall-data' || action === 'uninstall-dry') && !name) {
    failed(response, 400, 'bad-name', '缺少应用名 / missing app name')
    return
  }
  if (name.length > 200) {
    failed(response, 400, 'bad-name', '应用名过长 / app name too long')
    return
  }
  const lines = builder(name)
  const launched = await openInTerminal(lines)
  if (!launched.ok) {
    failed(response, 500, 'spawn', launched.error ?? '无法打开终端 / could not open Terminal')
    return
  }
  sendJson(response, 200, { ok: true, file: launched.file, command: lines.join(' && ') })
}

async function handleOpenPath(request, response) {
  let body
  try {
    body = await readBody(request)
  } catch (error) {
    failed(response, 400, 'body', String(error?.message ?? error))
    return
  }
  const target = String(body.path ?? '')
  if (!isAbsolute(target) || !existsSync(target)) {
    failed(response, 400, 'bad-path', '路径不存在 / path does not exist')
    return
  }
  const opened = await openInFinder(target)
  if (!opened.ok) {
    failed(response, 500, 'spawn', opened.error ?? '无法打开 Finder / could not open Finder')
    return
  }
  sendJson(response, 200, { ok: true })
}

function serveAsset(request, response, name) {
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    failed(response, 405, 'method', '只支持 GET / GET only')
    return
  }
  if (!ASSET_NAMES.has(name)) {
    failed(response, 404, 'not-found', '资源不存在 / asset not found')
    return
  }
  let bytes = assetCache.get(name)
  if (bytes === undefined) {
    try {
      bytes = readFileSync(join(ASSETS_DIR, name))
    } catch {
      failed(response, 404, 'not-found', '资源不存在 / asset not found')
      return
    }
    assetCache.set(name, bytes)
  }
  response.writeHead(200, {
    'content-type': 'image/png',
    'content-length': bytes.length,
    'cache-control': 'public, max-age=3600',
    'x-content-type-options': 'nosniff',
  })
  response.end(request.method === 'HEAD' ? undefined : bytes)
}

async function handle(request, response) {
  try {
    const url = new URL(request.url ?? '/', 'http://mole.local')
    const pathname = url.pathname.startsWith(ROUTE_PREFIX) ? url.pathname.slice(ROUTE_PREFIX.length) || '/' : url.pathname
    if (pathname.startsWith('/assets/')) {
      serveAsset(request, response, pathname.slice('/assets/'.length))
      return
    }
    switch (pathname) {
      case '/version':
      case '/ping': {
        const version = await moleVersion(url.searchParams.get('refresh') === '1')
        sendJson(response, 200, { ok: true, mole: version })
        return
      }
      case '/status': {
        await handleStatus(response)
        return
      }
      case '/history': {
        await handleHistory(response, url)
        return
      }
      case '/apps': {
        await handleApps(response, url.searchParams.get('refresh') === '1')
        return
      }
      case '/analyze': {
        await handleAnalyze(response, url)
        return
      }
      case '/clean-list': {
        sendJson(response, 200, { ok: true, cleanList: readCleanList() })
        return
      }
      case '/preview': {
        await handlePreview(request, response)
        return
      }
      case '/run': {
        await handleRun(request, response)
        return
      }
      case '/job/start': {
        if (!assertPost(request, response)) return
        let body
        try {
          body = await readBody(request)
        } catch (error) {
          failed(response, 400, 'body', String(error?.message ?? error))
          return
        }
        try {
          const job = startJob(body)
          sendJson(response, 200, { ok: true, jobId: job.id, job: jobView(job, false) })
        } catch (error) {
          failed(response, Number(error?.status) || 500, 'job', String(error?.message ?? error))
        }
        return
      }
      case '/job/status': {
        const id = url.searchParams.get('id')
        if (!id) {
          failed(response, 400, 'missing-id', '缺少任务 id / missing job id')
          return
        }
        const job = jobs.get(id)
        if (job === undefined) {
          failed(response, 404, 'not-found', '任务不存在或已过期 / job not found or expired')
          return
        }
        sendJson(response, 200, { ok: true, job: jobView(job) })
        return
      }
      case '/uninstall-plan': {
        if (!assertPost(request, response)) return
        await handleUninstallPlan(request, response)
        return
      }
      case '/open-terminal': {
        if (!assertPost(request, response)) return
        await handleOpenTerminal(request, response)
        return
      }
      case '/open-path': {
        if (!assertPost(request, response)) return
        await handleOpenPath(request, response)
        return
      }
      default: {
        failed(response, 404, 'not-found', `未知接口 / unknown endpoint: ${pathname}`)
      }
    }
  } catch (error) {
    try {
      failed(response, 500, 'internal', String(error?.message ?? error))
    } catch {
      // the response may already be sent
    }
  }
}

export function apply(ctx) {
  if (ctx.webServer === undefined) return
  ctx.effect(
    () =>
      ctx.webServer.register({
        kind: 'prefix',
        path: ROUTE_PREFIX,
        handler: (request, response) => handle(request, response),
      }),
    'dsh-mole: panel routes',
  )
}
