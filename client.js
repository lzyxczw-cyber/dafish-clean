/**
 * Mole panel · Client half (browser)
 *
 * Renders as a main-column panel opened from the sidebar rail. Plain
 * JavaScript: React arrives from the browser module table, there is no JSX and
 * no bundler. All visible text goes through the Client locale service.
 */
window.__ModuleLoader__.load({
  id: '@local/dsh-mole',
  factory(require) {
    const React = require('react')
    const h = React.createElement
    const { useState, useEffect, useRef, useCallback, useMemo } = React

    const NS = 'dsh-mole'
    const PANEL_ID = 'dsh-mole'

    // ------------------------------------------------------------------
    // request helpers
    // ------------------------------------------------------------------
    function api(path) {
      const relative = String(path).replace(/^\/+/, '')
      if (typeof document === 'undefined') return `/${relative}`
      try {
        const url = new URL(relative, document.baseURI)
        return url.pathname + url.search
      } catch {
        return `/${relative}`
      }
    }

    async function requestJson(path, options = {}) {
      const headers = { 'x-dsh-mole': '1' }
      if (options.method === 'POST') headers['content-type'] = 'application/json'
      let response
      try {
        response = await fetch(api(path), {
          method: options.method ?? 'GET',
          cache: 'no-store',
          headers,
          body: options.body === undefined ? undefined : JSON.stringify(options.body),
          signal: options.signal,
        })
      } catch (error) {
        if (error && error.name === 'AbortError') throw error
        throw new Error('无法连接本机 Mole 服务 / cannot reach the local Mole service')
      }
      let payload = null
      try {
        payload = await response.json()
      } catch {
        throw new Error(`HTTP ${response.status}`)
      }
      if (!response.ok || payload?.ok === false) {
        throw new Error(payload?.error?.message ?? `HTTP ${response.status}`)
      }
      return payload
    }

    // ------------------------------------------------------------------
    // background job progress
    // ------------------------------------------------------------------
    const jobBus = (() => {
      let current = null
      let timer = null
      const listeners = new Set()
      const emit = () => {
        for (const listener of listeners) listener(current)
      }
      return {
        get: () => current,
        subscribe(listener) {
          listeners.add(listener)
          return () => {
            listeners.delete(listener)
          }
        },
        publish(next) {
          if (timer !== null) {
            clearTimeout(timer)
            timer = null
          }
          current = next
          emit()
        },
        clearAfter(ms) {
          if (timer !== null) clearTimeout(timer)
          timer = setTimeout(() => {
            current = null
            timer = null
            emit()
          }, ms)
        },
      }
    })()

    function delay(ms) {
      return new Promise((resolve) => setTimeout(resolve, ms))
    }

    async function runJob(request, t, labelKey) {
      const startedMs = Date.now()
      const label = t(labelKey)
      const key = jobRecordKey(request)
      recordJob(request, { state: 'running', label, startedMs, error: null, result: null })
      jobBus.publish({ key, state: 'starting', label, action: request.action, kind: request.kind, startedMs, phase: { groups: 0, items: 0, bytes: 0 }, lines: [] })
      let started
      try {
        started = await requestJson('/dsh-mole/job/start', { method: 'POST', body: request })
      } catch (error) {
        const message = String(error?.message ?? error)
        recordJob(request, { state: 'error', error: message })
        jobBus.publish({ key, state: 'error', label, error: message, startedMs })
        jobBus.clearAfter(9000)
        throw error
      }
      const jobId = started.jobId
      let last = null
      try {
        for (;;) {
          await delay(500)
          const payload = await requestJson(`/dsh-mole/job/status?id=${encodeURIComponent(jobId)}`)
          last = payload.job
          jobBus.publish({ ...last, key, label, startedMs: last.startedMs ?? startedMs })
          recordJob(request, { state: last.state === 'running' ? 'running' : last.state, jobId, job: last, label, startedMs: last.startedMs ?? startedMs, error: last.error ?? null })
          if (last.state !== 'running') break
        }
      } catch (error) {
        const message = String(error?.message ?? error)
        recordJob(request, { state: 'error', error: message })
        jobBus.publish({ key, state: 'error', label, error: message, startedMs })
        jobBus.clearAfter(9000)
        throw error
      }
      if (last.state === 'error') {
        recordJob(request, { state: 'error', error: last.error ?? 'job failed' })
        jobBus.clearAfter(9000)
        const failure = new Error(last.error ?? 'job failed')
        failure.job = last
        throw failure
      }
      recordJob(request, { state: 'done', result: last.result, job: last, error: null, durationMs: last.durationMs })
      jobBus.clearAfter(3200)
      return last.result
    }

    const jobRecords = new Map()

    function jobRecordKey(request) {
      if (request.action === 'analyze') return 'analyze'
      return `${request.action}:${request.kind ?? ''}`
    }

    function recordJob(request, patch) {
      const key = jobRecordKey(request)
      jobRecords.set(key, { ...(jobRecords.get(key) ?? {}), key, request, ...patch, updatedAt: Date.now() })
    }

    function useJobBus() {
      const [job, setJob] = useState(() => jobBus.get())
      useEffect(() => jobBus.subscribe(setJob), [])
      return job
    }

    const persistentPanels = new Map()
    const persistentListeners = new Map()

    function writePersistent(key, value) {
      persistentPanels.set(key, value)
      const listeners = persistentListeners.get(key)
      if (listeners === undefined) return
      for (const listener of listeners) listener(value)
    }

    /** State that survives unmounting a tab, and follows updates from a previous instance. */
    function usePersistentState(key, initial) {
      const [value, setValue] = useState(() => (persistentPanels.has(key) ? persistentPanels.get(key) : initial))
      const setValuePersisted = useCallback((next) => {
        const resolved = typeof next === 'function' ? next(persistentPanels.has(key) ? persistentPanels.get(key) : initial) : next
        writePersistent(key, resolved)
      }, [])
      useEffect(() => {
        let listeners = persistentListeners.get(key)
        if (listeners === undefined) {
          listeners = new Set()
          persistentListeners.set(key, listeners)
        }
        listeners.add(setValue)
        return () => {
          listeners.delete(setValue)
          if (listeners.size === 0) persistentListeners.delete(key)
        }
      }, [key])
      return [value, setValuePersisted]
    }

    function ProgressLine({ t }) {
      const job = useJobBus()
      const [expanded, setExpanded] = useState(false)
      const [, setTick] = useState(0)
      const running = job !== null && (job.state === 'running' || job.state === 'starting')
      useEffect(() => {
        if (!running) return undefined
        const timer = setInterval(() => setTick((value) => value + 1), 500)
        return () => clearInterval(timer)
      }, [running])
      if (job === null) return null
      const elapsedMs = running ? Date.now() - (job.startedMs ?? Date.now()) : job.durationMs ?? 0
      const elapsed = Math.max(0, Math.round(elapsedMs / 1000))
      const phase = job.phase ?? {}
      const counters = []
      if (phase.groups) counters.push(t('progress.groups', { value: phase.groups }))
      if (phase.items) counters.push(t('progress.items', { value: phase.items }))
      if (phase.bytes) counters.push(t('progress.size', { value: fmtBytes(phase.bytes) }))
      let detail = ''
      if (job.state === 'error') detail = job.error ?? t('progress.failed')
      else if (running) detail = phase.group || phase.item ? [zhCli(phase.group), zhCli(phase.item)].filter(Boolean).join(' · ') : zhCli(phase.line) || t('progress.waiting')
      else detail = t('progress.done')
      const hasLog = Array.isArray(job.lines) && job.lines.length > 0
      return h(
        'div',
        { className: 'mole-runbar-wrap' },
        h(
          'div',
          { className: `mole-runbar mole-runbar-${job.state}` },
          h('span', { className: 'mole-runbar-indicator', 'aria-hidden': true }),
          h('span', { className: 'mole-runbar-label' }, job.label ?? t('progress.running')),
          h('span', { className: 'mole-runbar-detail', title: zhCli(phase.line ?? '') }, detail),
          counters.length > 0 ? h('span', { className: 'mole-runbar-counters' }, counters.join(' · ')) : null,
          h('span', { className: 'mole-runbar-time' }, t('progress.elapsed', { value: elapsed })),
          running && hasLog ? h('button', { type: 'button', className: 'mole-runbar-toggle', onClick: () => setExpanded((value) => !value) }, expanded ? t('progress.hideLog') : t('progress.showLog')) : null,
        ),
        expanded && hasLog ? h('pre', { className: 'mole-runbar-log' }, job.lines.slice(-60).map(zhCliLine).join('\n')) : null,
      )
    }

    // ------------------------------------------------------------------
    // formatting
    // ------------------------------------------------------------------
    function fmtBytes(bytes, digits = 1) {
      const value = Number(bytes)
      if (!Number.isFinite(value) || value <= 0) return '0 B'
      const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
      let index = 0
      let size = value
      while (size >= 1024 && index < units.length - 1) {
        size /= 1024
        index += 1
      }
      const fixed = size >= 100 || index === 0 ? 0 : digits
      return `${size.toFixed(fixed)} ${units[index]}`
    }

    function fmtPercent(value, digits = 0) {
      const number = Number(value)
      if (!Number.isFinite(number)) return '—'
      return `${number.toFixed(digits)}%`
    }

    function fmtDuration(seconds) {
      const value = Number(seconds)
      if (!Number.isFinite(value) || value <= 0) return '—'
      const days = Math.floor(value / 86400)
      const hours = Math.floor((value % 86400) / 3600)
      const minutes = Math.floor((value % 3600) / 60)
      if (days > 0) return `${days}d ${hours}h`
      if (hours > 0) return `${hours}h ${minutes}m`
      return `${minutes}m`
    }

    function parseSizeText(text) {
      const match = /(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)\b/i.exec(String(text ?? ''))
      if (!match) return 0
      const units = { B: 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3, TB: 1024 ** 4 }
      return Number(match[1]) * (units[match[2].toUpperCase()] ?? 1)
    }

    function shortPath(path, depth = 3) {
      const parts = String(path ?? '').split('/').filter(Boolean)
      if (parts.length <= depth) return path
      return `…/${parts.slice(-depth).join('/')}`
    }

    const EMPTY_ARRAY = []
    function useStable(array) {
      return array ?? EMPTY_ARRAY
    }

    // ------------------------------------------------------------------
    // Mole CLI output localization (the CLI itself prints English only)
    // ------------------------------------------------------------------
    let activeLocale = 'en'

    function isZhLocale() {
      return /^zh/i.test(String(activeLocale))
    }

    const CLI_ZH = {
  "User essentials": "用户基础项",
  "App caches": "应用缓存",
  "Browsers": "浏览器",
  "Cloud & Office": "云与办公",
  "Developer tools": "开发工具",
  "Apps & utilities": "应用与工具",
  "Virtualization": "虚拟化",
  "Application Support": "应用支持",
  "App leftovers": "应用残留",
  "Apple Silicon updates": "Apple Silicon 更新",
  "Device backups & firmware": "设备备份与固件",
  "Time Machine": "Time Machine",
  "Large files": "大文件",
  "Project artifacts": "项目构建产物",
  "System data": "系统数据",
  "System caches": "系统缓存",
  "External volume": "外部磁盘",
  "System cleanup requires admin access": "系统清理需要管理员权限",
  "Trash": "废纸篓",
  "User app cache": "用户应用缓存",
  "User app logs": "用户应用日志",
  "npm cache": "npm 缓存",
  "npm logs": "npm 日志",
  "pnpm cache": "pnpm 缓存",
  "Corepack cache": "Corepack 缓存",
  "pip cache": "pip 缓存",
  "uv cache": "uv 缓存",
  "Docker unused data": "Docker 未使用数据",
  "Project caches": "项目缓存",
  "Xcode XCTestDevices": "Xcode 测试设备",
  "Stale login item": "过期的登录项",
  "Time Machine local snapshots": "Time Machine 本地快照",
  "Docker Desktop data": "Docker Desktop 数据",
  "Build artifacts": "构建产物",
  "Cleanup complete": "清理完成",
  "Cleanup incomplete": "清理未完成",
  "Cleanup interrupted": "清理被中断",
  "Cleanup cancelled": "清理已取消",
  "Dry run complete - no changes made": "预览完成 — 未做任何更改",
  "Dry run incomplete": "预览未完成",
  "Dry run interrupted": "预览被中断",
  "Dry run cancelled": "预览已取消",
  "No additional reclaimable space detected.": "未检测到更多可回收空间。",
  "No additional space freed.": "没有额外释放空间。",
  "No significant reclaimable space detected, system already clean.": "未检测到明显可回收空间，系统已是干净状态。",
  "System was already clean; no additional space freed.": "系统本来就很干净，没有额外释放空间。",
  "Partially measured": "部分测量",
  "Unknown": "未知",
  "SKIPPED": "已跳过",
  "SPACE": "空间",
  "Files to be removed": "将要删除的文件",
  "Item count": "项目数量",
  "Free space": "可用空间",
  "Potential space": "预计可释放",
  "Cleaning...": "正在清理…",
  "Preparing summary...": "正在生成摘要…",
  "Running in non-interactive mode": "以非交互模式运行",
  "Optimize": "优化",
  "Performance diagnosis": "性能诊断",
  "Optimization Complete": "优化完成",
  "Optimization pass complete": "优化过程完成",
  "Review the warnings above": "请查看上方警告",
  "System optimization requires admin access": "系统优化需要管理员权限",
  "Optimize task outcomes are incomplete": "优化结果不完整",
  "Finder Cache Refresh": "Finder 缓存刷新",
  "App State Cleanup": "应用状态清理",
  "Broken Config Repair": "损坏配置修复",
  "Network Cache Refresh": "网络缓存刷新",
  "Database Optimization": "数据库优化",
  "Prevent Finder .DS_Store": "阻止 Finder 生成 .DS_Store",
  "Legacy Overrides": "旧版兼容覆盖",
  "Network Stack Refresh": "网络栈刷新",
  "Permission Repair": "权限修复",
  "Spotlight Optimization": "Spotlight 优化",
  "Spotlight Orphan Rules": "Spotlight 孤立规则",
  "Periodic Maintenance": "定期维护",
  "Shared File Lists": "共享文件列表",
  "Disk Health": "磁盘健康",
  "Login Items": "登录项",
  "Quarantine Database Cleanup": "隔离区数据库清理",
  "Launch Agents Cleanup": "启动代理清理",
  "Notifications": "通知",
  "Usage Data": "使用数据",
  "Additional sustained pressure:": "其他持续占用：",
  "Gatekeeper status: assessments enabled": "Gatekeeper 状态：评估已启用",
  "Mounted image adds assessment overhead:": "挂载的镜像增加评估开销：",
  "Review these mounted images and detach any you no longer need": "请检查这些已挂载镜像，不需要的请推出",
  "Gatekeeper and code-signature assessment activity is elevated.": "Gatekeeper 与代码签名评估活动偏高。",
  "QuickLook thumbnails refreshed": "QuickLook 缩略图已刷新",
  "Icon services cache rebuilt": "图标服务缓存已重建",
  "App saved states optimized": "应用保存状态已优化",
  "All preference files valid": "所有偏好设置文件均有效",
  "Network cache refresh skipped (admin access required)": "网络缓存刷新已跳过（需要管理员权限）",
  "Skipping sudo-required optimizations: admin access not granted": "已跳过需要 sudo 的优化：未获得管理员授权",
  "Optimized 1 databases for Mail, Safari, Messages": "已优化 1 个数据库（邮件、Safari、信息）",
  "Skipped 1 databases over the 100 MB safety limit": "已跳过 1 个超过 100 MB 安全上限的数据库",
  ".DS_Store prevention enabled on network & USB volumes": "已在网络与 USB 卷上启用 .DS_Store 防护",
  "No legacy App Nap or disk-image overrides found": "未发现旧版 App Nap 或磁盘映像覆盖设置",
  "Network stack refresh skipped, active VPN detected": "网络栈刷新已跳过：检测到 VPN 正在使用",
  "User directory permissions already optimal": "用户目录权限已是最佳状态",
  "Spotlight index verified": "Spotlight 索引已验证",
  "Spotlight search rules already clean": "Spotlight 搜索规则已是干净状态",
  "Periodic maintenance skipped (not available on this macOS version)": "定期维护已跳过（当前 macOS 版本不支持）",
  "Shared file lists all healthy": "共享文件列表全部正常",
  "Disk verify skipped": "磁盘校验已跳过",
  "Login items all healthy (2 checked)": "登录项全部正常（已检查 2 项）",
  "Quarantine database already clean": "隔离区数据库已是干净状态",
  "Notification Center database is healthy (623KB)": "通知中心数据库正常（623KB）",
  "Knowledge database is healthy (11.7MB)": "知识数据库正常（11.7MB）",
  "Installer scan timed out; no files selected": "安装包扫描超时，未选择文件",
  "Check that the scan locations are readable and responsive, then retry mo installer.": "请确认扫描目录可读且响应正常，然后重试 mo installer。",
  "No installer files to clean": "没有可清理的安装包文件",
  "Installers cleaned": "安装包已清理",
  "No installers were removed": "未删除任何安装包",
  "Installer cleanup incomplete": "安装包清理未完成",
  "No apps with Intel code to remove": "没有可移除 Intel 代码的应用",
  "Run mo slim in a terminal to choose apps.": "请在终端中运行 mo slim 选择应用。",
  "Purge complete": "项目清理完成",
  "Purge Project Artifacts": "清理项目构建产物",
  "clean": "清理",
  "optimize": "优化",
  "purge": "项目清理",
  "installer": "安装包清理",
  "slim": "Intel 代码移除",
  "uninstall": "卸载",
  "analyze": "磁盘分析",
  "status": "状态",
  "clean-dry-run": "清理预览",
  "optimize-dry-run": "优化预览",
  "purge-dry-run": "项目清理预览",
  "installer-dry-run": "安装包预览",
  "slim-dry-run": "Intel 代码预览",
  "Clean Your Mac": "清理你的 Mac",
  "Nothing to clean": "无需清理",
  "Dry Run Mode, Preview only, no deletions": "预览模式：仅展示，不会删除任何文件",
  "DRY RUN MODE, No files will be modified": "预览模式：不会修改任何文件",
  "System": "系统",
  "uninstall-dry-run": "卸载预览"
}

    const CLI_PATTERNS = [
      [/would empty, (\d+) items?/g, '将清空，$1 项'],
      [/\[DRY RUN\]/g, '[预览]'],
      [/^Dry Run Mode, Preview only, no deletions$/, '预览模式：仅展示，不会删除任何文件'],
      [/^DRY RUN MODE, No files will be modified$/, '预览模式：不会修改任何文件'],
      [/^DRY RUN MODE, No project artifacts will be removed$/, '预览模式：不会删除任何项目构建产物'],
      [/^DRY RUN MODE, No installer files will be removed$/, '预览模式：不会删除任何安装包文件'],
      [/^Dry run incomplete - no changes made$/, '预览未完成 — 未做任何更改'],
      [/^Would free approximately: (.+) \| Items: (\d+) \| Free: (.+)$/, '预计可释放约：$1 | 项目：$2 | 可用：$3'],
      [/^Some artifacts were skipped or could not be processed\.$/, '部分构建产物被跳过或无法处理。'],
      [/^Could not inspect (.+); kept$/, '无法检查 $1；已保留'],
      [/^System\s+(\d+)\/(\d+) GB RAM \| (\d+)\/(\d+) GB Disk \| Uptime (.*)$/, '系统 $1/$2 GB 内存 · $3/$4 GB 磁盘 · 已运行 $5'],
      [/^([^·]+) · would empty, (\d+) items?$/, '$1 · 将清空，$2 项'],
      [/^([^·]+) would clean$/, '$1 将清理'],
      [/\bwould clean\b/g, '将清理'],
      [/\bwould empty\b/g, '将清空'],
      [/(?<!-)\bdry\b/g, '（预览）'],
      [/\b(\d+) items?\b/g, '$1 项'],
      [/\b(\d+) dirs?\b/g, '$1 个目录'],
      [/\b(\d+) files?\b/g, '$1 个文件'],
      [/Skipping incomplete download \(open-file check unavailable\):\s*/g, '跳过未完成下载（无法检查文件占用）：'],
      [/\bskipped 1 slow\/incomplete root scan\b/g, '已跳过 1 个缓慢或不完整的根目录扫描'],
      [/\bskipped \(process state unknown\)/g, '已跳过（进程状态未知）'],
      [/\bMissing app\/helper target:\s*/g, '缺少应用或后台目标：'],
      [/\breview before removing\b/g, '删除前请检查'],
      [/\bmacOS removes them when space runs low\b/g, '空间不足时 macOS 会自动移除'],
      [/\breview with (.+)$/g, '请用 $1 查看'],
      [/^Optimized (\d+) databases? for (.+)$/, '已优化 $1 个数据库（$2）'],
      [/^Skipped (\d+) databases? over the (.+) safety limit$/, '已跳过 $1 个超过 $2 安全上限的数据库'],
      [/^(\d+) apps, (.+) of Intel code$/, '$1 个应用，$2 Intel 代码'],
      [/^Installer scan (timed out|failed); no files selected(?: \((.+)\))?$/, '安装包扫描超时或失败，未选择文件$2'],
      [/^Check that the scan locations are readable and responsive, then retry mo installer\.$/, '请确认扫描目录可读且响应正常，然后重试 mo installer。'],
      [/^Potential space: (.+) · (.+) items$/, '预计可释放：$1 · $2 项'],
      [/^Potential space: (.+)$/, '预计可释放：$1'],
      [/^Detailed file list: (.+)$/, '详细文件列表：$1'],
      [/^Free space: (.+)$/, '可用空间：$1'],
      [/^Removed: (.+)$/, '已删除：$1'],
      [/^Applied (\d+) optimizations?, (\d+) databases? optimized$/, '已应用 $1 项优化，优化 $2 个数据库'],
      [/^Applied (\d+) optimizations?$/, '已应用 $1 项优化'],
      [/^(\d+) unchanged \| (\d+) skipped \| (\d+) unavailable \| (\d+) need attention$/, '$1 项无变化 | 已跳过 $2 项 | $3 项不可用 | $4 项需关注'],
      [/^Likely bottleneck: (.+)$/, '可能瓶颈：$1'],
      [/^Memory pressure: swap (.+) of (.+) used \((.+)\), (.+) free$/, '内存压力：交换区已用 $1 / $2（$3），空闲 $4'],
      [/^Additional sustained pressure:$/, '其他持续占用：'],
      [/^Gatekeeper status: (.+)$/, 'Gatekeeper 状态：$1'],
      [/^Mounted image adds assessment overhead:$/, '挂载的镜像增加评估开销：'],
      [/^Review these mounted images and detach any you no longer need$/, '请检查这些已挂载镜像，不需要的请推出'],
      [/^Gatekeeper and code-signature assessment activity is elevated\.$/, 'Gatekeeper 与代码签名评估活动偏高。'],
      [/^System (\d+)\/(\d+) GB RAM \| (\d+)\/(\d+) GB Disk \| Uptime (.*)$/, '系统 $1/$2 GB 内存 · $3/$4 GB 磁盘 · 已运行 $5'],
      [/^Apple Silicon \| Free space: (.+)$/, 'Apple Silicon · 可用空间 $1'],
      [/^Whitelist: (\d+) core patterns active$/, '保护清单：$1 条核心规则已启用'],
      [/^System caches need sudo, run (.+) for full preview$/, '系统缓存需要管理员权限，运行 $1 可查看完整预览'],
      [/\s+（预览）/g, '（预览）'],
      [/(\d+\+?)\s*dirs?\b/g, '$1 个目录'],
      [/(\d+\+?)\s*files?\b/g, '$1 个文件'],
      [/~([\d.]+)% CPU sustained/g, '持续约 $1% CPU'],
      [/^Launch Agent (.+): program missing at (.+)$/, '启动代理 $1：程序缺失（$2）'],
      [/^(\d+) Launch Agent\(s\) point at a missing program$/, '$1 个启动代理指向缺失的程序'],
      [/^left in (.+)$/, '保留在 $1'],
      [/^(\d+) Launch Agent\(s\) point at a missing program · left in (.+)$/, '$1 个启动代理指向缺失的程序 · 保留在 $2'],
    ]

    const CLI_GENERIC_PATTERNS = [
      [/^(.+) refreshed$/, '$1 已刷新'],
      [/^(.+) rebuilt$/, '$1 已重建'],
      [/^(.+) optimized$/, '$1 已优化'],
      [/^(.+) skipped, active VPN detected$/, '$1 已跳过：检测到 VPN 正在使用'],
      [/^(.+) skipped \(admin access required\)$/, '$1 已跳过（需要管理员权限）'],
      [/^(.+) skipped \(not available on this macOS version\)$/, '$1 已跳过（当前 macOS 版本不支持）'],
      [/^(.+) verified$/, '$1 已验证'],
      [/^(.+) already clean$/, '$1 已是干净状态'],
      [/^(.+) already optimal$/, '$1 已是最佳状态'],
      [/^(.+) all healthy( \(.*\))?$/, '$1 全部正常$2'],
      [/^(.+) is healthy \((.+)\)$/, '$1 正常（$2）'],
      [/^No (.+) found$/, '未发现 $1'],
    ]

    const CLI_WORDS = [
      [/\bpreview attachment\b/gi, '预览附件'],
      [/\bstickers?\b/gi, '贴纸'],
      [/\bsuggestions?\b/gi, '建议'],
      [/\bsnapshots?\b/gi, '快照'],
      [/\bbackups?\b/gi, '备份'],
      [/\bfirmware\b/gi, '固件'],
      [/\binstallers?\b/gi, '安装包'],
      [/\barchives?\b/gi, '归档'],
      [/\bsimulators?\b/gi, '模拟器'],
      [/\bleftovers?\b/gi, '残留'],
      [/\bunused data\b/gi, '未使用数据'],
      [/\bbytecode\b/gi, '字节码'],
      [/\bdebug symbols?\b/gi, '调试符号'],
      [/\btemp files?\b/gi, '临时文件'],
      [/\btemporary\b/gi, '临时'],
      [/\bcaches?\b/gi, '缓存'],
      [/\blogs?\b/gi, '日志'],
      [/\bdata\b/gi, '数据'],
      [/\bfiles?\b/gi, '文件'],
      [/\bdirs?\b/gi, '目录'],
      [/\bsystem\b/gi, '系统'],
      [/\buser\b/gi, '用户'],
      [/\bapps?\b/gi, '应用'],
      [/\bhistory\b/gi, '历史'],
      [/\btrash\b/gi, '废纸篓'],
      [/\bmessages\b/gi, '信息'],
      [/\bmail\b/gi, '邮件'],
    ]

    function translateCliWords(value) {
      let out = value
      for (const [pattern, replacement] of CLI_WORDS) out = out.replace(pattern, replacement)
      return out
    }

    function translateCli(text) {
      const value = String(text ?? '')
      if (!value) return value
      const exact = CLI_ZH[value]
      if (exact !== undefined) return exact
      let out = value
      for (const [pattern, replacement] of CLI_PATTERNS) out = out.replace(pattern, replacement)
      if (out === value) {
        for (const [pattern, replacement] of CLI_GENERIC_PATTERNS) out = out.replace(pattern, replacement)
      }
      if (out === value && !value.includes('/') && value.length < 120) out = translateCliWords(value)
      out = out.replace(/\s+（预览）/g, '（预览）')
      return out
    }

    function translateCliLine(line) {
      const text = String(line ?? '')
      const match = /^(\s*(?:[➤>⚙•]\s*|[○✓◎⊙]\s*))(.*)$/.exec(text)
      const marker = match ? match[1] : ''
      const content = match ? match[2] : text
      const localized = content.split(' · ').map((part) => translateCli(part)).join(' · ')
      return marker + localized
    }

    function zhCli(text) {
      const value = String(text ?? '')
      return isZhLocale() ? translateCli(value) : value
    }

    function zhCliLine(line) {
      return isZhLocale() ? translateCliLine(line) : String(line ?? '')
    }

    const HEALTH_ZH = { Excellent: '优秀', Good: '良好', Fair: '一般', Poor: '较差', Critical: '严重', Warning: '警告', 'All checks passed': '所有检查通过' }
    const BATTERY_STATUS_ZH = { AC: '电源', Charging: '充电中', Discharging: '使用电池', Full: '已充满', Unknown: '未知' }
    const BATTERY_HEALTH_ZH = { Good: '良好', Normal: '正常', 'Service Battery': '建议维修', 'Replace Soon': '建议更换', Unknown: '未知' }
    const SMART_ZH = { verified: '已验证', unverified: '未验证', unknown: '未知' }
    const PRESSURE_ZH = { normal: '正常', warn: '偏高', critical: '紧张' }

    function zhMapValue(map, value) {
      const key = String(value ?? '')
      if (!isZhLocale()) return key
      return map[key] ?? key
    }

    function zhDuration(value) {
      const text = String(value ?? '')
      if (!isZhLocale()) return text
      return text
        .replace(/(\d+)d\b/g, '$1 天')
        .replace(/(\d+)h\b/g, '$1 小时')
        .replace(/(\d+)m\b/g, '$1 分钟')
        .replace(/(\d+)s\b/g, '$1 秒')
    }

    // ------------------------------------------------------------------
    // dry-run parser
    // ------------------------------------------------------------------
    const SIZE_UNITS = { B: 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3, TB: 1024 ** 4 }

    function parsePreview(text) {
      const lines = String(text ?? '').split(/\r?\n/)
      const groups = []
      const notes = []
      let current = null
      let systemLine = ''
      let title = ''
      let totalBytes = 0
      let itemCount = 0
      let itemLines = 0
      for (const raw of lines) {
        const line = raw.replace(/\s+$/, '')
        const trimmed = line.trim()
        if (!trimmed) continue
        if (/^[➤>]/.test(trimmed)) {
          current = { title: zhCli(trimmed.replace(/^[➤>]\s*/, '')), items: [], note: '' }
          groups.push(current)
          continue
        }
        const mark = /^([○✓◎⊙↳•])\s*(.*)$/.exec(trimmed)
        if (mark && current) {
          if (mark[1] === '↳') {
            const last = current.items[current.items.length - 1]
            if (last) last.note = zhCli(mark[2].trim())
            continue
          }
          const rest = mark[2]
          const sizeMatch = /(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)\b/i.exec(rest)
          const sizeBytes = sizeMatch ? Number(sizeMatch[1]) * (SIZE_UNITS[sizeMatch[2].toUpperCase()] ?? 1) : 0
          const parts = rest.split(' · ')
          const item = {
            mark: mark[1],
            name: zhCli((parts[0] ?? '').trim()),
            detail: zhCli(parts.slice(1).join(' · ').trim()),
            sizeBytes,
            sizeText: sizeMatch ? `${sizeMatch[1]} ${sizeMatch[2]}` : '',
            note: '',
          }
          if (sizeBytes > 0) totalBytes += sizeBytes
          const countMatch = /(\d+)\s+items?/.exec(rest)
          if (countMatch) itemCount += Number(countMatch[1])
          itemLines += 1
          current.items.push(item)
          continue
        }
        if (/^⚙/.test(trimmed)) {
          const value = trimmed.replace(/^⚙\s*/, '')
          systemLine = systemLine ? `${systemLine} · ${zhCli(value)}` : zhCli(value)
          continue
        }
        if (groups.length === 0) {
          if (!title && /^[A-Za-z]/u.test(trimmed) && trimmed.length < 40 && !/[=]/.test(trimmed)) {
            title = zhCli(trimmed)
          } else {
            notes.push(zhCli(trimmed))
          }
          continue
        }
        if (current && current.items.length === 0 && !current.note) {
          current.note = zhCli(trimmed)
        } else {
          notes.push(zhCli(trimmed))
        }
      }
      return { title, groups, notes, systemLine: zhDuration(systemLine), totalBytes, itemCount, itemLines, raw: text }
    }

    function summarizeRun(text) {
      const lines = String(text ?? '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean)
      const keep = []
      for (const line of lines) {
        if (/^(Removed|Cleanup complete|Optimization complete|Purge complete|Installer|Free space|Freed|Applied|Estimated|Trash|Skipped|Failed|Dry run|Uninstall|Mole )/i.test(line)) {
          keep.push(line)
        } else if (/^[=\-]{4,}$/.test(line)) {
          continue
        }
      }
      if (keep.length === 0) return lines.slice(-6).map(zhCli)
      return keep.slice(-10).map(zhCli)
    }

    // ------------------------------------------------------------------
    // icons
    // ------------------------------------------------------------------
    function Svg({ size = 18, children, viewBox = '0 0 24 24', className, style }) {
      return h(
        'svg',
        {
          width: size,
          height: size,
          viewBox,
          fill: 'none',
          stroke: 'currentColor',
          strokeWidth: 1.7,
          strokeLinecap: 'round',
          strokeLinejoin: 'round',
          'aria-hidden': true,
          className,
          style,
        },
        children,
      )
    }

    function MoleMark({ size = 20 }) {
      return h(
        Svg,
        { size, viewBox: '0 0 24 24', strokeWidth: 1.6 },
        h('path', { d: 'M3.8 12.1c0-4.6 3.6-8.3 8.2-8.3s8.2 3.7 8.2 8.3c0 4.9-3.7 8.1-8.2 8.1s-8.2-3.2-8.2-8.1Z' }),
        h('circle', { cx: 9.1, cy: 10.4, r: 1.05, fill: 'currentColor', stroke: 'none' }),
        h('circle', { cx: 14.9, cy: 10.4, r: 1.05, fill: 'currentColor', stroke: 'none' }),
        h('path', { d: 'M11.2 14.2c.5-.55 1.1-.55 1.6 0 .45.5-.1 1.5-1.05 1.5-.95 0-1.5-1-1.05-1.5Z', fill: 'currentColor', stroke: 'none' }),
        h('path', { d: 'M7.4 15.6 5.2 16.6M16.6 15.6l2.2 1' }),
        h('path', { d: 'M12 3.8V2.6' }),
      )
    }

    function Icon({ name, size = 17 }) {
      const common = { size }
      switch (name) {
        case 'clean':
          return h(Svg, common, h('path', { d: 'M12 4.2v2.4M12 17.4v2.4M4.2 12h2.4M17.4 12h2.4' }), h('path', { d: 'M7.8 7.8 9.5 9.5M14.5 14.5l1.7 1.7M16.2 7.8l-1.7 1.7M9.5 14.5l-1.7 1.7' }), h('circle', { cx: 12, cy: 12, r: 2.6 }))
        case 'apps':
          return h(Svg, common, h('rect', { x: 4, y: 4, width: 6.4, height: 6.4, rx: 2 }), h('rect', { x: 13.6, y: 4, width: 6.4, height: 6.4, rx: 2 }), h('rect', { x: 4, y: 13.6, width: 6.4, height: 6.4, rx: 2 }), h('rect', { x: 13.6, y: 13.6, width: 6.4, height: 6.4, rx: 2 }))
        case 'optimize':
          return h(Svg, common, h('path', { d: 'm13.2 3.6-8 10.4h5.4l-1 6.4 8.2-10.6h-5.4l.8-6.2Z' }))
        case 'analyze':
          return h(Svg, common, h('circle', { cx: 12, cy: 12, r: 8 }), h('path', { d: 'M12 4v8h8' }), h('path', { d: 'M12 12 6.1 6.9' }))
        case 'status':
          return h(Svg, common, h('path', { d: 'M3.2 12.4h3.4l1.9-4.6 3 9 2.4-6.8 1.7 3.2h5.2' }))
        case 'tools':
          return h(Svg, common, h('path', { d: 'M14.6 6.2a3.6 3.6 0 0 0 4.6 4.6l-7.4 7.4a1.7 1.7 0 0 1-2.4 0 1.7 1.7 0 0 1 0-2.4l7.4-7.4' }), h('path', { d: 'm5.6 5.6 3.6 3.6' }))
        case 'history':
          return h(Svg, common, h('path', { d: 'M4.4 12a7.6 7.6 0 1 1 2.3 5.4' }), h('path', { d: 'M4 18.4V14h4.4' }), h('path', { d: 'M12 8.6V12l2.4 1.6' }))
        case 'search':
          return h(Svg, common, h('circle', { cx: 11, cy: 11, r: 6 }), h('path', { d: 'm16 16 3.2 3.2' }))
        case 'refresh':
          return h(Svg, common, h('path', { d: 'M19.6 9A7.8 7.8 0 1 0 20 14' }), h('path', { d: 'M19.6 4.6V9H15' }))
        case 'chevron':
          return h(Svg, common, h('path', { d: 'm9.4 6.4 5.2 5.6-5.2 5.6' }))
        case 'folder':
          return h(Svg, common, h('path', { d: 'M3.6 7.2c0-1 .8-1.8 1.8-1.8h3.2l1.8 2h8c1 0 1.8.8 1.8 1.8v8c0 1-.8 1.8-1.8 1.8H5.4c-1 0-1.8-.8-1.8-1.8V7.2Z' }))
        case 'file':
          return h(Svg, common, h('path', { d: 'M6 3.8h7.2L18 8.6v11.6H6V3.8Z' }), h('path', { d: 'M13.2 3.8v4.8H18' }))
        case 'trash':
          return h(Svg, common, h('path', { d: 'M4.6 6.8h14.8' }), h('path', { d: 'M9 6.8V4.9c0-.6.4-1.1 1-1.1h4c.6 0 1 .5 1 1.1v1.9' }), h('path', { d: 'M6.6 6.8 7.5 19c0 .7.6 1.2 1.3 1.2h6.4c.7 0 1.3-.5 1.3-1.2l.9-12.2' }), h('path', { d: 'M10.4 10.4v6M13.6 10.4v6' }))
        case 'bolt':
          return h(Svg, common, h('path', { d: 'm13.4 3.4-7.2 9.8h4.6l-.9 7.4 7.5-10.2h-4.8l.8-7Z' }))
        case 'terminal':
          return h(Svg, common, h('rect', { x: 3.4, y: 5, width: 17.2, height: 14, rx: 2.4 }), h('path', { d: 'm7.6 9.6 2.8 2.4-2.8 2.4M12.8 14.6h3.6' }))
        case 'open':
          return h(Svg, common, h('path', { d: 'M13.6 4.4h6v6' }), h('path', { d: 'M19.6 4.4 11 13' }), h('path', { d: 'M18 14.4v4.2c0 .8-.6 1.4-1.4 1.4H5.4c-.8 0-1.4-.6-1.4-1.4V7.4c0-.8.6-1.4 1.4-1.4H9' }))
        case 'check':
          return h(Svg, common, h('path', { d: 'm5 12.6 4.4 4.4L19 7.4' }))
        case 'close':
          return h(Svg, common, h('path', { d: 'm6 6 12 12M18 6 6 18' }))
        case 'cpu':
          return h(Svg, common, h('rect', { x: 6.6, y: 6.6, width: 10.8, height: 10.8, rx: 2 }), h('path', { d: 'M10 2.8v3.8M14 2.8v3.8M10 17.4v3.8M14 17.4v3.8M2.8 10h3.8M2.8 14h3.8M17.4 10h3.8M17.4 14h3.8' }))
        case 'memory':
          return h(Svg, common, h('rect', { x: 3.4, y: 7, width: 17.2, height: 10, rx: 2 }), h('path', { d: 'M7 7V4.6M12 7V4.6M17 7V4.6M8.4 11.2v2.6M12 11.2v2.6M15.6 11.2v2.6' }))
        case 'disk':
          return h(Svg, common, h('ellipse', { cx: 12, cy: 6.6, rx: 7.4, ry: 2.9 }), h('path', { d: 'M4.6 6.6v10.8c0 1.6 3.3 2.9 7.4 2.9s7.4-1.3 7.4-2.9V6.6' }), h('path', { d: 'M4.6 12c0 1.6 3.3 2.9 7.4 2.9s7.4-1.3 7.4-2.9' }))
        case 'network':
          return h(Svg, common, h('circle', { cx: 12, cy: 12, r: 8.4 }), h('path', { d: 'M3.6 12h16.8M12 3.6c2.2 2.3 3.3 5.2 3.3 8.4s-1.1 6.1-3.3 8.4c-2.2-2.3-3.3-5.2-3.3-8.4S9.8 5.9 12 3.6Z' }))
        case 'battery':
          return h(Svg, common, h('rect', { x: 2.8, y: 7.4, width: 16, height: 9.2, rx: 2.2 }), h('path', { d: 'M21.2 10.4v3.2' }), h('path', { d: 'M6.2 10.4v3.2M9.4 10.4v3.2' }))
        case 'thermal':
          return h(Svg, common, h('path', { d: 'M10 13.6V5.4a2 2 0 1 1 4 0v8.2a3.6 3.6 0 1 1-4 0Z' }), h('path', { d: 'M12 17.2v-2' }))
        case 'health':
          return h(Svg, common, h('path', { d: 'M12 20s-7.4-4.2-7.4-9.4A4.2 4.2 0 0 1 12 7.2a4.2 4.2 0 0 1 7.4 3.4C19.4 15.8 12 20 12 20Z' }))
        case 'sparkle':
          return h(Svg, common, h('path', { d: 'M12 3.6l1.7 4.7 4.7 1.7-4.7 1.7L12 16.4l-1.7-4.7L5.6 10l4.7-1.7L12 3.6Z' }), h('path', { d: 'M18.4 15.2l.8 2.2 2.2.8-2.2.8-.8 2.2-.8-2.2-2.2-.8 2.2-.8.8-2.2Z' }))
        case 'warning':
          return h(Svg, common, h('path', { d: 'M12 4 2.8 19.6h18.4L12 4Z' }), h('path', { d: 'M12 10v4.2M12 17.2v.1' }))
        default:
          return h(Svg, common, h('circle', { cx: 12, cy: 12, r: 7 }))
      }
    }

    const ASSET_DATA = {
      mascot: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAb8AAAHgCAMAAAACFZNiAAACQFBMVEUMYGwJpZgMVW4OT2fmn6Kn6eIlWZ2c6OKW6OAL37EkYc+LeJ1n1s8Jj43V8/Dez96kk6qPldBbX5IWL1bI7+sAKDwLbW1tqPEBEHtPLlgGlXcQLJi45OUQM1V3hq8FlYwZbot7e3tXEjcBYKFFXsEGlo8QeIVuVnbLsNDYbXUAg34A/wCdVm8rEDpx29J13tYOOVM8kfUp8epYyrqrq6sAAP9//39Rs7AAhH3ie4MAZRQHhHwXPcL/AACu/66QM09IxbxKw7uNHzqBf8RVVar/AP9/AH8A/38fxrX/qqr//6oAf/9Ksq3H6eV+wP4Av///f38AAAD5+PtDWbQVGFE8VLD26uosOolZhOQwSahSY7pohuQ1Qo4uNXYaJ29PZ8bS1vXQx+oJDEbl2u9XdtQkK2sWI1jT4v1qedTGuuP8185MlvLR+PAVK4hka7kbMor///8A//0XN62yqNP85dtogts6ZMp0l+8Afn43heqOpvNxdLVSVo24tNMAVVW0x/cSHGVLdfSNme6p7eUoF0doZ5GvuPCLiK36ubQlJlrMxtZIR3XHuNhcgd0AioIGCDwAiINFSotDbfyWlbM2e+e48emKwvwAAFWRt/cZNHJ0dZjK9e4Ai4Tyycs2W8MDmI42Q3dvfuCSlswnVO4ZRbAClY+ppbk4Z/mmms0PNVjb+fQBk4yY6eEPNFoiTelzpfK70vp///+EiugFppbn+PYAkon8w7oF/dFkbMSnq/FUU3sON1cNV2oBjIYaSecMV2t0/aHCAAAAwHRSTlNdGemh/1z1I5oV//8eWZn///7+3G0ELv8C/xn/2aD83p8C/w3/qmP////lAf//W5hd/w0SAwECC6H/A0//AQP/bLL//wMBAgJRAwMCdDz/BAIA/v/+///+//7///7+/v////7///77///////O////AQL///////8C//////8D//7//6v///////7/////0f7x/////7T/A//7/7aw//8w/f////9P////0eZvqfH///8C/y/Sjv8P////snCS/5Cce7irAACpHUlEQVR42u39iUNUV7bwDRegGEUjYGISzdgZn9vD7dt9+47v+8zPO3xzUcOhqs45NZ6iMAEsSwShSko6VSWDIQQ7CgmGEGM0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',
      mascotLine: 'data:image/png;base64,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',
      earth: 'data:image/png;base64,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',
      jupiter: 'data:image/png;base64,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',
      sun: 'data:image/png;base64,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',
    }

    const PLANET_TONES = {
      earth: { a: '#a7d3ff', b: '#3f86cf', c: '#1c4d90', d: '#0b2447' },
      jupiter: { a: '#ffd8b8', b: '#de8a5f', c: '#a94a2a', d: '#4d1d12' },
      sun: { a: '#fff8de', b: '#ffc85f', c: '#f0902c', d: '#9c4a0e' },
    }

    function Planet({ variant = 'earth', size = 220 }) {
      const [failed, setFailed] = useState(false)
      const tone = PLANET_TONES[variant] ?? PLANET_TONES.earth
      if (failed) {
        return h('span', {
          className: 'mole-planet-fallback',
          style: { width: size, height: size, background: `radial-gradient(circle at 34% 28%, ${tone.a} 0%, ${tone.b} 32%, ${tone.c} 74%, ${tone.d} 100%)` },
          'aria-hidden': true,
        })
      }
      return h('img', {
        className: 'mole-planet-img',
        src: ASSET_DATA[variant] ?? ASSET_DATA.earth,
        width: size,
        height: size,
        alt: '',
        draggable: false,
        onError: () => setFailed(true),
      })
    }

    function Mascot({ size = 28 }) {
      const [failed, setFailed] = useState(false)
      if (failed) return h(MoleMark, { size })
      return h('img', {
        className: 'mole-mascot',
        src: ASSET_DATA.mascot,
        width: size,
        height: size,
        alt: '',
        draggable: false,
        onError: () => setFailed(true),
      })
    }

    function SidebarMascot({ size = 18 }) {
      const px = Math.max(14, Math.min(32, Number(size) || 18))
      return h('img', {
        className: 'mole-nav-icon',
        src: ASSET_DATA.mascotLine,
        width: px,
        height: px,
        alt: '',
        title: 'Mole',
        draggable: false,
        style: { width: px, height: px },
      })
    }

    // ------------------------------------------------------------------
    // UI atoms
    // ------------------------------------------------------------------
    function Card({ className = '', children, title, subtitle, right, tone }) {
      return h(
        'section',
        { className: `mole-card ${tone ? `mole-card-${tone}` : ''} ${className}`.trim() },
        title !== undefined
          ? h(
              'header',
              { className: 'mole-card-head' },
              h('div', null, h('h3', { className: 'mole-card-title' }, title), subtitle ? h('p', { className: 'mole-card-sub' }, subtitle) : null),
              right ? h('div', { className: 'mole-card-right' }, right) : null,
            )
          : null,
        children,
      )
    }

    function Btn({ children, onClick, variant = 'default', size = 'md', disabled, icon, title, className = '' }) {
      return h(
        'button',
        {
          type: 'button',
          className: `mole-btn mole-btn-${variant} mole-btn-${size} ${className}`.trim(),
          onClick,
          disabled,
          title,
        },
        icon ? h(Icon, { name: icon, size: size === 'sm' ? 14 : 16 }) : null,
        h('span', null, children),
      )
    }

    function Bar({ value, tone = 'accent', height = 8 }) {
      const percent = Math.max(0, Math.min(100, Number(value) || 0))
      return h('div', { className: 'mole-bar', style: { height } }, h('div', { className: `mole-bar-fill mole-tone-${tone}`, style: { width: `${percent}%` } }))
    }

    function Dot({ tone = 'idle' }) {
      return h('span', { className: `mole-dot mole-dot-${tone}` })
    }

    function Spinner({ label }) {
      return h('span', { className: 'mole-spinner-wrap' }, h('span', { className: 'mole-spinner' }), label ? h('span', null, label) : null)
    }

    function Empty({ icon = 'sparkle', title, children }) {
      return h('div', { className: 'mole-empty' }, h(Icon, { name: icon, size: 26 }), h('p', { className: 'mole-empty-title' }, title), children ? h('p', { className: 'mole-empty-sub' }, children) : null)
    }

    function ErrorNote({ children, onRetry, retryLabel }) {
      return h('div', { className: 'mole-error' }, h(Icon, { name: 'warning', size: 16 }), h('span', null, children), onRetry ? h(Btn, { size: 'sm', variant: 'ghost', onClick: onRetry }, retryLabel ?? 'Retry') : null)
    }

    function Modal({ title, onClose, children, footer }) {
      useEffect(() => {
        const onKey = (event) => {
          if (event.key === 'Escape') onClose()
        }
        document.addEventListener('keydown', onKey)
        return () => document.removeEventListener('keydown', onKey)
      }, [onClose])
      return h(
        'div',
        {
          className: 'mole-modal-backdrop',
          role: 'dialog',
          'aria-modal': 'true',
          onClick: (event) => {
            if (event.target === event.currentTarget) onClose()
          },
        },
        h(
          'div',
          { className: 'mole-modal' },
          h('header', { className: 'mole-modal-head' }, h('h2', null, title), h('button', { type: 'button', className: 'mole-icon-btn', onClick: onClose, 'aria-label': 'Close' }, h(Icon, { name: 'close', size: 16 }))),
          h('div', { className: 'mole-modal-body' }, children),
          footer ? h('footer', { className: 'mole-modal-foot' }, footer) : null,
        ),
      )
    }

    function Output({ text, tone = 'default', collapsed }) {
      const body = useStable(String(text ?? '').trim().split(/\r?\n/))
      const [open, setOpen] = useState(!collapsed)
      if (body.length === 0 || (body.length === 1 && body[0] === '')) return null
      return h(
        'div',
        { className: `mole-output mole-output-${tone}` },
        h(
          'div',
          { className: 'mole-output-head' },
          h('span', null, isZhLocale() ? `输出 · 共 ${body.length} 行` : `输出 / output · ${body.length} lines`),
          h('button', { type: 'button', className: 'mole-link-btn', onClick: () => setOpen((value) => !value) }, open ? '收起' : '展开'),
        ),
        open ? h('pre', { className: 'mole-pre' }, body.map(zhCliLine).join('\n')) : null,
      )
    }

    // ------------------------------------------------------------------
    // status tab
    // ------------------------------------------------------------------
    function Stat({ label, value, sub, tone }) {
      return h('div', { className: 'mole-stat' }, h('div', { className: 'mole-stat-label' }, label), h('div', { className: `mole-stat-value ${tone ? `mole-tone-text-${tone}` : ''}`.trim() }, value), sub ? h('div', { className: 'mole-stat-sub' }, sub) : null)
    }

    function StatusTab({ t }) {
      const [state, setState] = useState({ phase: 'loading', data: null, error: null })
      const [paused, setPaused] = useState(false)
      const mounted = useRef(true)

      const load = useCallback(async (signal) => {
        try {
          const payload = await requestJson('/dsh-mole/status', { signal })
          if (!mounted.current) return
          setState({ phase: 'ready', data: payload, error: null })
        } catch (error) {
          if (error?.name === 'AbortError') return
          if (mounted.current) setState((previous) => ({ phase: previous.data ? 'ready' : 'error', data: previous.data, error: String(error?.message ?? error) }))
        }
      }, [])

      useEffect(() => {
        mounted.current = true
        const controller = new AbortController()
        load(controller.signal)
        return () => {
          mounted.current = false
          controller.abort()
        }
      }, [load])

      useEffect(() => {
        if (paused) return undefined
        const timer = setInterval(() => load(), 3000)
        return () => clearInterval(timer)
      }, [paused, load])

      const status = state.data?.status
      const mole = state.data?.mole
      if (state.phase === 'loading') return h('div', { className: 'mole-loading' }, h(Spinner, { label: t('common.loading') }))
      if (!status) {
        return h(
          Card,
          { title: t('status.unavailable.title') },
          h(ErrorNote, { children: state.error ?? t('status.unavailable.body') }),
          h('div', { className: 'mole-actions' }, h(Btn, { onClick: () => load(), icon: 'refresh' }, t('common.retry'))),
        )
      }

      const cpu = status.cpu ?? {}
      const memory = status.memory ?? {}
      const disks = useStable(status.disks)
      const disk = disks[0] ?? {}
      const io = status.disk_io ?? {}
      const network = useStable(status.network).filter((entry) => !/^(lo|utun|awdl|llw|bridge)/.test(entry.name ?? ''))
      const activeNetwork = network.find((entry) => (entry.ip ?? '') !== '') ?? network[0] ?? {}
      const battery = useStable(status.batteries)[0]
      const thermal = status.thermal ?? {}
      const hardware = status.hardware ?? {}
      const health = Number(status.health_score)
      const healthTone = health >= 85 ? 'ok' : health >= 60 ? 'warn' : 'bad'
      const processes = useStable(status.top_processes).slice(0, 10)
      const maxCpu = Math.max(1, ...processes.map((entry) => Number(entry.cpu) || 0))
      const diskPercent = Number(disk.used_percent) || (disk.total ? (disk.used / disk.total) * 100 : 0)

      return h(
        'div',
        { className: 'mole-status' },
        h(
          'div',
          { className: 'mole-status-top' },
          h(
            Card,
            { className: 'mole-health', tone: 'hero' },
            h(
              'div',
              { className: 'mole-health-main' },
              h(
                'div',
                null,
                h('div', { className: 'mole-eyebrow' }, h(Icon, { name: 'health', size: 14 }), t('status.health')),
                h('div', { className: 'mole-health-score' }, h('span', { className: `mole-tone-text-${healthTone}` }, Number.isFinite(health) ? health : '—'), h('span', { className: 'mole-health-score-sub' }, zhMapValue(HEALTH_ZH, status.health_score_msg ?? ''))),
                h('p', { className: 'mole-health-note' }, status.health_score_msg ? t('status.health.note') : ''),
                h('div', { className: 'mole-health-foot' }, h('span', null, t('status.uptime', { value: zhDuration(status.uptime ?? fmtDuration(status.uptime_seconds)) })), h('span', null, t('status.procs', { value: status.procs ?? '—' }))),
              ),
              h(Planet, { variant: 'sun', size: 150 }),
            ),
          ),
          h(
            Card,
            { className: 'mole-card-metric' },
            h('div', { className: 'mole-metric-head' }, h('span', { className: 'mole-eyebrow' }, h(Icon, { name: 'cpu', size: 14 }), 'CPU'), h('span', { className: 'mole-chip' }, t('status.cores', { value: cpu.logical_cpu ?? cpu.core_count ?? '—' }))),
            h('div', { className: 'mole-metric-value' }, fmtPercent(cpu.usage, 1)),
            h('div', { className: 'mole-core-bars' }, useStable(cpu.per_core).map((value, index) => h('span', { key: index, className: 'mole-core-bar', style: { height: `${Math.max(8, Math.min(100, Number(value) || 0))}%` } }))),
            h('div', { className: 'mole-metric-foot' }, h('span', null, `${t('status.load')} ${[cpu.load1, cpu.load5, cpu.load15].map((value) => (Number.isFinite(Number(value)) ? Number(value).toFixed(2) : '—')).join(' / ')}`)),
          ),
          h(
            Card,
            { className: 'mole-card-metric' },
            h('div', { className: 'mole-metric-head' }, h('span', { className: 'mole-eyebrow' }, h(Icon, { name: 'memory', size: 14 }), t('status.memory')), h('span', { className: 'mole-chip' }, memory.pressure ? `${t('status.pressure')} ${zhMapValue(PRESSURE_ZH, memory.pressure)}` : '')),
            h('div', { className: 'mole-metric-value' }, fmtPercent(memory.used_percent, 1)),
            h(Bar, { value: memory.used_percent, tone: Number(memory.used_percent) > 85 ? 'bad' : Number(memory.used_percent) > 70 ? 'warn' : 'accent' }),
            h('div', { className: 'mole-metric-foot' }, h('span', null, `${fmtBytes(memory.used)} / ${fmtBytes(memory.total)}`), h('span', null, `${t('status.swap')} ${fmtBytes(memory.swap_used)}`)),
          ),
          h(
            Card,
            { className: 'mole-card-metric' },
            h('div', { className: 'mole-metric-head' }, h('span', { className: 'mole-eyebrow' }, h(Icon, { name: 'disk', size: 14 }), t('status.disk')), h('span', { className: 'mole-chip' }, zhMapValue(SMART_ZH, disk.smart_status ?? ''))),
            h('div', { className: 'mole-metric-value' }, fmtBytes(disk.used)),
            h(Bar, { value: diskPercent, tone: diskPercent > 88 ? 'bad' : diskPercent > 75 ? 'warn' : 'accent' }),
            h('div', { className: 'mole-metric-foot' }, h('span', null, t('status.disk.free', { value: fmtBytes((disk.total ?? 0) - (disk.used ?? 0)) })), h('span', null, t('status.diskio', { read: Number(io.read_rate ?? 0).toFixed(1), write: Number(io.write_rate ?? 0).toFixed(1) }))),
          ),
          h(
            Card,
            { className: 'mole-card-metric' },
            h('div', { className: 'mole-metric-head' }, h('span', { className: 'mole-eyebrow' }, h(Icon, { name: 'network', size: 14 }), t('status.network')), h('span', { className: 'mole-chip' }, activeNetwork.name ?? '—')),
            h('div', { className: 'mole-metric-value' }, `${Number(activeNetwork.rx_rate_mbs ?? 0).toFixed(2)} MB/s`),
            h(
              'div',
              { className: 'mole-net-row' },
              h('span', { className: 'mole-tone-text-ok' }, `↓ ${Number(activeNetwork.rx_rate_mbs ?? 0).toFixed(2)}`),
              h('span', { className: 'mole-tone-text-accent' }, `↑ ${Number(activeNetwork.tx_rate_mbs ?? 0).toFixed(2)}`),
            ),
            h('div', { className: 'mole-metric-foot' }, h('span', null, activeNetwork.ip || t('status.network.noip'))),
          ),
          battery
            ? h(
                Card,
                { className: 'mole-card-metric' },
                h('div', { className: 'mole-metric-head' }, h('span', { className: 'mole-eyebrow' }, h(Icon, { name: 'battery', size: 14 }), t('status.battery')), h('span', { className: 'mole-chip' }, zhMapValue(BATTERY_HEALTH_ZH, battery.health ?? ''))),
                h('div', { className: 'mole-metric-value' }, fmtPercent(battery.percent)),
                h(Bar, { value: battery.percent, tone: Number(battery.percent) > 30 ? 'ok' : 'warn' }),
                h('div', { className: 'mole-metric-foot' }, h('span', null, zhMapValue(BATTERY_STATUS_ZH, battery.status ?? '')), h('span', null, t('status.battery.cycles', { value: battery.cycle_count ?? '—' }))),
              )
            : null,
          h(
            Card,
            { className: 'mole-card-metric' },
            h('div', { className: 'mole-metric-head' }, h('span', { className: 'mole-eyebrow' }, h(Icon, { name: 'thermal', size: 14 }), t('status.thermal')), h('span', { className: 'mole-chip' }, thermal.fan_count ? t('status.fan.count', { value: thermal.fan_count }) : t('status.thermal.fanless') ?? '')),
            h('div', { className: 'mole-metric-value' }, `${Number(thermal.cpu_temp ?? 0).toFixed(0)}°C`),
            h('div', { className: 'mole-metric-foot' }, h('span', null, t('status.battery.temp', { value: Number(thermal.battery_temp ?? 0).toFixed(0) })), h('span', null, t('status.fan.speed', { value: Number(thermal.fan_speed ?? 0).toFixed(0) }))),
            h('div', { className: 'mole-metric-foot' }, h('span', null, t('status.power', { value: Number(thermal.system_power ?? 0).toFixed(1) })), h('span', null, t('status.adapter', { value: Number(thermal.adapter_power ?? 0).toFixed(0) }))),
          ),
          h(
            Card,
            { className: 'mole-card-metric' },
            h('div', { className: 'mole-metric-head' }, h('span', { className: 'mole-eyebrow' }, h(Icon, { name: 'trash', size: 14 }), t('status.trash')), h('span', { className: 'mole-chip' }, t('status.trash.zombie', { value: status.zombie_count ?? 0 }))),
            h('div', { className: 'mole-metric-value' }, fmtBytes(status.trash_size)),
            h('div', { className: 'mole-metric-foot' }, h('span', null, t('status.trash.note')), h('span', null, status.trash_approx ? '≈' : '')),
          ),
          h(
            Card,
            { className: 'mole-card-metric mole-card-device' },
            h('div', { className: 'mole-metric-head' }, h('span', { className: 'mole-eyebrow' }, h(Icon, { name: 'cpu', size: 14 }), t('status.device')), h('span', { className: 'mole-chip' }, zhDuration(status.uptime ?? '—'))),
            h('div', { className: 'mole-device-name' }, hardware.model ?? status.host ?? '—'),
            h('div', { className: 'mole-device-lines' }, h('span', null, hardware.cpu_model ?? '—'), h('span', null, hardware.total_ram ?? ''), h('span', null, hardware.os_version ?? status.platform ?? ''), h('span', null, t('status.procs', { value: status.procs ?? '—' }))),
          ),
        ),
        h(
          Card,
          {
            title: t('status.processes'),
            subtitle: t('status.processes.sub'),
            right: h(
              'div',
              { className: 'mole-inline' },
              h(Btn, { size: 'sm', variant: 'ghost', icon: 'refresh', onClick: () => load() }, paused ? t('common.refresh') : t('status.refresh')),
              h(Btn, { size: 'sm', variant: 'ghost', onClick: () => setPaused((value) => !value) }, paused ? t('common.resume') : t('common.pause')),
            ),
          },
          h(
            'div',
            { className: 'mole-table mole-process-table' },
            h('div', { className: 'mole-tr mole-th' }, h('span', null, t('status.col.process')), h('span', null, t('status.col.cpu')), h('span', null, t('status.col.memory')), h('span', null, 'PID')),
            processes.map((entry) =>
              h(
                'div',
                { className: 'mole-tr', key: `${entry.pid}-${entry.name}` },
                h('span', { className: 'mole-cell-name', title: entry.command ?? entry.name }, entry.name),
                h('span', { className: 'mole-cell-cpu' }, h('span', { className: 'mole-mini-bar' }, h('span', { style: { width: `${Math.max(2, (Number(entry.cpu) / maxCpu) * 100)}%` } })), ` ${Number(entry.cpu ?? 0).toFixed(1)}%`),
                h('span', null, fmtBytes(entry.memory_bytes ?? (Number(entry.memory) || 0) * 1024 ** 2)),
                h('span', { className: 'mole-cell-muted' }, entry.pid),
              ),
            ),
          ),
          h('p', { className: 'mole-footnote' }, t('status.footnote')),
        ),
      )
    }

    // ------------------------------------------------------------------
    // cleanup tab
    // ------------------------------------------------------------------
    function CleanTab({ t, onHistoryChanged }) {
      const [preview, setPreview] = usePersistentState('clean.preview', { phase: 'idle', data: null, error: null })
      const [runState, setRunState] = usePersistentState('clean.run', { phase: 'idle', data: null, error: null })
      const [confirming, setConfirming] = useState(false)
      const [showList, setShowList] = useState(false)


      const scan = useCallback(async () => {
        setPreview({ phase: 'running', data: null, error: null })
        try {
          const payload = await runJob({ action: 'preview', kind: 'clean' }, t, 'progress.preview.clean')
          setPreview({ phase: 'done', data: payload, error: null })
        } catch (error) {
          setPreview({ phase: 'error', data: null, error: String(error?.message ?? error) })
        }
      }, [t])

      const run = useCallback(async () => {
        setConfirming(false)
        setRunState({ phase: 'running', data: null, error: null })
        try {
          const payload = await runJob({ action: 'run', kind: 'clean', confirm: 'Mole' }, t, 'progress.run.clean')
          setRunState({ phase: 'done', data: payload, error: null })
          onHistoryChanged?.()
        } catch (error) {
          setRunState({ phase: 'error', data: null, error: String(error?.message ?? error) })
        }
      }, [onHistoryChanged, t])

      async function openTerminal(action) {
        try {
          await requestJson('/dsh-mole/open-terminal', { method: 'POST', body: { action } })
        } catch (error) {
          setPreview((previous) => ({ ...previous, error: String(error?.message ?? error) }))
        }
      }

      const parsed = preview.data ? parsePreview(preview.data.output) : null
      const cleanList = preview.data?.cleanList
      const listPaths = cleanList ? String(cleanList.text ?? '').split('\n').filter((line) => line && !line.trim().startsWith('#')).length : 0
      const running = preview.phase === 'running'
      const done = preview.phase === 'done'

      return h(
        'div',
        { className: 'mole-clean' },
        h(
          'div',
          { className: 'mole-hero mole-hero-clean' },
          h('span', { className: 'mole-stars', 'aria-hidden': true }),
          h(Planet, { variant: 'earth', size: 250 }),
          h(
            'div',
            { className: 'mole-hero-copy' },
            h('h2', { className: 'mole-hero-title' }, done && parsed ? (parsed.totalBytes > 0 ? t('clean.found', { size: fmtBytes(parsed.totalBytes) }) : t('clean.nothing')) : t('clean.hero.title')),
            h(
              'p',
              { className: 'mole-hero-sub' },
              running
                ? t('clean.scanning')
                : done && parsed
                  ? t('clean.summary', { groups: parsed.groups.length, items: parsed.itemCount || parsed.itemLines })
                  : t('clean.hero.sub'),
            ),
            h(
              'div',
              { className: 'mole-hero-actions' },
              h(Btn, { variant: 'hero', size: 'lg', onClick: scan, disabled: running }, running ? t('clean.scanning.btn') : done ? t('clean.rescan') : t('clean.scan')),
              done ? h(Btn, { variant: 'hero-ghost', size: 'lg', onClick: () => setConfirming(true), disabled: runState.phase === 'running' }, t('clean.run')) : null,
            ),
            parsed?.systemLine ? h('p', { className: 'mole-hero-meta' }, parsed.systemLine) : null,
          ),
        ),
        preview.phase === 'error' ? h(ErrorNote, { children: preview.error, onRetry: scan, retryLabel: t('common.retry') }) : null,
        runState.phase === 'running'
          ? h(Card, { className: 'mole-running' }, h(Spinner, { label: t('clean.running') }), h('p', { className: 'mole-footnote' }, t('clean.running.sub')))
          : null,
        runState.phase === 'done'
          ? h(
              Card,
              {
                title: t('clean.done.title'),
                right: h(Btn, { size: 'sm', variant: 'ghost', icon: 'refresh', onClick: () => setRunState({ phase: 'idle', data: null, error: null }) }, t('common.close')),
              },
              h('div', { className: 'mole-summary-list' }, summarizeRun(runState.data.output).map((line, index) => h('div', { className: 'mole-summary-row', key: index }, line))),
              h('div', { className: 'mole-actions' }, h(Btn, { variant: 'ghost', icon: 'history', onClick: () => onHistoryChanged?.() }, t('clean.seeHistory'))),
              h(Output, { text: runState.data.output, collapsed: true }),
            )
          : null,
        runState.phase === 'error' ? h(ErrorNote, { children: runState.error, onRetry: () => setRunState({ phase: 'idle', data: null, error: null }), retryLabel: t('common.close') }) : null,
        done && parsed
          ? h(
              'div',
              { className: 'mole-preview-grid' },
              h(
                Card,
                {
                  title: t('clean.groups.title'),
                  subtitle: t('clean.groups.sub'),
                  right: h(
                    'div',
                    { className: 'mole-inline' },
                    cleanList
                      ? h(Btn, { size: 'sm', variant: 'ghost', icon: 'file', onClick: () => setShowList((value) => !value) }, t('clean.pathList', { count: listPaths }))
                      : null,
                    h(Btn, { size: 'sm', variant: 'ghost', icon: 'terminal', onClick: () => openTerminal('clean') }, t('clean.openTerminal')),
                  ),
                },
                showList && cleanList
                  ? h('pre', { className: 'mole-pre mole-pre-tall' }, String(cleanList.text ?? '').split('\n').slice(0, 400).join('\n'))
                  : h(
                      'div',
                      { className: 'mole-groups' },
                      parsed.groups.map((group, groupIndex) =>
                        h(
                          'div',
                          { className: 'mole-group', key: `${group.title}-${groupIndex}` },
                          h(
                            'div',
                            { className: 'mole-group-head' },
                            h('span', { className: 'mole-group-title' }, group.title),
                            h(
                              'span',
                              { className: 'mole-group-count' },
                              t('clean.group.count', {
                                count: group.items.length,
                                size: fmtBytes(group.items.reduce((sum, item) => sum + (item.sizeBytes || 0), 0)),
                              }),
                            ),
                          ),
                          group.note ? h('p', { className: 'mole-group-note' }, group.note) : null,
                          group.items.map((item, itemIndex) =>
                            h(
                              'div',
                              { className: `mole-item mole-item-${markTone(item.mark)}`, key: `${item.name}-${itemIndex}` },
                              h('span', { className: 'mole-item-mark' }, item.mark),
                              h('span', { className: 'mole-item-name' }, item.name, item.detail ? h('span', { className: 'mole-item-detail' }, item.detail) : null, item.note ? h('span', { className: 'mole-item-detail' }, item.note) : null),
                              h('span', { className: 'mole-item-size' }, item.sizeText || ''),
                            ),
                          ),
                        ),
                      ),
                    ),
              ),
              h(
                Card,
                { title: t('clean.notice.title'), className: 'mole-notice-card' },
                h('ul', { className: 'mole-list' }, ['clean.notice.trash', 'clean.notice.permanent', 'clean.notice.sudo', 'clean.notice.rescan'].map((key) => h('li', { key }, t(key)))),
                h('p', { className: 'mole-footnote' }, t('clean.notice.foot')),
              ),
            )
          : null,
        parsed && parsed.notes.length > 0 ? h(Output, { text: parsed.notes.join('\n'), collapsed: true }) : null,
        confirming
          ? h(Modal, {
              title: t('clean.confirm.title'),
              onClose: () => setConfirming(false),
              children: h(
                'div',
                null,
                h('p', null, t('clean.confirm.body', { size: fmtBytes(parsed?.totalBytes ?? 0), items: parsed?.itemLines ?? 0 })),
                h('ul', { className: 'mole-list' }, h('li', null, t('clean.notice.trash')), h('li', null, t('clean.notice.permanent')), h('li', null, t('clean.notice.rescan'))),
                h('p', { className: 'mole-footnote' }, t('clean.confirm.foot')),
              ),
              footer: h(
                'div',
                { className: 'mole-modal-actions' },
                h(Btn, { variant: 'ghost', onClick: () => setConfirming(false) }, t('common.cancel')),
                h(Btn, { variant: 'danger', onClick: run }, t('clean.confirm.run')),
              ),
            })
          : null,
      )
    }

    function markTone(mark) {
      if (mark === '✓') return 'ok'
      if (mark === '○') return 'todo'
      if (mark === '◎') return 'skip'
      if (mark === '⊙') return 'warn'
      return 'note'
    }

    // ------------------------------------------------------------------
    // apps tab
    // ------------------------------------------------------------------
    function AppsTab({ t }) {
      const [state, setState] = useState({ phase: 'loading', data: null, error: null })
      const [query, setQuery] = useState('')
      const [selected, setSelected] = useState(null)
      const [plan, setPlan] = useState({ phase: 'idle', data: null, error: null })
      const [notice, setNotice] = useState(null)

      const load = useCallback(async (refresh) => {
        setState((previous) => ({ phase: previous.data ? 'ready' : 'loading', data: previous.data, error: null }))
        try {
          const payload = await requestJson(`/dsh-mole/apps${refresh ? '?refresh=1' : ''}`)
          setState({ phase: 'ready', data: payload, error: null })
        } catch (error) {
          setState((previous) => ({ phase: 'error', data: previous.data, error: String(error?.message ?? error) }))
        }
      }, [])

      useEffect(() => {
        load(false)
      }, [load])

      const apps = useStable(state.data?.apps)
      const filtered = useMemo(() => {
        const needle = query.trim().toLowerCase()
        const list = needle ? apps.filter((app) => `${app.name} ${app.bundle_id} ${app.path}`.toLowerCase().includes(needle)) : apps.slice()
        return list.sort((left, right) => parseSizeText(right.size) - parseSizeText(left.size))
      }, [apps, query])

      const inspect = useCallback(async (app) => {
        setSelected(app)
        setPlan({ phase: 'loading', data: null, error: null })
        try {
          const payload = await requestJson('/dsh-mole/uninstall-plan', { method: 'POST', body: { name: app.uninstall_name ?? app.name } })
          setPlan({ phase: 'done', data: payload, error: null })
        } catch (error) {
          setPlan({ phase: 'error', data: null, error: String(error?.message ?? error) })
        }
      }, [])

      async function handoff(action) {
        if (!selected) return
        try {
          const payload = await requestJson('/dsh-mole/open-terminal', { method: 'POST', body: { action, name: selected.uninstall_name ?? selected.name } })
          setNotice(t('apps.handoff', { command: payload.command }))
        } catch (error) {
          setNotice(String(error?.message ?? error))
        }
      }

      const totalSize = filtered.reduce((sum, app) => sum + parseSizeText(app.size), 0)

      return h(
        'div',
        { className: 'mole-apps' },
        h(
          Card,
          {
            title: t('apps.title'),
            subtitle: t('apps.subtitle', { count: filtered.length, size: fmtBytes(totalSize) }),
            right: h(
              'div',
              { className: 'mole-inline' },
              h(
                'label',
                { className: 'mole-search' },
                h(Icon, { name: 'search', size: 15 }),
                h('input', { value: query, onChange: (event) => setQuery(event.target.value), placeholder: t('apps.search'), 'aria-label': t('apps.search') }),
              ),
              h(Btn, { size: 'sm', variant: 'ghost', icon: 'refresh', onClick: () => load(true), disabled: state.phase === 'loading' }, t('common.refresh')),
            ),
          },
          state.phase === 'error' ? h(ErrorNote, { children: state.error, onRetry: () => load(false) }) : null,
          state.phase === 'loading' && !state.data ? h('div', { className: 'mole-loading' }, h(Spinner, { label: t('apps.loading') })) : null,
          apps.length === 0 && state.phase === 'ready'
            ? h(Empty, { icon: 'apps', title: t('apps.empty') })
            : h(
                'div',
                { className: 'mole-table mole-app-table' },
                h('div', { className: 'mole-tr mole-th' }, h('span', null, t('apps.col.app')), h('span', null, t('apps.col.source')), h('span', null, t('apps.col.size')), h('span', null, '')),
                filtered.map((app) =>
                  h(
                    'button',
                    {
                      type: 'button',
                      key: `${app.path}-${app.bundle_id}`,
                      className: `mole-tr mole-app-row ${selected && selected.path === app.path ? 'is-selected' : ''}`,
                      onClick: () => inspect(app),
                    },
                    h(
                      'span',
                      { className: 'mole-app-name' },
                      h('span', { className: 'mole-app-avatar', style: { background: avatarColor(app.name) } }, String(app.name ?? '?').slice(0, 1).toUpperCase()),
                      h('span', { className: 'mole-app-text' }, h('span', { className: 'mole-app-label' }, app.name), h('span', { className: 'mole-app-path' }, shortPath(app.path, 4))),
                    ),
                    h('span', { className: `mole-chip ${app.source === 'Homebrew' ? 'mole-chip-brew' : ''}` }, app.source ?? 'App'),
                    h('span', { className: 'mole-app-size' }, app.size ?? '—'),
                    h(Icon, { name: 'chevron', size: 15 }),
                  ),
                ),
              ),
        ),
        notice ? h('div', { className: 'mole-toast' }, h(Icon, { name: 'terminal', size: 16 }), h('span', null, notice), h('button', { type: 'button', className: 'mole-icon-btn', onClick: () => setNotice(null) }, h(Icon, { name: 'close', size: 14 }))) : null,
        selected
          ? h(
              Card,
              {
                title: selected.name,
                subtitle: t('apps.plan.sub', { source: selected.source ?? 'App' }),
                className: 'mole-app-detail',
                right: h(
                  'div',
                  { className: 'mole-inline' },
                  h(Btn, { size: 'sm', variant: 'ghost', icon: 'terminal', onClick: () => handoff('uninstall') }, t('apps.uninstall')),
                  h(Btn, { size: 'sm', variant: 'ghost', icon: 'trash', onClick: () => handoff('uninstall-data') }, t('apps.uninstallData')),
                  h(Btn, { size: 'sm', variant: 'ghost', icon: 'close', onClick: () => setSelected(null) }, t('common.close')),
                ),
              },
              plan.phase === 'loading' ? h(Spinner, { label: t('apps.plan.loading') }) : null,
              plan.phase === 'error' ? h(ErrorNote, { children: plan.error }) : null,
              plan.phase === 'done' ? h('pre', { className: 'mole-pre' }, plan.data.output) : null,
              h('p', { className: 'mole-footnote' }, t('apps.plan.foot')),
            )
          : null,
      )
    }

    function avatarColor(name) {
      let hash = 0
      for (const char of String(name ?? '')) hash = (hash * 31 + char.codePointAt(0)) % 360
      return `linear-gradient(140deg, hsl(${hash} 62% 52%), hsl(${(hash + 48) % 360} 58% 42%))`
    }

    // ------------------------------------------------------------------
    // optimize tab
    // ------------------------------------------------------------------
    function OptimizeTab({ t, onHistoryChanged }) {
      const [preview, setPreview] = usePersistentState('optimize.preview', { phase: 'idle', data: null, error: null })
      const [runState, setRunState] = usePersistentState('optimize.run', { phase: 'idle', data: null, error: null })
      const [confirming, setConfirming] = useState(false)

      const scan = useCallback(async () => {
        setPreview({ phase: 'running', data: null, error: null })
        try {
          const payload = await runJob({ action: 'preview', kind: 'optimize' }, t, 'progress.preview.optimize')
          setPreview({ phase: 'done', data: payload, error: null })
        } catch (error) {
          setPreview({ phase: 'error', data: null, error: String(error?.message ?? error) })
        }
      }, [t])

      const run = useCallback(async () => {
        setConfirming(false)
        setRunState({ phase: 'running', data: null, error: null })
        try {
          const payload = await runJob({ action: 'run', kind: 'optimize', confirm: 'Mole' }, t, 'progress.run.optimize')
          setRunState({ phase: 'done', data: payload, error: null })
          onHistoryChanged?.()
        } catch (error) {
          setRunState({ phase: 'error', data: null, error: String(error?.message ?? error) })
        }
      }, [onHistoryChanged, t])

      async function openTerminal(action) {
        try {
          await requestJson('/dsh-mole/open-terminal', { method: 'POST', body: { action } })
        } catch (error) {
          setPreview((previous) => ({ ...previous, error: String(error?.message ?? error) }))
        }
      }

      const parsed = preview.data ? parsePreview(preview.data.output) : null
      const running = preview.phase === 'running'

      return h(
        'div',
        { className: 'mole-optimize' },
        h(
          'div',
          { className: 'mole-hero mole-hero-optimize' },
          h('span', { className: 'mole-stars', 'aria-hidden': true }),
          h(Planet, { variant: 'jupiter', size: 210 }),
          h(
            'div',
            { className: 'mole-hero-copy' },
            h('h2', { className: 'mole-hero-title' }, t('optimize.hero.title')),
            h('p', { className: 'mole-hero-sub' }, running ? t('optimize.scanning') : preview.phase === 'done' && parsed ? t('optimize.summary', { groups: parsed.groups.length }) : t('optimize.hero.sub')),
            h(
              'div',
              { className: 'mole-hero-actions' },
              h(Btn, { variant: 'hero', size: 'lg', onClick: scan, disabled: running }, running ? t('optimize.scanning.btn') : preview.phase === 'done' ? t('optimize.rescan') : t('optimize.scan')),
              preview.phase === 'done' ? h(Btn, { variant: 'hero-ghost', size: 'lg', onClick: () => setConfirming(true), disabled: runState.phase === 'running' }, t('optimize.run')) : null,
              h(Btn, { variant: 'hero-ghost', size: 'lg', icon: 'terminal', onClick: () => openTerminal('optimize') }, t('clean.openTerminal')),
            ),
          ),
        ),
        preview.phase === 'error' ? h(ErrorNote, { children: preview.error, onRetry: scan }) : null,
        runState.phase === 'running' ? h(Card, { className: 'mole-running' }, h(Spinner, { label: t('optimize.running') })) : null,
        runState.phase === 'done'
          ? h(
              Card,
              { title: t('optimize.done.title'), right: h(Btn, { size: 'sm', variant: 'ghost', onClick: () => setRunState({ phase: 'idle', data: null, error: null }) }, t('common.close')) },
              h('div', { className: 'mole-summary-list' }, summarizeRun(runState.data.output).map((line, index) => h('div', { className: 'mole-summary-row', key: index }, line))),
              h(Output, { text: runState.data.output, collapsed: true }),
            )
          : null,
        runState.phase === 'error' ? h(ErrorNote, { children: runState.error }) : null,
        parsed
          ? h(
              Card,
              { title: t('optimize.tasks.title'), subtitle: t('optimize.tasks.sub') },
              parsed.groups.map((group, groupIndex) =>
                h(
                  'div',
                  { className: 'mole-opt-group', key: `${group.title}-${groupIndex}` },
                  h('div', { className: 'mole-opt-title' }, group.title),
                  group.items.map((item, itemIndex) =>
                    h(
                      'div',
                      { className: `mole-item mole-item-${markTone(item.mark)}`, key: `${item.name}-${itemIndex}` },
                      h('span', { className: 'mole-item-mark' }, item.mark),
                      h('span', { className: 'mole-item-name' }, item.name, item.detail ? h('span', { className: 'mole-item-detail' }, item.detail) : null, item.note ? h('span', { className: 'mole-item-detail' }, item.note) : null),
                    ),
                  ),
                ),
              ),
              h('p', { className: 'mole-footnote' }, t('optimize.footnote')),
            )
          : null,
        confirming
          ? h(Modal, {
              title: t('optimize.confirm.title'),
              onClose: () => setConfirming(false),
              children: h('div', null, h('p', null, t('optimize.confirm.body')), h('ul', { className: 'mole-list' }, h('li', null, t('optimize.notice.admin')), h('li', null, t('optimize.notice.skip')), h('li', null, t('optimize.notice.vpn')))),
              footer: h(
                'div',
                { className: 'mole-modal-actions' },
                h(Btn, { variant: 'ghost', onClick: () => setConfirming(false) }, t('common.cancel')),
                h(Btn, { variant: 'danger', onClick: run }, t('optimize.confirm.run')),
              ),
            })
          : null,
      )
    }

    // ------------------------------------------------------------------
    // analyze tab
    // ------------------------------------------------------------------
    function treemapLayout(items, x, y, width, height, output) {
      if (!items.length || width <= 1 || height <= 1) return
      if (items.length === 1) {
        const item = items[0]
        output.push({ ...item, x, y, width, height })
        return
      }
      let total = 0
      for (const item of items) total += item.size
      if (total <= 0) return
      let accumulated = 0
      let split = 1
      for (let index = 0; index < items.length - 1; index += 1) {
        accumulated += items[index].size
        split = index + 1
        if (accumulated >= total / 2) break
      }
      const first = items.slice(0, split)
      const second = items.slice(split)
      if (second.length === 0) {
        output.push({ ...items[0], x, y, width, height })
        return
      }
      const firstTotal = first.reduce((sum, item) => sum + item.size, 0)
      const ratio = Math.min(0.85, Math.max(0.15, firstTotal / total))
      if (width >= height) {
        treemapLayout(first, x, y, width * ratio, height, output)
        treemapLayout(second, x + width * ratio, y, width * (1 - ratio), height, output)
      } else {
        treemapLayout(first, x, y, width, height * ratio, output)
        treemapLayout(second, x, y + height * ratio, width, height * (1 - ratio), output)
      }
    }

    const TREEMAP_COLORS = ['#4b7f6f', '#5f8f5a', '#8a7a4a', '#8a5f4a', '#7b6f8f', '#4f7f8f', '#6f8f4f', '#8f6f8f', '#5f7f8f', '#7f8f6f']

    function AnalyzeTab({ t }) {
      const [path, setPath] = usePersistentState('analyze.path', '')
      const [state, setState] = usePersistentState('analyze.state', { phase: 'idle', data: null, error: null })
      const [input, setInput] = useState('')

      const load = useCallback(async (target) => {
        setState({ phase: 'running', data: null, error: null })
        try {
          const payload = await runJob({ action: 'analyze', path: target || undefined }, t, 'progress.analyze')
          setState({ phase: 'done', data: payload, error: null })
          setPath(payload.path ?? target ?? '')
        } catch (error) {
          setState({ phase: 'error', data: null, error: String(error?.message ?? error) })
        }
      }, [t])

      const report = state.data?.report
      const entries = useStable(report?.entries).filter((entry) => Number(entry.size) > 0)
      const sorted = useMemo(() => entries.slice().sort((left, right) => Number(right.size) - Number(left.size)), [entries])
      const treemap = useMemo(() => {
        if (!sorted.length) return []
        const output = []
        treemapLayout(sorted.map((entry) => ({ ...entry, size: Number(entry.size) || 1 })), 0, 0, 1000, 520, output)
        return output
      }, [sorted])
      const maxSize = sorted.length ? Number(sorted[0].size) : 1
      const largeFiles = useStable(report?.large_files).slice(0, 12)
      const scanTone = report?.scan_status === 'complete' ? 'ok' : report?.scan_status === 'partial' ? 'warn' : 'bad'

      async function openPath(target) {
        try {
          await requestJson('/dsh-mole/open-path', { method: 'POST', body: { path: target } })
        } catch {
          // opening Finder is a convenience; ignore a failure
        }
      }

      function enter(entry) {
        if (!entry.is_dir) {
          openPath(entry.path)
          return
        }
        load(entry.path)
      }

      return h(
        'div',
        { className: 'mole-analyze' },
        h(
          Card,
          {
            title: t('analyze.title'),
            subtitle: t('analyze.subtitle'),
            right: h(
              'div',
              { className: 'mole-inline' },
              h(
                'label',
                { className: 'mole-search mole-search-wide' },
                h(Icon, { name: 'folder', size: 15 }),
                h('input', { value: input, onChange: (event) => setInput(event.target.value), placeholder: t('analyze.placeholder'), onKeyDown: (event) => { if (event.key === 'Enter') load(input.trim() || undefined) } }),
              ),
              h(Btn, { size: 'sm', onClick: () => load(input.trim() || undefined), disabled: state.phase === 'running', icon: 'analyze' }, t('analyze.scan')),
              path ? h(Btn, { size: 'sm', variant: 'ghost', icon: 'open', onClick: () => openPath(path) }, t('analyze.reveal')) : null,
            ),
          },
          state.phase === 'idle' ? h(Empty, { icon: 'analyze', title: t('analyze.empty.title'), children: t('analyze.empty.sub') }) : null,
          state.phase === 'running' ? h('div', { className: 'mole-loading' }, h(Spinner, { label: t('analyze.scanning') }), h('p', { className: 'mole-footnote' }, t('analyze.scanning.sub'))) : null,
          state.phase === 'error' ? h(ErrorNote, { children: state.error, onRetry: () => load(path || undefined) }) : null,
          report
            ? h(
                'div',
                null,
                h(
                  'div',
                  { className: 'mole-analyze-bar' },
                  h('div', { className: 'mole-breadcrumb' }, h(Icon, { name: 'folder', size: 15 }), h('span', { title: report.path }, shortPath(report.path, 5))),
                  h('div', { className: 'mole-inline' }, h('span', { className: `mole-chip mole-chip-${scanTone}` }, t(`analyze.scan.${report.scan_status ?? 'complete'}`)), h('span', { className: 'mole-chip' }, t('analyze.files', { value: report.total_files ?? '—' })), h('span', { className: 'mole-chip' }, fmtBytes(report.total_size))),
                ),
                h(
                  'div',
                  { className: 'mole-analyze-body' },
                  h(
                    'div',
                    { className: 'mole-analyze-side' },
                    entries.length === 0 ? h(Empty, { icon: 'folder', title: t('analyze.noentries') }) : null,
                    sorted.slice(0, 40).map((entry) =>
                      h(
                        'button',
                        { type: 'button', className: 'mole-analyze-row', key: entry.path, onClick: () => enter(entry), title: entry.path },
                        h(Icon, { name: entry.is_dir ? 'folder' : 'file', size: 15 }),
                        h('span', { className: 'mole-analyze-row-text' }, h('span', { className: 'mole-analyze-row-name' }, entry.name), h('span', { className: 'mole-analyze-row-bar' }, h('span', { style: { width: `${Math.max(2, (Number(entry.size) / maxSize) * 100)}%` } }))),
                        h('span', { className: 'mole-analyze-row-size' }, fmtBytes(entry.size)),
                      ),
                    ),
                  ),
                  h(
                    'div',
                    { className: 'mole-treemap' },
                    treemap.map((block, index) =>
                      h(
                        'button',
                        {
                          type: 'button',
                          key: `${block.path}-${index}`,
                          className: 'mole-treemap-cell',
                          style: {
                            left: `${(block.x / 1000) * 100}%`,
                            top: `${(block.y / 520) * 100}%`,
                            width: `${(block.width / 1000) * 100}%`,
                            height: `${(block.height / 520) * 100}%`,
                            background: TREEMAP_COLORS[index % TREEMAP_COLORS.length],
                          },
                          onClick: () => enter(block),
                          title: `${block.path} · ${fmtBytes(block.size)}`,
                        },
                        h('span', { className: 'mole-treemap-label' }, h(Icon, { name: block.is_dir ? 'folder' : 'file', size: 13 }), block.name),
                        h('span', { className: 'mole-treemap-size' }, fmtBytes(block.size)),
                      ),
                    ),
                  ),
                ),
                largeFiles.length > 0
                  ? h(
                      Card,
                      { title: t('analyze.large'), subtitle: t('analyze.large.sub') },
                      h(
                        'div',
                        { className: 'mole-table' },
                        largeFiles.map((file) =>
                          h(
                            'div',
                            { className: 'mole-tr', key: file.path },
                            h('span', { className: 'mole-cell-name', title: file.path }, file.name),
                            h('span', { className: 'mole-cell-muted' }, shortPath(file.path, 4)),
                            h('span', { className: 'mole-app-size' }, fmtBytes(file.size)),
                            h(Btn, { size: 'sm', variant: 'ghost', icon: 'open', onClick: () => openPath(file.path) }, t('analyze.reveal')),
                          ),
                        ),
                      ),
                    )
                  : null,
              )
            : null,
        ),
        h('p', { className: 'mole-footnote' }, t('analyze.footnote')),
      )
    }

    // ------------------------------------------------------------------
    // tools tab (purge / installer / slim)
    // ------------------------------------------------------------------
    const TOOL_KINDS = ['purge', 'installer', 'slim']

    function ToolCard({ t, kind }) {
      const [state, setState] = usePersistentState(`tools.${kind}`, { phase: 'idle', data: null, error: null })

      const scan = useCallback(async () => {
        setState({ phase: 'running', data: null, error: null })
        try {
          const payload = await runJob({ action: 'preview', kind }, t, `progress.preview.${kind}`)
          setState({ phase: 'done', data: payload, error: null })
        } catch (error) {
          setState({ phase: 'error', data: null, error: String(error?.message ?? error) })
        }
      }, [kind, t])

      async function openTerminal() {
        try {
          await requestJson('/dsh-mole/open-terminal', { method: 'POST', body: { action: kind } })
        } catch (error) {
          setState((previous) => ({ ...previous, error: String(error?.message ?? error) }))
        }
      }

      const parsed = state.data ? parsePreview(state.data.output) : null
      const hasGroups = parsed && parsed.groups.length > 0

      return h(
        Card,
        {
          title: t(`tools.${kind}.title`),
          subtitle: t(`tools.${kind}.sub`),
          className: 'mole-tool-card',
          right: h(Btn, { size: 'sm', variant: state.phase === 'done' ? 'ghost' : 'default', icon: 'search', onClick: scan, disabled: state.phase === 'running' }, state.phase === 'running' ? t('tools.scanning') : t('tools.preview')),
        },
        state.phase === 'error' ? h(ErrorNote, { children: state.error, onRetry: scan }) : null,
        state.phase === 'running' ? h(Spinner, { label: t('tools.scanning.sub') }) : null,
        state.phase === 'idle' ? h('p', { className: 'mole-tool-hint' }, t(`tools.${kind}.hint`)) : null,
        parsed
          ? h(
              'div',
              null,
              h(
                'div',
                { className: 'mole-summary-list' },
                h('div', { className: 'mole-summary-row' }, t('tools.result', { groups: parsed.groups.length, items: parsed.itemLines, size: fmtBytes(parsed.totalBytes) })),
              ),
              hasGroups
                ? h(
                    'div',
                    { className: 'mole-groups mole-groups-compact' },
                    parsed.groups.slice(0, 12).map((group, index) =>
                      h(
                        'div',
                        { className: 'mole-group', key: `${group.title}-${index}` },
                        h('div', { className: 'mole-group-head' }, h('span', { className: 'mole-group-title' }, group.title), h('span', { className: 'mole-group-count' }, String(group.items.length))),
                        group.items.slice(0, 8).map((item, itemIndex) =>
                          h('div', { className: `mole-item mole-item-${markTone(item.mark)}`, key: itemIndex }, h('span', { className: 'mole-item-mark' }, item.mark), h('span', { className: 'mole-item-name' }, item.name), h('span', { className: 'mole-item-size' }, item.sizeText)),
                        ),
                      ),
                    ),
                  )
                : h(Output, { text: state.data.output, collapsed: true }),
            )
          : null,
        h('div', { className: 'mole-actions' }, h(Btn, { size: 'sm', variant: 'ghost', icon: 'terminal', onClick: openTerminal }, t('tools.openTerminal')), kind === 'purge' ? h('span', { className: 'mole-footnote' }, t('tools.purge.footnote')) : null),
      )
    }

    function ToolsTab({ t }) {
      return h(
        'div',
        { className: 'mole-tools' },
        h(
          'div',
          { className: 'mole-tools-head' },
          h('h2', { className: 'mole-tools-title' }, t('tools.title')),
          h('p', { className: 'mole-tools-sub' }, t('tools.sub')),
        ),
        h('div', { className: 'mole-tools-grid' }, TOOL_KINDS.map((kind) => h(ToolCard, { key: kind, t, kind }))),
      )
    }

    // ------------------------------------------------------------------
    // history tab
    // ------------------------------------------------------------------
    function HistoryTab({ t, refreshToken }) {
      const [state, setState] = useState({ phase: 'loading', data: null, error: null })
      const [showDeletions, setShowDeletions] = useState(false)

      const load = useCallback(async () => {
        setState((previous) => ({ phase: previous.data ? 'ready' : 'loading', data: previous.data, error: null }))
        try {
          const payload = await requestJson('/dsh-mole/history?limit=60')
          setState({ phase: 'ready', data: payload, error: null })
        } catch (error) {
          setState((previous) => ({ phase: 'error', data: previous.data, error: String(error?.message ?? error) }))
        }
      }, [])

      useEffect(() => {
        load()
      }, [load, refreshToken])

      const history = state.data?.history
      const sessions = useStable(history?.sessions)
      const deletions = useStable(history?.deletions)

      return h(
        'div',
        { className: 'mole-history' },
        h(
          Card,
          {
            title: t('history.title'),
            subtitle: t('history.sub', { count: sessions.length }),
            right: h(
              'div',
              { className: 'mole-inline' },
              h(Btn, { size: 'sm', variant: 'ghost', icon: 'refresh', onClick: load }, t('common.refresh')),
              h(Btn, { size: 'sm', variant: 'ghost', icon: 'terminal', onClick: async () => { try { await requestJson('/dsh-mole/open-terminal', { method: 'POST', body: { action: 'history' } }) } catch { /* optional */ } } }, t('clean.openTerminal')),
            ),
          },
          state.phase === 'error' ? h(ErrorNote, { children: state.error, onRetry: load }) : null,
          sessions.length === 0 && state.phase !== 'loading' ? h(Empty, { icon: 'history', title: t('history.empty') }) : null,
          sessions.length > 0
            ? h(
                'div',
                { className: 'mole-table mole-history-table' },
                h('div', { className: 'mole-tr mole-th' }, h('span', null, t('history.col.command')), h('span', null, t('history.col.started')), h('span', null, t('history.col.size')), h('span', null, t('history.col.items')), h('span', null, t('history.col.result'))),
                sessions.map((session) =>
                  h(
                    'div',
                    { className: 'mole-tr', key: session.run_id || `${session.command}-${session.started_at}` },
                    h('span', { className: 'mole-cell-name' }, zhCli(session.command)),
                    h('span', { className: 'mole-cell-muted' }, session.started_at ?? '—'),
                    h('span', null, session.size ?? '—'),
                    h('span', null, String(session.items ?? 0)),
                    h(
                      'span',
                      { className: 'mole-history-actions' },
                      Number(session.actions?.removed) > 0 ? h('span', { className: 'mole-chip mole-chip-ok' }, `${t('history.removed')} ${session.actions.removed}`) : null,
                      Number(session.actions?.trashed) > 0 ? h('span', { className: 'mole-chip' }, `${t('history.trashed')} ${session.actions.trashed}`) : null,
                      Number(session.failed_tasks) > 0 ? h('span', { className: 'mole-chip mole-chip-bad' }, `${t('history.failed')} ${session.failed_tasks}`) : null,
                      session.ended_at ? null : h('span', { className: 'mole-chip mole-chip-warn' }, t('history.open')),
                    ),
                  ),
                ),
              )
            : null,
        ),
        deletions.length > 0
          ? h(
              Card,
              {
                title: t('history.deletions', { count: deletions.length }),
                right: h(Btn, { size: 'sm', variant: 'ghost', onClick: () => setShowDeletions((value) => !value) }, showDeletions ? t('common.collapse') : t('common.expand')),
              },
              showDeletions
                ? h(
                    'div',
                    { className: 'mole-table mole-deletion-table' },
                    deletions.slice(0, 200).map((deletion, index) =>
                      h(
                        'div',
                        { className: 'mole-tr', key: `${deletion.timestamp}-${index}` },
                        h('span', { className: 'mole-cell-muted' }, deletion.timestamp),
                        h('span', { className: `mole-chip ${deletion.mode === 'trash' ? 'mole-chip-ok' : 'mole-chip-bad'}` }, deletion.mode),
                        h('span', { className: 'mole-cell-muted' }, deletion.status),
                        h('span', { title: deletion.path }, shortPath(deletion.path, 5)),
                        h('span', { className: 'mole-app-size' }, `${(Number(deletion.size_kb ?? 0) / 1024).toFixed(1)} MB`),
                      ),
                    ),
                  )
                : h('p', { className: 'mole-footnote' }, t('history.deletions.sub')),
            )
          : null,
        h('p', { className: 'mole-footnote' }, t('history.footnote')),
      )
    }

    // ------------------------------------------------------------------
    // panel shell
    // ------------------------------------------------------------------
    const TAB_KEYS = [
      { key: 'clean', icon: 'clean' },
      { key: 'apps', icon: 'apps' },
      { key: 'optimize', icon: 'optimize' },
      { key: 'analyze', icon: 'analyze' },
      { key: 'status', icon: 'status' },
      { key: 'tools', icon: 'tools' },
      { key: 'history', icon: 'history' },
    ]

    function MissingMoleBanner({ t, version, onRecheck }) {
      const [copied, setCopied] = useState(false)
      const [installing, setInstalling] = useState(false)
      const [notice, setNotice] = useState('')
      if (!version || version.available !== false) return null
      const command = version.install?.brew ?? 'brew install mole'
      const script = version.install?.script ?? ''
      const docs = version.install?.docs ?? 'https://github.com/tw93/Mole'
      const isMac = version.platform === undefined || version.platform === 'darwin'
      const copyText = version.brew ? command : script
      async function install() {
        if (installing) return
        setInstalling(true)
        setNotice('')
        try {
          const result = await runJob({ action: 'install-mole' }, t, 'progress.installMole')
          setNotice(t('missing.installDone'))
          await onRecheck()
          if (result && result.mole && result.mole.available) setNotice(t('missing.installDone'))
        } catch (error) {
          setNotice(String(error?.message ?? error))
        } finally {
          setInstalling(false)
        }
      }
      async function installInTerminal() {
        try {
          await requestJson('/dsh-mole/open-terminal', { method: 'POST', body: { action: 'install-mole' } })
          setNotice(t('missing.terminalOpened'))
        } catch (error) {
          setNotice(String(error?.message ?? error))
        }
      }
      function copy() {
        try {
          if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
            navigator.clipboard.writeText(copyText).then(() => {
              setCopied(true)
              setTimeout(() => setCopied(false), 2000)
            }).catch(() => setNotice(copyText))
            return
          }
        } catch {
          // fall through to showing the command
        }
        setNotice(copyText)
      }
      return h(
        'div',
        { className: 'mole-missing' },
        h('div', { className: 'mole-missing-icon' }, h(Icon, { name: 'warning', size: 18 })),
        h(
          'div',
          { className: 'mole-missing-body' },
          h('div', { className: 'mole-missing-title' }, t('missing.title')),
          h('p', { className: 'mole-missing-text' }, isMac ? t('missing.body') : t('missing.platform')),
          isMac ? h('code', { className: 'mole-missing-code' }, copyText) : null,
          isMac && !version.brew ? h('p', { className: 'mole-missing-text' }, t('missing.noBrew')) : null,
          isMac
            ? h(
                'div',
                { className: 'mole-missing-actions' },
                version.brew ? h(Btn, { size: 'sm', variant: 'primary', icon: 'terminal', onClick: install, disabled: installing }, installing ? t('missing.installing') : t('missing.brew')) : null,
                version.brew ? h(Btn, { size: 'sm', variant: 'ghost', icon: 'terminal', onClick: installInTerminal }, t('missing.terminal')) : null,
                h(Btn, { size: 'sm', variant: 'ghost', icon: 'file', onClick: copy }, copied ? t('missing.copied') : t('missing.copy')),
                h(Btn, { size: 'sm', variant: 'ghost', icon: 'refresh', onClick: onRecheck }, t('missing.recheck')),
                h('a', { className: 'mole-missing-link', href: docs, target: '_blank', rel: 'noreferrer' }, t('missing.docs')),
              )
            : null,
          notice ? h('p', { className: 'mole-missing-text' }, notice) : null,
          isMac ? h('p', { className: 'mole-missing-hint' }, t('missing.after')) : null,
        ),
      )
    }

    function MainPanel({ ctx }) {
      const [tab, setTab] = useState('status')
      const [version, setVersion] = useState(null)
      const [historyToken, setHistoryToken] = useState(0)
      const t = useLocale(ctx)
      const activeVersion = version

      const loadVersion = useCallback(async (refresh) => {
        try {
          const payload = await requestJson(`/dsh-mole/version${refresh ? '?refresh=1' : ''}`)
          setVersion(payload.mole)
        } catch {
          setVersion({ available: false })
        }
      }, [])

      useEffect(() => {
        loadVersion(false)
      }, [loadVersion])

      const bumpHistory = useCallback(() => setHistoryToken((value) => value + 1), [])

      let content = null
      if (tab === 'clean') content = h(CleanTab, { t, onHistoryChanged: bumpHistory })
      else if (tab === 'apps') content = h(AppsTab, { t })
      else if (tab === 'optimize') content = h(OptimizeTab, { t, onHistoryChanged: bumpHistory })
      else if (tab === 'analyze') content = h(AnalyzeTab, { t })
      else if (tab === 'tools') content = h(ToolsTab, { t })
      else if (tab === 'history') content = h(HistoryTab, { t, refreshToken: historyToken })
      else content = h(StatusTab, { t })

      const missing = activeVersion && activeVersion.available === false

      return h(
        'div',
        { className: 'mole-root' },
        h('style', null, MOLE_CSS),
        h(
          'header',
          { className: 'mole-head' },
          h(
            'div',
            { className: 'mole-head-top' },
            h(
              'div',
              { className: 'mole-brand' },
              h('span', { className: 'mole-brand-mark' }, h(Mascot, { size: 27 })),
              h('span', { className: 'mole-brand-text' }, h('span', { className: 'mole-brand-title' }, t('nav.title')), h('span', { className: 'mole-brand-sub' }, t('nav.subtitle'))),
            ),
            h(
              'div',
              { className: 'mole-meta' },
              h('span', { className: `mole-dot ${missing ? 'mole-dot-bad' : activeVersion ? 'mole-dot-ok' : 'mole-dot-idle'}` }),
              h('span', { className: 'mole-meta-text' }, activeVersion ? (activeVersion.available ? `mole ${activeVersion.version ?? ''}` : t('nav.missing')) : 'mole'),
              activeVersion && activeVersion.available ? h('span', { className: 'mole-meta-sep' }, '·') : null,
              activeVersion && activeVersion.available ? h('span', { className: 'mole-meta-sub' }, t('nav.local')) : null,
            ),
          ),
          h(
            'nav',
            { className: 'mole-nav', 'aria-label': t('nav.title') },
            TAB_KEYS.map((item) => h('button', { type: 'button', key: item.key, className: `mole-tab ${tab === item.key ? 'is-active' : ''}`, onClick: () => setTab(item.key) }, t(`nav.${item.key}`))),
          ),
        ),
        h(ProgressLine, { t }),
        h(MissingMoleBanner, { t, version: activeVersion, onRecheck: () => loadVersion(true) }),
        h('main', { className: 'mole-body' }, h('div', { className: 'mole-page' }, content)),
      )
    }

    function useLocale(ctx) {
      const [revision, setRevision] = useState(0)
      const translate = useMemo(() => ctx.locale.bind(NS), [ctx])
      useEffect(() => {
        let unsubscribe = null
        try {
          unsubscribe = ctx.locale.subscribe(() => setRevision((value) => value + 1))
        } catch {
          unsubscribe = null
        }
        return () => {
          if (typeof unsubscribe === 'function') unsubscribe()
        }
      }, [ctx])
      try {
        const snapshot = ctx.locale.getLocale()
        if (snapshot && typeof snapshot.active === 'string') activeLocale = snapshot.active
      } catch {
        // keep the previous locale
      }
      return useCallback((key, values) => translate(key, values), [translate, revision])
    }

    // ------------------------------------------------------------------
    // styles
    // ------------------------------------------------------------------
    const MOLE_CSS = `
.mole-root{--mole-ink:#eef2fc;--mole-sub:rgba(215,225,248,.62);--mole-faint:rgba(215,225,248,.42);--mole-line:rgba(255,255,255,.08);--mole-line-2:rgba(255,255,255,.15);--mole-card:rgba(255,255,255,.045);--mole-card-2:rgba(255,255,255,.07);--mole-accent:#7c8bff;--mole-accent-2:#a78bfa;--mole-accent-soft:rgba(124,139,255,.16);--mole-ok:#4ade80;--mole-warn:#fbbf24;--mole-bad:#f87171;--mole-radius:14px;position:relative;height:100%;display:flex;flex-direction:column;overflow:hidden;background:radial-gradient(1100px 520px at 10% -10%,rgba(92,116,255,.22),transparent 62%),radial-gradient(900px 460px at 92% -6%,rgba(150,96,255,.16),transparent 60%),radial-gradient(1000px 720px at 50% 118%,rgba(38,88,160,.2),transparent 66%),linear-gradient(180deg,#080d18,#060a12 55%,#05080f);color:var(--mole-ink);color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;-webkit-font-smoothing:antialiased;font-variant-numeric:tabular-nums}
.mole-root *{box-sizing:border-box}
.mole-root::before{content:"";position:absolute;inset:0;pointer-events:none;background-image:radial-gradient(1.1px 1.1px at 22% 18%,rgba(255,255,255,.55),transparent),radial-gradient(1px 1px at 68% 32%,rgba(255,255,255,.4),transparent),radial-gradient(1.4px 1.4px at 42% 62%,rgba(255,255,255,.5),transparent),radial-gradient(1px 1px at 88% 74%,rgba(255,255,255,.35),transparent),radial-gradient(1px 1px at 12% 88%,rgba(255,255,255,.42),transparent);background-size:460px 460px;opacity:.4}
.mole-root::after{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(70% 50% at 85% -8%,rgba(124,139,255,.16),transparent 70%),radial-gradient(50% 40% at 5% 105%,rgba(80,50,140,.14),transparent 70%)}
.mole-head{position:relative;z-index:2;flex:0 0 auto;padding:20px 26px 0;background:linear-gradient(180deg,rgba(8,12,22,.78),rgba(8,12,22,0))}
.mole-head-top{display:flex;align-items:center;gap:16px;justify-content:space-between;max-width:1260px;margin:0 auto;width:100%}
.mole-brand{display:flex;align-items:center;gap:11px;min-width:0}
.mole-brand-mark{width:34px;height:34px;border-radius:10px;background:linear-gradient(155deg,#1b2649,#0b1020) padding-box,linear-gradient(155deg,rgba(255,255,255,.28),rgba(255,255,255,.04)) border-box;color:#dfe6ff;display:flex;align-items:center;justify-content:center;border:1px solid rgba(255,255,255,.12);box-shadow:0 8px 22px rgba(0,0,0,.4),inset 0 1px 0 rgba(255,255,255,.14)}
.mole-brand-text{display:flex;flex-direction:column;line-height:1.25;min-width:0}
.mole-brand-title{font-size:15.5px;font-weight:650;letter-spacing:.01em;color:#f2f5ff;white-space:nowrap}
.mole-brand-sub{font-size:12px;color:var(--mole-sub);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mole-meta{margin-left:auto;display:flex;align-items:center;gap:7px;color:var(--mole-sub);font-size:11.5px;white-space:nowrap}
.mole-mascot{display:block;object-fit:contain;user-select:none;filter:drop-shadow(0 2px 6px rgba(0,0,0,.4))}
.mole-brand-mark .mole-mascot{width:30px;height:30px;margin:0 auto}
.mole-nav-icon{display:block;flex:0 0 auto;object-fit:contain;filter:invert(1);opacity:.92}body[data-ds-dark-theme] .mole-nav-icon{filter:none}
.mole-meta-sep{opacity:.35}
.mole-meta-sub{color:var(--mole-faint)}
.mole-dot{width:7px;height:7px;border-radius:50%;background:rgba(255,255,255,.28);display:inline-block;flex:0 0 auto}
.mole-dot-ok{background:var(--mole-ok);box-shadow:0 0 0 3px rgba(74,222,128,.16)}
.mole-dot-bad{background:var(--mole-bad);box-shadow:0 0 0 3px rgba(248,113,113,.16)}
.mole-dot-idle{background:rgba(255,255,255,.3)}
.mole-nav{display:flex;align-items:center;gap:2px;max-width:1260px;margin:16px auto 0;width:100%;border-bottom:1px solid var(--mole-line);overflow-x:auto;scrollbar-width:none}
.mole-nav::-webkit-scrollbar{display:none}
.mole-tab{border:0;background:transparent;color:var(--mole-sub);font:inherit;font-size:13px;font-weight:550;padding:9px 12px 11px;margin-bottom:-1px;border-bottom:2px solid transparent;cursor:pointer;white-space:nowrap;transition:color .16s ease,border-color .16s ease}
.mole-tab:hover{color:var(--mole-ink)}
.mole-tab.is-active{color:#fff;border-bottom-color:var(--mole-accent)}
.mole-body{flex:1 1 auto;min-height:0;overflow:auto;position:relative;z-index:1}
.mole-body::-webkit-scrollbar{width:10px;height:10px}
.mole-body::-webkit-scrollbar-thumb{background:rgba(255,255,255,.14);border-radius:999px;border:2px solid transparent;background-clip:padding-box}
.mole-body::-webkit-scrollbar-track{background:transparent}
.mole-page{max-width:1260px;margin:0 auto;padding:22px 26px 46px;display:flex;flex-direction:column;gap:16px}
.mole-status,.mole-clean,.mole-apps,.mole-optimize,.mole-analyze,.mole-tools,.mole-history{display:flex;flex-direction:column;gap:16px}
.mole-status-top{display:grid;grid-template-columns:1.22fr repeat(3,minmax(0,1fr));gap:12px}
.mole-card{position:relative;background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.025));border:1px solid var(--mole-line);border-radius:var(--mole-radius);padding:16px 18px;display:flex;flex-direction:column;gap:12px;min-width:0;box-shadow:inset 0 1px 0 rgba(255,255,255,.05),0 18px 40px rgba(0,0,0,.18)}
.mole-card-head{display:flex;align-items:flex-start;gap:12px;justify-content:space-between}
.mole-card-title{margin:0;font-size:13.5px;font-weight:620;letter-spacing:.01em;color:#eef2fc}
.mole-card-sub{margin:3px 0 0;font-size:11.5px;color:var(--mole-sub)}
.mole-card-right{flex:0 0 auto}
.mole-card-metric{justify-content:space-between;min-height:168px;padding:15px 17px}
.mole-metric-head{display:flex;align-items:center;gap:8px;justify-content:space-between;color:var(--mole-sub);font-size:11.5px}
.mole-eyebrow{display:inline-flex;align-items:center;gap:6px;font-size:10.5px;font-weight:620;letter-spacing:.09em;text-transform:uppercase;color:var(--mole-sub)}
.mole-metric-value{font-size:29px;font-weight:650;letter-spacing:-.015em;line-height:1.02;color:#f4f7ff}
.mole-metric-foot{display:flex;justify-content:space-between;gap:10px;font-size:11px;color:var(--mole-sub);flex-wrap:wrap}
.mole-chip{display:inline-flex;align-items:center;gap:4px;padding:2.5px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.12);font-size:10.5px;color:var(--mole-sub);white-space:nowrap;background:rgba(255,255,255,.04)}
.mole-chip-ok{color:var(--mole-ok);border-color:rgba(74,222,128,.35);background:rgba(74,222,128,.08)}
.mole-chip-warn{color:var(--mole-warn);border-color:rgba(251,191,36,.35);background:rgba(251,191,36,.08)}
.mole-chip-bad{color:var(--mole-bad);border-color:rgba(248,113,113,.35);background:rgba(248,113,113,.08)}
.mole-chip-brew{border-color:rgba(124,139,255,.4);color:#aab5ff;background:rgba(124,139,255,.1)}
.mole-status-top .mole-health{grid-column:1/-1}.mole-health{overflow:hidden;border-color:rgba(255,255,255,.1);background:radial-gradient(130% 150% at 88% 6%,rgba(255,176,92,.2),transparent 52%),radial-gradient(120% 140% at 6% 100%,rgba(96,124,255,.24),transparent 58%),linear-gradient(155deg,#121a30,#0c1322 62%,#0a0f1c)}
.mole-health-main{position:relative;z-index:1;display:flex;align-items:center;justify-content:space-between;gap:28px;padding-right:174px;min-height:118px}
.mole-health .mole-planet-img,.mole-health .mole-planet-fallback{position:absolute;right:10px;top:50%;transform:translateY(-50%);width:146px;height:146px;margin:0;opacity:.95;filter:drop-shadow(0 18px 28px rgba(0,0,0,.55));pointer-events:none}
.mole-health .mole-eyebrow{color:rgba(225,232,255,.6)}
.mole-health-score{display:flex;align-items:baseline;gap:8px;font-size:50px;font-weight:700;letter-spacing:-.03em;line-height:1;margin-top:6px;color:#fff}
.mole-health-score-sub{font-size:13px;font-weight:560;color:rgba(235,240,255,.66);letter-spacing:0}
.mole-health-note{margin:6px 0 0;font-size:11px;color:rgba(235,240,255,.5);line-height:1.5;max-width:430px}
.mole-health-foot{display:flex;flex-wrap:wrap;gap:4px 16px;font-size:11px;margin-top:10px;color:rgba(235,240,255,.52)}
.mole-chip-row{display:flex;flex-wrap:wrap;gap:4px;margin-top:9px;max-width:calc(100% - 84px)}
.mole-health .mole-chip{background:rgba(255,255,255,.07);border-color:rgba(255,255,255,.12);color:rgba(240,244,255,.78);font-size:9.5px;padding:1px 6px}
.mole-device-name{font-size:17px;font-weight:620;color:#f4f7ff;letter-spacing:-.01em}
.mole-device-lines{display:flex;flex-direction:column;gap:2px;font-size:10.5px;color:var(--mole-sub)}

.mole-tone-text-ok{color:var(--mole-ok)!important}
.mole-tone-text-warn{color:var(--mole-warn)!important}
.mole-tone-text-bad{color:var(--mole-bad)!important}
.mole-tone-text-accent{color:#aab5ff!important}
.mole-bar{width:100%;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}
.mole-bar-fill{height:100%;border-radius:999px;transition:width .5s cubic-bezier(.2,.8,.2,1)}
.mole-tone-accent{background:linear-gradient(90deg,#6d7cff,#a78bfa)}
.mole-tone-ok{background:linear-gradient(90deg,#34d399,#6ee7b7)}
.mole-tone-warn{background:linear-gradient(90deg,#f59e0b,#fcd34d)}
.mole-tone-bad{background:linear-gradient(90deg,#ef4444,#fca5a5)}
.mole-core-bars{display:flex;align-items:flex-end;gap:4px;height:38px;padding:3px 0}
.mole-core-bar{flex:1 1 0;min-width:4px;border-radius:3px 3px 1px 1px;background:linear-gradient(180deg,#8b98ff,rgba(124,139,255,.25))}
.mole-net-row{display:flex;gap:14px;font-size:13px;font-weight:600}
.mole-table{display:flex;flex-direction:column;gap:1px}
.mole-tr{display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,.9fr) minmax(0,.7fr) minmax(0,.55fr);align-items:center;gap:12px;padding:9px 10px;border-radius:9px;font-size:12.5px;text-align:left;color:var(--mole-ink)}
.mole-th{color:var(--mole-faint);font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;padding-bottom:3px}
.mole-tr:not(.mole-th):hover{background:rgba(255,255,255,.045)}
.mole-cell-name{font-weight:550;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:flex;align-items:center;gap:7px}
.mole-cell-muted{color:var(--mole-sub);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mole-cell-cpu{display:flex;align-items:center;gap:6px;font-size:12px}
.mole-mini-bar{display:inline-block;width:54px;height:5px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}
.mole-mini-bar>span{display:block;height:100%;border-radius:999px;background:linear-gradient(90deg,#6d7cff,#c084fc)}
.mole-process-table .mole-tr{grid-template-columns:minmax(0,1.5fr) minmax(0,1fr) minmax(0,.8fr) 70px}
.mole-footnote{margin:0;color:var(--mole-faint);font-size:11px;line-height:1.65}
.mole-actions{display:flex;align-items:center;flex-wrap:wrap;gap:8px}
.mole-inline{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.mole-btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);color:var(--mole-ink);font:inherit;font-size:12.5px;font-weight:550;padding:7px 13px;border-radius:9px;cursor:pointer;transition:background .16s ease,border-color .16s ease,transform .12s ease,box-shadow .16s ease;white-space:nowrap}
.mole-btn:hover:not(:disabled){background:rgba(255,255,255,.1);border-color:rgba(255,255,255,.22)}
.mole-btn:active:not(:disabled){transform:translateY(1px)}
.mole-btn:disabled{opacity:.45;cursor:default}
.mole-btn:focus-visible{outline:2px solid var(--mole-accent);outline-offset:2px}
.mole-btn-sm{padding:5px 10px;font-size:11.5px;border-radius:8px}
.mole-btn-lg{padding:10px 20px;font-size:13.5px;font-weight:600;border-radius:10px}
.mole-btn-primary,.mole-btn-hero{background:linear-gradient(180deg,#7c8bff,#5f6df0);border-color:transparent;color:#fff;box-shadow:0 8px 22px rgba(95,109,240,.35)}
.mole-btn-primary:hover:not(:disabled),.mole-btn-hero:hover:not(:disabled){background:linear-gradient(180deg,#8b98ff,#6d7bff);border-color:transparent}
.mole-btn-ghost{background:transparent;border-color:rgba(255,255,255,.14);color:var(--mole-sub)}
.mole-btn-ghost:hover:not(:disabled){color:var(--mole-ink);background:rgba(255,255,255,.05)}
.mole-btn-danger{background:linear-gradient(180deg,#ef5b5b,#d43c3c);color:#fff;border-color:transparent;box-shadow:0 8px 20px rgba(212,60,60,.32)}
.mole-btn-hero-ghost{background:rgba(255,255,255,.08);border-color:rgba(255,255,255,.18);color:#eef2ff}
.mole-btn-hero-ghost:hover:not(:disabled){background:rgba(255,255,255,.14);border-color:rgba(255,255,255,.28)}
.mole-planet-img{flex:0 0 auto;margin:6px 0 14px;object-fit:contain;user-select:none;pointer-events:none;filter:drop-shadow(0 26px 42px rgba(4,8,20,.5))}
.mole-planet-fallback{flex:0 0 auto;display:inline-block;margin:6px 0 14px;border-radius:50%;box-shadow:0 26px 42px rgba(4,8,20,.5)}
.mole-hero{position:relative;overflow:hidden;border-radius:20px;padding:34px 38px;display:flex;flex-direction:row-reverse;align-items:center;justify-content:space-between;gap:32px;text-align:left;color:#f3f6ff;border:1px solid rgba(255,255,255,.09);min-height:330px;background:radial-gradient(120% 150% at 84% 18%,rgba(86,120,255,.2),transparent 55%),radial-gradient(90% 120% at 8% 100%,rgba(96,60,160,.24),transparent 62%),linear-gradient(155deg,rgba(255,255,255,.05),rgba(255,255,255,.012) 45%,rgba(0,0,0,.14))}
.mole-hero-clean{background:radial-gradient(120% 150% at 84% 18%,rgba(70,130,220,.22),transparent 55%),radial-gradient(90% 120% at 8% 100%,rgba(40,90,160,.24),transparent 62%),linear-gradient(155deg,rgba(255,255,255,.05),rgba(255,255,255,.012) 45%,rgba(0,0,0,.14))}
.mole-hero-optimize{background:radial-gradient(120% 150% at 84% 18%,rgba(220,110,80,.16),transparent 55%),radial-gradient(90% 120% at 8% 100%,rgba(120,60,140,.22),transparent 62%),linear-gradient(155deg,rgba(255,255,255,.05),rgba(255,255,255,.012) 45%,rgba(0,0,0,.14))}
.mole-stars{position:absolute;inset:0;background-image:radial-gradient(1.3px 1.3px at 14% 24%,rgba(255,255,255,.7),transparent),radial-gradient(1px 1px at 30% 70%,rgba(255,255,255,.45),transparent),radial-gradient(1.5px 1.5px at 74% 16%,rgba(255,255,255,.6),transparent),radial-gradient(1px 1px at 88% 56%,rgba(255,255,255,.4),transparent),radial-gradient(1.1px 1.1px at 55% 88%,rgba(255,255,255,.5),transparent);opacity:.7;pointer-events:none}
.mole-hero .mole-planet-img,.mole-hero .mole-planet-fallback{width:min(300px,30vw);height:auto;margin:0;flex:0 0 auto;filter:drop-shadow(0 34px 64px rgba(0,0,0,.6))}
.mole-hero-copy{position:relative;z-index:1;display:flex;flex-direction:column;align-items:flex-start;text-align:left;max-width:580px}
.mole-hero-title{margin:0;font-size:clamp(26px,3vw,36px);font-weight:680;letter-spacing:-.02em;line-height:1.12;color:#fff}
.mole-hero-sub{margin:10px 0 0;color:rgba(224,232,255,.66);font-size:13px;line-height:1.68;max-width:520px}
.mole-hero-meta{margin:14px 0 0;font-size:11px;color:rgba(224,232,255,.4)}
.mole-hero-actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:22px}
.mole-preview-grid{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(0,.95fr);gap:14px}
.mole-groups{display:flex;flex-direction:column;gap:2px;max-height:520px;overflow:auto;padding-right:4px}
.mole-groups-compact{max-height:320px}
.mole-group{padding:10px 0;border-bottom:1px dashed rgba(255,255,255,.08)}
.mole-group:last-child{border-bottom:0}
.mole-group-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:2px 6px}
.mole-group-title{font-size:12px;font-weight:620;color:var(--mole-ink)}
.mole-group-count{font-size:10.5px;color:var(--mole-faint)}
.mole-group-note{margin:6px 6px 0;font-size:11px;color:var(--mole-sub)}
.mole-item{display:flex;align-items:center;gap:9px;padding:5px 6px;border-radius:7px;font-size:12px;line-height:1.45}
.mole-item:hover{background:rgba(255,255,255,.035)}
.mole-item-mark{width:15px;flex:0 0 15px;text-align:center;font-size:11px;color:var(--mole-faint)}
.mole-item-ok .mole-item-mark{color:var(--mole-ok)}
.mole-item-todo .mole-item-mark{color:#aab5ff}
.mole-item-skip .mole-item-mark{color:rgba(215,225,248,.35)}
.mole-item-warn .mole-item-mark{color:var(--mole-warn)}
.mole-item-name{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;gap:1px;color:var(--mole-ink)}
.mole-item-detail{color:var(--mole-faint);font-size:10.5px}
.mole-item-size{color:var(--mole-sub);font-size:10.5px;white-space:nowrap}
.mole-notice-card{gap:10px}
.mole-list{margin:0;padding-left:16px;display:flex;flex-direction:column;gap:6px;font-size:11.5px;color:var(--mole-sub);line-height:1.6}
.mole-list li::marker{color:rgba(124,139,255,.7)}
.mole-output{border:1px solid var(--mole-line);border-radius:11px;padding:10px 12px;background:rgba(0,0,0,.22)}
.mole-output-head{display:flex;align-items:center;justify-content:space-between;gap:10px;font-size:10.5px;color:var(--mole-faint)}
.mole-link-btn{border:0;background:transparent;color:#aab5ff;font:inherit;font-size:10.5px;cursor:pointer;padding:0}
.mole-pre{margin:8px 0 0;max-height:300px;overflow:auto;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;line-height:1.6;white-space:pre-wrap;word-break:break-word;color:var(--mole-sub)}
.mole-pre-tall{max-height:420px}
.mole-search{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:9px;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.04);color:var(--mole-faint);min-width:180px}
.mole-search-wide{min-width:260px}
.mole-search input{border:0;background:transparent;color:var(--mole-ink);font:inherit;font-size:12px;outline:none;width:100%;min-width:0}
.mole-search input::placeholder{color:rgba(215,225,248,.35)}
.mole-table.mole-app-table .mole-tr{grid-template-columns:minmax(0,1.8fr) minmax(0,.55fr) minmax(0,.45fr) 24px}
.mole-app-row{width:100%;border:1px solid transparent;background:transparent;color:inherit;font:inherit;cursor:pointer}
.mole-app-row:hover{border-color:rgba(255,255,255,.1);background:rgba(255,255,255,.03)}
.mole-app-row.is-selected{border-color:rgba(124,139,255,.4);background:rgba(124,139,255,.1)}
.mole-app-name{display:flex;align-items:center;gap:10px;min-width:0}
.mole-app-avatar{width:28px;height:28px;border-radius:8px;display:inline-flex;align-items:center;justify-content:center;color:#fff;font-size:12.5px;font-weight:640;flex:0 0 auto;box-shadow:inset 0 0 0 1px rgba(255,255,255,.12)}
.mole-app-text{display:flex;flex-direction:column;min-width:0}
.mole-app-label{font-weight:550;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--mole-ink)}
.mole-app-path{font-size:10.5px;color:var(--mole-faint);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mole-app-size{font-size:11px;color:var(--mole-sub);white-space:nowrap}
.mole-toast{display:flex;align-items:center;gap:9px;padding:10px 14px;border-radius:11px;border:1px solid rgba(124,139,255,.4);background:rgba(124,139,255,.12);color:var(--mole-ink);font-size:12px}
.mole-toast .mole-icon-btn{margin-left:auto}
.mole-icon-btn{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:7px;border:0;background:transparent;color:var(--mole-sub);cursor:pointer}
.mole-icon-btn:hover{background:rgba(255,255,255,.08);color:var(--mole-ink)}
.mole-opt-group{padding:10px 2px;border-bottom:1px dashed rgba(255,255,255,.08)}
.mole-opt-group:last-child{border-bottom:0}
.mole-opt-title{font-size:10.5px;font-weight:620;padding:0 6px 4px;color:var(--mole-faint);text-transform:uppercase;letter-spacing:.07em}
.mole-analyze-bar{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:2px 2px 10px}
.mole-breadcrumb{display:inline-flex;align-items:center;gap:7px;font-size:12.5px;font-weight:600;min-width:0;color:var(--mole-ink)}
.mole-analyze-body{display:grid;grid-template-columns:minmax(220px,300px) minmax(0,1fr);gap:14px;align-items:start}
.mole-analyze-side{display:flex;flex-direction:column;gap:2px;max-height:560px;overflow:auto;padding-right:4px}
.mole-analyze-row{display:flex;align-items:center;gap:9px;width:100%;border:1px solid transparent;background:transparent;color:var(--mole-ink);font:inherit;font-size:12px;padding:7px 8px;border-radius:8px;cursor:pointer;text-align:left}
.mole-analyze-row:hover{border-color:rgba(255,255,255,.1);background:rgba(255,255,255,.04)}
.mole-analyze-row-text{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;gap:4px}
.mole-analyze-row-name{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:550}
.mole-analyze-row-bar{height:3px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}
.mole-analyze-row-bar>span{display:block;height:100%;border-radius:999px;background:linear-gradient(90deg,#34d399,#7c8bff)}
.mole-analyze-row-size{font-size:10.5px;color:var(--mole-faint);white-space:nowrap}
.mole-treemap{position:relative;width:100%;aspect-ratio:1000/520;border-radius:12px;overflow:hidden;background:rgba(255,255,255,.04);border:1px solid var(--mole-line)}
.mole-treemap-cell{position:absolute;display:flex;flex-direction:column;align-items:flex-start;justify-content:center;gap:3px;border:0;border-right:2px solid rgba(0,0,0,.28);border-bottom:2px solid rgba(0,0,0,.28);color:#fff;font:inherit;padding:8px;text-align:left;cursor:pointer;overflow:hidden;transition:filter .15s ease}
.mole-treemap-cell:hover{filter:brightness(1.14)}
.mole-treemap-label{display:inline-flex;align-items:center;gap:5px;font-size:11px;font-weight:600;max-width:100%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-shadow:0 1px 3px rgba(0,0,0,.45)}
.mole-treemap-size{font-size:10px;opacity:.78;text-shadow:0 1px 3px rgba(0,0,0,.45)}
.mole-tools-head{display:flex;flex-direction:column;gap:5px}
.mole-tools-title{margin:0;font-size:19px;font-weight:660;letter-spacing:-.01em;color:#f2f5ff}
.mole-tools-sub{margin:0;font-size:12px;color:var(--mole-sub)}
.mole-tools-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:14px}
.mole-tool-card{min-height:220px}
.mole-tool-hint{margin:0;font-size:11.5px;color:var(--mole-sub);line-height:1.65}
.mole-history-table .mole-tr{grid-template-columns:minmax(0,1fr) minmax(0,1fr) minmax(0,.5fr) 60px minmax(0,1.1fr)}
.mole-history-actions{display:flex;gap:5px;flex-wrap:wrap}
.mole-deletion-table .mole-tr{grid-template-columns:minmax(0,1.1fr) 60px 70px minmax(0,1.6fr) 70px}
.mole-empty{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:7px;padding:44px 20px;color:var(--mole-sub);text-align:center}
.mole-empty-title{margin:0;font-size:13px;font-weight:600;color:var(--mole-ink)}
.mole-empty-sub{margin:0;font-size:11.5px;max-width:420px;line-height:1.65;color:var(--mole-faint)}
.mole-loading{display:flex;flex-direction:column;align-items:center;gap:9px;padding:38px;color:var(--mole-sub);font-size:12px}
.mole-spinner-wrap{display:inline-flex;align-items:center;gap:9px;font-size:12px;color:var(--mole-sub)}
.mole-spinner{width:15px;height:15px;border-radius:50%;border:2px solid rgba(124,139,255,.22);border-top-color:#8b98ff;animation:mole-spin .8s linear infinite;display:inline-block}
@keyframes mole-spin{to{transform:rotate(360deg)}}
.mole-error{display:flex;align-items:center;gap:9px;padding:10px 13px;border-radius:11px;border:1px solid rgba(248,113,113,.35);background:rgba(248,113,113,.1);color:#fca5a5;font-size:12px}
.mole-error .mole-btn{margin-left:auto}
.mole-summary-list{display:flex;flex-direction:column;gap:3px;padding:10px 12px;border-radius:11px;background:rgba(0,0,0,.22);border:1px solid var(--mole-line);font-size:11.5px;color:var(--mole-ink)}
.mole-summary-row{color:var(--mole-ink)}
.mole-running{border-color:rgba(124,139,255,.35)}
.mole-missing{position:relative;z-index:2;display:flex;gap:12px;max-width:1260px;margin:14px auto 0;padding:14px 16px;border:1px solid rgba(251,191,36,.32);background:linear-gradient(180deg,rgba(251,191,36,.1),rgba(251,191,36,.035));border-radius:12px;color:var(--mole-ink);width:100%}
.mole-missing-icon{color:var(--mole-warn);flex:0 0 auto;padding-top:1px}
.mole-missing-body{min-width:0;display:flex;flex-direction:column;gap:8px}
.mole-missing-title{font-size:13px;font-weight:620}
.mole-missing-text{margin:0;font-size:11.5px;line-height:1.65;color:var(--mole-sub)}
.mole-missing-code{display:inline-block;max-width:100%;overflow:auto;padding:6px 9px;border-radius:7px;background:rgba(0,0,0,.35);border:1px solid rgba(255,255,255,.08);font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;color:#e9edf9;white-space:pre}
.mole-missing-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.mole-missing-link{font-size:11.5px;color:#aab5ff;text-decoration:none}
.mole-missing-link:hover{text-decoration:underline}
.mole-missing-hint{margin:0;font-size:10.5px;color:var(--mole-faint)}
@media (max-width:640px){.mole-missing{margin:10px 18px 0;padding:12px}}

.mole-modal-backdrop{position:fixed;inset:0;z-index:2147483000;background:rgba(3,6,14,.66);backdrop-filter:blur(6px);display:flex;align-items:center;justify-content:center;padding:20px}
.mole-modal{width:min(540px,100%);max-height:80vh;display:flex;flex-direction:column;background:#0b1222;border:1px solid rgba(255,255,255,.12);border-radius:16px;box-shadow:0 40px 120px rgba(0,0,0,.65);overflow:hidden;color:var(--mole-ink)}
.mole-modal-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:15px 18px;border-bottom:1px solid var(--mole-line);background:rgba(255,255,255,.02)}
.mole-modal-head h2{margin:0;font-size:14.5px;font-weight:620;color:#f2f5ff}
.mole-modal-body{padding:16px 18px;overflow:auto;display:flex;flex-direction:column;gap:12px;font-size:12px;line-height:1.7;color:var(--mole-sub)}
.mole-modal-body p{margin:0}
.mole-modal-foot{padding:13px 18px;border-top:1px solid var(--mole-line);display:flex;justify-content:flex-end;gap:9px;background:rgba(0,0,0,.18)}
.mole-modal-actions{display:flex;gap:9px}
@media (max-width:1100px){.mole-status-top{grid-template-columns:1fr 1fr}.mole-preview-grid{grid-template-columns:1fr}.mole-analyze-body{grid-template-columns:1fr}}
@media (max-width:880px){.mole-hero{flex-direction:column-reverse;align-items:flex-start;padding:26px;min-height:0;gap:18px}.mole-hero .mole-planet-img,.mole-hero .mole-planet-fallback{width:170px;align-self:flex-end;margin-top:-6px}.mole-head{padding:16px 18px 0}.mole-page{padding:18px 18px 36px}.mole-brand-sub{display:none}}
@media (max-width:640px){.mole-status-top{grid-template-columns:1fr}.mole-meta{display:none}.mole-tools-grid{grid-template-columns:1fr}.mole-history-table .mole-tr,.mole-deletion-table .mole-tr{grid-template-columns:1fr 1fr}.mole-health-main{padding-right:0}.mole-health .mole-planet-img,.mole-health .mole-planet-fallback{opacity:.35;right:-30px;bottom:-40px}}
.mole-runbar-wrap{max-width:1260px;margin:0 auto;padding:14px 26px 0;width:100%;flex:0 0 auto;display:flex;flex-direction:column;gap:4px;position:relative;z-index:2}
.mole-runbar{display:flex;align-items:center;gap:10px;padding:8px 12px;border:1px solid rgba(124,139,255,.3);border-radius:10px;background:rgba(124,139,255,.1);font-size:11.5px;color:var(--mole-sub);min-height:36px}
.mole-runbar-starting,.mole-runbar-running{border-color:rgba(124,139,255,.32)}
.mole-runbar-done{border-color:rgba(74,222,128,.32);background:rgba(74,222,128,.08)}
.mole-runbar-error{border-color:rgba(248,113,113,.38);background:rgba(248,113,113,.1);color:#fca5a5}
.mole-runbar-indicator{width:8px;height:8px;border-radius:50%;background:var(--mole-accent);box-shadow:0 0 0 3px rgba(124,139,255,.18);flex:0 0 auto;animation:mole-pulse 1.2s ease-in-out infinite}
.mole-runbar-done .mole-runbar-indicator{background:var(--mole-ok);animation:none;box-shadow:0 0 0 3px rgba(74,222,128,.16)}
.mole-runbar-error .mole-runbar-indicator{background:var(--mole-bad);animation:none;box-shadow:0 0 0 3px rgba(248,113,113,.16)}
.mole-runbar-label{font-weight:600;color:var(--mole-ink);white-space:nowrap;flex:0 0 auto}
.mole-runbar-detail{flex:1 1 auto;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mole-runbar-counters{white-space:nowrap;color:var(--mole-faint)}
.mole-runbar-time{white-space:nowrap;color:var(--mole-faint)}
.mole-runbar-toggle{border:0;background:transparent;color:#aab5ff;font:inherit;font-size:11px;cursor:pointer;padding:0 2px;white-space:nowrap}
.mole-runbar-toggle:hover{color:#c7cdff}
.mole-runbar-log{margin:0;max-height:170px;overflow:auto;padding:10px 12px;border-radius:9px;background:rgba(0,0,0,.4);border:1px solid rgba(255,255,255,.08);font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:10.5px;line-height:1.6;color:var(--mole-sub);white-space:pre-wrap;word-break:break-word}
@keyframes mole-pulse{0%,100%{opacity:.4}50%{opacity:1}}
@media (max-width:880px){.mole-runbar-wrap{padding:10px 18px 0}.mole-runbar-counters{display:none}}

`

    // ------------------------------------------------------------------
    // locale dictionaries
    // ------------------------------------------------------------------
    const ZH = {
      'nav.title': '大肥鱼 clean',
      'nav.subtitle': '非官方 Mole CLI 面板 · 本机 mo 驱动',
      'nav.clean': '清理',
      'nav.apps': '应用',
      'nav.optimize': '优化',
      'nav.analyze': '分析',
      'nav.status': '状态',
      'nav.tools': '工具',
      'nav.history': '历史',
      'nav.missing': '未安装 mole',
      'nav.local': '本机 CLI',
      'common.loading': '加载中…',
      'common.refresh': '刷新',
      'common.retry': '重试',
      'common.pause': '暂停',
      'common.resume': '继续',
      'common.close': '关闭',
      'common.cancel': '取消',
      'common.expand': '展开',
      'common.collapse': '收起',
      'status.health': '健康度',
      'status.health.note': '基于 CPU、内存、磁盘、温度与电池综合评分',
      'status.uptime': '已运行 {value}',
      'status.procs': '进程 {value}',
      'status.cores': '{value} 核',
      'status.memory': '内存',
      'status.disk': '磁盘',
      'status.disk.free': '可用 {value}',
      'status.network': '网络',
      'status.network.noip': '未分配 IPv4',
      'status.battery': '电池',
      'status.battery.cycles': '循环 {value} 次',
      'status.thermal': '温度与风扇',
      'status.thermal.fanless': '无风扇',
      'status.power': '整机 {value} W',
      'status.adapter': '适配器 {value} W',
      'status.trash': '回收站',
      'status.trash.zombie': '僵尸 {value}',
      'status.trash.note': '废纸篓占用',
      'status.device': '设备',
      'status.processes': '进程占用',
      'status.processes.sub': '按 CPU 排序的前十个进程，来自 mo status --json',
      'status.refresh': '刷新一次',
      'status.col.process': '进程',
      'status.col.cpu': 'CPU',
      'status.col.memory': '内存',
      'status.footnote': '状态面板每 3 秒自动刷新，全部为只读数据。',
      'status.unavailable.title': '暂时读不到系统状态',
      'status.unavailable.body': '请确认本机已安装 mole（brew install mole），并允许它运行。',
      'clean.hero.title': '把空间找回来',
      'clean.hero.sub': 'Mole 会扫描缓存、日志、卸载残留和可重建的开发缓存，逐项列出后再决定是否删除。',
      'clean.scan': '扫描可清理项',
      'clean.rescan': '重新扫描',
      'clean.scanning': '正在扫描缓存、日志和卸载残留…',
      'clean.scanning.btn': '扫描中…',
      'clean.running': '正在清理，请保持窗口打开…',
      'clean.running.sub': 'Mole 会重新扫描并清理默认选中的项目，可能需要几分钟。',
      'clean.found': '发现约 {size} 可清理',
      'clean.nothing': '没有找到可清理的内容',
      'clean.summary': '{groups} 个分组 · {items} 项',
      'clean.run': '执行清理',
      'clean.openTerminal': '在终端中运行',
      'clean.done.title': '清理完成',
      'clean.seeHistory': '查看清理历史',
      'clean.groups.title': '可清理内容',
      'clean.groups.sub': '这是 --dry-run 预览，实际执行时会重新扫描',
      'clean.group.count': '{count} 项 · {size}',
      'clean.pathList': '路径清单（{count}）',
      'clean.notice.title': '执行前请确认',
      'clean.notice.trash': '清理会清空废纸篓（默认开启，可在保护清单中关闭）',
      'clean.notice.permanent': 'clean 删除的文件不可恢复，不走废纸篓',
      'clean.notice.sudo': '系统级缓存需要管理员权限，面板不会请求 sudo；需要完整清理时请到终端运行',
      'clean.notice.rescan': '预览是扫描快照，执行时会重新扫描，数量与大小可能变化',
      'clean.notice.foot': '想保护某些路径，请在终端运行 mo clean --whitelist 保存默认清单后再追加路径。',
      'clean.confirm.title': '确认开始清理？',
      'clean.confirm.body': '即将清理预览中约 {size} 的默认选中项（{items} 项，实际执行时会重新扫描）。',
      'clean.confirm.foot': '此操作删除的文件不可恢复，请确认预览内容符合预期。',
      'clean.confirm.run': '确认清理',
      'apps.title': '已安装应用',
      'apps.subtitle': '{count} 个应用 · 合计 {size}',
      'apps.search': '搜索应用、路径或 bundle id',
      'apps.loading': '正在读取应用列表…',
      'apps.empty': '没有读到应用列表',
      'apps.col.app': '应用',
      'apps.col.source': '来源',
      'apps.col.size': '大小',
      'apps.uninstall': '卸载应用',
      'apps.uninstallData': '连个人数据一起卸载',
      'apps.handoff': '已在新终端窗口打开：{command}，按提示确认即可。',
      'apps.plan.sub': '来源 {source} · 卸载计划来自 mo uninstall --dry-run',
      'apps.plan.loading': '正在生成卸载计划…',
      'apps.plan.foot': '应用卸载需要交互确认和选择，因此交给终端完成；默认保留个人数据，可在终端里用 D 选择。',
      'optimize.hero.title': '让系统轻快一些',
      'optimize.hero.sub': '刷新 Finder 缓存、网络缓存、数据库与常见系统服务，有风险或暂不适用的任务会被跳过。',
      'optimize.scan': '扫描可优化项',
      'optimize.rescan': '重新扫描',
      'optimize.scanning': '正在检查维护任务…',
      'optimize.scanning.btn': '扫描中…',
      'optimize.summary': '共 {groups} 组维护任务',
      'optimize.run': '执行优化',
      'optimize.running': '正在执行维护任务…',
      'optimize.done.title': '优化完成',
      'optimize.tasks.title': '维护任务预览',
      'optimize.tasks.sub': '来自 mo optimize --dry-run',
      'optimize.footnote': '需要管理员权限的任务会被跳过；在终端里运行可以让 Mole 统一请求一次授权。',
      'optimize.confirm.title': '确认执行系统维护？',
      'optimize.confirm.body': 'Mole 将执行预览中不带风险的维护任务，涉及重建缓存和数据库，可能需要几分钟。',
      'optimize.notice.admin': '需要管理员权限的任务会被跳过',
      'optimize.notice.skip': '不适用或有风险的任务自动跳过',
      'optimize.notice.vpn': '检测到 VPN 时会跳过网络缓存刷新',
      'optimize.confirm.run': '确认优化',
      'analyze.title': '磁盘分析',
      'analyze.subtitle': '按占用大小展开文件夹，定位大文件与大目录',
      'analyze.placeholder': '输入要分析的绝对路径，留空分析主目录',
      'analyze.scan': '分析',
      'analyze.reveal': '在 Finder 中显示',
      'analyze.empty.title': '选择一个路径开始分析',
      'analyze.empty.sub': '第一次扫描大目录可能需要几十秒，结果会显示色块地图和最大的条目。',
      'analyze.scanning': '正在扫描目录…',
      'analyze.scanning.sub': '文件较多时会慢一些，请保持窗口打开。',
      'analyze.files': '{value} 个文件',
      'analyze.scan.complete': '完整扫描',
      'analyze.scan.partial': '部分结果',
      'analyze.scan.unavailable': '无法读取',
      'analyze.noentries': '这个目录没有可读条目',
      'analyze.large': '最大的文件',
      'analyze.large.sub': '来自 mo analyze --json 的 large_files',
      'analyze.footnote': '分析是只读操作；系统目录可能因权限只能看到部分结果。',
      'tools.title': '更多清理工具',
      'tools.sub': '项目构建产物、安装包和通用二进制里的 Intel 代码，都可以先预览再决定。',
      'tools.scanning': '扫描中…',
      'tools.scanning.sub': '正在查找候选项目…',
      'tools.preview': '预览',
      'tools.result': '{groups} 组 · {items} 项 · 约 {size}',
      'tools.openTerminal': '在终端中处理',
      'tools.purge.title': '项目构建产物',
      'tools.purge.sub': '清理 node_modules、target、dist 等可重建目录',
      'tools.purge.hint': 'Mole 会按项目分组列出可重建的构建产物与依赖目录，近期有改动的默认不选。',
      'tools.purge.footnote': '清理依赖目录后需要重新下载依赖才能构建。',
      'tools.installer.title': '安装包',
      'tools.installer.sub': '找出下载目录里的 dmg、pkg、iso 等安装包',
      'tools.installer.hint': 'Mole 会扫描下载、桌面与缓存目录中的安装镜像，并列出大小与来源。',
      'tools.slim.title': '移除 Intel 代码',
      'tools.slim.sub': '为 Apple Silicon 应用删除通用二进制里的 x86 部分',
      'tools.slim.hint': '只对带开发者签名的应用生效，执行前会先复制并校验签名，原应用移入废纸篓。',
      'history.title': '清理历史',
      'history.sub': '最近 {count} 次清理与预览记录，来自 mo history --json',
      'history.empty': '还没有清理记录',
      'history.col.command': '命令',
      'history.col.started': '开始时间',
      'history.col.size': '大小',
      'history.col.items': '项目',
      'history.col.result': '结果',
      'history.removed': '删除',
      'history.trashed': '移入废纸篓',
      'history.failed': '失败',
      'history.open': '未结束',
      'history.deletions': '删除明细（{count}）',
      'history.deletions.sub': '展开查看每条被删除或移入废纸篓的路径与状态。',
      'history.footnote': '历史记录来自 ~/Library/Logs/mole/operations.log；可用 MO_NO_OPLOG=1 关闭记录。',
      'status.load': '负载',
      'status.swap': '交换',
      'status.diskio': '读 {read} · 写 {write} MB/s',
      'status.pressure': '压力',
      'status.fan.count': '风扇 {value}',
      'status.battery.temp': '电池 {value}°C',
      'status.fan.speed': '风扇 {value} rpm',
      'missing.installing': '正在安装…',
      'missing.installDone': 'Mole CLI 安装完成，已重新检测。',
      'missing.terminal': '在终端中安装',
      'progress.installMole': '正在安装 Mole CLI',
      'missing.title': '需要先安装 Mole CLI',
      'missing.body': '大肥鱼 clean 是本机 Mole CLI 的图形面板，但系统里还没有找到 mole 命令。安装 Mole CLI 后即可使用系统状态、清理、磁盘分析和应用管理等功能。',
      'missing.platform': '当前插件依赖 macOS 上的 Mole CLI，请在 macOS 上使用。',
      'missing.brew': '用 Homebrew 安装',
      'missing.copy': '复制安装命令',
      'missing.copied': '已复制',
      'missing.recheck': '重新检测',
      'missing.docs': 'Mole CLI 安装说明',
      'missing.after': '安装完成后点「重新检测」；如果终端里 mole 可用但这里仍检测不到，刷新一下页面。',
      'missing.noBrew': '没有检测到 Homebrew，可在终端执行上面的官方安装脚本（会从 GitHub 下载安装）。',
      'missing.terminalOpened': '已在终端打开安装命令，按提示完成安装。',
      'progress.running': '正在运行',
      'progress.waiting': '等待 Mole 输出…',
      'progress.done': '完成',
      'progress.failed': '失败',
      'progress.elapsed': '已用 {value}s',
      'progress.groups': '{value} 组',
      'progress.items': '{value} 项',
      'progress.size': '约 {value}',
      'progress.showLog': '日志',
      'progress.hideLog': '收起',
      'progress.preview.clean': '正在扫描可清理内容',
      'progress.preview.optimize': '正在检查维护任务',
      'progress.preview.purge': '正在查找项目构建产物',
      'progress.preview.installer': '正在查找安装包',
      'progress.preview.slim': '正在扫描通用应用',
      'progress.run.clean': '正在清理',
      'progress.run.optimize': '正在执行维护',
      'progress.run.purge': '正在清理项目构建产物',
      'progress.analyze': '正在分析磁盘',
    }

    const EN = {
      'nav.title': 'Big Fat Fish clean',
      'nav.subtitle': 'Unofficial panel for the Mole CLI',
      'nav.clean': 'Clean',
      'nav.apps': 'Apps',
      'nav.optimize': 'Optimize',
      'nav.analyze': 'Analyze',
      'nav.status': 'Status',
      'nav.tools': 'Tools',
      'nav.history': 'History',
      'nav.missing': 'mole not installed',
      'nav.local': 'local CLI',
      'common.loading': 'Loading…',
      'common.refresh': 'Refresh',
      'common.retry': 'Retry',
      'common.pause': 'Pause',
      'common.resume': 'Resume',
      'common.close': 'Close',
      'common.cancel': 'Cancel',
      'common.expand': 'Expand',
      'common.collapse': 'Collapse',
      'status.health': 'Health',
      'status.health.note': 'Combined score across CPU, memory, disk, thermals and battery',
      'status.uptime': 'up {value}',
      'status.procs': '{value} processes',
      'status.cores': '{value} cores',
      'status.memory': 'Memory',
      'status.disk': 'Disk',
      'status.disk.free': '{value} free',
      'status.network': 'Network',
      'status.network.noip': 'no IPv4 address',
      'status.battery': 'Battery',
      'status.battery.cycles': '{value} cycles',
      'status.thermal': 'Thermals',
      'status.thermal.fanless': 'fanless',
      'status.power': '{value} W system',
      'status.adapter': '{value} W adapter',
      'status.trash': 'Trash',
      'status.trash.zombie': '{value} zombies',
      'status.trash.note': 'Trash size',
      'status.device': 'Device',
      'status.processes': 'Process load',
      'status.processes.sub': 'Top ten processes by CPU from mo status --json',
      'status.refresh': 'Refresh now',
      'status.col.process': 'Process',
      'status.col.cpu': 'CPU',
      'status.col.memory': 'Memory',
      'status.footnote': 'This dashboard refreshes every three seconds and is read-only.',
      'status.unavailable.title': 'System status is unavailable',
      'status.unavailable.body': 'Make sure mole is installed on this Mac (brew install mole) and allowed to run.',
      'clean.hero.title': 'Win your space back',
      'clean.hero.sub': 'Mole scans caches, logs, uninstall leftovers and rebuildable developer caches, then lists every item before anything is removed.',
      'clean.scan': 'Scan for clutter',
      'clean.rescan': 'Scan again',
      'clean.scanning': 'Scanning caches, logs and leftovers…',
      'clean.scanning.btn': 'Scanning…',
      'clean.running': 'Cleaning — keep this window open…',
      'clean.running.sub': 'Mole rescans and removes the default selection; this can take a few minutes.',
      'clean.found': 'About {size} found',
      'clean.nothing': 'Nothing to clean was found',
      'clean.summary': '{groups} groups · {items} items',
      'clean.run': 'Run cleanup',
      'clean.openTerminal': 'Run in Terminal',
      'clean.done.title': 'Cleanup complete',
      'clean.seeHistory': 'See cleanup history',
      'clean.groups.title': 'What Mole found',
      'clean.groups.sub': 'A --dry-run preview; the real run rescans first',
      'clean.group.count': '{count} items · {size}',
      'clean.pathList': 'Path list ({count})',
      'clean.notice.title': 'Before you run it',
      'clean.notice.trash': 'Cleanup empties the Trash by default (protect it in the whitelist to keep it)',
      'clean.notice.permanent': 'clean deletes permanently — there is no Trash to restore from',
      'clean.notice.sudo': 'System caches need administrator access; this panel never asks for sudo — use the Terminal for a full run',
      'clean.notice.rescan': 'A preview is a snapshot; the real run rescans, so counts and sizes can change',
      'clean.notice.foot': 'To protect a path, run mo clean --whitelist once in Terminal to save the defaults, then append your paths.',
      'clean.confirm.title': 'Start the cleanup?',
      'clean.confirm.body': 'Mole will clean the default selection previewed at about {size} ({items} items; it rescans before removing anything).',
      'clean.confirm.foot': 'Deleted files cannot be restored. Confirm the preview matches what you expect.',
      'clean.confirm.run': 'Clean now',
      'apps.title': 'Installed apps',
      'apps.subtitle': '{count} apps · {size} total',
      'apps.search': 'Search apps, paths or bundle ids',
      'apps.loading': 'Reading the app inventory…',
      'apps.empty': 'No app inventory was returned',
      'apps.col.app': 'App',
      'apps.col.source': 'Source',
      'apps.col.size': 'Size',
      'apps.uninstall': 'Uninstall',
      'apps.uninstallData': 'Uninstall with data',
      'apps.handoff': 'Opened in a new Terminal window: {command}. Confirm there to continue.',
      'apps.plan.sub': 'Source {source} · plan from mo uninstall --dry-run',
      'apps.plan.loading': 'Building the removal plan…',
      'apps.plan.foot': 'Uninstall needs interactive confirmation and selection, so it is handed to Terminal; personal data stays selected off unless you choose otherwise.',
      'optimize.hero.title': 'Make the system feel new',
      'optimize.hero.sub': 'Refresh Finder caches, network caches, databases and common system services; risky or inapplicable tasks are skipped.',
      'optimize.scan': 'Scan maintenance tasks',
      'optimize.rescan': 'Scan again',
      'optimize.scanning': 'Checking maintenance tasks…',
      'optimize.scanning.btn': 'Scanning…',
      'optimize.summary': '{groups} maintenance groups',
      'optimize.run': 'Run maintenance',
      'optimize.running': 'Running maintenance tasks…',
      'optimize.done.title': 'Maintenance complete',
      'optimize.tasks.title': 'Maintenance preview',
      'optimize.tasks.sub': 'From mo optimize --dry-run',
      'optimize.footnote': 'Tasks that need administrator access are skipped; running it in Terminal asks for the password once.',
      'optimize.confirm.title': 'Run system maintenance?',
      'optimize.confirm.body': 'Mole will run the previewed maintenance tasks, which rebuild caches and databases and can take a few minutes.',
      'optimize.notice.admin': 'Tasks that need administrator access are skipped',
      'optimize.notice.skip': 'Inapplicable or risky tasks are skipped automatically',
      'optimize.notice.vpn': 'Network cache refresh is skipped while a VPN is active',
      'optimize.confirm.run': 'Optimize now',
      'analyze.title': 'Disk analysis',
      'analyze.subtitle': 'Open folders by size and find the largest files and directories',
      'analyze.placeholder': 'Absolute path to scan; leave empty for the home folder',
      'analyze.scan': 'Analyze',
      'analyze.reveal': 'Reveal in Finder',
      'analyze.empty.title': 'Pick a path to start',
      'analyze.empty.sub': 'The first scan of a large folder can take tens of seconds; results show a treemap and the largest entries.',
      'analyze.scanning': 'Scanning the folder…',
      'analyze.scanning.sub': 'Large folders take longer; keep this window open.',
      'analyze.files': '{value} files',
      'analyze.scan.complete': 'complete',
      'analyze.scan.partial': 'partial',
      'analyze.scan.unavailable': 'unavailable',
      'analyze.noentries': 'No readable entries in this folder',
      'analyze.large': 'Largest files',
      'analyze.large.sub': 'large_files from mo analyze --json',
      'analyze.footnote': 'Analysis is read-only; system folders may show partial results without disk access.',
      'tools.title': 'More cleanup tools',
      'tools.sub': 'Project artifacts, installer images and Intel code inside universal apps — preview each before deciding.',
      'tools.scanning': 'Scanning…',
      'tools.scanning.sub': 'Looking for candidates…',
      'tools.preview': 'Preview',
      'tools.result': '{groups} groups · {items} items · about {size}',
      'tools.openTerminal': 'Handle in Terminal',
      'tools.purge.title': 'Project artifacts',
      'tools.purge.sub': 'Remove node_modules, target, dist and other rebuildable folders',
      'tools.purge.hint': 'Mole groups rebuildable build output and dependency folders per project; recently touched ones start unselected.',
      'tools.purge.footnote': 'Dependency folders need a fresh download before the next build.',
      'tools.installer.title': 'Installer images',
      'tools.installer.sub': 'Find dmg, pkg, iso and zip installers in downloads',
      'tools.installer.hint': 'Mole scans Downloads, Desktop and caches for installer images and lists size and source.',
      'tools.slim.title': 'Remove Intel code',
      'tools.slim.sub': 'Strip x86 slices from Apple Silicon universal apps',
      'tools.slim.hint': 'Only Developer ID apps are touched; each app is slimmed on a copy that must keep its signature, and the original moves to the Trash.',
      'history.title': 'Cleanup history',
      'history.sub': 'The last {count} runs and previews from mo history --json',
      'history.empty': 'No cleanup history yet',
      'history.col.command': 'Command',
      'history.col.started': 'Started',
      'history.col.size': 'Size',
      'history.col.items': 'Items',
      'history.col.result': 'Result',
      'history.removed': 'removed',
      'history.trashed': 'trashed',
      'history.failed': 'failed',
      'history.open': 'open',
      'history.deletions': 'Deletion detail ({count})',
      'history.deletions.sub': 'Expand to see every removed or trashed path and its status.',
      'history.footnote': 'History comes from ~/Library/Logs/mole/operations.log; set MO_NO_OPLOG=1 to opt out.',
      'status.load': 'load',
      'status.swap': 'swap',
      'status.diskio': 'R {read} · W {write} MB/s',
      'status.pressure': 'pressure',
      'status.fan.count': 'fan {value}',
      'status.battery.temp': 'battery {value}°C',
      'status.fan.speed': 'fan {value} rpm',
      'missing.installing': 'Installing…',
      'missing.installDone': 'Mole CLI installed — re-checked.',
      'missing.terminal': 'Install in Terminal',
      'progress.installMole': 'Installing Mole CLI',
      'missing.title': 'Install the Mole CLI first',
      'missing.body': 'Big Fat Fish clean is a graphical panel for the local Mole CLI, but the mole command was not found. Install the Mole CLI to use system health, cleanup, disk analysis and app management.',
      'missing.platform': 'This plugin needs the Mole CLI on macOS.',
      'missing.brew': 'Install with Homebrew',
      'missing.copy': 'Copy install command',
      'missing.copied': 'Copied',
      'missing.recheck': 'Check again',
      'missing.docs': 'Mole CLI install guide',
      'missing.after': 'After installing, click "Check again". If mole works in your terminal but is still not detected here, refresh the page.',
      'missing.noBrew': 'Homebrew was not detected. Run the official install script above in your Terminal (it downloads from GitHub).',
      'missing.terminalOpened': 'Opened the install command in Terminal — follow the prompts.',
      'progress.running': 'Running',
      'progress.waiting': 'Waiting for Mole output…',
      'progress.done': 'Done',
      'progress.failed': 'Failed',
      'progress.elapsed': '{value}s elapsed',
      'progress.groups': '{value} groups',
      'progress.items': '{value} items',
      'progress.size': 'about {value}',
      'progress.showLog': 'Log',
      'progress.hideLog': 'Hide',
      'progress.preview.clean': 'Scanning cleanable files',
      'progress.preview.optimize': 'Checking maintenance tasks',
      'progress.preview.purge': 'Looking for build artifacts',
      'progress.preview.installer': 'Looking for installers',
      'progress.preview.slim': 'Scanning universal apps',
      'progress.run.clean': 'Cleaning',
      'progress.run.optimize': 'Running maintenance',
      'progress.run.purge': 'Removing build artifacts',
      'progress.analyze': 'Analyzing disk',
    }

    return {
      inject: ['slots', 'locale'],
      apply(ctx) {
        ctx.effect(() => ctx.locale.register(NS, { zh: ZH, en: EN }), 'dsh-mole: dictionaries')
        ctx.effect(
          () =>
            ctx.slots.register(
              { name: 'main', key: PANEL_ID, locale: NS },
              (props) => h(MainPanel, { ...props, ctx }),
            ),
          'dsh-mole: main panel',
        )
        ctx.effect(
          () =>
            ctx.slots.register(
              {
                name: 'sidebar.panellist',
                id: PANEL_ID,
                order: 6,
                label: () => ctx.locale.bind(NS)('nav.title'),
                locale: NS,
              },
              (owner) => h(SidebarMascot, { size: owner?.size ?? 18 }),
            ),
          'dsh-mole: sidebar entry',
        )
        ctx.effect(() => {
          const style = document.createElement('style')
          style.setAttribute('data-dsh-mole', 'nav')
          style.textContent = '.mole-nav-mark{display:inline-flex;align-items:center;justify-content:center;color:inherit}.mole-nav-icon{display:block;object-fit:contain;filter:invert(1);opacity:.92}body[data-ds-dark-theme] .mole-nav-icon{filter:none}'
          document.head.appendChild(style)
          return () => style.remove()
        }, 'dsh-mole: nav styles')
      },
    }
  },
})

