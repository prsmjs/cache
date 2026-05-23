import { EventEmitter } from 'node:events'
import { randomUUID } from 'node:crypto'
import { createClient } from 'redis'
import ms from '@prsm/ms'

const RELEASE_SCRIPT = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`

function toMs(value, fallback = null) {
  if (value === null || value === undefined) return fallback
  if (typeof value === 'number') return value
  return ms(value)
}

function makeRedis(redisOpt) {
  if (redisOpt && typeof redisOpt.duplicate === 'function') return redisOpt
  const opts = {}
  if (redisOpt?.url) opts.url = redisOpt.url
  if (redisOpt?.host || redisOpt?.port) {
    opts.socket = { host: redisOpt.host ?? '127.0.0.1', port: redisOpt.port ?? 6379 }
  }
  if (redisOpt?.password) opts.password = redisOpt.password
  if (redisOpt?.db !== undefined) opts.database = redisOpt.db
  return createClient(opts)
}

export function createCache(options = {}) {
  const tracer = options.tracer ?? null
  const prefix = options.prefix ?? 'cache:'
  const defaultTtl = toMs(options.defaultTtl ?? '5m', 5 * 60_000)
  const defaultStaleWhile = toMs(options.defaultStaleWhile, 0)
  const defaultNegativeTtl = toMs(options.defaultNegativeTtl, null)
  const defaultErrorTtl = toMs(options.defaultErrorTtl, 0)
  const defaultLockTtl = toMs(options.defaultLockTtl ?? '30s', 30_000)
  const defaultWaitTimeout = toMs(options.waitTimeout ?? '10s', 10_000)
  const serialize = options.serialize ?? JSON.stringify
  const deserialize = options.deserialize ?? JSON.parse

  const emitter = new EventEmitter()
  emitter.setMaxListeners(0)
  emitter.on('error', () => {})

  const client = makeRedis(options.redis)
  const subscriber = client.duplicate()

  client.on('error', (err) => emitter.emit('error', { source: 'client', error: err }))
  subscriber.on('error', (err) => emitter.emit('error', { source: 'subscriber', error: err }))

  const stats = {
    hits: 0,
    misses: 0,
    sets: 0,
    dels: 0,
    errors: 0,
    refreshes: 0,
    stampedeLeads: 0,
    stampedeWaits: 0,
    stampedeSavings: 0,
    invalidations: 0,
  }

  let readyPromise = null
  let closed = false
  const refreshing = new Set()

  const valueKey = (k) => `${prefix}v:${k}`
  const lockKey = (k) => `${prefix}lock:${k}`
  const tagKey = (t) => `${prefix}tag:${t}`
  const keyTagsKey = (k) => `${prefix}keytags:${k}`
  const resultChannel = (k) => `${prefix}result:${k}`
  const errorChannel = (k) => `${prefix}error:${k}`

  function ensureReady() {
    if (!readyPromise) {
      readyPromise = (async () => {
        if (!client.isOpen) await client.connect()
        if (!subscriber.isOpen) await subscriber.connect()
      })().catch((err) => {
        readyPromise = null
        throw err
      })
    }
    return readyPromise
  }

  async function readRaw(key) {
    const raw = await client.get(valueKey(key))
    if (!raw) return null
    try {
      return deserialize(raw)
    } catch (err) {
      emitter.emit('error', { source: 'deserialize', key, error: err })
      return null
    }
  }

  async function writeRaw(key, entry, ttlMs, tags) {
    const payload = serialize(entry)
    const fullKey = valueKey(key)
    const tx = client.multi()
    if (ttlMs > 0) tx.set(fullKey, payload, { PX: ttlMs })
    else tx.set(fullKey, payload)

    const prevTags = await client.sMembers(keyTagsKey(key))
    for (const oldTag of prevTags) {
      if (!tags?.includes(oldTag)) tx.sRem(tagKey(oldTag), key)
    }

    if (tags?.length) {
      tx.del(keyTagsKey(key))
      tx.sAdd(keyTagsKey(key), tags)
      for (const tag of tags) tx.sAdd(tagKey(tag), key)
    } else if (prevTags.length) {
      tx.del(keyTagsKey(key))
    }

    await tx.exec()
  }

  async function tryAcquireLock(key, lockTtlMs) {
    const id = randomUUID()
    const ok = await client.set(lockKey(key), id, { NX: true, PX: lockTtlMs })
    return ok ? id : null
  }

  async function releaseLock(key, id) {
    try {
      await client.eval(RELEASE_SCRIPT, { keys: [lockKey(key)], arguments: [id] })
    } catch (err) {
      emitter.emit('error', { source: 'release-lock', key, error: err })
    }
  }

  function makeEntry(value, ttlMs, staleWhileMs, tags) {
    const now = Date.now()
    return {
      value,
      expiresAt: now + ttlMs,
      staleUntil: now + ttlMs + staleWhileMs,
      tags: tags ?? null,
    }
  }

  function isFresh(entry) {
    return entry && Date.now() < entry.expiresAt
  }

  function isStaleServeable(entry) {
    if (!entry) return false
    const now = Date.now()
    return now >= entry.expiresAt && now < entry.staleUntil
  }

  async function publishResult(key, entry) {
    try {
      await client.publish(resultChannel(key), serialize(entry))
    } catch (err) {
      emitter.emit('error', { source: 'publish-result', key, error: err })
    }
  }

  async function publishError(key, err) {
    try {
      const payload = JSON.stringify({ message: err?.message ?? String(err), name: err?.name ?? 'Error' })
      await client.publish(errorChannel(key), payload)
    } catch (e) {
      emitter.emit('error', { source: 'publish-error', key, error: e })
    }
  }

  async function waitForLeader(key, waitTimeoutMs) {
    const resultCh = resultChannel(key)
    const errorCh = errorChannel(key)

    let settled = false
    let timer = null
    let onResult, onError

    const cleanup = async () => {
      try { await subscriber.unsubscribe(resultCh, onResult) } catch {}
      try { await subscriber.unsubscribe(errorCh, onError) } catch {}
    }

    const outcome = await new Promise((resolve, reject) => {
      onResult = (message) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        try { resolve({ status: 'value', entry: deserialize(message) }) }
        catch (err) { reject(err) }
      }
      onError = (message) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        try {
          const { message: m, name } = JSON.parse(message)
          const err = new Error(m)
          err.name = name
          resolve({ status: 'error', error: err })
        } catch (err) { reject(err) }
      }
      timer = setTimeout(() => {
        if (settled) return
        settled = true
        resolve({ status: 'timeout' })
      }, waitTimeoutMs)

      Promise.all([
        subscriber.subscribe(resultCh, onResult),
        subscriber.subscribe(errorCh, onError),
      ]).then(async () => {
        const entry = await readRaw(key)
        if (settled) return
        if (entry && !entry.error && Date.now() < entry.expiresAt) {
          settled = true
          clearTimeout(timer)
          resolve({ status: 'value', entry })
        }
      }).catch((err) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        reject(err)
      })
    })

    await cleanup()
    return outcome
  }

  async function fetchInternal(key, loader, fetchOptions = {}) {
    if (closed) throw new Error('cache is closed')
    if (typeof loader !== 'function') throw new TypeError('loader must be a function')

    const ttlMs = toMs(fetchOptions.ttl, defaultTtl)
    const staleWhileMs = toMs(fetchOptions.staleWhile, defaultStaleWhile)
    const negativeTtlMs = toMs(fetchOptions.negativeTtl, defaultNegativeTtl)
    const errorTtlMs = toMs(fetchOptions.errorTtl, defaultErrorTtl)
    const lockTtlMs = toMs(fetchOptions.lockTtl, defaultLockTtl)
    const waitTimeoutMs = toMs(fetchOptions.waitTimeout, defaultWaitTimeout)
    const tags = fetchOptions.tags ?? null

    if (ttlMs === 0) {
      const value = await loader()
      return value
    }

    const entry = await readRaw(key)

    if (isFresh(entry)) {
      if (entry.error) {
        stats.hits++
        emitter.emit('hit', { key, fresh: true, error: true })
        const err = new Error(entry.error.message)
        err.name = entry.error.name
        err.cached = true
        throw err
      }
      stats.hits++
      emitter.emit('hit', { key, fresh: true })
      return entry.value
    }

    if (isStaleServeable(entry) && staleWhileMs > 0 && !entry.error) {
      stats.hits++
      emitter.emit('hit', { key, fresh: false })
      if (!refreshing.has(key)) {
        refreshing.add(key)
        backgroundRefresh(key, loader, { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, lockTtlMs, tags })
          .finally(() => refreshing.delete(key))
      }
      return entry.value
    }

    stats.misses++
    emitter.emit('miss', { key })

    return loadWithSingleFlight(key, loader, { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, lockTtlMs, waitTimeoutMs, tags })
  }

  async function fetch(key, loader, fetchOptions = {}) {
    await ensureReady()
    if (!tracer) return fetchInternal(key, loader, fetchOptions)
    return tracer.span('cache.fetch', { 'cache.key': key, 'cache.prefix': prefix }, async (span) => {
      const value = await fetchInternal(key, async () => {
        return tracer.span('cache.loader', { 'cache.key': key }, async () => loader())
      }, fetchOptions)
      return value
    })
  }

  async function loadWithSingleFlight(key, loader, opts) {
    const { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, lockTtlMs, waitTimeoutMs, tags } = opts

    const lockId = await tryAcquireLock(key, lockTtlMs)

    if (lockId) {
      stats.stampedeLeads++
      emitter.emit('stampede:lead', { key })
      return runLoader(key, lockId, loader, { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, tags })
    }

    stats.stampedeWaits++
    emitter.emit('stampede:wait', { key })
    const waitedFrom = Date.now()

    const outcome = await waitForLeader(key, waitTimeoutMs)
    const waitedMs = Date.now() - waitedFrom

    if (outcome.status === 'value') {
      stats.stampedeSavings++
      emitter.emit('stampede:result', { key, waitedMs })
      return outcome.entry.value
    }

    if (outcome.status === 'error') {
      emitter.emit('stampede:result', { key, waitedMs })
      throw outcome.error
    }

    const settled = await readRaw(key)
    if (settled && (isFresh(settled) || isStaleServeable(settled))) {
      stats.stampedeSavings++
      emitter.emit('stampede:result', { key, waitedMs })
      return settled.value
    }

    emitter.emit('stampede:timeout', { key, waitedMs })
    return loader()
  }

  async function runLoader(key, lockId, loader, opts) {
    const { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, tags } = opts
    try {
      const value = await loader()
      const isNull = value === null
      const isUndefined = value === undefined
      const shouldCache = !isUndefined && (!isNull || negativeTtlMs !== null)
      const effectiveTtl = isNull && negativeTtlMs !== null ? negativeTtlMs : ttlMs
      if (shouldCache) {
        const entry = makeEntry(value, effectiveTtl, staleWhileMs, tags)
        await writeRaw(key, entry, effectiveTtl + staleWhileMs, tags)
        stats.sets++
        emitter.emit('set', { key, ttl: effectiveTtl, tags })
        await publishResult(key, entry)
      }
      return value
    } catch (err) {
      stats.errors++
      emitter.emit('error', { source: 'loader', key, error: err })
      if (errorTtlMs > 0) {
        const entry = { value: null, expiresAt: Date.now() + errorTtlMs, staleUntil: Date.now() + errorTtlMs, tags: null, error: { name: err?.name ?? 'Error', message: err?.message ?? String(err) } }
        try {
          await client.set(valueKey(key), serialize(entry), { PX: errorTtlMs })
        } catch {}
      }
      await publishError(key, err)
      throw err
    } finally {
      await releaseLock(key, lockId)
    }
  }

  async function backgroundRefresh(key, loader, opts) {
    const { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, lockTtlMs, tags } = opts
    const lockId = await tryAcquireLock(key, lockTtlMs)
    if (!lockId) return
    stats.refreshes++
    emitter.emit('refresh', { key })
    try {
      await runLoader(key, lockId, loader, { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, tags })
    } catch {}
  }

  async function get(key) {
    await ensureReady()
    const entry = await readRaw(key)
    if (isFresh(entry) && !entry.error) {
      stats.hits++
      emitter.emit('hit', { key, fresh: true })
      return entry.value
    }
    stats.misses++
    emitter.emit('miss', { key })
    return undefined
  }

  async function setInternal(key, value, setOptions = {}) {
    const ttlMs = toMs(setOptions.ttl, defaultTtl)
    const staleWhileMs = toMs(setOptions.staleWhile, defaultStaleWhile)
    const tags = setOptions.tags ?? null
    if (ttlMs === 0) {
      await del(key)
      return
    }
    const entry = makeEntry(value, ttlMs, staleWhileMs, tags)
    await writeRaw(key, entry, ttlMs + staleWhileMs, tags)
    stats.sets++
    emitter.emit('set', { key, ttl: ttlMs, tags })
  }

  async function set(key, value, setOptions = {}) {
    await ensureReady()
    if (!tracer) return setInternal(key, value, setOptions)
    return tracer.span('cache.set', { 'cache.key': key }, () => setInternal(key, value, setOptions))
  }

  async function del(key) {
    await ensureReady()
    const prevTags = await client.sMembers(keyTagsKey(key))
    const tx = client.multi()
    tx.del(valueKey(key))
    tx.del(keyTagsKey(key))
    for (const tag of prevTags) tx.sRem(tagKey(tag), key)
    const results = await tx.exec()
    const removed = (results?.[0] ?? 0) > 0
    if (removed) {
      stats.dels++
      emitter.emit('del', { key })
    }
    return removed
  }

  async function has(key) {
    await ensureReady()
    const entry = await readRaw(key)
    return !!isFresh(entry) && !entry.error
  }

  async function invalidateTagInternal(tag) {
    const keys = await client.sMembers(tagKey(tag))
    if (!keys.length) {
      await client.del(tagKey(tag))
      return 0
    }
    const tx = client.multi()
    for (const k of keys) {
      tx.del(valueKey(k))
      tx.del(keyTagsKey(k))
    }
    tx.del(tagKey(tag))
    await tx.exec()
    stats.invalidations++
    emitter.emit('invalidate', { tag, count: keys.length })
    return keys.length
  }

  async function invalidateTag(tag) {
    await ensureReady()
    if (!tracer) return invalidateTagInternal(tag)
    return tracer.span('cache.invalidateTag', { 'cache.tag': tag }, () => invalidateTagInternal(tag))
  }

  async function close() {
    if (closed) return
    closed = true
    try {
      if (subscriber.isOpen) await subscriber.quit()
    } catch {}
    try {
      if (client.isOpen) await client.quit()
    } catch {}
  }

  return {
    ready: ensureReady,
    fetch,
    get,
    set,
    del,
    has,
    invalidateTag,
    close,
    stats: () => ({ ...stats }),
    on: emitter.on.bind(emitter),
    off: emitter.off.bind(emitter),
  }
}
