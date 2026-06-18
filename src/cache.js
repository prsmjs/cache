import { EventEmitter } from 'node:events'
import { randomUUID } from 'node:crypto'
import { createClient } from 'redis'
import ms from '@prsm/ms'

const RELEASE_SCRIPT = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`

/**
 * @typedef {Object} RedisOptions
 * Connection settings forwarded to node-redis `createClient`. Omit to connect to redis on localhost:6379. You may also pass an existing node-redis client instead of this object, in which case the cache uses it directly and calls `.duplicate()` for its subscriber.
 * @property {string} [url] - full connection string, e.g. `redis://user:pass@host:6379/0`. Takes precedence over the discrete host/port fields below
 * @property {string} [host] - redis host (default `127.0.0.1`)
 * @property {number} [port] - redis port (default `6379`)
 * @property {string} [password] - password, if the server requires auth
 * @property {number} [db] - database index to select (default: the server default, usually `0`)
 */

/**
 * @typedef {Object} CacheOptions
 * @property {RedisOptions|object} [redis] - redis connection settings, or an existing node-redis client to reuse. When a client is passed, the cache duplicates it for the pub/sub subscriber rather than opening fresh connections
 * @property {string} [prefix] - namespace prepended to every redis key this cache touches (default `"cache:"`). Two caches sharing a redis instance but using different prefixes do not see each other's data or invalidation events
 * @property {string|number} [defaultTtl] - default fresh lifetime for `fetch` and `set`, as a duration string (`"5m"`, `"30s"`) or milliseconds (default `"5m"`). A ttl of `0` disables caching: `fetch` just runs the loader and `set` deletes the key
 * @property {string|number} [defaultStaleWhile] - default stale-while-revalidate window measured past the ttl (default `0`, disabled). When greater than zero, an expired-but-still-stale entry is served immediately while a single background refresh runs, so requests never block on regeneration
 * @property {string|number|null} [defaultNegativeTtl] - default ttl for caching `null` loader results so repeated misses do not keep hitting the backend (default `null`, meaning nulls are not cached). `undefined` results are never cached regardless of this setting
 * @property {string|number} [defaultErrorTtl] - default ttl for caching a thrown loader error so a backend outage does not cascade into a stampede (default `0`, disabled). A cached error is re-thrown to callers with `error.cached === true` until it expires
 * @property {string|number} [defaultLockTtl] - default lifetime of the per-key single-flight lock held by the loader leader (default `"30s"`). This is a crash safety net: if the leader dies mid-load the lock auto-expires so another caller can take over. Set it longer than the slowest expected loader run
 * @property {string|number} [waitTimeout] - how long a non-leader request waits on the pub/sub result channel for the leader to finish before giving up and running the loader itself (default `"10s"`)
 * @property {(value: any) => string} [serialize] - function used to encode cache entries before writing to redis (default `JSON.stringify`). Must round-trip with `deserialize`
 * @property {(raw: string) => any} [deserialize] - function used to decode cache entries read from redis (default `JSON.parse`). Must round-trip with `serialize`
 * @property {object} [tracer] - optional `@prsm/trace` tracer; when set, `fetch`, `set`, `invalidateTag`, and each loader run execute inside spans
 */

/**
 * @typedef {Object} FetchOptions
 * @property {string|number} [ttl] - fresh lifetime for this entry, overriding `defaultTtl`. A ttl of `0` bypasses the cache and just runs the loader
 * @property {string|number} [staleWhile] - stale-while-revalidate window for this entry, overriding `defaultStaleWhile`
 * @property {string|number|null} [negativeTtl] - ttl for caching a `null` result, overriding `defaultNegativeTtl`
 * @property {string|number} [errorTtl] - ttl for caching a thrown error, overriding `defaultErrorTtl`
 * @property {string|number} [lockTtl] - single-flight lock lifetime for this fetch, overriding `defaultLockTtl`
 * @property {string|number} [waitTimeout] - how long to wait for the leader's result before loading locally, overriding the cache-level `waitTimeout`
 * @property {string[]} [tags] - tags to attach to the cached entry so `invalidateTag` can wipe it later
 */

/**
 * @typedef {Object} SetOptions
 * @property {string|number} [ttl] - fresh lifetime for the value, overriding `defaultTtl`. A ttl of `0` deletes the key instead of writing it
 * @property {string|number} [staleWhile] - stale-while-revalidate window past the ttl, overriding `defaultStaleWhile`
 * @property {string[]} [tags] - tags to attach so `invalidateTag` can wipe this key later
 */

/**
 * @typedef {Object} CacheStats
 * @property {number} hits - entries served from cache, fresh or stale
 * @property {number} misses - lookups that found no usable entry and triggered a load
 * @property {number} sets - values written to redis
 * @property {number} dels - keys removed via `del`
 * @property {number} errors - loader invocations that threw
 * @property {number} refreshes - background stale-while-revalidate refreshes started
 * @property {number} stampedeLeads - times this instance won the single-flight lock and ran the loader
 * @property {number} stampedeWaits - times this instance waited on a leader instead of loading
 * @property {number} stampedeSavings - waits that received the leader's result, avoiding a redundant load
 * @property {number} invalidations - tag invalidations performed
 */

/**
 * @callback Loader
 * @returns {Promise<any>|any} the value to cache. Returning `undefined` caches nothing; returning `null` caches only when a negative ttl applies
 */

/**
 * @typedef {Object} Cache
 * @property {() => Promise<void>} ready - ensure both redis connections are open and the event subscription is active. Called implicitly by every method, but useful to await up front so the first request does not pay the connect cost
 * @property {(key: string, loader: Loader, options?: FetchOptions) => Promise<any>} fetch - read-through fetch: return the cached value, or run `loader` on a miss with single-flight coordination so concurrent callers across all instances trigger only one load
 * @property {(key: string) => Promise<any|undefined>} get - return the value only if the key is currently fresh, otherwise `undefined`. Never runs a loader and never serves stale entries
 * @property {(key: string, value: any, options?: SetOptions) => Promise<void>} set - write a value directly, bypassing the loader path
 * @property {(key: string) => Promise<boolean>} del - remove a key and its tag membership. Resolves `true` if a key was actually removed
 * @property {(key: string) => Promise<boolean>} has - resolve `true` if the key is fresh and not an error entry
 * @property {(tag: string) => Promise<number>} invalidateTag - remove every key tagged with `tag` and resolve the count removed
 * @property {() => Promise<void>} close - quit both redis connections. The cache is unusable afterward and `fetch` will throw
 * @property {() => CacheStats} stats - snapshot the running counters for this instance (counters are per-process, not shared across instances)
 * @property {(event: string, handler: (data: any) => void) => void} on - subscribe to a cache event. Events: `hit`, `miss`, `set`, `del`, `invalidate`, `refresh`, `stampede:lead`, `stampede:wait`, `stampede:result`, `stampede:timeout`, and `error`. The distributed events also fire when other instances act on the shared prefix
 * @property {(event: string, handler: (data: any) => void) => void} off - unsubscribe a handler previously registered with `on`
 */

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

/**
 * Create a distributed read-through cache backed by redis. The cache prevents
 * stampedes via per-key single-flight locking, supports stale-while-revalidate,
 * and can invalidate groups of keys by tag.
 * @param {CacheOptions} [options]
 * @returns {Cache}
 */
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
  const instanceId = randomUUID()
  const eventsChannel = `${prefix}events`

  client.on('error', (err) => emitter.emit('error', { source: 'client', error: err }))
  subscriber.on('error', (err) => emitter.emit('error', { source: 'subscriber', error: err }))

  const DISTRIBUTED_EVENTS = new Set([
    'hit', 'miss', 'set', 'del', 'refresh', 'invalidate',
    'stampede:lead', 'stampede:wait', 'stampede:result', 'stampede:timeout',
  ])

  function emit(name, data) {
    const tagged = DISTRIBUTED_EVENTS.has(name) ? { ...data, instanceId } : data
    emitter.emit(name, tagged)
    if (DISTRIBUTED_EVENTS.has(name) && client.isOpen) {
      client.publish(eventsChannel, JSON.stringify({ name, data: tagged, from: instanceId })).catch(() => {})
    }
  }

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

  /**
   * Ensure both redis connections are open and the event subscription is active.
   * Idempotent and called implicitly by every method; await it up front to avoid
   * paying the connect cost on the first request.
   * @returns {Promise<void>}
   */
  function ensureReady() {
    if (!readyPromise) {
      readyPromise = (async () => {
        if (!client.isOpen) await client.connect()
        if (!subscriber.isOpen) await subscriber.connect()
        await subscriber.subscribe(eventsChannel, (message) => {
          try {
            const { name, data, from } = JSON.parse(message)
            if (from === instanceId) return
            emitter.emit(name, data)
          } catch {}
        })
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
        emit('hit', { key, fresh: true, error: true })
        const err = new Error(entry.error.message)
        err.name = entry.error.name
        err.cached = true
        throw err
      }
      stats.hits++
      emit('hit', { key, fresh: true })
      return entry.value
    }

    if (isStaleServeable(entry) && staleWhileMs > 0 && !entry.error) {
      stats.hits++
      emit('hit', { key, fresh: false })
      if (!refreshing.has(key)) {
        refreshing.add(key)
        backgroundRefresh(key, loader, { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, lockTtlMs, tags })
          .finally(() => refreshing.delete(key))
      }
      return entry.value
    }

    stats.misses++
    emit('miss', { key })

    return loadWithSingleFlight(key, loader, { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, lockTtlMs, waitTimeoutMs, tags })
  }

  /**
   * Read-through fetch. Returns the cached value if fresh; on a miss, coordinates
   * a single-flight load across every instance sharing this prefix so the loader
   * runs once and the rest receive the result over pub/sub.
   * @param {string} key - cache key (stored under the cache prefix)
   * @param {Loader} loader - called on a miss to produce the value
   * @param {FetchOptions} [fetchOptions]
   * @returns {Promise<any>} the cached or freshly loaded value
   */
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
      emit('stampede:lead', { key })
      return runLoader(key, lockId, loader, { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, tags })
    }

    stats.stampedeWaits++
    emit('stampede:wait', { key })
    const waitedFrom = Date.now()

    const outcome = await waitForLeader(key, waitTimeoutMs)
    const waitedMs = Date.now() - waitedFrom

    if (outcome.status === 'value') {
      stats.stampedeSavings++
      emit('stampede:result', { key, waitedMs })
      return outcome.entry.value
    }

    if (outcome.status === 'error') {
      emit('stampede:result', { key, waitedMs })
      throw outcome.error
    }

    const settled = await readRaw(key)
    if (settled && (isFresh(settled) || isStaleServeable(settled))) {
      stats.stampedeSavings++
      emit('stampede:result', { key, waitedMs })
      return settled.value
    }

    emit('stampede:timeout', { key, waitedMs })
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
        emit('set', { key, ttl: effectiveTtl, tags })
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
    emit('refresh', { key })
    try {
      await runLoader(key, lockId, loader, { ttlMs, staleWhileMs, negativeTtlMs, errorTtlMs, tags })
    } catch {}
  }

  /**
   * Return the value only if the key is currently fresh. Does not run any loader
   * and does not serve stale entries.
   * @param {string} key - cache key
   * @returns {Promise<any|undefined>} the value, or `undefined` if missing, stale, or an error entry
   */
  async function get(key) {
    await ensureReady()
    const entry = await readRaw(key)
    if (isFresh(entry) && !entry.error) {
      stats.hits++
      emit('hit', { key, fresh: true })
      return entry.value
    }
    stats.misses++
    emit('miss', { key })
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
    emit('set', { key, ttl: ttlMs, tags })
  }

  /**
   * Write a value directly, bypassing the loader path. A ttl of `0` deletes the key.
   * @param {string} key - cache key
   * @param {any} value - value to store
   * @param {SetOptions} [setOptions]
   * @returns {Promise<void>}
   */
  async function set(key, value, setOptions = {}) {
    await ensureReady()
    if (!tracer) return setInternal(key, value, setOptions)
    return tracer.span('cache.set', { 'cache.key': key }, () => setInternal(key, value, setOptions))
  }

  /**
   * Remove a key and its tag membership.
   * @param {string} key - cache key
   * @returns {Promise<boolean>} true if a key was actually removed
   */
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
      emit('del', { key })
    }
    return removed
  }

  /**
   * Check whether a key is fresh without serving stale entries or running a loader.
   * @param {string} key - cache key
   * @returns {Promise<boolean>} true if the key is fresh and not an error entry
   */
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
    emit('invalidate', { tag, count: keys.length })
    return keys.length
  }

  /**
   * Remove every key that was stored with `tag`. The invalidation is broadcast to
   * other instances sharing the prefix via the `invalidate` event.
   * @param {string} tag - tag to invalidate
   * @returns {Promise<number>} the number of keys removed
   */
  async function invalidateTag(tag) {
    await ensureReady()
    if (!tracer) return invalidateTagInternal(tag)
    return tracer.span('cache.invalidateTag', { 'cache.tag': tag }, () => invalidateTagInternal(tag))
  }

  /**
   * Quit both redis connections. The cache is unusable afterward.
   * @returns {Promise<void>}
   */
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
