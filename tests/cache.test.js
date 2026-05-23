import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createClient } from 'redis'
import { createCache } from '../src/index.js'

const REDIS = { host: '127.0.0.1', port: 6379 }
let admin

async function flush() {
  if (!admin) {
    admin = createClient({ socket: REDIS })
    admin.on('error', () => {})
    await admin.connect()
  }
  await admin.flushDb()
}

const caches = []
function make(opts = {}) {
  const c = createCache({ redis: REDIS, prefix: 'test:', ...opts })
  caches.push(c)
  return c
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

beforeEach(async () => {
  await flush()
})

afterEach(async () => {
  while (caches.length) {
    const c = caches.pop()
    await c.close().catch(() => {})
  }
})

describe('fetch / get / set / del / has', () => {
  it('fetch runs the loader on miss and caches the result', async () => {
    const c = make()
    let calls = 0
    const v1 = await c.fetch('k1', async () => { calls++; return { name: 'alice' } })
    const v2 = await c.fetch('k1', async () => { calls++; return { name: 'bob' } })
    expect(v1).toEqual({ name: 'alice' })
    expect(v2).toEqual({ name: 'alice' })
    expect(calls).toBe(1)
  })

  it('fetch throws TypeError if loader is not a function', async () => {
    const c = make()
    await expect(c.fetch('k', 'not-a-fn')).rejects.toThrow(TypeError)
  })

  it('get returns undefined on miss and value on hit', async () => {
    const c = make()
    expect(await c.get('absent')).toBeUndefined()
    await c.set('present', 42, { ttl: '5s' })
    expect(await c.get('present')).toBe(42)
  })

  it('set followed by del removes the value', async () => {
    const c = make()
    await c.set('x', 'hello', { ttl: '5s' })
    expect(await c.get('x')).toBe('hello')
    expect(await c.del('x')).toBe(true)
    expect(await c.get('x')).toBeUndefined()
    expect(await c.del('x')).toBe(false)
  })

  it('has reflects freshness', async () => {
    const c = make()
    expect(await c.has('k')).toBe(false)
    await c.set('k', 1, { ttl: '5s' })
    expect(await c.has('k')).toBe(true)
  })

  it('ttl: 0 bypasses caching entirely', async () => {
    const c = make()
    let calls = 0
    await c.fetch('k', async () => { calls++; return 1 }, { ttl: 0 })
    await c.fetch('k', async () => { calls++; return 1 }, { ttl: 0 })
    expect(calls).toBe(2)
    expect(await c.get('k')).toBeUndefined()
  })

  it('expired keys miss on get', async () => {
    const c = make()
    await c.set('k', 'v', { ttl: 80 })
    await sleep(120)
    expect(await c.get('k')).toBeUndefined()
  })

  it('falsy values (0, "", false) are cached correctly', async () => {
    const c = make()
    let calls = 0
    const zero = await c.fetch('zero', async () => { calls++; return 0 })
    const zero2 = await c.fetch('zero', async () => { calls++; return 99 })
    expect(zero).toBe(0)
    expect(zero2).toBe(0)

    const empty = await c.fetch('empty', async () => '')
    expect(empty).toBe('')
    expect(await c.get('empty')).toBe('')

    const f = await c.fetch('flag', async () => false)
    expect(f).toBe(false)
    expect(await c.get('flag')).toBe(false)

    expect(calls).toBe(1)
  })

  it('explicit ready() is safe to call before and after first op', async () => {
    const c = make()
    await c.ready()
    await c.ready()
    await c.set('k', 1)
    expect(await c.get('k')).toBe(1)
  })

  it('operations after close throw', async () => {
    const c = make()
    await c.set('k', 1)
    await c.close()
    await expect(c.fetch('k', async () => 2)).rejects.toThrow()
    caches.pop()
  })
})

describe('stampede prevention', () => {
  it('a single loader runs for concurrent fetchers on the same cache', async () => {
    const c = make()
    let calls = 0
    const loader = async () => { calls++; await sleep(150); return 'result' }
    const results = await Promise.all([
      c.fetch('hot', loader),
      c.fetch('hot', loader),
      c.fetch('hot', loader),
      c.fetch('hot', loader),
    ])
    expect(results.every((r) => r === 'result')).toBe(true)
    expect(calls).toBe(1)
  })

  it('a single loader runs across multiple cache instances racing the same key', async () => {
    const a = make()
    const b = make()
    const counters = { a: 0, b: 0 }
    const ldr = (which) => async () => { counters[which]++; await sleep(200); return `from-${which}` }
    const [ra, rb] = await Promise.all([
      a.fetch('shared', ldr('a')),
      b.fetch('shared', ldr('b')),
    ])
    expect(ra).toBe(rb)
    expect(counters.a + counters.b).toBe(1)
  })

  it('stats reflect exactly one lead and one wait when two instances race', async () => {
    const a = make()
    const b = make()
    await Promise.all([
      a.fetch('k', async () => { await sleep(80); return 1 }),
      b.fetch('k', async () => { await sleep(80); return 2 }),
    ])
    const leads = a.stats().stampedeLeads + b.stats().stampedeLeads
    const waits = a.stats().stampedeWaits + b.stats().stampedeWaits
    const savings = a.stats().stampedeSavings + b.stats().stampedeSavings
    expect(leads).toBe(1)
    expect(waits).toBe(1)
    expect(savings).toBe(1)
  })

  it('a thrown loader error propagates to waiting instances', async () => {
    const a = make()
    const b = make()
    let calls = 0
    const fail = async () => {
      calls++
      await sleep(80)
      throw new Error('downstream-out')
    }
    const results = await Promise.allSettled([
      a.fetch('k', fail),
      b.fetch('k', fail),
    ])
    expect(calls).toBe(1)
    expect(results.every((r) => r.status === 'rejected')).toBe(true)
    expect(results.every((r) => r.reason.message === 'downstream-out')).toBe(true)
  })

  it('multiple waiters on the same instance all receive the leader result', async () => {
    const c = make()
    let calls = 0
    const loader = async () => { calls++; await sleep(100); return 'shared-result' }
    const results = await Promise.all(
      Array.from({ length: 8 }, () => c.fetch('k', loader))
    )
    expect(results.every((r) => r === 'shared-result')).toBe(true)
    expect(calls).toBe(1)
    expect(c.stats().stampedeLeads).toBe(1)
    expect(c.stats().stampedeWaits).toBe(7)
    expect(c.stats().stampedeSavings).toBe(7)
  })

  it('after a stampede completes the cache can be used again normally', async () => {
    const c = make()
    let calls = 0
    const loader = async () => { calls++; await sleep(50); return calls }
    await Promise.all([
      c.fetch('k', loader),
      c.fetch('k', loader),
      c.fetch('k', loader),
    ])
    await c.del('k')
    const fresh = await c.fetch('k', loader)
    expect(fresh).toBe(2)
    expect(calls).toBe(2)
  })

  it('five instances racing produce exactly one lead and four waits', async () => {
    const instances = Array.from({ length: 5 }, () => make())
    let calls = 0
    const loader = async () => { calls++; await sleep(120); return 'one' }
    const results = await Promise.all(instances.map((c) => c.fetch('k', loader)))
    expect(results.every((r) => r === 'one')).toBe(true)
    expect(calls).toBe(1)
    const totalLeads = instances.reduce((sum, c) => sum + c.stats().stampedeLeads, 0)
    const totalWaits = instances.reduce((sum, c) => sum + c.stats().stampedeWaits, 0)
    expect(totalLeads).toBe(1)
    expect(totalWaits).toBe(4)
  })

  it('a waiter that times out falls back to running its own loader once', async () => {
    const a = make({ defaultLockTtl: '5s' })
    const b = make({ waitTimeout: 60 })

    let leaderCalls = 0
    let waiterCalls = 0

    const leader = a.fetch('k', async () => { leaderCalls++; await sleep(400); return 'leader' })
    await sleep(20)
    const waiter = b.fetch('k', async () => { waiterCalls++; return 'fallback' })

    const w = await waiter
    expect(w).toBe('fallback')
    expect(waiterCalls).toBe(1)

    const l = await leader
    expect(l).toBe('leader')
    expect(leaderCalls).toBe(1)
  })
})

describe('stale-while-revalidate', () => {
  it('serves stale immediately and refreshes in the background', async () => {
    const c = make()
    let calls = 0
    const loader = async () => { calls++; return calls }
    await c.fetch('k', loader, { ttl: 80, staleWhile: 5_000 })
    await sleep(120)
    const stale = await c.fetch('k', loader, { ttl: 80, staleWhile: 5_000 })
    expect(stale).toBe(1)
    await sleep(150)
    const fresh = await c.fetch('k', loader, { ttl: 80, staleWhile: 5_000 })
    expect(fresh).toBe(2)
  })

  it('past the stale window a real refetch happens', async () => {
    const c = make()
    let calls = 0
    await c.fetch('k', async () => { calls++; return calls }, { ttl: 60, staleWhile: 100 })
    await sleep(250)
    const v = await c.fetch('k', async () => { calls++; return calls }, { ttl: 60, staleWhile: 100 })
    expect(v).toBe(2)
    expect(calls).toBe(2)
  })

  it('emits a refresh event on background revalidation', async () => {
    const c = make()
    let refreshes = 0
    c.on('refresh', () => refreshes++)
    let calls = 0
    const loader = async () => { calls++; return calls }
    await c.fetch('k', loader, { ttl: 60, staleWhile: 5_000 })
    await sleep(120)
    await c.fetch('k', loader, { ttl: 60, staleWhile: 5_000 })
    await sleep(50)
    expect(refreshes).toBe(1)
  })

  it('concurrent stale reads coalesce into a single background refresh', async () => {
    const c = make()
    let calls = 0
    const loader = async () => { calls++; await sleep(100); return calls }
    await c.fetch('k', loader, { ttl: 60, staleWhile: 5_000 })
    expect(calls).toBe(1)
    await sleep(100)
    await Promise.all([
      c.fetch('k', loader, { ttl: 60, staleWhile: 5_000 }),
      c.fetch('k', loader, { ttl: 60, staleWhile: 5_000 }),
      c.fetch('k', loader, { ttl: 60, staleWhile: 5_000 }),
    ])
    await sleep(200)
    expect(calls).toBe(2)
  })
})

describe('negative and error caching', () => {
  it('caches null under negativeTtl and returns null on subsequent fetches', async () => {
    const c = make()
    let calls = 0
    const v = await c.fetch('absent', async () => { calls++; return null }, { ttl: '10s', negativeTtl: '1s' })
    expect(v).toBeNull()
    const v2 = await c.fetch('absent', async () => { calls++; return 'late-arrival' }, { ttl: '10s', negativeTtl: '1s' })
    expect(v2).toBeNull()
    expect(calls).toBe(1)
  })

  it('null without negativeTtl is not cached', async () => {
    const c = make()
    let calls = 0
    await c.fetch('k', async () => { calls++; return null })
    await c.fetch('k', async () => { calls++; return null })
    expect(calls).toBe(2)
  })

  it('undefined is never cached even with negativeTtl', async () => {
    const c = make()
    let calls = 0
    await c.fetch('k', async () => { calls++; return undefined }, { negativeTtl: '1s' })
    await c.fetch('k', async () => { calls++; return undefined }, { negativeTtl: '1s' })
    expect(calls).toBe(2)
  })

  it('a non-null loader result does not trigger negativeTtl', async () => {
    const c = make()
    let calls = 0
    await c.fetch('k', async () => { calls++; return 'real' }, { ttl: '10s', negativeTtl: '50ms' })
    await sleep(120)
    const v = await c.fetch('k', async () => { calls++; return 'should not run' }, { ttl: '10s', negativeTtl: '50ms' })
    expect(v).toBe('real')
    expect(calls).toBe(1)
  })

  it('loader errors propagate without errorTtl', async () => {
    const c = make()
    let calls = 0
    const fail = async () => { calls++; throw new Error('boom') }
    await expect(c.fetch('k', fail)).rejects.toThrow('boom')
    await expect(c.fetch('k', fail)).rejects.toThrow('boom')
    expect(calls).toBe(2)
  })

  it('errorTtl caches the thrown error so the loader is not re-run', async () => {
    const c = make()
    let calls = 0
    const fail = async () => { calls++; throw new Error('downstream-out') }
    await expect(c.fetch('k', fail, { errorTtl: '500ms' })).rejects.toThrow('downstream-out')
    await expect(c.fetch('k', fail, { errorTtl: '500ms' })).rejects.toThrow('downstream-out')
    expect(calls).toBe(1)
  })

  it('cached error expires and the loader runs again', async () => {
    const c = make()
    let calls = 0
    const flake = async () => {
      calls++
      if (calls === 1) throw new Error('first-fail')
      return 'recovered'
    }
    await expect(c.fetch('k', flake, { errorTtl: 60 })).rejects.toThrow('first-fail')
    await sleep(100)
    const v = await c.fetch('k', flake, { errorTtl: 60 })
    expect(v).toBe('recovered')
    expect(calls).toBe(2)
  })

  it('get returns undefined while an error is cached, not null', async () => {
    const c = make()
    await c.fetch('k', async () => { throw new Error('x') }, { errorTtl: '500ms' }).catch(() => {})
    expect(await c.get('k')).toBeUndefined()
    expect(await c.has('k')).toBe(false)
  })
})

describe('tag invalidation', () => {
  it('invalidateTag wipes every key carrying that tag', async () => {
    const c = make()
    await c.set('user:1', { name: 'a' }, { ttl: '1h', tags: ['user', 'user:1'] })
    await c.set('user:2', { name: 'b' }, { ttl: '1h', tags: ['user', 'user:2'] })
    await c.set('order:9', { id: 9 }, { ttl: '1h', tags: ['order'] })

    const count = await c.invalidateTag('user')
    expect(count).toBe(2)
    expect(await c.get('user:1')).toBeUndefined()
    expect(await c.get('user:2')).toBeUndefined()
    expect(await c.get('order:9')).toEqual({ id: 9 })
  })

  it('fetch with tags causes invalidateTag to wipe the cached value', async () => {
    const c = make()
    let calls = 0
    const loader = async () => { calls++; return 'value' }
    await c.fetch('k', loader, { ttl: '1h', tags: ['t1'] })
    await c.invalidateTag('t1')
    await c.fetch('k', loader, { ttl: '1h', tags: ['t1'] })
    expect(calls).toBe(2)
  })

  it('overwriting a key with different tags cleans the previous tag membership', async () => {
    const c = make()
    await c.set('k', 1, { ttl: '1h', tags: ['a'] })
    await c.set('k', 2, { ttl: '1h', tags: ['b'] })
    expect(await c.invalidateTag('a')).toBe(0)
    expect(await c.get('k')).toBe(2)
    expect(await c.invalidateTag('b')).toBe(1)
    expect(await c.get('k')).toBeUndefined()
  })

  it('del removes the key from all of its tag sets', async () => {
    const c = make()
    await c.set('k', 1, { ttl: '1h', tags: ['a', 'b'] })
    await c.del('k')
    expect(await c.invalidateTag('a')).toBe(0)
    expect(await c.invalidateTag('b')).toBe(0)
  })

  it('invalidateTag on an unknown tag returns 0 and does not throw', async () => {
    const c = make()
    expect(await c.invalidateTag('never-seen')).toBe(0)
  })

  it('emits an invalidate event with the affected count', async () => {
    const c = make()
    await c.set('k1', 1, { ttl: '1h', tags: ['t'] })
    await c.set('k2', 2, { ttl: '1h', tags: ['t'] })
    let captured = null
    c.on('invalidate', (e) => { captured = e })
    await c.invalidateTag('t')
    expect(captured).toEqual({ tag: 't', count: 2 })
  })
})

describe('events and stats', () => {
  it('emits hit / miss / set / del across normal usage', async () => {
    const c = make()
    const events = []
    c.on('hit', (e) => events.push(['hit', e.key, e.fresh]))
    c.on('miss', (e) => events.push(['miss', e.key]))
    c.on('set', (e) => events.push(['set', e.key]))
    c.on('del', (e) => events.push(['del', e.key]))

    await c.fetch('k', async () => 1)
    await c.fetch('k', async () => 2)
    await c.del('k')

    const kinds = events.map((e) => e[0])
    expect(kinds).toContain('miss')
    expect(kinds).toContain('set')
    expect(kinds).toContain('hit')
    expect(kinds).toContain('del')
  })

  it('emits stampede:lead on exactly the leader and stampede:wait on exactly the waiters', async () => {
    const a = make()
    const b = make()
    const seen = []
    a.on('stampede:lead', () => seen.push('lead-a'))
    b.on('stampede:lead', () => seen.push('lead-b'))
    a.on('stampede:wait', () => seen.push('wait-a'))
    b.on('stampede:wait', () => seen.push('wait-b'))
    await Promise.all([
      a.fetch('k', async () => { await sleep(80); return 1 }),
      b.fetch('k', async () => { await sleep(80); return 2 }),
    ])
    expect(seen.filter((e) => e.startsWith('lead-')).length).toBe(1)
    expect(seen.filter((e) => e.startsWith('wait-')).length).toBe(1)
  })

  it('off() removes a previously registered listener', async () => {
    const c = make()
    let hits = 0
    const handler = () => hits++
    c.on('hit', handler)
    await c.set('k', 1, { ttl: '5s' })
    await c.get('k')
    expect(hits).toBe(1)
    c.off('hit', handler)
    await c.get('k')
    expect(hits).toBe(1)
  })

  it('stats() returns the current counter snapshot and is not live-bound', async () => {
    const c = make()
    const before = c.stats()
    expect(before).toMatchObject({
      hits: 0,
      misses: 0,
      sets: 0,
      dels: 0,
      stampedeLeads: 0,
      stampedeWaits: 0,
    })
    await c.fetch('k', async () => 1)
    await c.fetch('k', async () => 1)
    const after = c.stats()
    expect(before.hits).toBe(0)
    expect(after.hits).toBe(1)
    expect(after.misses).toBe(1)
    expect(after.sets).toBe(1)
  })
})

describe('configuration', () => {
  it('respects prefix isolation - caches with different prefixes do not see each other', async () => {
    const a = make({ prefix: 'pfx-a:' })
    const b = make({ prefix: 'pfx-b:' })
    await a.set('shared-key', 'A', { ttl: '5s' })
    await b.set('shared-key', 'B', { ttl: '5s' })
    expect(await a.get('shared-key')).toBe('A')
    expect(await b.get('shared-key')).toBe('B')
  })

  it('accepts a custom serializer / deserializer', async () => {
    const c = make({
      prefix: 'custom:',
      serialize: (v) => Buffer.from(JSON.stringify(v)).toString('base64'),
      deserialize: (s) => JSON.parse(Buffer.from(s, 'base64').toString('utf8')),
    })
    await c.set('k', { hello: 'world' }, { ttl: '5s' })
    expect(await c.get('k')).toEqual({ hello: 'world' })
  })

  it('accepts duration strings or raw ms numbers for ttl', async () => {
    const c = make()
    await c.set('a', 1, { ttl: 5_000 })
    await c.set('b', 2, { ttl: '5s' })
    expect(await c.get('a')).toBe(1)
    expect(await c.get('b')).toBe(2)
  })
})
