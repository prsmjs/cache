<p align="center">
  <img src="logo.svg" width="80" height="80" alt="@prsm/cache">
</p>

<h1 align="center">@prsm/cache</h1>

<p align="center">
  <a href="https://github.com/prsmjs/cache/actions/workflows/test.yml"><img src="https://github.com/prsmjs/cache/actions/workflows/test.yml/badge.svg" alt="test"></a>
  <a href="https://www.npmjs.com/package/@prsm/cache"><img src="https://img.shields.io/npm/v/@prsm/cache.svg" alt="npm"></a>
</p>

Distributed read-through cache for Redis. Single-flight loader execution across instances, stale-while-revalidate, tag-based invalidation, negative and error caching.

## Install

```
npm install @prsm/cache
```

## The pattern

```js
import { createCache } from '@prsm/cache'

const cache = createCache({
  redis: { host: '127.0.0.1', port: 6379 },
  defaultTtl: '5m',
})

const user = await cache.fetch(`user:${id}`, async () => {
  return await db.users.findById(id)
}, { ttl: '5m' })
```

The loader is called only on a miss. When N instances race for the same missing key, only one of them runs the loader. The rest wait for the result and receive it from Redis pub/sub. Your database sees one query, not N.

## Why not just use Redis directly?

The pattern most teams write looks correct but causes outages under load:

```js
const cached = await redis.get(`user:${id}`)
if (cached) return JSON.parse(cached)
const user = await db.users.findById(id)
await redis.setex(`user:${id}`, 300, JSON.stringify(user))
return user
```

When a hot key expires and 800 concurrent requests arrive in the same millisecond, all 800 see a miss, all 800 call the database. The database melts. This is the cache stampede problem.

This package handles it by acquiring a per-key lock when a miss happens. The lock winner runs the loader, writes the value, and publishes the result. Everyone else waits on the pub/sub channel and receives the value when the leader finishes. One database query for N concurrent requests.

## Stale-while-revalidate

When `staleWhile` is set, a key past its TTL but still within the stale window is served immediately while a background refresh runs. Requests never block on cache regeneration.

```js
const trending = await cache.fetch('trending', loadTrending, {
  ttl: '1m',          // fresh window
  staleWhile: '10m',  // serve stale up to 10 minutes past TTL while refreshing
})
```

## Tag-based invalidation

Keys can carry tags. Invalidating a tag wipes every key that was set with it.

```js
await cache.set(`order:${id}`, order, {
  ttl: '1h',
  tags: [`user:${order.userId}`, `org:${order.orgId}`],
})

await cache.invalidateTag(`user:42`)  // removes every key tagged user:42
```

## Negative and error caching

```js
const profile = await cache.fetch(`profile:${id}`, loadProfile, {
  ttl: '10m',
  negativeTtl: '30s',  // cache nulls for 30s so missing-user lookups don't hit the DB forever
  errorTtl: '5s',      // briefly cache thrown errors so an outage doesn't cascade
})
```

`undefined` returns are never cached. `null` is cached using `negativeTtl` if it is set.

## API

### `createCache(options)`

```js
const cache = createCache({
  redis: { host, port, password, db, url },  // or a node-redis client
  prefix: 'cache:',
  defaultTtl: '5m',
  defaultStaleWhile: 0,
  defaultNegativeTtl: null,
  defaultErrorTtl: 0,
  defaultLockTtl: '30s',
  waitTimeout: '10s',
  serialize: JSON.stringify,
  deserialize: JSON.parse,
})
```

### `cache.fetch(key, loader, options?)`

Returns the cached value, or runs `loader` if missing. Options override defaults: `ttl`, `staleWhile`, `negativeTtl`, `errorTtl`, `lockTtl`, `waitTimeout`, `tags`.

### `cache.get(key)`

Returns the value if the key is fresh, otherwise `undefined`. Does not call any loader.

### `cache.set(key, value, options?)`

Writes a value directly. Options: `ttl`, `staleWhile`, `tags`.

### `cache.del(key)`

Removes a key and its tag membership. Returns `true` if a key was removed.

### `cache.has(key)`

Returns `true` if the key is fresh.

### `cache.invalidateTag(tag)`

Removes every key associated with the tag. Returns the number of keys removed.

### `cache.stats()`

Returns a snapshot of counters: `hits`, `misses`, `sets`, `dels`, `errors`, `refreshes`, `stampedeLeads`, `stampedeWaits`, `stampedeSavings`, `invalidations`.

### `cache.on(event, handler)`

Events: `hit`, `miss`, `set`, `del`, `invalidate`, `stampede:lead`, `stampede:wait`, `stampede:result`, `stampede:timeout`, `refresh`, `error`.

### `cache.close()`

Closes the underlying Redis connections.

## Dev

```
make up      # start Redis
make test    # run tests
make down    # stop Redis
```

Redis must be running on localhost:6379 for tests.
