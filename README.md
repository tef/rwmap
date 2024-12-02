# RWMap, a sync.Map equivalent 

`sync.Map` uses a read-only copy and a locked dirty map to implement a concurrent map type in go.
`RWMap` uses a `RWMutex` instead, but also uses a dirty map to batch up changes.

## FAQ

- Why would you use a map like this over `sync.Map`

  - `sync.Map` has non blocking readers, but the amortised cost of writes increases with map size
  - a map like this has a fixed overhead, and every so often, reads will be blocked
  - for something with more mixed read/write loads, it might work out better. who knows

- Why would you use this library itself?
  
  - Good luck.

