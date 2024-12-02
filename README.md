# RWMap, a sync.Map equivalent 

`sync.Map` uses a read-only copy and a locked dirty map to implement a concurrent map type in go.
`RWMap` uses a `RWMutex` instead, but also uses a dirty map to batch up changes.
