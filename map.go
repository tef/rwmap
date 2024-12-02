package rwmap

import (
	"sync"
	"sync/atomic"
)

type mapEntry struct {
	v atomic.Value
}

func (b *mapEntry) Load() any {
	return b.v.Load()
}

func (b *mapEntry) Store(o any) {
	b.v.Store(o)
}

func (b *mapEntry) CompareAndSwap(old any, new any) bool {
	return b.v.CompareAndSwap(old, new)
}

func (b *mapEntry) Delete() {
	b.v.Store(nil)
}

// We keep two maps, a big map, and a little map. Both under RWMutexes,
// and we always take a big lock before taking a little lock, then release both

// Reads go through the big map, and occasionally through the little map if there's a miss
// Writes go through the little map, and get batched up into the big map
// Deletes nil out the entry, and the entry is added to the little map,
// so that the entry can be removed from the big map, later

// only creating a new key, or deleting a key, requires a write lock
// on the little lock, and a big write lock precludes any lock on
// the little map, allowing access to both big and little maps

type RWMap struct {
	bigLock    sync.RWMutex
	bigMap     map[any]*mapEntry
	littleLock sync.RWMutex // must hold big lock first
	littleMap  map[any]*mapEntry

	littleReads atomic.Uintptr
	shouldMerge atomic.Bool
}

func (m *RWMap) merge() {
	// big write lock
	if len(m.littleMap) > 0 {
		if m.bigMap == nil {
			m.bigMap = make(map[any]*mapEntry, len(m.littleMap))
		}

		for k, v := range m.littleMap {
			if v == nil || v.Load() == nil {
				delete(m.bigMap, k)
			} else {
				m.bigMap[k] = v
			}
		}
	}
	m.littleMap = nil
	m.littleReads.Store(0)
	m.shouldMerge.Store(false)
}

func (m *RWMap) forceMerge() {
	// no lock
	m.bigLock.Lock()
	defer m.bigLock.Unlock()
	m.merge()
}

func (m *RWMap) checkMerge() {
	// no lock
	if m.shouldMerge.Load() {
		if m.bigLock.TryLock() {
			defer m.bigLock.Unlock()
			m.merge()
		}
	}
}

func (m *RWMap) scoreMiss() {
	// have little lock, read or write
	l := len(m.littleMap)
	if l > 0 {
		r := m.littleReads.Add(1)
		if l >= 64 || r >= 64 {
			m.shouldMerge.Store(true)
		}
	}
}

func (m *RWMap) Load(key any) (value any, ok bool) {
	m.checkMerge()

	m.bigLock.RLock()
	defer m.bigLock.RUnlock()

	if m.bigMap != nil {
		v, ok := m.bigMap[key]
		if ok && v != nil {
			value := v.Load()
			if value != nil {
				return value, true
			}
		}
	}

	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap == nil {
		return nil, false
	}

	v, ok := m.littleMap[key]
	if ok && v != nil {
		value := v.Load()
		if value != nil {
			m.scoreMiss()
			return value, true
		}
	}

	return nil, false
}

func (m *RWMap) Store(key, value any) {
	m.checkMerge()

	m.bigLock.RLock()
	defer m.bigLock.RUnlock()

	if m.bigMap != nil {
		v, ok := m.bigMap[key]
		if ok && v != nil {
			if v.Load() != nil {
				v.Store(value)
				return
			}
		}
	}

	m.littleLock.Lock()
	defer m.littleLock.Unlock()

	if m.littleMap == nil {
		m.littleMap = make(map[any]*mapEntry, 8)
	} else {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			if v.Load() != nil {
				v.Store(value)
				m.scoreMiss()
				return
			}
		}
	}

	v := new(mapEntry)
	v.Store(value)
	m.littleMap[key] = v
	m.scoreMiss()
}

func (m *RWMap) Delete(key any) {
	m.checkMerge()

	m.bigLock.RLock()
	defer m.bigLock.RUnlock()

	if m.bigMap != nil {
		v, ok := m.bigMap[key]
		if ok && v != nil {
			if v.Load() != nil {
				v.Store(nil)
				// it exists in big, so therefore it does not exist in little
				m.littleLock.Lock()
				m.littleMap[key] = v
				m.scoreMiss() // as it creates work to be done on big
				m.littleLock.Unlock()
			}
			return
		}
	}

	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			m.scoreMiss()
			if v.Load() != nil {
				v.Store(nil)
			}
			return
		}
	}
}

func (m *RWMap) Swap(key, value any) (previous any, loaded bool) {
	m.checkMerge()

	m.bigLock.RLock()
	defer m.bigLock.RUnlock()

	if m.bigMap != nil {
		v, ok := m.bigMap[key]
		if ok && v != nil {
			old := v.Load()
			if old != nil {
				v.Store(value)
				if value == nil {
					m.littleLock.Lock()
					m.littleMap[key] = v
					m.scoreMiss()
					m.littleLock.Unlock()
				}
				return old, true
			}
		}
	}

	m.littleLock.Lock()
	defer m.littleLock.Unlock()

	if m.littleMap == nil {
		m.littleMap = make(map[any]*mapEntry, 8)
	} else {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			m.scoreMiss()
			old := v.Load()
			if old != nil {
				v.Store(value)
				return old, true
			}
		}
	}

	if value != nil {
		v := new(mapEntry)
		v.Store(value)
		m.littleMap[key] = v // if old deleted entry in big, will get overwritten
		m.scoreMiss()
	}
	return value, false
}

func (m *RWMap) CompareAndDelete(key, old any) (deleted bool) {
	if old == nil {
		return false
	}

	m.checkMerge()

	m.bigLock.RLock()
	defer m.bigLock.RUnlock()

	if m.bigMap != nil {
		v, ok := m.bigMap[key]
		if ok && v != nil {
			value := v.Load()
			if value != nil && value == old {
				if v.CompareAndSwap(value, nil) {
					m.littleLock.Lock()
					m.littleMap[key] = v
					m.scoreMiss()
					m.littleLock.Unlock()
					return true
				}
				return false
			}
		}
	}

	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			m.scoreMiss()
			value := v.Load()
			if value == old {
				return v.CompareAndSwap(value, nil)
			}
		}
	}

	return false
}

func (m *RWMap) CompareAndSwap(key, old any, newv any) (swapped bool) {
	if old == nil || newv == nil {
		return false
	}

	m.checkMerge()
	m.bigLock.RLock()
	defer m.bigLock.RUnlock()

	if m.bigMap != nil {
		v, ok := m.bigMap[key]
		if ok && v != nil {
			value := v.Load()
			if value != nil && value == old {
				return v.CompareAndSwap(old, newv)
			}
		}
	}
	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			m.scoreMiss()
			value := v.Load()
			if value != nil && value == old {
				return v.CompareAndSwap(old, newv)
			}
		}
	}

	return false
}

func (m *RWMap) LoadAndDelete(key any) (value any, loaded bool) {
	m.checkMerge()

	m.bigLock.RLock()
	defer m.bigLock.RUnlock()

	if m.bigMap != nil {
		v, ok := m.bigMap[key]
		if ok && v != nil {
			value = v.Load()
			if value != nil {
				v.Delete()
				m.littleLock.Lock()
				m.littleMap[key] = v
				m.scoreMiss()
				m.littleLock.Unlock()
				return value, true
			}
		}
	}

	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			value = v.Load()
			v.Delete()
			m.scoreMiss()
			return value, true
		}
	}

	return nil, false

}

func (m *RWMap) LoadOrStore(key, value any) (actual any, loaded bool) {
	m.checkMerge()

	m.bigLock.RLock()
	defer m.bigLock.RUnlock()

	if m.bigMap != nil {
		v, ok := m.bigMap[key]
		if ok && v != nil {
			value = v.Load()
			if value != nil {
				return value, true
			}
		}
	}

	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	m.scoreMiss()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			value = v.Load()
			if value != nil {
				return value, true
			}
		}
	}
	v := new(mapEntry)
	v.Store(value)
	m.littleMap[key] = v
	return value, false
}

func (m *RWMap) Range(f func(key, value any) bool) {
	m.checkMerge()
	copy := make(map[any]any)

	m.bigLock.RLock()

	for k, v := range m.bigMap {
		var a any
		if v != nil {
			a = v.Load()
		}
		if a != nil {
			copy[k] = a
		}
	}

	m.littleLock.RLock()
	m.scoreMiss()

	for k, v := range m.littleMap {
		var a any
		if v != nil {
			a = v.Load()
		}
		if a != nil {
			copy[k] = a
		} else {
			delete(copy, k)
		}
	}

	m.littleLock.RUnlock()
	m.bigLock.RUnlock()

	for k, v := range copy {
		if !f(k, v) {
			break
		}
	}
}

func (m *RWMap) Clear() {
	m.bigLock.Lock()
	defer m.bigLock.Unlock()

	// big lock implies little lock

	m.bigMap = nil
	m.littleMap = nil
}
