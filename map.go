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

type RWMap struct {
	bigLock    sync.RWMutex
	bigMap     map[any]*mapEntry
	littleLock sync.RWMutex // must hold big lock first
	littleMap  map[any]*mapEntry
}

func (m *RWMap) merge() {
	if m.littleMap == nil {
		return
	}

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
	m.littleMap = nil
}

func (m *RWMap) forceMerge() {
	m.bigLock.Lock()
	defer m.bigLock.Unlock()
	m.merge()
}

func (m *RWMap) checkMerge() {
	// if number of misses > num writes, then every entry has been read at least once, on average
	// if number of writes > max batch size
	// try to obtain the lock, and merge
}

func (m *RWMap) tryLock() bool {
	if m.bigLock.TryLock() {
		m.merge()
		return true
	}
	return false
}

func (m *RWMap) scoreMiss() {

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

	m.scoreMiss()
	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap == nil {
		return nil, false
	}

	v, ok := m.littleMap[key]
	if ok && v != nil {
		value := v.Load()
		if value != nil {
			return value, true
		}
	}

	return nil, false
}

func (m *RWMap) Store(key, value any) {
	if m.tryLock() {
		defer m.bigLock.Unlock()
		if m.bigMap == nil {
			m.bigMap = make(map[any]*mapEntry, 8)
		}

		v, ok := m.bigMap[key]
		if ok && v != nil && v.Load() != nil {
			v.Store(value)
		} else {
			v = new(mapEntry)
			v.Store(value)
			m.bigMap[key] = v
		}

		return
	}

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

	m.scoreMiss()
	m.littleLock.Lock()
	defer m.littleLock.Unlock()

	if m.littleMap == nil {
		m.littleMap = make(map[any]*mapEntry, 8)
	} else {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			if v.Load() != nil {
				v.Store(value)
				return
			}
		}
	}

	v := new(mapEntry)
	v.Store(value)
	m.littleMap[key] = v
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
				m.littleLock.Unlock()
			}
			return
		}
	}

	m.scoreMiss()
	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
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
					m.littleLock.Unlock()
				}
				return old, true
			}
		}
	}

	m.scoreMiss()
	m.littleLock.Lock()
	defer m.littleLock.Unlock()

	if m.littleMap == nil {
		m.littleMap = make(map[any]*mapEntry, 8)
	} else {
		v, ok := m.littleMap[key]
		if ok && v != nil {
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
					m.littleLock.Unlock()
					return true
				}
				return false
			}
		}
	}
	m.scoreMiss()
	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
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
	m.scoreMiss()
	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
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
				m.littleLock.Unlock()
				return value, true
			}
		}
	}

	m.scoreMiss()
	m.littleLock.Lock()
	defer m.littleLock.Unlock()

	if m.littleMap != nil {
		v, ok := m.littleMap[key]
		if ok && v != nil {
			value = v.Load()
			v.Delete()
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

	m.scoreMiss()
	m.littleLock.RLock()
	defer m.littleLock.RUnlock()

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

	m.scoreMiss()
	m.littleLock.RLock()
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

	m.scoreMiss()
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
