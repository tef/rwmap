package rwmap

import (
	//"fmt"
	"testing"
)

// reminder:
// 	t.Log(...)   / t.Logf(...)    log message
// 	t.Error(...) / t.Errorf(...)  mark failure, continue
// 	t.Fatal(...) / t.Fatalf(...)  mark failure, exit

type concurrentMap interface {
	Clear()
	CompareAndDelete(key, old any) (deleted bool)
	CompareAndSwap(key, old, new any) (swapped bool)
	Delete(key any)
	Load(key any) (value any, ok bool)
	LoadAndDelete(key any) (value any, loaded bool)
	LoadOrStore(key, value any) (actual any, loaded bool)
	Range(f func(key, value any) bool)
	Store(key, value any)
	Swap(key, value any) (previous any, loaded bool)
}

func TestMap(t *testing.T) {

	m := &RWMap{}

	m.Store("foo", "bar")
	out, ok := m.Load("foo")
	if !ok {
		t.Error("missing value")
	}

	s, ok := out.(string)
	if !ok {
		t.Error("bad value")
	} else if s != "bar" {
		t.Error("wrong value")
	}
	t.Log(m.shouldMerge.Load(), len(m.littleMap))
	m.shouldMerge.Store(true)
	m.checkMerge()
	out, ok = m.Load("foo")
	if !ok {
		t.Error("missing value")
	}
	t.Log(m.shouldMerge.Load(), len(m.littleMap))

	s, ok = out.(string)
	if !ok {
		t.Error("bad value")
	} else if s != "bar" {
		t.Error("wrong value")
	}
	m.Store("foo", "bar1")
	//m.Store("foo2", "bar1")
	t.Log(m.shouldMerge.Load(), len(m.littleMap))
}

func BenchMap(b *testing.B) {
	// run setup and call b.ResetTimer()
	// or b.Run() /  b.RunParallel(func(pb *testing.PB) { ... })
	for range b.N {

	}
}
