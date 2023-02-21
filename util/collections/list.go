package collections

import "sync"

type List struct {
	lock sync.RWMutex //互斥锁

	elements []interface{}
	size     int
}

func NewList(values ...interface{}) *List {
	l := &List{}
	if len(values) > 0 {
		l.Add(values...)
	}
	return l
}

func (s *List) within(index int) bool {
	return index >= 0 && index < s.size
}

func (s *List) Add(values ...interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(values) > 0 {
		s.elements = append(s.elements, values)
		s.size = s.size + len(values)
	}
}

func (s *List) Get(index int) interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if !s.within(index) {
		return nil
	}

	return s.elements[index]
}

func (s *List) Remove(index int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.within(index) {
		return
	}

	s.elements = append(s.elements[:index], s.elements[index+1:]...)

	s.size--
}

func (s *List) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.size
}
