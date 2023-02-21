package collections

import "sync"

type Queue struct {
	lock sync.RWMutex

	first *node
	last  *node
	size  int
}

type node struct {
	value interface{}
	next  *node
}

func NewQueue(values ...interface{}) *Queue {
	q := &Queue{}
	if len(values) > 0 {
		q.Offer(values...)
	}
	return q
}

// 将元素插入到队列末尾
func (q *Queue) Offer(values ...interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()

	for _, value := range values {
		element := &node{value: value}
		if q.size == 0 {
			q.first = element
			q.last = element
		} else {
			q.last.next = element
			q.last = element
		}
		q.size++
	}
}

// 获取队首元素，若成功，则返回队首元素；否则返回null
func (q *Queue) Peek() (interface{}, bool) {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if q.size == 0 {
		return nil, false
	}
	return q.first.value, true
}

// 移除并获取队首元素，若成功，则返回队首元素；否则返回null
func (q *Queue) Poll() (interface{}, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.size == 0 {
		return nil, false
	}

	if q.size == 1 {
		val := q.first.value
		q.first = nil
		q.last = nil
		q.size = 0
		return val, true
	}

	element := q.first
	q.first = q.first.next
	val := element.value
	element = nil
	q.size--
	return val, true
}

func (q *Queue) Size() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.size
}

func (q *Queue) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.size = 0
	q.first = nil
	q.last = nil
}
