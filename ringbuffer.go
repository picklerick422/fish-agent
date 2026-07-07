package main

import "sync"

const defaultRingBufferSize = 8 * 1024 * 1024 // 8MB

type RingBuffer struct {
	buf    []byte
	size   int
	head   int
	filled bool
	mu     sync.Mutex
}

func NewRingBuffer(size int) *RingBuffer {
	if size <= 0 {
		size = defaultRingBufferSize
	}
	return &RingBuffer{
		buf:  make([]byte, size),
		size: size,
	}
}

func (rb *RingBuffer) Write(data []byte) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for _, b := range data {
		rb.buf[rb.head] = b
		rb.head = (rb.head + 1) % rb.size
		if rb.head == 0 {
			rb.filled = true
		}
	}
}

func (rb *RingBuffer) Snapshot() []byte {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if !rb.filled {
		result := make([]byte, rb.head)
		copy(result, rb.buf[:rb.head])
		return result
	}
	result := make([]byte, rb.size)
	n := copy(result, rb.buf[rb.head:])
	copy(result[n:], rb.buf[:rb.head])
	return result
}
