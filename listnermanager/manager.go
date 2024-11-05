package listnermanager

import (
	"fmt"
	c "goBroadcaster/constants"
	"log"
	"sync"
	"time"
)

type ListenerManager struct {
	listeners []chan c.Message
	mu        sync.Mutex
}

func (lm *ListenerManager) AddListener() chan c.Message {
	time.Sleep(1 * time.Millisecond)
	lm.mu.Lock()
	defer lm.mu.Unlock()
	ch := make(chan c.Message)
	lm.listeners = append(lm.listeners, ch)
	return ch
}

func (lm *ListenerManager) Broadcast(msg c.Message) {
	time.Sleep(1 * time.Millisecond)
	log.Println("Entered Broadcast")
	lm.mu.Lock()

	defer func() {
		fmt.Println("Broadcaster Done")
	}()
	defer lm.mu.Unlock()
	if len(lm.listeners) > 0 {
		log.Println("Publishing")
		for _, listener := range lm.listeners {
			fmt.Println(listener)
			listener <- msg
		}
	} else {
		fmt.Println("No active listners to publish message")
	}
}

func (lm *ListenerManager) RemoveListener(ch chan c.Message) {
	log.Println("Entered to remove")
	lm.mu.Lock()
	defer lm.mu.Unlock()
	defer func() {
		fmt.Println("Done")
	}()

	for i, listener := range lm.listeners {
		if listener == ch {
			lm.listeners = append(lm.listeners[:i], lm.listeners[i+1:]...)
			close(ch)
			break
		}
	}
}
