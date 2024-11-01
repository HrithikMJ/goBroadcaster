package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type ListenerManager struct {
	listeners []chan string
	mu        sync.Mutex
}

func (lm *ListenerManager) AddListener() chan string {
	time.Sleep(1 * time.Millisecond)
	lm.mu.Lock()
	defer lm.mu.Unlock()
	ch := make(chan string)
	lm.listeners = append(lm.listeners, ch)
	return ch
}

func (lm *ListenerManager) Broadcast(msg string) {
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

func (lm *ListenerManager) RemoveListener(ch chan string) {
	log.Println("Entered to remove")
	lm.mu.Lock()
	defer lm.mu.Unlock()
	defer func() {
		fmt.Println("Done")
	}()

	for i, listener := range lm.listeners {
		if listener == ch {
			log.Println(listener, ch, i)
			lm.listeners = append(lm.listeners[:i], lm.listeners[i+1:]...)
			close(ch)
			break
		}
	}
}

func consumer(lm *ListenerManager, timeout int, id int, ch chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	defer lm.RemoveListener(ch)

	ticker := time.NewTicker(time.Duration(timeout) * time.Second)
	for {
		select {
		case <-ticker.C:
			log.Printf("Consumer %d timedout \n", id)
			return
		case msg := <-ch:
			log.Printf("Consumer %d received message: %s\n", id, msg)
			go processMessage(id)
		}
	}
}

func processMessage(id int) {
	time.Sleep(1 * time.Second)
}

func main() {
	var wg sync.WaitGroup

	lm := &ListenerManager{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 1; i <= 10; i++ {
			message := fmt.Sprintf("%d", i)
			log.Printf("Broadcasting: %s\n", message)
			lm.Broadcast(message)
			time.Sleep(1 * time.Second)
		}
		fmt.Println("broadCaster Done")
	}()

	go func() {
		defer wg.Done()

		for i := 1; i <= 3; i++ {

			ch := lm.AddListener()
			wg.Add(1)
			go consumer(lm, 4, i, ch, &wg)
			// time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		defer wg.Done()

		time.Sleep(1 * time.Second)
		ch := lm.AddListener()
		wg.Add(1)
		go consumer(lm, 4, 4, ch, &wg)
		log.Println("Consumer 4 added.")

	}()

	wg.Wait()
	fmt.Println("All consumers finished.")
}
