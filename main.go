package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrCompacted is returned when the requested revision has been compacted.
var ErrCompacted = errors.New("etcdserver: mvcc: required revision has been compacted")

// Event represents a key-value mutation event.
type Event struct {
	Revision int64
	Key      string
	Value    string
}

// Server simulates an etcd cluster/server.
type Server struct {
	mu                sync.Mutex
	events            []Event
	currentRevision   int64
	compactedRevision int64
	leader            string
	activeStreams     map[int64]chan Event
	nextStreamID      int64
}

func NewServer() *Server {
	return &Server{
		events:        make([]Event, 0),
		activeStreams: make(map[int64]chan Event),
		leader:        "node-1",
	}
}

// Put simulates writing a key-value pair.
func (s *Server) Put(key, value string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentRevision++
	event := Event{
		Revision: s.currentRevision,
		Key:      key,
		Value:    value,
	}
	s.events = append(s.events, event)

	// Dispatch to active streams
	for _, ch := range s.activeStreams {
		select {
		case ch <- event:
		default:
			// Non-blocking send to avoid deadlock in simulation
		}
	}
	return s.currentRevision
}

// Compact simulates compacting the history up to a revision.
func (s *Server) Compact(rev int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rev > s.compactedRevision {
		s.compactedRevision = rev
		fmt.Printf("[Server] Compacted history up to revision %d\n", rev)
	}
}

// Watch establishes a watch stream starting from startRevision.
func (s *Server) Watch(startRevision int64) (<-chan Event, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if startRevision <= s.compactedRevision {
		return nil, 0, ErrCompacted
	}

	ch := make(chan Event, 100)
	streamID := s.nextStreamID
	s.nextStreamID++
	s.activeStreams[streamID] = ch

	// Replay historical events from startRevision
	go func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, ev := range s.events {
			if ev.Revision >= startRevision {
				ch <- ev
			}
		}
	}()

	return ch, streamID, nil
}

func (s *Server) CloseStream(streamID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.activeStreams[streamID]; ok {
		close(ch)
		delete(s.activeStreams, streamID)
	}
}

// Client represents the watch client.
type Client struct {
	server               *Server
	lastReceivedRevision int64
	mu                   sync.Mutex
}

func NewClient(server *Server) *Client {
	return &Client{
		server: server,
	}
}

// WatchStream manages the reconnection and event processing.
func (c *Client) WatchStream(keyPrefix string, eventHandler func(Event)) {
	for {
		c.mu.Lock()
		startRev := c.lastReceivedRevision + 1
		c.mu.Unlock()

		fmt.Printf("[Client] Attempting to watch from revision %d...\n", startRev)
		ch, streamID, err := c.server.Watch(startRev)
		if err != nil {
			if errors.Is(err, ErrCompacted) {
				fmt.Printf("[Client] Error: Revision %d compacted. Triggering full sync...\n", startRev)
				// Handle compaction gracefully: sync to current revision
				c.mu.Lock()
				c.server.mu.Lock()
				c.lastReceivedRevision = c.server.currentRevision
				c.server.mu.Unlock()
				c.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}
			fmt.Printf("[Client] Connection error: %v. Retrying...\n", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		fmt.Printf("[Client] Watch stream established (ID: %d)\n", streamID)

		// Read events from stream
		streamActive := true
		for streamActive {
			select {
			case ev, ok := <-ch:
				if !ok {
					fmt.Printf("[Client] Stream %d closed by server (e.g., leader election/disconnect)\n", streamID)
					streamActive = false
					break
				}
				c.mu.Lock()
				if ev.Revision > c.lastReceivedRevision {
					c.lastReceivedRevision = ev.Revision
					eventHandler(ev)
				} else {
					fmt.Printf("[Client] Warning: Received duplicate event at revision %d (ignored)\n", ev.Revision)
				}
				c.mu.Unlock()
			}
		}

		c.server.CloseStream(streamID)
		time.Sleep(100 * time.Millisecond) // Backoff before reconnecting
	}
}

func main() {
	fmt.Println("Starting Watch Stream Leader Election Simulation...")
	server := NewServer()
	client := NewClient(server)

	// Write initial events
	server.Put("/foo/1", "val1")
	server.Put("/foo/2", "val2")

	var receivedEvents []Event
	var mu sync.Mutex
	handleEvent := func(ev Event) {
		mu.Lock()
		receivedEvents = append(receivedEvents, ev)
		fmt.Printf("[Client] Received Event: Rev %d, Key %s, Val %s\n", ev.Revision, ev.Key, ev.Value)
		mu.Unlock()
	}

	// Start client watch in a goroutine
	go client.WatchStream("/foo/", handleEvent)

	// Allow client to receive initial events
	time.Sleep(200 * time.Millisecond)

	// Simulate Leader Election / Connection Loss
	fmt.Println("\n--- Simulating Leader Election & Connection Loss ---")
	// Disconnect all active streams to simulate connection drop
	server.mu.Lock()
	for id, ch := range server.activeStreams {
		close(ch)
		delete(server.activeStreams, id)
	}
	server.leader = "node-2" // New leader elected
	server.mu.Unlock()

	// Write events during the election window (while client is disconnected)
	server.Put("/foo/3", "val3")
	server.Put("/foo/4", "val4")

	// Allow client to reconnect and catch up
	time.Sleep(500 * time.Millisecond)

	// Write more events after reconnection
	server.Put("/foo/5", "val5")
	time.Sleep(200 * time.Millisecond)

	// Verify all events received sequentially
	mu.Lock()
	fmt.Printf("\nTotal events received: %d\n", len(receivedEvents))
	for _, ev := range receivedEvents {
		fmt.Printf("  Rev %d: %s = %s\n", ev.Revision, ev.Key, ev.Value)
	}
	mu.Unlock()

	// Simulate Compaction Scenario
	fmt.Println("\n--- Simulating Compaction Scenario ---")
	// Disconnect client again
	server.mu.Lock()
	for id, ch := range server.activeStreams {
		close(ch)
		delete(server.activeStreams, id)
	}
	server.mu.Unlock()

	// Compact history up to revision 4
	server.Compact(4)

	// Write new event
	server.Put("/foo/6", "val6")

	// Allow client to reconnect, hit compaction error, perform full sync, and resume
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	fmt.Printf("\nFinal events list:\n")
	for _, ev := range receivedEvents {
		fmt.Printf("  Rev %d: %s = %s\n", ev.Revision, ev.Key, ev.Value)
	}
	mu.Unlock()
}