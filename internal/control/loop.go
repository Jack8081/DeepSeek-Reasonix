package control

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// loopMachine runs a prompt on a recurring interval. After each turn completes,
// it waits the configured interval and submits again. The first submission fires
// immediately. This matches Claude Code's /loop: each iteration is a full
// think→act→finish cycle, then wait, then repeat.
type loopMachine struct {
	mu sync.Mutex

	prompt   string
	interval time.Duration

	running bool
	ticks   int
	started time.Time
	lastRan time.Time
	nextRun time.Time

	stopCh chan struct{}
	doneCh chan struct{}
}

type LoopInfo struct {
	Running  bool   `json:"running"`
	Prompt   string `json:"prompt,omitempty"`
	Interval string `json:"interval,omitempty"`
	Ticks    int    `json:"ticks"`
	Started  string `json:"started,omitempty"`
	LastRan  string `json:"lastRan,omitempty"`
	NextRun  string `json:"nextRun,omitempty"`
}

func (l *loopMachine) startLoop(interval time.Duration, prompt string, submit func(string, string), isRunning func() bool) {
	l.mu.Lock()
	if l.running {
		// Signal the previous goroutine and move on. It exits at its next
		// stopCh check; we must not wait for it here — it may be blocked on
		// l.mu itself, or inside a synchronous submit for an entire turn.
		close(l.stopCh)
	}
	now := time.Now()
	l.prompt = prompt
	l.interval = interval
	l.running = true
	l.ticks = 0
	l.started = now
	l.lastRan = time.Time{}
	l.nextRun = now // fire immediately
	l.stopCh = make(chan struct{})
	l.doneCh = make(chan struct{})
	stopCh := l.stopCh
	doneCh := l.doneCh
	l.mu.Unlock()

	go func() {
		defer close(doneCh)
		next := now // fire immediately
		tick := 0
		for {
			if delay := time.Until(next); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-stopCh:
					timer.Stop()
					return
				case <-timer.C:
				}
			}

			// If a turn is already in flight (user-initiated, or a still-
			// running iteration of a replaced loop), wait it out rather than
			// colliding with it. Poll every 500ms so we don't busy-wait.
			if !l.waitTurnIdle(stopCh, isRunning) {
				return
			}

			tick++
			l.publish(stopCh, func() {
				l.nextRun = time.Now().Add(interval)
			})
			submit(prompt, fmt.Sprintf("[loop #%d] %s", tick, prompt))

			// Wait for the turn to finish before starting the interval
			// countdown (covers async submit implementations).
			if !l.waitTurnIdle(stopCh, isRunning) {
				return
			}

			next = time.Now().Add(interval)
			l.publish(stopCh, func() {
				l.ticks = tick
				l.lastRan = time.Now()
				l.nextRun = next
			})
		}
	}()
}

// waitTurnIdle polls isRunning until the current turn finishes. It returns
// false if the loop was stopped while waiting.
func (l *loopMachine) waitTurnIdle(stopCh chan struct{}, isRunning func() bool) bool {
	for {
		select {
		case <-stopCh:
			return false
		default:
		}
		if !isRunning() {
			return true
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-stopCh:
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

// publish applies a status update only if the calling goroutine still owns
// the loop (identified by its stopCh) — a replaced goroutine must not clobber
// the state of the loop that superseded it.
func (l *loopMachine) publish(stopCh chan struct{}, update func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopCh == stopCh && l.running {
		update()
	}
}

func (l *loopMachine) stopLoop() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.running {
		return false
	}
	// Signal only; don't wait for doneCh. The goroutine may be blocked on
	// l.mu (deadlock) or inside a synchronous submit for the rest of the
	// current turn (which we let finish). Either way it submits nothing new.
	close(l.stopCh)
	l.running = false
	return true
}

func (l *loopMachine) info() LoopInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	info := LoopInfo{Running: l.running}
	if l.running {
		info.Prompt = l.prompt
		info.Interval = l.interval.String()
		info.Ticks = l.ticks
		info.Started = l.started.Format(time.RFC3339)
		if !l.lastRan.IsZero() {
			info.LastRan = l.lastRan.Format(time.RFC3339)
		}
		info.NextRun = l.nextRun.Format(time.RFC3339)
	}
	return info
}

func (l *loopMachine) Running() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.running
}

func ParseLoopArgs(input string) (action, intervalStr, prompt string, interval time.Duration, err error) {
	fields := strings.Fields(strings.TrimSpace(input))
	if len(fields) < 2 {
		return "status", "", "", 0, nil
	}

	sub := strings.ToLower(fields[1])
	switch sub {
	case "stop":
		return "stop", "", "", 0, nil
	case "status":
		return "status", "", "", 0, nil
	}

	intervalStr = fields[1]
	interval, err = time.ParseDuration(intervalStr)
	if err != nil {
		return "", "", "", 0, fmt.Errorf("invalid interval %q: must be like 5m, 30s, 1h", intervalStr)
	}

	if interval < 10*time.Second {
		return "", "", "", 0, fmt.Errorf("interval too short (%v): minimum is 10s", interval)
	}

	if interval > 24*time.Hour {
		return "", "", "", 0, fmt.Errorf("interval too long (%v): maximum is 24h", interval)
	}

	if len(fields) < 3 {
		return "", "", "", 0, fmt.Errorf("missing prompt: /loop %s <prompt>", intervalStr)
	}

	prompt = strings.Join(fields[2:], " ")
	return "start", intervalStr, prompt, interval, nil
}
