package control

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// continuousLoopDelay is the pause between iterations of a self-paced loop —
// just enough to avoid hot-spinning if a turn returns instantly.
const continuousLoopDelay = 2 * time.Second

// loopMachine re-submits a prompt so the agent keeps working unattended,
// matching Claude Code's /loop. Two modes:
//
//   - interval (interval > 0): each iteration is a full think→act→finish
//     cycle, then wait the interval, then repeat, until /loop stop.
//   - self-paced (interval == 0): iterations run back to back continuously;
//     each turn finishes, a brief pause avoids hot-spinning, then the next
//     iteration starts. Only /loop stop (or a new /loop start) stops it —
//     the loop never stops itself.
type loopMachine struct {
	mu sync.Mutex

	prompt   string
	interval time.Duration // 0 = self-paced

	running bool
	ticks   int
	started time.Time
	lastRan time.Time
	nextRun time.Time

	stopCh chan struct{}
	doneCh chan struct{}
}

// loopConfig carries everything a loop run needs from its host controller.
type loopConfig struct {
	interval  time.Duration // 0 = self-paced
	prompt    string        // the user's task, unwrapped
	submit    func(input, display string)
	isRunning func() bool  // Controller.Running()
	notify    func(string) // lifecycle notices; may be nil
}

type LoopInfo struct {
	Running   bool   `json:"running"`
	Prompt    string `json:"prompt,omitempty"`
	Interval  string `json:"interval,omitempty"`
	SelfPaced bool   `json:"selfPaced,omitempty"`
	Ticks     int    `json:"ticks"`
	Started   string `json:"started,omitempty"`
	LastRan   string `json:"lastRan,omitempty"`
	NextRun   string `json:"nextRun,omitempty"`
}

func (l *loopMachine) startLoop(cfg loopConfig) {
	l.mu.Lock()
	if l.running {
		// Signal the previous goroutine and move on. It exits at its next
		// stopCh check; we must not wait for it here — it may be blocked on
		// l.mu itself, or inside a synchronous submit for an entire turn.
		close(l.stopCh)
	}
	now := time.Now()
	l.prompt = cfg.prompt
	l.interval = cfg.interval
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
		// The wait between iterations: the configured interval, or a brief
		// pause in self-paced mode to avoid hot-spinning.
		delay := cfg.interval
		if delay <= 0 {
			delay = continuousLoopDelay
		}
		next := now // fire immediately
		tick := 0
		for {
			if wait := time.Until(next); wait > 0 {
				timer := time.NewTimer(wait)
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
			if !waitTurnIdle(stopCh, cfg.isRunning) {
				return
			}

			tick++
			input := cfg.prompt
			estNext := time.Now().Add(delay)
			l.publish(stopCh, func() {
				l.nextRun = estNext
			})
			cfg.submit(input, fmt.Sprintf("[loop #%d] %s", tick, cfg.prompt))

			// Wait for the turn to finish before starting the interval
			// countdown (covers async submit implementations).
			if !waitTurnIdle(stopCh, cfg.isRunning) {
				return
			}

			next = time.Now().Add(delay)
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
func waitTurnIdle(stopCh chan struct{}, isRunning func() bool) bool {
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
		info.Ticks = l.ticks
		info.Started = l.started.Format(time.RFC3339)
		if l.interval > 0 {
			info.Interval = l.interval.String()
		} else {
			info.SelfPaced = true
		}
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

// LoopStartNotice is the user-facing confirmation for a started loop.
// intervalStr is empty for a self-paced loop.
func LoopStartNotice(intervalStr, prompt string) string {
	if intervalStr == "" {
		return fmt.Sprintf("loop started — self-paced, runs until /loop stop: %s", prompt)
	}
	return fmt.Sprintf("loop started — every %s: %s", intervalStr, prompt)
}

// LoopStatusNotice renders a LoopInfo for the /loop status notice.
func LoopStatusNotice(info LoopInfo) string {
	if !info.Running {
		return "no loop running"
	}
	cadence := "every " + info.Interval
	if info.SelfPaced {
		cadence = "self-paced"
	}
	return fmt.Sprintf("loop running — %d ticks, %s, next run at %s\n  prompt: %s",
		info.Ticks, cadence, info.NextRun, info.Prompt)
}

// ParseLoopArgs parses a "/loop" command line.
//
//	/loop                     → status
//	/loop status              → status
//	/loop stop                → stop
//	/loop 5m <prompt>         → start, interval 5m (10s–24h)
//	/loop <prompt>            → start, self-paced (interval 0): iterations run
//	                            back to back until /loop stop
func ParseLoopArgs(input string) (action, intervalStr, prompt string, interval time.Duration, err error) {
	rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(input), "/loop"))
	if rest == "" {
		return "status", "", "", 0, nil
	}

	fields := strings.Fields(rest)
	switch strings.ToLower(fields[0]) {
	case "stop":
		return "stop", "", "", 0, nil
	case "status":
		return "status", "", "", 0, nil
	}

	// A leading duration selects interval mode; anything else is the prompt
	// of a self-paced loop.
	if d, derr := time.ParseDuration(fields[0]); derr == nil {
		if d < 10*time.Second {
			return "", "", "", 0, fmt.Errorf("interval too short (%v): minimum is 10s", d)
		}
		if d > 24*time.Hour {
			return "", "", "", 0, fmt.Errorf("interval too long (%v): maximum is 24h", d)
		}
		if len(fields) < 2 {
			return "", "", "", 0, fmt.Errorf("missing prompt: /loop %s <prompt>", fields[0])
		}
		return "start", fields[0], strings.Join(fields[1:], " "), d, nil
	}

	return "start", "", rest, 0, nil
}
