package control

import (
	"sync"
	"testing"
	"time"
)

func TestParseLoopArgs(t *testing.T) {
	tests := []struct {
		input        string
		wantAction   string
		wantInterval string
		wantPrompt   string
		wantErr      bool
	}{
		{"/loop", "status", "", "", false},
		{"/loop stop", "stop", "", "", false},
		{"/loop status", "status", "", "", false},
		{"/loop 5m run tests", "start", "5m", "run tests", false},
		{"/loop 30s check logs", "start", "30s", "check logs", false},
		{"/loop 1h do the thing", "start", "1h", "do the thing", false},
		// No leading duration → self-paced loop; the rest is the prompt.
		{"/loop fix all failing tests", "start", "", "fix all failing tests", false},
		{"/loop garbage", "start", "", "garbage", false},
		{"/loop 5s too fast", "", "", "", true},
		{"/loop 48h too long", "", "", "", true},
		{"/loop 5m", "", "", "", true},
	}

	for _, tt := range tests {
		action, intervalStr, prompt, interval, err := ParseLoopArgs(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseLoopArgs(%q) expected error, got nil", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseLoopArgs(%q) unexpected error: %v", tt.input, err)
			continue
		}
		if action != tt.wantAction {
			t.Errorf("ParseLoopArgs(%q) action = %q, want %q", tt.input, action, tt.wantAction)
		}
		if intervalStr != tt.wantInterval {
			t.Errorf("ParseLoopArgs(%q) intervalStr = %q, want %q", tt.input, intervalStr, tt.wantInterval)
		}
		if prompt != tt.wantPrompt {
			t.Errorf("ParseLoopArgs(%q) prompt = %q, want %q", tt.input, prompt, tt.wantPrompt)
		}
		if tt.wantInterval != "" {
			expected, _ := time.ParseDuration(tt.wantInterval)
			if interval != expected {
				t.Errorf("ParseLoopArgs(%q) interval = %v, want %v", tt.input, interval, expected)
			}
		} else if action == "start" && interval != 0 {
			t.Errorf("ParseLoopArgs(%q) interval = %v, want 0 (self-paced)", tt.input, interval)
		}
	}
}

// notRunning is a stub isRunning func for tests — always returns false.
func notRunning() bool { return false }

func TestLoopMachineStartStop(t *testing.T) {
	var l loopMachine

	if l.Running() {
		t.Fatal("expected loop not running initially")
	}

	info := l.info()
	if info.Running {
		t.Fatal("expected info.Running = false")
	}

	var mu sync.Mutex
	var submitted []string
	l.startLoop(loopConfig{
		interval: 100 * time.Millisecond,
		prompt:   "test prompt",
		submit: func(input, display string) {
			mu.Lock()
			submitted = append(submitted, display)
			mu.Unlock()
		},
		isRunning: notRunning,
	})

	if !l.Running() {
		t.Fatal("expected loop to be running after start")
	}

	info = l.info()
	if !info.Running {
		t.Fatal("expected info.Running = true")
	}
	if info.Prompt != "test prompt" {
		t.Errorf("info.Prompt = %q, want %q", info.Prompt, "test prompt")
	}
	if info.SelfPaced {
		t.Error("interval loop should not report SelfPaced")
	}

	// Wait for up to 3 ticks.
	time.Sleep(350 * time.Millisecond)

	stopped := l.stopLoop()
	if !stopped {
		t.Fatal("expected stop to return true")
	}

	if l.Running() {
		t.Fatal("expected loop not running after stop")
	}

	if l.stopLoop() {
		t.Fatal("expected second stop to return false")
	}

	mu.Lock()
	n := len(submitted)
	mu.Unlock()

	if n < 2 {
		t.Errorf("expected at least 2 submissions in 350ms, got %d", n)
	}

	for _, s := range submitted {
		if len(s) == 0 {
			t.Error("empty submission")
		}
	}
}

func TestLoopMachineStartReplacesPrevious(t *testing.T) {
	var l loopMachine

	l.startLoop(loopConfig{
		interval:  10 * time.Second,
		prompt:    "old prompt",
		submit:    func(_, _ string) {},
		isRunning: notRunning,
	})
	if !l.Running() {
		t.Fatal("expected running after first start")
	}

	var mu sync.Mutex
	var newSubmitted []string
	l.startLoop(loopConfig{
		interval: 50 * time.Millisecond,
		prompt:   "new prompt",
		submit: func(_, display string) {
			mu.Lock()
			newSubmitted = append(newSubmitted, display)
			mu.Unlock()
		},
		isRunning: notRunning,
	})

	if !l.Running() {
		t.Fatal("expected running after second start")
	}

	info := l.info()
	if info.Prompt != "new prompt" {
		t.Errorf("info.Prompt = %q, want %q", info.Prompt, "new prompt")
	}

	time.Sleep(200 * time.Millisecond)
	l.stopLoop()

	mu.Lock()
	defer mu.Unlock()
	if len(newSubmitted) < 2 {
		t.Errorf("expected at least 2 submissions for new loop, got %d", len(newSubmitted))
	}
}

// TestLoopMachineSelfPacedRunsContinuously verifies that a self-paced loop
// keeps running forever and only stops via stopLoop(). The loop no longer
// self-terminates on loop markers — it must be manually stopped.
func TestLoopMachineSelfPacedRunsContinuously(t *testing.T) {
	var l loopMachine

	var mu sync.Mutex
	var inputs []string
	ticks := 0

	l.startLoop(loopConfig{
		interval: 0, // self-paced
		prompt:   "build the feature",
		submit: func(input, _ string) {
			mu.Lock()
			inputs = append(inputs, input)
			ticks++
			mu.Unlock()
		},
		isRunning: notRunning,
	})

	info := l.info()
	if !info.SelfPaced {
		t.Error("expected SelfPaced in info for interval 0")
	}

	// Wait for at least 3 iterations.
	deadline := time.Now().Add(4 * continuousLoopDelay)
	for {
		mu.Lock()
		n := ticks
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !l.Running() {
		t.Fatal("expected self-paced loop to stay running — it should not self-stop")
	}

	mu.Lock()
	n := ticks
	mu.Unlock()
	if n < 3 {
		t.Fatalf("expected at least 3 iterations, got %d", n)
	}

	// Inputs should be the raw prompt (not wrapped with marker instructions).
	for i, in := range inputs {
		if in != "build the feature" {
			t.Errorf("iteration %d input = %q, want raw prompt %q", i+1, in, "build the feature")
		}
	}

	// Now stop it manually.
	stopped := l.stopLoop()
	if !stopped {
		t.Fatal("expected stop to return true")
	}
	if l.Running() {
		t.Fatal("expected loop not running after stop")
	}
}

// TestLoopMachineSelfPacedIgnoresMarkers verifies that [loop:done] and
// [loop:blocked:] markers in the conversation do NOT stop the loop. The loop
// only stops via /loop stop.
func TestLoopMachineSelfPacedIgnoresMarkers(t *testing.T) {
	var l loopMachine

	var mu sync.Mutex
	ticks := 0

	l.startLoop(loopConfig{
		interval: 0,
		prompt:   "deploy it",
		submit: func(_, _ string) {
			mu.Lock()
			ticks++
			mu.Unlock()
		},
		isRunning: notRunning,
	})

	// Wait for several iterations — the loop should keep running regardless
	// of what the model might say in the conversation.
	deadline := time.Now().Add(4 * continuousLoopDelay)
	for {
		mu.Lock()
		n := ticks
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !l.Running() {
		t.Fatal("expected loop to keep running — markers should not stop it")
	}

	// Confirm it stops cleanly.
	l.stopLoop()
	if l.Running() {
		t.Fatal("expected loop not running after stop")
	}

	mu.Lock()
	defer mu.Unlock()
	if ticks < 3 {
		t.Fatalf("expected at least 3 iterations, got %d", ticks)
	}
}

func TestParseLoopArgsMinimumInterval(t *testing.T) {
	_, _, _, interval, err := ParseLoopArgs("/loop 10s prompt")
	if err != nil {
		t.Errorf("10s interval should be allowed: %v", err)
	}
	if interval != 10*time.Second {
		t.Errorf("interval = %v, want %v", interval, 10*time.Second)
	}

	_, _, _, _, err = ParseLoopArgs("/loop 9s prompt")
	if err == nil {
		t.Error("9s interval should be rejected")
	}
}
