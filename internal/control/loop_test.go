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
		{"/loop 5s too fast", "", "", "", true},
		{"/loop 48h too long", "", "", "", true},
		{"/loop 5m", "", "", "", true},
		{"/loop garbage", "", "", "", true},
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
	l.startLoop(100*time.Millisecond, "test prompt", func(input, display string) {
		mu.Lock()
		submitted = append(submitted, display)
		mu.Unlock()
	}, notRunning)

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

	l.startLoop(10*time.Second, "old prompt", func(_, _ string) {}, notRunning)
	if !l.Running() {
		t.Fatal("expected running after first start")
	}

	var mu sync.Mutex
	var newSubmitted []string
	l.startLoop(50*time.Millisecond, "new prompt", func(_, display string) {
		mu.Lock()
		newSubmitted = append(newSubmitted, display)
		mu.Unlock()
	}, notRunning)

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
