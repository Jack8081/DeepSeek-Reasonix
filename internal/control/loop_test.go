package control

import (
	"strings"
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

func TestParseLoopDoneMarker(t *testing.T) {
	tests := []struct {
		text       string
		wantDone   bool
		wantReason string
	}{
		{"all finished\n\n[loop:done]", true, ""},
		{"all finished\n[LOOP:DONE]\n\n", true, ""},
		{"stuck\n[loop:blocked: need API credentials]", true, "need API credentials"},
		{"more to do\n[loop:continue]", false, ""},
		{"no marker at all", false, ""},
		{"[loop:done] mentioned mid-text\nbut final line is prose", false, ""},
		{"", false, ""},
	}
	for _, tt := range tests {
		done, reason := parseLoopDoneMarker(tt.text)
		if done != tt.wantDone || reason != tt.wantReason {
			t.Errorf("parseLoopDoneMarker(%q) = (%v, %q), want (%v, %q)",
				tt.text, done, reason, tt.wantDone, tt.wantReason)
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

// TestLoopMachineSelfPacedStopsOnDone runs a self-paced loop whose stubbed
// agent reports [loop:done] on the third iteration, and verifies the loop
// stops itself with a completion notice.
func TestLoopMachineSelfPacedStopsOnDone(t *testing.T) {
	var l loopMachine

	var mu sync.Mutex
	var inputs []string
	var reply string
	var notices []string

	l.startLoop(loopConfig{
		interval: 0, // self-paced
		prompt:   "build the feature",
		submit: func(input, _ string) {
			mu.Lock()
			inputs = append(inputs, input)
			if len(inputs) < 3 {
				reply = "made progress\n[loop:continue]"
			} else {
				reply = "all done\n[loop:done]"
			}
			mu.Unlock()
		},
		isRunning: notRunning,
		lastText: func() string {
			mu.Lock()
			defer mu.Unlock()
			return reply
		},
		notify: func(msg string) {
			mu.Lock()
			notices = append(notices, msg)
			mu.Unlock()
		},
	})

	info := l.info()
	if !info.SelfPaced {
		t.Error("expected SelfPaced in info for interval 0")
	}

	// Iterations 1 and 2 continue; each ends with a continuousLoopDelay wait,
	// so give the third (final) iteration ample time.
	deadline := time.Now().Add(3 * continuousLoopDelay)
	for l.Running() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if l.Running() {
		t.Fatal("expected self-paced loop to stop itself after [loop:done]")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(inputs) != 3 {
		t.Fatalf("expected exactly 3 iterations, got %d", len(inputs))
	}
	for i, in := range inputs {
		if !strings.Contains(in, "build the feature") {
			t.Errorf("iteration %d input missing task prompt: %q", i+1, in)
		}
		if !strings.Contains(in, "[loop:done]") {
			t.Errorf("iteration %d input missing marker instructions: %q", i+1, in)
		}
	}
	if len(notices) != 1 || !strings.Contains(notices[0], "complete") {
		t.Errorf("expected one completion notice, got %v", notices)
	}
}

// TestLoopMachineSelfPacedStopsOnBlocked verifies [loop:blocked: reason]
// stops the loop and surfaces the reason.
func TestLoopMachineSelfPacedStopsOnBlocked(t *testing.T) {
	var l loopMachine

	var mu sync.Mutex
	var notices []string

	l.startLoop(loopConfig{
		interval:  0,
		prompt:    "deploy it",
		submit:    func(_, _ string) {},
		isRunning: notRunning,
		lastText:  func() string { return "cannot continue\n[loop:blocked: missing prod token]" },
		notify: func(msg string) {
			mu.Lock()
			notices = append(notices, msg)
			mu.Unlock()
		},
	})

	deadline := time.Now().Add(2 * continuousLoopDelay)
	for l.Running() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if l.Running() {
		t.Fatal("expected loop to stop after [loop:blocked]")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(notices) != 1 || !strings.Contains(notices[0], "missing prod token") {
		t.Errorf("expected blocked notice with reason, got %v", notices)
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
