package core

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDrainSource is a minimal drainSource for unit tests. It exposes simple
// counters so tests can drive AnyActive without a real Engine.
type fakeDrainSource struct {
	mu         sync.Mutex
	count      int
	hasPending bool
	lastUpdate time.Time
	probeCalls atomic.Int64
}

func newFakeDrainSource() *fakeDrainSource {
	return &fakeDrainSource{lastUpdate: time.Now()}
}

func (f *fakeDrainSource) AnyActive() (int, bool, time.Time) {
	f.probeCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count, f.hasPending, f.lastUpdate
}

func (f *fakeDrainSource) ProjectName() string { return "fake" }

func (f *fakeDrainSource) set(count int, pending bool, lastUpdate time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count = count
	f.hasPending = pending
	f.lastUpdate = lastUpdate
}

// fastDrainerCfg returns a drain config tuned for speed: sub-second windows
// so unit tests don't wait minutes. With duration-based DrainConfig (review
// P1-5), production run() and tests share the same loop — no shadow code.
func fastDrainerCfg(idle, maxWait time.Duration) DrainConfig {
	return DrainConfig{
		Enabled:              true,
		IdleDuration:         idle,
		MaxWaitDuration:      maxWait,
		PollInterval:         5 * time.Millisecond,
		ProgressInterval:     10 * time.Millisecond,
		ConsecutiveSigWindow: 50 * time.Millisecond,
	}
}

// --- Tests ---

func TestDrainConfigDefaults(t *testing.T) {
	d := DrainConfigDefaults()
	if d.IdleDuration != 5*time.Minute {
		t.Errorf("IdleDuration default = %v, want 5m", d.IdleDuration)
	}
	if d.MaxWaitDuration != 30*time.Minute {
		t.Errorf("MaxWaitDuration default = %v, want 30m", d.MaxWaitDuration)
	}
	if d.NewMessagePolicy != "reject" {
		t.Errorf("NewMessagePolicy default = %q, want reject", d.NewMessagePolicy)
	}
	if d.PollInterval != 5*time.Second {
		t.Errorf("PollInterval default = %v, want 5s", d.PollInterval)
	}
}

func TestApplyDefaults_RespectsCallerValues(t *testing.T) {
	cfg := DrainConfig{IdleDuration: 1 * time.Minute, MaxWaitDuration: 2 * time.Minute}
	cfg.applyDefaults()
	if cfg.IdleDuration != 1*time.Minute || cfg.MaxWaitDuration != 2*time.Minute {
		t.Errorf("caller values overwritten: %+v", cfg)
	}
	if cfg.PollInterval == 0 {
		t.Error("zero PollInterval was not filled")
	}
}

func TestBeginDrain_Disabled_Noop(t *testing.T) {
	d := NewDrainer(newFakeDrainSource(), DrainConfig{Enabled: false})
	doneCh := d.BeginDrain(false)
	select {
	case <-doneCh:
		// expected: closed immediately
	case <-time.After(50 * time.Millisecond):
		t.Fatal("disabled drain should return closed channel immediately")
	}
	if d.IsDraining() {
		t.Error("IsDraining should be false when Enabled=false")
	}
}

func TestBeginDrain_NilDrainer_Safe(t *testing.T) {
	var d *Drainer
	if d.IsDraining() {
		t.Error("nil drainer should not report draining")
	}
	doneCh := d.BeginDrain(false)
	select {
	case <-doneCh:
		// expected
	case <-time.After(50 * time.Millisecond):
		t.Fatal("nil drainer doneCh should be closed")
	}
	if got := d.SubscribeProgress(); got == nil {
		t.Error("nil drainer SubscribeProgress should return non-nil channel")
	}
}

func TestDrainer_NoActive_FastPath(t *testing.T) {
	src := newFakeDrainSource()
	src.set(0, false, time.Now().Add(-1*time.Hour)) // long-idle
	d := NewDrainer(src, fastDrainerCfg(10*time.Millisecond, 5*time.Second))
	doneCh := d.BeginDrain(false)

	select {
	case <-doneCh:
		// expected: drains immediately
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected fast-path drain when no active sessions")
	}

	snap := d.final.Load()
	if snap == nil {
		t.Fatal("final snapshot missing")
	}
	if snap.Forced || snap.TimedOut {
		t.Errorf("expected normal completion, got %+v", *snap)
	}
}

func TestDrainer_BusySession_Waits(t *testing.T) {
	src := newFakeDrainSource()
	src.set(1, false, time.Now()) // active turn
	d := NewDrainer(src, fastDrainerCfg(10*time.Millisecond, 5*time.Second))
	doneCh := d.BeginDrain(false)

	// Should NOT finish while busy.
	select {
	case <-doneCh:
		t.Fatal("drain finished while session was busy")
	case <-time.After(80 * time.Millisecond):
	}

	// Now release: count=0 + old lastUpdate.
	src.set(0, false, time.Now().Add(-1*time.Hour))

	select {
	case <-doneCh:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain did not complete after sessions went idle")
	}
}

func TestDrainer_PendingPermission_Waits(t *testing.T) {
	src := newFakeDrainSource()
	// AnyActive returns count >= pendingCount, so encode as count=1 + pending=true.
	src.set(1, true, time.Now())
	d := NewDrainer(src, fastDrainerCfg(10*time.Millisecond, 5*time.Second))
	doneCh := d.BeginDrain(false)

	select {
	case <-doneCh:
		t.Fatal("drain finished while permission was pending")
	case <-time.After(80 * time.Millisecond):
	}

	src.set(0, false, time.Now().Add(-1*time.Hour))
	select {
	case <-doneCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain did not complete after pending cleared")
	}
}

func TestDrainer_IdleDurationEffect(t *testing.T) {
	src := newFakeDrainSource()
	// count=0 but lastUpdate just now → still within idle window.
	src.set(0, false, time.Now())
	d := NewDrainer(src, fastDrainerCfg(100*time.Millisecond, 5*time.Second))
	doneCh := d.BeginDrain(false)

	select {
	case <-doneCh:
		t.Fatal("drain finished before idle window elapsed")
	case <-time.After(50 * time.Millisecond):
	}

	// After the idle window elapses, drain should complete on next probe.
	select {
	case <-doneCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain did not complete after idle window elapsed")
	}
	snap := d.final.Load()
	if snap.TimedOut {
		t.Errorf("idle-completion should not be marked TimedOut: %+v", *snap)
	}
}

func TestDrainer_MaxWaitTimeout(t *testing.T) {
	src := newFakeDrainSource()
	src.set(1, false, time.Now()) // never goes idle
	d := NewDrainer(src, fastDrainerCfg(10*time.Millisecond, 80*time.Millisecond))
	doneCh := d.BeginDrain(false)

	select {
	case <-doneCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain did not time out when never idle")
	}
	snap := d.final.Load()
	if !snap.TimedOut {
		t.Errorf("expected TimedOut=true, got %+v", *snap)
	}
	if snap.Forced {
		t.Errorf("expected Forced=false on timeout, got %+v", *snap)
	}
}

func TestDrainer_Force_Immediate(t *testing.T) {
	src := newFakeDrainSource()
	src.set(5, true, time.Now()) // many active
	d := NewDrainer(src, fastDrainerCfg(10*time.Millisecond, 5*time.Second))
	doneCh := d.BeginDrain(true)

	select {
	case <-doneCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("forced drain did not finish quickly")
	}
	snap := d.final.Load()
	if !snap.Forced || snap.TimedOut {
		t.Errorf("expected Forced=true TimedOut=false, got %+v", *snap)
	}
}

func TestBeginDrain_ConsecutiveTriggers_EscalateForce(t *testing.T) {
	src := newFakeDrainSource()
	src.set(1, false, time.Now())
	cfg := DrainConfig{
		Enabled:              true,
		IdleDuration:         30 * time.Minute, // never idle naturally during the test
		MaxWaitDuration:      30 * time.Minute, // never times out during the test
		PollInterval:         5 * time.Millisecond,
		ProgressInterval:     20 * time.Millisecond,
		ConsecutiveSigWindow: 100 * time.Millisecond,
	}
	d := NewDrainer(src, cfg)

	doneCh1 := d.BeginDrain(false)
	if !d.IsDraining() {
		t.Fatal("IsDraining should be true after first BeginDrain")
	}
	// Within window — second call should escalate to force.
	time.Sleep(20 * time.Millisecond)
	doneCh2 := d.BeginDrain(false)
	if doneCh1 != doneCh2 {
		t.Error("BeginDrain should return same doneCh across calls")
	}

	select {
	case <-doneCh1:
	case <-time.After(2 * time.Second):
		t.Fatal("force-escalated drain did not finish")
	}
	snap := d.final.Load()
	if !snap.Forced {
		t.Errorf("expected Forced=true after consecutive triggers, got %+v", *snap)
	}
}

func TestBeginDrain_Idempotent(t *testing.T) {
	src := newFakeDrainSource()
	src.set(0, false, time.Now().Add(-1*time.Hour))
	cfg := fastDrainerCfg(10*time.Millisecond, 5*time.Second)
	d := NewDrainer(src, cfg)

	// Force=true on first call so the drain finishes deterministically.
	ch1 := d.BeginDrain(true)
	ch2 := d.BeginDrain(false)
	ch3 := d.BeginDrain(true)
	if ch1 != ch2 || ch2 != ch3 {
		t.Error("BeginDrain should always return the same doneCh")
	}
	select {
	case <-ch1:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("forced drain did not finish")
	}
	// Subsequent BeginDrain after done returns same closed channel.
	ch4 := d.BeginDrain(false)
	select {
	case <-ch4:
	default:
		t.Error("post-done BeginDrain should return already-closed channel")
	}
}

func TestSubscribeProgress_DeliversTicks(t *testing.T) {
	src := newFakeDrainSource()
	src.set(1, false, time.Now()) // busy → drain stays alive
	d := NewDrainer(src, fastDrainerCfg(10*time.Millisecond, 200*time.Millisecond))
	d.BeginDrain(false)
	sub := d.SubscribeProgress()

	got := 0
	deadline := time.After(500 * time.Millisecond)
	doneSeen := false
	for !doneSeen {
		select {
		case evt, ok := <-sub:
			if !ok {
				doneSeen = true
				break
			}
			got++
			if evt.Done {
				doneSeen = true
			}
		case <-deadline:
			t.Fatal("did not see drain completion via subscriber within deadline")
		}
	}
	if got < 2 {
		t.Errorf("expected at least 2 progress events (intermediate + final), got %d", got)
	}
}

func TestSubscribeProgress_AfterDone(t *testing.T) {
	src := newFakeDrainSource()
	src.set(0, false, time.Now().Add(-1*time.Hour))
	d := NewDrainer(src, fastDrainerCfg(1*time.Millisecond, 5*time.Second))
	doneCh := d.BeginDrain(false)

	// Wait for completion.
	select {
	case <-doneCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain did not finish for post-done test setup")
	}

	sub := d.SubscribeProgress()
	select {
	case evt, ok := <-sub:
		if !ok {
			// Already closed without an event — also acceptable when final
			// event was raced; final.Load() is the canonical source.
			if d.final.Load() == nil {
				t.Error("post-done subscribe got closed channel and final is also nil")
			}
			return
		}
		if !evt.Done {
			t.Errorf("expected Done=true on post-completion subscribe, got %+v", evt)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("post-completion subscribe did not receive final event")
	}
}

func TestBeginDrain_StartedAtRecorded(t *testing.T) {
	src := newFakeDrainSource()
	src.set(1, false, time.Now())
	cfg := fastDrainerCfg(30*time.Minute, 30*time.Minute)
	d := NewDrainer(src, cfg)
	before := time.Now().UnixNano()
	d.BeginDrain(true)
	after := time.Now().UnixNano()
	got := d.startedAt.Load()
	if got < before || got > after {
		t.Errorf("startedAt %d not within [%d,%d]", got, before, after)
	}
	<-d.doneCh
}

// TestSubscribeProgress_RaceWithFinish stresses the SubscribeProgress vs
// finish race that P1-2 fixed: many subscribers spinning up while drain
// completes. None of them should block forever.
func TestSubscribeProgress_RaceWithFinish(t *testing.T) {
	src := newFakeDrainSource()
	src.set(0, false, time.Now().Add(-1*time.Hour))
	d := NewDrainer(src, fastDrainerCfg(1*time.Millisecond, 5*time.Second))

	const N = 50
	var wg sync.WaitGroup
	subs := make([]<-chan DrainProgress, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			subs[i] = d.SubscribeProgress()
		}(i)
	}

	d.BeginDrain(false)
	wg.Wait()

	// Every subscriber must close (no leaks) within deadline.
	deadline := time.After(2 * time.Second)
	for i, sub := range subs {
		drained := false
		for !drained {
			select {
			case _, ok := <-sub:
				if !ok {
					drained = true
				}
			case <-deadline:
				t.Fatalf("subscriber %d never closed", i)
			}
		}
	}
}
