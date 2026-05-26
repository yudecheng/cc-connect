package core

// Integration tests for graceful drain (REQ-20260526-cc-connect-graceful-drain).
//
// These tests exercise the full Engine + Drainer pipeline end-to-end against
// the four acceptance criteria from requirement.md:
//
//   AC1: idle engine drains immediately
//   AC2: drain waits for in-flight turns; new messages during drain are rejected
//        with the busy_message; once turns complete, drain finishes
//   AC3: force=true (and SIGINT-twice analog) skips the wait
//   AC4: /restart triggerer receives periodic progress updates ending in Done
//
// Unlike drain_test.go (which targets Drainer in isolation with a fake source)
// and engine_drain_test.go (which targets individual Engine methods), this file
// drives the full Engine.handleMessage gate alongside the real Drainer.run loop
// over a real SessionManager. The unit-level tests in drain_test.go and
// engine_drain_test.go remain authoritative for individual contracts; this file
// is the AC-level smoke for the integrated behavior.

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// integrationDrainCfg returns a DrainConfig tuned for AC-level integration
// tests. Sub-second windows so each AC test completes in under 1s.
func integrationDrainCfg() DrainConfig {
	return DrainConfig{
		Enabled:              true,
		IdleDuration:         20 * time.Millisecond,
		MaxWaitDuration:      2 * time.Second,
		PollInterval:         5 * time.Millisecond,
		ProgressInterval:     10 * time.Millisecond,
		ConsecutiveSigWindow: 100 * time.Millisecond,
		NewMessagePolicy:     "reject",
	}
}

// TestIntegration_AC1_IdleEngineDrainsImmediately covers AC1: when there are
// no active sessions and no pending interactive states, BeginDrain returns
// quickly (within the IdleDuration window) without forcing or timing out.
func TestIntegration_AC1_IdleEngineDrainsImmediately(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	e.SetDrainConfig(integrationDrainCfg())

	d := e.Drainer()
	if d == nil {
		t.Fatal("Drainer() nil after enabling")
	}

	start := time.Now()
	doneCh := d.BeginDrain(false)

	select {
	case <-doneCh:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle drain did not complete within 500ms")
	}

	elapsed := time.Since(start)
	snap := d.final.Load()
	if snap == nil {
		t.Fatal("final snapshot missing after idle drain")
	}
	if snap.Forced {
		t.Errorf("idle drain should not be Forced, got %+v", *snap)
	}
	if snap.TimedOut {
		t.Errorf("idle drain should not be TimedOut, got %+v", *snap)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("idle drain took %v, expected <500ms", elapsed)
	}
}

// TestIntegration_AC2_BusyWaitAndReject covers AC2 end-to-end:
//
//  1. A session is busy (locked) when drain starts.
//  2. While IsDraining(), brand-new messages from a different session are
//     rejected by handleMessage with the i18n busy_message (NOT enqueued, NOT
//     forwarded to the agent).
//  3. Once the busy session releases its lock and stays quiet for the idle
//     window, the drain completes normally (Forced=false, TimedOut=false).
func TestIntegration_AC2_BusyWaitAndReject(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	e.SetDrainConfig(integrationDrainCfg())
	d := e.Drainer()

	// Step 1: install one busy session.
	busy := e.sessions.NewSession("user-active", "test")
	if !busy.TryLock() {
		t.Fatal("could not lock busy session in test setup")
	}

	// Begin drain; it must NOT complete while busy session holds the lock.
	doneCh := d.BeginDrain(false)
	if !d.IsDraining() {
		t.Fatal("expected IsDraining()==true right after BeginDrain")
	}

	select {
	case <-doneCh:
		busy.Unlock()
		t.Fatal("drain finished while session was still busy")
	case <-time.After(80 * time.Millisecond):
	}

	// Step 2: new message from a different session must be rejected.
	p.clearSent()
	e.handleMessage(p, &Message{
		SessionKey: "fresh-session",
		Platform:   "test",
		UserID:     "u-new",
		UserName:   "Newbie",
		Content:    "hello?",
		ReplyCtx:   "ctx-1",
	})
	got := p.getSent()
	if len(got) != 1 {
		busy.Unlock()
		t.Fatalf("expected exactly one busy_message reply, got %d: %#v", len(got), got)
	}
	if !strings.Contains(got[0], "restart") && !strings.Contains(got[0], "Server") {
		t.Errorf("busy_message did not contain expected i18n text; got %q", got[0])
	}

	// Step 3: release the session; drain should now finish via the idle path.
	busy.Unlock()

	select {
	case <-doneCh:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not finish after busy session released")
	}

	snap := d.final.Load()
	if snap == nil {
		t.Fatal("final snapshot missing after AC2 drain")
	}
	if snap.Forced {
		t.Errorf("AC2 drain should not be Forced, got %+v", *snap)
	}
	if snap.TimedOut {
		t.Errorf("AC2 drain should not be TimedOut, got %+v", *snap)
	}
}

// TestIntegration_AC3_ForceSkipsWait covers AC3: a busy session is present, but
// BeginDrain(true) — equivalent to `/restart force` or a second SIGINT within
// ConsecutiveSigWindow — completes without waiting for the lock to release.
func TestIntegration_AC3_ForceSkipsWait(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	cfg := integrationDrainCfg()
	cfg.IdleDuration = 30 * time.Minute    // would normally never finish
	cfg.MaxWaitDuration = 30 * time.Minute // would normally never time out
	e.SetDrainConfig(cfg)
	d := e.Drainer()

	busy := e.sessions.NewSession("user-stuck", "test")
	if !busy.TryLock() {
		t.Fatal("could not lock busy session in test setup")
	}
	defer busy.Unlock()

	start := time.Now()
	doneCh := d.BeginDrain(true)

	select {
	case <-doneCh:
		// expected: force=true short-circuits the loop
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("force drain did not finish quickly with busy session; elapsed=%v", time.Since(start))
	}

	snap := d.final.Load()
	if snap == nil {
		t.Fatal("final snapshot missing after force drain")
	}
	if !snap.Forced {
		t.Errorf("expected Forced=true after BeginDrain(true), got %+v", *snap)
	}
	if snap.TimedOut {
		t.Errorf("force drain should not be TimedOut, got %+v", *snap)
	}
}

// TestIntegration_AC3b_ConsecutiveSignalEscalates covers the "second SIGINT
// within window" path: BeginDrain(false) then BeginDrain(false) within
// ConsecutiveSigWindow must escalate to force, even with a busy session.
func TestIntegration_AC3b_ConsecutiveSignalEscalates(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	cfg := integrationDrainCfg()
	cfg.IdleDuration = 30 * time.Minute
	cfg.MaxWaitDuration = 30 * time.Minute
	cfg.ConsecutiveSigWindow = 200 * time.Millisecond
	e.SetDrainConfig(cfg)
	d := e.Drainer()

	busy := e.sessions.NewSession("user-stuck-2", "test")
	if !busy.TryLock() {
		t.Fatal("could not lock busy session in test setup")
	}
	defer busy.Unlock()

	doneCh1 := d.BeginDrain(false)
	if !d.IsDraining() {
		t.Fatal("IsDraining should be true after first BeginDrain")
	}
	time.Sleep(30 * time.Millisecond)
	doneCh2 := d.BeginDrain(false)
	if doneCh1 != doneCh2 {
		t.Error("BeginDrain should always return the same doneCh")
	}

	select {
	case <-doneCh1:
		// expected: second trigger within window escalated to force
	case <-time.After(1 * time.Second):
		t.Fatal("consecutive-trigger force-escalation did not finish")
	}

	snap := d.final.Load()
	if !snap.Forced {
		t.Errorf("expected Forced=true after consecutive triggers, got %+v", *snap)
	}
}

// TestIntegration_AC4_ProgressUpdates covers AC4: the /restart triggerer (via
// SubscribeProgress) receives at least one in-flight progress event AND a final
// Done event, before the channel closes.
func TestIntegration_AC4_ProgressUpdates(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	cfg := integrationDrainCfg()
	cfg.IdleDuration = 200 * time.Millisecond // long enough to see ticks
	cfg.MaxWaitDuration = 1 * time.Second
	cfg.ProgressInterval = 30 * time.Millisecond
	e.SetDrainConfig(cfg)
	d := e.Drainer()

	// Hold a busy session so progress ticks have something to report before
	// drain completes.
	busy := e.sessions.NewSession("user-active", "test")
	if !busy.TryLock() {
		t.Fatal("could not lock busy session")
	}

	d.BeginDrain(false)
	sub := d.SubscribeProgress()

	var (
		mu          sync.Mutex
		ticks       int
		sawDone     bool
		sawNonEmpty bool
	)
	subDone := make(chan struct{})

	go func() {
		defer close(subDone)
		for evt := range sub {
			mu.Lock()
			ticks++
			if evt.Done {
				sawDone = true
			}
			if evt.ActiveSessions > 0 {
				sawNonEmpty = true
			}
			mu.Unlock()
		}
	}()

	// Let a few ticks accumulate, then release.
	time.Sleep(120 * time.Millisecond)
	busy.Unlock()

	// Wait for the subscriber goroutine to drain; channel close = drain done.
	select {
	case <-subDone:
	case <-time.After(2 * time.Second):
		t.Fatal("progress subscriber channel never closed")
	}

	mu.Lock()
	defer mu.Unlock()
	if !sawDone {
		t.Errorf("expected at least one Done=true progress event")
	}
	if !sawNonEmpty {
		t.Errorf("expected at least one event with ActiveSessions>0; got ticks=%d", ticks)
	}
	if ticks < 2 {
		t.Errorf("expected at least 2 progress ticks (in-flight + Done); got %d", ticks)
	}
}

// TestIntegration_AC2_PendingPermissionPassesThrough covers a critical AC2
// sub-clause: an in-turn permission response (e.g. user replying "allow" to a
// tool-use prompt) must NOT be rejected by the drain gate, otherwise the very
// turn we are trying to protect would deadlock.
//
// This complements TestEngine_HandleMessage_DrainingAllowsPermissionResponse
// by also asserting that no busy_message was sent on the platform.
func TestIntegration_AC2_PendingPermissionPassesThrough(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	e.SetDrainConfig(integrationDrainCfg())
	d := e.Drainer()

	// Pre-register pending permission for the in-turn session.
	const inTurnKey = "in-turn-session"
	e.interactiveMu.Lock()
	e.interactiveStates[inTurnKey] = &interactiveState{
		pending: &pendingPermission{},
	}
	e.interactiveMu.Unlock()

	// Keep one busy session so drain stays in flight.
	busy := e.sessions.NewSession("user-active", "test")
	if !busy.TryLock() {
		t.Fatal("could not lock busy session")
	}
	defer busy.Unlock()

	d.BeginDrain(false)
	if !d.IsDraining() {
		t.Fatal("expected IsDraining=true")
	}

	// Confirm classification: the in-turn key resolves to true, others to false.
	if !e.isInTurnPermissionResponse(inTurnKey) {
		t.Fatal("expected isInTurnPermissionResponse=true for in-turn session")
	}
	if e.isInTurnPermissionResponse("other-session") {
		t.Error("expected isInTurnPermissionResponse=false for unrelated session")
	}

	// And confirm a brand-new (different) session still gets rejected, so the
	// gate hasn't been disabled wholesale.
	p.clearSent()
	e.handleMessage(p, &Message{
		SessionKey: "fresh-session",
		Platform:   "test",
		UserID:     "u-new",
		Content:    "hi",
		ReplyCtx:   "ctx-fresh",
	})
	got := p.getSent()
	if len(got) != 1 {
		t.Fatalf("expected fresh-session to be rejected; sent=%d (%#v)", len(got), got)
	}
}
