// Graceful drain (REQ-20260526-cc-connect-graceful-drain).
//
// The Drainer waits for active turns to finish before the engine shuts down,
// so /restart and SIGINT/SIGTERM no longer interrupt in-flight conversations.
//
// Lifecycle:
//
//	idle  --BeginDrain(false)-->  draining  -- AnyActive==0 for IdleMinutes -->  done
//	            ^                       |
//	            |                       +-- 2nd BeginDrain within ConsecutiveSigWindow --> force=true --> done
//	            |                       +-- BeginDrain(true) -----------------> force=true --> done
//	            |                       +-- elapsed > MaxWaitMinutes ---------> timedOut=true --> done
//	            |
//	            +-- Disabled config: BeginDrain returns an already-closed doneCh (noop)
//
// Hot path (handleMessage): the drain check uses an atomic.Bool, no mutex.
package core

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// DrainConfig is the per-Engine graceful-drain config.
//
// Zero value (Enabled=false) preserves the legacy "shut down immediately"
// behavior. When Enabled=true, BeginDrain blocks the shutdown sequence until
// all active turns finish or the timeouts fire.
//
// All durations are stored as time.Duration so production and unit tests
// share one run() loop (review P1-5). The TOML layer (cmd/cc-connect/main.go
// toDrainConfig) maps minute/second integers to durations.
type DrainConfig struct {
	Enabled              bool
	IdleDuration         time.Duration // wait this long after AnyActive==0 before declaring done
	MaxWaitDuration      time.Duration // hard upper bound for the entire drain
	NewMessagePolicy     string        // "reject" only for now
	BusyMessage          string        // empty = use i18n MsgServerDraining
	PollInterval         time.Duration // how often to evaluate AnyActive
	ProgressInterval     time.Duration // how often to push DrainProgress to subscribers
	ConsecutiveSigWindow time.Duration // 2nd BeginDrain within this window escalates to force
}

// DrainConfigDefaults returns the default values applied when callers leave
// individual fields zero. Enabled stays at the caller's setting.
func DrainConfigDefaults() DrainConfig {
	return DrainConfig{
		IdleDuration:         5 * time.Minute,
		MaxWaitDuration:      30 * time.Minute,
		NewMessagePolicy:     "reject",
		PollInterval:         5 * time.Second,
		ProgressInterval:     30 * time.Second,
		ConsecutiveSigWindow: 5 * time.Second,
	}
}

// applyDefaults fills any zero/negative duration field on cfg with the
// default value. Note that "explicit 0" is treated as "use default" — this
// is documented as a config caveat (review P2-2).
func (c *DrainConfig) applyDefaults() {
	d := DrainConfigDefaults()
	if c.IdleDuration <= 0 {
		c.IdleDuration = d.IdleDuration
	}
	if c.MaxWaitDuration <= 0 {
		c.MaxWaitDuration = d.MaxWaitDuration
	}
	if c.NewMessagePolicy == "" {
		c.NewMessagePolicy = d.NewMessagePolicy
	}
	if c.PollInterval <= 0 {
		c.PollInterval = d.PollInterval
	}
	if c.ProgressInterval <= 0 {
		c.ProgressInterval = d.ProgressInterval
	}
	if c.ConsecutiveSigWindow <= 0 {
		c.ConsecutiveSigWindow = d.ConsecutiveSigWindow
	}
}

// DrainProgress is pushed to /restart triggerer subscribers every
// ProgressInterval while drain is in flight, plus a final event with Done=true.
type DrainProgress struct {
	ActiveSessions int
	HasPending     bool
	Elapsed        time.Duration
	Done           bool
	Forced         bool
	TimedOut       bool
}

// drainSource abstracts the engine-side activity probe so the Drainer can be
// unit-tested without spinning a real Engine. The methods correspond to
// design.md §5.4 AnyActive() and the engine's project name (used in logs).
type drainSource interface {
	AnyActive() (count int, hasPending bool, lastUpdate time.Time)
	ProjectName() string
}

// Drainer manages the graceful-drain lifecycle for a single Engine.
//
// All public methods are safe for concurrent use. IsDraining is the hot-path
// query made by Engine.handleMessage on every message; it must be lock-free.
type Drainer struct {
	src drainSource
	cfg DrainConfig

	// State machine flags (atomic for hot-path reads).
	draining atomic.Bool
	force    atomic.Bool

	// Trigger times in unix nano. lastTrigger is updated on every BeginDrain
	// so the second call within ConsecutiveSigWindow escalates to force.
	startedAt   atomic.Int64
	lastTrigger atomic.Int64

	// doneCh is closed exactly once by the drain goroutine when shutdown
	// is allowed to proceed (idle reached, force, or timeout).
	doneCh    chan struct{}
	closeOnce sync.Once

	// Snapshot of the final progress so callers reading after-the-fact still
	// see a meaningful state.
	final atomic.Pointer[DrainProgress]

	// progressSubs holds subscribers; protected by progressMu. Each subscriber
	// receives a buffered channel that is closed alongside doneCh.
	progressMu   sync.Mutex
	progressSubs []chan DrainProgress
}

// NewDrainer creates a Drainer bound to the given source. Callers should
// re-create the Drainer if config changes between Engine restarts; in steady
// state config is set once at startup.
func NewDrainer(src drainSource, cfg DrainConfig) *Drainer {
	cfg.applyDefaults()
	return &Drainer{
		src:    src,
		cfg:    cfg,
		doneCh: make(chan struct{}),
	}
}

// IsDraining reports whether drain is currently active. Hot-path safe.
func (d *Drainer) IsDraining() bool {
	if d == nil {
		return false
	}
	return d.draining.Load()
}

// Config returns a copy of the active DrainConfig.
func (d *Drainer) Config() DrainConfig {
	if d == nil {
		return DrainConfig{}
	}
	return d.cfg
}

// BeginDrain starts the drain sequence (or escalates an in-progress drain).
//
// Idempotent semantics:
//   - First call when Enabled=false: returns an already-closed doneCh (noop).
//   - First call when Enabled=true: starts the drain goroutine.
//   - Second call with force=true: sets force flag, drain goroutine completes ASAP.
//   - Second call with force=false within ConsecutiveSigWindow: same as force=true.
//   - Second call with force=false outside the window: noop (already draining).
//
// Returns the doneCh so callers can <-BeginDrain(...).
func (d *Drainer) BeginDrain(force bool) <-chan struct{} {
	if d == nil || !d.cfg.Enabled {
		// Disabled: behave like legacy direct shutdown.
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	now := time.Now().UnixNano()
	last := d.lastTrigger.Swap(now)

	if d.draining.CompareAndSwap(false, true) {
		// First trigger.
		d.startedAt.Store(now)
		if force {
			d.force.Store(true)
		}
		go d.run()
		slog.Info("drain: started", "project", d.src.ProjectName(), "force", force)
		return d.doneCh
	}

	// Already draining: maybe escalate to force.
	if force {
		if d.force.CompareAndSwap(false, true) {
			slog.Info("drain: forced (explicit)", "project", d.src.ProjectName())
		}
	} else if last > 0 {
		elapsed := time.Duration(now - last)
		if elapsed < d.cfg.ConsecutiveSigWindow {
			if d.force.CompareAndSwap(false, true) {
				slog.Info("drain: forced (consecutive trigger)",
					"project", d.src.ProjectName(), "interval", elapsed)
			}
		}
	}
	return d.doneCh
}

// SubscribeProgress returns a channel that receives DrainProgress events
// every ProgressInterval, plus a final event when the drain finishes. The
// channel is closed after the final event.
//
// Subscribers that fail to consume in time will drop intermediate events
// (channel buffer = 4). The final event is delivered best-effort.
//
// If drain has already finished, returns a channel that immediately yields
// the final snapshot and closes.
//
// Race-safe (review P1-2): the doneCh check and progressSubs append are
// both performed under progressMu, so no subscriber can be appended after
// closeAllSubs has cleared the slice. If finish() is in flight while we
// take the lock, we'll see doneCh closed and emit the final snapshot
// directly without joining the subscriber list.
func (d *Drainer) SubscribeProgress() <-chan DrainProgress {
	ch := make(chan DrainProgress, 4)
	if d == nil {
		close(ch)
		return ch
	}

	d.progressMu.Lock()
	select {
	case <-d.doneCh:
		d.progressMu.Unlock()
		if snap := d.final.Load(); snap != nil {
			ch <- *snap // buffer=4 guarantees this is non-blocking on a fresh ch
		}
		close(ch)
		return ch
	default:
	}
	d.progressSubs = append(d.progressSubs, ch)
	d.progressMu.Unlock()
	return ch
}

// run is the drain goroutine. It evaluates AnyActive() on PollInterval,
// pushes DrainProgress on ProgressInterval, and closes doneCh once one of:
//   - force.Load() is true
//   - elapsed >= MaxWaitMinutes
//   - AnyActive returns 0 AND lastUpdate is older than IdleMinutes
//
// Goroutine exits when doneCh is closed; subscribers are closed in the same
// closeOnce.Do so SubscribeProgress callers always observe a final event.
func (d *Drainer) run() {
	pollT := time.NewTicker(d.cfg.PollInterval)
	progT := time.NewTicker(d.cfg.ProgressInterval)
	defer pollT.Stop()
	defer progT.Stop()

	maxWait := d.cfg.MaxWaitDuration
	idleNeeded := d.cfg.IdleDuration
	startedAt := time.Unix(0, d.startedAt.Load())

	emit := func(forced, timedOut bool, done bool) DrainProgress {
		count, hasPending, _ := d.src.AnyActive()
		evt := DrainProgress{
			ActiveSessions: count,
			HasPending:     hasPending,
			Elapsed:        time.Since(startedAt),
			Done:           done,
			Forced:         forced,
			TimedOut:       timedOut,
		}
		d.publish(evt)
		return evt
	}

	finish := func(forced, timedOut bool) {
		// Build the final snapshot WITHOUT publishing to subscribers yet —
		// we want a strict order:
		//   1. Store final atomically.
		//   2. Close doneCh (under closeOnce).
		//   3. Publish the Done event AND close the subscriber channels,
		//      both inside progressMu so a racing SubscribeProgress caller
		//      observes either an open subscriber list (and gets the Done
		//      event via its channel) or a closed doneCh (and gets the
		//      final snapshot from final.Load()).
		count, hasPending, _ := d.src.AnyActive()
		evt := DrainProgress{
			ActiveSessions: count,
			HasPending:     hasPending,
			Elapsed:        time.Since(startedAt),
			Done:           true,
			Forced:         forced,
			TimedOut:       timedOut,
		}
		d.final.Store(&evt)
		slog.Info("drain: complete",
			"project", d.src.ProjectName(),
			"elapsed", evt.Elapsed,
			"forced", forced,
			"timedOut", timedOut,
			"active", evt.ActiveSessions,
		)
		d.closeOnce.Do(func() {
			close(d.doneCh)
			d.publishAndCloseLocked(evt)
		})
	}

	for {
		// Fast paths first so a force/timeout never blocks on a tick.
		if d.force.Load() {
			finish(true, false)
			return
		}
		if elapsed := time.Since(startedAt); elapsed >= maxWait {
			slog.Warn("drain: timeout", "project", d.src.ProjectName(), "elapsed", elapsed)
			finish(false, true)
			return
		}

		count, _, lastUpdate := d.src.AnyActive()
		if count == 0 && time.Since(lastUpdate) >= idleNeeded {
			finish(false, false)
			return
		}

		select {
		case <-pollT.C:
			// continue loop
		case <-progT.C:
			emit(false, false, false)
		}
	}
}

// publish delivers evt to all current progress subscribers.
// Drops on full buffer (best-effort delivery for intermediate ticks).
func (d *Drainer) publish(evt DrainProgress) {
	d.progressMu.Lock()
	subs := d.progressSubs
	d.progressMu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- evt:
		default:
		}
	}
}

// publishAndCloseLocked delivers the final Done event to every current
// subscriber and then closes their channels, all under progressMu. Caller
// must hold the closeOnce guarantee (review P1-2). Performing the Done
// publish + close under the same lock guarantees that a racing
// SubscribeProgress observes consistent state: either it gets in before
// finish runs (lands in subscriber list, receives the Done event from this
// function, then sees ch closed), or it sees doneCh already closed and
// reads final.Load() directly — never both, never neither.
func (d *Drainer) publishAndCloseLocked(final DrainProgress) {
	d.progressMu.Lock()
	subs := d.progressSubs
	d.progressSubs = nil
	for _, ch := range subs {
		select {
		case ch <- final:
		default: // buffer full; subscriber lagged — final.Load() still works
		}
		close(ch)
	}
	d.progressMu.Unlock()
}
