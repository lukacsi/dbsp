package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	"github.com/l7mp/dbsp/engine/circuit"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

const (
	watcherComponentName   = "operator-controller.watcher"
	updaterComponentName   = "operator-controller.updater"
	processorComponentName = "operator-controller.processor"

	stopWaitTimeout = 5 * time.Second

	// Failed operator initializations are retried with exponential backoff:
	// transient startup conditions (API server discovery not ready, CRDs not
	// yet registered) must never permanently wedge an operator.
	retryBaseDelay = time.Second
	retryMaxDelay  = 2 * time.Minute
)

var (
	operatorInputTopic  = circuit.InputTopic("operator-controller", "operator")
	operatorStatusTopic = circuit.OutputTopic("operator-controller", "operator-status")
)

var operatorGVK = opv1a1.GroupVersion.WithKind("Operator")

// OperatorController reconciles Operator CRs using DBSP runtime components.
//
// The controller wires:
//  1. a Kubernetes Watcher producer for Operator CRDs,
//  2. a processor that manages in-memory Operators and emits status updates,
//  3. a Kubernetes Updater consumer that writes Operator status.
type OperatorController struct {
	k8sRuntime  *k8sruntime.Runtime
	dbspRuntime *dbspruntime.Runtime
	processor   *operatorProcessor

	log logr.Logger
}

// NewOperatorController creates the full OperatorController runtime wiring.
func NewOperatorController(cfg k8sruntime.Config) (*OperatorController, error) {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("operator-controller")

	k8srt, err := k8sruntime.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes runtime: %w", err)
	}

	dbsprt := dbspruntime.NewRuntime(log.WithName("dbsp-runtime"))

	watcher, err := k8sproducer.NewWatcher(k8sproducer.Config{
		Name:      watcherComponentName,
		Client:    k8srt.GetClient(),
		SourceGVK: operatorGVK,
		InputName: operatorInputTopic,
		Runtime:   dbsprt,
		Logger:    log,
	})
	if err != nil {
		return nil, fmt.Errorf("create operator watcher: %w", err)
	}

	updater, err := k8sconsumer.NewUpdater(k8sconsumer.Config{
		Name:       updaterComponentName,
		Client:     k8srt.GetClient(),
		OutputName: operatorStatusTopic,
		TargetGVK:  operatorGVK,
		Runtime:    dbsprt,
		Logger:     log,
	})
	if err != nil {
		return nil, fmt.Errorf("create operator status updater: %w", err)
	}

	proc, err := newOperatorProcessor(processorConfig{
		Name:        processorComponentName,
		InputTopic:  operatorInputTopic,
		OutputTopic: operatorStatusTopic,
		Runtime:     dbsprt,
		K8sRuntime:  k8srt,
		Logger:      log,
	})
	if err != nil {
		return nil, fmt.Errorf("create operator processor: %w", err)
	}

	if err := dbsprt.Add(watcher); err != nil {
		return nil, fmt.Errorf("register watcher: %w", err)
	}
	if err := dbsprt.Add(proc); err != nil {
		return nil, fmt.Errorf("register processor: %w", err)
	}
	if err := dbsprt.Add(updater); err != nil {
		return nil, fmt.Errorf("register updater: %w", err)
	}

	return &OperatorController{
		k8sRuntime:  k8srt,
		dbspRuntime: dbsprt,
		processor:   proc,
		log:         log,
	}, nil
}

// Start runs the Kubernetes runtime and the DBSP runtime until ctx is cancelled.
func (c *OperatorController) Start(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return c.k8sRuntime.Start(gctx)
	})

	g.Go(func() error {
		return c.dbspRuntime.Start(gctx)
	})

	return g.Wait()
}

// GetClient returns the composite Kubernetes client used by the operator controller.
func (c *OperatorController) GetClient() client.Client {
	return c.k8sRuntime.GetClient()
}

type processorConfig struct {
	Name        string
	InputTopic  string
	OutputTopic string
	Runtime     *dbspruntime.Runtime
	K8sRuntime  *k8sruntime.Runtime
	Logger      logr.Logger
}

type managedOperator struct {
	op     *Operator
	spec   opv1a1.OperatorSpec
	cancel context.CancelFunc
	done   chan struct{}
}

type pendingRetry struct {
	spec    *opv1a1.Operator
	attempt int
	timer   *time.Timer
}

// operatorProcessor is a DBSP runtime processor that translates Operator CR deltas
// into operator lifecycle operations and status update events.
type operatorProcessor struct {
	*dbspruntime.BaseProcessor

	outputTopic string
	k8srt       *k8sruntime.Runtime

	mu        sync.Mutex
	ctx       context.Context
	operators map[types.NamespacedName]*managedOperator

	// upsertMu serializes operator creation/deletion between Consume and retry
	// timers. Deliberate head-of-line blocking: a slow operator stop can stall
	// other operators' lifecycle work for up to stopWaitTimeout.
	upsertMu sync.Mutex
	retryMu  sync.Mutex
	retries  map[types.NamespacedName]*pendingRetry

	log logr.Logger
}

var _ dbspruntime.Processor = (*operatorProcessor)(nil)

func newOperatorProcessor(cfg processorConfig) (*operatorProcessor, error) {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	sub := cfg.Runtime.NewSubscriber()
	base, err := dbspruntime.NewBaseProcessor(dbspruntime.BaseProcessorConfig{
		Name:          cfg.Name,
		Publisher:     cfg.Runtime.NewPublisher(),
		Subscriber:    sub,
		ErrorReporter: cfg.Runtime,
		Logger:        log.WithName("processor"),
		Topics:        []string{cfg.InputTopic},
	})
	if err != nil {
		return nil, err
	}

	return &operatorProcessor{
		BaseProcessor: base,
		outputTopic:   cfg.OutputTopic,
		k8srt:         cfg.K8sRuntime,
		operators:     map[types.NamespacedName]*managedOperator{},
		retries:       map[types.NamespacedName]*pendingRetry{},
		log:           log.WithName("processor"),
	}, nil
}

func (p *operatorProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	p.ctx = ctx
	p.mu.Unlock()

	defer p.stopAllOperators()

	return p.Run(ctx, p)
}

func (p *operatorProcessor) Consume(ctx context.Context, in dbspruntime.Event) error {
	type opAction struct {
		op     *opv1a1.Operator
		weight zset.Weight
	}

	actions := make([]opAction, 0, in.Data.Size())
	for _, entry := range in.Data.Entries() {
		op, err := decodeOperator(entry.Document)
		if err != nil {
			return fmt.Errorf("decode operator event: %w", err)
		}
		actions = append(actions, opAction{op: op, weight: entry.Weight})
	}

	// An update arrives as a {-old, +new} pair: the delete side of a pair must
	// not tear down retry state, otherwise the status-update echo of a failed
	// upsert would reset the backoff on every round.
	upserted := make(map[types.NamespacedName]bool, len(actions))
	for _, action := range actions {
		if action.weight > 0 {
			upserted[client.ObjectKeyFromObject(action.op)] = true
		}
	}

	for _, action := range actions {
		if action.weight < 0 {
			key := client.ObjectKeyFromObject(action.op)
			if upserted[key] {
				continue
			}
			p.upsertMu.Lock()
			p.cancelRetry(key)
			p.deleteOperator(key)
			p.upsertMu.Unlock()
		}
	}

	// A failed upsert must not abort the rest of the batch, and must not
	// discard the event: schedule a retry so transient conditions (API server
	// discovery not ready, CRDs not yet registered) cannot wedge the operator.
	var errs error
	for _, action := range actions {
		if action.weight > 0 {
			errs = errorsJoin(errs, p.tryUpsert(ctx, action.op))
		}
	}

	return errs
}

// tryUpsert attempts the upsert; on failure it publishes a failed status and
// schedules a retry. A pending retry for an unchanged spec resumes its attempt
// count (the event was just a status echo), a changed spec restarts the
// backoff. Holding upsertMu across pop+upsert guarantees a pending retry can
// never resurrect a spec that a newer event has superseded.
func (p *operatorProcessor) tryUpsert(ctx context.Context, spec *opv1a1.Operator) error {
	p.upsertMu.Lock()

	// A MODIFIED event carrying the same spec as the already-running operator
	// is a status echo (our own publishStatus write re-triggering the watcher).
	// Recreating the operator would re-list every source as ADDED and re-publish
	// status, looping. Skip when the running spec is unchanged.
	if p.runningSpecUnchanged(spec) {
		p.upsertMu.Unlock()
		return nil
	}

	attempt := 1
	if pending := p.popRetry(client.ObjectKeyFromObject(spec)); pending != nil &&
		apiequality.Semantic.DeepEqual(pending.spec.Spec, spec.Spec) {
		attempt = pending.attempt + 1
	}
	err := p.upsertOperator(ctx, spec)
	p.upsertMu.Unlock()
	if err == nil {
		return nil
	}

	status := failedOperatorStatus(spec, err)
	if pubErr := p.publishStatus(spec, status); pubErr != nil {
		err = errorsJoin(err, fmt.Errorf("publish failed status: %w", pubErr))
	}

	if ctx.Err() == nil {
		p.scheduleRetry(spec, attempt)
	}

	return err
}

// runningSpecUnchanged reports whether an operator for the key is already
// running with a spec identical to the incoming one. Must be called under
// upsertMu (acquires mu in the consistent upsertMu-then-mu order).
func (p *operatorProcessor) runningSpecUnchanged(spec *opv1a1.Operator) bool {
	key := client.ObjectKeyFromObject(spec)
	p.mu.Lock()
	entry, ok := p.operators[key]
	p.mu.Unlock()
	return ok && apiequality.Semantic.DeepEqual(entry.spec, spec.Spec)
}

func (p *operatorProcessor) scheduleRetry(spec *opv1a1.Operator, attempt int) {
	if attempt < 1 {
		attempt = 1
	}
	delay := retryMaxDelay
	if attempt <= 8 {
		delay = retryBaseDelay << (attempt - 1) // 1s, 2s, ... 128s
		if delay > retryMaxDelay {
			delay = retryMaxDelay
		}
	}

	key := client.ObjectKeyFromObject(spec)

	p.retryMu.Lock()
	defer p.retryMu.Unlock()
	if old, ok := p.retries[key]; ok {
		old.timer.Stop()
	}
	entry := &pendingRetry{spec: spec, attempt: attempt}
	entry.timer = time.AfterFunc(delay, func() { p.retryUpsert(key) })
	p.retries[key] = entry

	p.log.Info("scheduling operator retry", "operator", key.String(), "attempt", attempt,
		"delay", delay.String())
}

func (p *operatorProcessor) retryUpsert(key types.NamespacedName) {
	p.mu.Lock()
	ctx := p.ctx
	p.mu.Unlock()
	if ctx == nil || ctx.Err() != nil {
		return
	}

	p.upsertMu.Lock()

	// Re-check after possibly blocking on upsertMu: a shutdown may have
	// completed its sweep in the meantime.
	if ctx.Err() != nil {
		p.upsertMu.Unlock()
		return
	}

	// Validate under upsertMu: an event processed since this timer fired has
	// cancelled the entry, and a superseded spec must never be resurrected.
	entry := p.popRetry(key)
	if entry == nil {
		p.upsertMu.Unlock()
		return
	}

	err := p.upsertOperator(ctx, entry.spec)
	// Publish and re-schedule outside the critical section (as tryUpsert
	// does): a blocking publish must not stall other operators' lifecycle.
	p.upsertMu.Unlock()

	if err == nil {
		p.log.Info("operator initialized after retry", "operator", key.String(),
			"attempt", entry.attempt)
		return
	}

	status := failedOperatorStatus(entry.spec, err)
	if pubErr := p.publishStatus(entry.spec, status); pubErr != nil {
		err = errorsJoin(err, fmt.Errorf("publish failed status: %w", pubErr))
	}
	if ctx.Err() == nil {
		p.scheduleRetry(entry.spec, entry.attempt+1)
	}

	p.HandleError(fmt.Errorf("retry %d for operator %q: %w", entry.attempt, key.String(), err))
}

func (p *operatorProcessor) cancelRetry(key types.NamespacedName) {
	p.popRetry(key)
}

// popRetry removes and returns the pending retry for the key, stopping its
// timer; it returns nil if none is pending.
func (p *operatorProcessor) popRetry(key types.NamespacedName) *pendingRetry {
	p.retryMu.Lock()
	defer p.retryMu.Unlock()
	entry, ok := p.retries[key]
	if !ok {
		return nil
	}
	entry.timer.Stop()
	delete(p.retries, key)
	return entry
}

func (p *operatorProcessor) cancelAllRetries() {
	p.retryMu.Lock()
	defer p.retryMu.Unlock()
	for key, entry := range p.retries {
		entry.timer.Stop()
		delete(p.retries, key)
	}
}

func (p *operatorProcessor) upsertOperator(ctx context.Context, spec *opv1a1.Operator) error {
	key := client.ObjectKeyFromObject(spec)

	p.deleteOperator(key)

	op, err := New(spec.GetName(), Config{
		Spec:       spec.Spec,
		K8sRuntime: p.k8srt,
		Logger:     p.log.WithValues("operator", spec.GetName()),
	})
	if err != nil {
		return fmt.Errorf("create operator %q: %w", spec.GetName(), err)
	}

	opCtx, cancel := context.WithCancel(ctx)
	entry := &managedOperator{op: op, spec: spec.Spec, cancel: cancel, done: make(chan struct{})}

	p.mu.Lock()
	p.operators[key] = entry
	p.mu.Unlock()

	go func() {
		defer close(entry.done)
		if startErr := op.Start(opCtx); startErr != nil && opCtx.Err() == nil {
			p.HandleError(fmt.Errorf("operator %q exited with error: %w", op.GetName(), startErr))
		}
	}()

	if err := p.publishStatus(spec, op.GetStatus(spec.GetGeneration())); err != nil {
		return fmt.Errorf("publish status for %q: %w", spec.GetName(), err)
	}

	return nil
}

func (p *operatorProcessor) deleteOperator(key types.NamespacedName) {
	p.mu.Lock()
	entry, ok := p.operators[key]
	if ok {
		delete(p.operators, key)
	}
	p.mu.Unlock()

	if !ok {
		return
	}

	entry.op.UnregisterGVKs()
	if entry.cancel != nil {
		entry.cancel()
	}

	if entry.done != nil {
		select {
		case <-entry.done:
		case <-time.After(stopWaitTimeout):
			p.log.V(1).Info("timeout while waiting for operator to stop", "operator", key.String())
		}
	}
}

func (p *operatorProcessor) stopAllOperators() {
	// Exclude in-flight retry upserts so the sweep cannot be raced by a timer
	// that already passed its ctx check.
	p.upsertMu.Lock()
	defer p.upsertMu.Unlock()

	p.cancelAllRetries()

	p.mu.Lock()
	keys := make([]types.NamespacedName, 0, len(p.operators))
	for key := range p.operators {
		keys = append(keys, key)
	}
	p.mu.Unlock()

	for _, key := range keys {
		p.deleteOperator(key)
	}
}

func (p *operatorProcessor) publishStatus(spec *opv1a1.Operator, status opv1a1.OperatorStatus) error {
	obj := spec.DeepCopy()
	obj.Status = status

	unstructuredObj, err := apiruntime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return fmt.Errorf("convert operator status to unstructured: %w", err)
	}

	zs := zset.New()
	zs.Insert(dbspunstructured.New(unstructuredObj, nil), 1)

	return p.Publish(dbspruntime.Event{Name: p.outputTopic, Data: zs})
}

func decodeOperator(doc any) (*opv1a1.Operator, error) {
	udoc, ok := doc.(*dbspunstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unsupported document type %T", doc)
	}

	obj := &opv1a1.Operator{}
	if err := apiruntime.DefaultUnstructuredConverter.FromUnstructured(udoc.Fields(), obj); err != nil {
		return nil, fmt.Errorf("convert unstructured document: %w", err)
	}

	if obj.GetName() == "" {
		return nil, fmt.Errorf("operator object is missing metadata.name")
	}

	return obj, nil
}

// failedOperatorStatus builds a Ready=False status on top of the operator's
// existing conditions: SetStatusCondition keeps LastTransitionTime stable for
// an unchanged condition, so repeated failures publish byte-identical status
// and the API server write becomes a no-op instead of an endless watch echo.
func failedOperatorStatus(op *opv1a1.Operator, err error) opv1a1.OperatorStatus {
	status := opv1a1.OperatorStatus{
		Conditions: append([]metav1.Condition{}, op.Status.Conditions...),
	}
	status.LastErrors = []string{err.Error()}

	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               string(opv1a1.OperatorConditionReady),
		Status:             metav1.ConditionFalse,
		Reason:             string(opv1a1.OperatorReasonNotReady),
		ObservedGeneration: op.GetGeneration(),
		LastTransitionTime: metav1.Now(),
		Message:            "failed to initialize operator",
	})

	return status
}

func errorsJoin(a, b error) error {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return errors.Join(a, b)
}
