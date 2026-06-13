package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crevent "sigs.k8s.io/controller-runtime/pkg/event"
	crpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"

	kpredicate "github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

type baseProducer struct {
	*dbspruntime.BaseProducer

	client    client.WithWatch
	sourceGVK schema.GroupVersionKind
	inputName string

	listOpts   []client.ListOption
	predicates []crpredicate.TypedPredicate[client.Object]

	sourceCache map[schema.GroupVersionKind]*store.Store

	log logr.Logger
}

func newBase(cfg Config, producerType string) (*baseProducer, error) {
	if cfg.Runtime == nil {
		return nil, fmt.Errorf("producer: runtime is required")
	}
	if cfg.Client == nil {
		return nil, fmt.Errorf("producer: client is required")
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	inputName := cfg.InputName
	base, err := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          cfg.Name,
		Publisher:     cfg.Runtime.NewPublisher(),
		ErrorReporter: cfg.Runtime,
		Logger:        log.WithName(producerType).WithValues("topic", inputName),
		Topics:        []string{inputName},
	})
	if err != nil {
		return nil, err
	}

	p := &baseProducer{
		BaseProducer: base,
		client:       cfg.Client,
		sourceGVK:    cfg.SourceGVK,
		inputName:    inputName,
		sourceCache:  map[schema.GroupVersionKind]*store.Store{},
		log:          log.WithName(producerType).WithValues("topic", inputName),
	}

	if cfg.Namespace != "" {
		p.listOpts = append(p.listOpts, client.InNamespace(cfg.Namespace))
	}

	if cfg.LabelSelector != nil {
		lp, err := kpredicate.FromLabelSelector(*cfg.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("producer: invalid label selector: %w", err)
		}
		p.predicates = append(p.predicates, lp)

		sel, err := v1.LabelSelectorAsSelector(cfg.LabelSelector)
		if err == nil {
			p.listOpts = append(p.listOpts, client.MatchingLabelsSelector{Selector: sel})
		}
	}

	if cfg.Predicate != nil {
		pred, err := kpredicate.FromPredicate(*cfg.Predicate)
		if err != nil {
			return nil, fmt.Errorf("producer: invalid predicate: %w", err)
		}
		p.predicates = append(p.predicates, pred)
	}

	if cfg.Namespace != "" {
		p.predicates = append(p.predicates, kpredicate.FromNamespace(cfg.Namespace))
	}

	return p, nil
}

// watchRetryDelay backs off before retrying a watch that failed to establish
// (API server transiently unreachable). A cleanly closed watch channel
// re-watches immediately.
const watchRetryDelay = 5 * time.Second

// start runs the producer's watch loop, re-establishing the watch whenever its
// result channel closes. API server / load-balancer idle timeouts, expired
// resourceVersions (410 Gone), and network blips all close the channel WITHOUT
// an error; the previous single-shot implementation returned nil on that close
// and exited silently, leaving the operator permanently deaf to all events
// while the process stayed alive and healthy-looking (silent-stall incident
// 2026-06-13). Re-calling Watch re-delivers the current objects as ADDED, so
// the engine catches up on adds/updates missed during the gap. Only ctx
// cancellation stops the loop.
//
// KNOWN LIMITATION: objects DELETED during the reconnect gap are not replayed
// as DELETED (a fresh watch only re-adds what currently exists), so the engine
// retains a stale entry until that object next changes. A fully correct
// catch-up would List() on reconnect and synthesize deletions by diffing — a
// larger change deferred for now; far preferable to the permanent-deafness bug.
func (p *baseProducer) start(ctx context.Context, onEvent func(context.Context, watch.Event) error) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		w, err := p.client.Watch(ctx, p.newListObject(), p.listOpts...)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.HandleError(fmt.Errorf("producer: watch failed: %w", err))
			if sleepInterrupted(ctx, watchRetryDelay) {
				return nil
			}
			continue
		}

		p.log.V(2).Info("watch started")
		stop, gotEvent := p.consumeWatch(ctx, w, onEvent)
		if stop {
			return nil
		}
		p.log.V(2).Info("watch channel closed; re-establishing")

		// A watch that established but closed without delivering anything is a
		// flapping connection (or an empty result set); back off so a sustained
		// accept-then-drop does not become a hot reconnect loop. A normal watch
		// always delivers the initial ADDEDs, so this never delays real re-syncs.
		if !gotEvent && sleepInterrupted(ctx, watchRetryDelay) {
			return nil
		}
	}
}

// consumeWatch drains a single watch until its result channel closes or ctx is
// cancelled. stop is true only when the producer should exit (ctx cancelled);
// a closed channel returns stop=false so start re-establishes the watch.
// gotEvent reports whether any event was received before the channel closed.
func (p *baseProducer) consumeWatch(ctx context.Context, w watch.Interface, onEvent func(context.Context, watch.Event) error) (stop, gotEvent bool) {
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return true, gotEvent
		case evt, ok := <-w.ResultChan():
			if !ok {
				return false, gotEvent
			}
			gotEvent = true
			if err := onEvent(ctx, evt); err != nil {
				p.HandleError(err)
			}
		}
	}
}

// sleepInterrupted waits for d or until ctx is cancelled; it returns true if
// ctx was cancelled (the caller should stop).
func sleepInterrupted(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(d):
		return false
	}
}

func (p *baseProducer) allowEvent(t watch.EventType, oldObj, newObj *unstructured.Unstructured) bool {
	if len(p.predicates) == 0 {
		return true
	}

	for _, pred := range p.predicates {
		var ok bool
		switch t {
		case watch.Added:
			ok = pred.Create(crevent.TypedCreateEvent[client.Object]{Object: newObj})
		case watch.Modified:
			if oldObj == nil {
				ok = true
			} else {
				ok = pred.Update(crevent.TypedUpdateEvent[client.Object]{ObjectOld: oldObj, ObjectNew: newObj})
			}
		case watch.Deleted:
			ok = pred.Delete(crevent.TypedDeleteEvent[client.Object]{Object: newObj})
		default:
			ok = false
		}
		if !ok {
			return false
		}
	}

	return true
}

func (p *baseProducer) allowObject(obj *unstructured.Unstructured) bool {
	if len(p.predicates) == 0 {
		return true
	}

	for _, pred := range p.predicates {
		if !pred.Create(crevent.TypedCreateEvent[client.Object]{Object: obj}) {
			return false
		}
	}

	return true
}

func (p *baseProducer) listSnapshot(ctx context.Context) (zset.ZSet, error) {
	list := p.newListObject()
	if err := p.client.List(ctx, list, p.listOpts...); err != nil {
		return zset.New(), fmt.Errorf("producer: list failed: %w", err)
	}

	zs := zset.New()
	for i := range list.Items {
		obj := list.Items[i].DeepCopy()
		if !p.allowObject(obj) {
			continue
		}
		zs.Insert(toDocument(obj), 1)
	}

	return zs, nil
}

func (p *baseProducer) newListObject() *unstructured.UnstructuredList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(p.sourceGVK)
	return list
}
