package producer

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// sequentialWatchClient hands out a fresh FakeWatcher on every Watch call, so a
// test can close one and assert the producer establishes the next.
type sequentialWatchClient struct {
	client.Client
	mu       sync.Mutex
	watchers []*watch.FakeWatcher
}

func (f *sequentialWatchClient) Watch(_ context.Context, _ client.ObjectList, _ ...client.ListOption) (watch.Interface, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := watch.NewFake()
	f.watchers = append(f.watchers, w)
	return w, nil
}

func (f *sequentialWatchClient) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.watchers)
}

func (f *sequentialWatchClient) at(i int) *watch.FakeWatcher {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watchers[i]
}

var _ = Describe("Kubernetes watcher re-establishment", func() {
	// Regression guard for the 2026-06-13 silent-stall: a closed watch result
	// channel must trigger a re-watch, not a silent permanent exit.
	It("re-watches after the result channel closes and keeps delivering", func() {
		scheme := kruntime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		wc := &sequentialWatchClient{Client: c}

		rt := dbspruntime.NewRuntime(logr.Discard())
		sub := rt.NewSubscriber()
		sub.Subscribe("in")

		p, err := NewWatcher(Config{
			Name:      "rewatch-test",
			Client:    wc,
			SourceGVK: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			InputName: "in",
			Namespace: "default",
			Runtime:   rt,
			Logger:    logr.Discard(),
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- p.Start(ctx) }()

		// First watch established and delivering (like a real watch's initial
		// state-of-the-world ADDEDs).
		Eventually(wc.count, "2s", "10ms").Should(Equal(1))
		wc.at(0).Add(triggerEventObject("cm-a", "default"))
		var evt dbspruntime.Event
		Eventually(sub.GetChannel(), "2s", "10ms").Should(Receive(&evt))

		// Close the first watch's result channel. The pre-fix code returned nil
		// here and the producer exited permanently; having delivered an event,
		// the fixed code re-watches immediately (no flapping backoff).
		wc.at(0).Stop()

		// The producer must establish a fresh watch.
		Eventually(wc.count, "2s", "10ms").Should(BeNumerically(">=", 2))

		// The re-watched stream is live: an event on the new watcher is delivered.
		wc.at(1).Add(triggerEventObject("cm-x", "default"))
		Eventually(sub.GetChannel(), "2s", "10ms").Should(Receive(&evt))
		Expect(evt.Name).To(Equal("in"))

		// The producer keeps running until ctx is cancelled.
		Consistently(errCh, "200ms", "20ms").ShouldNot(Receive())

		cancel()
		Eventually(errCh, "2s", "10ms").Should(Receive(BeNil()))
	})
})
