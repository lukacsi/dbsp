package operator

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// normalizeOperator roundtrips an Operator through the unstructured converter
// so its raw JSON fields take the same canonical form as event-decoded specs.
func normalizeOperator(op *opv1a1.Operator) *opv1a1.Operator {
	u, err := apiruntime.DefaultUnstructuredConverter.ToUnstructured(op)
	Expect(err).NotTo(HaveOccurred())
	out := &opv1a1.Operator{}
	Expect(apiruntime.DefaultUnstructuredConverter.FromUnstructured(u, out)).To(Succeed())
	return out
}

// makeNativeControllerSpec returns a controller spec that cannot be created
// in headless mode: native kinds have no RESTMapping without a REST config,
// which is the same failure mode as an API server that is not yet reachable.
func makeNativeControllerSpec(name string) opv1a1.Controller {
	apps := "apps"
	return opv1a1.Controller{
		Name: name,
		Sources: []opv1a1.Source{{
			Resource: opv1a1.Resource{Group: &apps, Kind: "Deployment"},
			Type:     opv1a1.Lister,
		}},
		Pipeline: rawJSON(`[
			{"@project":{
				"metadata":{"name":"$.metadata.name","namespace":"$.metadata.namespace"}
			}}
		]`),
		Targets: []opv1a1.Target{{Resource: opv1a1.Resource{Kind: "Bar"}, Type: opv1a1.Updater}},
	}
}

func pendingRetryFor(p *operatorProcessor, key types.NamespacedName) func() bool {
	return func() bool {
		p.retryMu.Lock()
		defer p.retryMu.Unlock()
		_, ok := p.retries[key]
		return ok
	}
}

func managedOperatorFor(p *operatorProcessor, key types.NamespacedName) func() bool {
	return func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		_, ok := p.operators[key]
		return ok
	}
}

var _ = Describe("OperatorController retry", func() {
	var (
		oc     *OperatorController
		ctx    context.Context
		cancel context.CancelFunc
		errCh  chan error
	)

	BeforeEach(func() {
		old := operatorGVK
		operatorGVK = viewv1a1.GroupVersionKind("dcontroller", "Operator")
		DeferCleanup(func() { operatorGVK = old })

		cfg := k8sruntime.Config{Logger: logr.Discard(), RESTConfig: nil}
		var err error
		oc, err = NewOperatorController(cfg)
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel = context.WithCancel(context.Background())
		errCh = make(chan error, 1)
		go func() { errCh <- oc.Start(ctx) }()
	})

	AfterEach(func() {
		cancel()
		Eventually(errCh, operatorTestTimeout).Should(Receive(BeNil()))
	})

	It("schedules a retry instead of discarding the event when operator creation fails", func() {
		opCR := makeOperatorCR("failing-op", makeNativeControllerSpec("native-copy"))
		key := types.NamespacedName{Name: opCR.GetName()}

		Expect(oc.k8sRuntime.GetViewCache().Add(toViewOperatorObject("dcontroller", opCR))).To(Succeed())

		Eventually(pendingRetryFor(oc.processor, key), operatorTestTimeout, operatorTestInterval).
			Should(BeTrue(), "a failed upsert must leave a retry pending")
		Expect(managedOperatorFor(oc.processor, key)()).To(BeFalse())
	})

	It("cancels the pending retry when the operator CR is deleted", func() {
		opCR := makeOperatorCR("deleted-op", makeNativeControllerSpec("native-copy"))
		key := types.NamespacedName{Name: opCR.GetName()}
		viewOp := toViewOperatorObject("dcontroller", opCR)

		Expect(oc.k8sRuntime.GetViewCache().Add(viewOp)).To(Succeed())
		Eventually(pendingRetryFor(oc.processor, key), operatorTestTimeout, operatorTestInterval).
			Should(BeTrue())

		Expect(oc.k8sRuntime.GetViewCache().Delete(viewOp)).To(Succeed())
		Eventually(pendingRetryFor(oc.processor, key), operatorTestTimeout, operatorTestInterval).
			Should(BeFalse(), "deletion must cancel the pending retry")
	})

	It("installs the operator when a scheduled retry succeeds", func() {
		// Simulate the recovery half of the startup race: the first upsert
		// failed and scheduled a retry, and by the time the timer fires the
		// transient condition has cleared (here: the spec is creatable).
		opCR := makeOperatorCR("recovering-op", makeSimpleControllerSpec("retry-copy", "Foo", "Bar"))
		key := types.NamespacedName{Name: opCR.GetName()}

		oc.processor.scheduleRetry(opCR, 1)

		Eventually(managedOperatorFor(oc.processor, key), operatorTestTimeout, operatorTestInterval).
			Should(BeTrue(), "the retry timer must install the operator")
		Expect(pendingRetryFor(oc.processor, key)()).To(BeFalse())
	})

	It("resumes the backoff instead of resetting it when an unchanged spec is re-delivered", func() {
		// A failed-status write echoes back as an update event; the attempt
		// counter must carry over or the backoff degenerates to the base delay.
		opCR := makeOperatorCR("echo-op", makeNativeControllerSpec("native-copy"))
		key := types.NamespacedName{Name: opCR.GetName()}

		// Simulate an already-advanced backoff: attempt 3 pending (4s timer).
		// The stored spec must be in event-decoded canonical form, as it would
		// be in production where every pending entry comes from a decoded event.
		oc.processor.scheduleRetry(normalizeOperator(opCR), 3)

		// The echoed event re-delivers the same spec through Consume.
		Expect(oc.k8sRuntime.GetViewCache().Add(toViewOperatorObject("dcontroller", opCR))).To(Succeed())

		attemptOf := func() int {
			oc.processor.retryMu.Lock()
			defer oc.processor.retryMu.Unlock()
			if entry, ok := oc.processor.retries[key]; ok {
				return entry.attempt
			}
			return 0
		}

		// The attempt count must exceed the pending 3 well before timers could
		// advance there naturally (1+2+4 = 7s of timer fires). The status-write
		// echo may consume one extra attempt before the published status
		// stabilizes byte-identically.
		Eventually(attemptOf, operatorTestTimeout, operatorTestInterval).Should(BeNumerically(">=", 4),
			"the re-delivered unchanged spec must resume the pending attempt count")

		// Convergence: once the failed status is stable, echoes stop and the
		// attempt only advances by its own (8s+) timer — not within this window.
		stable := attemptOf()
		Consistently(attemptOf, 1500*time.Millisecond, operatorTestInterval).Should(Equal(stable),
			"the status echo loop must converge instead of churning")
	})

	It("backs off exponentially while the failure persists", func() {
		opCR := makeOperatorCR("backoff-op", makeNativeControllerSpec("native-copy"))
		key := types.NamespacedName{Name: opCR.GetName()}

		Expect(oc.k8sRuntime.GetViewCache().Add(toViewOperatorObject("dcontroller", opCR))).To(Succeed())

		attemptOf := func() int {
			oc.processor.retryMu.Lock()
			defer oc.processor.retryMu.Unlock()
			if entry, ok := oc.processor.retries[key]; ok {
				return entry.attempt
			}
			return 0
		}

		Eventually(attemptOf, operatorTestTimeout, operatorTestInterval).Should(BeNumerically(">=", 1))
		// The first retry fires after retryBaseDelay (1s) and fails again,
		// re-scheduling with the next attempt number.
		Eventually(attemptOf, 3*time.Second, operatorTestInterval).Should(BeNumerically(">=", 2))
	})
})
