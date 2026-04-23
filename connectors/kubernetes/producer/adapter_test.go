package producer

import (
	"encoding/base64"

	"k8s.io/apimachinery/pkg/runtime/schema"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// firstDoc returns the first document of a 1-element ZSet. Tests panic on
// anything else to keep assertions tight.
func firstDoc(zs zset.ZSet) datamodel.Document {
	var out datamodel.Document
	zs.Iter(func(doc datamodel.Document, _ zset.Weight) bool {
		out = doc
		return false
	})
	return out
}

var _ = Describe("Producer adapters", func() {
	It("converts add/update/delete lifecycle to zset deltas", func() {
		p := &baseProducer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

		obj := kobject.New()
		gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace("default")
		obj.SetName("app")
		kobject.SetContent(obj, map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{"replicas": int64(1)},
		})

		zs, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.Size()).To(Equal(1))

		updated := kobject.DeepCopy(obj)
		kobject.SetContent(updated, map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{"replicas": int64(2)},
		})

		zs, err = p.convertDeltaToZSet(kobject.Delta{Type: kobject.Updated, Object: updated})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.Size()).To(Equal(2))

		zs, err = p.convertDeltaToZSet(kobject.Delta{Type: kobject.Deleted, Object: updated})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.Size()).To(Equal(1))
	})

	It("suppresses noop updates", func() {
		p := &baseProducer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

		obj := kobject.New()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace("default")
		obj.SetName("cfg")
		kobject.SetContent(obj, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "cfg",
				"namespace": "default",
			},
			"data": map[string]any{"a": "1"},
		})

		_, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
		Expect(err).NotTo(HaveOccurred())

		zs, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Updated, Object: kobject.DeepCopy(obj)})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.IsZero()).To(BeTrue())
	})

	It("wraps Secrets so .data.* reads plaintext for every JSONPath form while Hash/JSON stay base64", func() {
		// Kubernetes serialises Secret values as base64 on the wire.
		// Pipeline expressions must see plaintext regardless of JSONPath
		// form (dotted, bracket, rooted) — the previous adaptor-based
		// approach only matched dotted paths and silently returned base64
		// for every other form, which is exactly how composite joins
		// access Secret fields. The wrapper also preserves base64 on the
		// serialised / hashed / logged representation so debug dumps and
		// zset identity do not leak plaintext.
		p := &baseProducer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

		username := "synapse"
		password := "s3cr3t"
		encUser := base64.StdEncoding.EncodeToString([]byte(username))
		encPass := base64.StdEncoding.EncodeToString([]byte(password))

		obj := kobject.New()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Secret"}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace("default")
		obj.SetName("db")
		kobject.SetContent(obj, map[string]any{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]any{
				"name":      "db",
				"namespace": "default",
			},
			"type": "Opaque",
			"data": map[string]any{
				"username": encUser,
				"password": encPass,
			},
		})

		zs, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.Size()).To(Equal(1))

		doc := firstDoc(zs)
		Expect(doc).NotTo(BeNil())

		// Every JSONPath form the compiler may emit must return plaintext.
		for _, path := range []string{
			"data.username",
			"data['username']",
			`data["username"]`,
			"$.data.username",
			"$.data['username']",
			`$["data"]["username"]`,
		} {
			v, err := doc.GetField(path)
			Expect(err).NotTo(HaveOccurred(), "path %q", path)
			Expect(v).To(Equal(username), "path %q", path)
		}
		pass, err := doc.GetField("data.password")
		Expect(err).NotTo(HaveOccurred())
		Expect(pass).To(Equal(password))

		// Hash / JSON / String must emit base64 so that V(2)+ zset dumps
		// and primary-key identity never surface plaintext secrets.
		raw, err := doc.MarshalJSON()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).To(ContainSubstring(encUser))
		Expect(string(raw)).To(ContainSubstring(encPass))
		Expect(string(raw)).NotTo(ContainSubstring(username))
		Expect(string(raw)).NotTo(ContainSubstring(password))
		Expect(doc.Hash()).To(ContainSubstring(encUser))
		Expect(doc.Hash()).NotTo(ContainSubstring(password))

		// Caller's original object must NOT be mutated — we deep-copy
		// before wrapping so the informer cache stays intact.
		origData := obj.UnstructuredContent()["data"].(map[string]any)
		Expect(origData["username"]).To(Equal(encUser))
		Expect(origData["password"]).To(Equal(encPass))
	})

	It("leaves non-Secret objects as plain Unstructured", func() {
		// Guard: adaptor wrap is scoped to Secrets only.
		p := &baseProducer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

		obj := kobject.New()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace("default")
		obj.SetName("cfg")
		encoded := base64.StdEncoding.EncodeToString([]byte("plain"))
		kobject.SetContent(obj, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "cfg", "namespace": "default"},
			"data":       map[string]any{"looks-like-base64": encoded},
		})

		zs, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
		Expect(err).NotTo(HaveOccurred())

		doc := firstDoc(zs)
		Expect(doc).NotTo(BeNil())
		v, err := doc.GetField("data.looks-like-base64")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(encoded))
	})

	It("uses cached object on delete tombstones", func() {
		p := &baseProducer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

		obj := kobject.New()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace("default")
		obj.SetName("cfg")
		kobject.SetContent(obj, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":            "cfg",
				"namespace":       "default",
				"resourceVersion": "10",
			},
			"data": map[string]any{"a": "1"},
		})

		zsAdd, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
		Expect(err).NotTo(HaveOccurred())
		Expect(zsAdd.Size()).To(Equal(1))

		tombstone := kobject.DeepCopy(obj)
		kobject.SetContent(tombstone, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":            "cfg",
				"namespace":       "default",
				"resourceVersion": "11",
			},
			"data": map[string]any{"a": "1"},
		})

		zsDel, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Deleted, Object: tombstone})
		Expect(err).NotTo(HaveOccurred())
		Expect(zsDel.Size()).To(Equal(1))

		combined := zsAdd.Add(zsDel)
		Expect(combined.IsZero()).To(BeTrue())
	})
})
