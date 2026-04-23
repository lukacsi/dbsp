package secretdoc_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel/secretdoc"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

func TestSecretDoc(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Secretdoc Suite")
}

var _ = Describe("Secret document wrapper", func() {
	encoded := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	It("decodes .data.* on GetField for every JSONPath form", func() {
		base := unstructured.New(map[string]any{
			"kind": "Secret",
			"data": map[string]any{
				"username": encoded("synapse"),
				"password": encoded("s3cr3t"),
			},
		}, nil)
		doc := secretdoc.New(base)

		forms := []string{
			"data.username",
			"data['username']",
			`data["username"]`,
			"$.data.username",
			"$.data['username']",
			`$["data"]["username"]`,
		}
		for _, path := range forms {
			v, err := doc.GetField(path)
			Expect(err).NotTo(HaveOccurred(), "path %q", path)
			Expect(v).To(Equal("synapse"), "path %q", path)
		}
	})

	It("leaves the raw data map untouched so callers can iterate", func() {
		base := unstructured.New(map[string]any{
			"data": map[string]any{"k": encoded("v")},
		}, nil)
		doc := secretdoc.New(base)

		v, err := doc.GetField("data")
		Expect(err).NotTo(HaveOccurred())
		m, ok := v.(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(m["k"]).To(Equal(encoded("v")))
	})

	It("does not touch non-.data fields", func() {
		base := unstructured.New(map[string]any{
			"metadata": map[string]any{"name": encoded("not-decoded")},
		}, nil)
		doc := secretdoc.New(base)

		v, err := doc.GetField("metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(encoded("not-decoded")))
	})

	It("returns the raw string when a .data value is not valid base64", func() {
		base := unstructured.New(map[string]any{
			"data": map[string]any{"broken": "not base64 !!!"},
		}, nil)
		doc := secretdoc.New(base)

		v, err := doc.GetField("data.broken")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("not base64 !!!"))
	})

	It("re-encodes .data.* string writes to base64", func() {
		base := unstructured.New(map[string]any{
			"data": map[string]any{},
		}, nil)
		doc := secretdoc.New(base)

		Expect(doc.SetField("data.password", "s3cr3t")).To(Succeed())

		v, err := base.GetField("data.password")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(encoded("s3cr3t")))

		back, err := doc.GetField("data.password")
		Expect(err).NotTo(HaveOccurred())
		Expect(back).To(Equal("s3cr3t"))
	})

	It("keeps Hash/String/MarshalJSON at the raw base64 representation", func() {
		base := unstructured.New(map[string]any{
			"kind": "Secret",
			"data": map[string]any{"password": encoded("s3cr3t")},
		}, nil)
		doc := secretdoc.New(base)

		Expect(doc.Hash()).To(Equal(base.Hash()))
		Expect(doc.String()).To(Equal(base.String()))

		j, err := json.Marshal(doc)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(j)).To(ContainSubstring(encoded("s3cr3t")))
		Expect(string(j)).NotTo(ContainSubstring("s3cr3t\""))
	})

	It("preserves wrapping across Copy/New/Merge", func() {
		base := unstructured.New(map[string]any{
			"data": map[string]any{"k": encoded("v")},
		}, nil)
		doc := secretdoc.New(base)

		cp := doc.Copy()
		v, err := cp.GetField("data.k")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("v"))

		fresh := doc.New()
		Expect(fresh.SetField("data.k", "new")).To(Succeed())
		got, err := fresh.GetField("data.k")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("new"))

		other := unstructured.New(map[string]any{
			"data": map[string]any{"k2": encoded("v2")},
		}, nil)
		merged := doc.Merge(secretdoc.New(other))
		v2, err := merged.GetField("data.k2")
		Expect(err).NotTo(HaveOccurred())
		Expect(v2).To(Equal("v2"))
	})
})
