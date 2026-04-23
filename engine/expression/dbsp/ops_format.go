package dbsp

import (
	"encoding/json"
	"fmt"
	"sort"

	yaml "go.yaml.in/yaml/v3"

	"github.com/l7mp/dbsp/engine/expression"
)

// yamlExpr implements @yaml — evaluates its operand (typically a @dict or
// @list) and marshals the resulting Go value to a YAML string. Map keys are
// sorted recursively so the output is deterministic (important for hashing
// and diffing).
type yamlExpr struct {
	operand Expression
}

func (e *yamlExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if e.operand == nil {
		return "", nil
	}
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	node, err := toSortedYAMLNode(value)
	if err != nil {
		return nil, fmt.Errorf("@yaml: %w", err)
	}
	out, err := yaml.Marshal(node)
	if err != nil {
		return nil, fmt.Errorf("@yaml: %w", err)
	}
	s := string(out)
	ctx.Logger().V(8).Info("eval", "op", "@yaml", "result", s)
	return s, nil
}

func (e *yamlExpr) String() string { return fmt.Sprintf("@yaml(%v)", e.operand) }

func (e *yamlExpr) MarshalJSON() ([]byte, error)  { return marshalUnaryOp("@yaml", e.operand) }
func (e *yamlExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// jsonExpr implements @json — evaluates its operand and marshals the
// resulting Go value to a compact JSON string. encoding/json sorts
// map[string]any keys, so output is deterministic by default.
type jsonExpr struct {
	operand Expression
}

func (e *jsonExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if e.operand == nil {
		return "", nil
	}
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("@json: %w", err)
	}
	s := string(out)
	ctx.Logger().V(8).Info("eval", "op", "@json", "result", s)
	return s, nil
}

func (e *jsonExpr) String() string { return fmt.Sprintf("@json(%v)", e.operand) }

func (e *jsonExpr) MarshalJSON() ([]byte, error)  { return marshalUnaryOp("@json", e.operand) }
func (e *jsonExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// toSortedYAMLNode walks v and produces a yaml.Node whose mappings have
// their keys sorted alphabetically at every depth. Sequences preserve
// order. Scalars are encoded via yaml.Node.Encode.
func toSortedYAMLNode(v any) (*yaml.Node, error) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		node := &yaml.Node{Kind: yaml.MappingNode}
		for _, k := range keys {
			kNode := &yaml.Node{Kind: yaml.ScalarNode, Value: k, Tag: "!!str"}
			vNode, err := toSortedYAMLNode(x[k])
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, kNode, vNode)
		}
		return node, nil
	case []any:
		node := &yaml.Node{Kind: yaml.SequenceNode}
		for _, el := range x {
			elNode, err := toSortedYAMLNode(el)
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, elNode)
		}
		return node, nil
	default:
		node := &yaml.Node{}
		if err := node.Encode(v); err != nil {
			return nil, err
		}
		return node, nil
	}
}

// wrapFormatOperand adapts whatever the parser hands us into a single
// Expression. The parser delivers one of: nil, a single Expression, a
// map[string]Expression (dict args), a []Expression (list args), or a raw
// scalar. Format operators marshal the fully-evaluated Go value, so the
// operand must be a real expression tree — wrapping bare scalars in
// constExpr, composite args in their matching dict/list expression.
func wrapFormatOperand(args any) Expression {
	switch v := args.(type) {
	case nil:
		return nil
	case Expression:
		return v
	case map[string]Expression:
		return &dictExpr{entries: v}
	case []Expression:
		return &listExpr{elements: v}
	default:
		return &constExpr{value: v}
	}
}

func init() {
	MustRegister("@yaml", func(args any) (Expression, error) {
		return &yamlExpr{operand: wrapFormatOperand(args)}, nil
	})
	MustRegister("@json", func(args any) (Expression, error) {
		return &jsonExpr{operand: wrapFormatOperand(args)}, nil
	})
}
