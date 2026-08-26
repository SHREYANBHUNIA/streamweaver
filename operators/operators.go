package operators

import "github.com/manus/streamweaver/ingestion"

type Operator interface {
	Apply(ingestion.Event) (ingestion.Event, bool, error)
}

type Filter func(ingestion.Event) bool

func (f Filter) Apply(event ingestion.Event) (ingestion.Event, bool, error) {
	return event, f(event), nil
}

type Map func(ingestion.Event) ingestion.Event

func (m Map) Apply(event ingestion.Event) (ingestion.Event, bool, error) {
	return m(event), true, nil
}

type Pipeline struct {
	operators []Operator
}

func NewPipeline(operators ...Operator) Pipeline { return Pipeline{operators: operators} }

func (p Pipeline) Apply(event ingestion.Event) (ingestion.Event, bool, error) {
	for _, operator := range p.operators {
		var keep bool
		var err error
		event, keep, err = operator.Apply(event)
		if err != nil || !keep {
			return event, keep, err
		}
	}
	return event, true, nil
}
