package bank

type Mutator func(evt interface{})

type Aggregate struct {
	evChanges []interface{}
	mutator   Mutator
}

func (a *Aggregate) Apply(evt interface{}) {
	if a.evChanges == nil {
		a.evChanges = []interface{}{}
	}
	a.evChanges = append(a.evChanges, evt)
	a.mutate(evt)
}

func (a *Aggregate) SetMutator(mutator Mutator) {
	a.mutator = mutator
}

func (a *Aggregate) mutate(evt interface{}) {
	if a.mutator == nil {
		panic("invalid aggregate, state mutator not defined")
	}

	a.mutator(evt)
}

func (a *Aggregate) EvClearChanges() []interface{} {
	defer func() {
		a.evChanges = nil
	}()

	return a.evChanges
}
