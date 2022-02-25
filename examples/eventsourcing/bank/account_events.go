package bank

type AccountOpened struct {
	ID    string
	Owner string
	At    int64
}

type MoneyDeposited struct {
	Amount    int
	AccountID string
	At        int64
}

type MoneyWithrawn struct {
	Amount    int
	AccountID string
	At        int64
}

func (a *Account) onEvent(evt interface{}) {
	switch ev := evt.(type) {
	case *AccountOpened:
		a.ID = ev.ID
		a.OpenedAt = ev.At
	case *MoneyDeposited:
		a.Balance += ev.Amount
	case *MoneyWithrawn:
		a.Balance -= ev.Amount
	}
}
