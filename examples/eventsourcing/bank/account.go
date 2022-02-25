package bank

import (
	"errors"
	"fmt"
	"time"
)

type Account struct {
	Aggregate

	ID, Owner string
	Balance   int
	OpenedAt  int64
}

func OpenAccount(accountID, owner string) (*Account, error) {
	if len(accountID) == 0 || len(owner) == 0 {
		return nil, fmt.Errorf("invalid account info: %s,%s", accountID, owner)
	}

	a := &Account{}
	a.SetMutator(a.onEvent)

	a.Apply(&AccountOpened{
		Owner: owner,
		ID:    accountID,
		At:    time.Now().Unix(),
	})
	return a, nil
}

func BuildAccount(evts []interface{}) (*Account, error) {
	if len(evts) == 0 {
		return nil, errors.New("account not found")
	}
	a := &Account{}
	a.SetMutator(a.onEvent)
	for _, e := range evts {
		a.mutate(e)
	}
	return a, nil
}

func (a *Account) DepositMoney(amount int) error {
	if amount < 0 {
		return fmt.Errorf("invalid amount to deposit: %d", amount)
	}
	a.Apply(&MoneyDeposited{
		AccountID: a.ID,
		Amount:    amount,
		At:        time.Now().Unix(),
	})
	return nil
}

func (a *Account) WithrawMoney(amount int) error {
	if amount < 0 || a.Balance-amount < 0 {
		return fmt.Errorf("invalid amount to withdraw: %d", amount)
	}
	a.Apply(&MoneyWithrawn{
		AccountID: a.ID,
		Amount:    amount,
		At:        time.Now().Unix(),
	})
	return nil
}
