package app

import "context"

type Command interface {
	Execute(context.Context) error
}
