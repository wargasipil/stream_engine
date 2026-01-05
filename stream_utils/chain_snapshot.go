package stream_utils

import "reflect"

type NextFunc func(key string, kind reflect.Kind, value any) error
type NextHandler func(next NextFunc) NextFunc

func NewChainSnapshot(chains ...NextHandler) NextFunc {
	var next NextFunc = func(key string, kind reflect.Kind, value any) error {
		return nil
	}

	reverse(chains)
	for _, chain := range chains {
		next = chain(next)
	}

	return next
}
func reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func PararelChainSnapshot(chains ...NextFunc) NextHandler {
	return func(next NextFunc) NextFunc {
		return func(key string, kind reflect.Kind, value any) error {

			for _, chain := range chains {
				err := chain(key, kind, value)
				if err != nil {
					return err
				}
			}

			return next(key, kind, value)
		}
	}
}
