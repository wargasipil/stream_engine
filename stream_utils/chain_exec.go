package stream_utils

type ChainNextFunc[T any] func(data T) error
type ChainNextHandler[T any] func(next ChainNextFunc[T]) ChainNextFunc[T]

func NewChain[T any](chains ...ChainNextHandler[T]) ChainNextFunc[T] {

	var next ChainNextFunc[T] = func(data T) error {
		return nil
	}

	reverse(chains)
	for _, chain := range chains {
		next = chain(next)
	}

	return next
}
