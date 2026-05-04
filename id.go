package alosdbclient

import (
	"math/rand/v2"
	"sync"
)

const idAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const idLen = 60

var pcgPool = sync.Pool{
	New: func() interface{} {
		return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	},
}

var idBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, idLen)
		return &b
	},
}

func generateDocID() string {
	rng := pcgPool.Get().(*rand.Rand)

	bufPtr := idBufPool.Get().(*[]byte)
	buf := *bufPtr

	i := 0
	for i+4 <= idLen {
		r := rng.Uint64()
		buf[i] = idAlphabet[r%52]
		r /= 52
		buf[i+1] = idAlphabet[r%52]
		r /= 52
		buf[i+2] = idAlphabet[r%52]
		r /= 52
		buf[i+3] = idAlphabet[r%52]
		i += 4
	}
	for i < idLen {
		buf[i] = idAlphabet[rng.Uint64()%52]
		i++
	}

	s := string(buf)

	*bufPtr = buf
	idBufPool.Put(bufPtr)
	pcgPool.Put(rng)

	return s
}
