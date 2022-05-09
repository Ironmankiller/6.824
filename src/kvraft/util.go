package kvraft

import "log"

const (
	NoPrint = -1
	Debug   = 0
	Info    = 1
)

const Level = NoPrint

// LPrintf print dense info
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Level == Debug {
		log.Printf(format, a...)
	}
	return
}

// LPrintf print dense info
func LPrintf(format string, a ...interface{}) (n int, err error) {
	if Level == Info {
		log.Printf(format, a...)
	}
	return
}
