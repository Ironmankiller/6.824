package raft

import "fmt"

type Entry struct {
	Command interface{}
	Term    int
}

func (e Entry) String() string {
	return fmt.Sprintf("Command %v Term %v", e.Command, e.Term)
}

func (e Entry) isEqual(tmp Entry) bool {
	return e.Term == tmp.Term && e.Command == tmp.Command
}

type LogEntries struct {
	Entries []Entry
	Index0  int
}

func (logs *LogEntries) FirstIndex() int {
	return logs.Index0
}

func (logs *LogEntries) LastIndex() int {
	return logs.Index0 + len(logs.Entries) - 1
}

func (logs *LogEntries) GetAt(index int) Entry {
	return logs.Entries[index-logs.Index0]
}

func (logs *LogEntries) Splice(index int) []Entry {
	return logs.Entries[index-logs.Index0:]
}

func (logs *LogEntries) Append(e Entry) {
	logs.Entries = append(logs.Entries, e)
}

// erase the log entry before a specific index (not include index)
func (logs *LogEntries) EraseBefore(index int) {
	logs.Entries = logs.Entries[index-logs.Index0:]
	logs.Index0 = index
}

func (logs *LogEntries) EraseAfter(index int) {
	logs.Entries = logs.Entries[:index-logs.Index0]
}

// add an dummy entry
func makeLogEntriesEmpty() LogEntries {
	var emptyEntry Entry
	var logEntries LogEntries
	logEntries.Entries = append(logEntries.Entries, emptyEntry)
	return logEntries
}
