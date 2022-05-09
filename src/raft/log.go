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

func (logs *LogEntries) Get(left int, right int) []Entry {
	return logs.Entries[left-logs.Index0 : right-logs.Index0]
}

func (logs *LogEntries) Splice(index int) []Entry {
	return logs.Entries[index-logs.Index0:]
}

func (logs *LogEntries) Append(e Entry) {
	logs.Entries = append(logs.Entries, e)
}

func (logs *LogEntries) AppendAfterIndex(index int, entrys []Entry) {

	logs.Entries = append(logs.Entries[:index-logs.Index0], entrys...)
	logs.Entries = append([]Entry{}, logs.Entries...) // some entry has been aborted, we must gc underling array
}

// erase the log entry before a specific index (not include index)
func (logs *LogEntries) EraseBefore(index int) {
	logs.Entries = logs.Entries[index-logs.Index0:]
	logs.Entries = append([]Entry{}, logs.Entries...) // some entry has been aborted, we must gc underling array
	logs.Index0 = index
}

func (logs *LogEntries) EraseAfter(index int) {
	logs.Entries = logs.Entries[:index-logs.Index0]
}

// add an dummy entry
func makeLogEntriesEmpty() LogEntries {
	var logEntries LogEntries
	logEntries.Index0 = 0
	logEntries.Entries = make([]Entry, 1)
	return logEntries
}
