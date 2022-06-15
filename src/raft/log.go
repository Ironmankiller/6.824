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
	return e.Term == tmp.Term
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

func (logs *LogEntries) SetFirst(index int, entry Entry) {
	logs.Entries[0] = entry
	logs.Index0 = index
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

// AppendAfterIndex erase entries after index (include index), and append entries.
func (logs *LogEntries) AppendAfterIndex(index int, entries []Entry) {

	before := append(logs.Entries[:index-logs.Index0], entries...)
	logLen := len(before)
	newLog := make([]Entry, logLen)
	copy(newLog, before)
	logs.Entries = newLog

	//logs.Entries = append(logs.Entries[:index-logs.Index0], entries...)
	//logs.Entries = append(make([]Entry, 0, len(logs.Entries)), logs.Entries...) // some entry has been aborted, we must gc underling array
}

// EraseBefore erase the log entry before a specific index (not include index)
func (logs *LogEntries) EraseBefore(index int) {

	after := logs.Entries[index-logs.Index0:]
	logLen := len(after)
	newLog := make([]Entry, logLen)
	copy(newLog, after)
	logs.Entries = nil
	logs.Entries = newLog
	logs.Index0 = index
}

// EraseAfter erase the log entry after a specific index (include index)
func (logs *LogEntries) EraseAfter(index int) {
	before := logs.Entries[index-logs.Index0:]
	logLen := len(before)
	newLog := make([]Entry, logLen)
	copy(newLog, before)
	logs.Entries = nil
	logs.Entries = newLog
}

func (logs *LogEntries) GetLogLength() int {
	return len(logs.Entries)
}

// makeLogEntriesEmpty Add a dummy entry
func makeLogEntriesEmpty() LogEntries {
	var logEntries LogEntries
	logEntries.Index0 = 0
	logEntries.Entries = make([]Entry, 1)
	return logEntries
}
