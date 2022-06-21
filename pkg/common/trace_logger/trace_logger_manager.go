/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/**
 * @Author: kiritoxkiriko
 * @Date: 2022/6/16
 * @Description:
 */

package trace_logger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/emirpasic/gods/maps/linkedhashmap"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	// log "github.com/sirupsen/logrus"
)

// define errors
const (
	NoKeyError         = "no key has been set"
	NoTraceError       = "no trace has been set"
	LogLevelStringSize = 4
	MaxCacheSize       = 10000
	CacheLoadFactor    = 0.8
)

// assert implements
var (
	_ TraceLogger        = (*defaultTraceLogger)(nil)
	_ TraceLoggerManager = (*DefaultTraceLoggerManager)(nil)
)

// define interface

type Trace struct {
	logs       []traceLog
	updateTime time.Time
	// lastSyncIndex store the last synced index in the logs
	lastSyncIndex int
}

// the name of trace log fields is same as logrus/Entry
// for easy marshal to json
type traceLog struct {
	Key   string       `json:"key"`
	Msg   string       `json:"msg"`
	Level logrus.Level `json:"level"`
	Time  time.Time    `json:"time"`
}

func (t traceLog) String() string {
	timeStr := t.Time.Format("2006-01-02 15:04:05.06")
	level := strings.ToUpper(t.Level.String()[:LogLevelStringSize])
	return fmt.Sprintf("[%s] [%s] %s %s", level, t.Key, timeStr, t.Msg)
}

func (t Trace) String() string {
	lines := make([]string, len(t.logs))
	for i, traceLog := range t.logs {
		lines[i] = fmt.Sprintf("%s", traceLog)
	}
	return strings.Join(lines, "\n")
}

func (d DefaultTraceLoggerManager) String() string {
	d.RLock()
	defer d.RUnlock()
	lines := make([]string, d.cache.Size())
	for i, trace := range d.cache.Values() {
		lines[i] = fmt.Sprintf("%s", trace.(Trace))
	}
	return strings.Join(lines, "\n")
}

type TraceLogger interface {
	// logger interface
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})

	// trace interface
	SetKey(key string)
	GetKey() string
	GetTrace() Trace
	UpdateTraceWithKey(key string) error
	UpdateTrace() error
}

type DeleteMethod func(key string) bool

var DefaultDeleteMethod DeleteMethod = func(key string) bool {
	return true
}

type TraceLoggerManager interface {
	NewTraceLogger() TraceLogger

	GetTraceFromCache(key string) (Trace, bool)
	GetAllTraceFromCache() []Trace
	SetTraceToCache(key string, trace Trace) error
	UpdateKey(key string, newKey string) error
	Key(key string) TraceLogger

	SyncAll() error
	LoadAll(path string) error
	ClearAll() error
	AutoDelete(timeout, duration time.Duration) error
	AutoDeleteWithMethod(timeout, duration time.Duration, method DeleteMethod) error
	CancelAutoDelete() error
}

// define implementation
// implementation for trace logger

type defaultTraceLogger struct {
	trace   Trace
	key     string
	manager TraceLoggerManager
}

func (d *defaultTraceLogger) saveOneLog(level logrus.Level, format string, args ...interface{}) {
	d.trace.logs = append(d.trace.logs, traceLog{
		Key:   d.key,
		Msg:   fmt.Sprintf(format, args...),
		Level: level,
		Time:  time.Now(),
	})

	// call update trace after every log
	_ = d.UpdateTrace()
}

func (d *defaultTraceLogger) Infof(format string, args ...interface{}) {
	d.saveOneLog(logrus.InfoLevel, format, args...)
}

func (d *defaultTraceLogger) Debugf(format string, args ...interface{}) {
	d.saveOneLog(logrus.DebugLevel, format, args...)
}

func (d *defaultTraceLogger) Warnf(format string, args ...interface{}) {
	d.saveOneLog(logrus.WarnLevel, format, args...)
}

func (d *defaultTraceLogger) Errorf(format string, args ...interface{}) {
	d.saveOneLog(logrus.ErrorLevel, format, args...)
}

func (d *defaultTraceLogger) Fatalf(format string, args ...interface{}) {
	d.saveOneLog(logrus.FatalLevel, format, args...)
}

func (d *defaultTraceLogger) Panicf(format string, args ...interface{}) {
	d.saveOneLog(logrus.PanicLevel, format, args...)
}

func (d *defaultTraceLogger) UpdateTraceWithKey(key string) error {
	d.key = key
	return d.UpdateTrace()
}

func (d *defaultTraceLogger) UpdateTrace() error {
	if d.key == "" {
		return fmt.Errorf("no key has been set")
	}
	return d.manager.SetTraceToCache(d.key, d.trace)
}

// SetKey
// it won't return error for now
func (d *defaultTraceLogger) SetKey(key string) {
	d.key = key
	for i := range d.trace.logs {
		d.trace.logs[i].Key = key
	}
}

func (d *defaultTraceLogger) GetKey() string {
	return d.key
}

func (d *defaultTraceLogger) GetTrace() Trace {
	return d.trace
}

// implementation for trace logger manager

type DefaultTraceLoggerManager struct {
	// use linked list to sustain the order of the trace
	cache       *linkedhashmap.Map
	tmpKeyMap   sync.Map
	lastSyncKey string
	l           *logrus.Logger
	sync.RWMutex

	// auto delete
	autoDeleteFlag bool
	autoDeleteLock sync.Mutex
	cancelChan     chan struct{}
}

func NewDefaultTraceLoggerManager() *DefaultTraceLoggerManager {
	return &DefaultTraceLoggerManager{
		cache:          linkedhashmap.New(),
		tmpKeyMap:      sync.Map{},
		RWMutex:        sync.RWMutex{},
		l:              logger,
		autoDeleteLock: sync.Mutex{},
		cancelChan:     make(chan struct{}, 1),
	}
}

func (d *DefaultTraceLoggerManager) StoreTraceToFile(trace Trace) {
	d.RLock()
	defer d.RUnlock()
	for _, traceLog := range trace.logs[trace.lastSyncIndex:] {
		d.storeTraceLogToFile(traceLog)
	}
}

func (d *DefaultTraceLoggerManager) storeTraceLogToFile(traceLog traceLog) {
	d.l.WithFields(map[string]interface{}{
		"key": traceLog.Key,
	}).WithTime(traceLog.Time).WithTime(traceLog.Time).Log(traceLog.Level, traceLog.Msg)
}

func (d *DefaultTraceLoggerManager) NewTraceLogger() TraceLogger {
	return &defaultTraceLogger{
		trace:   Trace{},
		manager: d,
	}
}

func (d *DefaultTraceLoggerManager) GetTraceFromCache(key string) (Trace, bool) {
	// add lock
	d.RLock()
	defer d.RUnlock()
	val, ok := d.cache.Get(key)
	return val.(Trace), ok
}

func (d *DefaultTraceLoggerManager) GetAllTraceFromCache() []Trace {
	d.RLock()
	defer d.RUnlock()

	vals := d.cache.Values()
	traces := make([]Trace, len(vals), len(vals))
	for i := range vals {
		traces[i] = vals[i].(Trace)
	}
	return traces
}

func (d *DefaultTraceLoggerManager) SetTraceToCache(key string, trace Trace) (err error) {
	// add lock
	d.Lock()
	defer d.Unlock()
	defer func() {
		if err1 := recover(); err1 != nil {
			err = fmt.Errorf("%v", err1)
		}
	}()

	// add updateTime
	old, ok := d.cache.Get(key)

	// if key exists, remove old trace and add it to the end of the list, in order to maintain the insertion order
	if ok {
		trace.lastSyncIndex = old.(Trace).lastSyncIndex
		d.cache.Remove(key)
	}
	trace.updateTime = time.Now()
	d.cache.Put(key, trace)
	// delete outdated trace
	d.deleteOldTrace()
	return
}

func (d *DefaultTraceLoggerManager) deleteOldTrace() {
	if d.cache.Size() < MaxCacheSize {
		return // do not delete if cache is not full
	}
	newSize := int(MaxCacheSize * CacheLoadFactor)

	iter := d.cache.Iterator()
	for i := d.cache.Size(); i > newSize; i = d.cache.Size() {
		if ok := iter.First(); !ok {
			break
		}
		d.cache.Remove(iter.Key())
	}
}

// SyncAll dont call this method in a concurrent environment
func (d *DefaultTraceLoggerManager) SyncAll() error {
	// get lock
	d.RLock()
	defer d.RUnlock()

	// sync all the Trace backward until the last sync key
	iter := d.cache.Iterator()
	ok := iter.Last()
	if !ok {
		return fmt.Errorf("no trace has been set")
	}

	// get the last sync key
	tmpIter := d.cache.Iterator()
	tmpIter.End()
	newLastSyncedKey := tmpIter.Key().(string)

	// find the last synced key
	_ = iter.PrevTo(func(key interface{}, value interface{}) bool {
		k := key.(string)
		// find last sync key, stop iterating
		return k == d.lastSyncKey
	})

	// log trace
	for iter.Next() {
		val := iter.Value()
		key := iter.Key().(string)
		trace := val.(Trace)
		d.StoreTraceToFile(trace)
		// update synced index
		trace.lastSyncIndex = len(trace.logs)
		d.cache.Put(key, trace)
	}

	d.lastSyncKey = newLastSyncedKey
	return nil
}

// LoadAll will load all the trace from the file, and replace local cache
func (d *DefaultTraceLoggerManager) LoadAll(path string) (err error) {

	d.Lock()
	defer d.Unlock()
	// clear the cache
	d.clearCache()

	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to get file stat: %v", err)
	}

	// if is a directory, open the files in the directory
	if stat.IsDir() {
		filesEntries, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("failed to read dir: %v", err)
		}
		for _, fileEntry := range filesEntries {
			if !fileEntry.IsDir() {
				err := d.loadFromFile(filepath.Join(path, fileEntry.Name()))
				if err != nil {
					return err
				}
			}
		}
	} else {
		err := d.loadFromFile(path)
		if err != nil {
			return err
		}
	}

	// update last sync key
	iter := d.cache.Iterator()
	if iter.Last() {
		d.lastSyncKey = iter.Key().(string)
	}

	return nil
}

func (d *DefaultTraceLoggerManager) loadFromFile(filePath string) (err error) {

	file, err := os.Open(filePath)
	defer func() {
		err1 := file.Close()
		if err1 != nil {
			err = fmt.Errorf("failed to close file: %v", err1)
		}
	}()

	if err != nil {
		return fmt.Errorf("read log file fail: %w", err)
	}
	scanner := bufio.NewScanner(file)

	var (
		currentTrace Trace
		currentKey   string
	)

	for scanner.Scan() {
		jsonBytes := scanner.Bytes()
		traceLog := traceLog{}
		err = json.Unmarshal(jsonBytes, &traceLog)
		if err != nil {
			return fmt.Errorf("parse log fail: %w", err)
		}
		if currentKey != traceLog.Key {
			// save the previous trace to cache
			if currentKey != "" {
				d.cache.Put(currentKey, currentTrace)
			}
			currentKey = traceLog.Key
			val, ok := d.cache.Get(currentKey)
			if !ok {
				currentTrace = Trace{}
			} else {
				currentTrace = val.(Trace)
			}

			// update updateTime
			currentTrace.updateTime = time.Now()
		}
		currentTrace.logs = append(currentTrace.logs, traceLog)
	}
	d.cache.Put(currentKey, currentTrace)

	return nil
}

func (d *DefaultTraceLoggerManager) AutoDelete(timeout, duration time.Duration) error {
	d.autoDeleteLock.Lock()
	defer d.autoDeleteLock.Unlock()
	if d.autoDeleteFlag {
		return fmt.Errorf("auto delete has started")
	}

	go func() {
		for {
			select {
			case <-d.cancelChan:
				return
			case <-time.After(duration):
				fmt.Println("auto delete")
				d.Lock()
				d.deleteTraceFromCacheBefore(timeout, DefaultDeleteMethod)
				d.Unlock()
			}
		}
	}()

	return nil
}

func (d *DefaultTraceLoggerManager) AutoDeleteWithMethod(timeout, duration time.Duration, method DeleteMethod) error {
	d.autoDeleteLock.Lock()
	defer d.autoDeleteLock.Unlock()
	if d.autoDeleteFlag {
		return fmt.Errorf("auto delete has started")
	}

	go func() {
		for {
			select {
			case <-d.cancelChan:
				return
			case <-time.After(duration):
				fmt.Println("auto delete")
				d.Lock()
				d.deleteTraceFromCacheBefore(timeout, method)
				d.Unlock()
			}
		}
	}()

	return nil
}

func (d *DefaultTraceLoggerManager) CancelAutoDelete() error {
	d.autoDeleteLock.Lock()
	defer d.autoDeleteLock.Unlock()

	if !d.autoDeleteFlag {
		return fmt.Errorf("auto delete has not started")
	}
	d.cancelChan <- struct{}{}
	return nil
}

func (d *DefaultTraceLoggerManager) deleteTraceFromCacheBefore(timeout time.Duration, method DeleteMethod) {

	// check if synced already, if not, no need to delete
	if d.lastSyncKey == "" {
		return // no need to delete
	}

	timeBefore := time.Now().Add(-timeout)

	// delete all the trace logs before the timeBefore
	iter := d.cache.Iterator()

	// find the timeBefore first
	_ = iter.NextTo(func(key interface{}, value interface{}) bool {
		trace := value.(Trace)

		// if not synced yet, stop iterating
		if key.(string) == d.lastSyncKey {
			return true
		}
		// if updateTime after timeBefore, stop iterating
		return trace.updateTime.After(timeBefore)
	})
	// delete the trace logs before
	// must remove item in backwards order while iterating,
	// otherwise the index will be wrong
	for iter.Prev() {
		key := iter.Key().(string)
		if !method(key) {
			return
		}
		d.cache.Remove(iter.Key())
		// also remove it from tmp map
		d.tmpKeyMap.Delete(iter.Key())
	}
}

func (d *DefaultTraceLoggerManager) deleteUnusedTmpKey(timeout time.Duration) {
	d.tmpKeyMap.Range(func(key, value interface{}) bool {
		tmpKey := key.(string)
		traceLogger := value.(TraceLogger)
		// if no key set, then this logger is temporary, and can be deleted
		if traceLogger.GetKey() == "" {
			updateTime := traceLogger.GetTrace().updateTime
			if updateTime.Before(time.Now().Add(-timeout)) {
				d.tmpKeyMap.Delete(tmpKey)
			}
		}
		return true
	})
}

// ClearAll will clear all the trace from the cache
// please sync the cache before calling this function
func (d *DefaultTraceLoggerManager) ClearAll() error {
	d.Lock()
	defer d.Unlock()
	d.clearCache()
	return nil
}

func (d *DefaultTraceLoggerManager) clearCache() {
	d.cache.Clear()
	d.lastSyncKey = ""
}

func (d *DefaultTraceLoggerManager) UpdateKey(key string, newKey string) error {

	// delete tmp key map
	val, ok := d.tmpKeyMap.LoadAndDelete(key)
	if !ok {
		return fmt.Errorf("key %s not found", key)
	}

	// update tmp key
	d.tmpKeyMap.Store(newKey, val)
	traceLogger := val.(TraceLogger)
	traceLogger.SetKey(newKey)
	return nil
}

func (d *DefaultTraceLoggerManager) Key(key string) TraceLogger {
	var traceLogger TraceLogger
	// use load or store
	val, _ := d.tmpKeyMap.LoadOrStore(key, d.NewTraceLogger())
	traceLogger = val.(TraceLogger)
	return traceLogger
}
