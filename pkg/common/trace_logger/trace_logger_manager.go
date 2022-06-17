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
	"fmt"
	"github.com/emirpasic/gods/maps/linkedhashmap"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
	// log "github.com/sirupsen/logrus"
)

// define errors
const (
	NoKeyError   = "no key has been set"
	NoTraceError = "no trace has been set"
)

// define interface

type Trace struct {
	logs []traceLog
	time time.Time
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
	return fmt.Sprintf("[%s] %s - %s: %s", t.Level, t.Time, t.Key, t.Msg)
}

func (t Trace) String() string {
	return fmt.Sprint(t.logs)
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
	CommitTraceWithKey(key string) error
	CommitTrace() error
	SetKey(key string)
	RollbackTrace() error
}

type TraceLoggerManager interface {
	NewTraceLogger() TraceLogger

	GetTraceFromCache(key string) (Trace, bool)
	GetAllTraceFromCache() []Trace
	SetTraceToCache(key string, trace Trace) error

	SyncAll() error
	LoadAll() error
	AutoDelete(timeout, duration time.Duration) error
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
		Msg:   fmt.Sprintf(format, args...),
		Level: level,
		Time:  time.Now(),
	})
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

func (d *defaultTraceLogger) CommitTraceWithKey(key string) error {
	d.key = key
	return d.CommitTrace()
}

func (d *defaultTraceLogger) CommitTrace() error {
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

func (d *defaultTraceLogger) RollbackTrace() error {
	// noting to do here
	return nil
}

// implementation for trace logger manager

type DefaultTraceLoggerManager struct {
	// use linked list to sustain the order of the trace
	cache       *linkedhashmap.Map
	lastSyncKey string
	l           *logrus.Logger
	*sync.RWMutex

	// auto delete
	autoDeleteFlag bool
	autoDeleteLock *sync.Mutex
	cancelChan     chan struct{}
}

func NewDefaultTraceLoggerManager() *DefaultTraceLoggerManager {
	return &DefaultTraceLoggerManager{
		cache:          linkedhashmap.New(),
		RWMutex:        &sync.RWMutex{},
		l:              logger,
		autoDeleteLock: &sync.Mutex{},
		cancelChan:     make(chan struct{}, 1),
	}
}

func (d *DefaultTraceLoggerManager) StoreTraceToFile(trace Trace) {
	for _, traceLog := range trace.logs {
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

	// add time
	trace.time = time.Now()
	d.cache.Put(key, trace)
	return
}

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
	lastSyncKey := tmpIter.Key().(string)

	// find the last synced key
	_ = iter.PrevTo(func(key interface{}, value interface{}) bool {
		k := key.(string)
		// find last sync key, stop iterating
		return k == d.lastSyncKey
	})

	// log trace
	for iter.Next() {
		val := iter.Value()
		trace := val.(Trace)
		d.StoreTraceToFile(trace)
	}

	d.lastSyncKey = lastSyncKey
	return nil
}

func (d *DefaultTraceLoggerManager) LoadAll() error {
	//TODO implement me
	panic("implement me")
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
				d.deleteTraceFromCacheBefore(timeout)
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

func (d *DefaultTraceLoggerManager) deleteTraceFromCacheBefore(timeout time.Duration) {
	d.Lock()
	defer d.Unlock()

	timeBefore := time.Now().Add(-timeout)

	// delete all the trace logs before the timeBefore
	iter := d.cache.Iterator()

	// find the timeBefore first

	_ = iter.NextTo(func(key interface{}, value interface{}) bool {
		trace := value.(Trace)

		// if time after timeBefore, stop iterating
		return trace.time.After(timeBefore)
	})

	// delete the trace logs before
	// must remove item backwards while iterating,
	// otherwise the index will be wrong
	for iter.Prev() {
		d.cache.Remove(iter.Key())
	}
}
