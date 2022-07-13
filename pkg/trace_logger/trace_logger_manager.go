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

package trace_logger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/binaryheap"
	"github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	"github.com/viney-shih/go-lock"
)

// define errors
const (
	NoKeyError         = "no key has been set"
	NoTraceError       = "no trace has been set"
	LogLevelStringSize = 4
	CacheLoadFactor    = 0.75
	LogStringFormat    = "2006-01-02 15:04:05.06"
)

// assert implements
var (
	_ TraceLogger        = (*defaultTraceLogger)(nil)
	_ TraceLoggerManager = (*DefaultTraceLoggerManager)(nil)
)

// heap comparator

// define interface

type Trace struct {
	Logs       []traceLog
	UpdateTime time.Time
	// lastSyncIndex store the last synced index in the Logs
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
	timeStr := t.Time.Format(LogStringFormat)
	level := strings.ToUpper(t.Level.String()[:LogLevelStringSize])
	return fmt.Sprintf("[%s] [%s] %s %s", timeStr, level, t.Key, t.Msg)
}

func (t Trace) String() string {
	lines := make([]string, len(t.Logs))
	for i, traceLog := range t.Logs {
		lines[i] = traceLog.String()
	}
	return strings.Join(lines, "\n")
}

func (d *DefaultTraceLoggerManager) String() string {
	lines := make([]string, 0, d.cache.Count())
	for x := range d.cache.IterBuffered() {
		v := x.Val.(Trace)
		lines = append(lines, v.String())
	}
	return strings.Join(lines, "\n")
}

type TraceLogger interface {
	// fileLogger interface
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

// DeleteMethod return true to delete
type DeleteMethod func(key string) bool

// TODO: add custom delete method
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
	KeyWithUpdate(key string) TraceLogger

	SyncAll() error
	LoadAll(path string, prefix ...string) error
	ClearAll() error
	DeleteUnusedCache(timeout time.Duration, method ...DeleteMethod) error

	AutoDelete(duration time.Duration, method ...DeleteMethod) error
	CancelAutoDelete() error

	AutoSync(duration time.Duration) error
	CancelAutoSync() error
}

// define implementation
// implementation for trace fileLogger

type defaultTraceLogger struct {
	trace   Trace
	key     string
	manager TraceLoggerManager
	showLog bool
}

func (d *defaultTraceLogger) saveOneLog(level logrus.Level, format string, args ...interface{}) {

	log := traceLog{
		Key:   d.key,
		Msg:   fmt.Sprintf(format, args...),
		Level: level,
		Time:  time.Now(),
	}

	if d.showLog {
		fmt.Println(log.String())
	}

	d.trace.Logs = append(d.trace.Logs, log)

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
	d.trace.UpdateTime = time.Now()
	if d.key == "" {
		return fmt.Errorf("no key has been set")
	}
	return d.manager.SetTraceToCache(d.key, d.trace)
}

// SetKey
// it won't return error for now
func (d *defaultTraceLogger) SetKey(key string) {
	d.key = key
	for i := range d.trace.Logs {
		d.trace.Logs[i].Key = key
	}
}

func (d *defaultTraceLogger) GetKey() string {
	return d.key
}

func (d *defaultTraceLogger) GetTrace() Trace {
	return d.trace
}

// implementation for trace fileLogger manager

type DefaultTraceLoggerManager struct {
	// use more efficient map as local cache
	// see https://github.com/orcaman/concurrent-map
	cache cmap.ConcurrentMap
	// save temp trace fileLogger before call update key
	tmpKeyMap sync.Map
	l         *logrus.Logger

	// use  unblocking for eliminate lock
	evictLock lock.RWMutex
	lock      sync.RWMutex

	timeout      time.Duration
	maxCacheSize int

	debug bool

	// auto delete
	autoDeleteFlag       bool
	autoDeleteLock       sync.Mutex
	autoDeleteCancelChan chan struct{}

	// auto sync
	autoSyncFlag       bool
	autoSyncLock       sync.Mutex
	autoSyncCancelChan chan struct{}
}

func NewDefaultTraceLoggerManager(cacheSize int, timeout time.Duration, debug bool) *DefaultTraceLoggerManager {
	return &DefaultTraceLoggerManager{
		cache:                cmap.New(),
		tmpKeyMap:            sync.Map{},
		l:                    fileLogger,
		evictLock:            lock.NewCASMutex(),
		timeout:              timeout,
		debug:                debug,
		maxCacheSize:         cacheSize,
		autoDeleteCancelChan: make(chan struct{}, 1),
		autoSyncCancelChan:   make(chan struct{}, 1),
	}
}

func (d *DefaultTraceLoggerManager) StoreTraceToFile(trace Trace) {
	for _, traceLog := range trace.Logs[trace.lastSyncIndex:] {
		d.storeTraceLogToFile(traceLog)
	}
}

func (d *DefaultTraceLoggerManager) storeTraceLogToFile(traceLog traceLog) {
	d.l.WithFields(map[string]interface{}{
		"key": traceLog.Key,
	}).WithTime(traceLog.Time).Log(traceLog.Level, traceLog.Msg)
}

func (d *DefaultTraceLoggerManager) NewTraceLogger() TraceLogger {
	return &defaultTraceLogger{
		trace:   Trace{},
		manager: d,
		showLog: d.debug,
	}
}

func (d *DefaultTraceLoggerManager) GetTraceFromCache(key string) (Trace, bool) {
	val, ok := d.cache.Get(key)
	if !ok {
		return Trace{}, false
	}
	trace := val.(Trace)
	logger.Debugf("get trace from cache [%s]: %s", key, trace)
	return trace, ok
}

func (d *DefaultTraceLoggerManager) GetAllTraceFromCache() []Trace {
	iter := d.cache.IterBuffered()
	traces := make([]Trace, 0, d.cache.Count())
	for x := range iter {
		traces = append(traces, x.Val.(Trace))
	}
	return traces
}

func (d *DefaultTraceLoggerManager) SetTraceToCache(key string, trace Trace) (err error) {

	//defer func() {
	//	if err1 := recover(); err1 != nil {
	//		err = fmt.Errorf("%v", err1)
	//	}
	//}()
	// delete outdated trace
	// fmt.Println("maxCacheSize:", d.maxCacheSize, d.cache.Count())

	// if reach max count after insertion, evict first
	newCount := d.cache.Count() + 1
	if newCount > d.maxCacheSize {
		logger.Debugf("cache size is too large, evict cache")
		d.evictCache()
	}

	d.cache.Upsert(key, trace, func(exist bool, valueInMap interface{}, newValue interface{}) interface{} {
		// if trace exists, copy last synced index to new trace
		newVal := newValue.(Trace)
		if exist {
			newVal.lastSyncIndex = valueInMap.(Trace).lastSyncIndex
		}
		return newVal
	})

	return
}

// evictCache eliminate cache by lazy call
// will decrease cache size until it is less than maxCacheSize
func (d *DefaultTraceLoggerManager) evictCache() {

	// use try lock to avoid deadlock
	success := d.evictLock.TryLock()
	// if not get evictLock, omit evictCache
	if !success {
		return
	}
	defer d.evictLock.Unlock()

	if d.cache.Count() < d.maxCacheSize {
		return // do not delete if cache is not full
	}

	// sync all before evict
	errTmp := d.SyncAll()
	if errTmp != nil {
		logger.Warnf("sync failed before evict: %v", errTmp)
	}

	newSize := int(float64(d.maxCacheSize) * CacheLoadFactor)
	numberToBeDeleted := d.cache.Count() - newSize

	iter := d.cache.IterBuffered()

	timeout := d.timeout

	runHeap := binaryheap.NewWithStringComparator()
	ddl := time.Now().Add(-timeout)

	for x := range iter {
		if numberToBeDeleted <= 0 {
			break
		}
		k, v := x.Key, x.Val.(Trace)

		if v.UpdateTime.Before(ddl) {
			logger.Debugf("evict key: %s", k)
			d.removeKey(k)
			numberToBeDeleted--
		} else {
			// add it to heap
			runHeap.Push(k)
		}
	}

	// if deletion not done, remove item with smaller key
	for ; numberToBeDeleted > 0; numberToBeDeleted-- {
		x, ok := runHeap.Pop()
		// no more value, then break
		if !ok {
			break
		}
		key := x.(string)
		logger.Debugf("evict key: %s", key)
		d.removeKey(key)
	}

}

func (d *DefaultTraceLoggerManager) SyncAll() error {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.sync()
}

func (d *DefaultTraceLoggerManager) sync() error {
	// iter all traces in cache, and sync them
	iter := d.cache.IterBuffered()

	// sync trace to file
	for x := range iter {
		k, v := x.Key, x.Val.(Trace)
		d.StoreTraceToFile(v)
		v.lastSyncIndex = len(v.Logs)
		d.cache.Set(k, v)
	}
	return nil
}

// LoadAll will load all the trace from the file, and replace local cache
func (d *DefaultTraceLoggerManager) LoadAll(path string, prefixes ...string) (err error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// clear the cache
	d.clearCache()
	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to get file stat: %w", err)
	}

	var prefix string
	if len(prefixes) > 0 {
		prefix = prefixes[0]
	}

	// if is a directory, open the files in the directory
	if stat.IsDir() {
		filesEntries, err := os.ReadDir(path)

		// new file info slice
		fileInfos := make([]fs.FileInfo, 0, len(filesEntries))

		if err != nil {
			return fmt.Errorf("failed to read dir: %w", err)
		}

		for _, fileEntry := range filesEntries {
			if !fileEntry.IsDir() &&
				fileEntry.Name() != "." &&
				fileEntry.Name() != ".." &&
				strings.HasPrefix(fileEntry.Name(), prefix) {
				info, err := fileEntry.Info()
				if err != nil {
					return fmt.Errorf("failed to read file: %w", err)
				}

				// add it to slice
				fileInfos = append(fileInfos, info)
			}
		}

		// read in order
		size := 0

		// sort in descending order
		sort.Slice(fileInfos, func(i, j int) bool {
			return fileInfos[i].ModTime().After(fileInfos[j].ModTime())
		})

		for _, fileInfo := range fileInfos {
			if size >= d.maxCacheSize {
				break
			}

			count, err := d.loadFromFile(filepath.Join(path, fileInfo.Name()))
			if err != nil {
				return err
			}

			size += count
		}

	} else {
		_, err := d.loadFromFile(path)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DefaultTraceLoggerManager) loadFromFile(filePath string) (count int, err error) {

	file, err := os.Open(filePath)
	defer func() {
		err1 := file.Close()
		if err1 != nil {
			err = fmt.Errorf("failed to close file: %w", err1)
		}
	}()

	if err != nil {
		return count, fmt.Errorf("read log file fail: %w", err)
	}
	scanner := bufio.NewScanner(file)

	// sync cache
	for scanner.Scan() {
		jsonBytes := scanner.Bytes()
		log := traceLog{}
		err = json.Unmarshal(jsonBytes, &log)
		if err != nil {
			return count, fmt.Errorf("parse log fail: %w", err)
		}
		d.cache.Upsert(log.Key, log, func(exist bool, valueInMap interface{}, newValue interface{}) interface{} {
			val := Trace{}
			if exist {
				val = valueInMap.(Trace)
			}
			val.Logs = append(val.Logs, newValue.(traceLog))
			val.lastSyncIndex = len(val.Logs)
			val.UpdateTime = time.Now()
			return val
		})
		count++
	}

	return count, nil
}

func (d *DefaultTraceLoggerManager) AutoDelete(duration time.Duration, methods ...DeleteMethod) error {
	d.autoDeleteLock.Lock()
	defer d.autoDeleteLock.Unlock()
	if d.autoDeleteFlag {
		return fmt.Errorf("auto delete has started")
	}

	go func() {
		for {
			select {
			case <-d.autoDeleteCancelChan:
				return
			case <-time.After(duration):
				logger.Infof("auto deleting...")
				_ = d.DeleteUnusedCache(d.timeout, methods...)
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
	d.autoDeleteCancelChan <- struct{}{}
	return nil
}

func (d *DefaultTraceLoggerManager) DeleteUnusedCache(timeout time.Duration, methods ...DeleteMethod) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	method := DefaultDeleteMethod
	if len(methods) > 0 {
		method = methods[0]
	}

	err := d.sync()
	if err != nil {
		logrus.Warnf("sync failed: %v", err)
	}

	d.deleteExpiredCache(timeout, method)
	// delete unused key
	d.deleteUnusedTmpKey(timeout)
	return nil
}

func (d *DefaultTraceLoggerManager) deleteExpiredCache(timeout time.Duration, method DeleteMethod) {

	ddl := time.Now().Add(-timeout)

	// delete all the trace Logs before the ddl
	iter := d.cache.IterBuffered()
	for x := range iter {
		k, v := x.Key, x.Val.(Trace)
		if v.UpdateTime.Before(ddl) && method(k) {
			logger.Debugf("delete expired cache: %s", k)
			d.removeKey(k)
		}
	}
}

func (d *DefaultTraceLoggerManager) removeKey(key string) bool {
	return d.cache.RemoveCb(key, func(key string, v interface{}, exists bool) bool {
		if !exists {
			return false
		}
		// remove key from tmpLogger
		d.tmpKeyMap.Delete(key)
		return true
	})
}

func (d *DefaultTraceLoggerManager) deleteUnusedTmpKey(timeout time.Duration) {
	d.tmpKeyMap.Range(func(key, value interface{}) bool {
		tmpKey := key.(string)
		traceLogger := value.(TraceLogger)
		// if no key set, then this fileLogger is temporary, and can be deleted
		if traceLogger.GetKey() == "" {
			updateTime := traceLogger.GetTrace().UpdateTime
			if updateTime.Before(time.Now().Add(-timeout)) {
				logger.Debugf("delete unused tmp key: %s", tmpKey)
				d.tmpKeyMap.Delete(tmpKey)
			}
		}
		return true
	})
}

// ClearAll will clear all the trace from the cache
// please sync the cache before calling this function
func (d *DefaultTraceLoggerManager) ClearAll() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.clearCache()
	return nil
}

func (d *DefaultTraceLoggerManager) clearCache() {
	d.cache.Clear()
}

func (d *DefaultTraceLoggerManager) UpdateKey(key string, newKey string) error {

	// delete tmp key map
	val, ok := d.tmpKeyMap.LoadAndDelete(key)
	if !ok {
		return fmt.Errorf("key %s not found", key)
	}

	// update tmp key
	traceLogger := val.(TraceLogger)
	traceLogger.SetKey(newKey)
	d.tmpKeyMap.Store(newKey, traceLogger)
	return nil
}

func (d *DefaultTraceLoggerManager) Key(key string) TraceLogger {
	var traceLogger TraceLogger
	// use load or store
	val, _ := d.tmpKeyMap.LoadOrStore(key, d.NewTraceLogger())
	traceLogger = val.(TraceLogger)
	return traceLogger
}

func (d *DefaultTraceLoggerManager) KeyWithUpdate(key string) TraceLogger {
	var traceLogger TraceLogger
	// use load or store
	val, loaded := d.tmpKeyMap.LoadOrStore(key, d.NewTraceLogger())
	traceLogger = val.(TraceLogger)
	if !loaded {
		traceLogger.SetKey(key)
	}
	return traceLogger
}

func (d *DefaultTraceLoggerManager) AutoSync(duration time.Duration) error {
	d.autoSyncLock.Lock()
	defer d.autoSyncLock.Unlock()
	if d.autoSyncFlag {
		return fmt.Errorf("auto sync has started")
	}

	go func() {
		for {
			select {
			case <-d.autoSyncCancelChan:
				return
			case <-time.After(duration):
				logger.Infof("auto syncing")
				_ = d.SyncAll()
			}
		}
	}()

	return nil
}

func (d *DefaultTraceLoggerManager) CancelAutoSync() error {
	d.autoSyncLock.Lock()
	defer d.autoSyncLock.Unlock()

	if !d.autoSyncFlag {
		return fmt.Errorf("auto sync has not started")
	}
	d.autoSyncCancelChan <- struct{}{}
	return nil
}
