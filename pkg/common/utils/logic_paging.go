package utils

import (
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
)

var (
	// maximum number of lines loaded from the apiserver
	lineReadLimit int64 = 5000
	// maximum number of bytes loaded from the apiserver
	byteReadLimit int64 = 500000
)

type LogPage struct {
	LogFilePosition string
	LineLimit       int
	SizeLimit       int64
	HasNextPage     bool
	Truncated       bool
}

// Paging divided content into pages
func (l *LogPage) Paging(content string, logContentLineNum int) string {
	content = strings.TrimLeft(content, " ")
	content = strings.TrimRight(content, " ")
	if content == "" {
		return ""
	}
	if logContentLineNum == 0 {
		logContentLineNum = len(strings.Split(strings.TrimRight(content, "\n"), "\n"))
	}
	contentLength := len(content)
	log.Debugf("Paging: get logs index, bytesLoaded %d, lineNumber %d, LogFilePosition %s, "+
		"lineLimit %d, sizeLimit %d", int64(contentLength), logContentLineNum, l.LogFilePosition, l.LineLimit, l.SizeLimit)
	startIndex, endIndex := l.Dividing(logContentLineNum, contentLength)
	// split log
	finalContent := SplitLog(content, startIndex, endIndex, false)
	return finalContent
}

// SlicePaging divided slice into pages
func (l *LogPage) SlicePaging(content []string) []string {
	var contentByteLength int
	for _, c := range content {
		contentByteLength += len(c)
	}
	logContentLineNum := len(content)
	log.Debugf("slicePaging: get logs index, bytesLoaded %d, lineNumber %d, LogFilePosition %s, "+
		"lineLimit %d, sizeLimit %d", contentByteLength, logContentLineNum, l.LogFilePosition, l.LineLimit, l.SizeLimit)
	startIndex, endIndex := l.Dividing(logContentLineNum, contentByteLength)
	log.Debugf("startIndex %d, endIndex %d", startIndex, endIndex)
	var finalContent []string
	if startIndex == -1 && endIndex == -1 {
		finalContent = content[:]
	} else if startIndex == -1 {
		finalContent = content[:endIndex]
	} else if endIndex == -1 {
		finalContent = content[startIndex:]
	} else {
		finalContent = content[startIndex:endIndex]
	}
	return finalContent
}

// Dividing get startIndex and endIndex of contents, by
func (l *LogPage) Dividing(contentLineNum, contentByteLength int) (int, int) {
	startIndex := -1
	endIndex := -1
	// hasNextPage means has more content in next page or lines
	hasNextPage := false
	// truncated means oversize content been truncated
	truncated := false
	limitFlag := IsReadLimitReached(int64(contentByteLength), int64(contentLineNum), l.LogFilePosition)

	lineLimit := l.LineLimit
	if int64(contentByteLength) > l.SizeLimit {
		sizeLimitLines := int((l.SizeLimit * int64(contentLineNum)) / int64(contentByteLength))
		if sizeLimitLines < lineLimit {
			lineLimit = sizeLimitLines
		}
	}
	// overFlag means line range out of line limit, but contentLineNum is a number but range
	// overFlag := false
	// split logs
	switch l.LogFilePosition {
	case common.EndFilePosition:
		startIndex = contentLineNum - lineLimit
		endIndex = -1
		if startIndex <= 0 {
			startIndex = -1
			truncated = limitFlag
		} else {
			hasNextPage = true
		}
		if endIndex == contentLineNum {
			endIndex = -1
		}
	case common.BeginFilePosition:
		startIndex = 0
		if lineLimit < contentLineNum {
			endIndex = lineLimit
			hasNextPage = true
		} else {
			truncated = limitFlag
		}
	}
	l.HasNextPage = hasNextPage
	l.Truncated = truncated
	return startIndex, endIndex
}

func SplitLog(logContent string, startIndex, endIndex int, overFlag bool) string {
	if overFlag || logContent == "" {
		return ""
	}
	logContent = strings.TrimRight(logContent, "\n")
	var logLines []string
	if startIndex == -1 && endIndex == -1 {
		logLines = strings.Split(logContent, "\n")[:]
	} else if startIndex == -1 {
		logLines = strings.Split(logContent, "\n")[:endIndex]
	} else if endIndex == -1 {
		logLines = strings.Split(logContent, "\n")[startIndex:]
	} else {
		logLines = strings.Split(logContent, "\n")[startIndex:endIndex]
	}
	return strings.Join(logLines, "\n") + "\n"
}

// IsReadLimitReached checks logs if truncated by apiServer
func IsReadLimitReached(bytesLoaded int64, linesLoaded int64, logFilePosition string) bool {
	return (logFilePosition == common.BeginFilePosition && bytesLoaded >= byteReadLimit) ||
		(logFilePosition == common.EndFilePosition && linesLoaded >= lineReadLimit)
}
