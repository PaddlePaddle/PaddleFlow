package util

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	uuidMaxLength   = 32
	defaultIDLength = 8
)

func UriEncode(uri string, encodeSlash bool) string {
	var byteBuf bytes.Buffer
	for _, b := range []byte(uri) {
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') ||
			b == '-' || b == '_' || b == '.' || b == '~' || (b == '/' && !encodeSlash) {
			byteBuf.WriteByte(b)
		} else {
			byteBuf.WriteString(fmt.Sprintf("%%%02X", b))
		}
	}
	return byteBuf.String()
}

func GenerateID(Prefix string) string {
	return GenerateIDWithLength(Prefix, defaultIDLength)
}

func GenerateIDWithLength(Prefix string, Len int) string {
	if Len > uuidMaxLength {
		Len = uuidMaxLength
	}
	uuidStr := strings.ToLower(Prefix) + "-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:Len]
	return uuidStr
}
