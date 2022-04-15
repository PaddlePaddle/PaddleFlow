package util

import (
	"bytes"
	"fmt"
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
