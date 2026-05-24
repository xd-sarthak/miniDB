package utils

import "runtime"

// IntSize provides the size of an int
var IntSize = 8

// init checks the architecture and sets the IntSize accordingly
// On 32-bit architectures, the size of an int is 4 bytes, while on 64-bit architectures, it is 8 bytes.
func init() {
	if runtime.GOARCH == "386" || runtime.GOARCH == "arm" {
		IntSize = 4
	}
}