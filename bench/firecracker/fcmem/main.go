// fcmem keeps Firecracker's guest memory in a bucket: `save` puts a snapshot there, `serve` is
// the page-fault handler a restore from it runs.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: fcmem save|serve [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "save":
		err = save(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	default:
		err = fmt.Errorf("no command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "fcmem:", err)
		os.Exit(1)
	}
}
