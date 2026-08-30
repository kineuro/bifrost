// bifrost: the command line client of kineuro's data bridge.
//
//	bifrost login <token>            remember a bridge token (or set BIFROST_TOKEN)
//	bifrost push <folder or file>    send data over the bridge (parallel, resumable, verified)
//	bifrost pull <folder>            fetch what kineuro prepared for you (parallel, resumable, verified)
//	bifrost ls [path]                list files on the bridge
//	bifrost status                   what this bridge allows, and how much has been used
//	bifrost verify <folder>          compare a local folder with what the bridge holds
//	bifrost update                   install the newest version
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(mainCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "login":
		err = cmdLogin(args)
	case "logout":
		err = cmdLogout()
	case "push":
		err = cmdPush(ctx, args)
	case "pull":
		err = cmdPull(ctx, args)
	case "ls":
		err = cmdLs(args)
	case "status":
		err = cmdStatus()
	case "verify":
		err = cmdVerify(ctx, args)
	case "update":
		err = cmdUpdate()
	case "version", "--version", "-v":
		fmt.Println("bifrost", version)
	case "help", "--help", "-h":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(1)
	}
	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "\ninterrupted; run the same command again to resume")
			os.Exit(130)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		if strings.HasPrefix(err.Error(), "transfer finished with") {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`bifrost ` + version + `: the data bridge of Experimental Neuroradiology Research at Karolinska Institutet

Usage:
  bifrost login <token>                 remember your bridge token (asks for the passcode if the bridge has one)
  bifrost push <folder|file> [options]  send data over the bridge
  bifrost pull <folder> [options]       fetch the data prepared for you into <folder>
  bifrost ls [path]                     list what is on the bridge
  bifrost status                        what the bridge allows and how much is used
  bifrost verify <folder>               compare a local folder with the bridge, byte for byte
  bifrost update                        install the newest version
  bifrost version

Options for push and pull:
  --to <path>        push: put the data under this folder on the bridge (default: the folder's own name)
  --from <path>      pull: fetch only this folder from the bridge
  --workers <n>      parallel streams (default 6, the bridge caps it)
  --limit <rate>     bandwidth cap, e.g. 50M (bytes per second, K/M/G suffix)
  --exclude <glob>   skip matching paths (repeatable, e.g. --exclude '*.tmp' --exclude '.git/*')
  --checksum         push: hash every local file first and skip only exact matches (default: size match is enough)
  --dry-run          show what would be transferred, transfer nothing
  --json             one JSON line per event, for scripts

Environment: BIFROST_TOKEN (instead of login), BIFROST_URL (default https://bifrost.kineuro.se), BIFROST_PASSCODE.
Exit codes: 0 done, 2 some files failed (run again to resume), 130 interrupted (run again to resume).
`)
}
