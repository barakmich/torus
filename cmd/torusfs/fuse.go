package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/spf13/cobra"

	torusfuse "github.com/coreos/torus/fs/fuse"
)

var (
	userMount bool
)

var fuseCommand = &cobra.Command{
	Use:    "fuse VOLUME PATH",
	Short:  "Mount a multi-writer volume",
	PreRun: fusePreRun,
	Run:    fuseAction,
}

func init() {
	fuseCommand.Flags().BoolVarP(&userMount, "user-mount", "", false, "Mount FUSE under normal user account only")
}

func fusePreRun(cmd *cobra.Command, args []string) {
	if os.Geteuid() != 0 {
		userMount = true
	}
}

func fuseAction(cmd *cobra.Command, args []string) {
	if len(args) < 2 {
		cmd.Usage()
		os.Exit(1)
	}
	srv := createServer()
	defer srv.Close()
	vol := args[0]
	mnt := args[1]
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")
			os.Exit(0)
		}
	}()

	fsSrv, err := srv.FS()
	if err != nil {
		fmt.Println("server doesn't support filesystems:", err)
		os.Exit(0)
	}
	torusfuse.MustMount(mnt, vol, fsSrv, !userMount)
}
