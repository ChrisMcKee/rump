package main

import (
	"github.com/chrismckee/rump/pkg/config"
	"github.com/chrismckee/rump/pkg/run"
)

func main() {
	// parse config flags, will exit in case of errors.
	cfg := config.Parse()

	run.Run(cfg)
}
