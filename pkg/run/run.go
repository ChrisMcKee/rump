// Package run manages Read, Write and Signal goroutines.
package run

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/chrismckee/rump/pkg/config"
	"github.com/chrismckee/rump/pkg/file"
	"github.com/chrismckee/rump/pkg/message"
	"github.com/chrismckee/rump/pkg/redis"
	"github.com/chrismckee/rump/pkg/signal"
	"github.com/mediocregopher/radix/v4"
)

// Exit helper
func exit(e error) {
	fmt.Println(e)
	os.Exit(1)
}

// Run orchestrate the Reader, Writer and Signal handler.
func Run(cfg config.Config) {
	// create ErrGroup to manage goroutines
	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)

	// Start signal handling goroutine
	g.Go(func() error {
		return signal.Run(gctx, cancel)
	})

	// Create shared message bus
	ch := make(message.Bus, 100)

	// Create and run either a Redis or File Source reader.
	if cfg.Source.IsRedis() {
		if cfg.Source.IsCluster() {
			source, err := redis.NewCluster(cfg.Source.FormattedString(), cfg, ch)
			if err != nil {
				exit(err)
			}

			g.Go(func() error {
				if err := source.Read(gctx); err != nil {
					log.Fatal(errors.Wrap(err, "reading from source"))
				}

				return err
			})
		} else {
			poolCfg := radix.PoolConfig{Size: 10}
			ctx := context.Background()
			sourcePool, err := poolCfg.New(ctx, "tcp", cfg.Source.FormattedString())
			if err != nil {
				exit(err)
			}
			source := redis.New(sourcePool, ch, cfg.Silent, cfg.TTL)
			g.Go(func() error {
				if err := source.Read(gctx); err != nil {
					log.Fatal(err)
				}
				return err
			})
		}
	} else {
		source := file.New(cfg.Source.String(), ch, cfg.Silent, cfg.TTL, cfg.MaxBuf)

		g.Go(func() error {
			return source.Read(gctx)
		})
	}

	// Create and run either a Redis or File Target writer.
	if cfg.Target.IsRedis() {
		if cfg.Target.IsCluster() {
			target, err := redis.NewCluster(cfg.Target.FormattedString(), cfg, ch)
			if err != nil {
				exit(err)
			}

			g.Go(func() error {
				defer cancel()
				return target.Write(gctx)
			})
		} else {
			poolCfg := radix.PoolConfig{Size: 10}
			ctx := context.Background()
			targetPool, err := poolCfg.New(ctx, "tcp", cfg.Target.FormattedString())
			if err != nil {
				exit(err)
			}
			target := redis.New(targetPool, ch, cfg.Silent, cfg.TTL)
			g.Go(func() error {
				defer cancel()
				return target.Write(gctx)
			})
		}
	} else {
		target := file.New(cfg.Target.String(), ch, cfg.Silent, cfg.TTL, cfg.MaxBuf)

		g.Go(func() error {
			defer cancel()
			return target.Write(gctx)
		})
	}

	// Block and wait for goroutines
	err := g.Wait()
	if err != nil && err != context.Canceled {
		exit(errors.Wrap(err, "error in process"))
	} else {
		fmt.Println("done")
	}
}
