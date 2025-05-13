// Example to test the command line output.
// Expected Redis monitor output: redis-cli -h redis monitor
// OK
// "SELECT" "9"
// "SELECT" "10"
// "SET" "key1" "value1"
// "SELECT" "9"
// "SELECT" "10"
// "SCAN" "0"
// "DUMP" "key1"
//
//	"RESTORE" "key1" "0" "..." "REPLACE"
//
// "FLUSHDB"
//
//	"FLUSHDB"
package run_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/mediocregopher/radix/v4"

	"github.com/chrismckee/rump/pkg/config"
	"github.com/chrismckee/rump/pkg/run"
)

var (
	db1  radix.Client
	db2  radix.Client
	path string
)

func setup(t *testing.T) {
	ctx := context.Background()
	poolCfg := radix.PoolConfig{Size: 1}
	var err error
	db1, err = poolCfg.New(ctx, "tcp", "redis://localhost:6379/9")
	if err != nil {
		t.Fatalf("failed to create db1: %v", err)
	}
	db2, err = poolCfg.New(ctx, "tcp", "redis://localhost:6379/10")
	if err != nil {
		t.Fatalf("failed to create db2: %v", err)
	}
	path = "dump.rump"

	// generate source test data on db1
	for i := 1; i <= 1; i++ {
		k := fmt.Sprintf("key%v", i)
		v := fmt.Sprintf("value%v", i)
		err := db1.Do(ctx, radix.Cmd(nil, "SET", k, v))
		if err != nil {
			return
		}
		err2 := db1.Do(ctx, radix.Cmd(nil, "PEXPIRE", k, "10000"))
		if err2 != nil {
			return
		}
	}
}

func teardown(t *testing.T) {
	ctx := context.Background()
	if err := db1.Do(ctx, radix.Cmd(nil, "FLUSHDB")); err != nil {
		t.Fatalf("failed to flush db1: %v", err)
	}
	if err := db2.Do(ctx, radix.Cmd(nil, "FLUSHDB")); err != nil {
		t.Fatalf("failed to flush db2: %v", err)
	}
	_ = os.Remove(path)
}

func TestMain(m *testing.M) {
	t := &testing.T{}
	setup(t)
	code := m.Run()
	teardown(t)
	os.Exit(code)
}

func exampleSetupTeardown(t *testing.T, fn func()) {
	setup(t)
	defer teardown(t)
	fn()
}

func ExampleRun_redisToRedis() {
	t := &testing.T{}
	exampleSetupTeardown(t, func() {
		from, _ := url.Parse("redis://localhost:6379/9")
		to, _ := url.Parse("redis://localhost:6379/10")

		cfg := config.Config{
			Source: config.Resource{URL: *from},
			Target: config.Resource{URL: *to},
			Silent: false,
		}

		run.Run(cfg)
	})
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToRedisTTL() {
	t := &testing.T{}
	exampleSetupTeardown(t, func() {
		from, _ := url.Parse("redis://localhost:6379/9")
		to, _ := url.Parse("redis://localhost:6379/10")

		cfg := config.Config{
			Source: config.Resource{URL: *from},
			Target: config.Resource{URL: *to},
			Silent: false,
			TTL:    true,
		}

		run.Run(cfg)
	})
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToRedisSilent() {
	t := &testing.T{}
	exampleSetupTeardown(t, func() {
		from, _ := url.Parse("redis://localhost:6379/9")
		to, _ := url.Parse("redis://localhost:6379/10")

		cfg := config.Config{
			Source: config.Resource{URL: *from},
			Target: config.Resource{URL: *to},
			Silent: true,
		}

		run.Run(cfg)
	})
	// Output:
	// signal: exit
	// done
}

func ExampleRun_redisToFile() {
	t := &testing.T{}
	exampleSetupTeardown(t, func() {
		from, _ := url.Parse("redis://localhost:6379/9")
		to, _ := url.Parse("dump.rump")

		cfg := config.Config{
			Source: config.Resource{URL: *from},
			Target: config.Resource{URL: *to},
		}

		run.Run(cfg)
	})
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToFileTTL() {
	t := &testing.T{}
	exampleSetupTeardown(t, func() {
		from, _ := url.Parse("redis://localhost:6379/9")
		to, _ := url.Parse("dump.rump")

		cfg := config.Config{
			Source: config.Resource{URL: *from},
			Target: config.Resource{URL: *to},
			TTL:    true,
		}

		run.Run(cfg)
	})
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_fileToRedis() {
	t := &testing.T{}
	exampleSetupTeardown(t, func() {
		from, _ := url.Parse("redis://localhost:6379/9")
		to, _ := url.Parse("dump.rump")

		cfgFileDump := config.Config{
			Source: config.Resource{URL: *from},
			Target: config.Resource{URL: *to},
		}
		run.Run(cfgFileDump)

		cfg := config.Config{
			Source: config.Resource{URL: *to},
			Target: config.Resource{URL: *from},
		}
		run.Run(cfg)
	})
	// Output:
	// rw
	// signal: exit
	// done
	// rw
	// signal: exit
	// done
}

func ExampleRun_fileToRedisTTL() {
	t := &testing.T{}
	exampleSetupTeardown(t, func() {
		from, _ := url.Parse("redis://localhost:6379/9")
		to, _ := url.Parse("dump.rump")

		cfgFileDump := config.Config{
			Source: config.Resource{URL: *from},
			Target: config.Resource{URL: *to},
		}
		run.Run(cfgFileDump)

		cfg := config.Config{
			Source: config.Resource{URL: *to},
			Target: config.Resource{URL: *from},
			TTL:    true,
		}
		run.Run(cfg)
	})
	// Output:
	// rw
	// signal: exit
	// done
	// rw
	// signal: exit
	// done
}
