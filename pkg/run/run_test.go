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

	"github.com/mediocregopher/radix/v4"

	"github.com/chrismckee/rump/pkg/config"
	"github.com/chrismckee/rump/pkg/run"
)

var (
	db1  radix.Client
	db2  radix.Client
	path string
)

func setup() {
	ctx := context.Background()
	poolCfg := radix.PoolConfig{Size: 1}
	db1, _ = poolCfg.New(ctx, "tcp", "redis://redis:6379/9")
	db2, _ = poolCfg.New(ctx, "tcp", "redis://redis:6379/10")
	path = "/app/dump.rump"

	// generate source test data on db1
	for i := 1; i <= 1; i++ {
		k := fmt.Sprintf("key%v", i)
		v := fmt.Sprintf("value%v", i)
		db1.Do(ctx, radix.Cmd(nil, "SET", k, v))
		db1.Do(ctx, radix.Cmd(nil, "PEXPIRE", k, "10000"))
	}
}

func teardown() {
	ctx := context.Background()
	// Reset test dbs
	db1.Do(ctx, radix.Cmd(nil, "FLUSHDB"))
	db2.Do(ctx, radix.Cmd(nil, "FLUSHDB"))
	// Delete dump file
	os.Remove(path)
}

func ExampleRun_redisToRedis() {
	setup()
	defer teardown()

	from, _ := url.Parse("redis://redis:6379/9")
	to, _ := url.Parse("redis://redis:6379/10")

	cfg := config.Config{
		Source: config.Resource{*from},
		Target: config.Resource{*to},
		Silent: false,
	}

	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToRedisTTL() {
	setup()
	defer teardown()

	from, _ := url.Parse("redis://redis:6379/9")
	to, _ := url.Parse("redis://redis:6379/10")

	cfg := config.Config{
		Source: config.Resource{*from},
		Target: config.Resource{*to},
		Silent: false,
		TTL:    true,
	}

	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToRedisSilent() {
	setup()
	defer teardown()

	from, _ := url.Parse("redis://redis:6379/9")
	to, _ := url.Parse("redis://redis:6379/10")

	cfg := config.Config{
		Source: config.Resource{*from},
		Target: config.Resource{*to},
		Silent: true,
	}

	run.Run(cfg)
	// Output:
	// signal: exit
	// done
}

func ExampleRun_redisToFile() {
	setup()
	defer teardown()

	from, _ := url.Parse("redis://redis:6379/9")
	to, _ := url.Parse("/app/dump.rump")

	cfg := config.Config{
		Source: config.Resource{*from},
		Target: config.Resource{*to},
	}

	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToFileTTL() {
	setup()
	defer teardown()

	from, _ := url.Parse("redis://redis:6379/9")
	to, _ := url.Parse("/app/dump.rump")

	cfg := config.Config{
		Source: config.Resource{*from},
		Target: config.Resource{*to},
		TTL:    true,
	}

	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_fileToRedis() {
	setup()
	defer teardown()

	from, _ := url.Parse("redis://redis:6379/9")
	to, _ := url.Parse("/app/dump.rump")

	cfgFileDump := config.Config{
		Source: config.Resource{*from},
		Target: config.Resource{*to},
	}
	run.Run(cfgFileDump)

	cfg := config.Config{
		Source: config.Resource{*to},
		Target: config.Resource{*from},
	}
	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
	// rw
	// signal: exit
	// done
}

func ExampleRun_fileToRedisTTL() {
	setup()
	defer teardown()

	from, _ := url.Parse("redis://redis:6379/9")
	to, _ := url.Parse("/app/dump.rump")

	cfgFileDump := config.Config{
		Source: config.Resource{*from},
		Target: config.Resource{*to},
	}
	run.Run(cfgFileDump)

	cfg := config.Config{
		Source: config.Resource{*to},
		Target: config.Resource{*from},
		TTL:    true,
	}
	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
	// rw
	// signal: exit
	// done
}
