// Package file_test is the end-to-end test for Rump.
// Since we need the real Redis DUMP protocol to test files,
// we use pkg/redis as a data generator, violating Unit Testing
// principles.
package file_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/mediocregopher/radix/v4"

	"github.com/chrismckee/rump/pkg/file"
	"github.com/chrismckee/rump/pkg/message"
	"github.com/chrismckee/rump/pkg/redis"
)

var (
	db1      radix.Client
	db2      radix.Client
	ch       message.Bus
	expected map[string]string
	path     string
	ctx      context.Context
)

func setup() {
	ctx = context.Background()
	poolCfg := radix.PoolConfig{Size: 1}
	db1, _ = poolCfg.New(ctx, "tcp", "redis://redis:6379/5")
	db2, _ = poolCfg.New(ctx, "tcp", "redis://redis:6379/6")
	ch = make(message.Bus, 100)
	expected = make(map[string]string)
	path = "/app/dump.rump"

	// generate source test data
	for i := 1; i <= 20; i++ {
		k := fmt.Sprintf("key%v", i)
		v := fmt.Sprintf("value%v", i)
		db1.Do(ctx, radix.Cmd(nil, "SET", k, v))
		expected[k] = v
	}
}

func teardown() {
	// Reset test dbs
	db1.Do(ctx, radix.Cmd(nil, "FLUSHDB"))
	db2.Do(ctx, radix.Cmd(nil, "FLUSHDB"))
	// Delete dump file
	os.Remove(path)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func TestWriteRead(t *testing.T) {
	// Read all keys from db1, push to shared message bus
	source := redis.New(db1, ch, false, false)
	if err := source.Read(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Write rump dump from shared message bus
	target := file.New(path, ch, false, false, 65536)
	if err := target.Write(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Create second channel to test reading from file
	ch2 := make(message.Bus, 100)

	// Read rump dump file
	source2 := file.New(path, ch2, false, false, 65536)
	if err := source2.Read(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Write from shared message bus to db2
	target2 := redis.New(db2, ch2, false, false)
	if err := target2.Write(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Get all db2 keys
	result := map[string]string{}
	var v string
	for k := range expected {
		db2.Do(ctx, radix.Cmd(&v, "GET", k))
		result[k] = v
	}

	// Compare db1 keys with db2 keys
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected: %v, result: %v", expected, result)
	}
}
