package redis_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/chrismckee/rump/pkg/redis"

	"github.com/mediocregopher/radix/v4"

	"github.com/chrismckee/rump/pkg/message"
)

var (
	db1      radix.Client
	db2      radix.Client
	ch       message.Bus
	expected map[string]string
)

func setup(t *testing.T) {
	ctx := context.Background()
	poolCfg := radix.PoolConfig{Size: 1}
	var err error
	db1, err = poolCfg.New(ctx, "tcp", "redis://localhost:6379/3")
	if err != nil {
		t.Fatalf("failed to create db1: %v", err)
	}
	db2, err = poolCfg.New(ctx, "tcp", "redis://localhost:6379/4")
	if err != nil {
		t.Fatalf("failed to create db2: %v", err)
	}
	expected = make(map[string]string)

	// generate source test data on db1
	for i := 1; i <= 20; i++ {
		k := fmt.Sprintf("key%v", i)
		v := fmt.Sprintf("value%v", i)
		err := db1.Do(ctx, radix.Cmd(nil, "SET", k, v))
		if err != nil {
			return
		}
		err2 := db1.Do(ctx, radix.Cmd(nil, "PEXPIRE", k, "30000"))
		if err2 != nil {
			return
		}
		expected[k] = v
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
}

func TestMain(m *testing.M) {
	t := &testing.T{}
	setup(t)
	code := m.Run()
	teardown(t)
	os.Exit(code)
}

// Test db1 to db2 sync
func TestReadWrite(t *testing.T) {
	setup(t)
	defer teardown(t)
	ch = make(message.Bus, 100)

	source := redis.New(db1, ch, false, false)
	target := redis.New(db2, ch, false, false)
	ctx := context.Background()

	// Read all keys from db1, push to shared message bus
	if err := source.Read(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Write all keys from message bus to db2
	if err := target.Write(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Get all db2 keys
	result := map[string]string{}
	var v string
	for k := range expected {
		err := db2.Do(ctx, radix.Cmd(&v, "GET", k))
		if err != nil {
			return
		}
		result[k] = v
	}

	// Compare db1 keys with db2 keys
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected: %v, result: %v", expected, result)
	}
}

//// Test db1 to db2 sync with TTL
//func TestReadWriteTTL(t *testing.T) {
//	ch = make(message.Bus, 100)
//	source := redis.New(db1, ch, false, true)
//	target := redis.New(db2, ch, false, true)
//	ctx := context.Background()
//
//	// Read all keys from db1, push to shared message bus
//	if err := source.Read(ctx); err != nil {
//		t.Error("error: ", err)
//	}
//
//	// Write all keys from message bus to db2
//	if err := target.Write(ctx); err != nil {
//		t.Error("error: ", err)
//	}
//
//	// Get all db2 keys
//	result := map[string]string{}
//	var v string
//	var ttl string
//	for k := range expected {
//		db2.Do(radix.Cmd(&v, "GET", k))
//		db2.Do(radix.Cmd(&ttl, "PTTL", k))
//		if ttl == "0" {
//			t.Errorf("ttl non transferred")
//		}
//		result[k] = v
//	}
//
//	// Compare db1 keys with db2 keys
//	if !reflect.DeepEqual(expected, result) {
//		t.Errorf("expected: %v, result: %v", expected, result)
//	}
//}
