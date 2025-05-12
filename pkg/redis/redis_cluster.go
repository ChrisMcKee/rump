// Package redis allows reading/writing from/to a Redis DB.
package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"

	"github.com/mediocregopher/radix/v4"
	"github.com/pkg/errors"

	"github.com/chrismckee/rump/pkg/config"
	"github.com/chrismckee/rump/pkg/message"
)

// Redis holds references to a DB pool and a shared message bus.
// Silent disables verbose mode.
// TTL enables TTL sync.
type RedisCluster struct {
	Cluster *radix.Cluster
	Bus     message.Bus
	Silent  bool
	TTL     bool
}

// New creates the Redis struct, used to read/write.
func NewCluster(addr string, cfg config.Config, bus message.Bus) (*RedisCluster, error) {
	ctx := context.Background()

	password, _ := cfg.Source.User.Password()
	dialer := radix.Dialer{
		AuthPass: password,
	}
	if cfg.Source.IsSecure() {
		dialer.NetDialer = &tls.Dialer{
			NetDialer: &net.Dialer{},
			Config: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
			},
		}
	}

	poolCfg := radix.PoolConfig{
		Dialer: dialer,
		Size:   10, // adjust as needed
	}
	clusterCfg := radix.ClusterConfig{
		PoolConfig: poolCfg,
	}

	vanillaCluster, err := clusterCfg.New(ctx, []string{addr})
	if err != nil {
		log.Fatalf("Error preparing for benchmark, while creating new connection. error = %v", err)
	}

	// do a PING and make sure you are connected
	var pong string
	if err := vanillaCluster.Do(ctx, radix.Cmd(&pong, "PING")); err != nil {
		return nil, errors.Wrap(err, "error in checking source connectivity")
	}

	// Issue CLUSTER SLOTS command (Sync)
	err = vanillaCluster.Sync(ctx)
	if err != nil {
		log.Fatalf("Error preparing for benchmark, while issuing CLUSTER SLOTS. error = %v", err)
	}

	return &RedisCluster{
		Cluster: vanillaCluster,
		Bus:     bus,
		Silent:  cfg.Silent,
		TTL:     cfg.TTL,
	}, nil
}

// maybeLog may log, depending on the Silent flag
func (r *RedisCluster) maybeLog(s string) {
	if r.Silent {
		return
	}
	fmt.Print(s)
}

// maybeTTL may sync the TTL, depending on the TTL flag
func (r *RedisCluster) maybeTTL(key string) (string, error) {
	if !r.TTL {
		return "0", nil
	}
	var ttl string
	ctx := context.Background()
	err := r.Cluster.Do(ctx, radix.Cmd(&ttl, "PTTL", key))
	if err != nil {
		return ttl, err
	}
	if ttl == "-1" {
		ttl = "0"
	}
	return ttl, nil
}

// Read gently scans an entire Redis DB for keys, then dumps
// the key/value pair (Payload) on the message Bus channel.
// It leverages implicit pipelining to speedup large DB reads.
// To be used in an ErrGroup.
func (r *RedisCluster) Read(ctx context.Context) error {
	defer close(r.Bus)

	scanner := (radix.ScannerConfig{}).NewMulti(r.Cluster)

	var key string
	var value string
	var ttl string

	for scanner.Next(ctx, &key) {
		err := r.Cluster.Do(ctx, radix.Cmd(&value, "DUMP", key))
		if err != nil {
			return err
		}
		ttl, err = r.maybeTTL(key)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("redis read: exit")
			return ctx.Err()
		case r.Bus <- message.Payload{Key: key, Value: value, TTL: ttl}:
			r.maybeLog("r")
		}
	}

	return scanner.Close()
}

// Write restores keys on the db as they come on the message bus.
func (r *RedisCluster) Write(ctx context.Context) error {
	for r.Bus != nil {
		select {
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("redis write: exit")
			return ctx.Err()
		case p, ok := <-r.Bus:
			if !ok {
				r.Bus = nil
				continue
			}
			err := r.Cluster.Do(ctx, radix.Cmd(nil, "RESTORE", p.Key, p.TTL, p.Value, "REPLACE"))
			if err != nil {
				return err
			}
			r.maybeLog("w")
		}
	}
	return nil
}
