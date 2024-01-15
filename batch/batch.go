package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/timeplus-io/proton-go-driver/v2"
)

var wg sync.WaitGroup

func example(addr string) error {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{addr},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			// Debug:           true,
			DialTimeout:     time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	)

	if err != nil {
		return err
	}

	ctx = proton.Context(ctx, proton.WithProgress(func(p *proton.Progress) {
		fmt.Println("progress: ", p)

	}))

	//if err := conn.Exec(ctx, `DROP STREAM IF EXISTS example`); err != nil {
	//    return err
	//

	if err := conn.Exec(ctx, `
        CREATE STREAM IF NOT EXISTS example (
              Col1 uint64, Col2 string
            ) SETTINGS shards=1, replication_factor=3`); err != nil {
		return err
	}

	println(addr)
	time.Sleep(time.Second * 2)

	buffer, err := conn.PrepareStreamingBuffer(ctx, "INSERT INTO example (* except _tp_time)")
	if err != nil {
		return err
	}
	defer buffer.Close()

	for i := 0; i < 60; i++ {
		for j := 0; j < 100000; j++ {
			if err := buffer.Append(
				uint64(i*100000+j),
				fmt.Sprintf("num_%d_%d", j, i),
			); err != nil {
				return err
			}
		}

		if err := buffer.Send(); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	start := time.Now()

	wg.Add(3)

	cluster := []string{
		"127.0.0.1:18463",
		"127.0.0.1:28463",
		"127.0.0.1:38463",
	}

	for _, addr := range cluster {
		addr := addr
		go func() {
			defer wg.Done()
			if err := example(addr); err != nil {
				log.Println(addr, err)
			}
		}()

	}
	wg.Wait()

	fmt.Println(time.Since(start))

}
