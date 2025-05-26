package proton

import (
	"database/sql"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.com/chenziliang/dataloader/models"
	"go.uber.org/zap"
)

func (ch *proton) loadTraceData(source *models.Source, wg *sync.WaitGroup) {
	if source.Settings.Table == "" {
		source.Settings.Table = "default.otel_traces"
	}

	ch.logger.Info("target table", zap.String("table", source.Settings.Table))

	if err := ch.newTraceTable(source.Settings.Table, source.Settings.CleanBeforeLoad); err != nil {
		return
	}

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go ch.doLoadTraceData(source, wg, i)
	}
}

func (ch *proton) doLoadTraceData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	var currentIteration int32

	start := time.Now().UnixNano()
	prev := start

	for {
		records := models.GenerateTraces(source.Settings.BatchSize)

		atomic.AddUint64(&ch.ingested, (uint64)(len(records)*len(records[0])))
		atomic.AddUint64(&ch.ingested_total, (uint64)(len(records)*len(records[0])))

		if err := ch.doTraceSpanInsert(records, source.Settings.Table, source.Type); err != nil {
			time.Sleep(2 * time.Second)
		}

		now := time.Now().UnixNano()
		if now-prev >= 2*1000*1000*1000 && i == 0 {
			current_duration_ms := uint64((now - prev) / 1000000)
			current_ingested := atomic.LoadUint64(&ch.ingested)

			ingested_total := atomic.LoadUint64(&ch.ingested_total)
			duration_total_ms := uint64((now - start) / 1000000)

			/// reset to 0
			atomic.StoreUint64(&ch.ingested, 0)

			ch.logger.Info("ingest trace spans", zap.Uint64("ingested", current_ingested), zap.Uint64("duration_ms", current_duration_ms), zap.Uint64("eps", (current_ingested*1000)/current_duration_ms), zap.Uint64("ingested_total", ingested_total), zap.Uint64("duration_total_ms", duration_total_ms), zap.Uint64("overall_eps", (ingested_total*1000)/duration_total_ms))

			prev = now
		}

		if source.Settings.Iteration > 0 && currentIteration >= source.Settings.Iteration {
			break
		}

		if source.Settings.Interval > 0 {
			time.Sleep(time.Duration(source.Settings.Interval) * time.Millisecond)
		}
	}
}

func (ch *proton) doTraceSpanInsert(records [][]models.TraceSpan, table, typ string) error {
	query := "INSERT INTO " + table + " (trace_id, span_id, parent_span_id, name, start_time, end_time, attributes) VALUES (?, ?, ?, ?, ?, ?, ?)"

	return ch.doInsert(
		func(stmt *sql.Stmt) (int, error) {
			for _, trace_spans := range records {
				for _, trace_span := range trace_spans {
					attrs, _ := json.Marshal(trace_span.Attributes)
					_, err := stmt.Exec(
						trace_span.TraceID,
						trace_span.SpanID,
						trace_span.ParentSpanID,
						trace_span.Name,
						trace_span.StartTime,
						trace_span.EndTime,
						attrs,
					)
					if err != nil {
						ch.logger.Error("failed to insert records", zap.String("type", typ), zap.Error(err))
						return 0, err
					}

				}
			}

			return len(records) * len(records[0]), nil
		},
		query,
		typ,
	)
}

func (ch *proton) newTraceTable(table string, cleanBeforeLoad bool) error {
	if cleanBeforeLoad {
		if _, err := ch.db.Exec(`DROP STREAM IF EXISTS ` + table); err != nil {
			ch.logger.Error("failed to drop table", zap.String("table", table), zap.Error(err))
			return err
		}
		ch.logger.Info("dropped trace table")
	}

	if _, err := ch.db.Exec(`DESCRIBE STREAM ` + table); err == nil {
		ch.logger.Info("table already exists", zap.String("table", table))
		return nil
	}

	///	) SETTINGS shards=1, replication_factor=3, storage_type='streaming';
	/// ) settings shards=3, storage_type='memory'
	_, err := ch.db.Exec(`
		CREATE STREAM IF NOT EXISTS ` + table + ` (
			trace_id string,
			span_id string,
			parent_span_id string,
			name string,
			start_time datetime64(3, 'UTC'),
			end_time datetime64(3, 'UTC'),
			attributes json
		) SETTINGS sharding_expr=weak_hash32(trace_id), shards=8;
	`)
	if err != nil {
		ch.logger.Error("failed to create table", zap.String("table", table), zap.Error(err))
	}

	time.Sleep(2 * time.Second)
	return err
}
