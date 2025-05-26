package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type TraceSpan struct {
	TraceID      string                 `json:"trace_id"`
	SpanID       string                 `json:"span_id"`
	ParentSpanID *string                `json:"parent_span_id"` // nil for root
	Name         string                 `json:"name"`
	StartTime    string                 `json:"start_time"`
	EndTime      string                 `json:"end_time"`
	Attributes   map[string]interface{} `json:"attributes"`
}

// Utilities

func randomID(length int) string {
	letters := []rune("abcdef0123456789")
	id := make([]rune, length)
	for i := range id {
		id[i] = letters[rand.Intn(len(letters))]
	}
	return string(id)
}

func isoTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func createSpan(traceID, name, service, component string, parent *string, attrs map[string]interface{}, start time.Time, duration time.Duration) (TraceSpan, time.Time, string) {
	spanID := randomID(8)
	end := start.Add(duration)

	// Merge attributes
	attributes := map[string]interface{}{
		"service.name": service,
		"component":    component,
	}
	for k, v := range attrs {
		attributes[k] = v
	}

	return TraceSpan{
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parent,
		Name:         name,
		StartTime:    isoTime(start),
		EndTime:      isoTime(end),
		Attributes:   attributes,
	}, end, spanID
}

func generateTrace() []TraceSpan {
	traceID := randomID(16)
	now := time.Now()

	var spans []TraceSpan

	// Proxy
	span1, t1End, span1ID := createSpan(
		traceID,
		"Frontend Proxy - Receive Request", "frontend-proxy", "proxy",
		nil,
		map[string]interface{}{
			"http.method": "GET",
			"http.url":    "/checkout",
		},
		now,
		time.Millisecond*time.Duration(rand.Intn(10)+5),
	)
	spans = append(spans, span1)

	// Frontend
	span2, t2End, span2ID := createSpan(
		traceID,
		"Frontend - Handle Checkout", "frontend", "http",
		&span1ID,
		map[string]interface{}{
			"http.method": "GET",
			"http.route":  "/checkout",
			"user.id":     "12345",
		},
		t1End,
		time.Millisecond*time.Duration(rand.Intn(30)+90),
	)
	spans = append(spans, span2)

	// Checkout
	span3, t3End, span3ID := createSpan(
		traceID,
		"Checkout - Process Order", "checkout", "checkout",
		&span2ID,
		map[string]interface{}{
			"http.method": "POST",
			"http.route":  "/process",
			"order.id":    fmt.Sprintf("ORD-%04d", rand.Intn(9999)),
		},
		t2End.Add(-90*time.Millisecond),
		time.Millisecond*time.Duration(rand.Intn(20)+80),
	)
	spans = append(spans, span3)

	// Product Catalog
	span4, _, _ := createSpan(
		traceID,
		"ProductCatalog - Get Products", "product-catalog", "product-catalog",
		&span3ID,
		map[string]interface{}{
			"http.method":   "GET",
			"http.route":    "/products",
			"catalog.query": "all",
		},
		t3End.Add(-60*time.Millisecond),
		time.Millisecond*time.Duration(rand.Intn(10)+15),
	)
	spans = append(spans, span4)

	// Payment
	span5, _, _ := createSpan(
		traceID,
		"Payment - Process Payment", "payment", "payment",
		&span3ID,
		map[string]interface{}{
			"http.method":      "POST",
			"http.route":       "/pay",
			"payment.amount":   "49.99",
			"payment.currency": "USD",
		},
		t3End.Add(-30*time.Millisecond),
		time.Millisecond*time.Duration(rand.Intn(20)+25),
	)
	spans = append(spans, span5)

	return spans
}

func main() {
	for i := 0; i < 3; i++ {
		trace := generateTrace()

		traceJSON, err := json.MarshalIndent(trace, "", "  ")
		if err != nil {
			fmt.Println("Error marshaling trace:", err)
			continue
		}

		fmt.Println(string(traceJSON))
		fmt.Println()
	}
}
