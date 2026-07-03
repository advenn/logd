package template

import (
	"testing"

	"github.com/advenn/logd/internal/config"
)

// ---- Compilation tests ----

func TestCompileUint32(t *testing.T) {
	engine, err := NewEngine([]config.Template{
		{
			Name:    "order_created",
			Pattern: "order-{order_id:uint32} created",
			Fields: []config.FieldConfig{
				{Name: "order_id", Type: "uint32", Index: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	matches := engine.Match("order-1234 created")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].FieldName != "order_id" || matches[0].Value != "1234" {
		t.Errorf("got field=%s value=%s, want order_id=1234", matches[0].FieldName, matches[0].Value)
	}

	// No match for non-matching message.
	if m := engine.Match("something else"); len(m) != 0 {
		t.Errorf("expected no matches, got %d", len(m))
	}
}

func TestCompileUint64(t *testing.T) {
	engine, err := NewEngine([]config.Template{
		{
			Name:    "big_id",
			Pattern: "txn-{txn_id:uint64} done",
			Fields: []config.FieldConfig{
				{Name: "txn_id", Type: "uint64", Index: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	matches := engine.Match("txn-18446744073709551615 done")
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].Value != "18446744073709551615" {
		t.Errorf("got %s, want 18446744073709551615", matches[0].Value)
	}
}

func TestCompileString(t *testing.T) {
	engine, err := NewEngine([]config.Template{
		{
			Name:    "payment",
			Pattern: "payment-{payment_id:string} status={status:enum}",
			Fields: []config.FieldConfig{
				{Name: "payment_id", Type: "string", Index: true},
				{Name: "status", Type: "enum", Index: true, Values: []string{"pending", "completed", "failed"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	matches := engine.Match("payment-PAY-001 status=completed")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matches))
	}

	found := make(map[string]string)
	for _, m := range matches {
		found[m.FieldName] = m.Value
	}
	if found["payment_id"] != "PAY-001" {
		t.Errorf("payment_id: got %q, want %q", found["payment_id"], "PAY-001")
	}
	if found["status"] != "completed" {
		t.Errorf("status: got %q, want %q", found["status"], "completed")
	}

	// Non-matching enum value.
	if m := engine.Match("payment-PAY-002 status=refunded"); len(m) != 0 {
		t.Errorf("expected no matches for invalid enum value, got %d", len(m))
	}
}

func TestCompileMultipleTemplates(t *testing.T) {
	engine, err := NewEngine([]config.Template{
		{
			Name:    "order",
			Pattern: "order-{order_id:uint32} created",
			Fields:  []config.FieldConfig{{Name: "order_id", Type: "uint32", Index: true}},
		},
		{
			Name:    "driver",
			Pattern: "driver-{driver_id:uint32} assigned",
			Fields:  []config.FieldConfig{{Name: "driver_id", Type: "uint32", Index: true}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m1 := engine.Match("order-100 created")
	if len(m1) != 1 || m1[0].FieldName != "order_id" {
		t.Errorf("order template: got %v", m1)
	}

	m2 := engine.Match("driver-42 assigned")
	if len(m2) != 1 || m2[0].FieldName != "driver_id" {
		t.Errorf("driver template: got %v", m2)
	}

	// No template matches.
	if m := engine.Match("random log message"); len(m) != 0 {
		t.Errorf("expected no matches, got %d", len(m))
	}
}

func TestNonIndexedFields(t *testing.T) {
	engine, err := NewEngine([]config.Template{
		{
			Name:    "order",
			Pattern: "order-{order_id:uint32} status={status:enum}",
			Fields: []config.FieldConfig{
				{Name: "order_id", Type: "uint32", Index: true},
				{Name: "status", Type: "enum", Index: false, Values: []string{"new", "done"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	matches := engine.Match("order-500 status=done")
	// Only order_id is indexed.
	if len(matches) != 1 {
		t.Fatalf("expected 1 indexed match, got %d", len(matches))
	}
	if matches[0].FieldName != "order_id" {
		t.Errorf("got %s, want order_id", matches[0].FieldName)
	}
}

func TestCompileErrors(t *testing.T) {
	tests := []struct {
		name    string
		tmpl    config.Template
		wantErr string
	}{
		{
			name:    "empty pattern",
			tmpl:    config.Template{Name: "t", Pattern: "", Fields: []config.FieldConfig{{Name: "f", Type: "string"}}},
			wantErr: "pattern is required",
		},
		{
			name:    "no fields",
			tmpl:    config.Template{Name: "t", Pattern: "hello", Fields: nil},
			wantErr: "at least one field is required",
		},
		{
			name:    "bad type",
			tmpl:    config.Template{Name: "t", Pattern: "{f:float64}", Fields: []config.FieldConfig{{Name: "f", Type: "float64"}}},
			wantErr: "unsupported field type",
		},
		{
			name:    "enum without values",
			tmpl:    config.Template{Name: "t", Pattern: "{f:enum}", Fields: []config.FieldConfig{{Name: "f", Type: "enum"}}},
			wantErr: "enum type requires values",
		},
		{
			name: "duplicate field",
			tmpl: config.Template{Name: "t", Pattern: "{f:string}",
				Fields: []config.FieldConfig{
					{Name: "f", Type: "string"},
					{Name: "f", Type: "string"},
				}},
			wantErr: "duplicate field",
		},
		{
			name:    "unclosed brace",
			tmpl:    config.Template{Name: "t", Pattern: "hello {f:string", Fields: []config.FieldConfig{{Name: "f", Type: "string"}}},
			wantErr: "unclosed '{'",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewEngine([]config.Template{tc.tmpl})
			if err == nil {
				t.Fatalf("expected error containing %q", tc.wantErr)
			}
			if !contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestEmptyTemplates(t *testing.T) {
	engine, err := NewEngine(nil)
	if err != nil {
		t.Fatal(err)
	}
	if matches := engine.Match("anything"); len(matches) != 0 {
		t.Errorf("expected no matches, got %d", len(matches))
	}
}

// ---- IndexBuilder tests ----

func TestIndexBuilderAddAndFlushUint32(t *testing.T) {
	tmpDir := t.TempDir()

	ib := NewIndexBuilder([]FieldDescriptor{
		{Name: "order_id", Type: FieldTypeUint32, Indexed: true},
	})

	ib.Add("order_id", "1234", 1, 5)
	ib.Add("order_id", "1234", 1, 6)
	ib.Add("order_id", "5678", 1, 5)
	ib.Add("order_id", "1234", 2, 3)

	if err := ib.Flush(tmpDir); err != nil {
		t.Fatal(err)
	}

	// Read back and verify.
	ir, err := OpenIndexReader(tmpDir, "order_id")
	if err != nil {
		t.Fatal(err)
	}

	refs, err := ir.Lookup("1234")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Errorf("expected 3 refs for 1234, got %d", len(refs))
	}

	refs, err = ir.Lookup("5678")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Errorf("expected 1 ref for 5678, got %d", len(refs))
	}

	refs, err = ir.Lookup("9999")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Errorf("expected 0 refs for 9999, got %d", len(refs))
	}
}

func TestIndexBuilderStringField(t *testing.T) {
	tmpDir := t.TempDir()

	ib := NewIndexBuilder([]FieldDescriptor{
		{Name: "payment_id", Type: FieldTypeString, Indexed: true},
	})

	ib.Add("payment_id", "PAY-001", 1, 1)
	ib.Add("payment_id", "PAY-002", 1, 2)
	ib.Add("payment_id", "PAY-001", 2, 1)

	if err := ib.Flush(tmpDir); err != nil {
		t.Fatal(err)
	}

	ir, err := OpenIndexReader(tmpDir, "payment_id")
	if err != nil {
		t.Fatal(err)
	}

	refs, err := ir.LookupString("PAY-001")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 {
		t.Errorf("expected 2 refs for PAY-001, got %d", len(refs))
	}

	refs, err = ir.LookupString("PAY-999")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}
}

func TestIndexBuilderMergeOnFlush(t *testing.T) {
	tmpDir := t.TempDir()

	// First segment.
	ib1 := NewIndexBuilder([]FieldDescriptor{
		{Name: "order_id", Type: FieldTypeUint32, Indexed: true},
	})
	ib1.Add("order_id", "100", 1, 1)
	if err := ib1.Flush(tmpDir); err != nil {
		t.Fatal(err)
	}

	// Second segment — should merge with existing.
	ib2 := NewIndexBuilder([]FieldDescriptor{
		{Name: "order_id", Type: FieldTypeUint32, Indexed: true},
	})
	ib2.Add("order_id", "200", 2, 1)
	if err := ib2.Flush(tmpDir); err != nil {
		t.Fatal(err)
	}

	// Verify both values are in the index.
	ir, err := OpenIndexReader(tmpDir, "order_id")
	if err != nil {
		t.Fatal(err)
	}

	refs, err := ir.Lookup("100")
	if err != nil || len(refs) != 1 {
		t.Errorf("expected 1 ref for 100, got %d (err=%v)", len(refs), err)
	}

	refs, err = ir.Lookup("200")
	if err != nil || len(refs) != 1 {
		t.Errorf("expected 1 ref for 200, got %d (err=%v)", len(refs), err)
	}
}

func TestIndexBuilderDeduplicateRefs(t *testing.T) {
	tmpDir := t.TempDir()

	ib := NewIndexBuilder([]FieldDescriptor{
		{Name: "order_id", Type: FieldTypeUint32, Indexed: true},
	})

	// Same value, same page ref twice.
	ib.Add("order_id", "42", 1, 3)
	ib.Add("order_id", "42", 1, 3)

	if err := ib.Flush(tmpDir); err != nil {
		t.Fatal(err)
	}

	ir, err := OpenIndexReader(tmpDir, "order_id")
	if err != nil {
		t.Fatal(err)
	}

	refs, err := ir.Lookup("42")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Errorf("expected 1 deduplicated ref, got %d", len(refs))
	}
}

func TestIndexBuilderNonexistentIndex(t *testing.T) {
	tmpDir := t.TempDir()
	_, err := OpenIndexReader(tmpDir, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent index")
	}
}

func TestIndexedFields(t *testing.T) {
	engine, err := NewEngine([]config.Template{
		{
			Name:    "order",
			Pattern: "order-{order_id:uint32} status={status:enum}",
			Fields: []config.FieldConfig{
				{Name: "order_id", Type: "uint32", Index: true},
				{Name: "status", Type: "enum", Index: false, Values: []string{"new", "done"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	fields := engine.IndexedFields()
	if len(fields) != 1 {
		t.Fatalf("expected 1 indexed field, got %d", len(fields))
	}
	if fields[0].Name != "order_id" {
		t.Errorf("got %s, want order_id", fields[0].Name)
	}
}

// ---- IndexReader typed lookup tests ----

func TestIndexReaderUint32Lookup(t *testing.T) {
	tmpDir := t.TempDir()

	ib := NewIndexBuilder([]FieldDescriptor{
		{Name: "order_id", Type: FieldTypeUint32, Indexed: true},
	})
	ib.Add("order_id", "42", 1, 1)
	ib.Add("order_id", "100", 1, 2)
	ib.Add("order_id", "65535", 2, 1)
	ib.Flush(tmpDir)

	ir, err := OpenIndexReader(tmpDir, "order_id")
	if err != nil {
		t.Fatal(err)
	}

	refs, err := ir.LookupUint32(42)
	if err != nil || len(refs) != 1 {
		t.Errorf("expected 1 ref for 42, got %d (err=%v)", len(refs), err)
	}

	refs, err = ir.LookupUint32(99999)
	if err != nil || len(refs) != 0 {
		t.Errorf("expected 0 refs, got %d", len(refs))
	}
}

func TestIndexBuilderEnumField(t *testing.T) {
	tmpDir := t.TempDir()

	ib := NewIndexBuilder([]FieldDescriptor{
		{Name: "status", Type: FieldTypeEnum, Indexed: true},
	})

	ib.Add("status", "completed", 1, 1)
	ib.Add("status", "pending", 1, 2)
	ib.Add("status", "completed", 1, 3)

	if err := ib.Flush(tmpDir); err != nil {
		t.Fatal(err)
	}

	ir, err := OpenIndexReader(tmpDir, "status")
	if err != nil {
		t.Fatal(err)
	}

	refs, err := ir.LookupString("completed")
	if err != nil || len(refs) != 2 {
		t.Errorf("expected 2 refs for completed, got %d", len(refs))
	}

	refs, err = ir.LookupString("pending")
	if err != nil || len(refs) != 1 {
		t.Errorf("expected 1 ref for pending, got %d", len(refs))
	}
}

// ---- helpers ----

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
