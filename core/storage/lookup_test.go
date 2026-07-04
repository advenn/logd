package storage

import (
	"testing"
)

func TestNewLookupTable_Empty(t *testing.T) {
	lt := NewLookupTable()

	// ID 1 should not exist in a fresh table.
	_, ok := lt.GetName(1)
	if ok {
		t.Error("fresh LookupTable should not have any entries")
	}
}

func TestGetOrCreate_SequentialIDs(t *testing.T) {
	lt := NewLookupTable()

	id1 := lt.GetOrCreate("django_app")
	id2 := lt.GetOrCreate("celery_worker")
	id3 := lt.GetOrCreate("celery_beat")

	if id1 != 1 {
		t.Errorf("first ID = %d, want 1", id1)
	}
	if id2 != 2 {
		t.Errorf("second ID = %d, want 2", id2)
	}
	if id3 != 3 {
		t.Errorf("third ID = %d, want 3", id3)
	}
}

func TestGetOrCreate_ExistingName(t *testing.T) {
	lt := NewLookupTable()

	id1 := lt.GetOrCreate("django_app")
	id2 := lt.GetOrCreate("django_app")

	if id1 != id2 {
		t.Errorf("GetOrCreate for same name returned %d then %d, want identical", id1, id2)
	}
}

func TestGetName_ValidID(t *testing.T) {
	lt := NewLookupTable()
	lt.GetOrCreate("django_app")
	lt.GetOrCreate("celery_worker")

	name, ok := lt.GetName(1)
	if !ok {
		t.Fatal("GetName(1) returned false, want true")
	}
	if name != "django_app" {
		t.Errorf("GetName(1) = %q, want %q", name, "django_app")
	}

	name, ok = lt.GetName(2)
	if !ok {
		t.Fatal("GetName(2) returned false, want true")
	}
	if name != "celery_worker" {
		t.Errorf("GetName(2) = %q, want %q", name, "celery_worker")
	}
}

func TestGetName_InvalidID(t *testing.T) {
	lt := NewLookupTable()
	lt.GetOrCreate("django_app")

	_, ok := lt.GetName(999)
	if ok {
		t.Error("GetName(999) should return false for non-existent ID")
	}

	_, ok = lt.GetName(0)
	if ok {
		t.Error("GetName(0) should return false for reserved ID 0")
	}
}

func TestSerializeDeserialize(t *testing.T) {
	lt := NewLookupTable()
	lt.GetOrCreate("django_app")
	lt.GetOrCreate("celery_worker")
	lt.GetOrCreate("celery_beat")

	data, err := lt.Serialize()
	if err != nil {
		t.Fatalf("Serialize() returned error: %v", err)
	}

	restored, err := DeserializeLookupTable(data)
	if err != nil {
		t.Fatalf("DeserializeLookupTable() returned error: %v", err)
	}

	// Verify all entries survived the roundtrip.
	for id := uint16(1); id <= 3; id++ {
		origName, ok := lt.GetName(id)
		if !ok {
			t.Fatalf("original table missing ID %d", id)
		}
		restoredName, ok := restored.GetName(id)
		if !ok {
			t.Errorf("restored table missing ID %d", id)
		}
		if restoredName != origName {
			t.Errorf("ID %d: restored name = %q, want %q", id, restoredName, origName)
		}
	}
}

func TestSerializeDeserialize_EmptyTable(t *testing.T) {
	lt := NewLookupTable()

	data, err := lt.Serialize()
	if err != nil {
		t.Fatalf("Serialize() returned error: %v", err)
	}

	restored, err := DeserializeLookupTable(data)
	if err != nil {
		t.Fatalf("DeserializeLookupTable() returned error: %v", err)
	}

	_, ok := restored.GetName(1)
	if ok {
		t.Error("deserialized empty table should not have any entries")
	}
}

func TestRoundtrip_NewIDsAfterRestore(t *testing.T) {
	lt := NewLookupTable()
	lt.GetOrCreate("service_a") // ID 1
	lt.GetOrCreate("service_b") // ID 2

	data, err := lt.Serialize()
	if err != nil {
		t.Fatalf("Serialize() returned error: %v", err)
	}

	restored, err := DeserializeLookupTable(data)
	if err != nil {
		t.Fatalf("DeserializeLookupTable() returned error: %v", err)
	}

	// New entries in the restored table should continue from the correct next ID.
	id := restored.GetOrCreate("service_c")
	if id != 3 {
		t.Errorf("new ID after restore = %d, want 3", id)
	}

	// Existing names should still resolve to their original IDs.
	id = restored.GetOrCreate("service_a")
	if id != 1 {
		t.Errorf("GetOrCreate(service_a) after restore = %d, want 1", id)
	}
}

func TestDeserialize_MalformedData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"only count byte", []byte{0x00}},
		{"count=1 but no entries", []byte{0x00, 0x01}},
		{"truncated entry header", []byte{0x00, 0x01, 0x00, 0x01, 0x00}},
		{"name extends past data", []byte{0x00, 0x01, 0x00, 0x01, 0x00, 0x05, 0x41}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DeserializeLookupTable(tt.data)
			if err == nil {
				t.Error("expected error for malformed data, got nil")
			}
		})
	}
}

func TestDeserialize_ReservedIDZero(t *testing.T) {
	// Craft data with ID 0 which is reserved.
	// Format: [count=1][id=0][name_len=4]["test"]
	data := []byte{
		0x00, 0x01, // count = 1
		0x00, 0x00, // id = 0 (reserved)
		0x00, 0x04, // name_len = 4
		't', 'e', 's', 't',
	}

	_, err := DeserializeLookupTable(data)
	if err == nil {
		t.Error("expected error for reserved ID 0, got nil")
	}
}
