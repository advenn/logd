package label

import "testing"

func TestCardinalityApply(t *testing.T) {
	c := NewCardinality(2) // cap 2 distinct values per key

	keep := func(name string, in Set, wantLen int) {
		t.Helper()
		if got := c.Apply(in); len(got) != wantLen {
			t.Fatalf("%s: kept %d pairs, want %d (%v)", name, len(got), wantLen, got)
		}
	}
	keep("eu (1st)", NewSet(map[string]string{"region": "eu"}), 1)
	keep("us (2nd)", NewSet(map[string]string{"region": "us"}), 1)
	keep("ap (3rd, over cap)", NewSet(map[string]string{"region": "ap"}), 0) // dropped
	keep("eu again (known)", NewSet(map[string]string{"region": "eu"}), 1)   // still kept
	keep("de (over cap)", NewSet(map[string]string{"region": "de"}), 0)

	if ck := c.CappedKeys(); len(ck) != 1 || ck[0] != "region" {
		t.Fatalf("CappedKeys = %v, want [region]", ck)
	}
	if b := c.Breaches(); b["region"] != 2 { // ap + de dropped
		t.Fatalf("Breaches[region] = %d, want 2", b["region"])
	}
}

// A capped key drops only its over-cap value; other keys in the same set are unaffected.
func TestCardinalityPerKey(t *testing.T) {
	c := NewCardinality(1)
	c.Apply(NewSet(map[string]string{"a": "1", "b": "1"})) // a=1, b=1 both kept (1st each)
	got := c.Apply(NewSet(map[string]string{"a": "1", "b": "2"}))
	// a=1 known (kept); b=2 is a's... no: b=2 is new for b which is at cap → b dropped, a kept.
	if len(got) != 1 || got[0].Key != "a" || got[0].Value != "1" {
		t.Fatalf("expected only a=1 kept, got %v", got)
	}
	if ck := c.CappedKeys(); len(ck) != 1 || ck[0] != "b" {
		t.Fatalf("only b should be capped, got %v", ck)
	}
}

// A cap of 0 (or below) disables the cap.
func TestCardinalityDisabled(t *testing.T) {
	c := NewCardinality(0)
	for _, v := range []string{"a", "b", "c", "d"} {
		if got := c.Apply(NewSet(map[string]string{"k": v})); len(got) != 1 {
			t.Fatalf("disabled cap dropped %q", v)
		}
	}
	if len(c.CappedKeys()) != 0 {
		t.Fatal("disabled cap should never mark a key capped")
	}
}

// Drops counts each dropped pair, per key and per value of the scope key, across the
// per-segment caps that feed it; a cap that is not given one counts nothing beyond itself.
func TestCardinalityDropsCounted(t *testing.T) {
	d := NewDrops("tenant")
	set := func(tenant, region string) Set {
		m := map[string]string{"region": region}
		if tenant != "" {
			m["tenant"] = tenant
		}
		return NewSet(m)
	}
	for seg := 0; seg < 2; seg++ { // two segments: the cap starts afresh, the count does not
		c := NewCardinality(2)
		c.CountDropsIn(d)
		for _, s := range []Set{set("a", "r0"), set("a", "r1"), set("a", "r2"), set("b", "r3"), set("b", "r0"), set("", "r4")} {
			c.Apply(s)
		}
	}
	got := d.Snapshot()
	// Per segment: r0 and r1 are kept; r2 (a), r3 (b) and r4 (no tenant) dropped; and the
	// tenant key itself never passes two values. Twice over.
	want := map[string]map[string]int64{"a": {"region": 2}, "b": {"region": 2}, "": {"region": 2}}
	if len(got) != len(want) {
		t.Fatalf("Snapshot = %v, want %v", got, want)
	}
	for scope, byKey := range want {
		for k, n := range byKey {
			if got[scope][k] != n || len(got[scope]) != len(byKey) {
				t.Fatalf("Snapshot = %v, want %v", got, want)
			}
		}
	}

	// A cap with no Drops attached still caps, and counts nothing outside itself.
	c := NewCardinality(1)
	c.Apply(set("a", "r0"))
	c.Apply(set("a", "r1"))
	if b := c.Breaches(); b["region"] != 1 {
		t.Fatalf("Breaches = %v", b)
	}
	if got := d.Snapshot(); got["a"]["region"] != 2 {
		t.Fatalf("an unattached cap changed the count: %v", got)
	}
	// The snapshot is a copy.
	got["a"]["region"] = 99
	if d.Snapshot()["a"]["region"] != 2 {
		t.Fatal("Snapshot shares its maps with the counter")
	}
}
