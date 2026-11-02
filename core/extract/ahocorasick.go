package extract

// Aho-Corasick multi-pattern search. Building one automaton over every literal
// fragment lets a single O(len(message)) pass report all literal occurrences and their
// positions — far cheaper than running each template's matcher over each line. The
// automaton is a byte trie augmented with "fail" links: when the next byte doesn't
// extend the current match, the fail link jumps to the longest proper suffix that is
// still a prefix of some pattern, so no input byte is ever re-scanned.
//
// This is stdlib-only and deliberately explicit (a learning goal). Nodes are indices
// into a slice rather than pointers, which keeps the whole automaton in one allocation
// and easy to reason about.

// acNode is one trie node. children maps a byte to the next node; fail is the suffix
// link; outputs holds the indices of every pattern that ends at this node (including
// those reachable via fail links, flattened at build time).
type acNode struct {
	children map[byte]int
	fail     int
	outputs  []int
}

type ahoCorasick struct {
	nodes    []acNode
	patterns []string

	// The search runs on a dense transition table compiled from the trie and its fail links
	// (a DFA): delta[state*numClasses+class[b]] is the state after reading byte b, with every
	// fail-link walk already folded in. The trie keeps maps because they make building it
	// easy to follow; the hot loop must not use them. Profiling the ingest path found ~70%
	// of extraction time in map lookups — one per input byte, more along fail chains.
	//
	// Bytes that occur in no pattern all behave alike (every state sends them to the root's
	// row for "no edge"), so they share class 0, and the table has one column per byte that
	// does occur. That keeps it to a few KB for realistic configs instead of 1 KB per state.
	class      [256]uint8
	numClasses int
	delta      []int32
}

// acHit is one occurrence of a pattern in the searched text: which pattern, and the
// byte index where it starts.
type acHit struct {
	pattern int
	start   int
}

// newAhoCorasick builds the automaton over the given distinct patterns. Node 0 is the
// root.
func newAhoCorasick(patterns []string) *ahoCorasick {
	ac := &ahoCorasick{patterns: patterns}
	ac.nodes = []acNode{{children: map[byte]int{}}} // root

	// Insert each pattern as a path from the root, marking its terminal node.
	for idx, p := range patterns {
		cur := 0
		for i := 0; i < len(p); i++ {
			b := p[i]
			nxt, ok := ac.nodes[cur].children[b]
			if !ok {
				nxt = len(ac.nodes)
				ac.nodes = append(ac.nodes, acNode{children: map[byte]int{}})
				ac.nodes[cur].children[b] = nxt
			}
			cur = nxt
		}
		ac.nodes[cur].outputs = append(ac.nodes[cur].outputs, idx)
	}

	ac.buildFailLinks()
	ac.compileDFA()
	return ac
}

// compileDFA fills class and delta from the trie and its fail links. The goto function is
// completed in breadth-first order: a missing edge from s on c leads wherever the same byte
// leads from fail(s), whose row is already final because a fail target is always shallower.
func (ac *ahoCorasick) compileDFA() {
	// Class 0 is every byte that no pattern contains; each byte that some pattern contains
	// gets its own class. If the patterns use all 256 byte values there is no byte left for
	// class 0, so that (unrealistic) config falls back to one class per byte value.
	var used [256]bool
	for _, p := range ac.patterns {
		for i := 0; i < len(p); i++ {
			used[p[i]] = true
		}
	}
	n := 1
	for b := 0; b < 256; b++ {
		if used[b] {
			n++
		}
	}
	identity := n > 256
	if identity {
		for b := 0; b < 256; b++ {
			ac.class[b] = uint8(b)
		}
		n = 256
	} else {
		next := 1
		for b := 0; b < 256; b++ {
			if used[b] {
				ac.class[b] = uint8(next)
				next++
			}
		}
	}
	ac.numClasses = n

	// Representative byte per class, to look up trie edges (class 0 has no edges anywhere).
	rep := make([]int, n)
	for i := range rep {
		rep[i] = -1
	}
	for b := 255; b >= 0; b-- {
		if used[b] || identity {
			rep[ac.class[b]] = b
		}
	}

	ac.delta = make([]int32, len(ac.nodes)*n)
	const root = 0
	order := make([]int, 0, len(ac.nodes))
	order = append(order, root)
	for i := 0; i < len(order); i++ { // breadth-first, so fail(s) precedes s
		s := order[i]
		row := ac.delta[s*n : (s+1)*n]
		for c := 0; c < n; c++ {
			if b := rep[c]; b >= 0 {
				if child, ok := ac.nodes[s].children[byte(b)]; ok {
					row[c] = int32(child)
					order = append(order, child)
					continue
				}
			}
			if s == root {
				row[c] = root
			} else {
				row[c] = ac.delta[ac.nodes[s].fail*n+c]
			}
		}
	}
}

// buildFailLinks computes the fail link and flattened outputs for every node via a
// breadth-first pass, so that by the time a node is processed its fail target is
// already finalized.
func (ac *ahoCorasick) buildFailLinks() {
	const root = 0
	var queue []int
	for _, u := range ac.nodes[root].children {
		ac.nodes[u].fail = root
		queue = append(queue, u)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for b, u := range ac.nodes[cur].children {
			queue = append(queue, u)
			// Follow fail links from cur's fail node until one has an edge on b
			// (or we reach the root).
			f := ac.nodes[cur].fail
			for f != root {
				if _, ok := ac.nodes[f].children[b]; ok {
					break
				}
				f = ac.nodes[f].fail
			}
			if nf, ok := ac.nodes[f].children[b]; ok && nf != u {
				ac.nodes[u].fail = nf
			} else {
				ac.nodes[u].fail = root
			}
			// Flatten: u also emits everything its fail node emits.
			ac.nodes[u].outputs = append(ac.nodes[u].outputs, ac.nodes[ac.nodes[u].fail].outputs...)
		}
	}
}

// search returns every pattern occurrence in text (one acHit per occurrence).
func (ac *ahoCorasick) search(text string) []acHit {
	return ac.searchInto(nil, text)
}

// searchInto appends every pattern occurrence in text to dst and returns it, so a caller can
// reuse one slice across lines. One table lookup per byte: the fail-link walks of the
// textbook formulation are already folded into delta (compileDFA).
func (ac *ahoCorasick) searchInto(dst []acHit, text string) []acHit {
	n := ac.numClasses
	delta, class := ac.delta, &ac.class
	cur := int32(0)
	for i := 0; i < len(text); i++ {
		cur = delta[int(cur)*n+int(class[text[i]])]
		if outs := ac.nodes[cur].outputs; len(outs) > 0 {
			for _, pidx := range outs {
				dst = append(dst, acHit{pattern: pidx, start: i - len(ac.patterns[pidx]) + 1})
			}
		}
	}
	return dst
}
