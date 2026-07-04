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
	return ac
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
	const root = 0
	var hits []acHit
	cur := root
	for i := 0; i < len(text); i++ {
		b := text[i]
		// Follow fail links until b extends the current state (or we're at root).
		for cur != root {
			if _, ok := ac.nodes[cur].children[b]; ok {
				break
			}
			cur = ac.nodes[cur].fail
		}
		if nxt, ok := ac.nodes[cur].children[b]; ok {
			cur = nxt
		}
		for _, pidx := range ac.nodes[cur].outputs {
			hits = append(hits, acHit{pattern: pidx, start: i - len(ac.patterns[pidx]) + 1})
		}
	}
	return hits
}
