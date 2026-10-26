// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"fmt"
	"strconv"
	"strings"
)

// ExpandHostlist expands a Slurm hostlist ("gpu-[1-3,7]", "a,b[01-02]c")
// into host names. One bracket group per element; zero padding is kept.
func ExpandHostlist(spec string) ([]string, error) {
	var out []string
	for _, elem := range splitHostlist(spec) {
		elem = strings.TrimSpace(elem)
		if elem == "" {
			continue
		}
		open := strings.IndexByte(elem, '[')
		if open < 0 {
			if strings.ContainsAny(elem, "]") {
				return nil, fmt.Errorf("invalid hostlist element %q", elem)
			}
			out = append(out, elem)
			continue
		}
		closeIdx := strings.IndexByte(elem, ']')
		if closeIdx < open {
			return nil, fmt.Errorf("invalid hostlist element %q", elem)
		}
		prefix, ranges, suffix := elem[:open], elem[open+1:closeIdx], elem[closeIdx+1:]
		if strings.ContainsAny(suffix, "[]") {
			return nil, fmt.Errorf("nested brackets in %q are not supported", elem)
		}
		for _, r := range strings.Split(ranges, ",") {
			lo, hi, isRange := strings.Cut(r, "-")
			if !isRange {
				hi = lo
			}
			a, errA := strconv.Atoi(lo)
			b, errB := strconv.Atoi(hi)
			if errA != nil || errB != nil || b < a {
				return nil, fmt.Errorf("invalid range %q in %q", r, elem)
			}
			width := len(lo)
			for i := a; i <= b; i++ {
				out = append(out, fmt.Sprintf("%s%0*d%s", prefix, width, i, suffix))
			}
		}
	}
	return out, nil
}

// splitHostlist splits on commas outside brackets.
func splitHostlist(spec string) []string {
	var parts []string
	depth, start := 0, 0
	for i, c := range spec {
		switch c {
		case '[':
			depth++
		case ']':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, spec[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, spec[start:])
}
