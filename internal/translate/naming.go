// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Naming is how SlurmKube names what it creates for a Slurm job. The node
// name prefix is configurable because the site's gres.conf must list the
// dynamic nodes by name; the rest is fixed.
type Naming struct {
	// NodeNamePrefix precedes "<index>-<jobID>" ("slurmkube-w").
	NodeNamePrefix string
	nodeRE         *regexp.Regexp
}

const (
	DefaultNodeNamePrefix = "slurmkube-w"
	jobSetPrefix          = "slurmkube-"
	pinPrefix             = "slurmkube-pin-"
)

// NewNaming builds a Naming for the prefix ("" means the default).
func NewNaming(nodeNamePrefix string) Naming {
	if nodeNamePrefix == "" {
		nodeNamePrefix = DefaultNodeNamePrefix
	}
	return Naming{NodeNamePrefix: nodeNamePrefix,
		nodeRE: regexp.MustCompile(`^` + regexp.QuoteMeta(nodeNamePrefix) + `([0-9]+)-([0-9]+)$`)}
}

var defaultNodeRE = regexp.MustCompile(`^` + DefaultNodeNamePrefix + `([0-9]+)-([0-9]+)$`)

// prefix tolerates a zero Naming (the default prefix).
func (n Naming) prefix() string {
	if n.NodeNamePrefix == "" {
		return DefaultNodeNamePrefix
	}
	return n.NodeNamePrefix
}

func (n Naming) re() *regexp.Regexp {
	switch {
	case n.nodeRE != nil:
		return n.nodeRE
	case n.prefix() == DefaultNodeNamePrefix:
		return defaultNodeRE
	}
	return NewNaming(n.NodeNamePrefix).nodeRE
}

// JobSetName is the JobSet of job id (an array: its root ID).
func (Naming) JobSetName(id uint64) string { return jobSetPrefix + strconv.FormatUint(id, 10) }

// PinFeature is the feature the job is constrained to and its nodes
// advertise.
func (Naming) PinFeature(id uint64) string { return pinPrefix + strconv.FormatUint(id, 10) }

// IsPinFeature reports whether a constraint string is only a pin.
func (Naming) IsPinFeature(features string) bool {
	rest, ok := strings.CutPrefix(strings.TrimSpace(features), pinPrefix)
	if !ok || rest == "" {
		return false
	}
	_, err := strconv.ParseUint(rest, 10, 64)
	return err == nil
}

// NodeName is the dynamic node of pod index idx of job id.
func (n Naming) NodeName(id uint64, idx int32) string {
	return fmt.Sprintf("%s%d-%d", n.prefix(), idx, id)
}

// NodeNames lists the job's pods' nodes.
func (n Naming) NodeNames(id uint64, pods int32) []string {
	out := make([]string, pods)
	for i := range out {
		out[i] = n.NodeName(id, int32(i))
	}
	return out
}

// NodeNameEnvValue is the node name as the pod computes it from its
// completion index.
func (n Naming) NodeNameEnvValue(id uint64) string {
	return fmt.Sprintf("%s$(%s)-%d", n.prefix(), NodeIndexEnv, id)
}

// ParseNodeName splits a dynamic node name; ok is false for other names.
func (n Naming) ParseNodeName(name string) (id uint64, idx int32, ok bool) {
	m := n.re().FindStringSubmatch(name)
	if m == nil {
		return 0, 0, false
	}
	i, err1 := strconv.ParseInt(m[1], 10, 32)
	id, err2 := strconv.ParseUint(m[2], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return id, int32(i), true
}
