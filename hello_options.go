package chunkdb

import (
	"bytes"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ServerLimits reports negotiated ceilings without implying feature support.
type ServerLimits struct {
	MaxAreaChunks, MaxAreaResponseBytes              uint64
	TxnMaxWrittenChunks, TxnMaxReadChunks            uint64
	MaxWatches, MaxReturningRows, MaxExpressionNodes uint64
}

// Supports reports a protocol capability advertised by the server.
func (i *ServerInfo) Supports(feature string) bool {
	return i != nil && slices.Contains(i.Features, feature)
}

func sessionOptions(options map[string]any) (map[string]any, error) {
	normalized := make(map[string]any, len(options))
	for key, value := range options {
		name := strings.ToLower(key)
		if !isName(name) {
			return nil, requestErrorf("HELLO", "invalid option name %q", key)
		}
		if _, exists := normalized[name]; exists {
			return nil, requestErrorf("HELLO", "duplicate option %s", name)
		}
		switch name {
		case "client_name":
			text, ok := value.(string)
			if !ok || !utf8.ValidString(text) {
				return nil, requestErrorf("HELLO", "client_name requires UTF-8 text")
			}
		case "statement_timeout_ms":
			negative, _, ok := integerOf(value)
			if !ok || negative {
				return nil, requestErrorf("HELLO", "statement_timeout_ms requires nonnegative milliseconds")
			}
		case "default_tag":
			data, ok := value.([]byte)
			if !ok || len(data) > 256 {
				return nil, requestErrorf("HELLO", "default_tag requires at most 256 bytes")
			}
		}
		switch v := value.(type) {
		case []byte:
			normalized[name] = append([]byte{}, v...)
		case string:
			if !utf8.ValidString(v) {
				return nil, requestErrorf("HELLO", "option %s requires valid UTF-8", name)
			}
			normalized[name] = v
		case bool, nil:
			normalized[name] = v
		default:
			if _, _, ok := integerOf(value); !ok {
				return nil, requestErrorf("HELLO", "unsupported option value for %s", name)
			}
			normalized[name] = value
		}
	}
	return normalized, nil
}

func helloOptions(statement string, params [][]byte, options map[string]any) (string, [][]byte) {
	if len(options) == 0 {
		return statement, params
	}
	names := slices.Sorted(maps.Keys(options))
	assignments := make([]string, 0, len(names))
	for _, name := range names {
		var value string
		switch v := options[name].(type) {
		case string:
			params = append(params, []byte(v))
			value = "$" + strconv.Itoa(len(params))
		case []byte:
			params = append(params, v)
			value = "$" + strconv.Itoa(len(params))
		case bool:
			if v {
				value = "TRUE"
			} else {
				value = "FALSE"
			}
		case nil:
			value = "NULL"
		default:
			negative, magnitude, _ := integerOf(v)
			value = formatInteger(negative, magnitude)
		}
		assignments = append(assignments, name+" = "+value)
	}
	return statement + " WITH " + strings.Join(assignments, ", "), params
}

func parseHelloExtensions(reply Reply, info *ServerInfo) error {
	bad := func(key string) error { return protocolErrorf("HELLO", "invalid %s", key) }
	features, hasFeatures := reply.Lookup("features")
	if hasFeatures {
		if features.Kind != ReplyArray {
			return bad("features")
		}
		for _, feature := range features.Array {
			if feature.Kind != ReplyBulk || !utf8.Valid(feature.Bulk) {
				return bad("features")
			}
			info.Features = append(info.Features, string(feature.Bulk))
		}
	}
	required := info.Supports("protocol_foundation")
	for key, target := range map[string]*uint64{"connection_id": &info.ConnectionID, "idle_timeout_ms": &info.IdleTimeoutMS} {
		r, exists := reply.Lookup(key)
		if !exists {
			if required {
				return bad(key)
			}
			continue
		}
		n, ok := r.Uint64()
		if !ok || (key == "connection_id" && n == 0) {
			return bad(key)
		}
		*target = n
	}
	options, exists := reply.Lookup("options")
	if exists {
		if options.Kind != ReplyMap {
			return bad("options")
		}
		info.Options = make(map[string]Reply, len(options.Map))
		for _, pair := range options.Map {
			if pair.Key.Kind != ReplyBulk {
				return bad("options key")
			}
			name := string(pair.Key.Bulk)
			if _, found := info.Options[name]; found {
				return bad("duplicate options key")
			}
			switch name {
			case "client_name":
				if pair.Value.Kind != ReplyBulk || !utf8.Valid(pair.Value.Bulk) {
					return bad(name)
				}
			case "statement_timeout_ms":
				if _, ok := pair.Value.Uint64(); !ok {
					return bad(name)
				}
			case "default_tag":
				if pair.Value.Kind != ReplyBulk || len(pair.Value.Bulk) > 256 {
					return bad(name)
				}
			}
			info.Options[name] = pair.Value
		}
	} else if required {
		return bad("options")
	}
	notApplied, exists := reply.Lookup("not_applied")
	if exists {
		if notApplied.Kind != ReplyMap {
			return bad("not_applied")
		}
		info.NotApplied = make(map[string]string, len(notApplied.Map))
		for _, pair := range notApplied.Map {
			if pair.Key.Kind != ReplyBulk || pair.Value.Kind != ReplyMap {
				return bad("not_applied entry")
			}
			reason, found := pair.Value.Lookup("reason")
			if !found || reason.Kind != ReplyBulk {
				return bad("not_applied reason")
			}
			name := string(pair.Key.Bulk)
			if _, found := info.NotApplied[name]; found {
				return bad("duplicate not_applied key")
			}
			info.NotApplied[name] = string(reason.Bulk)
		}
	} else if required {
		return bad("not_applied")
	}
	limits, exists := reply.Lookup("limits")
	if exists {
		if limits.Kind != ReplyMap {
			return bad("limits")
		}
		for key, target := range map[string]*uint64{"max_area_chunks": &info.Limits.MaxAreaChunks, "max_area_response_bytes": &info.Limits.MaxAreaResponseBytes, "txn_max_written_chunks": &info.Limits.TxnMaxWrittenChunks, "txn_max_read_chunks": &info.Limits.TxnMaxReadChunks, "max_watches": &info.Limits.MaxWatches, "max_returning_rows": &info.Limits.MaxReturningRows, "max_expression_nodes": &info.Limits.MaxExpressionNodes} {
			r, exists := limits.Lookup(key)
			n, ok := r.Uint64()
			if !exists || !ok || n == 0 {
				return bad("limits " + key)
			}
			*target = n
		}
	} else if required {
		return bad("limits")
	}
	return nil
}

func cloneReply(r Reply) Reply {
	r.Bulk = bytes.Clone(r.Bulk)
	r.Keys = maps.Clone(r.Keys)
	if r.BigNumber != nil {
		r.BigNumber, _ = r.BigInt()
	}
	r.Array = slices.Clone(r.Array)
	for i := range r.Array {
		r.Array[i] = cloneReply(r.Array[i])
	}
	r.Map = slices.Clone(r.Map)
	for i := range r.Map {
		r.Map[i].Key = cloneReply(r.Map[i].Key)
		r.Map[i].Value = cloneReply(r.Map[i].Value)
	}
	return r
}
