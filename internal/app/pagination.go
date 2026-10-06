package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"net/http"
	"strconv"

	"github.com/livepeer/clearinghouse/internal/store"
)

const defaultListLimit = 100
const maxListLimit = 1000

type listCursor struct {
	Version      int    `json:"version"`
	Kind         string `json:"kind"`
	GrantID      string `json:"grant_id"`
	AllocationID string `json:"allocation_id"`
	ManifestID   string `json:"manifest_id,omitempty"`
	Seq          int64  `json:"seq"`
}

func (c listCursor) encode() string {
	data, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(data)
}

func managementListQuery(r *http.Request, kind string, filters ...string) (store.ListOptions, error) {
	query, err := managementQuery(r, append([]string{"limit", "cursor"}, filters...)...)
	if err != nil {
		return store.ListOptions{}, err
	}
	options := store.ListOptions{GrantID: query.Get("grant_id"), AllocationID: query.Get("allocation_id"), ManifestID: query.Get("manifest_id"), Limit: defaultListLimit}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || limit < 1 || limit > maxListLimit {
			return store.ListOptions{}, badManagementRequest("limit must be an integer from 1 through 1000")
		}
		options.Limit = int(limit)
	}
	if raw := query.Get("cursor"); raw != "" {
		data, err := base64.RawURLEncoding.Strict().DecodeString(raw)
		if err != nil {
			return store.ListOptions{}, badManagementRequest("invalid cursor")
		}
		var cursor listCursor
		if err := jsonv2.Unmarshal(data, &cursor, jsonv2.RejectUnknownMembers(true)); err != nil || cursor.Version != 1 || cursor.Seq <= 0 {
			return store.ListOptions{}, badManagementRequest("invalid cursor")
		}
		if cursor.Kind != kind || cursor.GrantID != options.GrantID || cursor.AllocationID != options.AllocationID || cursor.ManifestID != options.ManifestID {
			return store.ListOptions{}, badManagementRequest("cursor does not match resource or filters")
		}
		options.AfterSeq = cursor.Seq
	}
	return options, nil
}

func managementList(ctx context.Context, db *store.Store, kind string, options store.ListOptions) (map[string]any, error) {
	limit := options.Limit
	options.Limit++
	rows, err := db.List(ctx, kind, options)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		next = (listCursor{Version: 1, Kind: kind, GrantID: options.GrantID, AllocationID: options.AllocationID, ManifestID: options.ManifestID, Seq: rows[limit-1]["seq"].(int64)}).encode()
	}
	for _, row := range rows {
		delete(row, "seq")
	}
	return map[string]any{"items": rows, "next_cursor": next}, nil
}
