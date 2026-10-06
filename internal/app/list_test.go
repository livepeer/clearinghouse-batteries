package app

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/stretchr/testify/require"
)

type listOwner struct {
	grant, allocation, key, session string
}

func newListFixture(t *testing.T) (*testutil.Fixture, []listOwner) {
	t.Helper()
	f := testutil.New(t, "100")
	owners := []listOwner{{f.Grant, f.Allocation, f.KeyID, f.Session}}
	require.NoError(t, f.DB.Fund(t.Context(), "grant", f.Grant, "100"))
	for i := 1; i < 4; i++ {
		grant, allocation := f.Grant, f.Allocation
		if i == 2 {
			var err error
			grant, err = f.DB.Create(t.Context(), "grant", store.Create{Name: "other grant", Amount: "100", Currency: "eth", Status: "active"})
			require.NoError(t, err)
		}
		// The final key and session reuse the first allocation to span filtered pages.
		if i < 3 {
			var err error
			allocation, err = f.DB.Create(t.Context(), "allocation", store.Create{Name: "other allocation", GrantID: grant, Amount: "100"})
			require.NoError(t, err)
		}
		keyID, key, err := f.DB.CreateKey(t.Context(), allocation, "other key")
		require.NoError(t, err)
		request := f.Request
		request.Headers = http.Header{"Authorization": {"Bearer " + key}}
		state := *f.Request.State
		state.StateID = fmt.Sprint("list-state-", i)
		request.State = &state
		decision, err := f.DB.Authorize(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, decision.Status)
		owners = append(owners, listOwner{grant, allocation, keyID, decision.AuthID})
	}

	// Seed historical records directly to test their stored ownership links.
	for i, owner := range owners {
		id := fmt.Sprint("usage-", i)
		manifest := "manifest-1"
		if i == 1 {
			manifest = "manifest-2"
		}
		_, err := f.DB.DB.Exec("INSERT INTO usage_events (id,event_id,topic,partition,offset,raw_payload,payment_session_id,request_id,pipeline,manifest_id,status,created_at_ms) VALUES (?,?,'list',0,?,?,?,?,?,?,'applied',1)", id, id, i, []byte("{}"), owner.session, "request-"+id, "live", manifest)
		require.NoError(t, err)
	}
	_, err := f.DB.DB.Exec("INSERT INTO usage_events (id,topic,partition,offset,raw_payload,status,created_at_ms) VALUES ('usage-unassociated','list',0,4,?,'quarantined',1)", []byte("{bad"))
	require.NoError(t, err)
	for i, session := range []any{owners[0].session, owners[1].session, owners[2].session, owners[3].session, nil, nil} {
		match, status := "matched", "settled"
		if i == 1 {
			status = "orphaned"
		} else if i == 4 {
			match = "unmatched"
		} else if i == 5 {
			match = "ambiguous"
		}
		_, err := f.DB.DB.Exec("INSERT INTO settlements (id,chain_id,contract_address,tx_hash,log_index,block_number,block_hash,sender,recipient,face_value_wei,paid_amount_wei,deposit_paid_wei,reserve_paid_wei,win_probability,sender_nonce,recipient_rand,pm_session_id,aux_data,payment_session_id,match_status,status,created_at_ms,settled_at_ms) VALUES (?,'42161',?,?,0,0,?,?,?,'0','0','0','0','0','0','0',?,'0x',?,?,?,1,1)",
			fmt.Sprint("settlement-", i), testutil.Contract, fmt.Sprintf("0x%064x", i+1), testutil.PM, testutil.Sender, testutil.Orch, testutil.PM, session, match, status)
		require.NoError(t, err)
	}
	// Timestamps deliberately disagree with insertion order on every list.
	for _, table := range []string{"grants", "grant_allocations", "api_keys", "payment_sessions", "usage_events", "settlements"} {
		_, err := f.DB.DB.Exec("UPDATE " + table + " SET created_at_ms=100-seq")
		require.NoError(t, err)
	}
	return f, owners
}

func listIDs(t *testing.T, rows []map[string]any) []string {
	t.Helper()
	ids := make([]string, len(rows))
	for i, row := range rows {
		var ok bool
		ids[i], ok = row["id"].(string)
		require.True(t, ok)
	}
	return ids
}

func TestResourceListFiltersAndPagination(t *testing.T) {
	f, owners := newListFixture(t)
	t.Setenv("CLEARINGHOUSE_DB_PATH", f.Path)
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	type filterCase struct {
		name    string
		options store.ListOptions
		owners  []int
	}
	for _, resource := range []struct {
		path, kind   string
		ids, unowned []string
	}{
		{"grants", "grant", []string{owners[0].grant, owners[1].grant, owners[2].grant, owners[3].grant}, nil},
		{"allocations", "allocation", []string{owners[0].allocation, owners[1].allocation, owners[2].allocation, owners[3].allocation}, nil},
		{"api-keys", "api-key", []string{owners[0].key, owners[1].key, owners[2].key, owners[3].key}, nil},
		{"sessions", "session", []string{owners[0].session, owners[1].session, owners[2].session, owners[3].session}, nil},
		{"usage", "usage", []string{"usage-0", "usage-1", "usage-2", "usage-3"}, []string{"usage-unassociated"}},
		{"settlements", "settlement", []string{"settlement-0", "settlement-1", "settlement-2", "settlement-3"}, []string{"settlement-4", "settlement-5"}},
	} {
		cases := []filterCase{{"unfiltered", store.ListOptions{}, []int{0, 1, 2, 3}}}
		if resource.kind != "grant" {
			cases = append(cases,
				filterCase{"grant", store.ListOptions{GrantID: owners[0].grant}, []int{0, 1, 3}},
				filterCase{"other grant", store.ListOptions{GrantID: owners[2].grant}, []int{2}},
				filterCase{"unknown grant", store.ListOptions{GrantID: "missing"}, nil},
				filterCase{"literal grant ID", store.ListOptions{GrantID: "' OR 1=1 --"}, nil},
			)
		}
		if resource.kind != "grant" && resource.kind != "allocation" {
			cases = append(cases,
				filterCase{"allocation", store.ListOptions{AllocationID: owners[0].allocation}, []int{0, 3}},
				filterCase{"unknown allocation", store.ListOptions{AllocationID: "missing"}, nil},
				filterCase{"literal allocation ID", store.ListOptions{AllocationID: "' OR 1=1 --"}, nil},
				filterCase{"both matching", store.ListOptions{GrantID: owners[0].grant, AllocationID: owners[0].allocation}, []int{0, 3}},
				filterCase{"ownership mismatch", store.ListOptions{GrantID: owners[2].grant, AllocationID: owners[0].allocation}, nil},
				filterCase{"both unknown", store.ListOptions{GrantID: "missing", AllocationID: "missing"}, nil},
			)
		}
		if resource.kind == "usage" {
			cases = append(cases,
				filterCase{"manifest", store.ListOptions{ManifestID: "manifest-1"}, []int{0, 2, 3}},
				filterCase{"other manifest", store.ListOptions{ManifestID: "manifest-2"}, []int{1}},
				filterCase{"unknown manifest", store.ListOptions{ManifestID: "missing"}, nil},
				filterCase{"case-sensitive manifest", store.ListOptions{ManifestID: "Manifest-1"}, nil},
				filterCase{"literal manifest ID", store.ListOptions{ManifestID: "' OR 1=1 --"}, nil},
				filterCase{"grant and manifest", store.ListOptions{GrantID: owners[0].grant, ManifestID: "manifest-1"}, []int{0, 3}},
				filterCase{"allocation and manifest", store.ListOptions{AllocationID: owners[0].allocation, ManifestID: "manifest-1"}, []int{0, 3}},
				filterCase{"all matching", store.ListOptions{GrantID: owners[0].grant, AllocationID: owners[0].allocation, ManifestID: "manifest-1"}, []int{0, 3}},
				filterCase{"manifest ownership mismatch", store.ListOptions{AllocationID: owners[0].allocation, ManifestID: "manifest-2"}, nil},
				filterCase{"all ownership mismatch", store.ListOptions{GrantID: owners[2].grant, AllocationID: owners[0].allocation, ManifestID: "manifest-1"}, nil},
			)
		}
		for _, tc := range cases {
			t.Run(resource.path+"/"+tc.name, func(t *testing.T) {
				want := []string{}
				for _, i := range tc.owners {
					if !slices.Contains(want, resource.ids[i]) {
						want = append(want, resource.ids[i])
					}
				}
				if tc.options == (store.ListOptions{}) {
					want = append(want, resource.unowned...)
				}
				query := url.Values{}
				args := []string{resource.kind, "list"}
				if tc.options.GrantID != "" {
					query.Set("grant_id", tc.options.GrantID)
					args = append(args, "--grant-id", tc.options.GrantID)
				}
				if tc.options.AllocationID != "" {
					query.Set("allocation_id", tc.options.AllocationID)
					args = append(args, "--allocation-id", tc.options.AllocationID)
				}
				if tc.options.ManifestID != "" {
					query.Set("manifest_id", tc.options.ManifestID)
					args = append(args, "--manifest-id", tc.options.ManifestID)
				}
				output := cli(t, args...)
				var cliRows []map[string]any
				require.NoError(t, json.Unmarshal([]byte(output), &cliRows))
				require.Equal(t, want, listIDs(t, cliRows))
				limits := []int{1}
				if tc.options == (store.ListOptions{}) {
					limits = slices.Compact([]int{1, 2, len(want), maxListLimit})
				}
				for _, limit := range limits {
					require.Equal(t, cliRows, managementPages(t, handler, "/v1/"+resource.path, query, limit, len(want)))
				}
				if resource.kind == "api-key" {
					require.NotContains(t, output, "secret_hash")
					require.NotContains(t, output, f.Key)
				}
				if resource.kind == "usage" {
					require.NotContains(t, output, "raw_payload")
					if tc.options == (store.ListOptions{}) {
						for i, row := range cliRows {
							for _, field := range []string{"allocation_id", "payment_session_id", "request_id", "pipeline", "manifest_id"} {
								require.Contains(t, row, field)
							}
							if i >= len(owners) {
								require.Nil(t, row["allocation_id"])
								require.Nil(t, row["payment_session_id"])
								continue
							}
							require.Equal(t, owners[i].allocation, row["allocation_id"])
							require.Equal(t, owners[i].session, row["payment_session_id"])
							require.Equal(t, "request-"+resource.ids[i], row["request_id"])
							require.Equal(t, "live", row["pipeline"])
							manifest := "manifest-1"
							if i == 1 {
								manifest = "manifest-2"
							}
							require.Equal(t, manifest, row["manifest_id"])
						}
					}
				}
			})
		}
	}
}

func TestGatewayUsageForJob(t *testing.T) {
	f, owners := newListFixture(t)
	t.Setenv("CLEARINGHOUSE_DB_PATH", f.Path)
	const manifest = "manifest+%/&é"
	// A second job shares the first allocation; another allocation shares its manifest.
	_, err := f.DB.DB.Exec(`UPDATE usage_events SET manifest_id=? WHERE id IN ('usage-0','usage-2')`, manifest)
	require.NoError(t, err)
	registry := testRegistry(t, `{"id":"gateway","secret":"gateway-secret","management":{"allow":["usage.read"]}}`)
	handler := managementHandler(t.Context(), f.DB, registry)
	query := url.Values{"allocation_id": {owners[0].allocation}, "manifest_id": {manifest}, "limit": {"1"}}
	response := managementRequest(t, handler, "GET", "/v1/usage?"+query.Encode(), "", "", "gateway-secret")
	items, next := managementPage(t, response, http.StatusOK)
	require.Equal(t, []string{"usage-0"}, listIDs(t, items))
	require.Empty(t, next)
	var cliRows []map[string]any
	require.NoError(t, json.Unmarshal([]byte(cli(t, "usage", "list", "--allocation-id", owners[0].allocation, "--manifest-id", manifest)), &cliRows))
	require.Equal(t, items, cliRows)
}

func TestUsageManifestCursors(t *testing.T) {
	f, owners := newListFixture(t)
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	_, filtered := managementPage(t, managementRequest(t, handler, "GET", "/v1/usage?manifest_id=manifest-1&limit=1", "", ""), http.StatusOK)
	require.NotEmpty(t, filtered)
	_, unfiltered := managementPage(t, managementRequest(t, handler, "GET", "/v1/usage?limit=1", "", ""), http.StatusOK)
	for _, query := range []url.Values{
		{"cursor": {filtered}},
		{"cursor": {filtered}, "manifest_id": {"manifest-2"}},
		{"cursor": {unfiltered}, "manifest_id": {"manifest-1"}},
	} {
		for _, method := range []string{"GET", "HEAD"} {
			result := managementObject(t, managementRequest(t, handler, method, "/v1/usage?"+query.Encode(), "", ""), http.StatusBadRequest)
			require.Equal(t, "cursor does not match resource or filters", result["error"])
		}
	}
	valid := url.Values{"cursor": {filtered}, "manifest_id": {"manifest-1"}}
	require.Equal(t, http.StatusOK, managementRequest(t, handler, "HEAD", "/v1/usage?"+valid.Encode(), "", "").Code)

	// A v1 cursor created before manifest filtering has no manifest_id member.
	var seq int64
	require.NoError(t, f.DB.DB.QueryRow(`SELECT seq FROM usage_events WHERE id='usage-0'`).Scan(&seq))
	legacy, err := json.Marshal(map[string]any{"version": 1, "kind": "usage", "grant_id": owners[0].grant, "allocation_id": "", "seq": seq})
	require.NoError(t, err)
	query := url.Values{"grant_id": {owners[0].grant}, "cursor": {base64.RawURLEncoding.EncodeToString(legacy)}}
	items, next := managementPage(t, managementRequest(t, handler, "GET", "/v1/usage?"+query.Encode(), "", ""), http.StatusOK)
	require.Equal(t, []string{"usage-1", "usage-3"}, listIDs(t, items))
	require.Empty(t, next)
}

func TestManagementListQueries(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	for _, route := range []struct {
		path    string
		allowed []string
	}{
		{"grants", nil},
		{"allocations", []string{"grant_id"}},
		{"api-keys", []string{"grant_id", "allocation_id"}},
		{"sessions", []string{"grant_id", "allocation_id"}},
		{"usage", []string{"grant_id", "allocation_id", "manifest_id"}},
		{"settlements", []string{"grant_id", "allocation_id"}},
	} {
		managementPage(t, managementRequest(t, handler, "GET", "/v1/"+route.path, "", ""), http.StatusOK)
		for _, name := range []string{"grant_id", "allocation_id", "manifest_id", "unknown"} {
			t.Run(route.path+"/"+name, func(t *testing.T) {
				response := managementRequest(t, handler, "GET", "/v1/"+route.path+"?"+name+"=missing", "", "")
				if slices.Contains(route.allowed, name) {
					items, next := managementPage(t, response, http.StatusOK)
					require.Empty(t, items)
					require.Empty(t, next)
				} else {
					require.Equal(t, "unknown query parameter: "+name, managementObject(t, response, http.StatusBadRequest)["error"])
				}
			})
		}
	}
	// All list routes share the same parser; exercise its edge cases once.
	invalid := []string{"unknown=", "GrantID=x", "grant-id=x", "grant_id=%zz", "grant_id=x;y", "grant_id=missing&unknown=%zz", "grant_id=x&grant%5Fid=x", "manifest_id=%zz", "manifest_id=x&manifest%5Fid=x"}
	for _, name := range []string{"grant_id", "allocation_id", "manifest_id", "limit", "cursor"} {
		invalid = append(invalid, name, name+"=", name+"=x&"+name+"=x", name+"=x&"+name+"=y", name+"=&"+name+"=", name+"=x&"+name+"=")
	}
	for _, query := range invalid {
		t.Run(query, func(t *testing.T) {
			result := managementObject(t, managementRequest(t, handler, "GET", "/v1/usage?"+query, "", ""), http.StatusBadRequest)
			require.NotEmpty(t, result["error"])
		})
	}
	require.Equal(t, http.StatusBadRequest, managementRequest(t, handler, "HEAD", "/v1/usage?manifest_id=", "", "").Code)
	// List query validation does not apply to item or report routes.
	managementObject(t, managementRequest(t, handler, "GET", "/v1/grants/"+f.Grant+"?unknown=", "", ""), http.StatusOK)
	managementArray(t, managementRequest(t, handler, "GET", "/v1/ledger/report?unknown=", "", ""), http.StatusOK)
}

func TestManagementListQueryAuthFirst(t *testing.T) {
	f := testutil.New(t, "100")
	registry := testRegistry(t, testHTTPCredentials, `{"id":"limited","secret":"limited-secret","management":{"allow":["ledger.read"]}}`)
	handler := managementHandler(t.Context(), f.DB, registry)
	for _, path := range []string{"grants", "allocations", "api-keys", "sessions", "usage", "settlements"} {
		queries := []string{"grant_id=%zz"}
		if path == "sessions" {
			queries = append(queries, "limit=0", "cursor=bad")
		}
		for _, credential := range []struct {
			token  string
			status int
		}{
			{"", http.StatusUnauthorized},
			{"wrong", http.StatusUnauthorized},
			{testWebhookToken, http.StatusUnauthorized},
			{"limited-secret", http.StatusForbidden},
		} {
			for _, method := range []string{"GET", "HEAD"} {
				for _, query := range queries {
					response := managementRequest(t, handler, method, "/v1/"+path+"?"+query, "", "", credential.token)
					require.Equal(t, credential.status, response.Code, method+" "+path)
					require.True(t, strings.HasPrefix(response.Header().Get("Content-Type"), "text/plain"))
				}
			}
		}
	}
}

func managementPages(t *testing.T, handler http.Handler, path string, query url.Values, limit, total int) []map[string]any {
	t.Helper()
	query = maps.Clone(query)
	query.Set("limit", fmt.Sprint(limit))
	all := []map[string]any{}
	for range total + 1 {
		items, next := managementPage(t, managementRequest(t, handler, "GET", path+"?"+query.Encode(), "", ""), http.StatusOK)
		require.Len(t, items, min(limit, total-len(all)))
		all = append(all, items...)
		require.Equal(t, len(all) < total, next != "")
		if next == "" {
			return all
		}
		require.NotEqual(t, query.Get("cursor"), next)
		query.Set("cursor", next)
	}
	t.Fatal("pagination did not terminate")
	return nil
}

func TestManagementPaginationLimitAndLiveData(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	// Opposing IDs and timestamps establish that only seq controls ordering.
	_, err := f.DB.DB.Exec(`INSERT INTO grants(id,name,total_units,status,created_at_ms) VALUES ('z','first','0','draft',2),('a','second','0','draft',1)`)
	require.NoError(t, err)
	first, cursor := managementPage(t, managementRequest(t, handler, "GET", "/v1/grants?limit=1", "", ""), http.StatusOK)
	require.Equal(t, []string{f.Grant}, listIDs(t, first))
	require.NotEmpty(t, cursor)
	// A deletion makes a gap, and a later insert remains visible to this traversal.
	_, err = f.DB.DB.Exec(`DELETE FROM grants WHERE id='z'`)
	require.NoError(t, err)
	_, err = f.DB.DB.Exec(`INSERT INTO grants(id,name,total_units,status,created_at_ms) VALUES ('0','third','0','draft',0)`)
	require.NoError(t, err)
	remaining, next := managementPage(t, managementRequest(t, handler, "GET", "/v1/grants?limit=2&cursor="+cursor, "", ""), http.StatusOK)
	require.Equal(t, []string{"a", "0"}, listIDs(t, remaining))
	require.Empty(t, next)
	_, err = f.DB.DB.Exec(`DELETE FROM grants WHERE id='0'`)
	require.NoError(t, err)
	// Include enough rows to prove the default is bounded and the CLI is not.
	require.NoError(t, f.DB.Write(t.Context(), func(tx *sql.Tx) error {
		for i := range defaultListLimit {
			if _, err := tx.Exec(`INSERT INTO grants(id,name,total_units,status,created_at_ms) VALUES (?,?,'0','draft',0)`, fmt.Sprint("more-", i), "grant"); err != nil {
				return err
			}
		}
		return nil
	}))
	items, next := managementPage(t, managementRequest(t, handler, "GET", "/v1/grants", "", ""), http.StatusOK)
	require.Len(t, items, defaultListLimit)
	require.NotEmpty(t, next)
	t.Setenv("CLEARINGHOUSE_DB_PATH", f.Path)
	var cliRows []map[string]any
	require.NoError(t, json.Unmarshal([]byte(cli(t, "grant", "list")), &cliRows))
	require.Len(t, cliRows, defaultListLimit+2)
	for _, row := range cliRows {
		require.NotContains(t, row, "seq")
	}
	item := managementObject(t, managementRequest(t, handler, "GET", "/v1/grants/"+f.Grant, "", ""), http.StatusOK)
	require.NotContains(t, item, "seq")
}

func TestManagementPaginationInvalidQueries(t *testing.T) {
	f := testutil.New(t, "100")
	handler := managementHandler(t.Context(), f.DB, testRegistry(t))
	invalid := []string{"limit=0", "limit=-1", "limit=1001", "limit=18446744073709551616", "limit=1.0", "limit=1e2", "limit=%2B1", "limit=%201", "cursor=bad", "cursor=%zz"}
	for _, raw := range []string{
		`null`, `{}`, `[]`, `{"version":1,"kind":"session","seq":0}`,
		`{"version":1,"kind":"session","seq":-1}`, `{"version":1,"kind":"session","seq":9223372036854775808}`,
		`{"version":1,"kind":"session","seq":1.5}`, `{"version":1,"kind":"session","seq":"1"}`,
		`{"version":2,"kind":"session","seq":1}`, `{"version":1,"kind":"session","seq":1,"seq":2}`,
		`{"version":1,"kind":"session","seq":1,"extra":true}`, `{"version":1,"kind":"session","seq":1} {}`,
	} {
		invalid = append(invalid, "cursor="+base64.RawURLEncoding.EncodeToString([]byte(raw)))
	}
	for _, cursor := range []listCursor{
		{Version: 1, Kind: "grant", Seq: 1},
		{Version: 1, Kind: "session", GrantID: f.Grant, Seq: 1},
		{Version: 1, Kind: "session", AllocationID: f.Allocation, Seq: 1},
	} {
		invalid = append(invalid, "cursor="+cursor.encode())
	}
	for _, query := range invalid {
		t.Run(query, func(t *testing.T) {
			for _, method := range []string{"GET", "HEAD"} {
				response := managementRequest(t, handler, method, "/v1/sessions?"+query, "", "")
				managementObject(t, response, http.StatusBadRequest)
			}
		})
	}
	for _, query := range []string{"limit=1", "limit=1000", "cursor=" + (listCursor{Version: 1, Kind: "session", Seq: 9223372036854775807}).encode()} {
		response := managementRequest(t, handler, "HEAD", "/v1/sessions?"+query, "", "")
		require.Equal(t, http.StatusOK, response.Code)
	}
	items, next := managementPage(t, managementRequest(t, handler, "GET", "/v1/sessions?cursor="+(listCursor{Version: 1, Kind: "session", Seq: 9223372036854775807}).encode(), "", ""), http.StatusOK)
	require.Empty(t, items)
	require.Empty(t, next)
}

func TestResourceSequencesPersist(t *testing.T) {
	f, _ := newListFixture(t)
	before := map[string][]map[string]any{}
	for _, table := range []string{"grants", "grant_allocations", "api_keys", "payment_sessions", "usage_events", "settlements"} {
		rows, err := f.DB.Rows(t.Context(), "SELECT id,seq FROM "+table+" ORDER BY seq")
		require.NoError(t, err)
		for i, row := range rows {
			require.Equal(t, int64(i+1), row["seq"], table)
		}
		before[table] = rows
		var definition string
		require.NoError(t, f.DB.DB.QueryRow("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&definition))
		require.Contains(t, definition, "seq INTEGER PRIMARY KEY AUTOINCREMENT")
	}
	var deleted, inserted int64
	require.NoError(t, f.DB.DB.QueryRow(`INSERT INTO grants(id,name,total_units,status,created_at_ms) VALUES ('deleted','grant','0','draft',0) RETURNING seq`).Scan(&deleted))
	_, err := f.DB.DB.Exec(`DELETE FROM grants WHERE id='deleted'`)
	require.NoError(t, err)
	_, err = f.DB.DB.Exec("VACUUM")
	require.NoError(t, err)
	for table, want := range before {
		rows, err := f.DB.Rows(t.Context(), "SELECT id,seq FROM "+table+" ORDER BY seq")
		require.NoError(t, err)
		require.Equal(t, want, rows, table)
	}
	require.NoError(t, f.DB.DB.QueryRow(`INSERT INTO grants(id,name,total_units,status,created_at_ms) VALUES ('inserted','grant','0','draft',0) RETURNING seq`).Scan(&inserted))
	require.Greater(t, inserted, deleted)
	violations, err := f.DB.Rows(t.Context(), "PRAGMA foreign_key_check")
	require.NoError(t, err)
	require.Empty(t, violations)
}

func TestCLIListFilterFlags(t *testing.T) {
	for _, command := range []struct {
		args    []string
		allowed []string
	}{
		{[]string{"grant", "list"}, nil},
		{[]string{"allocation", "list"}, []string{"grant-id"}},
		{[]string{"api-key", "list"}, []string{"grant-id", "allocation-id"}},
		{[]string{"session", "list"}, []string{"grant-id", "allocation-id"}},
		{[]string{"usage", "list"}, []string{"grant-id", "allocation-id", "manifest-id"}},
		{[]string{"settlement", "list"}, []string{"grant-id", "allocation-id"}},
		{[]string{"grant", "show"}, nil},
		{[]string{"allocation", "show"}, nil},
		{[]string{"session", "show"}, nil},
		{[]string{"ledger", "report"}, nil},
		{[]string{"escrow", "report"}, nil},
		{[]string{"escrow", "activity"}, nil},
		{[]string{"migrate", "status"}, nil},
	} {
		t.Run(strings.Join(command.args, " "), func(t *testing.T) {
			help := cli(t, append(slices.Clone(command.args), "--help")...)
			for _, name := range []string{"grant-id", "allocation-id", "manifest-id"} {
				if slices.Contains(command.allowed, name) {
					require.Contains(t, help, "--"+name)
					for _, flag := range [][]string{{"--" + name + "="}, {"--" + name, ""}} {
						var output bytes.Buffer
						err := Execute(t.Context(), append(slices.Clone(command.args), flag...), &output, &output)
						require.ErrorContains(t, err, "--"+name+" must not be empty")
					}
				} else {
					require.NotContains(t, help, "--"+name)
					var output bytes.Buffer
					err := Execute(t.Context(), append(slices.Clone(command.args), "--"+name+"=value"), &output, &output)
					require.ErrorContains(t, err, "unknown flag: --"+name)
				}
			}
		})
	}
}
