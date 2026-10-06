package app

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	for i := 1; i < 3; i++ {
		grant := f.Grant
		if i == 2 {
			var err error
			grant, err = f.DB.Create(t.Context(), "grant", store.Create{Name: "other grant", Amount: "100", Currency: "eth", Status: "active"})
			require.NoError(t, err)
		}
		allocation, err := f.DB.Create(t.Context(), "allocation", store.Create{Name: "other allocation", GrantID: grant, Amount: "100"})
		require.NoError(t, err)
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
		_, err := f.DB.DB.Exec("INSERT INTO usage_events (id,event_id,topic,partition,offset,raw_payload,payment_session_id,status,created_at_ms) VALUES (?,?,'list',0,?,?,?,'applied',1)", id, id, i, []byte("{}"), owner.session)
		require.NoError(t, err)
	}
	_, err := f.DB.DB.Exec("INSERT INTO usage_events (id,topic,partition,offset,raw_payload,status,created_at_ms) VALUES ('usage-unassociated','list',0,3,?,'quarantined',1)", []byte("{bad"))
	require.NoError(t, err)
	for i, session := range []any{owners[0].session, owners[1].session, owners[2].session, nil, nil} {
		match, status := "matched", "settled"
		if i == 1 {
			status = "orphaned"
		} else if i == 3 {
			match = "unmatched"
		} else if i == 4 {
			match = "ambiguous"
		}
		_, err := f.DB.DB.Exec("INSERT INTO settlements (id,chain_id,contract_address,tx_hash,log_index,block_number,block_hash,sender,recipient,face_value_wei,paid_amount_wei,deposit_paid_wei,reserve_paid_wei,win_probability,sender_nonce,recipient_rand,pm_session_id,aux_data,payment_session_id,match_status,status,created_at_ms,settled_at_ms) VALUES (?,'42161',?,?,0,0,?,?,?,'0','0','0','0','0','0','0',?,'0x',?,?,?,1,1)",
			fmt.Sprint("settlement-", i), testutil.Contract, fmt.Sprintf("0x%064x", i+1), testutil.PM, testutil.Sender, testutil.Orch, testutil.PM, session, match, status)
		require.NoError(t, err)
	}
	// Equal timestamps exercise the ID tie-breaker on every list.
	for _, table := range []string{"grants", "grant_allocations", "api_keys", "payment_sessions"} {
		_, err := f.DB.DB.Exec("UPDATE " + table + " SET created_at_ms=1")
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

func TestResourceListFilters(t *testing.T) {
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
		{"allocations", "allocation", []string{owners[0].allocation, owners[1].allocation, owners[2].allocation}, nil},
		{"api-keys", "api-key", []string{owners[0].key, owners[1].key, owners[2].key}, nil},
		{"sessions", "session", []string{owners[0].session, owners[1].session, owners[2].session}, nil},
		{"usage", "usage", []string{"usage-0", "usage-1", "usage-2"}, []string{"usage-unassociated"}},
		{"settlements", "settlement", []string{"settlement-0", "settlement-1", "settlement-2"}, []string{"settlement-3", "settlement-4"}},
	} {
		cases := []filterCase{
			{"unfiltered", store.ListOptions{}, []int{0, 1, 2}},
			{"grant", store.ListOptions{GrantID: owners[0].grant}, []int{0, 1}},
			{"other grant", store.ListOptions{GrantID: owners[2].grant}, []int{2}},
			{"unknown grant", store.ListOptions{GrantID: "missing"}, nil},
			{"literal grant ID", store.ListOptions{GrantID: "' OR 1=1 --"}, nil},
		}
		if resource.kind != "allocation" {
			cases = append(cases,
				filterCase{"allocation", store.ListOptions{AllocationID: owners[0].allocation}, []int{0}},
				filterCase{"unknown allocation", store.ListOptions{AllocationID: "missing"}, nil},
				filterCase{"literal allocation ID", store.ListOptions{AllocationID: "' OR 1=1 --"}, nil},
				filterCase{"both matching", store.ListOptions{GrantID: owners[0].grant, AllocationID: owners[1].allocation}, []int{1}},
				filterCase{"ownership mismatch", store.ListOptions{GrantID: owners[2].grant, AllocationID: owners[0].allocation}, nil},
				filterCase{"both unknown", store.ListOptions{GrantID: "missing", AllocationID: "missing"}, nil},
			)
		}
		for _, tc := range cases {
			t.Run(resource.path+"/"+tc.name, func(t *testing.T) {
				want := []string{}
				for _, i := range tc.owners {
					want = append(want, resource.ids[i])
				}
				if tc.options == (store.ListOptions{}) {
					want = append(want, resource.unowned...)
				}
				slices.Sort(want)
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
				response := managementRequest(t, handler, "GET", "/v1/"+resource.path+"?"+query.Encode(), "", "")
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				require.Equal(t, "application/json", response.Header().Get("Content-Type"))
				require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
				var httpRows []map[string]any
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &httpRows))
				require.NotNil(t, httpRows)
				require.Equal(t, want, listIDs(t, httpRows))
				output := cli(t, args...)
				require.JSONEq(t, response.Body.String(), output)
				if resource.kind == "api-key" {
					require.NotContains(t, output, "secret_hash")
					require.NotContains(t, output, f.Key)
				}
				if resource.kind == "usage" {
					require.NotContains(t, output, "raw_payload")
				}
			})
		}
	}
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
		{"usage", []string{"grant_id", "allocation_id"}},
		{"settlements", []string{"grant_id", "allocation_id"}},
	} {
		managementArray(t, managementRequest(t, handler, "GET", "/v1/"+route.path, "", ""), http.StatusOK)
		for _, name := range []string{"grant_id", "allocation_id", "unknown"} {
			t.Run(route.path+"/"+name, func(t *testing.T) {
				response := managementRequest(t, handler, "GET", "/v1/"+route.path+"?"+name+"=missing", "", "")
				if slices.Contains(route.allowed, name) {
					require.Empty(t, managementArray(t, response, http.StatusOK))
				} else {
					require.Equal(t, "unknown query parameter: "+name, managementObject(t, response, http.StatusBadRequest)["error"])
				}
			})
		}
	}
	// All list routes share the same parser; exercise its edge cases once.
	invalid := []string{"unknown=", "GrantID=x", "grant-id=x", "grant_id=%zz", "grant_id=x;y", "grant_id=missing&unknown=%zz", "grant_id=x&grant%5Fid=x"}
	for _, name := range []string{"grant_id", "allocation_id"} {
		invalid = append(invalid, name, name+"=", name+"=x&"+name+"=x", name+"=x&"+name+"=y", name+"=&"+name+"=", name+"=x&"+name+"=")
	}
	for _, query := range invalid {
		t.Run(query, func(t *testing.T) {
			result := managementObject(t, managementRequest(t, handler, "GET", "/v1/sessions?"+query, "", ""), http.StatusBadRequest)
			require.NotEmpty(t, result["error"])
		})
	}
	require.Equal(t, http.StatusBadRequest, managementRequest(t, handler, "HEAD", "/v1/sessions?grant_id=", "", "").Code)
	// List query validation does not apply to item or report routes.
	managementObject(t, managementRequest(t, handler, "GET", "/v1/grants/"+f.Grant+"?unknown=", "", ""), http.StatusOK)
	managementArray(t, managementRequest(t, handler, "GET", "/v1/ledger/report?unknown=", "", ""), http.StatusOK)
}

func TestManagementListQueryAuthFirst(t *testing.T) {
	f := testutil.New(t, "100")
	registry := testRegistry(t, testHTTPCredentials, `{"id":"limited","secret":"limited-secret","management":{"allow":["ledger.read"]}}`)
	handler := managementHandler(t.Context(), f.DB, registry)
	for _, path := range []string{"grants", "allocations", "api-keys", "sessions", "usage", "settlements"} {
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
				response := managementRequest(t, handler, method, "/v1/"+path+"?grant_id=%zz", "", "", credential.token)
				require.Equal(t, credential.status, response.Code, method+" "+path)
				require.True(t, strings.HasPrefix(response.Header().Get("Content-Type"), "text/plain"))
			}
		}
	}
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
		{[]string{"usage", "list"}, []string{"grant-id", "allocation-id"}},
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
			for _, name := range []string{"grant-id", "allocation-id"} {
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
