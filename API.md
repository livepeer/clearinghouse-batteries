# HTTP API

The management API and signer webhook use separate listeners; see the [README](README.md)
for setup and credentials. Both listeners provide the health endpoints below.

## Management API

### Requests and responses

Request bodies accept `application/x-www-form-urlencoded`, `multipart/form-data`,
or `application/json`. JSON bodies must be objects with string values. Unknown
fields, repeated form fields, and file uploads are rejected. The body limit is 1 MiB.

Currency amounts are unsigned decimal strings with up to 18 fractional digits. Specify at
most one of `amount_usd` and `amount_eth`; the currency must match the grant or
allocation being funded. Responses use decimal `*_usd` or `*_eth` strings, Unix
millisecond timestamps, and string `metadata`. Empty amounts return `""`, null
amounts return `null`, and failed conversions return `"[error]"`. Allocation
reads include `available_*`, which can be negative, and `spent_*`, including late
charges after revocation.

Item reads return a JSON object; list reads return a page as described below.
Creation returns `201`; other successful operations return `200`.

### Grants and allocations

| Method and path | Request or result |
| --- | --- |
| `GET /v1/grants` | List grants. |
| `GET /v1/grants/{id}` | Read a grant. |
| `POST /v1/grants` | Create a grant; returns `{"id":"..."}`. |
| `POST /v1/grants/{id}/fund` | Add funds; returns `{"id":"..."}`. |
| `PATCH /v1/grants/{id}/status` | Set `status` to `draft`, `active`, `paused`, or `closed`; returns `id` and `status`. A closed grant cannot be reopened. |
| `GET /v1/allocations` | List allocations. |
| `GET /v1/allocations/{id}` | Read an allocation. |
| `POST /v1/allocations` | Create an allocation; returns `{"id":"..."}`. |
| `POST /v1/allocations/{id}/fund` | Move unallocated grant funds to the allocation; returns `{"id":"..."}`. |
| `PATCH /v1/allocations/{id}/status` | Set `status` to `active` or `paused`; returns `id` and `status`. |
| `POST /v1/allocations/{id}/revoke` | Revoke the allocation and return unused funds to the grant; returns `id` and `status`. A revoked allocation cannot be reopened. |

Grant creation requires `name` and accepts `sponsor`, `amount_usd` or
`amount_eth`, `status`, `metadata`, `starts_at`, and `ends_at`. The default amount
is USD 0 and the default status is `draft`; initial status can be `draft`, `active`,
or `paused`.

Allocation creation requires `grant_id` and `name` and accepts `beneficiary`,
`amount_usd` or `amount_eth`, `status`, `metadata`, `starts_at`, and `ends_at`.
The amount defaults to 0 and the currency is inherited from the grant. Initial
status can be `active` (the default) or `paused`. An active allocation with zero
funding becomes `exhausted`. Funding reactivates an exhausted allocation if its
balance becomes positive; it does not reactivate a paused allocation.

`starts_at` and `ends_at` are optional RFC3339 timestamps; the end must follow
the start. Funding requires a positive `amount_usd` or `amount_eth`. Allocation
creation and funding also accept `all` as the amount to use the grant's unallocated
balance. For example, `POST /v1/allocations` accepts:

```json
{"grant_id":"GRANT_ID","name":"Example app","amount_usd":"25"}
```

### API keys and sessions

| Method and path | Request or result |
| --- | --- |
| `GET /v1/api-keys` | List API keys without their secrets. |
| `GET /v1/api-keys/{id}` | Read an API key without its secret. |
| `POST /v1/api-keys` | Create a key; returns `allocation_id`, `id`, and `api_key`. |
| `POST /v1/api-keys/{id}/revoke` | Revoke a key; returns `id` and `status`. |
| `GET /v1/sessions` | List payment sessions. |
| `GET /v1/sessions/{id}` | Read a payment session. |
| `POST /v1/sessions/{id}/revoke` | Revoke a session; returns `id` and `status`. |

Key creation requires `name` and exactly one of `allocation_id` or `grant_id`.
With `allocation_id`, the key uses that allocation and no amount is allowed.
With `grant_id`, the request creates an allocation and requires `amount_usd` or
`amount_eth` (including `all`). Save `api_key` from the creation response; reads
do not return it. A matching idempotent retry also returns the original secret.

### Reports

| Method and path | Result |
| --- | --- |
| `GET /v1/settlements` | Page of on-chain settlements. |
| `GET /v1/usage` | Page of applied and quarantined usage events. |
| `GET /v1/ledger/report` | Array of accounting balances. |
| `GET /v1/escrow/report` | Array of on-chain escrow balances. |
| `GET /v1/escrow/activity` | Array of on-chain escrow activity. |

### Lists and pagination

The six resource lists accept `limit` (default 100, range 1–1,000) and an opaque
`cursor`. They also accept the filters shown below:

| List | Filters |
| --- | --- |
| `/v1/grants` | None |
| `/v1/allocations` | `grant_id` |
| `/v1/api-keys`, `/v1/sessions`, `/v1/settlements` | `grant_id`, `allocation_id` |
| `/v1/usage` | `grant_id`, `allocation_id`, `manifest_id` |

Filters match exactly and are combined. Unknown IDs or an allocation outside the
specified grant return an empty page. Usage and settlements without a session
appear only in unfiltered lists. Unknown, repeated, empty, or malformed query
parameters return `400`; HEAD requests use the same validation.

```json
{"items": [], "next_cursor": ""}
```

Pass a nonempty `next_cursor` as `cursor` with the same resource and filters to
fetch the next page. An empty cursor means no more items. Results are ordered by
insertion, not timestamp or ID. Pagination reads live data: later inserts may
appear on subsequent pages, and ownership changes may affect filtered results.

### Idempotency

Allocation and API-key creation, and grant and allocation funding, accept an
optional `Idempotency-Key` header. Use a new random key for each operation and
reuse it for retries. Keys are case-sensitive and 1–256 characters, using the
base64url alphabet (`A-Z`, `a-z`, `0-9`, `-`, `_`) or `=` padding characters.
The server checks the characters, not whether the key is valid base64url encoding.

Keys are shared across these operations and callers within a grant; different
grants may reuse a key. A matching retry returns the original successful status
and response, including any API-key secret. A different operation, target, or
body returns `409`. Matching compares decoded strings: field order and body
format do not matter, but `"1"` and `"1.0"` differ, as do an omitted field and an
empty or default field. Form values must be valid UTF-8.

Successful responses survive restarts and currently do not expire. A retry after
closure or revocation returns the original response without restoring access or
moving funds. Errors are not saved. Retries are still validated and require the
same permissions. Stored API-key responses contain unencrypted secrets; see
[SECURITY.md](SECURITY.md).

### Errors

Resource handler errors return a JSON object with an `error` string. For example,
a missing resource returns `404` with `{"error":"resource not found"}`. Missing
credentials return `401` with plain text `Unauthorized`, and a missing route
permission returns `403` with plain text `Forbidden`. An additional funding
permission failure returns `403` with `{"error":"Forbidden"}`.

| HTTP status | Meaning |
| --- | --- |
| `400` | Invalid input or query. |
| `401` | Missing or invalid credentials. |
| `403` | Insufficient permission. |
| `404` | Resource not found. |
| `409` | State, balance, or idempotency conflict. |
| `413` | Body exceeds 1 MiB. |
| `415` | Unsupported or missing content type. |
| `500` | Unexpected failure. |

## Signer webhook

See the [Livepeer Node signer documentation](https://github.com/livepeer/node/blob/main/docs/signer.md)
for the signer and its authorization webhook.

`POST /v1/signer/authorize` receives a JSON object containing `headers` and
`state`. The nested `headers.Authorization` is an array containing one
`Bearer API_KEY` value. The state uses the signer's field names:

```json
{
  "headers": {"Authorization": ["Bearer lpg_..."]},
  "state": {
    "StateID": "session-state-id",
    "OrchestratorAddress": "0x0000000000000000000000000000000000000001",
    "App": "example",
    "Type": "live",
    "FeeWei": "1000000000000000",
    "FeeUSD": "0.01",
    "AuthID": ""
  }
}
```

`OrchestratorAddress` must be a valid Ethereum address. For an ETH allocation,
`FeeWei` is an unsigned integer in wei with no leading zeros; for a USD allocation,
`FeeUSD` is an unsigned decimal with up to 18 fractional digits. `AuthID` may be
omitted for a new session or supplied to match an existing one. Extra signer
state fields are ignored.

A valid request returns HTTP `200`. The JSON response's `status` indicates whether
the payment is authorized. For example, an approval returns the session ID:

```json
{"status":200,"expiry":0,"auth_id":"SESSION_ID"}
```

A rejection includes a reason, for example:

```json
{"status":401,"reason":"invalid API key","expiry":0}
```

The response `status` can be `400`, `401`, `402`, `403`, or `503` for a rejection;
`expiry` is currently `0`. The endpoint checks the key, grant, allocation, balance,
fee, session binding, and [Kafka lag](README.md#configure-kafka-lag), creating a
session when needed. A lag denial has status `503` and reason `kafka lag exceeded`
or `kafka lag unavailable`; it creates no session or activity update. Authorization
does not reserve funds, so delayed accounting can overdraw an allocation.
Malformed requests return HTTP `400` as plain text; an unavailable authorization
service returns HTTP `503`.

## Health

Both listeners provide `GET /livez`, which returns `200` while the server can
answer, and `GET /readyz`, which returns `503` during shutdown or database
failure and `200` otherwise.
