# Configurable API Credentials

## Overview

Allow users to update API credentials for a provider after initial upload, without re-uploading the provider code.

## Problem Statement

When a provider is uploaded, API credentials (e.g., `api_key`, `api_secret`) are encrypted using a key derived from the provider file's hash. These credentials are stored as `(nonce, ciphertext)` in the `code_registry` table.

Currently, there is no way to update these credentials without re-uploading the entire provider file. This is problematic because:

- **Key rotation**: Security best practices require periodic credential rotation
- **Mistakes**: Typos in credentials during upload require full re-upload to fix
- **Account migration**: Switching to a different API account (e.g., upgrading to premium tier) requires re-upload
- **Expired keys**: Some providers issue time-limited credentials

## Solution

Add an API endpoint to accept new credentials, re-encrypt them using the existing file hash, and store the updated `(nonce, ciphertext)`. Expose this through the existing Provider Config Modal's "API Secrets" tab.

### How It Works

**Fetching Key Names (for UI):**
```
1. UI requests: GET /api/registry/config/secret-keys?class_name=EODHD
2. Backend decrypts ciphertext, extracts JSON keys (not values)
3. Returns: ["api_key", "api_secret"]
4. UI renders input fields with those labels
```

**Updating Credentials:**
```
1. User fills in all credential fields
2. UI submits: PATCH /api/registry/config/secrets
   {
     "class_name": "EODHD",
     "class_type": "provider",
     "secrets": { "api_key": "new_key", "api_secret": "new_secret" }
   }
3. Backend:
   a. Fetches existing file_hash from code_registry
   b. Derives encryption key from file_hash (same as original)
   c. Generates new nonce (12 random bytes)
   d. Encrypts new secrets JSON with AES-256-GCM
   e. Updates (nonce, ciphertext) in code_registry
   f. Unloads provider from DataHub (if loaded)
4. Next data request triggers automatic provider reload with new credentials
```

### Security Model

- **No values returned to frontend**: Only key names are sent to UI, never actual secrets
- **All-or-nothing updates**: User must provide all secrets together (no partial updates)
- **Same encryption scheme**: Uses existing HKDF + AES-256-GCM with file hash as derivation input
- **New nonce per update**: Critical for AES-GCM security; reusing nonces would be catastrophic
- **Provider unloaded**: Ensures stale credentials aren't used; reload picks up new values

### Configuration Scope

- **Per-provider**: Each provider has its own encrypted credential blob
- **Immediate effect**: Provider unloads on update; next request triggers reload

### Data Model

No schema changes required. Uses existing columns in `code_registry`:
- `nonce` (BYTEA): Updated with new random 12 bytes
- `ciphertext` (BYTEA): Updated with new encrypted payload
- `file_hash` (BYTEA): Unchanged; still used for key derivation

### Backend Changes

**New endpoint in `quasar/services/registry/handlers/config.py`:**

```python
# GET /api/registry/config/secret-keys
async def handle_get_secret_keys(self, class_name: str, class_type: str) -> list[str]:
    """Return the names of stored secrets (not values) for a provider."""
    # 1. Fetch nonce, ciphertext, file_hash from code_registry
    # 2. Derive key from file_hash
    # 3. Decrypt ciphertext
    # 4. Parse JSON, return list of keys
    return list(secrets_dict.keys())  # e.g., ["api_key", "api_secret"]


# PATCH /api/registry/config/secrets
async def handle_update_secrets(
    self,
    class_name: str,
    class_type: str,
    secrets: dict[str, str]
) -> dict:
    """Re-encrypt and store new credentials for a provider."""
    # 1. Fetch file_hash from code_registry
    # 2. Derive key from file_hash
    # 3. Generate new nonce
    # 4. Encrypt secrets JSON
    # 5. UPDATE code_registry SET nonce=$1, ciphertext=$2 WHERE class_name=$3
    # 6. Notify DataHub to unload provider (via API call or shared signal)
    return {"status": "updated", "keys": list(secrets.keys())}
```

**DataHub provider unload:**

The registry service needs to signal DataHub to unload the provider. Options:
- **Option A**: Registry calls DataHub API endpoint (e.g., `POST /api/datahub/providers/{name}/unload`)
- **Option B**: Shared database flag that DataHub checks on refresh cycle
- **Option C**: Direct function call if services are co-located

Option A is cleanest for service separation.

---

## UI Design

### Location

The **"API Secrets"** tab already exists in the Provider Config Modal (added in PR #45 as a placeholder). Replace the "Coming Soon" content with the actual credential update form.

### Component: Secret Key Input Form

```
┌─────────────────────────────────────────────────────────────┐
│  Provider Settings: EODHD                              [X]  │
├─────────────────────────────────────────────────────────────┤
│  [Trading Preferences]  [Scheduling]  [API Secrets]         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  API Credentials                                            │
│  ───────────────                                            │
│  Update the API credentials for this provider. All fields   │
│  must be filled in together.                                │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  api_key                                            │   │
│  │  ┌─────────────────────────────────────────────┐   │   │
│  │  │ ••••••••••••••••                            │   │   │
│  │  └─────────────────────────────────────────────┘   │   │
│  │                                              [Show]│   │
│  │                                                    │   │
│  │  api_secret                                        │   │
│  │  ┌─────────────────────────────────────────────┐   │   │
│  │  │ ••••••••••••••••                            │   │   │
│  │  └─────────────────────────────────────────────┘   │   │
│  │                                              [Show]│   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  ⚠️  Updating credentials will temporarily          │   │
│  │     disconnect this provider. Data collection       │   │
│  │     will resume automatically on the next           │   │
│  │     scheduled pull.                                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│                                        [Cancel] [Update]    │
└─────────────────────────────────────────────────────────────┘
```

### UI Elements

1. **Dynamic Field Generation**
   - On tab open: `GET /api/registry/config/secret-keys` returns `["api_key", "api_secret"]`
   - Render one password input per key name
   - Fields pre-filled with `••••••••` (fake placeholder, not real data)

2. **Password Inputs**
   - Type: `password` (masked by default)
   - [Show] toggle reveals typed content (standard pattern)
   - Placeholder clears on focus (user types fresh value)

3. **Warning Banner**
   - Explains that updating will disconnect the provider temporarily
   - Reassures that data collection resumes automatically

4. **Validation**
   - All fields required (all-or-nothing)
   - [Update] button disabled until all fields have input
   - Confirm dialog before submission (destructive action)

### Interaction Flow

```
1. User opens Provider Config Modal
2. Clicks "API Secrets" tab
3. UI fetches GET /api/registry/config/secret-keys?class_name=EODHD
4. UI renders input fields: [api_key: ••••••••] [api_secret: ••••••••]
5. User clears fields, types new credentials
6. User clicks [Update]
7. Confirm dialog: "This will update credentials and reload the provider. Continue?"
8. User confirms
9. UI submits PATCH /api/registry/config/secrets with new values
10. Backend re-encrypts, stores, signals DataHub to unload
11. Toast: "Credentials updated. Provider will reload on next scheduled pull."
12. Modal closes
```

### Why This UI Works

- **Familiar pattern**: Password fields with show/hide toggle are universally understood
- **No secrets exposed**: Backend never returns actual values; UI shows fake asterisks
- **Clear warning**: User understands the temporary disconnection impact
- **All-or-nothing enforced**: Can't submit until all fields filled
- **Confirmation step**: Prevents accidental credential changes

### Edge Cases

- **Provider not loaded**: Update still works; new credentials used when eventually loaded
- **Invalid credentials**: Provider will fail on next load; user sees error in logs/UI
- **Missing keys**: If user provides different keys than original, old keys are lost (all-or-nothing)
- **Concurrent updates**: Last write wins; no locking needed for single-user system

---

## Implementation Notes

### Existing Infrastructure

- `SystemContext.get_derived_context(file_hash)` already derives keys from file hash
- `SystemContext.create_context_data(hash, data)` already encrypts with fresh nonce
- `DerivedContext.get(key)` already decrypts on-demand
- Config modal with "API Secrets" tab placeholder exists

### New Work Required

1. **Backend**:
   - New endpoint `GET /api/registry/config/secret-keys`
   - New endpoint `PATCH /api/registry/config/secrets`
   - DataHub unload trigger (API call or signal)

2. **Frontend**:
   - Replace "Coming Soon" placeholder in API Secrets tab
   - Dynamic password field generation from key names
   - Confirmation dialog before update

3. **DataHub**:
   - New endpoint `POST /api/datahub/providers/{name}/unload` (if using Option A)
   - Or: Check for unload signal in refresh cycle

### Testing

- Unit test: Re-encryption produces valid ciphertext decryptable with same key
- Unit test: New nonce generated on each update (never reused)
- Integration test: Credential update followed by provider reload uses new values
- Integration test: Provider unloads after credential update
- UI test: Fields render from key names, submission triggers update

---

## Design Decisions

1. **All-or-nothing updates**: Simplifies implementation and avoids partial state; users must provide complete credential set

2. **No values returned to frontend**: Key names only; fake asterisks in UI provide visual confirmation without security risk

3. **Provider unload on update**: Ensures no stale credentials in memory; automatic reload on next data request handles reconnection

4. **Same encryption scheme**: No crypto changes; file hash remains the key derivation input, just re-encrypt with new nonce
