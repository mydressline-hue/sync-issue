# Pipeline Verification Report: 20-Step Unified Pipeline vs. 7 Import Paths

**Date:** 2026-02-10
**Files analyzed:**
- `/home/user/sync-issue/aiImportRoutes (6).ts` lines 1896-3062 (`/execute`)
- `/home/user/sync-issue/aiImportRoutes (6).ts` lines 3988-4583 (`executeAIImport`)
- `/home/user/sync-issue/routes (29).ts` lines 322-802 (`processUrlDataSourceImport`)
- `/home/user/sync-issue/routes (29).ts` lines 2167-3073 (`performCombineImport`)
- `/home/user/sync-issue/routes (29).ts` lines 4132-4965 (`/upload`)
- `/home/user/sync-issue/routes (29).ts` lines 5063-5774 (`/fetch-url`)
- `/home/user/sync-issue/importUtils (10).ts` lines 1965-3030 (`processEmailAttachment`)
- `/home/user/sync-issue/routes (29).ts` lines 5958-6250 (`/reimport` -- bonus)

---

## 1. The 7x20 Verification Matrix

Legend:
- PASS = present and correct
- MISS = MISSING (bug to fix)
- DIFF = present but different logic (see notes)
- N/A  = not applicable for this path

### PHASE 1: PARSE

| # | Step | /execute | executeAIImport | processUrl | performCombine | /upload | /fetch-url | processEmail |
|---|------|----------|-----------------|------------|----------------|---------|------------|--------------|
| 1 | Read & consolidate files | PASS | PASS | PASS | DIFF [A] | PASS | PASS | PASS |
| 2 | autoDetectPivotFormat() | PASS | PASS | PASS | N/A [B] | PASS | PASS | PASS |
| 3 | parseIntelligentPivotFormat() | PASS | PASS | PASS | N/A [B] | PASS | PASS | PASS |
| 4 | applyCleaningToValue() on style | PASS | PASS | DIFF [C] | DIFF [D] | DIFF [C] | DIFF [C] | PASS |

**Notes:**
- [A] `performCombineImport` reads from pre-staged files in the database (previewData arrays), not from raw file buffers. Parsing already happened during staging.
- [B] Steps 2 and 3 are N/A for `performCombineImport` because files were already parsed during the staging step (via `/upload` or `processEmailAttachment`). The staged rows already contain standardized headers from the shared parsers.
- [C] In `processUrlDataSourceImport`, `/upload`, and `/fetch-url`: `applyCleaningToValue()` is only called explicitly when the shared parser path is used (i.e., `autoDetectPivotFormat` returns a format). When the legacy parsers (`parsePivotedExcelToInventory` or `parseExcelToInventory`) are used, cleaning is handled internally by those parsers with different logic. This means cleaning behavior may differ depending on which parser branch is taken.
- [D] `performCombineImport` uses inline cleaning logic (lines 2479-2502) for non-pre-parsed files: manual `trimWhitespace`, `removeLetters`, `removeNumbers`, `removeSpecialChars`, `findText/replaceText`. This is a subset of what `applyCleaningToValue()` does -- it MISSES: `findReplaceRules`, `removeFirstN`, `removeLastN`, `removePatterns`. For pre-parsed files, cleaning was already done during staging.

### PHASE 2: FILTER

| # | Step | /execute | executeAIImport | processUrl | performCombine | /upload | /fetch-url | processEmail |
|---|------|----------|-----------------|------------|----------------|---------|------------|--------------|
| 5 | Skip rule filtering | MISS | MISS | MISS | MISS | PASS | PASS | PASS |
| 6 | Filter discontinued zero-stock | MISS | MISS | MISS | MISS | PASS | PASS | PASS |
| 7 | deduplicateAndZeroFutureStock() | PASS | PASS | MISS | DIFF [E] | PASS | PASS | PASS |

**Notes:**
- [E] `performCombineImport` calls `deduplicateAndZeroFutureStock()` (line 2921), BUT it ALSO has a custom, older "future stock zeroing" block (lines 2659-2719) that runs BEFORE the standard function. This double-zeroing could cause issues: the custom block zeros stock for items with future ship dates, then `deduplicateAndZeroFutureStock()` runs again and may re-process. The custom block also has different logic -- it only runs when there are 2+ unique ship dates (snapshot mode detection) and uses a cutoff date calculation with offset.

### PHASE 3: TRANSFORM

| # | Step | /execute | executeAIImport | processUrl | performCombine | /upload | /fetch-url | processEmail |
|---|------|----------|-----------------|------------|----------------|---------|------------|--------------|
| 8 | Style prefix | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| 9 | cleanInventoryData() (AI color) | MISS | MISS | PASS | PASS | PASS | PASS | PASS |
| 10 | applyImportRules() | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| 11 | Global color mappings | PASS [F] | PASS [F] | DIFF [G] | DIFF [G] | DIFF [G] | DIFF [G] | DIFF [G] |
| 12 | applyVariantRules() | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| 13 | applyPriceBasedExpansion() | PASS | PASS | PASS | PASS | PASS | PASS | PASS |

**Notes:**
- [F] `/execute` and `executeAIImport` apply global color mappings EXPLICITLY by loading `storage.getColorMappings()` and mapping each item's color through the badColor->goodColor map. They also call `formatColorName()` and rebuild SKUs with the corrected color.
- [G] The 5 routes-based paths (`processUrl`, `performCombine`, `/upload`, `/fetch-url`, `processEmail`) do NOT have explicit global color mapping code. Instead, they rely on `cleanInventoryData()` to handle color normalization. `cleanInventoryData()` does AI-powered color fixing but it is **unclear** whether it applies the same global `badColor->goodColor` database mappings that `/execute` uses explicitly. If `cleanInventoryData()` does NOT query `storage.getColorMappings()`, then the routes-based paths are MISSING the global color mapping step entirely.

### PHASE 4: BUSINESS LOGIC

| # | Step | /execute | executeAIImport | processUrl | performCombine | /upload | /fetch-url | processEmail |
|---|------|----------|-----------------|------------|----------------|---------|------------|--------------|
| 14 | filterDiscontinuedStyles() | PASS | PASS | MISS | PASS | PASS | PASS | PASS |
| 15 | Sale file pricing | MISS | MISS | PASS | PASS | PASS | PASS | PASS |
| 16 | calculateStockInfo() | PASS | PASS | PASS | PASS | PASS | PASS | DIFF [H] |

**Notes:**
- [H] `processEmailAttachment` uses `getStockInfoRuleForEmail()` (line 2788) instead of `getStockInfoRule()`. This is a different function that may have different logic for resolving the stock info rule. All other paths use `getStockInfoRule()` (routes paths) or inline rule resolution (`/execute`, `executeAIImport`).

### PHASE 5: SAVE

| # | Step | /execute | executeAIImport | processUrl | performCombine | /upload | /fetch-url | processEmail |
|---|------|----------|-----------------|------------|----------------|---------|------------|--------------|
| 17 | Safety nets | PASS [I] | PASS [I] | PASS | PASS | PASS | PASS | DIFF [J] |
| 18 | Save to DB | PASS [K] | PASS [K] | PASS | PASS | PASS | PASS | PASS |
| 19 | Save import stats | PASS | MISS | MISS | MISS | MISS | MISS | MISS |
| 20 | Post-import | DIFF [L] | MISS | MISS | MISS | DIFF [M] | PASS [N] | DIFF [O] |

**Notes:**
- [I] `/execute` and `executeAIImport` implement safety nets inline (0-item check + 50% drop check) using `storage.getInventoryItemCountByDataSource()`. They do NOT use `checkSafetyThreshold()`. The routes-based paths (except processEmail) use `checkSafetyThreshold()` which may have different thresholds or logic.
- [J] `processEmailAttachment` has a custom safety implementation using a per-data-source `safetyThreshold` setting (default 50%), with immediate alert sending on safety blocks. This is different from both the /execute inline check and the routes `checkSafetyThreshold()`.
- [K] `/execute` and `executeAIImport` use `storage.deleteInventoryItemsByDataSource()` + `storage.createInventoryItems()` for full sync. The routes-based paths use `storage.atomicReplaceInventoryItems()` for full sync. The atomic version is safer (transactional).
- [L] `/execute` has post-import validation with checksums but does NOT trigger auto-consolidation or Shopify sync.
- [M] `/upload` triggers a background comparison job and registers sale file styles, but does NOT trigger auto-consolidation or Shopify sync.
- [N] `/fetch-url` triggers BOTH auto-consolidation AND Shopify sync -- the most complete post-import behavior.
- [O] `processEmailAttachment` signals `completeImport()`/`failImport()` for sync coordination, but does NOT trigger auto-consolidation or Shopify sync.

---

## 2. Summary of ALL Missing Steps (Bugs to Fix)

### Critical Missing Steps (affect data correctness):

| Path | Missing Step | Impact | Severity |
|------|-------------|--------|----------|
| `/execute` | Step 5: Skip rule filtering | Items with `shouldSkip=true` are not filtered out | HIGH |
| `/execute` | Step 6: Filter discontinued zero-stock | Discontinued items with zero stock are imported (dead inventory) | HIGH |
| `/execute` | Step 9: cleanInventoryData() | No AI color normalization, no no-size removal, no duplicate removal | HIGH |
| `/execute` | Step 15: Sale file pricing | Sale files imported via /execute won't have price multiplier or compare-at pricing | MEDIUM |
| `executeAIImport` | Step 5: Skip rule filtering | Same as /execute | HIGH |
| `executeAIImport` | Step 6: Filter discontinued zero-stock | Same as /execute | HIGH |
| `executeAIImport` | Step 9: cleanInventoryData() | Same as /execute | HIGH |
| `executeAIImport` | Step 15: Sale file pricing | Same as /execute | MEDIUM |
| `executeAIImport` | Step 19: Save import stats | No lastImportStats saved -- validation/checksum comparison will have stale data | MEDIUM |
| `executeAIImport` | Step 20: Post-import | No validation, no auto-consolidation, no Shopify sync | MEDIUM |
| `processUrl` | Step 5: Skip rule filtering | Items with shouldSkip not filtered | HIGH |
| `processUrl` | Step 6: Filter discontinued zero-stock | Dead inventory imported | HIGH |
| `processUrl` | Step 7: deduplicateAndZeroFutureStock() | Duplicate style-color-size items imported, future stock not zeroed | HIGH |
| `processUrl` | Step 14: filterDiscontinuedStyles() | Discontinued styles from sale files not filtered | HIGH |
| `processUrl` | Step 19: Save import stats | No lastImportStats saved | MEDIUM |
| `processUrl` | Step 20: Post-import | No validation, no auto-consolidation, no Shopify sync | MEDIUM |
| `performCombine` | Step 5: Skip rule filtering | Items with shouldSkip not filtered | HIGH |
| `performCombine` | Step 6: Filter discontinued zero-stock | Dead inventory imported | HIGH |
| `performCombine` | Step 19: Save import stats | No lastImportStats saved | MEDIUM |
| `performCombine` | Step 20: Post-import | No validation, no auto-consolidation, no Shopify sync | MEDIUM |
| `/upload` | Step 19: Save import stats | No lastImportStats saved | MEDIUM |
| `/fetch-url` | Step 19: Save import stats | No lastImportStats saved | MEDIUM |
| `processEmail` | Step 19: Save import stats | No lastImportStats saved | MEDIUM |

**Total critical bugs: 12 HIGH severity across 4 paths.**

### Step 19 (Save import stats) is MISSING from 6 of 7 paths.
Only `/execute` saves `lastImportStats`. All other paths skip this, meaning checksum validation and historical comparison only works for `/execute` imports.

---

## 3. Extra/Path-Specific Logic NOT in the 20 Steps

These behaviors exist in specific paths but are NOT part of the 20-step pipeline. They should remain in the route handler (not the engine) OR be added as optional engine steps.

### Pre-Import Validation (should stay in route handler)

| Logic | Paths that have it | Paths that DON'T |
|-------|-------------------|-----------------|
| `validateImportFile()` (file corruption/structure check) | processUrl, /upload, /fetch-url, processEmail | /execute, executeAIImport, performCombine |
| `checkSaleImportFirstRequirement()` (require sale file first) | processUrl, /fetch-url | /execute, executeAIImport, performCombine, /upload, processEmail |
| Template validation (`validateTemplate()`) | processEmail | All others |
| Row count validation (min/max/tolerance) | processEmail | All others |
| Default safety check (safetyThreshold on raw row count) | processEmail | All others |
| `requireAllColumns` validation | processEmail | All others |

### Import State Tracking (should stay in route handler)

| Logic | Paths that have it |
|-------|-------------------|
| `startImport()` / `completeImport()` / `failImport()` | /upload, processEmail |
| Immediate alert sending (`sendImmediateImportAlert`) | processEmail |

### Multi-File Staging (should stay in route handler)

| Logic | Paths that have it |
|-------|-------------------|
| Multi-file staging mode (stage without importing) | /upload, processEmail |
| `overrideConfig` support (UI sends config overrides) | /execute, executeAIImport |

### Post-Import Actions (should stay in route handler, but consistently)

| Logic | Paths that have it | Paths that DON'T |
|-------|-------------------|-----------------|
| `registerSaleFileStyles()` | performCombine, /upload, /fetch-url, processEmail | /execute, executeAIImport, processUrl |
| `cleanupSaleOwnedItemsFromRegular()` | /upload | All others |
| `triggerAutoConsolidationAfterImport()` | /fetch-url, /reimport | /execute, executeAIImport, processUrl, performCombine, /upload, processEmail |
| `triggerShopifySyncAfterImport()` | /fetch-url, /reimport | /execute, executeAIImport, processUrl, performCombine, /upload, processEmail |
| Background comparison job (`startComparisonJob`) | /upload | All others |
| Post-import validation with checksums | /execute | All others |
| Save detected format to DB (`updateDataSource`) | processUrl, /upload, /fetch-url, processEmail | /execute, executeAIImport, performCombine |

### Unique Logic per Path

| Path | Unique Logic |
|------|-------------|
| `performCombineImport` | Auto-detect column mapping from staged file headers (lines 2245-2292) |
| `performCombineImport` | Jovani sale file stateful parsing (lines 2414-2453) |
| `performCombineImport` | Combined variant column handling (lines 2456-2477) |
| `performCombineImport` | Custom inline future stock zeroing SEPARATE from deduplicateAndZeroFutureStock (lines 2659-2719) |
| `processEmail` | Email-specific diagnostic logging to /tmp/email_download.log |
| `processEmail` | Uses `getStockInfoRuleForEmail()` instead of `getStockInfoRule()` |

---

## 4. Ordering Differences Between Paths

The 20-step pipeline defines this order:
```
PHASE 1 (Parse):     1 → 2 → 3 → 4
PHASE 2 (Filter):    5 → 6 → 7
PHASE 3 (Transform): 8 → 9 → 10 → 11 → 12 → 13
PHASE 4 (Business):  14 → 15 → 16
PHASE 5 (Save):      17 → 18 → 19 → 20
```

### Actual order in each path:

**`/execute`:**
```
1,2,3,4 → 10,11,8 → 12,13,14 → 7,16 → 17,18,19,20
```
- applyImportRules (10) and global color mappings (11) run BEFORE prefix (8)
- dedup (7) runs AFTER filterDiscontinuedStyles (14) -- items that get discontinued-filtered were needlessly deduped
- Skip (5), Disco-zero (6), cleanInventoryData (9), Sale pricing (15) ALL MISSING

**`executeAIImport`:**
```
1,2,3,4 → 10,11,8 → 12,13,14 → 7,16 → 17,18
```
- Same order as /execute but missing steps 19, 20

**`processUrlDataSourceImport`:**
```
1,2,3,4 → 8,9,10 → 12,13 → 15,16 → 17,18
```
- Prefix (8) runs BEFORE cleanInventoryData (9) and importRules (10) -- correct
- Skip (5), Disco-zero (6), Dedup (7), Disco-styles (14) ALL MISSING
- Global color mappings (11) only via cleanInventoryData

**`performCombineImport`:**
```
1(staged),4(inline),8 → CUSTOM_FUTURE_ZEROING → 9,10 → 12,13,14 → 15 → 7,16 → 17,18
```
- Has EXTRA custom future stock zeroing before cleanInventoryData
- dedup (7) runs AFTER discontinued filter (14) and sale pricing (15)
- Skip (5), Disco-zero (6) MISSING

**`/upload` (closest to ideal):**
```
1,2,3,4 → 5,6,7 → 8,9,10 → 12,13 → 14,15,16 → 17,18
```
- Most aligned with the 20-step pipeline
- Missing: 19

**`/fetch-url`:**
```
1,2,3,4 → 5,6 → 8,9,10 → 12,13 → 15 → 7,16 → 14 → 17,18,20
```
- dedup (7) runs AFTER sale pricing (15) but BEFORE filterDiscontinuedStyles (14)
- filterDiscontinuedStyles (14) runs AFTER calculateStockInfo (16)
- Missing: 19

**`processEmailAttachment`:**
```
1,2,3,4 → 5,6,7 → 8,9,10 → 12,13 → 16 → 14 → 15 → 17,18
```
- calculateStockInfo (16) runs BEFORE filterDiscontinuedStyles (14) and sale pricing (15)
- This means stockInfo is calculated for items that later get filtered
- Missing: 19

### Key Ordering Inconsistencies:

| Issue | Paths Affected | Consequence |
|-------|---------------|-------------|
| Step 7 (dedup) runs AFTER step 14 (filter discontinued) | /execute, executeAIImport, performCombine, /fetch-url | Dedup operates on post-filtered items -- possibly OK but inconsistent |
| Step 8 (prefix) runs AFTER step 10 (importRules) | /execute, executeAIImport | Import rules see unprefixed styles; variant rules see prefixed styles |
| Step 16 (stockInfo) runs BEFORE step 14 (filterDiscontinued) | /fetch-url, processEmail | StockInfo calculated for items that may be removed |
| Step 16 (stockInfo) runs BEFORE step 15 (salePricing) | processEmail | StockInfo calculated before price adjustment |
| Custom future stock zeroing runs before dedup | performCombine | Double processing -- stock may be zeroed twice |

---

## 5. /reimport Route Analysis (Bonus)

The `/reimport` route (lines 5958-6250) is NOT one of the 7 primary paths but was reviewed as requested.

| # | Step | /reimport | Notes |
|---|------|-----------|-------|
| 1 | Read files | DIFF | Reads from existing DB items, not file buffers |
| 2 | autoDetect | N/A | No file to detect |
| 3 | parseIntelligent | N/A | No file to parse |
| 4 | applyCleaningToValue() | PASS | Applied to raw styles from DB items |
| 5 | Skip rule filtering | MISS | |
| 6 | Filter discontinued zero-stock | MISS | |
| 7 | deduplicateAndZeroFutureStock() | PASS | |
| 8 | Style prefix | PASS | |
| 9 | cleanInventoryData() | PASS | |
| 10 | applyImportRules() | DIFF [P] | |
| 11 | Global color mappings | DIFF | Via cleanInventoryData |
| 12 | applyVariantRules() | PASS | |
| 13 | applyPriceBasedExpansion() | PASS | |
| 14 | filterDiscontinuedStyles() | MISS | |
| 15 | Sale file pricing | MISS | |
| 16 | calculateStockInfo() | PASS | |
| 17 | Safety nets | MISS | Deletes all items unconditionally before re-import |
| 18 | Save to DB | DIFF | Uses createInventoryItems only (no upsert/atomic) |
| 19 | Save import stats | MISS | |
| 20 | Post-import | PASS | Auto-consolidation + Shopify sync |

**[P] BUG: Line 6112 references `rows` variable which is NEVER DEFINED in the /reimport scope.** The /reimport route reads from existing DB items, not from file parsing, so there are no `rows`. This passes `undefined` to `applyImportRules()`, which may cause the function to skip raw-data-dependent logic silently.

---

## 6. Recommendations for the Unified Engine

### Priority 1: Add missing steps to /execute and executeAIImport

These two AI import paths are missing 4 critical steps each. Since they are the most recent code and handle the "Import 2 Files" and email-combined workflows, these gaps mean:
- Skip rules are ignored (items that should be filtered get imported)
- Discontinued items with zero stock get imported (dead inventory)
- No AI color normalization or duplicate removal
- Sale files won't get proper pricing

**Recommendation:** Add steps 5, 6, 9, and 15 to both paths. Since /execute and executeAIImport use explicit global color mappings (step 11) instead of cleanInventoryData, step 9 should be added for its OTHER functions (no-size removal, AI color, duplicate removal) while keeping the explicit color mapping.

### Priority 2: Add missing steps to processUrlDataSourceImport

This path is missing 6 steps including dedup and discontinued filtering, which are critical for data correctness. Since URL imports often run on automated schedules, bad data here can propagate undetected.

**Recommendation:** Add steps 5, 6, 7, 14, 19, 20.

### Priority 3: Standardize step ordering

The engine should enforce a single canonical order. The `/upload` route is closest to the ideal:
```
Parse(1-4) → Filter(5-7) → Transform(8-13) → Business(14-16) → Save(17-20)
```

Key ordering fixes needed:
- `/execute` and `executeAIImport`: Move prefix (8) BEFORE importRules (10)
- All paths: Standardize when dedup (7) runs relative to other steps
- `processEmail` and `/fetch-url`: Move stockInfo (16) AFTER filterDiscontinued (14) and salePricing (15)

### Priority 4: Save import stats from ALL paths

Only `/execute` saves `lastImportStats`. This should be added to all 7 paths to enable historical comparison and checksum validation regardless of how data was imported.

### Priority 5: Standardize post-import actions

Auto-consolidation and Shopify sync should fire consistently. Currently only `/fetch-url` and `/reimport` trigger both. Either add these to all paths or document why they're intentionally omitted from certain paths.

### Priority 6: Eliminate performCombineImport's custom future stock zeroing

The custom inline future stock zeroing (lines 2659-2719) in `performCombineImport` is redundant with `deduplicateAndZeroFutureStock()` and uses different logic (snapshot mode detection, cutoff date calculation). This should be removed in favor of the standard function to prevent double-processing.

### Priority 7: Fix /reimport undefined `rows` bug

Line 6112 of `/reimport` passes an undefined `rows` variable to `applyImportRules()`. This should pass `undefined` explicitly or `[]` to avoid silent failures.

### Priority 8: Investigate cleanInventoryData vs. explicit global color mappings

Determine whether `cleanInventoryData()` applies the same `storage.getColorMappings()` badColor->goodColor mappings that `/execute` applies explicitly. If not, 5 out of 7 paths are missing global color database mappings and only get AI color normalization.

### Priority 9: Standardize safety net implementation

Three different safety net implementations exist:
1. `/execute` and `executeAIImport`: Inline 0-item + 50% drop check
2. Routes paths: `checkSafetyThreshold()` function
3. `processEmail`: Custom per-data-source `safetyThreshold` with alerts

The engine should use a single implementation. The `processEmail` approach (per-data-source threshold + alerts) is the most configurable.

### Priority 10: Standardize DB write strategy

- `/execute` and `executeAIImport` use `deleteInventoryItemsByDataSource()` + `createInventoryItems()` (non-atomic)
- Routes paths use `atomicReplaceInventoryItems()` (transactional)

The atomic version is safer and should be used universally.

---

## 7. Complete Bug List (Actionable)

| # | Bug | File | Lines | Severity |
|---|-----|------|-------|----------|
| 1 | `/execute` missing skip rule filtering (step 5) | aiImportRoutes (6).ts | ~2090 | HIGH |
| 2 | `/execute` missing discontinued zero-stock filter (step 6) | aiImportRoutes (6).ts | ~2090 | HIGH |
| 3 | `/execute` missing cleanInventoryData (step 9) | aiImportRoutes (6).ts | ~2207 | HIGH |
| 4 | `/execute` missing sale file pricing (step 15) | aiImportRoutes (6).ts | ~2413 | MEDIUM |
| 5 | `executeAIImport` missing skip rule filtering (step 5) | aiImportRoutes (6).ts | ~4157 | HIGH |
| 6 | `executeAIImport` missing discontinued zero-stock filter (step 6) | aiImportRoutes (6).ts | ~4157 | HIGH |
| 7 | `executeAIImport` missing cleanInventoryData (step 9) | aiImportRoutes (6).ts | ~4241 | HIGH |
| 8 | `executeAIImport` missing sale file pricing (step 15) | aiImportRoutes (6).ts | ~4380 | MEDIUM |
| 9 | `executeAIImport` missing save import stats (step 19) | aiImportRoutes (6).ts | ~4558 | MEDIUM |
| 10 | `executeAIImport` missing post-import actions (step 20) | aiImportRoutes (6).ts | ~4558 | MEDIUM |
| 11 | `processUrl` missing skip rule filtering (step 5) | routes (29).ts | ~487 | HIGH |
| 12 | `processUrl` missing discontinued zero-stock filter (step 6) | routes (29).ts | ~487 | HIGH |
| 13 | `processUrl` missing deduplicateAndZeroFutureStock (step 7) | routes (29).ts | ~573 | HIGH |
| 14 | `processUrl` missing filterDiscontinuedStyles (step 14) | routes (29).ts | ~647 | HIGH |
| 15 | `processUrl` missing save import stats (step 19) | routes (29).ts | ~778 | MEDIUM |
| 16 | `processUrl` missing post-import actions (step 20) | routes (29).ts | ~778 | MEDIUM |
| 17 | `performCombine` missing skip rule filtering (step 5) | routes (29).ts | ~2302 | HIGH |
| 18 | `performCombine` missing discontinued zero-stock filter (step 6) | routes (29).ts | ~2302 | HIGH |
| 19 | `performCombine` duplicate future stock zeroing logic | routes (29).ts | 2659-2719 | MEDIUM |
| 20 | `performCombine` inline cleaning misses applyCleaningToValue features | routes (29).ts | 2479-2502 | MEDIUM |
| 21 | `performCombine` missing save import stats (step 19) | routes (29).ts | ~3024 | MEDIUM |
| 22 | `performCombine` missing post-import actions (step 20) | routes (29).ts | ~3024 | MEDIUM |
| 23 | `/upload` missing save import stats (step 19) | routes (29).ts | ~4855 | MEDIUM |
| 24 | `/fetch-url` missing save import stats (step 19) | routes (29).ts | ~5731 | MEDIUM |
| 25 | `processEmail` missing save import stats (step 19) | importUtils (10).ts | ~3002 | MEDIUM |
| 26 | `processEmail` uses different stockInfo function (getStockInfoRuleForEmail) | importUtils (10).ts | 2788 | LOW |
| 27 | `/reimport` undefined `rows` variable passed to applyImportRules | routes (29).ts | 6112 | HIGH |
| 28 | `/reimport` missing skip rule filtering (step 5) | routes (29).ts | ~6033 | HIGH |
| 29 | `/reimport` missing discontinued zero-stock filter (step 6) | routes (29).ts | ~6033 | HIGH |
| 30 | `/reimport` missing filterDiscontinuedStyles (step 14) | routes (29).ts | ~6167 | MEDIUM |
| 31 | `/reimport` missing sale file pricing (step 15) | routes (29).ts | ~6167 | MEDIUM |
| 32 | `/reimport` missing safety nets (step 17) | routes (29).ts | 5986 | MEDIUM |
| 33 | `/reimport` non-atomic DB write | routes (29).ts | 6194 | LOW |
| 34 | `/execute` and `executeAIImport` non-atomic DB write | aiImportRoutes (6).ts | 2711,4554 | LOW |
| 35 | `/execute` prefix (step 8) runs AFTER importRules (step 10) | aiImportRoutes (6).ts | 2208 | MEDIUM |
| 36 | `executeAIImport` prefix (step 8) runs AFTER importRules (step 10) | aiImportRoutes (6).ts | 4243 | MEDIUM |
| 37 | `processEmail` stockInfo (16) calculated before discontinued filter (14) | importUtils (10).ts | 2787 | LOW |
| 38 | `/fetch-url` stockInfo (16) calculated before discontinued filter (14) | routes (29).ts | 5573 | LOW |
| 39 | `/execute` missing registerSaleFileStyles post-import | aiImportRoutes (6).ts | ~2714 | MEDIUM |
| 40 | `executeAIImport` missing registerSaleFileStyles post-import | aiImportRoutes (6).ts | ~4558 | MEDIUM |
| 41 | `processUrl` missing registerSaleFileStyles post-import | routes (29).ts | ~778 | MEDIUM |

**Total: 41 issues identified (12 HIGH, 22 MEDIUM, 7 LOW)**
