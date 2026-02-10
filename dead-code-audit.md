# Dead Code Audit Report

**Date:** 2026-02-10
**Files audited:**
- `routes (29).ts` (22,383 lines)
- `importUtils (10).ts` (6,077 lines)
- `aiImportRoutes (6).ts` (4,585 lines)

**Purpose:** Identify all dead code (duplicated, unused, or moving to importEngine) to prepare for unified importEngine creation.

---

## 1. routes (29).ts -- Dead Code Analysis

### 1.1 `checkSafetyThreshold()` -- Lines 120-137 (18 lines)
- **Status:** MOVING (still actively called)
- **Call sites in routes (29).ts:**
  - Line 730: URL Import path
  - Line 2951: Combine & Import path
  - Line 4792: Manual upload path
  - Line 5652: URL Fetch path
  - Line 6852: Manual Import path
- **Also exists in:** `importEngine-section-helpers.ts` (lines 323+, the new home)
- **Verdict:** Cannot remove yet -- actively called 5 times in routes (29).ts. Move to importEngine, then update all call sites to import from importEngine.

### 1.2 `isCSVBuffer()` -- Lines 145-196 (52 lines)
- **Status:** DEAD in routes (29).ts
- **Call sites in routes (29).ts:** Only line 1876 (inside `parseExcelToInventory`, which is itself dead -- see 1.10)
- **Duplicate exists in:** `importUtils (10).ts` line 758 (called at line 942 by importUtils' own `parseExcelToInventory`)
- **Verdict:** DEAD. Only called by `parseExcelToInventory` in routes, which is itself dead code (only used as last-resort fallback by other dead parsers). Remove when `parseExcelToInventory` is removed.

### 1.3 `parseCSVAsText()` -- Lines 199-271 (73 lines)
- **Status:** DEAD in routes (29).ts
- **Call sites in routes (29).ts:** Only line 1880 (inside `parseExcelToInventory`)
- **Duplicate exists in:** `importUtils (10).ts` line 815 (called at line 946)
- **Verdict:** DEAD. Same situation as `isCSVBuffer` -- only called by the dead `parseExcelToInventory`. Remove together.

### 1.4 `applyCleaningToValue()` -- Lines 808-876 (69 lines)
- **Status:** DEAD DUPLICATE (but still called in routes)
- **Call sites in routes (29).ts:** Lines 450, 1239, 1610, 1853, 2004, 4287, 5223, 6055
- **Canonical version:** `importUtils (10).ts` line 614 (exported, imported by aiImportRoutes)
- **Analysis:** Routes (29).ts defines its OWN local copy AND calls it. The lines 450, 4287, 5223, 6055 are live code paths (URL import, manual upload, URL fetch, manual re-import). Lines 1239, 1610, 1853, 2004 are inside dead parsers (parseJovaniFormat, parseGenericPivotedFormat, parseTarikEdizFormat fallback, parseExcelToInventory).
- **Verdict:** DUPLICATE but ACTIVELY USED. When moving to importEngine, the 4 live call sites (450, 4287, 5223, 6055) must be updated to import from importUtils or importEngine. Then the local copy can be removed.

### 1.5 `parseTarikEdizFormat()` -- Lines 879-1019 (141 lines)
- **Status:** DEAD
- **Call sites in routes (29).ts:** Only line 1848 (inside `parseExcelToInventory`)
- **Canonical version:** `aiImportRoutes (6).ts` line 624 (internal to `parseIntelligentPivotFormat`)
- **Verdict:** DEAD. Only called by the dead `parseExcelToInventory`. Remove.

### 1.6 `parseJovaniFormat()` -- Lines 1026-1258 (233 lines)
- **Status:** DEAD
- **Call sites in routes (29).ts:** Only line 1832 (inside `parseExcelToInventory`)
- **Not called from any other file.**
- **Canonical version:** `aiImportRoutes (6).ts` has `parseJovaniSaleFormat` (the unified version)
- **Verdict:** DEAD. Only called by the dead `parseExcelToInventory`. Remove.

### 1.7 `parseSherriHillFormat()` -- Lines 1265-1425 (161 lines)
- **Status:** DEAD
- **Call sites in routes (29).ts:** Only line 1823 (inside `parseExcelToInventory`)
- **Canonical version:** `aiImportRoutes (6).ts` line 775
- **Verdict:** DEAD. Only called by the dead `parseExcelToInventory`. Remove.

### 1.8 `parseGenericPivotedFormat()` -- Lines 1428-1631 (204 lines)
- **Status:** DEAD
- **Call sites in routes (29).ts:** Only line 1838 (inside `parseExcelToInventory`)
- **Canonical version:** `aiImportRoutes (6).ts` has `parseGenericPivotFormat` line 858
- **Verdict:** DEAD. Only called by the dead `parseExcelToInventory`. Remove.

### 1.9 `calculateItemStockInfo()` -- Lines 1637-1733 (97 lines)
- **Status:** MOVING (still actively called)
- **Call sites in routes (29).ts:** Lines 716, 2935, 4780, 5592, 6188
- **Duplicates:** `importUtils (10).ts` line 4071 (identical copy), `importEngine-section-helpers.ts` line 25 (new home)
- **Verdict:** MOVING. Cannot remove yet -- called in 5 places. Move to importEngine, update imports.

### 1.10 `getStockInfoRule()` -- Lines 1736-1814 (79 lines)
- **Status:** MOVING (still actively called)
- **Call sites in routes (29).ts:** Lines 709, 2928, 4773, 5585, 6181
- **Duplicates:** `importUtils (10).ts` line 4162 (`getStockInfoRuleForEmail`, identical logic), `importEngine-section-helpers.ts` line 143 (new home)
- **Verdict:** MOVING. Cannot remove yet -- called in 5 places. Move to importEngine, update imports.

### 1.11 `parseExcelToInventory()` -- Lines 1817-2160 (344 lines)
- **Status:** DEAD (legacy fallback, never reached in practice)
- **Call sites in routes (29).ts:** Lines 479, 4311, 5249
- **Analysis:** Called as the LAST fallback in 3 code paths (URL import, manual upload, URL fetch), after:
  1. `autoDetectPivotFormat` + `parseIntelligentPivotFormat` (shared parsers)
  2. `parsePivotedExcelToInventory` (legacy pivot parser)
  3. Only THEN: `parseExcelToInventory` (generic row fallback)

  The shared auto-detect covers all known vendor formats. This fallback only triggers for completely unrecognized files with no pivotConfig. It dispatches to the dead vendor parsers (parseSherriHillFormat, parseJovaniFormat, parseGenericPivotedFormat, parseTarikEdizFormat, parseFerianiGiaFormat) and then falls through to generic column-mapping parsing.
- **Internally calls:** `parseSherriHillFormat` (1823), `parseJovaniFormat` (1832), `parseGenericPivotedFormat` (1838), `parseTarikEdizFormat` (1848), `parseFerianiGiaFormat` (1863), `isCSVBuffer` (1876), `parseCSVAsText` (1880), `applyCleaningToValue` (2004)
- **Verdict:** DEAD as a standalone function. The generic row-format parsing logic should move to importEngine's `parseRowFormat`. The vendor-specific fallbacks are dead since the shared detector catches them first.

### 1.12 `parseFerianiGiaFormat()` -- Lines 22224-22383 (160 lines)
- **Status:** DEAD
- **Call sites in routes (29).ts:** Only line 1863 (inside `parseExcelToInventory`)
- **Canonical version:** `aiImportRoutes (6).ts` has `parseFerianiFormat` (the unified version)
- **Verdict:** DEAD. Only called by the dead `parseExcelToInventory`. Remove.

### 1.13 Unused Imports -- Line 90-96
- **Status:** DEAD IMPORTS
- **These imported functions are never called in routes (29).ts body:**
  - `parseGenericPivotFormat` (line 91) -- only in import + comment
  - `parseOTSFormat` (line 92) -- only in import
  - `isOTSFormat` (line 93) -- only in import
  - `parseGRNInvoiceFormat` (line 94) -- only in import
  - `parsePRDateHeaderFormat` (line 95) -- only in import
  - `parseStoreMultibrandFormat` (line 96) -- only in import
- **Verdict:** DEAD IMPORTS. These were imported when routes.ts was using them directly, but now the shared `parseIntelligentPivotFormat` handles dispatching. Remove the import lines.

### routes (29).ts Summary

| Function | Lines | Status | Removable Now? |
|---|---|---|---|
| `checkSafetyThreshold` | 120-137 (18) | MOVING | No -- 5 call sites |
| `isCSVBuffer` | 145-196 (52) | DEAD | Yes |
| `parseCSVAsText` | 199-271 (73) | DEAD | Yes |
| `applyCleaningToValue` | 808-876 (69) | DUPLICATE | No -- 4 live call sites need refactoring |
| `parseTarikEdizFormat` | 879-1019 (141) | DEAD | Yes |
| `parseJovaniFormat` | 1026-1258 (233) | DEAD | Yes |
| `parseSherriHillFormat` | 1265-1425 (161) | DEAD | Yes |
| `parseGenericPivotedFormat` | 1428-1631 (204) | DEAD | Yes |
| `calculateItemStockInfo` | 1637-1733 (97) | MOVING | No -- 5 call sites |
| `getStockInfoRule` | 1736-1814 (79) | MOVING | No -- 5 call sites |
| `parseExcelToInventory` | 1817-2160 (344) | DEAD | Yes (but keep generic row logic) |
| `parseFerianiGiaFormat` | 22224-22383 (160) | DEAD | Yes |
| Unused imports | 90-96 (6) | DEAD | Yes |

**Total dead code removable now:** ~1,374 lines (isCSVBuffer 52 + parseCSVAsText 73 + parseTarikEdiz 141 + parseJovani 233 + parseSherriHill 161 + parseGenericPivoted 204 + parseExcelToInventory 344 + parseFerianiGia 160 + unused imports 6)

**Total MOVING (remove after importEngine migration):** ~263 lines (checkSafetyThreshold 18 + applyCleaningToValue 69 + calculateItemStockInfo 97 + getStockInfoRule 79)

---

## 2. importUtils (10).ts -- Dead Code Analysis

### 2.1 `parseJovaniSaleFile()` -- Lines 1324-1442 (119 lines)
- **Status:** USED (internally)
- **Call sites:** Line 1028 (inside `parseExcelToInventory` in the same file)
- **Analysis:** Called as a sub-parser from `parseExcelToInventory` when `cleaningConfig.pivotedFormat.vendor === "jovani"`. Since `parseExcelToInventory` in importUtils IS still called (by `processEmailAttachment` as a fallback, line 2175), this function is technically reachable.
- **Verdict:** KEEP for now. Dies with `parseExcelToInventory` when importEngine replaces all fallbacks.

### 2.2 `parsePivotedExcelToInventory()` -- Lines 1455-1879 (425 lines)
- **Status:** LEGACY (still called as fallback)
- **Call sites in importUtils:** Line 2159 (inside `processEmailAttachment`, as the "pivot but no specific format" fallback)
- **Call sites in routes (29).ts:** Lines 469, 4301, 5239 (legacy pivot fallback in URL import, manual upload, URL fetch)
- **Exported:** Yes
- **Verdict:** LEGACY, MOVING. Still reachable as a fallback when pivotConfig is enabled but no specific format was detected. Will die when importEngine replaces all parsers. Cannot remove yet.

### 2.3 `calculateItemStockInfo()` -- Lines 4071-4160 (90 lines)
- **Status:** MOVING (still actively called)
- **Call sites in importUtils:** Lines 2795, 3729
- **Identical to:** routes (29).ts line 1637, `importEngine-section-helpers.ts` line 25
- **Verdict:** MOVING. Called in `processEmailAttachment` (line 2795) and `combineAndImportStagedFiles` (line 3729). Move to importEngine, update imports.

### 2.4 `getStockInfoRuleForEmail()` -- Lines 4162-4237 (76 lines)
- **Status:** MOVING (still actively called)
- **Call sites in importUtils:** Lines 2788, 3722
- **Identical logic to:** routes (29).ts `getStockInfoRule` line 1736, `importEngine-section-helpers.ts` line 143
- **Verdict:** MOVING. Called in `processEmailAttachment` (line 2788) and `combineAndImportStagedFiles` (line 3722). Move to importEngine, update imports.

### 2.5 `isOTSFormat()` -- Lines 4248-4273 (26 lines)
- **Status:** DEAD EXPORT (imported but not called)
- **Call sites in importUtils:** None (defined but not called internally)
- **Imported by routes (29).ts:** Line 93 -- but NEVER CALLED in routes body
- **Canonical version:** `autoDetectPivotFormat` in aiImportRoutes now handles OTS detection
- **Verdict:** DEAD EXPORT. No actual caller anywhere. Remove.

### 2.6 `parseOTSFormat()` -- Lines 4282-4446 (165 lines)
- **Status:** DEAD EXPORT (imported but not called)
- **Call sites in importUtils:** None (defined but not called internally)
- **Imported by routes (29).ts:** Line 92 -- but NEVER CALLED in routes body
- **Canonical version:** `aiImportRoutes (6).ts` line 1221
- **Verdict:** DEAD EXPORT. No actual caller anywhere. Remove.

### 2.7 `parseGenericPivotFormat()` -- Lines 4461-4743 (283 lines)
- **Status:** DEAD EXPORT (imported but not called)
- **Call sites in importUtils:** None
- **Imported by routes (29).ts:** Line 91 -- but NEVER CALLED in routes body
- **Canonical version:** `aiImportRoutes (6).ts` line 858
- **Verdict:** DEAD EXPORT. No actual caller anywhere. Remove.

### 2.8 `parseGRNInvoiceFormat()` -- Lines 4750-4922 (173 lines)
- **Status:** DEAD EXPORT (imported but not called)
- **Call sites in importUtils:** None
- **Imported by routes (29).ts:** Line 94 -- but NEVER CALLED in routes body
- **Canonical version:** `aiImportRoutes (6).ts` line 1137
- **Verdict:** DEAD EXPORT. No actual caller anywhere. Remove.

### 2.9 `excelSerialToDateStr()` -- Lines 4929-4934 (6 lines)
- **Status:** DEAD (helper for parsePRDateHeaderFormat)
- **Call sites:** Only used by `parsePRDateHeaderFormat` on line 5030
- **Verdict:** DEAD. Remove with parsePRDateHeaderFormat.

### 2.10 `parsePRDateHeaderFormat()` -- Lines 4936-5146 (211 lines)
- **Status:** DEAD EXPORT (imported but not called)
- **Call sites in importUtils:** None
- **Imported by routes (29).ts:** Line 95 -- but NEVER CALLED in routes body
- **Canonical version:** `aiImportRoutes (6).ts` line 989
- **Verdict:** DEAD EXPORT. No actual caller anywhere. Remove.

### 2.11 `parseStoreMultibrandFormat()` -- Lines 5153-5356 (204 lines)
- **Status:** DEAD EXPORT (imported but not called)
- **Call sites in importUtils:** None
- **Imported by routes (29).ts:** Line 96 -- but NEVER CALLED in routes body
- **Canonical version:** `aiImportRoutes (6).ts` line 1304
- **Verdict:** DEAD EXPORT. No actual caller anywhere. Remove.

### 2.12 `parseTarikEdizFormat()` -- Lines 5362-5503 (142 lines)
- **Status:** DEAD (not exported, not called)
- **Call sites in importUtils:** None. Not exported, not called by any internal function.
- **Canonical version:** `aiImportRoutes (6).ts` line 624
- **Verdict:** DEAD. Completely unreachable dead code. Remove.

### 2.13 `parseJovaniFormat()` -- Lines 5512-5743 (232 lines)
- **Status:** DEAD (not exported, not called)
- **Call sites in importUtils:** None. Not exported, not called by any internal function.
- **Canonical version:** `aiImportRoutes (6).ts` has `parseJovaniSaleFormat`
- **Verdict:** DEAD. Completely unreachable dead code. Remove.

### 2.14 `parseSherriHillFormat()` -- Lines 5751-5911 (161 lines)
- **Status:** DEAD (not exported, not called)
- **Call sites in importUtils:** None. Not exported, not called by any internal function.
- **Canonical version:** `aiImportRoutes (6).ts` line 775
- **Verdict:** DEAD. Completely unreachable dead code. Remove.

### 2.15 `parseFerianiGiaFormat()` -- Lines 5917-6077 (161 lines)
- **Status:** DEAD (not exported, not called)
- **Call sites in importUtils:** None. Not exported, not called by any internal function.
- **Canonical version:** `aiImportRoutes (6).ts` has `parseFerianiFormat`
- **Verdict:** DEAD. Completely unreachable dead code. Remove.

### importUtils (10).ts Summary

| Function | Lines | Status | Removable Now? |
|---|---|---|---|
| `parseJovaniSaleFile` | 1324-1442 (119) | USED by parseExcelToInventory | No |
| `parsePivotedExcelToInventory` | 1455-1879 (425) | LEGACY fallback | No -- still called |
| `calculateItemStockInfo` | 4071-4160 (90) | MOVING | No -- 2 call sites |
| `getStockInfoRuleForEmail` | 4162-4237 (76) | MOVING | No -- 2 call sites |
| `isOTSFormat` | 4248-4273 (26) | DEAD EXPORT | Yes |
| `parseOTSFormat` | 4282-4446 (165) | DEAD EXPORT | Yes |
| `parseGenericPivotFormat` | 4461-4743 (283) | DEAD EXPORT | Yes |
| `parseGRNInvoiceFormat` | 4750-4922 (173) | DEAD EXPORT | Yes |
| `excelSerialToDateStr` | 4929-4934 (6) | DEAD helper | Yes |
| `parsePRDateHeaderFormat` | 4936-5146 (211) | DEAD EXPORT | Yes |
| `parseStoreMultibrandFormat` | 5153-5356 (204) | DEAD EXPORT | Yes |
| `parseTarikEdizFormat` | 5362-5503 (142) | DEAD (not exported) | Yes |
| `parseJovaniFormat` | 5512-5743 (232) | DEAD (not exported) | Yes |
| `parseSherriHillFormat` | 5751-5911 (161) | DEAD (not exported) | Yes |
| `parseFerianiGiaFormat` | 5917-6077 (161) | DEAD (not exported) | Yes |

**Total dead code removable now:** ~1,764 lines (isOTSFormat 26 + parseOTSFormat 165 + parseGenericPivotFormat 283 + parseGRNInvoiceFormat 173 + excelSerialToDateStr 6 + parsePRDateHeaderFormat 211 + parseStoreMultibrandFormat 204 + parseTarikEdizFormat 142 + parseJovaniFormat 232 + parseSherriHillFormat 161 + parseFerianiGiaFormat 161)

**Total MOVING (remove after importEngine migration):** ~166 lines (calculateItemStockInfo 90 + getStockInfoRuleForEmail 76)

---

## 3. aiImportRoutes (6).ts -- Code Moving to importEngine

### 3.1 Type Definitions -- Lines 58-124 (67 lines)
- **Types:** `PivotItem`, `DiscontinuedConfig`, `FutureDateConfig`, `StockConfig`, `BrandDetectionConfig`, `UniversalParserConfig`
- **Status:** MOVING
- **Exported:** `UniversalParserConfig` (used by routes (29).ts import on line 56)
- **Verdict:** Move to importEngine types file. Update all imports.

### 3.2 Utility Functions -- Lines 130-213 (84 lines)
- **Functions:** `excelSerialToDate`, `parseStockValue`, `resolveColumnIndex`
- **Status:** MOVING
- **Call sites:** Used internally by all parsers in this file
- **Verdict:** Move to importEngine helpers. These are the canonical versions.

### 3.3 `autoDetectPivotFormat()` -- Lines 219-340 (122 lines)
- **Status:** MOVING (actively exported and used)
- **Exported:** Yes
- **Used by:**
  - routes (29).ts: Lines 407, 4219, 5180 (URL import, manual upload, URL fetch)
  - importUtils (10).ts: Line 2097 (email import, via lazy import)
  - Internal: Line 364 (inside parseIntelligentPivotFormat)
- **Verdict:** MOVING. Canonical format detector used by ALL import paths.

### 3.4 `parseIntelligentPivotFormat()` -- Lines 346-466 (121 lines)
- **Status:** MOVING (actively exported and used)
- **Exported:** Yes
- **Used by:**
  - routes (29).ts: Lines 421, 4257, 5194 (URL import, manual upload, URL fetch)
  - importUtils (10).ts: Line 2140 (email import, via lazy import)
  - Internal: Lines 1812, 2027, 4102
- **Verdict:** MOVING. The main parser dispatch function used by ALL import paths.

### 3.5 Vendor Parsers -- Lines 468-1503 (1,036 lines)
All these are internal (not exported), called only from `parseIntelligentPivotFormat`:

| Parser | Lines | Called from |
|---|---|---|
| `parseFerianiFormat` | 468-541 | switch case "feriani" (line 375) |
| `parseJovaniSaleFormat` | 543-623 | switch case "jovani_sale" (line 380) |
| `parseTarikEdizFormat` | 624-774 | switch case "tarik_ediz" (line 383) |
| `parseSherriHillFormat` | 775-857 | switch case "sherri_hill" (line 387) |
| `parseGenericPivotFormat` | 858-988 | switch case "generic_pivot" (line 390) |
| `parsePRDateHeaderFormat` | 989-1136 | switch case "pr_date_headers" (line 393) |
| `parseGRNInvoiceFormat` | 1137-1220 | switch case "grn_invoice" (line 396) |
| `parseOTSFormat` | 1221-1303 | switch case "ots_format" (line 402) |
| `parseStoreMultibrandFormat` | 1304-1404 | switch case "store_multibrand" (line 399) |
| `parseRowFormat` | 1411-1503 | default case (line 408) |

- **Status:** All MOVING to importEngine
- **Verdict:** These are the CANONICAL versions. All other copies (in routes and importUtils) are dead duplicates.

### aiImportRoutes (6).ts Summary

| Component | Lines | Status |
|---|---|---|
| Type definitions | 58-124 (67) | MOVING |
| Utility functions | 130-213 (84) | MOVING |
| `autoDetectPivotFormat` | 219-340 (122) | MOVING |
| `parseIntelligentPivotFormat` | 346-466 (121) | MOVING |
| Vendor parsers (10) | 468-1503 (1,036) | MOVING |

**Total code moving to importEngine:** ~1,430 lines

---

## 4. Cross-File Import Dependencies

### Exports from importUtils consumed by other files:

| Export | Used by | Status |
|---|---|---|
| `parsePivotedExcelToInventory` | routes (29).ts (3 fallback sites) | LEGACY -- keep until importEngine |
| `parseGenericPivotFormat` | routes (29).ts (import only, never called) | DEAD IMPORT |
| `parseOTSFormat` | routes (29).ts (import only, never called) | DEAD IMPORT |
| `isOTSFormat` | routes (29).ts (import only, never called) | DEAD IMPORT |
| `parseGRNInvoiceFormat` | routes (29).ts (import only, never called) | DEAD IMPORT |
| `parsePRDateHeaderFormat` | routes (29).ts (import only, never called) | DEAD IMPORT |
| `parseStoreMultibrandFormat` | routes (29).ts (import only, never called) | DEAD IMPORT |
| `parseExcelToInventory` | Internal only (processEmailAttachment) | LEGACY |
| `applyCleaningToValue` | aiImportRoutes (6).ts (line 48) | ACTIVE -- keep |
| `registerSaleFileStyles` | routes (29).ts | ACTIVE -- keep |
| `filterDiscontinuedStyles` | routes (29).ts, aiImportRoutes | ACTIVE -- keep |
| `removeDiscontinuedInventoryItems` | routes (29).ts, aiImportRoutes | ACTIVE -- keep |
| `checkSaleImportFirstRequirement` | routes (29).ts | ACTIVE -- keep |

### Exports from aiImportRoutes consumed by other files:

| Export | Used by | Status |
|---|---|---|
| `autoDetectPivotFormat` | routes (29).ts (3 sites) | ACTIVE -- move to importEngine |
| `parseIntelligentPivotFormat` | routes (29).ts (3 sites) | ACTIVE -- move to importEngine |
| `UniversalParserConfig` | routes (29).ts (type import) | ACTIVE -- move to importEngine |
| `executeAIImport` | emailFetcher (3).ts (line 25) | ACTIVE -- keep or move |
| default router | routes (29).ts (line 53) | ACTIVE -- keep |

---

## 5. Flagged Items -- Uncertain Status

### 5.1 `parseExcelToInventory` in routes (29).ts vs importUtils (10).ts
Both files have their own version. The routes version (line 1817) calls the routes-local dead parsers. The importUtils version (line 931) is the one used by `processEmailAttachment` as a fallback. These are DIFFERENT implementations (routes version has `parseFerianiGiaFormat` call, importUtils version has `parseJovaniSaleFile` call). Neither should be confused with the other.

**Recommendation:** When importEngine is complete, both can be removed. The importUtils version should be the last to go since it's the active fallback for email imports.

### 5.2 `parsePivotedExcelToInventory` in importUtils
Still called as a legacy fallback in 4 places (3 in routes, 1 in importUtils). It handles the old `pivotConfig.enabled` format. When all data sources have been migrated to use the shared format detector, this can be removed.

### 5.3 Unused vendor parser imports in routes (29).ts (lines 91-96)
These 6 imports (`parseGenericPivotFormat`, `parseOTSFormat`, `isOTSFormat`, `parseGRNInvoiceFormat`, `parsePRDateHeaderFormat`, `parseStoreMultibrandFormat`) are imported but NEVER called anywhere in the file body. They appear to be leftover from before the shared parser unification. Safe to remove immediately.

---

## 6. Summary

### Immediately removable dead code:

| File | Dead Lines |
|---|---|
| routes (29).ts | ~1,374 lines |
| importUtils (10).ts | ~1,764 lines |
| **Total** | **~3,138 lines** |

### Code moving to importEngine (remove after migration):

| File | Moving Lines |
|---|---|
| routes (29).ts | ~263 lines |
| importUtils (10).ts | ~166 lines |
| aiImportRoutes (6).ts | ~1,430 lines |
| **Total** | **~1,859 lines** |

### Recommended removal order:
1. **Phase 1 (safe, no risk):** Remove dead vendor parsers from importUtils (10).ts (lines 4248-6077) -- 1,764 lines. Remove unused imports from routes (29).ts (lines 91-96).
2. **Phase 2 (safe, no risk):** Remove dead vendor parsers + `parseExcelToInventory` chain from routes (29).ts (lines 145-271, 879-2160, 22224-22383) -- 1,368 lines.
3. **Phase 3 (requires import updates):** Move `applyCleaningToValue`, `checkSafetyThreshold`, `calculateItemStockInfo`, `getStockInfoRule` to importEngine and update all call sites.
4. **Phase 4 (requires full importEngine):** Move all parsers from aiImportRoutes to importEngine. Remove `parsePivotedExcelToInventory` and importUtils' `parseExcelToInventory` once all data sources use the shared format detector.
