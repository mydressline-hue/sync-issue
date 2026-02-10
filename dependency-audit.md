# Dependency Audit: importEngine-NEW.ts Integration

## 1. Current Dependency Graph

```
                        +-------------------+
                        | inventoryProcessing|
                        +-------------------+
                          ^   ^   ^   ^
                          |   |   |   |
          +---------------+   |   |   +----------------+
          |                   |   |                    |
   +------+------+   +-------+---+---+   +------------+---+
   |   routes    |   | aiImportRoutes |   |  importUtils   |
   +------+------+   +-------+-------+   +-------+--------+
          |                   |                   ^
          |                   |                   |
          +--------->---------+                   |
          |   autoDetectPivotFormat               |
          |   parseIntelligentPivotFormat          |
          |   UniversalParserConfig               |
          |                   |                   |
          +---------->--------+----->-------------+
          |   (routes imports from importUtils)   |
          |                                       |
          |   aiImportRoutes imports from importUtils:
          |     filterDiscontinuedStyles           |
          |     removeDiscontinuedInventoryItems   |
          |     applyCleaningToValue               |
          |                                       |
          |   importUtils LAZY imports from aiImportRoutes:
          |     await import("./aiImportRoutes")   |
          |     -> autoDetectPivotFormat            |
          |     -> parseIntelligentPivotFormat      |
          |                                       |
   +------+------+                                |
   | emailFetcher | ------>--- importUtils --------+
   +------+------+     (processEmailAttachment, logSystemError)
          |
          +------->--- routes (triggerAutoConsolidationAfterImport)
          +------->--- aiImportRoutes (executeAIImport)
```

### Detailed Static Imports

#### routes (29).ts imports FROM:
| Module              | Symbols                                                                                      |
|---------------------|----------------------------------------------------------------------------------------------|
| aiImportRoutes      | `default (router)`, `autoDetectPivotFormat`, `parseIntelligentPivotFormat`, `UniversalParserConfig` |
| importUtils         | `parsePivotedExcelToInventory`, `parseGenericPivotFormat`, `parseOTSFormat`, `isOTSFormat`, `parseGRNInvoiceFormat`, `parsePRDateHeaderFormat`, `parseStoreMultibrandFormat`, `registerSaleFileStyles`, `filterDiscontinuedStyles`, `removeDiscontinuedInventoryItems`, `checkSaleImportFirstRequirement` |
| inventoryProcessing | `cleanInventoryData`, `applyVariantRules`, `isColorCode`, `formatColorName`, `applyImportRules`, `applyPriceBasedExpansion`, `buildStylePriceMapFromCache`, `deduplicateAndZeroFutureStock` |
| importValidator     | `validateImportFile`, `logValidationFailure`                                                  |
| importState         | (none - not in routes)                                                                        |

#### aiImportRoutes (6).ts imports FROM:
| Module              | Symbols                                                                                      |
|---------------------|----------------------------------------------------------------------------------------------|
| storage             | `storage`                                                                                    |
| aiFormatDetection   | `detectFileFormat`                                                                            |
| enhancedImportProcessor | `parseWithEnhancedConfig`, `EnhancedImportConfig`                                        |
| importValidator     | `validateImportResults`, `ValidationConfig`, `PostImportValidationResult`, `captureSourceChecksums`, `SourceChecksums`, `DataSourceRules`, `LastImportStats` |
| inventoryProcessing | `applyImportRules`, `applyVariantRules`, `applyPriceBasedExpansion`, `buildStylePriceMapFromCache`, `formatColorName`, `isValidShipDate`, `deduplicateAndZeroFutureStock` |
| importUtils         | `filterDiscontinuedStyles`, `removeDiscontinuedInventoryItems`, `applyCleaningToValue`       |

#### importUtils (10).ts imports FROM:
| Module              | Symbols                                                                                      |
|---------------------|----------------------------------------------------------------------------------------------|
| storage             | `storage`                                                                                    |
| inventoryProcessing | `cleanInventoryData`, `applyVariantRules`, `isColorCode`, `formatColorName`, `applyImportRules`, `applyPriceBasedExpansion`, `buildStylePriceMapFromCache`, `deduplicateAndZeroFutureStock` |
| importValidator     | `validateImportFile`, `logValidationFailure`                                                  |
| sizeUtils           | `isSizeAllowed`, `SizeLimitConfig`                                                            |
| importState         | `startImport`, `completeImport`, `failImport`                                                |
| **aiImportRoutes** (LAZY) | `autoDetectPivotFormat`, `parseIntelligentPivotFormat` (via `await import("./aiImportRoutes")`) |
| **errorReporter** (LAZY) | `sendImmediateImportAlert` (via `await import("./errorReporter")`)                         |

#### inventoryProcessing (5).ts imports FROM:
| Module    | Symbols                                                       |
|-----------|---------------------------------------------------------------|
| storage   | `storage`                                                     |
| openai    | `suggestColorCorrections`                                     |
| sizeUtils | `LETTER_SIZES`, `LETTER_SIZE_MAP`, `NUMERIC_SIZES`, `NUMERIC_SIZE_MAP`, `isSizeAllowed`, `SizeLimitConfig` |

**inventoryProcessing has NO imports from routes, aiImportRoutes, or importUtils.** It is a leaf dependency.

### Existing Circular Dependency

```
aiImportRoutes ---(static)---> importUtils
importUtils ---(lazy await import)---> aiImportRoutes
```

This is the ONLY circular dependency. It is safe because importUtils uses `await import("./aiImportRoutes")` (lazy/dynamic import), which defers resolution to runtime and avoids the initialization-order deadlock.

### External Consumers

#### emailFetcher (3).ts imports FROM:
| Module         | Symbols                                      |
|----------------|----------------------------------------------|
| importUtils    | `processEmailAttachment`, `logSystemError`   |
| routes         | `triggerAutoConsolidationAfterImport`         |
| aiImportRoutes | `executeAIImport`                            |

#### importEngine-section-parsers.ts imports FROM:
| Module              | Symbols           |
|---------------------|-------------------|
| inventoryProcessing | `isValidShipDate` |


---

## 2. New Dependency Graph with importEngine

```
                        +-------------------+
                        | inventoryProcessing|
                        +-------------------+
                          ^       ^       ^
                          |       |       |
          +---------------+       |       +------+
          |                       |              |
   +------+------+       +-------+-------+      |
   |   routes    |       | importEngine  |------+
   |   (NEW)     |       |   (NEW)       |
   +------+------+       +--+----+---+---+
          |                  |    |   |
          |                  |    |   +---> importValidator
          |                  |    +------> storage
          |                  |
          |                  +----> importUtils (static: applyCleaningToValue,
          |                  |       filterDiscontinuedStyles, removeDiscontinuedInventoryItems)
          |                  |
          +---->-------------+  (routes imports from importEngine:
          |  autoDetectPivotFormat, parseIntelligentPivotFormat,
          |  UniversalParserConfig, executeAIImport, etc.)
          |
   +------+------+       +-------+-------+
   | aiImportRoutes|      |  importUtils   |
   |   (NEW)     |       |   (NEW)        |
   +------+------+       +-------+--------+
          |                       |
          +---> importEngine      +---> importEngine (LAZY)
          (re-exports parsers)     (await import("./importEngine"))
```

### New Static Import Map

#### importEngine-NEW imports FROM:
| Module              | Symbols                                                                |
|---------------------|------------------------------------------------------------------------|
| inventoryProcessing | `deduplicateAndZeroFutureStock`, `applyImportRules`, `applyVariantRules`, `applyPriceBasedExpansion`, `buildStylePriceMapFromCache`, `formatColorName`, `isValidShipDate`, `cleanInventoryData` |
| importUtils         | `applyCleaningToValue`, `filterDiscontinuedStyles`, `removeDiscontinuedInventoryItems` |
| storage             | `storage`                                                              |
| importValidator     | `validateImportResults`, `captureSourceChecksums`, etc.                |

#### aiImportRoutes-NEW imports FROM:
| Module       | Symbols (re-exports)                                                    |
|--------------|-------------------------------------------------------------------------|
| importEngine | `autoDetectPivotFormat`, `parseIntelligentPivotFormat`, `UniversalParserConfig`, `executeAIImport` |

#### routes-NEW imports FROM:
| Module       | Symbols                                                                 |
|--------------|-------------------------------------------------------------------------|
| importEngine | `autoDetectPivotFormat`, `parseIntelligentPivotFormat`, `UniversalParserConfig` |
| importUtils  | (same as before: legacy parsers, `registerSaleFileStyles`, etc.)        |

#### importUtils-NEW imports FROM:
| Module       | Symbols (LAZY)                                                          |
|--------------|-------------------------------------------------------------------------|
| importEngine | `autoDetectPivotFormat`, `parseIntelligentPivotFormat` (via `await import("./importEngine")`) |

---

## 3. Circular Dependency Risks and Mitigations

### Risk 1: importEngine <-> importUtils (MEDIUM - MITIGATED)

```
importEngine ---(static)---> importUtils  (applyCleaningToValue, filterDiscontinuedStyles)
importUtils  ---(lazy)-----> importEngine (autoDetectPivotFormat, parseIntelligentPivotFormat)
```

**Status: SAFE** - This mirrors the current `aiImportRoutes <-> importUtils` cycle exactly. The lazy `await import("./importEngine")` in importUtils ensures the cycle is broken at module initialization time. The importEngine module will be fully loaded by the time `processEmailAttachment()` calls its lazy import at runtime.

**Mitigation already in place**: importUtils uses `await import()` pattern (line 2079 in current code). The NEW version must preserve this pattern, changing only the target from `"./aiImportRoutes"` to `"./importEngine"`.

### Risk 2: emailFetcher -> routes -> ... (NO CHANGE)

```
emailFetcher ---(static)---> routes (triggerAutoConsolidationAfterImport)
```

This is a one-way dependency. No circular risk. This import stays the same in the new graph because `triggerAutoConsolidationAfterImport` remains in routes.

### Risk 3: emailFetcher -> aiImportRoutes -> importEngine (LOW)

```
emailFetcher ---(static)---> aiImportRoutes (executeAIImport)
aiImportRoutes ---(static)---> importEngine (re-exports executeAIImport)
```

**Status: SAFE** - This is a simple chain with no cycle. aiImportRoutes-NEW becomes a thin re-export layer.

### Risk 4: routes -> importEngine -> importUtils -> importEngine (POTENTIAL)

```
routes ---(static)---> importEngine
importEngine ---(static)---> importUtils
importUtils ---(lazy)---> importEngine
```

**Status: SAFE** - The lazy import in importUtils breaks the cycle. At module load time: routes loads importEngine, importEngine loads importUtils (static), importUtils does NOT load importEngine statically. The lazy import only executes at runtime inside `processEmailAttachment()`, by which time all modules are fully initialized.

### Risk 5: If importEngine imports from aiImportRoutes (AVOID!)

If importEngine were to import anything from aiImportRoutes, and aiImportRoutes re-exports from importEngine, this would create a static circular dependency:

```
importEngine ---(static)---> aiImportRoutes ---(static)---> importEngine  [DEADLOCK!]
```

**Mitigation**: importEngine must NEVER import from aiImportRoutes. All shared logic must flow: `inventoryProcessing -> importEngine -> aiImportRoutes` (one direction only).

---

## 4. External Imports That Might Break

### 4a. Imports from `aiImportRoutes` by external files

| File              | Symbols Imported                                           | Will Break? | Fix Required                          |
|-------------------|------------------------------------------------------------|-------------|---------------------------------------|
| routes (29).ts    | `default`, `autoDetectPivotFormat`, `parseIntelligentPivotFormat`, `UniversalParserConfig` | YES if removed | aiImportRoutes-NEW must re-export these from importEngine |
| emailFetcher (3).ts | `executeAIImport`                                       | YES if removed | aiImportRoutes-NEW must re-export `executeAIImport` from importEngine |

### 4b. Imports from `importUtils` by external files

| File              | Symbols Imported                                           | Will Break? | Fix Required                          |
|-------------------|------------------------------------------------------------|-------------|---------------------------------------|
| routes (29).ts    | `parsePivotedExcelToInventory`, `parseGenericPivotFormat`, `parseOTSFormat`, `isOTSFormat`, `parseGRNInvoiceFormat`, `parsePRDateHeaderFormat`, `parseStoreMultibrandFormat`, `registerSaleFileStyles`, `filterDiscontinuedStyles`, `removeDiscontinuedInventoryItems`, `checkSaleImportFirstRequirement` | NO | These stay in importUtils (not moved to importEngine) |
| aiImportRoutes (6).ts | `filterDiscontinuedStyles`, `removeDiscontinuedInventoryItems`, `applyCleaningToValue` | NO | These stay in importUtils |
| emailFetcher (3).ts | `processEmailAttachment`, `logSystemError`              | NO | These stay in importUtils |

### 4c. Imports from `routes` by external files

| File              | Symbols Imported                                           | Will Break? | Fix Required                          |
|-------------------|------------------------------------------------------------|-------------|---------------------------------------|
| emailFetcher (3).ts | `triggerAutoConsolidationAfterImport`                    | NO | This stays in routes |

### 4d. Imports from `inventoryProcessing` by external files

| File                           | Symbols Imported      | Will Break? | Fix Required |
|--------------------------------|-----------------------|-------------|--------------|
| importEngine-section-parsers.ts | `isValidShipDate`    | NO | inventoryProcessing unchanged |
| routes (29).ts                 | (8 symbols)           | NO | inventoryProcessing unchanged |
| aiImportRoutes (6).ts          | (7 symbols)           | NO | inventoryProcessing unchanged |
| importUtils (10).ts            | (8 symbols)           | NO | inventoryProcessing unchanged |

---

## 5. Recommended Re-exports for Backward Compatibility

### aiImportRoutes-NEW.ts MUST re-export:

```typescript
// Re-export from importEngine for backward compatibility
// (routes.ts and emailFetcher.ts import these from "./aiImportRoutes")
export {
  autoDetectPivotFormat,
  parseIntelligentPivotFormat,
  UniversalParserConfig,
  executeAIImport,
} from "./importEngine";
```

These 4 symbols are imported by external files (`routes`, `emailFetcher`) from `aiImportRoutes`. If aiImportRoutes-NEW stops exporting them, those files will break at compile time.

### importUtils-NEW.ts MUST change lazy import target:

```typescript
// OLD (current):
const { autoDetectPivotFormat, parseIntelligentPivotFormat } = await import(
  "./aiImportRoutes"
);

// NEW (updated):
const { autoDetectPivotFormat, parseIntelligentPivotFormat } = await import(
  "./importEngine"
);
```

### routes-NEW.ts CAN optionally switch imports:

```typescript
// OLD:
import { autoDetectPivotFormat, parseIntelligentPivotFormat, UniversalParserConfig } from "./aiImportRoutes";

// NEW (preferred - direct import, no indirection):
import { autoDetectPivotFormat, parseIntelligentPivotFormat, UniversalParserConfig } from "./importEngine";
```

This is optional if aiImportRoutes re-exports them, but preferred for clarity.

### No changes needed for:
- `importUtils` exports (all stay in importUtils)
- `inventoryProcessing` exports (leaf module, unchanged)
- `routes` exports (`triggerAutoConsolidationAfterImport` stays in routes)

---

## Summary

| Check                              | Status | Notes                                                    |
|------------------------------------|--------|----------------------------------------------------------|
| importEngine -> importUtils cycle  | SAFE   | importUtils uses lazy `await import()` to break cycle    |
| importEngine -> aiImportRoutes     | MUST AVOID | Would create static circular; importEngine must not import from aiImportRoutes |
| aiImportRoutes re-exports          | REQUIRED | 4 symbols: `autoDetectPivotFormat`, `parseIntelligentPivotFormat`, `UniversalParserConfig`, `executeAIImport` |
| importUtils lazy import target     | MUST UPDATE | Change from `"./aiImportRoutes"` to `"./importEngine"` |
| inventoryProcessing                | NO CHANGE | Leaf dependency, no circular risk                       |
| emailFetcher compatibility         | OK     | As long as aiImportRoutes re-exports `executeAIImport`   |
| routes compatibility               | OK     | Can import from either aiImportRoutes (re-export) or importEngine (direct) |
