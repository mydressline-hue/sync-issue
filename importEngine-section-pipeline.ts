// ============ SECTION 3: THE UNIFIED PIPELINE ============
//
// executeImport() is THE single entry point for all 7 import paths.
// It runs the full 20-step pipeline from parsing through post-import.
//
// Callers:
//   1. /api/ai-import/execute           → source='ai_import'    (manual multi-file upload)
//   2. executeAIImport() (email multi)  → source='ai_import'    (email multi-file via shared fn)
//   3. processEmailAttachment()         → source='email'        (email single-file)
//   4. /api/data-sources/:id/upload     → source='manual_upload' (manual single-file upload)
//   5. processUrlDataSourceImport()     → source='url'          (URL scheduled import)
//   6. /api/data-sources/:id/fetch-url  → source='url'          (URL manual fetch)
//   7. performCombineImport()           → source='combine'      (combine staged files)
//
// Known bug FIXED here: processUrlDataSourceImport was missing deduplicateAndZeroFutureStock().
// Now ALL paths go through Step 7 (dedup) uniformly.

import * as XLSX from "xlsx";
import {
  cleanInventoryData,
  applyImportRules,
  applyVariantRules,
  applyPriceBasedExpansion,
  deduplicateAndZeroFutureStock,
  buildStylePriceMapFromCache,
  formatColorName,
  isValidShipDate,
} from "./inventoryProcessing";
import {
  filterDiscontinuedStyles,
  removeDiscontinuedInventoryItems,
  applyCleaningToValue,
  registerSaleFileStyles,
} from "./importUtils";
import { storage } from "./storage";
import { validateImportFile, logValidationFailure } from "./importValidator";
import { triggerAutoConsolidationAfterImport } from "./routes";

// ---- Functions defined in THIS file (importEngine), other sections ----
// Section 1 (parsers):
//   autoDetectPivotFormat(rawData, dsName, filename) → string | null
//   parseIntelligentPivotFormat(buffer, format, config, dsName, filename) → { items, rows, headers }
//   parseWithEnhancedConfig(buffer, config, dataSourceId) → { success, items, stats, warnings }
//   UniversalParserConfig (type)
//   EnhancedImportConfig (type)
//
// Section 4 (helpers):
//   calculateItemStockInfo(item, stockInfoRule) → string | null
//   getStockInfoRule(dataSource, storage) → stockInfoRule | null
//   getStylePrefix(item, dataSource) → string

export async function executeImport(options: {
  // Input
  fileBuffers: { buffer: Buffer; originalname: string }[];
  dataSourceId: string;
  overrideConfig?: any;

  // Context flags
  source: "manual_upload" | "email" | "url" | "combine" | "ai_import";
  preConsolidatedItems?: any[]; // For combine path (already parsed/staged with prefix + SKU)

  // Callbacks for path-specific behavior
  onFileRecord?: (file: any) => void;
}): Promise<{
  success: boolean;
  itemCount: number;
  error?: string;
  safetyBlock?: boolean;
  fileId?: string;
  stats?: any;
  validation?: any;
}> {
  const {
    fileBuffers,
    dataSourceId,
    overrideConfig,
    source,
    preConsolidatedItems,
    onFileRecord,
  } = options;

  const LOG = `[ImportEngine:${source}]`;

  try {
    // ================================================================
    // LOAD DATA SOURCE
    // ================================================================
    const dataSource = await storage.getDataSource(dataSourceId);
    if (!dataSource) {
      return { success: false, itemCount: 0, error: "Data source not found" };
    }

    console.log(
      `${LOG} Starting import for "${dataSource.name}" (id=${dataSourceId})`,
    );

    const isSaleFile = (dataSource as any).sourceType === "sales";

    // Merge overrideConfig with DB config (override takes priority)
    const cleaningConfig =
      overrideConfig?.cleaningConfig ||
      ((dataSource.cleaningConfig || {}) as any);

    // CRITICAL FIX: stockValueConfig column doesn't exist in schema!
    // Fall back to cleaningConfig.stockTextMappings which is where the UI saves the data
    const stockValueConfig =
      overrideConfig?.stockValueConfig ||
      (dataSource as any).stockValueConfig ||
      (dataSource.cleaningConfig?.stockTextMappings?.length > 0
        ? { textMappings: dataSource.cleaningConfig.stockTextMappings }
        : undefined);

    // Pipeline stats — accumulated throughout all 20 steps
    const stats: Record<string, any> = {
      source,
      dataSourceName: dataSource.name,
    };

    // ================================================================
    // ===================== PHASE 1: PARSE ============================
    // Steps 1-4: Read files, detect format, parse, clean style
    // ================================================================

    let items: any[];
    let rawData: any[][] = []; // Full raw data (header + rows) for applyImportRules context
    let rows: any[][] = [];
    let headers: string[] = [];
    let parseResult: any = null;

    if (preConsolidatedItems && preConsolidatedItems.length > 0) {
      // ============================================================
      // COMBINE PATH: Items already parsed and staged
      // The combine caller (performCombineImport) extracts items from
      // staged files with prefix, SKU, color title-cased, stock parsed.
      // We skip the entire parse phase and go straight to filtering.
      // ============================================================
      console.log(
        `${LOG} Using ${preConsolidatedItems.length} pre-consolidated items (combine path — skipping Phase 1)`,
      );
      items = [...preConsolidatedItems];
      stats.parseSkipped = true;
      stats.preConsolidatedCount = preConsolidatedItems.length;
    } else {
      // ============================================================
      // STANDARD PATH: Parse from file buffers
      // ============================================================
      if (!fileBuffers || fileBuffers.length === 0) {
        return { success: false, itemCount: 0, error: "No files provided" };
      }

      const primaryFile = fileBuffers[0];

      // --------------------------------------------------
      // Step 1: Read & consolidate files
      // Multi-file: merge all sheets into one rawData array (first file's header, all files' rows)
      // Single-file: read as-is
      // --------------------------------------------------
      if (fileBuffers.length > 1) {
        console.log(
          `${LOG} Step 1: Consolidating ${fileBuffers.length} files`,
        );
        let headerRow: any[] | null = null;

        for (const file of fileBuffers) {
          const wb = XLSX.read(file.buffer, { type: "buffer" });
          const sheet = wb.Sheets[wb.SheetNames[0]];
          const data = XLSX.utils.sheet_to_json(sheet, {
            header: 1,
            defval: "",
          }) as any[][];

          if (headerRow === null && data.length > 0) {
            headerRow = data[0];
            rawData = data;
          } else if (data.length > 1) {
            // Skip header row of subsequent files, append data rows
            rawData.push(...data.slice(1));
          }
        }
        console.log(
          `${LOG} Step 1: Consolidated ${rawData.length} total rows from ${fileBuffers.length} files`,
        );
      } else {
        const workbook = XLSX.read(primaryFile.buffer, { type: "buffer" });
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        rawData = XLSX.utils.sheet_to_json(sheet, {
          header: 1,
          defval: "",
        }) as any[][];
      }

      // For multi-file: create a consolidated buffer so the pivot parser sees ALL data
      let consolidatedBuffer: Buffer;
      if (fileBuffers.length > 1) {
        const newWorkbook = XLSX.utils.book_new();
        const newSheet = XLSX.utils.aoa_to_sheet(rawData);
        XLSX.utils.book_append_sheet(newWorkbook, newSheet, "Consolidated");
        consolidatedBuffer = Buffer.from(
          XLSX.write(newWorkbook, { type: "buffer", bookType: "xlsx" }),
        );
        console.log(
          `${LOG} Step 1: Created consolidated buffer (${consolidatedBuffer.length} bytes) from ${rawData.length} rows`,
        );
      } else {
        consolidatedBuffer = primaryFile.buffer;
      }

      // --------------------------------------------------
      // Step 2: autoDetectPivotFormat — detect vendor format
      // Examines rawData headers + data source name to identify the format
      // (e.g., "tarik_ediz", "sherri_hill", "pivot_interleaved", etc.)
      // --------------------------------------------------
      const detectedPivotFormat = autoDetectPivotFormat(
        rawData,
        dataSource.name,
        primaryFile.originalname,
      );

      const configFormatType =
        overrideConfig?.formatType || (dataSource as any).formatType;

      const isPivotFormat =
        configFormatType?.startsWith("pivot") ||
        configFormatType === "pivoted" ||
        detectedPivotFormat !== null;

      console.log(
        `${LOG} Step 2: Format detection — detected="${detectedPivotFormat}", dbFormat="${configFormatType}", isPivot=${isPivotFormat}`,
      );

      // Save detected format for future imports (URL/email/upload paths do this)
      if (detectedPivotFormat && source !== "ai_import") {
        try {
          await storage.updateDataSource(dataSourceId, {
            formatType: detectedPivotFormat,
            pivotConfig: { enabled: true, format: detectedPivotFormat },
          });
        } catch (e) {
          // Non-critical — continue
        }
      }

      // --------------------------------------------------
      // Step 3: parseIntelligentPivotFormat — parse all vendors
      // For detected pivot/vendor formats: use the shared intelligent parser
      // For non-pivot (row-based): fall back to parseWithEnhancedConfig
      // --------------------------------------------------
      if (isPivotFormat) {
        const actualFormat =
          detectedPivotFormat || configFormatType || "pivot_interleaved";

        const universalConfig: UniversalParserConfig = {
          skipRows:
            overrideConfig?.pivotConfig?.skipRows ||
            (dataSource as any).pivotConfig?.skipRows,
          discontinuedConfig:
            overrideConfig?.discontinuedConfig ||
            (dataSource as any).discontinuedConfig,
          futureDateConfig:
            overrideConfig?.futureStockConfig ||
            (dataSource as any).futureStockConfig,
          stockConfig: stockValueConfig,
          columnMapping:
            overrideConfig?.columnMapping || dataSource.columnMapping,
        };

        console.log(
          `${LOG} Step 3: Parsing with shared pivot parser, format="${actualFormat}"`,
        );

        const pivotResult = parseIntelligentPivotFormat(
          consolidatedBuffer,
          actualFormat,
          universalConfig,
          dataSource.name,
          primaryFile.originalname,
        );

        items = pivotResult.items;
        rows = pivotResult.rows;
        headers = pivotResult.headers;

        parseResult = {
          success: true,
          items: pivotResult.items,
          stats: {
            totalRows: pivotResult.rows.length,
            totalItems: pivotResult.items.length,
            discontinuedItems: pivotResult.items.filter(
              (i: any) => i.discontinued,
            ).length,
            futureStockItems: pivotResult.items.filter(
              (i: any) => i.shipDate,
            ).length,
          },
        };
      } else {
        // Non-pivot fallback: row-based format using column mapping
        console.log(
          `${LOG} Step 3: Using row-based parser (no pivot format detected)`,
        );

        const enhancedConfig: EnhancedImportConfig = {
          formatType: configFormatType || "row",
          columnMapping:
            overrideConfig?.columnMapping || dataSource.columnMapping || {},
          pivotConfig:
            overrideConfig?.pivotConfig || (dataSource as any).pivotConfig,
          discontinuedConfig:
            overrideConfig?.discontinuedConfig ||
            (dataSource as any).discontinuedConfig ||
            (dataSource as any).discontinuedRules,
          futureStockConfig:
            overrideConfig?.futureStockConfig ||
            (dataSource as any).futureStockConfig,
          stockValueConfig,
          cleaningConfig,
        };

        parseResult = await parseWithEnhancedConfig(
          primaryFile.buffer,
          enhancedConfig,
          dataSourceId,
        );

        if (!parseResult.success) {
          return {
            success: false,
            itemCount: 0,
            error: "Failed to parse file",
            stats: { warnings: parseResult.warnings },
          };
        }

        items = parseResult.items;
        // rawData already set from XLSX read above
        rows = rawData.length > 1 ? rawData.slice(1) : [];
        headers = rawData.length > 0 ? rawData[0].map(String) : [];
      }

      if (!items || items.length === 0) {
        return {
          success: false,
          itemCount: 0,
          error: "File contains no valid data rows",
        };
      }

      stats.totalParsed = items.length;
      console.log(`${LOG} Step 3: Parsed ${items.length} items`);

      // --------------------------------------------------
      // Step 4: applyCleaningToValue() on style field
      // Data source cleaning rules: find/replace, removeLetters, removeNumbers,
      // removeSpecialChars, removeFirstN/LastN, removePatterns, trimWhitespace
      // --------------------------------------------------
      if (cleaningConfig && items.length > 0) {
        const hasAnyCleaning =
          cleaningConfig.findText ||
          cleaningConfig.findReplaceRules?.length > 0 ||
          cleaningConfig.removeLetters ||
          cleaningConfig.removeNumbers ||
          cleaningConfig.removeSpecialChars ||
          cleaningConfig.removeFirstN ||
          cleaningConfig.removeLastN ||
          cleaningConfig.removePatterns?.length > 0 ||
          cleaningConfig.trimWhitespace;

        if (hasAnyCleaning) {
          console.log(
            `${LOG} Step 4: Applying style cleaning rules to ${items.length} items`,
          );
          items = items.map((item: any) => ({
            ...item,
            style: applyCleaningToValue(
              String(item.style || ""),
              cleaningConfig,
              "style",
            ),
          }));
        }
      }
    } // end of PHASE 1 (standard path)

    // ================================================================
    // ===================== PHASE 2: FILTER ===========================
    // Steps 5-7: Skip rules, discontinued filter, dedup
    // ================================================================

    // --------------------------------------------------
    // Step 5: Skip rule filtering (shouldSkip flag from data source config)
    // Items with shouldSkip=true are filtered out, unless
    // skipUnlessContinueSelling=true AND continueSelling is enabled.
    // NOTE: shouldSkip is set by format-specific parsers. Combine items
    // (pre-consolidated) may not have this flag, but we still check.
    // --------------------------------------------------
    {
      const continueSelling = (dataSource as any).continueSelling ?? true;
      const beforeSkip = items.length;
      items = items.filter((item: any) => {
        if (item.shouldSkip) {
          if (item.skipUnlessContinueSelling && continueSelling) {
            return true; // Don't skip — continue selling is enabled
          }
          return false; // Skip this item
        }
        return true;
      });
      const skipFiltered = beforeSkip - items.length;
      if (skipFiltered > 0) {
        console.log(
          `${LOG} Step 5: Filtered out ${skipFiltered} items based on skip rules`,
        );
      }
      stats.skipRuleFiltered = skipFiltered;
    }

    // --------------------------------------------------
    // Step 6: Filter discontinued zero-stock items
    // "D" means discontinued — but if there's still stock, keep it for remaining inventory.
    // Filter out: discontinued=true AND stock=0 AND no future stock flags.
    // This removes truly dead inventory while preserving pre-orders.
    // --------------------------------------------------
    {
      const beforeFilter = items.length;
      items = items.filter((item: any) => {
        if (item.discontinued === true && item.stock === 0) {
          // Preserve items with future stock coming
          if (
            item.hasFutureStock ||
            item.preserveZeroStock ||
            item.shipDate
          ) {
            return true;
          }
          return false; // Truly dead inventory — filter out
        }
        return true;
      });
      const discontinuedZeroFiltered = beforeFilter - items.length;
      if (discontinuedZeroFiltered > 0) {
        console.log(
          `${LOG} Step 6: Filtered out ${discontinuedZeroFiltered} discontinued zero-stock items`,
        );
      }
      stats.discontinuedZeroStockFiltered = discontinuedZeroFiltered;
    }

    // --------------------------------------------------
    // Step 7: deduplicateAndZeroFutureStock
    // Step A: Zero stock for items where shipDate + offset > today
    // Step B: Dedup by style|color|size — prefer highest stock, else closest future date
    //
    // BUG FIX: processUrlDataSourceImport was previously MISSING this step.
    // Now ALL 7 paths go through this uniformly.
    // --------------------------------------------------
    const dedupOffset =
      (dataSource as any).stockInfoConfig?.dateOffsetDays ?? 0;
    const dedupResult = deduplicateAndZeroFutureStock(items, dedupOffset);
    items = dedupResult.items;
    stats.dedupDuplicatesRemoved = dedupResult.duplicatesRemoved;
    stats.dedupStockZeroed = dedupResult.stockZeroed;
    console.log(
      `${LOG} Step 7: After dedup — ${items.length} items (${dedupResult.duplicatesRemoved} dupes removed, ${dedupResult.stockZeroed} future stock zeroed, offset=${dedupOffset}d)`,
    );

    // ================================================================
    // ===================== PHASE 3: TRANSFORM ========================
    // Steps 8-13: Prefix, clean, import rules, colors, variant rules, price expansion
    // ================================================================

    // --------------------------------------------------
    // Step 8: Apply style prefix (custom rules or DS name)
    // For combine path: SKIP — items already have prefix from staged extraction
    // For sale files: strips "Sale"/"Sales" suffix from prefix to match regular file naming
    // If item has a brand (store_multibrand vendor column): use brand as prefix
    // --------------------------------------------------
    if (source !== "combine") {
      console.log(
        `${LOG} Step 8: Applying style prefix to ${items.length} items`,
      );
      items = items.map((item: any) => {
        const rawStyle = String(item.style || "").trim();
        const prefix = item.brand
          ? String(item.brand).trim()
          : rawStyle
            ? getStylePrefix({ style: rawStyle }, dataSource)
            : dataSource.name;
        const prefixedStyle = rawStyle ? `${prefix} ${rawStyle}` : rawStyle;

        // Normalize color to Title Case for SKU: "PURPLE" → "Purple"
        const normalizedColor = item.color
          ? String(item.color)
              .toLowerCase()
              .replace(/(?:^|[\s\-\/&])\S/g, (a: string) => a.toUpperCase())
          : item.color;

        // Rebuild SKU with prefixed style
        // Note: Use explicit null check for size to handle size "0" correctly
        const prefixedSku =
          prefixedStyle &&
          normalizedColor &&
          item.size != null &&
          item.size !== ""
            ? `${prefixedStyle}-${normalizedColor}-${item.size}`
                .replace(/\//g, "-")
                .replace(/\s+/g, "-")
                .replace(/-+/g, "-")
            : (item.sku || "").replace(/\//g, "-").replace(/-+/g, "-");

        return {
          ...item,
          style: prefixedStyle,
          sku: prefixedSku,
        };
      });
      console.log(`${LOG} Step 8: Applied prefix to ${items.length} items`);
    } else {
      console.log(
        `${LOG} Step 8: SKIPPED (combine path — items already prefixed)`,
      );
    }

    // --------------------------------------------------
    // Step 9: cleanInventoryData (AI color fixes)
    // Handles:
    //   - Remove items without size
    //   - Global color mappings (badColor → goodColor)
    //   - AI-powered color suggestions for unmapped color codes
    //   - D0 → 00 size normalization
    //   - Rebuild SKUs with corrected colors
    //   - Dedup by style|color|size (keep highest stock)
    // --------------------------------------------------
    console.log(
      `${LOG} Step 9: Running cleanInventoryData on ${items.length} items`,
    );

    // Ensure items have dataSourceId for downstream context
    const itemsForClean = items.map((item: any) => ({
      ...item,
      dataSourceId,
    }));

    const cleanResult = await cleanInventoryData(
      itemsForClean,
      dataSource.name,
    );
    items = cleanResult.items;
    stats.noSizeRemoved = cleanResult.noSizeRemoved;
    stats.colorsFixed = cleanResult.colorsFixed;
    stats.aiColorsFixed = cleanResult.aiColorsFixed;
    stats.cleanDuplicatesRemoved = cleanResult.duplicatesRemoved;
    console.log(
      `${LOG} Step 9: After clean — ${items.length} items (${cleanResult.noSizeRemoved} no-size removed, ${cleanResult.colorsFixed} colors fixed, ${cleanResult.aiColorsFixed} AI colors, ${cleanResult.duplicatesRemoved} dupes)`,
    );

    // --------------------------------------------------
    // Step 10: applyImportRules (pricing, dates, discontinued, etc.)
    // Handles: discontinued detection, sale pricing, date parsing,
    // stock text mappings, value replacements, price floor/ceiling,
    // required fields filtering, regular price config, etc.
    //
    // BUG FIX: Check overrideConfig FIRST for configs (from UI), then fall back to DB
    // --------------------------------------------------
    const importRulesConfig = {
      discontinuedRules:
        overrideConfig?.discontinuedConfig ||
        overrideConfig?.discontinuedRules ||
        (dataSource as any).discontinuedConfig ||
        (dataSource as any).discontinuedRules,
      salePriceConfig:
        overrideConfig?.salePriceConfig ||
        overrideConfig?.columnSaleConfig || // Frontend sends as columnSaleConfig
        (dataSource as any).salePriceConfig,
      priceFloorCeiling: (dataSource as any).priceFloorCeiling,
      minStockThreshold: (dataSource as any).minStockThreshold,
      stockThresholdEnabled: (dataSource as any).stockThresholdEnabled,
      requiredFieldsConfig: (dataSource as any).requiredFieldsConfig,
      dateFormatConfig: (dataSource as any).dateFormatConfig,
      valueReplacementRules: (dataSource as any).valueReplacementRules,
      regularPriceConfig:
        overrideConfig?.regularPriceConfig ||
        (dataSource as any).regularPriceConfig,
      cleaningConfig:
        overrideConfig?.cleaningConfig || (dataSource as any).cleaningConfig,
      futureStockConfig:
        overrideConfig?.futureStockConfig ||
        (dataSource as any).futureStockConfig,
      stockValueConfig,
      complexStockConfig:
        overrideConfig?.complexStockConfig ||
        (dataSource as any).complexStockConfig,
    };

    console.log(`${LOG} Step 10: Applying import rules...`);
    const importRulesResult = await applyImportRules(
      items,
      importRulesConfig,
      rawData, // Raw data rows for context (empty [] for combine path)
    );
    items = importRulesResult.items;
    stats.importRulesStats = importRulesResult.stats || {};
    stats.afterImportRules = items.length;
    console.log(
      `${LOG} Step 10: After import rules — ${items.length} items (${importRulesResult.stats?.discontinuedFiltered || 0} discontinued filtered, ${importRulesResult.stats?.datesParsed || 0} dates parsed)`,
    );

    // --------------------------------------------------
    // Step 11: Global color mappings
    // Second pass to ensure all colors are mapped and formatted consistently.
    // cleanInventoryData (Step 9) handles this, but import rules (Step 10) may
    // have modified items. This ensures colors remain correct after all transforms.
    // Also rebuilds SKUs with any corrected colors.
    // --------------------------------------------------
    let globalColorsFixed = 0;
    try {
      const colorMappings = await storage.getColorMappings();
      const colorMap = new Map<string, string>();
      for (const mapping of colorMappings) {
        const normalizedBad = mapping.badColor.trim().toLowerCase();
        colorMap.set(normalizedBad, mapping.goodColor);
      }

      if (colorMap.size > 0) {
        items = items.map((item: any) => {
          const color = String(item.color || "").trim();
          const normalizedColor = color.toLowerCase();
          const mappedColor = colorMap.get(normalizedColor);

          if (mappedColor && mappedColor.toLowerCase() !== normalizedColor) {
            globalColorsFixed++;
            const newColor = formatColorName(mappedColor);
            // Rebuild SKU with corrected color (same logic as cleanInventoryData)
            const newSku =
              item.style && item.size
                ? `${item.style}-${newColor}-${item.size}`
                    .replace(/\//g, "-")
                    .replace(/\s+/g, "-")
                    .replace(/-+/g, "-")
                : item.sku;
            return { ...item, color: newColor, sku: newSku };
          }
          // No mapping needed — still format consistently
          return { ...item, color: formatColorName(color) };
        });

        if (globalColorsFixed > 0) {
          console.log(
            `${LOG} Step 11: Fixed ${globalColorsFixed} colors using global mappings (second pass)`,
          );
        }
      }
    } catch (colorMapError: any) {
      console.error(
        `${LOG} Step 11: Error applying color mappings:`,
        colorMapError,
      );
      // Continue without color mapping — non-fatal
    }
    stats.globalColorsFixed = globalColorsFixed;

    // --------------------------------------------------
    // Step 12: applyVariantRules (size expansion/filter)
    // Handles: size limits (per-style overrides), zero stock filtering,
    // size expansion from variant rules, isExpandedSize flag
    //
    // BUG FIX: Pass filterZeroStock from overrideConfig if present
    // --------------------------------------------------
    const variantRulesConfigOverride =
      overrideConfig?.filterZeroStock !== undefined
        ? {
            filterZeroStock: overrideConfig.filterZeroStock,
            filterZeroStockWithFutureDates:
              overrideConfig?.filterZeroStockWithFutureDates,
          }
        : undefined;

    console.log(`${LOG} Step 12: Applying variant rules...`);
    const variantRulesResult = await applyVariantRules(
      items,
      dataSourceId,
      variantRulesConfigOverride,
    );
    items = variantRulesResult.items;
    stats.variantRulesAdded = variantRulesResult.addedCount || 0;
    stats.variantRulesFiltered = variantRulesResult.filteredCount || 0;
    stats.variantRulesSizeFiltered = variantRulesResult.sizeFiltered || 0;
    stats.afterVariantRules = items.length;
    console.log(
      `${LOG} Step 12: After variant rules — ${items.length} items (+${variantRulesResult.addedCount || 0} expanded, -${variantRulesResult.filteredCount || 0} zero-stock filtered, -${variantRulesResult.sizeFiltered || 0} size-limited)`,
    );

    // --------------------------------------------------
    // Step 13: applyPriceBasedExpansion
    // Expands sizes based on price tiers from Shopify cached variants.
    // e.g., styles priced $0-$500 get sizes 0-30, $500-$1000 get 0-24
    // --------------------------------------------------
    let priceBasedExpansionCount = 0;
    const priceBasedExpansionConfig =
      overrideConfig?.priceExpansionConfig ||
      overrideConfig?.priceBasedExpansionConfig ||
      (dataSource as any).priceBasedExpansionConfig;
    const sizeLimitConfig =
      overrideConfig?.sizeLimitConfig || (dataSource as any).sizeLimitConfig;

    if (
      priceBasedExpansionConfig?.enabled &&
      (priceBasedExpansionConfig.tiers?.length > 0 ||
        (priceBasedExpansionConfig.defaultExpandDown ?? 0) > 0 ||
        (priceBasedExpansionConfig.defaultExpandUp ?? 0) > 0)
    ) {
      const shopifyStoreIdForExpansion = (dataSource as any).shopifyStoreId;
      if (shopifyStoreIdForExpansion) {
        console.log(
          `${LOG} Step 13: Applying price-based size expansion...`,
        );
        try {
          const cacheVariants = await storage.getVariantCacheProductStyles(
            shopifyStoreIdForExpansion,
          );
          const stylePriceMap = buildStylePriceMapFromCache(cacheVariants);
          console.log(
            `${LOG} Step 13: Built style price map with ${stylePriceMap.size} styles`,
          );

          const expansionResult = applyPriceBasedExpansion(
            items,
            priceBasedExpansionConfig,
            stylePriceMap,
            sizeLimitConfig,
          );
          items = expansionResult.items;
          priceBasedExpansionCount = expansionResult.addedCount;

          if (priceBasedExpansionCount > 0) {
            console.log(
              `${LOG} Step 13: Price-based expansion added ${priceBasedExpansionCount} size variants`,
            );
          }
        } catch (expansionError) {
          console.error(
            `${LOG} Step 13: Price-based expansion error:`,
            expansionError,
          );
          // Continue without expansion — non-fatal
        }
      } else {
        console.log(
          `${LOG} Step 13: Price-based expansion enabled but no Shopify store linked — skipping`,
        );
      }
    }
    stats.priceBasedExpansion = priceBasedExpansionCount;
    stats.afterPriceExpansion = items.length;

    // ================================================================
    // ===================== PHASE 4: BUSINESS LOGIC ===================
    // Steps 14-16: Discontinued styles, sale pricing, stockInfo
    // ================================================================

    // --------------------------------------------------
    // Step 14: filterDiscontinuedStyles (sale file cross-reference)
    // If this is a regular file with a linked sale file, filter out
    // items whose styles appear in the sale file's discontinued list.
    // Also removes existing inventory items that have discontinued styles.
    // --------------------------------------------------
    const linkedSaleDataSourceId = (dataSource as any).assignedSaleDataSourceId;
    let discontinuedStylesFiltered = 0;
    let discontinuedItemsRemoved = 0;

    if (!isSaleFile && linkedSaleDataSourceId) {
      console.log(
        `${LOG} Step 14: Checking for discontinued styles from linked sale file`,
      );

      try {
        // First, remove any EXISTING inventory items that have discontinued styles
        discontinuedItemsRemoved = await removeDiscontinuedInventoryItems(
          dataSourceId,
          linkedSaleDataSourceId,
        );
        if (discontinuedItemsRemoved > 0) {
          console.log(
            `${LOG} Step 14: Removed ${discontinuedItemsRemoved} existing inventory items with discontinued styles`,
          );
        }

        // Then, filter out items from THIS import that have discontinued styles
        const filterResult = await filterDiscontinuedStyles(
          dataSourceId,
          items,
          linkedSaleDataSourceId,
        );
        items = filterResult.items;
        discontinuedStylesFiltered = filterResult.removedCount;

        if (discontinuedStylesFiltered > 0) {
          console.log(
            `${LOG} Step 14: Filtered out ${discontinuedStylesFiltered} items with ${filterResult.discontinuedStyles?.length || 0} discontinued styles: ${(filterResult.discontinuedStyles || []).slice(0, 3).join(", ")}${(filterResult.discontinuedStyles?.length || 0) > 3 ? "..." : ""}`,
          );
        }
      } catch (discontinuedError) {
        console.error(
          `${LOG} Step 14: Discontinued filtering error:`,
          discontinuedError,
        );
        // Continue without filtering — non-fatal
      }
    }
    stats.discontinuedStylesFiltered = discontinuedStylesFiltered;
    stats.discontinuedItemsRemoved = discontinuedItemsRemoved;

    // --------------------------------------------------
    // Step 15: Sale file pricing (Shopify compare-at)
    // For sale-type data sources:
    //   - Multiply price by configured multiplier (default 2x)
    //   - Look up Shopify's current price by SKU to use as compare-at (strike-through)
    //   - Store Shopify price in cost field for compare-at sync
    // --------------------------------------------------
    const shopifyStoreId = (dataSource as any).shopifyStoreId;
    const salesConfig = (dataSource as any).salesConfig || {
      priceMultiplier: 2,
      useCompareAtPrice: true,
    };
    const priceMultiplier = salesConfig.priceMultiplier || 2;
    const useCompareAtPrice = salesConfig.useCompareAtPrice ?? true;

    let shopifyPricesLoaded = 0;

    if (isSaleFile && shopifyStoreId && useCompareAtPrice) {
      try {
        const skus = items
          .map((item: any) => item.sku)
          .filter((sku: string | null) => sku && sku.trim());

        if (skus.length > 0) {
          console.log(
            `${LOG} Step 15: Looking up Shopify prices for ${skus.length} SKUs (sale file compare-at)`,
          );
          const cachedVariants = await storage.getVariantCacheBySKUs(
            shopifyStoreId,
            skus,
          );
          const shopifyPriceMap = new Map<string, string>();
          for (const v of cachedVariants) {
            if (v.sku && v.price) {
              shopifyPriceMap.set(v.sku.trim().toUpperCase(), v.price);
            }
          }
          shopifyPricesLoaded = shopifyPriceMap.size;
          console.log(
            `${LOG} Step 15: Loaded ${shopifyPricesLoaded} Shopify prices for compare-at`,
          );

          if (shopifyPricesLoaded > 0) {
            items = items.map((item: any) => {
              const basePrice = parseFloat(item.price || "0");
              let finalPrice = item.price;
              let cost = item.cost || null;

              if (basePrice > 0) {
                // Apply multiplier to get final sale price
                finalPrice = (basePrice * priceMultiplier).toFixed(2);

                // Look up Shopify price for compare-at
                if (item.sku && useCompareAtPrice) {
                  const normalizedSku = item.sku.trim().toUpperCase();
                  const shopifyPrice = shopifyPriceMap.get(normalizedSku);
                  if (shopifyPrice) {
                    cost = shopifyPrice; // Shopify's current price becomes strike-through
                  }
                }
              }
              return { ...item, price: finalPrice, cost };
            });
            console.log(
              `${LOG} Step 15: Applied sale pricing — ${priceMultiplier}x multiplier, ${shopifyPricesLoaded} compare-at prices set`,
            );
          }
        }
      } catch (err) {
        console.error(
          `${LOG} Step 15: Error loading Shopify prices:`,
          err,
        );
        // Continue without sale pricing — non-fatal
      }
    }
    stats.shopifyPricesLoaded = shopifyPricesLoaded;

    // --------------------------------------------------
    // Step 16: calculateStockInfo (messages)
    // Priority:
    //   (1) overrideConfig.stockInfoConfig (from UI — immediate, no race condition)
    //   (2) dataSource.stockInfoConfig (from DB)
    //   (3) Rule Engine metafield rules (legacy fallback)
    //
    // Message priority per item:
    //   1. Expanded size → sizeExpansionMessage
    //   2. In stock (stock > threshold) → inStockMessage
    //   3. Future date (shipDate + offset > today) → futureDateMessage with {date}
    //   4. Out of stock → outOfStockMessage
    // --------------------------------------------------
    let stockInfoRule: any = null;
    try {
      // BUG FIX: Check overrideConfig FIRST (sent from UI), THEN fall back to DB
      // This ensures newly configured settings are used immediately
      const overrideStockInfoConfig = overrideConfig?.stockInfoConfig;
      if (overrideStockInfoConfig) {
        const hasMessages =
          overrideStockInfoConfig.message1InStock ||
          overrideStockInfoConfig.message2ExtraSizes ||
          overrideStockInfoConfig.message3Default ||
          overrideStockInfoConfig.message4FutureDate;
        if (hasMessages) {
          stockInfoRule = {
            id: "override-config",
            name: "Override Stock Info Config",
            stockThreshold: 0,
            inStockMessage: overrideStockInfoConfig.message1InStock || "",
            sizeExpansionMessage:
              overrideStockInfoConfig.message2ExtraSizes || null,
            outOfStockMessage: overrideStockInfoConfig.message3Default || "",
            futureDateMessage:
              overrideStockInfoConfig.message4FutureDate || null,
            dateOffsetDays: overrideStockInfoConfig.dateOffsetDays ?? 0,
            enabled: true,
          };
          console.log(
            `${LOG} Step 16: Using overrideConfig stockInfoConfig`,
          );
        }
      }

      // If no override, use getStockInfoRule (checks DB config then metafield rules)
      if (!stockInfoRule) {
        stockInfoRule = await getStockInfoRule(dataSource, storage);
      }
    } catch (ruleError) {
      console.error(
        `${LOG} Step 16: Failed to get stock info rules:`,
        ruleError,
      );
    }

    if (stockInfoRule) {
      console.log(
        `${LOG} Step 16: Calculating stockInfo for ${items.length} items using rule: "${stockInfoRule.name}"`,
      );
      items = items.map((item: any) => ({
        ...item,
        stockInfo: calculateItemStockInfo(item, stockInfoRule),
      }));
      const itemsWithStockInfo = items.filter(
        (i: any) => i.stockInfo,
      ).length;
      console.log(
        `${LOG} Step 16: stockInfo — ${itemsWithStockInfo}/${items.length} items have messages`,
      );
      stats.stockInfoAssigned = itemsWithStockInfo;
    } else {
      console.log(
        `${LOG} Step 16: No stockInfo rule configured — stockInfo will be null`,
      );
      stats.stockInfoAssigned = 0;
    }

    // ================================================================
    // ===================== PHASE 5: SAVE =============================
    // Steps 17-20: Safety nets, DB save, import stats, post-import
    // ================================================================

    stats.afterDiscontinuedFilter = items.length;
    stats.finalCount = items.length;

    // --------------------------------------------------
    // Step 17: Safety nets (0-item check, percentage drop check)
    // Prevents accidental data wipes from empty/corrupted files.
    // Uses per-data-source safetyThreshold (default 50%). Set to 0 to disable.
    // --------------------------------------------------
    const updateStrategy = (dataSource as any).updateStrategy || "full_sync";

    if (updateStrategy === "full_sync") {
      const existingCount =
        await storage.getInventoryItemCountByDataSource(dataSourceId);

      // Safety net 1: 0 items would wipe all existing data
      if (items.length === 0 && existingCount > 0) {
        const errorMsg = `SAFETY NET: 0 items parsed but would delete ${existingCount} existing items. Import blocked to prevent data loss.`;
        console.error(`${LOG} Step 17: SAFETY BLOCK — ${errorMsg}`);
        return {
          success: false,
          itemCount: 0,
          error: errorMsg,
          safetyBlock: true,
          stats: { ...stats, existingCount },
        };
      }

      // Safety net 2: Item count dropped by more than threshold
      const safetyThreshold = (dataSource as any).safetyThreshold ?? 50;
      if (safetyThreshold > 0 && existingCount > 0 && items.length > 0) {
        const dropPercent =
          ((existingCount - items.length) / existingCount) * 100;
        if (dropPercent > safetyThreshold) {
          const errorMsg = `SAFETY NET: Item count dropped ${dropPercent.toFixed(0)}% (from ${existingCount} to ${items.length}). Threshold is ${safetyThreshold}%. Import blocked to prevent data loss.`;
          console.error(`${LOG} Step 17: SAFETY BLOCK — ${errorMsg}`);
          return {
            success: false,
            itemCount: 0,
            error: errorMsg,
            safetyBlock: true,
            stats: {
              ...stats,
              existingCount,
              newCount: items.length,
              dropPercent: Math.round(dropPercent),
            },
          };
        }
      }
    }

    // --------------------------------------------------
    // Step 18: Save to DB (atomic replace or upsert)
    // --------------------------------------------------

    // 18a: Create file record to get fileId
    const primaryFilename =
      source === "combine"
        ? `${preConsolidatedItems?.length || 0} staged files combined`
        : fileBuffers.length > 1
          ? `${fileBuffers.length} files consolidated`
          : fileBuffers[0]?.originalname || "import";

    const fileRecord = await storage.createUploadedFile({
      dataSourceId,
      fileName: primaryFilename,
      fileSize:
        source !== "combine" && fileBuffers?.length > 0
          ? fileBuffers.reduce((sum, f) => sum + (f.buffer?.length || 0), 0)
          : undefined,
      status: "completed",
      rowCount: items.length,
      processedAt: new Date(),
      headers: headers.length > 0 ? headers : undefined,
      fileStatus: "imported",
    } as any);

    // Notify caller about file record if callback provided
    if (onFileRecord) {
      onFileRecord(fileRecord);
    }

    // 18b: Map items for saving with dataSourceId and fileId
    const itemsToSave = items.map((item: any) => ({
      dataSourceId,
      fileId: fileRecord.id,
      sku:
        item.sku ||
        `${item.style}-${item.color}-${item.size}`
          .toUpperCase()
          .replace(/\s+/g, "-"),
      style: item.style,
      color: item.color || "",
      size: item.size || "",
      stock: item.stock || 0,
      price: item.price,
      cost: item.cost,
      shipDate: item.shipDate,
      discontinued: item.discontinued || false,
      isExpandedSize: item.isExpandedSize || false,
      stockInfo: item.stockInfo || null,
      rawData: item.rawData || null,
      // Preserve flags used by consolidation and sync
      saleOwnsStyle: isSaleFile ? true : undefined,
      hasFutureStock: item.hasFutureStock || false,
      preserveZeroStock: item.preserveZeroStock || false,
    }));

    // 18c: Save items based on update strategy
    console.log(
      `${LOG} Step 18: Saving ${itemsToSave.length} items (strategy=${updateStrategy})`,
    );

    let importedCount = 0;
    let addedCount = 0;
    let updatedCount = 0;

    if (itemsToSave.length > 0) {
      if (updateStrategy === "replace") {
        // Upsert: Create new items, update existing by SKU (items not in file are kept)
        console.log(
          `${LOG} Step 18: Upserting ${itemsToSave.length} items`,
        );
        const isRegularInventory = !isSaleFile;
        const result = await storage.upsertInventoryItems(
          itemsToSave,
          dataSourceId,
          { resetSaleFlags: isRegularInventory },
        );
        addedCount = result.added;
        updatedCount = result.updated;
        importedCount = addedCount + updatedCount;
        console.log(
          `${LOG} Step 18: Upsert complete — added ${addedCount}, updated ${updatedCount}`,
        );
      } else {
        // Full Sync (default): Atomic delete + insert — guarantees no stale items
        console.log(
          `${LOG} Step 18: Atomic replace with ${itemsToSave.length} items`,
        );
        const result = await storage.atomicReplaceInventoryItems(
          dataSourceId,
          itemsToSave,
        );
        importedCount = result.created;
        console.log(
          `${LOG} Step 18: Atomic replace complete — deleted ${result.deleted}, created ${result.created} items`,
        );
      }
    } else if (updateStrategy === "full_sync") {
      // 0 items + full_sync + 0 existing = nothing to do (safety net above blocks if existing > 0)
      console.log(
        `${LOG} Step 18: No items to import and no existing items — nothing to do`,
      );
    }

    stats.updateStrategy = updateStrategy;
    stats.importedCount = importedCount;
    stats.addedCount = addedCount;
    stats.updatedCount = updatedCount;

    // --------------------------------------------------
    // Step 19: Save import stats & update lastSync
    // These are the expected counts AFTER all rules are applied.
    // Includes product-level data for detailed validation.
    // --------------------------------------------------
    await storage.updateDataSource(dataSourceId, { lastSync: new Date() });

    try {
      const styles = new Set<string>();
      const colors = new Set<string>();
      const skuSet = new Set<string>();
      let totalStock = 0;
      let itemsWithPrice = 0;
      let itemsWithShipDate = 0;
      let itemsDiscontinued = 0;
      let itemsExpanded = 0;

      // Product-level tracking: group by style
      const productSummary: Record<
        string,
        {
          variantCount: number;
          colors: Set<string>;
          sizes: Set<string>;
          totalStock: number;
          hasDiscontinued: boolean;
          hasFutureDate: boolean;
          expandedCount: number;
          skus: string[];
        }
      > = {};

      for (const item of itemsToSave) {
        const prefixedStyle = String(item.style || "")
          .toUpperCase()
          .trim();
        const mappedColor = String(item.color || "")
          .toUpperCase()
          .trim();
        const size = String(item.size || "").trim();
        const sku = String(item.sku || "")
          .toUpperCase()
          .trim();
        const stock = Math.max(0, Number(item.stock) || 0);

        // Global counts
        if (prefixedStyle) styles.add(prefixedStyle);
        if (mappedColor) colors.add(mappedColor);
        if (sku) skuSet.add(sku);
        totalStock += stock;
        if (item.price && parseFloat(String(item.price)) > 0)
          itemsWithPrice++;
        if (item.shipDate) itemsWithShipDate++;
        if (item.discontinued) itemsDiscontinued++;
        if (item.isExpandedSize) itemsExpanded++;

        // Product-level tracking
        if (!productSummary[prefixedStyle]) {
          productSummary[prefixedStyle] = {
            variantCount: 0,
            colors: new Set(),
            sizes: new Set(),
            totalStock: 0,
            hasDiscontinued: false,
            hasFutureDate: false,
            expandedCount: 0,
            skus: [],
          };
        }
        const product = productSummary[prefixedStyle];
        product.variantCount++;
        if (mappedColor) product.colors.add(mappedColor);
        if (size) product.sizes.add(size);
        product.totalStock += stock;
        if (item.discontinued) product.hasDiscontinued = true;
        if (item.shipDate) product.hasFutureDate = true;
        if (item.isExpandedSize) product.expandedCount++;
        if (sku && product.skus.length < 50) product.skus.push(sku);
      }

      // Convert Sets to Arrays for JSON serialization
      const productData: Record<string, any> = {};
      for (const [style, data] of Object.entries(productSummary)) {
        productData[style] = {
          variantCount: data.variantCount,
          colors: Array.from(data.colors),
          sizes: Array.from(data.sizes),
          totalStock: data.totalStock,
          hasDiscontinued: data.hasDiscontinued,
          hasFutureDate: data.hasFutureDate,
          expandedCount: data.expandedCount,
          skus: data.skus,
        };
      }

      const stylePrefix = dataSource.name
        ? String(dataSource.name).toUpperCase().trim()
        : "";

      const importStats = {
        importedAt: new Date().toISOString(),
        source,
        // Global checksums
        itemCount: itemsToSave.length,
        totalStock: Math.round(totalStock),
        uniqueStyles: styles.size,
        uniqueColors: colors.size,
        itemsWithPrice,
        itemsWithShipDate,
        itemsDiscontinued,
        itemsExpanded,
        // Lists for comparison (with mappings applied)
        styleList: Array.from(styles).slice(0, 2000),
        colorList: Array.from(colors).slice(0, 500),
        // Product-level data for detailed validation
        productData,
        // Metadata
        stylePrefix: stylePrefix || null,
      };

      console.log(
        `${LOG} Step 19: Saving import stats — items=${importStats.itemCount}, styles=${importStats.uniqueStyles}, colors=${importStats.uniqueColors}, products=${Object.keys(productData).length}`,
      );
      await storage.updateDataSource(dataSourceId, {
        lastImportStats: importStats,
      } as any);
    } catch (statsErr: any) {
      console.warn(
        `${LOG} Step 19: Could not save import stats: ${statsErr.message}`,
      );
    }

    // --------------------------------------------------
    // Step 20: Post-import
    // 20a: Register sale file styles (if sale type)
    // 20b: Trigger auto-consolidation (dedup + sale-into-regular merge)
    // 20c: Mark staged files as imported (combine path)
    // 20d: Update lastSync timestamp
    // --------------------------------------------------
    let saleStylesRegistered = 0;

    // 20a: Register sale file styles in discontinued_styles table
    if (isSaleFile && itemsToSave.length > 0) {
      try {
        const styleResult = await registerSaleFileStyles(
          dataSourceId,
          itemsToSave,
        );
        saleStylesRegistered = styleResult.total;
        console.log(
          `${LOG} Step 20a: Registered ${saleStylesRegistered} styles from sale file (${styleResult.added} new, ${styleResult.updated} updated)`,
        );
      } catch (err) {
        console.error(
          `${LOG} Step 20a: Error registering sale file styles:`,
          err,
        );
      }
    }
    stats.saleStylesRegistered = saleStylesRegistered;

    // 20b: Trigger auto-consolidation
    // - Deduplicates inventory items
    // - For regular sources with sale file: merges sale items into regular
    // - For sale sources: triggers consolidation for linked regular sources
    try {
      await triggerAutoConsolidationAfterImport(dataSourceId);
    } catch (err: any) {
      console.error(
        `${LOG} Step 20b: Error in auto-consolidation: ${err.message}`,
      );
    }

    // 20c: Mark staged files as imported (combine path only)
    if (source === "combine") {
      try {
        const stagedFiles = await storage.getStagedFiles(dataSourceId);
        for (const sf of stagedFiles) {
          await storage.updateFileStatus(sf.id, "imported");
        }
        console.log(
          `${LOG} Step 20c: Marked ${stagedFiles.length} staged files as imported`,
        );
      } catch (err) {
        console.error(
          `${LOG} Step 20c: Error marking staged files:`,
          err,
        );
      }
    }

    // ================================================================
    // RETURN SUCCESS RESULT
    // ================================================================
    const finalStats = {
      ...stats,
      // Summary from parse phase
      totalParsed: stats.totalParsed || preConsolidatedItems?.length || 0,
      // Pipeline step counts
      afterImportRules: stats.afterImportRules,
      afterVariantRules: stats.afterVariantRules,
      afterPriceExpansion: stats.afterPriceExpansion,
      afterDiscontinuedFilter: stats.afterDiscontinuedFilter,
      finalCount: itemsToSave.length,
      // Detailed counts
      importRulesStats: stats.importRulesStats,
      sizesExpanded: variantRulesResult.addedCount || 0,
      sizeFiltered: variantRulesResult.sizeFiltered || 0,
      zeroStockFiltered: variantRulesResult.filteredCount || 0,
      priceBasedExpansion: priceBasedExpansionCount,
      colorsFixed: (stats.colorsFixed || 0) + globalColorsFixed,
      aiColorsFixed: stats.aiColorsFixed || 0,
      discontinuedStylesFiltered,
      discontinuedItemsRemoved,
      saleStylesRegistered,
      shopifyPricesLoaded,
      // DB operation results
      updateStrategy,
      importedCount,
      addedCount,
      updatedCount,
    };

    console.log(
      `${LOG} DONE: ${itemsToSave.length} items saved for "${dataSource.name}" (strategy=${updateStrategy})`,
    );

    return {
      success: true,
      itemCount: itemsToSave.length,
      fileId: fileRecord.id,
      stats: finalStats,
    };
  } catch (error: any) {
    console.error(`${LOG} Fatal error:`, error);
    return {
      success: false,
      itemCount: 0,
      error: error.message || "Import failed",
    };
  }
}
