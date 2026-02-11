/**
 * importEngine.ts — Unified Import Engine
 *
 * ALL import paths (manual upload, email, URL, combine, AI import) call
 * the single `executeImport()` function, ensuring every item passes through
 * the same pipeline:
 *
 *   PHASE 1  PARSE        – read file(s), detect format, parse items
 *   PHASE 2  FILTER       – skip rules, discontinued-zero-stock, dedup
 *   PHASE 3  TRANSFORM    – prefix, clean, import rules, colors, variants, expansion
 *   PHASE 4  BUSINESS     – discontinued styles, sale pricing, stockInfo
 *   PHASE 5  SAVE         – safety nets, DB write, stats, post-import hooks
 *
 * BUG FIX: processUrlDataSourceImport() was MISSING deduplicateAndZeroFutureStock().
 *          By routing through executeImport(), ALL paths now include it (Phase 2 Step 5).
 */

import * as XLSX from "xlsx";
import { storage } from "./storage";
import {
  autoDetectPivotFormat,
  parseIntelligentPivotFormat,
  fixSheetRange,
  type UniversalParserConfig,
} from "./aiImportRoutes";
import {
  cleanInventoryData,
  applyImportRules,
  applyVariantRules,
  applyPriceBasedExpansion,
  buildStylePriceMapFromCache,
  formatColorName,
  deduplicateAndZeroFutureStock,
} from "./inventoryProcessing";
import {
  applyCleaningToValue,
  filterDiscontinuedStyles,
  removeDiscontinuedInventoryItems,
  registerSaleFileStyles,
  parsePivotedExcelToInventory,
} from "./importUtils";
import { parseGroupedPivotData } from "./universalParser";

// ============================================================
// TYPE DEFINITIONS
// ============================================================

export interface ImportOptions {
  // === Input (provide ONE of these) ===
  /** Raw file buffers to parse (manual upload, email single-file, URL, AI import) */
  fileBuffers?: { buffer: Buffer; originalname: string }[];
  /** Pre-consolidated items from staged files (combine path — skips parse phase) */
  preConsolidatedItems?: any[];
  /** Pre-parsed rows for import rules (combine path provides these) */
  preConsolidatedRows?: any[][];

  // === Required context ===
  dataSourceId: string;

  // === Source flag — determines path-specific behavior ===
  source: "manual_upload" | "email" | "url" | "combine" | "ai_import";

  // === Optional overrides (AI import passes these) ===
  overrideConfig?: any;

  // === Optional callbacks ===
  /** Called after file record is created */
  onFileRecord?: (file: any) => void;

  // === Optional pre-computed values ===
  /** If the caller already has the dataSource object, pass it to avoid re-fetch */
  dataSource?: any;
  /** File ID to attach to inventory items (manual upload sets this) */
  fileId?: string | null;
}

export interface ImportResult {
  success: boolean;
  itemCount: number;
  error?: string;
  safetyBlock?: boolean;
  fileId?: string;
  stats?: ImportStats;
  validation?: any;
  headers?: string[];
}

export interface ImportStats {
  totalParsed: number;
  afterClean: number;
  afterImportRules: number;
  afterVariantRules: number;
  afterPriceExpansion: number;
  afterDiscontinuedFilter: number;
  finalCount: number;
  noSizeRemoved: number;
  colorsFixed: number;
  aiColorsFixed: number;
  duplicatesRemoved: number;
  priceBasedExpansion: number;
  discontinuedStylesFiltered: number;
  discontinuedItemsRemoved: number;
  saleStylesRegistered: number;
  variantRulesAdded: number;
  variantRulesFiltered: number;
  variantRulesSizeFiltered: number;
  importRulesStats: any;
  dedupStats?: any;
}

// ============================================================
// HELPER: Calculate stockInfo for a single item
// FROM: routes (29).ts line 1637 (canonical version — superset of all 3 copies)
// ============================================================

export function calculateItemStockInfo(
  item: any,
  stockInfoRule: any,
): string | null {
  if (!stockInfoRule) return null;

  const stock = item.stock || 0;
  const shipDate = item.shipDate;
  const isExpandedSize = item.isExpandedSize || false;
  const threshold = stockInfoRule.stockThreshold || 0;

  // Priority 1: Expanded size
  if (isExpandedSize && stockInfoRule.sizeExpansionMessage) {
    return stockInfoRule.sizeExpansionMessage;
  }

  // Priority 2: In stock — ALWAYS takes priority over future date
  if (stock > threshold) {
    return stockInfoRule.inStockMessage;
  }

  // Priority 3: Has future date — ONLY for zero/low stock items
  if (shipDate && stockInfoRule.futureDateMessage) {
    try {
      const dateStr = String(shipDate).trim();
      let targetDate: Date;

      const isoMatch = dateStr.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
      const usMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
      const usShortMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2})$/);

      if (isoMatch) {
        const [, year, month, day] = isoMatch;
        targetDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
      } else if (usMatch) {
        const [, month, day, year] = usMatch;
        targetDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
      } else if (usShortMatch) {
        const [, month, day, shortYear] = usShortMatch;
        targetDate = new Date(2000 + parseInt(shortYear), parseInt(month) - 1, parseInt(day));
      } else {
        targetDate = new Date(dateStr);
      }

      // Apply offset days
      const offsetDays = stockInfoRule.dateOffsetDays || 0;
      if (offsetDays !== 0) {
        targetDate.setDate(targetDate.getDate() + offsetDays);
      }

      const today = new Date();
      today.setHours(0, 0, 0, 0);
      targetDate.setHours(0, 0, 0, 0);

      if (targetDate > today) {
        const formattedDate = targetDate.toLocaleDateString("en-US", {
          month: "long",
          day: "numeric",
          year: "numeric",
        });
        return stockInfoRule.futureDateMessage.replace(/\{date\}/gi, formattedDate);
      }
    } catch (e) {
      // Ignore date parse errors
    }
  }

  // Priority 4: Out of stock
  let outOfStockMsg = stockInfoRule.outOfStockMessage;
  if (outOfStockMsg && outOfStockMsg.includes("{date}")) {
    outOfStockMsg = outOfStockMsg
      .replace(/\{date\}/gi, "")
      .replace(/\s+/g, " ")
      .trim();
  }
  return outOfStockMsg || null;
}

// ============================================================
// HELPER: Get stockInfo rule from data source config or DB
// FROM: routes (29).ts line 1736 (canonical — superset of routes + importUtils copies)
// ============================================================

export async function getStockInfoRule(
  dataSource: any,
  overrideConfig?: any,
): Promise<any> {
  let stockInfoRule: any = null;
  try {
    const stockInfoConfig =
      overrideConfig?.stockInfoConfig || (dataSource as any).stockInfoConfig;

    const hasStockInfoMessages =
      stockInfoConfig &&
      (stockInfoConfig.message1InStock ||
        stockInfoConfig.message2ExtraSizes ||
        stockInfoConfig.message3Default ||
        stockInfoConfig.message4FutureDate);

    if (hasStockInfoMessages) {
      stockInfoRule = {
        id: "ai-importer-config",
        name: "AI Importer Stock Info Config",
        stockThreshold: 0,
        inStockMessage: stockInfoConfig.message1InStock || "",
        sizeExpansionMessage: stockInfoConfig.message2ExtraSizes || null,
        outOfStockMessage: stockInfoConfig.message3Default || "",
        futureDateMessage: stockInfoConfig.message4FutureDate || null,
        dateOffsetDays: stockInfoConfig.dateOffsetDays ?? 0,
        enabled: true,
      };
    } else {
      const metafieldRules =
        await storage.getShopifyMetafieldRulesByDataSource(dataSource.id);
      const activeDbRule = metafieldRules.find(
        (r: any) => r.enabled !== false,
      );
      if (activeDbRule) {
        stockInfoRule = {
          id: activeDbRule.id,
          name: activeDbRule.name || "Rule Engine Metafield Rule",
          stockThreshold:
            activeDbRule.stockThreshold ?? activeDbRule.stock_threshold ?? 0,
          inStockMessage:
            activeDbRule.inStockMessage || activeDbRule.in_stock_message || "",
          sizeExpansionMessage:
            activeDbRule.sizeExpansionMessage ||
            activeDbRule.size_expansion_message ||
            null,
          outOfStockMessage:
            activeDbRule.outOfStockMessage ||
            activeDbRule.out_of_stock_message ||
            "",
          futureDateMessage:
            activeDbRule.futureDateMessage ||
            activeDbRule.future_date_message ||
            null,
          dateOffsetDays:
            activeDbRule.dateOffsetDays ?? activeDbRule.date_offset_days ?? 0,
          enabled: true,
        };
      }
    }
  } catch (ruleError) {
    console.error(`[ImportEngine] Failed to get stock info rules:`, ruleError);
  }
  return stockInfoRule;
}

// ============================================================
// HELPER: Get style prefix for an item
// FROM: routes (29).ts line 4397 (canonical — superset of all 4+ copies)
// ============================================================

export function getStylePrefix(
  style: string,
  dataSource: any,
  cleaningConfig: any,
): string {
  if (
    cleaningConfig?.useCustomPrefixes &&
    cleaningConfig?.stylePrefixRules?.length > 0
  ) {
    for (const rule of cleaningConfig.stylePrefixRules) {
      if (rule.pattern && rule.prefix) {
        try {
          const regex = new RegExp(rule.pattern, "i");
          if (regex.test(style)) {
            return rule.prefix;
          }
        } catch (e) {
          if (style.toLowerCase().startsWith(rule.pattern.toLowerCase())) {
            return rule.prefix;
          }
        }
      }
    }
  }
  let prefix = dataSource.name;
  if ((dataSource as any).sourceType === "sales") {
    const saleMatch = prefix.match(/^(.+?)\s*(Sale|Sales)$/i);
    if (saleMatch) {
      prefix = saleMatch[1].trim();
    }
  }
  return prefix;
}

// ============================================================
// HELPER: Safety threshold check
// FROM: routes (29).ts line 120
// ============================================================

export function checkSafetyThreshold(
  dataSource: any,
  existingCount: number,
  newCount: number,
  label: string,
): { blocked: boolean; message: string; dropPercent?: number } {
  const safetyThreshold = (dataSource as any).safetyThreshold ?? 50;
  if (safetyThreshold <= 0 || existingCount <= 0) {
    return { blocked: false, message: "" };
  }

  // 0-item guard
  if (newCount === 0) {
    const msg = `SAFETY NET: ${label} has 0 items but data source "${dataSource.name}" has ${existingCount} existing items. Import blocked.`;
    console.error(`[ImportEngine] ${msg}`);
    return { blocked: true, message: msg, dropPercent: 100 };
  }

  // 50% drop guard (configurable per DS)
  const dropPercent = ((existingCount - newCount) / existingCount) * 100;
  if (dropPercent > safetyThreshold) {
    const msg = `SAFETY NET: Item count dropped ${Math.round(dropPercent)}% (from ${existingCount} to ${newCount}). Threshold is ${safetyThreshold}%. Import blocked for "${dataSource.name}".`;
    console.error(`[ImportEngine] ${msg}`);
    return { blocked: true, message: msg, dropPercent: Math.round(dropPercent) };
  }

  return { blocked: false, message: "" };
}

// ============================================================
// HELPER: Title Case conversion
// ============================================================

export function toTitleCase(str: string): string {
  return str
    .toLowerCase()
    .replace(/(?:^|[\s\-\/&])\S/g, (a) => a.toUpperCase());
}

// ============================================================
// MAIN: executeImport() — THE unified import function
// ============================================================

export async function executeImport(
  options: ImportOptions,
): Promise<ImportResult> {
  const {
    fileBuffers,
    preConsolidatedItems,
    preConsolidatedRows,
    dataSourceId,
    source,
    overrideConfig,
    onFileRecord,
    fileId: externalFileId,
  } = options;

  const logPrefix = `[ImportEngine:${source}]`;

  // ──────────────────────────────────────────────────────────
  // STEP 0: Load data source
  // ──────────────────────────────────────────────────────────
  const dataSource = options.dataSource || await storage.getDataSource(dataSourceId);
  if (!dataSource) {
    return { success: false, itemCount: 0, error: "Data source not found" };
  }
  console.log(`${logPrefix} Loaded dataSource "${dataSource.name}"`);

  const cleaningConfig = overrideConfig?.cleaningConfig || (dataSource.cleaningConfig || {}) as any;
  const isSaleFile = (dataSource as any).sourceType === "sales";

  // ──────────────────────────────────────────────────────────
  // PHASE 1: PARSE
  // ──────────────────────────────────────────────────────────

  let items: any[] = [];
  let headers: string[] = [];
  let rows: any[][] = [];
  let rawData: any[][] = [];

  if (preConsolidatedItems) {
    // PHASE 1 BYPASS: Combine path provides pre-extracted items
    console.log(`${logPrefix} Using ${preConsolidatedItems.length} pre-consolidated items`);
    items = preConsolidatedItems;
    rows = preConsolidatedRows || [];
  } else if (fileBuffers && fileBuffers.length > 0) {
    // PHASE 1, Step 1: Read & consolidate files
    const primaryFile = fileBuffers[0];

    if (fileBuffers.length > 1) {
      // Multi-file consolidation (merge headers, append rows)
      console.log(`${logPrefix} Consolidating ${fileBuffers.length} files`);
      let headerRow: any[] | null = null;
      for (const file of fileBuffers) {
        const wb = XLSX.read(file.buffer, { type: "buffer" });
        const sheet = wb.Sheets[wb.SheetNames[0]];
        fixSheetRange(sheet);
        const data = XLSX.utils.sheet_to_json(sheet, {
          header: 1,
          defval: "",
          raw: false, // FIX: Prevent scientific notation corruption (e.g. "1921E0136" → 1.921e+139)
        }) as any[][];
        if (headerRow === null && data.length > 0) {
          headerRow = data[0];
          rawData = data;
        } else if (data.length > 1) {
          rawData.push(...data.slice(1));
        }
      }
      console.log(`${logPrefix} Consolidated ${rawData.length} total rows`);
    } else {
      const workbook = XLSX.read(primaryFile.buffer, { type: "buffer" });
      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      fixSheetRange(sheet);
      rawData = XLSX.utils.sheet_to_json(sheet, {
        header: 1,
        defval: "",
        raw: false,
      }) as any[][];
    }

    // PHASE 1, Step 2: Auto-detect format
    let consolidatedBuffer: Buffer;
    if (fileBuffers.length > 1) {
      const newWorkbook = XLSX.utils.book_new();
      const newSheet = XLSX.utils.aoa_to_sheet(rawData);
      XLSX.utils.book_append_sheet(newWorkbook, newSheet, "Consolidated");
      consolidatedBuffer = Buffer.from(
        XLSX.write(newWorkbook, { type: "buffer", bookType: "xlsx" }),
      );
    } else {
      consolidatedBuffer = primaryFile.buffer;
    }

    const detectedPivotFormat = rawData.length > 0
      ? autoDetectPivotFormat(rawData, dataSource.name, primaryFile.originalname)
      : null;

    // Determine which config to use for parsing
    const dsConfig = {
      formatType: overrideConfig?.formatType || (dataSource as any).formatType,
      columnMapping: overrideConfig?.columnMapping || dataSource.columnMapping || {},
      pivotConfig: overrideConfig?.pivotConfig || (dataSource as any).pivotConfig,
      discontinuedConfig: overrideConfig?.discontinuedConfig || (dataSource as any).discontinuedConfig,
      futureStockConfig: overrideConfig?.futureStockConfig || (dataSource as any).futureStockConfig,
      stockValueConfig: overrideConfig?.stockValueConfig || (dataSource as any).stockValueConfig,
    };

    const isPivotFormat =
      detectedPivotFormat !== null ||
      dsConfig.formatType?.startsWith("pivot") ||
      dsConfig.formatType === "pivoted" ||
      (dsConfig.pivotConfig?.format && dsConfig.pivotConfig.format !== "generic_legacy");

    // PHASE 1, Step 3: Parse using appropriate parser
    if (dsConfig.formatType === "pivot_grouped" && (dataSource as any).groupedPivotConfig?.enabled) {
      // Grouped pivot format (AI-detected) — use universal parser extractor
      const gpConfig = (dataSource as any).groupedPivotConfig;
      console.log(`${logPrefix} Using grouped pivot parser (universal)`);
      const groupedResult = parseGroupedPivotData(rawData, gpConfig);
      headers = groupedResult.headers;
      rows = groupedResult.rows;
      items = groupedResult.items;
    } else if (isPivotFormat || detectedPivotFormat) {
      const actualFormat =
        detectedPivotFormat || dsConfig.pivotConfig?.format || dsConfig.formatType || "pivot_interleaved";
      console.log(`${logPrefix} Using shared parser for format: "${actualFormat}"`);

      const universalConfig: UniversalParserConfig = {
        skipRows: dsConfig.pivotConfig?.skipRows,
        discontinuedConfig: dsConfig.discontinuedConfig as any,
        futureDateConfig: dsConfig.futureStockConfig as any,
        stockConfig: dsConfig.stockValueConfig as any,
        columnMapping: dsConfig.columnMapping,
      };

      const pivotResult = parseIntelligentPivotFormat(
        consolidatedBuffer,
        actualFormat,
        universalConfig,
        dataSource.name,
        primaryFile.originalname,
      );
      headers = pivotResult.headers;
      rows = pivotResult.rows;
      items = pivotResult.items;

      // If pivot parser returned 0 items and auto-detection didn't confirm the format,
      // the saved formatType is wrong for this file. Fall back to row-based parsing
      // and correct the saved format so future imports don't repeat this.
      if (items.length === 0 && !detectedPivotFormat) {
        console.log(`${logPrefix} Pivot parser returned 0 items and auto-detection didn't confirm format — falling back to row parser`);
        if (source === "ai_import") {
          const { parseWithEnhancedConfig } = await import("./enhancedImportProcessor");
          const parseResult = await parseWithEnhancedConfig(
            primaryFile.buffer,
            {
              formatType: "row",
              columnMapping: dsConfig.columnMapping,
              pivotConfig: dsConfig.pivotConfig,
              discontinuedConfig: dsConfig.discontinuedConfig,
              futureStockConfig: dsConfig.futureStockConfig,
              stockValueConfig: dsConfig.stockValueConfig,
              cleaningConfig,
            },
            dataSourceId,
          );
          if (parseResult.success && parseResult.items.length > 0) {
            items = parseResult.items;
            rows = rawData;
            console.log(`${logPrefix} Row parser fallback found ${items.length} items — correcting saved format to "row"`);
            await storage.updateDataSource(dataSourceId, {
              formatType: "row",
              pivotConfig: null,
            });
          }
        } else {
          const { parseExcelToInventory } = await import("./importUtils");
          const result = (parseExcelToInventory as any)(
            consolidatedBuffer,
            dsConfig.columnMapping,
            cleaningConfig,
          );
          if (result.items?.length > 0) {
            headers = result.headers;
            rows = result.rows;
            items = result.items;
            console.log(`${logPrefix} Row parser fallback found ${items.length} items — correcting saved format to "row"`);
            await storage.updateDataSource(dataSourceId, {
              formatType: "row",
              pivotConfig: null,
            });
          }
        }
      }

      // Save detected format for future imports
      if (detectedPivotFormat) {
        await storage.updateDataSource(dataSourceId, {
          formatType: detectedPivotFormat,
          pivotConfig: { enabled: true, format: detectedPivotFormat },
        });
      }
    } else if (dsConfig.pivotConfig?.enabled) {
      // Legacy pivoted parser
      console.log(`${logPrefix} Using legacy pivoted table parser`);
      const result = parsePivotedExcelToInventory(
        consolidatedBuffer,
        dsConfig.pivotConfig,
        cleaningConfig,
        dataSource.name,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
    } else if (source === "ai_import") {
      // AI import with row format — use enhanced parser
      const { parseWithEnhancedConfig } = await import("./enhancedImportProcessor");
      const parseResult = await parseWithEnhancedConfig(
        primaryFile.buffer,
        {
          formatType: dsConfig.formatType || "row",
          columnMapping: dsConfig.columnMapping,
          pivotConfig: dsConfig.pivotConfig,
          discontinuedConfig: dsConfig.discontinuedConfig,
          futureStockConfig: dsConfig.futureStockConfig,
          stockValueConfig: dsConfig.stockValueConfig,
          cleaningConfig,
        },
        dataSourceId,
      );
      if (!parseResult.success) {
        return { success: false, itemCount: 0, error: "Failed to parse file" };
      }
      items = parseResult.items;
      rows = rawData;
    } else {
      // Generic row parser (routes parseExcelToInventory)
      const { parseExcelToInventory } = await import("./importUtils");
      const result = (parseExcelToInventory as any)(
        consolidatedBuffer,
        dsConfig.columnMapping,
        cleaningConfig,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
    }

    console.log(`${logPrefix} Parsed ${items.length} items`);
  } else {
    return { success: false, itemCount: 0, error: "No file buffers or pre-consolidated items provided" };
  }

  // PHASE 1, Step 4: Apply cleaning to style field
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
    if (hasAnyCleaning && !preConsolidatedItems) {
      // Skip for pre-consolidated items (combine path applies cleaning inline during extraction)
      console.log(`${logPrefix} Applying cleaning rules to ${items.length} items`);
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

  if (items.length === 0 && !preConsolidatedItems) {
    return { success: false, itemCount: 0, error: "File contains no valid data rows" };
  }

  // ──────────────────────────────────────────────────────────
  // PHASE 2: FILTER
  // ──────────────────────────────────────────────────────────

  // PHASE 2, Step 5: Skip rule filtering (shouldSkip flag)
  // FROM: routes (29).ts line 4447 — applies to upload, email, fetch-url
  const continueSelling = (dataSource as any).continueSelling ?? true;
  const beforeSkip = items.length;
  items = items.filter((item: any) => {
    if (item.shouldSkip) {
      if (item.skipUnlessContinueSelling && continueSelling) {
        return true;
      }
      return false;
    }
    return true;
  });
  if (items.length < beforeSkip) {
    console.log(`${logPrefix} Skip rule filtered out ${beforeSkip - items.length} items`);
  }

  // PHASE 2, Step 6: Filter discontinued zero-stock items
  // FROM: routes (29).ts line 4467
  const discontinuedZeroStock = items.filter(
    (item: any) => item.discontinued === true && item.stock === 0,
  );
  if (discontinuedZeroStock.length > 0) {
    const beforeFilter = items.length;
    items = items.filter((item: any) => {
      if (!(item.discontinued === true && item.stock === 0)) return true;
      if (item.hasFutureStock || item.preserveZeroStock || item.shipDate) return true;
      return false;
    });
    const actuallyFiltered = beforeFilter - items.length;
    if (actuallyFiltered > 0) {
      console.log(
        `${logPrefix} Filtered out ${actuallyFiltered} discontinued items with zero stock`,
      );
    }
  }

  // PHASE 2, Step 7: Dedup by style-color-size & zero out stock for future ship dates
  // *** THIS FIXES THE BUG: processUrlDataSourceImport was missing this call ***
  const dedupOffset =
    overrideConfig?.stockInfoConfig?.dateOffsetDays ??
    (dataSource as any).stockInfoConfig?.dateOffsetDays ?? 0;
  const dedupResult = deduplicateAndZeroFutureStock(items, dedupOffset);
  items = dedupResult.items;
  console.log(`${logPrefix} After dedup: ${items.length} items (removed ${dedupResult.duplicatesRemoved || 0} dupes)`);

  // ──────────────────────────────────────────────────────────
  // PHASE 3: TRANSFORM
  // ──────────────────────────────────────────────────────────

  // PHASE 3, Step 8: Apply style prefix
  // FROM: routes (29).ts line 4397 (canonical version)
  // Skip if combine path already applied prefix during extraction
  if (!preConsolidatedItems) {
    items = items.map((item: any) => {
      const rawStyle = String(item.style || "").trim();
      const prefix = item.brand
        ? String(item.brand).trim()
        : rawStyle
          ? getStylePrefix(rawStyle, dataSource, cleaningConfig)
          : dataSource.name;
      const prefixedStyle = rawStyle ? `${prefix} ${rawStyle}` : rawStyle;
      const normalizedColor = item.color ? toTitleCase(item.color) : item.color;
      const prefixedSku =
        prefixedStyle && normalizedColor && item.size != null && item.size !== ""
          ? `${prefixedStyle}-${normalizedColor}-${item.size}`
              .replace(/\//g, "-")
              .replace(/\s+/g, "-")
              .replace(/-+/g, "-")
          : (item.sku || "").replace(/\//g, "-").replace(/-+/g, "-");
      return {
        ...item,
        style: prefixedStyle,
        sku: prefixedSku,
        dataSourceId,
        fileId: externalFileId || null,
        hasFutureStock: item.hasFutureStock || false,
        preserveZeroStock: item.preserveZeroStock || false,
        discontinued: item.discontinued || false,
      };
    });
  }

  // PHASE 3, Step 9: cleanInventoryData (AI color fixes, remove no-size items)
  // FROM: routes (29).ts line 4556
  const cleanResult = await cleanInventoryData(items, dataSource.name);
  let processedItems = cleanResult.items;

  // PHASE 3, Step 10: applyImportRules (pricing, dates, discontinued, etc.)
  // FROM: routes (29).ts line 4562
  const importRulesConfig = {
    discontinuedRules:
      overrideConfig?.discontinuedConfig ||
      overrideConfig?.discontinuedRules ||
      (dataSource as any).discontinuedConfig ||
      (dataSource as any).discontinuedRules,
    salePriceConfig:
      overrideConfig?.salePriceConfig ||
      overrideConfig?.columnSaleConfig ||
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
    cleaningConfig: overrideConfig?.cleaningConfig || (dataSource as any).cleaningConfig,
    futureStockConfig:
      overrideConfig?.futureStockConfig ||
      (dataSource as any).futureStockConfig,
    stockValueConfig:
      overrideConfig?.stockValueConfig ||
      (dataSource as any).stockValueConfig ||
      (dataSource.cleaningConfig?.stockTextMappings?.length > 0
        ? { textMappings: dataSource.cleaningConfig.stockTextMappings }
        : undefined),
    complexStockConfig:
      overrideConfig?.complexStockConfig ||
      (dataSource as any).complexStockConfig,
  };
  const importRulesResult = await applyImportRules(
    cleanResult.items,
    importRulesConfig,
    rows.length > 0 ? rows : rawData,
  );
  processedItems = importRulesResult.items;
  console.log(`${logPrefix} After import rules: ${processedItems.length} items`);

  // PHASE 3, Step 11: Global color mappings
  // FROM: aiImportRoutes (6).ts line 4202 (executeAIImport Step 5)
  // Applied for ALL paths (was previously AI-only)
  let colorsFixed = cleanResult.colorsFixed || 0;
  try {
    const colorMappings = await storage.getColorMappings();
    const colorMap = new Map<string, string>();
    for (const mapping of colorMappings) {
      const normalizedBad = mapping.badColor.trim().toLowerCase();
      colorMap.set(normalizedBad, mapping.goodColor);
    }
    if (colorMap.size > 0) {
      processedItems = processedItems.map((item: any) => {
        const color = String(item.color || "").trim();
        const normalizedColor = color.toLowerCase();
        const mappedColor = colorMap.get(normalizedColor);
        if (mappedColor && mappedColor.toLowerCase() !== normalizedColor) {
          colorsFixed++;
          const newColor = formatColorName(mappedColor);
          const newSku =
            item.style && item.size
              ? `${item.style}-${newColor}-${item.size}`
                  .replace(/\//g, "-")
                  .replace(/\s+/g, "-")
                  .replace(/-+/g, "-")
              : item.sku;
          return { ...item, color: newColor, sku: newSku };
        }
        return { ...item, color: formatColorName(color) };
      });
      if (colorsFixed > cleanResult.colorsFixed) {
        console.log(`${logPrefix} Fixed ${colorsFixed - (cleanResult.colorsFixed || 0)} colors using global mappings`);
      }
    }
  } catch (colorMapError: any) {
    console.error(`${logPrefix} Error applying color mappings:`, colorMapError);
  }

  // PHASE 3, Step 12: applyVariantRules (size expansion/filter)
  // FROM: routes (29).ts line 4586
  const variantRulesConfigOverride =
    overrideConfig?.filterZeroStock !== undefined
      ? {
          filterZeroStock: overrideConfig.filterZeroStock,
          filterZeroStockWithFutureDates: overrideConfig?.filterZeroStockWithFutureDates,
        }
      : undefined;
  const ruleResult = await applyVariantRules(
    processedItems,
    dataSourceId,
    variantRulesConfigOverride,
  );
  processedItems = ruleResult.items;
  console.log(`${logPrefix} After variant rules: ${processedItems.length} items`);

  // PHASE 3, Step 13: applyPriceBasedExpansion
  // FROM: routes (29).ts line 4591
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
    const shopifyStoreId = (dataSource as any).shopifyStoreId;
    if (shopifyStoreId) {
      try {
        const cacheVariants =
          await storage.getVariantCacheProductStyles(shopifyStoreId);
        const stylePriceMap = buildStylePriceMapFromCache(cacheVariants);
        const expansionResult = applyPriceBasedExpansion(
          ruleResult.items,
          priceBasedExpansionConfig,
          stylePriceMap,
          sizeLimitConfig,
        );
        processedItems = expansionResult.items;
        priceBasedExpansionCount = expansionResult.addedCount;
        if (priceBasedExpansionCount > 0) {
          console.log(`${logPrefix} Price-based expansion added ${priceBasedExpansionCount} size variants`);
        }
      } catch (expansionError) {
        console.error(`${logPrefix} Price-based expansion error:`, expansionError);
      }
    }
  }

  // ──────────────────────────────────────────────────────────
  // PHASE 4: BUSINESS LOGIC
  // ──────────────────────────────────────────────────────────

  // PHASE 4, Step 14: filterDiscontinuedStyles (sale file cross-reference)
  // FROM: routes (29).ts line 4646
  const linkedSaleDataSourceId = (dataSource as any).assignedSaleDataSourceId;
  let discontinuedStylesFiltered = 0;
  let discontinuedItemsRemoved = 0;
  let saleStylesRegistered = 0;

  if (!isSaleFile && linkedSaleDataSourceId) {
    try {
      discontinuedItemsRemoved = await removeDiscontinuedInventoryItems(
        dataSourceId,
        linkedSaleDataSourceId,
      );
      if (discontinuedItemsRemoved > 0) {
        console.log(`${logPrefix} Removed ${discontinuedItemsRemoved} existing items with discontinued styles`);
      }
      const filterResult = await filterDiscontinuedStyles(
        dataSourceId,
        processedItems,
        linkedSaleDataSourceId,
      );
      processedItems = filterResult.items;
      discontinuedStylesFiltered = filterResult.removedCount;
      if (discontinuedStylesFiltered > 0) {
        console.log(`${logPrefix} Filtered out ${discontinuedStylesFiltered} discontinued items`);
      }
    } catch (discontinuedError) {
      console.error(`${logPrefix} Discontinued filtering error:`, discontinuedError);
    }
  }

  // PHASE 4, Step 15: Sale file pricing (Shopify compare-at price)
  // FROM: routes (29).ts line 4689
  const shopifyStoreIdForCompareAt = (dataSource as any).shopifyStoreId;
  const salesConfig = (dataSource as any).salesConfig || {
    priceMultiplier: 2,
    useCompareAtPrice: true,
  };
  const priceMultiplier = salesConfig.priceMultiplier || 2;
  const useCompareAtPrice = salesConfig.useCompareAtPrice ?? true;

  if (shopifyStoreIdForCompareAt && useCompareAtPrice && isSaleFile) {
    try {
      const skus = processedItems
        .map((item: any) => item.sku)
        .filter((sku: string | null) => sku && sku.trim());
      if (skus.length > 0) {
        const cachedVariants = await storage.getVariantCacheBySKUs(
          shopifyStoreIdForCompareAt,
          skus,
        );
        const shopifyPriceMap = new Map<string, string>();
        for (const v of cachedVariants) {
          if (v.sku && v.price) {
            shopifyPriceMap.set(v.sku.trim().toUpperCase(), v.price);
          }
        }

        if (shopifyPriceMap.size > 0) {
          processedItems = processedItems.map((item: any) => {
            const basePrice = parseFloat(item.price || "0");
            let finalPrice = item.price;
            let cost = item.cost || null;
            if (basePrice > 0) {
              finalPrice = (basePrice * priceMultiplier).toFixed(2);
              if (item.sku && useCompareAtPrice) {
                const shopifyPrice = shopifyPriceMap.get(
                  item.sku.trim().toUpperCase(),
                );
                if (shopifyPrice) cost = shopifyPrice;
              }
            }
            return { ...item, price: finalPrice, cost };
          });
          console.log(
            `${logPrefix} Applied sale pricing: ${priceMultiplier}x multiplier, ${shopifyPriceMap.size} compare-at prices`,
          );
        }
      }
    } catch (err) {
      console.error(`${logPrefix} Error loading Shopify prices:`, err);
    }
  }

  // PHASE 4, Step 16: calculateStockInfo
  // FROM: routes (29).ts line 4773
  const stockInfoRule = await getStockInfoRule(dataSource, overrideConfig);
  if (stockInfoRule) {
    console.log(`${logPrefix} Calculating stockInfo for ${processedItems.length} items`);
    processedItems = processedItems.map((item: any) => ({
      ...item,
      stockInfo: calculateItemStockInfo(item, stockInfoRule),
    }));
  }

  // ──────────────────────────────────────────────────────────
  // PHASE 5: SAVE
  // ──────────────────────────────────────────────────────────

  // PHASE 5, Step 17: Safety nets
  const updateStrategy = (dataSource as any).updateStrategy || "full_sync";
  let importedCount = 0;
  let addedCount = 0;
  let updatedCount = 0;

  if (processedItems.length > 0) {
    if (updateStrategy === "full_sync") {
      const existingCount =
        await storage.getInventoryItemCountByDataSource(dataSourceId);
      const safetyCheck = checkSafetyThreshold(
        dataSource,
        existingCount,
        processedItems.length,
        source,
      );
      if (safetyCheck.blocked) {
        return {
          success: false,
          itemCount: 0,
          error: safetyCheck.message,
          safetyBlock: true,
        };
      }
      // PHASE 5, Step 18a: Atomic replace
      console.log(`${logPrefix} Atomic replace with ${processedItems.length} items`);
      const result = await storage.atomicReplaceInventoryItems(
        dataSourceId,
        processedItems,
      );
      importedCount = result.created;
      console.log(`${logPrefix} Atomic replace complete: deleted ${result.deleted}, created ${result.created}`);
    } else {
      // PHASE 5, Step 18b: Upsert
      console.log(`${logPrefix} Upserting ${processedItems.length} items`);
      const isRegularInventory = (dataSource as any).sourceType !== "sales";
      const result = await storage.upsertInventoryItems(
        processedItems,
        dataSourceId,
        { resetSaleFlags: isRegularInventory },
      );
      addedCount = result.added;
      updatedCount = result.updated;
      importedCount = addedCount + updatedCount;
    }
  } else if (updateStrategy === "full_sync") {
    // 0-item safety net
    const existingCount =
      await storage.getInventoryItemCountByDataSource(dataSourceId);
    if (existingCount > 0) {
      return {
        success: false,
        itemCount: 0,
        error: `SAFETY NET: 0 items but would delete ${existingCount} existing. Import blocked.`,
        safetyBlock: true,
      };
    }
  }

  // PHASE 5, Step 19: Save file record
  const fileRecord = await storage.createUploadedFile({
    dataSourceId,
    fileName:
      fileBuffers && fileBuffers.length > 1
        ? `${fileBuffers.length} files consolidated`
        : fileBuffers?.[0]?.originalname || "import",
    fileSize: fileBuffers?.[0]?.buffer?.length || 0,
    fileStatus: "imported",
    rowCount: processedItems.length,
    headers: headers.length > 0 ? headers : undefined,
  });
  if (onFileRecord) onFileRecord(fileRecord);

  // PHASE 5, Step 20: Post-import hooks
  // 20a: Register sale file styles
  if (isSaleFile && processedItems.length > 0) {
    try {
      const styleResult = await registerSaleFileStyles(
        dataSourceId,
        processedItems,
      );
      saleStylesRegistered = styleResult.total;
      console.log(`${logPrefix} Registered ${saleStylesRegistered} sale file styles`);
    } catch (err) {
      console.error(`${logPrefix} Error registering sale file styles:`, err);
    }
  }

  // 20b: Update lastSync
  await storage.updateDataSource(dataSourceId, {});

  console.log(`${logPrefix} DONE: ${importedCount} items saved for "${dataSource.name}"`);

  return {
    success: true,
    itemCount: importedCount,
    fileId: fileRecord.id,
    headers,
    stats: {
      totalParsed: preConsolidatedItems?.length || items.length,
      afterClean: cleanResult.items.length,
      afterImportRules: importRulesResult.items.length,
      afterVariantRules: ruleResult.items.length,
      afterPriceExpansion: processedItems.length,
      afterDiscontinuedFilter: processedItems.length,
      finalCount: importedCount,
      noSizeRemoved: cleanResult.noSizeRemoved || 0,
      colorsFixed,
      aiColorsFixed: cleanResult.aiColorsFixed || 0,
      duplicatesRemoved: cleanResult.duplicatesRemoved || 0,
      priceBasedExpansion: priceBasedExpansionCount,
      discontinuedStylesFiltered,
      discontinuedItemsRemoved,
      saleStylesRegistered,
      variantRulesAdded: ruleResult.addedCount || 0,
      variantRulesFiltered: ruleResult.filteredCount || 0,
      variantRulesSizeFiltered: ruleResult.sizeFiltered || 0,
      importRulesStats: importRulesResult.stats || {},
      dedupStats: dedupResult,
    },
  };
}

// ============================================================
// RE-EXPORTS for backward compatibility
// ============================================================

export { autoDetectPivotFormat, parseIntelligentPivotFormat };
export type { UniversalParserConfig };
