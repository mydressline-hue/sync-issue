import type { Express } from "express";
import { createServer, type Server } from "http";
import multer from "multer";
import * as XLSX from "xlsx";
import crypto from "crypto";
import fs from "fs";
import os from "os";
import path from "path";
import { storage } from "./storage";
import { db } from "./db";
import { eq, and, count, desc } from "drizzle-orm";
import {
  analyzeExcelForMapping,
  suggestColorCorrections,
  openai,
  analyzeImagesInParallel,
} from "./openai";
import {
  insertDataSourceSchema,
  insertUploadedFileSchema,
  insertInventoryItemSchema,
  insertVariantRuleSchema,
  insertShopifyMetafieldRuleSchema,
  insertChannelIntegrationSchema,
  insertShopifyStoreSchema,
  ebayVendorTemplates,
  uploadedFiles,
  inventoryItems,
  importLogs,
} from "@shared/schema";
import {
  ShopifyService,
  connectShopifyStore,
  syncInventoryToShopify,
  syncInventoryToShopifySequential,
  syncSalesFileToShopify,
  fetchShopifyProducts,
  createShopifyService,
  generateSyncPreview,
  cancelSyncPreview,
  isSyncPreviewRunning,
  getSyncPreviewProgress,
  getCacheRefreshProgress,
  getLastCacheRefreshStats,
  getHardResetProgress,
  isCacheRefreshRunning,
  refreshVariantCacheForStore,
  cancelCacheRefresh,
  isInventorySyncActive,
  cancelInventorySync,
  clearAllSyncLocks,
  clearStaleSyncLocks,
} from "./shopify";
import { triggerShopifySyncAfterImport } from "./scheduler";
import { registerVendorImportRoutes } from "./vendorImportRoutes";
import aiImportRoutes, {
  autoDetectPivotFormat,
  parseIntelligentPivotFormat,
  UniversalParserConfig,
} from "./aiImportRoutes";
import {
  setupAuth,
  isAuthenticated,
  requireApproved,
  requireAdmin,
} from "./replitAuth";
import {
  LETTER_SIZES,
  LETTER_SIZE_MAP,
  NUMERIC_SIZES,
  NUMERIC_SIZE_MAP,
  getSizeRank,
  isSizeAllowed,
  SizeLimitConfig,
} from "./sizeUtils";
import {
  generateTemplate,
  type TemplateType,
  type EbayTemplateData,
} from "@shared/ebayTemplates";
import {
  consolidateSaleIntoRegular,
  triggerConsolidationForSaleSource,
  cleanupSaleOwnedItemsFromRegular,
} from "./consolidation";
import {
  startComparisonJob,
  getComparisonJobStatus,
  abortComparisonJob,
} from "./sync/comparisonService";
import { validateImportFile, logValidationFailure } from "./importValidator";
import {
  parsePivotedExcelToInventory,
  parseGenericPivotFormat,
  parseOTSFormat,
  isOTSFormat,
  parseGRNInvoiceFormat,
  parsePRDateHeaderFormat,
  parseStoreMultibrandFormat,
  registerSaleFileStyles,
  filterDiscontinuedStyles,
  removeDiscontinuedInventoryItems,
  checkSaleImportFirstRequirement,
  applyCleaningToValue,
} from "./importUtils";
import {
  cleanInventoryData,
  applyVariantRules,
  isColorCode,
  formatColorName,
  applyImportRules,
  applyPriceBasedExpansion,
  buildStylePriceMapFromCache,
  deduplicateAndZeroFutureStock,
} from "./inventoryProcessing";
import { startImport, completeImport, failImport } from "./importState";
import { registerGlobalValidatorRoutes } from "./globalValidator";
import { executeImport, calculateItemStockInfo, getStockInfoRule, getStylePrefix, checkSafetyThreshold, toTitleCase } from "./importEngine";
export { getSizeRank };

// ============================================================
// CSV DETECTION AND PARSING HELPERS
// Prevents scientific notation conversion for style numbers like "1921E0136"
// ============================================================

// Detect if buffer is a CSV file (not Excel)
function isCSVBuffer(buffer: Buffer): boolean {
  // Check for Excel magic bytes (PK for xlsx, D0 CF for xls)
  if (buffer.length >= 4) {
    // XLSX files start with PK (0x50 0x4B)
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      return false;
    }
    // XLS files start with D0 CF 11 E0
    if (
      buffer[0] === 0xd0 &&
      buffer[1] === 0xcf &&
      buffer[2] === 0x11 &&
      buffer[3] === 0xe0
    ) {
      return false;
    }
  }

  // Check for UTF-16 BOM (some CSV files use this)
  const hasUTF16BOM =
    buffer.length >= 2 &&
    ((buffer[0] === 0xff && buffer[1] === 0xfe) ||
      (buffer[0] === 0xfe && buffer[1] === 0xff));

  // Sample first 1000 bytes to check if it looks like CSV
  let sampleText: string;
  if (hasUTF16BOM) {
    // UTF-16 LE
    if (buffer[0] === 0xff && buffer[1] === 0xfe) {
      sampleText = buffer.slice(2, 1000).toString("utf16le");
    } else {
      // UTF-16 BE - swap bytes
      const swapped = Buffer.alloc(Math.min(998, buffer.length - 2));
      for (let i = 2; i < Math.min(1000, buffer.length) - 1; i += 2) {
        swapped[i - 2] = buffer[i + 1];
        swapped[i - 1] = buffer[i];
      }
      sampleText = swapped.toString("utf16le");
    }
  } else {
    sampleText = buffer.slice(0, 1000).toString("utf8");
  }

  // CSV characteristics: has commas/tabs, has newlines, printable text
  const hasDelimiters = sampleText.includes(",") || sampleText.includes("\t");
  const hasNewlines = sampleText.includes("\n") || sampleText.includes("\r");
  const isPrintable = /^[\x09\x0A\x0D\x20-\x7E\u00A0-\uFFFF]*$/.test(
    sampleText.replace(/[\r\n]/g, ""),
  );

  return hasDelimiters && hasNewlines && isPrintable;
}

// Parse CSV buffer to array of arrays (preserving all values as strings)
function parseCSVAsText(buffer: Buffer): any[][] {
  let text: string;

  // Check for UTF-16 BOM
  const hasUTF16LE =
    buffer.length >= 2 && buffer[0] === 0xff && buffer[1] === 0xfe;
  const hasUTF16BE =
    buffer.length >= 2 && buffer[0] === 0xfe && buffer[1] === 0xff;

  if (hasUTF16LE) {
    text = buffer.slice(2).toString("utf16le");
  } else if (hasUTF16BE) {
    // Swap bytes for BE
    const swapped = Buffer.alloc(buffer.length - 2);
    for (let i = 2; i < buffer.length - 1; i += 2) {
      swapped[i - 2] = buffer[i + 1];
      swapped[i - 1] = buffer[i];
    }
    text = swapped.toString("utf16le");
  } else {
    // Check for UTF-8 BOM
    if (
      buffer.length >= 3 &&
      buffer[0] === 0xef &&
      buffer[1] === 0xbb &&
      buffer[2] === 0xbf
    ) {
      text = buffer.slice(3).toString("utf8");
    } else {
      text = buffer.toString("utf8");
    }
  }

  // Detect delimiter (comma or tab)
  const firstLine = text.split(/\r?\n/)[0] || "";
  const commaCount = (firstLine.match(/,/g) || []).length;
  const tabCount = (firstLine.match(/\t/g) || []).length;
  const delimiter = tabCount > commaCount ? "\t" : ",";

  // Parse CSV manually to preserve all values as strings
  const rows: any[][] = [];
  const lines = text.split(/\r?\n/);

  for (const line of lines) {
    if (!line.trim()) continue;

    const row: string[] = [];
    let current = "";
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];

      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"';
          i++; // Skip escaped quote
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === delimiter && !inQuotes) {
        row.push(current.trim());
        current = "";
      } else {
        current += char;
      }
    }
    row.push(current.trim()); // Don't forget last field
    rows.push(row);
  }

  return rows;
}

// Auto-consolidation after import: handles both regular and sale file scenarios
export async function triggerAutoConsolidationAfterImport(
  dataSourceId: string,
): Promise<void> {
  try {
    // Always deduplicate inventory items first to clean up any duplicates from this import
    const duplicatesRemoved =
      await storage.deduplicateInventoryItems(dataSourceId);
    if (duplicatesRemoved > 0) {
      console.log(
        `[Post-Import] Removed ${duplicatesRemoved} duplicate items for data source ${dataSourceId}`,
      );
    }

    const dataSource = await storage.getDataSource(dataSourceId);
    if (!dataSource) return;

    // Case 1: Regular data source with an assigned sale source
    // Consolidate sale items into regular so they're included in sync
    if (dataSource.assignedSaleDataSourceId) {
      console.log(
        `[Auto-Consolidation] Regular source "${dataSource.name}" has sale file assigned - consolidating`,
      );
      const consolidationResult = await consolidateSaleIntoRegular(
        dataSourceId,
        dataSource.assignedSaleDataSourceId,
      );
      console.log(
        `[Auto-Consolidation] Consolidated ${consolidationResult.saleVariantsAdded} sale variants into regular source`,
      );
      return;
    }

    // Case 2: Sale-type data source - trigger consolidation for linked regular sources
    if (dataSource.sourceType === "sales") {
      console.log(
        `[Auto-Consolidation] Sale file "${dataSource.name}" imported - triggering consolidation`,
      );
      await triggerConsolidationForSaleSource(dataSourceId);
    }
  } catch (error: any) {
    console.error(
      `[Auto-Consolidation] Error during auto-consolidation for ${dataSourceId}:`,
      error.message,
    );
  }
}

// Shared URL import processing function - used by both manual route and scheduler
export async function processUrlDataSourceImport(
  dataSourceId: string,
  buffer: Buffer,
  filename: string,
): Promise<{
  success: boolean;
  itemCount?: number;
  noSizeRemoved?: number;
  colorsFixed?: number;
  duplicatesRemoved?: number;
  error?: string;
  headers?: string[];
}> {
  try {
    const dataSource = await storage.getDataSource(dataSourceId);
    if (!dataSource) {
      return { success: false, error: "Data source not found" };
    }

    // Check if sale file import is required first
    const saleImportCheck = await checkSaleImportFirstRequirement(dataSourceId);
    if (saleImportCheck.requiresWarning) {
      console.log(`[URL Import] SKIPPED: ${dataSource.name} - ${saleImportCheck.warningMessage}`);
      return { success: false, error: `Sale file not imported: ${saleImportCheck.warningMessage}` };
    }

    // SAFETY NET: Validate file before import
    const validationConfig = (dataSource as any).importValidationConfig || {};
    if (validationConfig.enabled !== false) {
      const validation = await validateImportFile(buffer, dataSourceId, filename);
      if (!validation.valid) {
        await logValidationFailure(dataSourceId, filename, validation.errors, validation.warnings);
        return { success: false, error: `SAFETY NET: Import blocked - ${validation.errors.join("; ")}` };
      }
    }

    // Use unified import engine (FIXES BUG: now includes deduplicateAndZeroFutureStock)
    const result = await executeImport({
      fileBuffers: [{ buffer, originalname: filename }],
      dataSourceId,
      source: 'url',
      dataSource,
    });

    if (!result.success) {
      return { success: false, error: result.error };
    }

    return {
      success: true,
      itemCount: result.itemCount,
      noSizeRemoved: result.stats?.noSizeRemoved,
      colorsFixed: result.stats?.colorsFixed,
      duplicatesRemoved: result.stats?.duplicatesRemoved,
      headers: result.headers,
    };
  } catch (error: any) {
    console.error("[URL Import] Error:", error);
    return { success: false, error: error.message || "URL import failed" };
  }
}

// Configure multer for file uploads — disk storage to avoid OOM from large files in RAM
const uploadDir = path.join(os.tmpdir(), "uploads");
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
const _multer = multer({
  storage: multer.diskStorage({
    destination: uploadDir,
    filename: (_req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`),
  }),
  limits: { fileSize: 50 * 1024 * 1024 },
});

// Middleware: lazily load file.buffer from disk so existing handlers work unchanged
// Buffer is read on first access, then cached. Temp file is cleaned up when response finishes.
function loadBufferFromDisk(req: any, _res: any, next: any) {
  const patchFile = (file: any) => {
    if (file && file.path && !file.buffer) {
      let _buf: Buffer | null = null;
      Object.defineProperty(file, "buffer", {
        get() {
          if (!_buf) _buf = fs.readFileSync(file.path);
          return _buf;
        },
        configurable: true,
      });
    }
  };
  if (req.file) patchFile(req.file);
  if (Array.isArray(req.files)) req.files.forEach(patchFile);
  // Clean up temp files when response finishes
  _res.on("finish", () => {
    const files = req.file ? [req.file] : Array.isArray(req.files) ? req.files : [];
    for (const f of files) {
      if (f?.path) fs.unlink(f.path, () => {});
    }
  });
  next();
}

// Wrap multer methods to auto-chain disk→buffer middleware
const upload = {
  single: (field: string) => [_multer.single(field), loadBufferFromDisk],
  array: (field: string, maxCount?: number) => [_multer.array(field, maxCount), loadBufferFromDisk],
  any: () => [_multer.any(), loadBufferFromDisk],
  fields: (fields: any) => [_multer.fields(fields), loadBufferFromDisk],
};

// Helper function to detect and parse Tarik Ediz pivoted format
function parseTarikEdizFormat(
  buffer: Buffer,
): { headers: string[]; rows: any[][]; items: any[] } | null {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    raw: false,
  }) as any[][]; // CRITICAL FIX: Match Email import raw: false for consistent parsing

  if (data.length === 0) return null;

  // Detect Tarik Ediz format: First row contains "Up-to-Date Product Inventory Report"
  // or company name contains "EDİZ" or "EDIZ"
  const firstRowText = String(data[0]?.[0] || "").toLowerCase();
  const secondRowText = String(data[1]?.[0] || "").toLowerCase();

  const isTarikEdizFormat =
    firstRowText.includes("up-to-date product inventory") ||
    firstRowText.includes("inventory report") ||
    secondRowText.includes("ediz") ||
    secondRowText.includes("edi̇z");

  if (!isTarikEdizFormat) return null;

  console.log("Detected Tarik Ediz pivoted format - applying special parsing");

  // Parse the pivoted format
  const masterData: any[][] = [];
  const headers = ["style", "color", "size", "stock", "shipDate"];

  let currentStyle = "";
  let sizeHeaders: { index: number; size: string }[] = [];

  // Helper to detect if first cell is a date (DD/MM/YYYY or similar)
  const isDateString = (val: string): boolean => {
    // Match patterns like 24/03/2026, 2026-03-24, etc.
    return (
      /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(val) ||
      /^\d{4}-\d{2}-\d{2}$/.test(val) ||
      /^\d{1,2}-\d{1,2}-\d{4}$/.test(val)
    );
  };

  // Helper to parse date string to ISO format
  const parseDateToISO = (val: string): string | null => {
    // DD/MM/YYYY format (European)
    const ddmmyyyy = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (ddmmyyyy) {
      const [, day, month, year] = ddmmyyyy;
      return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
    }
    // Already ISO format
    if (/^\d{4}-\d{2}-\d{2}$/.test(val)) {
      return val;
    }
    return null;
  };

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    if (!row || row.length === 0) continue;

    const firstCell = String(row[0] || "").trim();

    // Product header rows have sizes like '0', '2', '4', etc. starting around column 13
    // and have a product name in column 7
    if (row[13] && String(row[13]).match(/^\d+$/) && row[7]) {
      currentStyle = firstCell;

      // Extract size headers from this row
      sizeHeaders = [];
      for (let j = 13; j < row.length - 1; j++) {
        if (row[j] !== null && row[j] !== undefined && row[j] !== "") {
          sizeHeaders.push({ index: j, size: String(row[j]) });
        }
      }
      continue;
    }

    // Data rows can start with:
    // - 'D' for current inventory
    // - A date string (e.g., '24/03/2026') for future ship dates
    // Both have color in column 11
    const isCurrentStock = firstCell === "D";
    const isFutureShipDate = isDateString(firstCell);

    if ((isCurrentStock || isFutureShipDate) && row[11] && currentStyle) {
      const color = String(row[11]).trim();
      const shipDate = isFutureShipDate ? parseDateToISO(firstCell) : null;

      // Extract stock values for each size
      for (const sh of sizeHeaders) {
        const stock = row[sh.index];
        const stockNum =
          stock !== null && stock !== undefined && !isNaN(Number(stock))
            ? Number(stock)
            : 0;

        // CRITICAL FIX: Include ALL items, not just stock > 0
        // Items with future ship dates should be included even with 0 current stock
        // The futureStockConfig will handle preserving these based on ship date
        if (stockNum > 0 || shipDate) {
          masterData.push([currentStyle, color, sh.size, stockNum, shipDate]);
        }
      }
    }
  }

  console.log(
    `[TarikEdiz] Parsed ${masterData.length} items (including future ship date items with 0 stock)`,
  );

  // Convert to items format
  const items = masterData.map((row) => ({
    sku: row[0],
    style: row[0],
    color: row[1],
    size: row[2],
    stock: row[3],
    shipDate: row[4] || null,
    // CRITICAL: Set hasFutureStock flag for items with future ship dates
    // This prevents them from being filtered by stock threshold
    hasFutureStock: row[4] ? true : false,
    preserveZeroStock: row[4] && row[3] === 0 ? true : false,
    rawData: {
      style: row[0],
      color: row[1],
      size: row[2],
      stock: row[3],
      shipDate: row[4],
    },
  }));

  return {
    headers,
    rows: masterData,
    items,
  };
}

// Helper function to detect and parse Jovani pivoted format
// Format: Style rows have style/price, color rows have color/stock values
// Row 1: [null, "00", 0, 2, 4, ...] - size headers
// Style row: ["#02861", 175, ...] - style in col 0, price in col 1
// Color row: ["Taupe-Off-White", 1, 1, 1, ...] - color in col 0, stock values
function parseJovaniFormat(
  buffer: Buffer,
  cleaningConfig: any,
): { headers: string[]; rows: any[][]; items: any[] } | null {
  // Check if Jovani format is explicitly enabled
  if (cleaningConfig?.pivotedFormat?.vendor !== "jovani") return null;

  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    raw: false,
  }) as any[][]; // CRITICAL FIX: Match Email import raw: false for consistent parsing

  if (data.length === 0) return null;

  console.log("Parsing Jovani pivoted format");

  // Row 0 contains size headers starting at column 1
  // Format: [null, "00", 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, "LOCATION"]
  const headerRow = data[0];
  const sizeHeaders: { index: number; size: string }[] = [];

  for (let i = 1; i < headerRow.length; i++) {
    const cell = headerRow[i];
    const cellStr = String(cell ?? "")
      .trim()
      .toUpperCase();
    // Skip non-size columns like "LOCATION"
    if (cellStr === "LOCATION" || cellStr === "") continue;
    // Size values are typically numbers or "00"
    sizeHeaders.push({ index: i, size: String(cell) });
  }

  console.log(`Found ${sizeHeaders.length} size columns in Jovani format`);

  if (sizeHeaders.length === 0) return null;

  // Parse data rows
  const masterData: any[][] = [];
  const outputHeaders = ["style", "color", "size", "stock", "price"];

  let currentStyle = "";
  let currentPrice: number | null = null;

  // Invalid color values to filter out (headers, totals, etc.)
  const invalidColorPatterns =
    /^(SIZE|SIZES|TOTAL|SUBTOTAL|SUM|COUNT|QTY|QUANTITY|STOCK|INVENTORY|DESCRIPTION|DESC|COLOR|COLOURS?|STYLE|STYLES?|PRICE|COST|LOCATION|WAREHOUSE|NOTES?|COMMENTS?)$/i;

  // Helper to check if row has stock values in size columns
  const hasStockValues = (row: any[]): boolean => {
    for (const sh of sizeHeaders) {
      const val = row[sh.index];
      if (
        val !== null &&
        val !== undefined &&
        val !== "" &&
        !isNaN(Number(val)) &&
        Number(val) > 0
      ) {
        return true;
      }
    }
    return false;
  };

  // Helper to detect if a row is a style row
  // Style rows: col 0 matches style pattern (starts with #, JVN, JB, D, AL or is numeric)
  // Option 1: col 1 has a price > 10
  // Option 2: No stock values in size columns (style without price in sale files)
  const isStyleRow = (row: any[]): boolean => {
    if (!row || row.length < 2) return false;
    const col0 = String(row[0] ?? "").trim();
    const col1 = row[1];

    // Style patterns: #02861, JVN04759, JB38224, D2020, AL12345, 37001, 1012, etc.
    // - Pure numbers (4-6 digits)
    // - # followed by 4-6 digits
    // - JVN/JB/AL followed by 3-6 digits
    // - D followed by 3-5 digits
    const isStylePattern =
      /^#?\d{4,6}$/.test(col0) ||
      /^(?:JVN|JB|AL)\d{3,6}$/i.test(col0) ||
      /^D\d{3,5}$/i.test(col0);
    if (!isStylePattern) return false;

    // Price can be number or string - parse it
    let priceValue = 0;
    if (typeof col1 === "number") {
      priceValue = col1;
    } else if (col1 !== null && col1 !== undefined) {
      const parsed = parseFloat(String(col1).replace(/[$,]/g, ""));
      if (!isNaN(parsed)) priceValue = parsed;
    }

    // If price > 10, definitely a style row
    if (priceValue > 10) return true;

    // If style pattern but no price, check if it has stock values
    // Style rows without prices (sale files) won't have stock values
    // Color rows have stock values in size columns
    if (!hasStockValues(row)) {
      return true; // Style pattern + no stock = style row without price
    }

    return false;
  };

  // Helper to detect if a row is a color row (has stock values)
  const isColorRow = (row: any[]): boolean => {
    if (!row || row.length < 2) return false;
    const col0 = String(row[0] ?? "").trim();

    // Color row has a non-empty text in col 0 that's not a style pattern
    // Exclude: #12345, JVN/JB/AL + digits, D + digits, pure 4-6 digit numbers
    if (
      !col0 ||
      /^#?\d{4,6}$/.test(col0) ||
      /^(?:JVN|JB|AL)\d{3,6}$/i.test(col0) ||
      /^D\d{3,5}$/i.test(col0)
    )
      return false;

    // Filter out invalid color values (headers, totals, etc.)
    if (invalidColorPatterns.test(col0)) return false;

    // Colors must have at least 2 letters and no digits
    // This prevents numeric values from slipping through as colors
    if (!/[a-zA-Z]{2,}/.test(col0) || /\d/.test(col0)) return false;

    // And has numeric values in stock columns
    let hasStock = false;
    for (const sh of sizeHeaders) {
      const val = row[sh.index];
      if (
        typeof val === "number" ||
        (val !== null && val !== undefined && !isNaN(Number(val)))
      ) {
        hasStock = true;
        break;
      }
    }
    return hasStock;
  };

  // Debug: track styles and colors for troubleshooting
  const stylesFound: string[] = [];
  const colorsByStyle: Record<string, string[]> = {};

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (!row || row.length === 0) continue;

    const col0 = String(row[0] ?? "").trim();

    if (isStyleRow(row)) {
      // Update current style and price
      currentStyle = col0;
      currentPrice =
        typeof row[1] === "number"
          ? row[1]
          : parseFloat(String(row[1])) || null;
      stylesFound.push(`${currentStyle} (price: ${currentPrice})`);
      if (!colorsByStyle[currentStyle]) colorsByStyle[currentStyle] = [];
      continue;
    }

    if (isColorRow(row) && currentStyle) {
      const color = col0;

      // Track colors per style for debugging
      if (!colorsByStyle[currentStyle]) colorsByStyle[currentStyle] = [];
      if (!colorsByStyle[currentStyle].includes(color)) {
        colorsByStyle[currentStyle].push(color);
      }

      // Extract stock values for each size
      for (const sh of sizeHeaders) {
        const stock = row[sh.index];
        if (stock !== null && stock !== undefined) {
          const stockNum =
            typeof stock === "number" ? stock : parseFloat(String(stock));
          if (!isNaN(stockNum)) {
            // Include all items (even zero stock) - filtering happens later
            masterData.push([
              currentStyle,
              color,
              sh.size,
              stockNum,
              currentPrice,
            ]);
          }
        }
      }
    }
  }

  // Log any styles that have suspicious color assignments (for debugging)
  const targetStyles = ["37001", "#37001", "38849", "#38849"];
  for (const style of targetStyles) {
    if (colorsByStyle[style]) {
      console.log(
        `[Jovani Debug] Style ${style} has colors: ${colorsByStyle[style].join(", ")}`,
      );
    }
  }

  console.log(`Parsed ${masterData.length} items from Jovani pivoted format`);

  // Convert to items format
  const items = masterData.map((row) => ({
    sku: row[0],
    style: applyCleaningToValue(String(row[0] || ""), cleaningConfig, "style"),
    color: row[1],
    size: row[2],
    stock: row[3],
    price: row[4],
    rawData: {
      style: row[0],
      color: row[1],
      size: row[2],
      stock: row[3],
      price: row[4],
    },
  }));

  return {
    headers: outputHeaders,
    rows: masterData,
    items,
  };
}

// ============================================================
// SHERRI HILL FORMAT PARSER
// Format: Style, Color, Custom, Special Date, Size1, Special Date, Size2, Special Date, ...
// Stock values: "Yes", "Last Piece", "No" (text-based)
// ============================================================
function parseSherriHillFormat(
  buffer: Buffer,
  cleaningConfig: any,
): { headers: string[]; rows: any[][]; items: any[] } | null {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false, // CRITICAL FIX: Match Email import raw: false for consistent parsing
  }) as any[][];

  if (data.length < 2) return null;

  // Check if this is Sherri Hill format
  const headerStr = data[0]
    .map((h: any) => String(h || "").toUpperCase())
    .join("|");
  if (!headerStr.includes("SPECIAL DATE")) return null;

  console.log(
    `[SherriHill] Parsing Sherri Hill format with ${data.length} rows`,
  );
  console.log(
    `[SherriHill] cleaningConfig.stockTextMappings:`,
    JSON.stringify(cleaningConfig?.stockTextMappings),
  );

  const headerRow = data[0];
  const sizePattern =
    /^(OO0|OOO|OO|0|2|4|6|8|10|12|14|16|18|20|22|24|26|28|30)$/i;
  const sizeColumns: { index: number; size: string; dateIndex: number }[] = [];

  // Find size columns (every other column starting from index 4)
  for (let i = 4; i < headerRow.length; i += 2) {
    const h = String(headerRow[i] ?? "").trim();
    if (sizePattern.test(h)) {
      let normalizedSize = h;
      if (h.toUpperCase() === "OO0" || h.toUpperCase() === "OOO")
        normalizedSize = "000";
      else if (h.toUpperCase() === "OO") normalizedSize = "00";
      sizeColumns.push({ index: i, size: normalizedSize, dateIndex: i + 1 });
    }
  }

  if (sizeColumns.length === 0) {
    console.log(`[SherriHill] No size columns found`);
    return null;
  }

  console.log(`[SherriHill] Found ${sizeColumns.length} size columns`);

  // Parse stock value with text mappings
  const parseStockValue = (value: any): number => {
    if (value === null || value === undefined || value === "") return 0;
    if (typeof value === "number") return Math.max(0, Math.floor(value));

    const strVal = String(value).trim().toLowerCase();

    // Check custom text mappings first
    if (
      cleaningConfig?.stockTextMappings &&
      Array.isArray(cleaningConfig.stockTextMappings)
    ) {
      for (const mapping of cleaningConfig.stockTextMappings) {
        if (mapping.text && mapping.text.toLowerCase().trim() === strVal) {
          return mapping.value;
        }
      }
    }

    // Default mappings
    const defaults: Record<string, number> = {
      yes: 1,
      no: 0,
      "last piece": 1,
      lastpiece: 1,
      "in stock": 1,
      "sold out": 0,
      "out of stock": 0,
      "&ndash;": 0,
      "&ndash; ": 0,
      "–": 0,
      "-": 0,
      "n/a": 0,
    };
    if (defaults[strVal] !== undefined) return defaults[strVal];

    const parsed = parseInt(strVal, 10);
    return isNaN(parsed) ? 0 : Math.max(0, parsed);
  };

  const items: any[] = [];
  const masterData: any[][] = [];

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const style = String(row[0] ?? "").trim();
    const color = String(row[1] ?? "").trim();
    if (!style || !color) continue;

    for (const sc of sizeColumns) {
      const stock = parseStockValue(row[sc.index]);
      const dateVal = row[sc.dateIndex];
      let shipDate: string | null = null;

      // Parse ship date if present and not a dash
      if (
        dateVal &&
        dateVal !== "&ndash;" &&
        dateVal !== "&ndash; " &&
        dateVal !== "–"
      ) {
        if (typeof dateVal === "number" && dateVal > 40000) {
          // Excel serial date
          const date = new Date((dateVal - 25569) * 86400 * 1000);
          shipDate = date.toISOString().split("T")[0];
        } else if (
          typeof dateVal === "string" &&
          dateVal.match(/\d{4}-\d{2}-\d{2}/)
        ) {
          shipDate = dateVal;
        } else if (typeof dateVal === "string" && dateVal.trim()) {
          // Handle US-format dates (M/D/YYYY) from raw:false Excel parsing
          const parsedDate = new Date(dateVal.trim());
          if (!isNaN(parsedDate.getTime())) {
            shipDate = parsedDate.toISOString().split("T")[0];
          }
        }
      }

      // Only add items with stock > 0 or future ship date
      if (stock > 0 || shipDate) {
        const item = {
          style,
          color,
          size: sc.size,
          stock,
          shipDate,
          hasFutureStock: shipDate ? true : false,
          preserveZeroStock: shipDate && stock === 0 ? true : false,
        };
        items.push(item);
        masterData.push([style, color, sc.size, stock, null, null, shipDate]);
      }
    }
  }

  console.log(
    `[SherriHill] Parsed ${items.length} items with stock > 0 or future date`,
  );

  return {
    headers: ["Style", "Color", "Size", "Stock", "Cost", "Price", "ShipDate"],
    rows: masterData,
    items,
  };
}

// Helper function to parse generic pivoted format (sizes as columns)
function parseGenericPivotedFormat(
  buffer: Buffer,
  columnMapping: any,
  cleaningConfig: any,
): { headers: string[]; rows: any[][]; items: any[] } | null {
  // Check if pivoted mode is enabled
  if (!cleaningConfig?.pivotedFormat?.enabled) return null;

  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    raw: false,
  }) as any[][]; // CRITICAL FIX: Match Email import raw: false for consistent parsing

  if (data.length === 0) return null;

  console.log("Parsing generic pivoted format");

  // Find header row (first row with data)
  let headerRowIndex = 0;
  for (let i = 0; i < Math.min(5, data.length); i++) {
    if (
      data[i] &&
      data[i].length > 0 &&
      data[i].some((c: any) => c !== null && c !== undefined)
    ) {
      headerRowIndex = i;
      break;
    }
  }

  const headerRow = data[headerRowIndex].map((h: any) =>
    String(h || "").trim(),
  );

  // Find column indices
  const styleColName = columnMapping?.style || "STYLE#";
  const colorColName = columnMapping?.color || "COLOR";
  const costColName = columnMapping?.cost || "";
  const priceColName = columnMapping?.price || "LIST PRICE";

  const styleIdx = headerRow.findIndex((h: string) =>
    h.toUpperCase().includes(styleColName.toUpperCase()),
  );
  const colorIdx = headerRow.findIndex((h: string) =>
    h.toUpperCase().includes(colorColName.toUpperCase()),
  );
  const costIdx = costColName
    ? headerRow.findIndex((h: string) =>
        h.toUpperCase().includes(costColName.toUpperCase()),
      )
    : -1;
  const priceIdx = priceColName
    ? headerRow.findIndex((h: string) =>
        h.toUpperCase().includes(priceColName.toUpperCase()),
      )
    : -1;

  // Size columns from config or default common sizes
  const sizeColumns = cleaningConfig.pivotedFormat.sizeColumns || [
    "OOO",
    "OO",
    "0",
    "2",
    "4",
    "6",
    "8",
    "10",
    "12",
    "14",
    "16",
    "18",
    "20",
    "22",
    "24",
    "26",
    "28",
    "30",
    "32",
  ];

  // Size column renaming map
  const sizeRenameMap: Record<string, string> = cleaningConfig.pivotedFormat
    .sizeRenameMap || {
    OOO: "000",
    OO: "00",
  };

  // Find size column indices
  const sizeHeaders: { index: number; size: string }[] = [];
  for (const sizeCol of sizeColumns) {
    const idx = headerRow.findIndex((h: string) => h.trim() === sizeCol);
    if (idx !== -1) {
      const renamedSize = sizeRenameMap[sizeCol] || sizeCol;
      sizeHeaders.push({ index: idx, size: renamedSize });
    }
  }

  console.log(`Found ${sizeHeaders.length} size columns`);

  if (styleIdx === -1 || colorIdx === -1 || sizeHeaders.length === 0) {
    console.log("Could not find required columns for pivoted format");
    return null;
  }

  // Parse data rows
  const masterData: any[][] = [];
  const outputHeaders = ["style", "color", "size", "stock", "cost", "price"];

  for (let i = headerRowIndex + 1; i < data.length; i++) {
    const row = data[i];
    if (!row || row.length === 0) continue;

    const style = String(row[styleIdx] || "").trim();
    const color = String(row[colorIdx] || "").trim();
    const cost = costIdx >= 0 ? row[costIdx] : null;
    const price = priceIdx >= 0 ? row[priceIdx] : null;

    if (!style) continue;

    // Create a row for each size with stock > 0
    for (const sh of sizeHeaders) {
      const stockValue = row[sh.index];
      let stockNum: number | null = null;

      // First, try numeric parsing
      if (stockValue !== null && stockValue !== undefined) {
        if (!isNaN(Number(stockValue))) {
          stockNum = Number(stockValue);
        } else if (typeof stockValue === "string") {
          // Try stock text mappings (e.g., "Yes" → 3, "Last Piece" → 1, "No" → 0)
          const stockText = stockValue.toLowerCase().trim();

          // Check stockTextMappings from cleaningConfig
          if (
            cleaningConfig?.stockTextMappings &&
            Array.isArray(cleaningConfig.stockTextMappings)
          ) {
            for (const mapping of cleaningConfig.stockTextMappings) {
              if (
                mapping.text &&
                mapping.text.toLowerCase().trim() === stockText
              ) {
                stockNum = mapping.value;
                break;
              }
            }
          }

          // Fallback to Yes/No conversion if not mapped
          if (stockNum === null && cleaningConfig?.convertYesNo) {
            if (
              stockText === "yes" ||
              stockText === "y" ||
              stockText === "true"
            ) {
              stockNum = cleaningConfig.yesValue || 1;
            } else if (
              stockText === "no" ||
              stockText === "n" ||
              stockText === "false"
            ) {
              stockNum = cleaningConfig.noValue || 0;
            }
          }
        }
      }

      // Only add items with stock > 0
      if (stockNum !== null && stockNum > 0) {
        masterData.push([style, color, sh.size, stockNum, cost, price]);
      }
    }
  }

  console.log(`Parsed ${masterData.length} items from pivoted format`);

  // Convert to items format
  const items = masterData.map((row) => ({
    sku: row[0],
    style: applyCleaningToValue(String(row[0] || ""), cleaningConfig, "style"),
    color: row[1],
    size: row[2],
    stock: row[3],
    cost: row[4],
    price: row[5],
    rawData: {
      style: row[0],
      color: row[1],
      size: row[2],
      stock: row[3],
      cost: row[4],
      price: row[5],
    },
  }));

  return {
    headers: outputHeaders,
    rows: masterData,
    items,
  };
}

// Helper function to parse Excel file and extract inventory items
function parseExcelToInventory(
  buffer: Buffer,
  columnMapping: any,
  cleaningConfig: any,
): { headers: string[]; rows: any[][]; items: any[] } {
  // Try Sherri Hill format first (auto-detect by "SPECIAL DATE" in headers)
  const sherriHillResult = parseSherriHillFormat(buffer, cleaningConfig);
  if (sherriHillResult) {
    console.log(
      `[FileParser] Using Sherri Hill parser - found ${sherriHillResult.items.length} items`,
    );
    return sherriHillResult;
  }

  // First, try Jovani pivoted format if configured
  const jovaniResult = parseJovaniFormat(buffer, cleaningConfig);
  if (jovaniResult) {
    return jovaniResult;
  }

  // Try generic pivoted format if configured
  const pivotedResult = parseGenericPivotedFormat(
    buffer,
    columnMapping,
    cleaningConfig,
  );
  if (pivotedResult) {
    return pivotedResult;
  }

  // Try to detect special pivoted formats (Tarik Ediz)
  const tarikEdizResult = parseTarikEdizFormat(buffer);
  if (tarikEdizResult) {
    // Apply cleaning to the items
    tarikEdizResult.items = tarikEdizResult.items.map((item) => ({
      ...item,
      style: applyCleaningToValue(
        String(item.style || ""),
        cleaningConfig,
        "style",
      ),
    }));
    return tarikEdizResult;
  }

  // Try to detect Feriani/GIA pivoted format (DELIVERY, STYLE, COLOR headers)
  const ferianiGiaResult = parseFerianiGiaFormat(buffer, cleaningConfig);
  if (ferianiGiaResult) {
    console.log(
      `[FileParser] Using Feriani/GIA parser - found ${ferianiGiaResult.items.length} items`,
    );
    return ferianiGiaResult;
  }

  // Standard parsing for normal Excel files
  // CRITICAL: Check for CSV first to prevent scientific notation conversion
  // Style numbers like "1921E0136" would become 1.921e+139 if parsed by XLSX
  let rawData: any[][];

  if (isCSVBuffer(buffer)) {
    console.log(
      "[FileParser] Detected CSV file - using text parser to preserve values",
    );
    rawData = parseCSVAsText(buffer);
  } else {
    const workbook = XLSX.read(buffer, { type: "buffer" });
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    rawData = XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      raw: false,
    }) as any[][]; // CRITICAL FIX: Match Email import raw: false for consistent parsing
  }

  if (rawData.length === 0) {
    return { headers: [], rows: [], items: [] };
  }

  // Intelligent header detection
  let headerRowIndex = 0;
  let maxMatchCount = 0;
  const keywords =
    /sku|code|id|name|title|desc|style|color|colour|size|stock|qty|price|cost|msrp/i;

  for (let i = 0; i < Math.min(10, rawData.length); i++) {
    const row = rawData[i];
    let matchCount = 0;
    if (Array.isArray(row)) {
      row.forEach((cell) => {
        if (cell && typeof cell === "string" && keywords.test(cell)) {
          matchCount++;
        }
      });
    }
    if (matchCount > maxMatchCount) {
      maxMatchCount = matchCount;
      headerRowIndex = i;
    }
  }

  const headers = (rawData[headerRowIndex] || []).map((h) => String(h || ""));
  const dataRows = rawData
    .slice(headerRowIndex + 1)
    .filter(
      (row: any[]) =>
        row &&
        row.some((cell) => cell !== null && cell !== undefined && cell !== ""),
    );

  // Parse rows into inventory items
  const items = dataRows
    .map((row: any[]) => {
      const getColValue = (colName: string) => {
        if (!colName) return null;
        const colIndex = headers.findIndex(
          (h) => h && h.toString().toLowerCase() === colName.toLowerCase(),
        );
        return colIndex >= 0 ? row[colIndex] : null;
      };

      let sku = getColValue(columnMapping?.sku || "") || "";
      let style = getColValue(columnMapping?.style || "") || "";
      let size = getColValue(columnMapping?.size || "") ?? "";
      let color = getColValue(columnMapping?.color || "") || "";
      let stockValue = getColValue(columnMapping?.stock || "");
      let costValue = getColValue(columnMapping?.cost || "");
      let priceValue = getColValue(columnMapping?.price || "");
      let shipDateValue = getColValue(columnMapping?.shipDate || "");
      let discontinuedValue = getColValue(columnMapping?.discontinued || "");
      let salePriceValue = getColValue(columnMapping?.salePrice || "");

      // Handle combined colorsize format (e.g., "Black/Multi > 00 >")
      const colorSizeColIndex = headers.findIndex(
        (h) => h && h.toString().toLowerCase().includes("colorsize"),
      );
      if (colorSizeColIndex >= 0 && !color && !size) {
        const combined = row[colorSizeColIndex]?.toString() || "";
        const parts = combined.split(">").map((s: string) => s.trim());
        if (parts.length >= 2) {
          color = parts[0] || "";
          size = parts[1] || "";
        }
      }

      // Handle combined variant code format (e.g., "AMARNI-BLK-0" = style-color-size)
      // This is commonly used by companies like Portia & Scarlett
      if (cleaningConfig?.combinedVariantColumn) {
        const variantColIndex = headers.findIndex(
          (h) =>
            h &&
            h.toString().toLowerCase() ===
              cleaningConfig.combinedVariantColumn.toLowerCase(),
        );
        if (variantColIndex >= 0) {
          const combined = row[variantColIndex]?.toString() || "";
          const delimiter = cleaningConfig.combinedVariantDelimiter || "-";
          const parts = combined.split(delimiter);

          if (parts.length >= 3) {
            // Split from right: last part is size, second-to-last is color, rest is style
            size = parts[parts.length - 1] || "";
            color = parts[parts.length - 2] || "";
            // Join remaining parts as style (handles styles with dashes like "PS21208")
            style = parts.slice(0, parts.length - 2).join(delimiter) || "";
          } else if (parts.length === 2) {
            // Two parts: assume style-size (no color)
            style = parts[0] || "";
            size = parts[1] || "";
          } else if (parts.length === 1) {
            // Single part: use as style
            style = parts[0] || "";
          }

          // Remove leading zeros from numeric sizes (02→2, 04→4, 06→6, 08→8)
          // But preserve special sizes like "0", "00", "000" (all zeros)
          if (size && /^0+[1-9]\d*$/.test(size)) {
            size = size.replace(/^0+/, "");
          }

          // Use SKU column as style if configured and style is empty or we want to override
          if (cleaningConfig?.useSkuAsStyle && !style) {
            style = sku || "";
          }
        }
      }

      // Apply cleaning ONLY to Style column
      style = applyCleaningToValue(
        String(style || ""),
        cleaningConfig,
        "style",
      );

      // Basic trim for other fields only
      sku = String(sku || "").trim();
      size = String(size ?? "").trim();
      color = String(color || "").trim();

      // Apply size transformations if configured (e.g., D0 → 00)
      if (
        cleaningConfig?.sizeTransformations &&
        Array.isArray(cleaningConfig.sizeTransformations)
      ) {
        for (const transform of cleaningConfig.sizeTransformations) {
          if (
            transform.from &&
            size.toUpperCase() === transform.from.toUpperCase()
          ) {
            size = transform.to || "";
            break; // Apply first matching transformation
          }
        }
      }

      // If no SKU, use style
      if (!sku && style) {
        sku = style;
      }

      // Convert stock to number
      let stock = 0;
      let stockMapped = false;

      // First, try stock text mappings (e.g., "Sold Out" → 0, "Very Low" → 1)
      if (
        cleaningConfig?.stockTextMappings &&
        Array.isArray(cleaningConfig.stockTextMappings) &&
        typeof stockValue === "string"
      ) {
        const stockText = stockValue.trim().toLowerCase();
        for (const mapping of cleaningConfig.stockTextMappings) {
          if (mapping.text && mapping.text.toLowerCase() === stockText) {
            stock = mapping.value;
            stockMapped = true;
            break;
          }
        }
      }

      // If not mapped, try convertYesNo
      if (
        !stockMapped &&
        cleaningConfig?.convertYesNo &&
        typeof stockValue === "string"
      ) {
        const lower = stockValue.toLowerCase().trim();
        if (
          lower === "yes" ||
          lower === "y" ||
          lower === "true" ||
          lower === "1"
        ) {
          stock = cleaningConfig.yesValue || 1;
          stockMapped = true;
        } else if (
          lower === "no" ||
          lower === "n" ||
          lower === "false" ||
          lower === "0"
        ) {
          stock = cleaningConfig.noValue || 0;
          stockMapped = true;
        }
      }

      // Fall back to numeric parsing
      if (!stockMapped) {
        if (typeof stockValue === "number") {
          stock = stockValue;
        } else if (typeof stockValue === "string") {
          const parsed = Math.max(
            0,
            Math.round(parseFloat(stockValue.replace(/[^0-9.-]/g, ""))),
          );
          stock = isNaN(parsed) ? 0 : parsed;
        }
      }

      // Clean cost/price values (keep as strings, remove currency symbols if needed)
      const cost = costValue
        ? String(costValue).replace(/[$,]/g, "").trim()
        : null;
      const price = priceValue
        ? String(priceValue).replace(/[$,]/g, "").trim()
        : null;

      // Parse ship date - keep as string for flexibility
      let shipDate: string | null = null;
      if (shipDateValue) {
        // Handle Excel date serial numbers
        if (typeof shipDateValue === "number") {
          // Excel stores dates as days since 1900-01-01 (with some quirks)
          const excelEpoch = new Date(1899, 11, 30); // Excel incorrectly includes 1900 as leap year
          const date = new Date(
            excelEpoch.getTime() + shipDateValue * 24 * 60 * 60 * 1000,
          );
          shipDate = date.toISOString().split("T")[0]; // Store as YYYY-MM-DD
        } else {
          // Already a string, try to parse and normalize
          const dateStr = String(shipDateValue).trim();
          if (dateStr) {
            // Try to parse common date formats
            const parsedDate = new Date(dateStr);
            if (!isNaN(parsedDate.getTime())) {
              shipDate = parsedDate.toISOString().split("T")[0]; // Store as YYYY-MM-DD
            } else {
              // Invalid date format (e.g., "N/A") - don't preserve raw string
              shipDate = null;
            }
          }
        }
      }

      return {
        sku: sku || "",
        style: style || null,
        size: size != null && size !== "" ? String(size) : null,
        color: color || null,
        stock,
        cost,
        price,
        shipDate,
        // CRITICAL: Set hasFutureStock flag for items with ship dates
        // This ensures size expansion works even with 0 stock
        hasFutureStock: shipDate ? true : false,
        preserveZeroStock: shipDate && stock === 0 ? true : false,
        discontinued: discontinuedValue
          ? (() => {
              const val = String(discontinuedValue).trim();
              // If a column is mapped to discontinued, any non-empty value means discontinued
              // This handles all vendor-specific wording (Discontinued, D, No More, etc.)
              return val.length > 0;
            })()
          : false,
        salePrice: salePriceValue
          ? String(salePriceValue).replace(/[$,]/g, "").trim()
          : null,
        rawData: Object.fromEntries(headers.map((h, i) => [h, row[i]])),
      };
    })
    .filter((item) => item.sku);

  return { headers, rows: dataRows, items };
}

/**
 * Shared combine-import logic — used by BOTH the manual route handler
 * AND the email fetcher. This ensures identical processing regardless
 * of how files were staged (manual upload or email attachment).
 */
export async function performCombineImport(dataSourceId: string): Promise<{
  success: boolean;
  rowCount: number;
  error?: string;
  details?: any;
}> {
  console.log(
    `[performCombineImport] ENTERED for dataSourceId=${dataSourceId}`,
  );
  const dataSource = await storage.getDataSource(dataSourceId);
  if (!dataSource) {
    console.log(
      `[performCombineImport] Data source NOT FOUND for id=${dataSourceId}`,
    );
    return { success: false, rowCount: 0, error: "Data source not found" };
  }
  console.log(
    `[performCombineImport] Data source found: name="${dataSource.name}", sourceType="${(dataSource as any).sourceType}"`,
  );

  const stagedFiles = await storage.getStagedFiles(dataSourceId);
  if (stagedFiles.length === 0) {
    console.log(
      `[performCombineImport] No staged files found for dataSourceId=${dataSourceId}`,
    );
    return { success: false, rowCount: 0, error: "No staged files to combine" };
  }
  console.log(
    `[performCombineImport] Found ${stagedFiles.length} staged files`,
  );

  // Combine items from all staged files
  const allItems: any[] = [];
  let allRows: any[][] = [];
  let columnMapping = (dataSource.columnMapping as any) || {};
  const cleaningConfig = (dataSource.cleaningConfig as any) || {};
  const pivotConfig = (dataSource as any).pivotConfig || {};
  console.log(
    `[performCombineImport] columnMapping keys: ${JSON.stringify(Object.keys(columnMapping))}, isEmpty: ${!columnMapping || Object.keys(columnMapping).length === 0}`,
  );
  console.log(
    `[performCombineImport] cleaningConfig.pivotedFormat: ${JSON.stringify(cleaningConfig?.pivotedFormat || "none")}, pivotConfig: ${JSON.stringify(pivotConfig)}`,
  );

  // AUTO-DETECT column mapping when empty (needed for email import where
  // pre-parsed files have standard headers like sku, style, color, size, stock
  // but the data source has no columnMapping configured)
  if (!columnMapping || Object.keys(columnMapping).length === 0) {
    const firstFile = stagedFiles[0];
    const firstHeaders = (firstFile?.headers as string[]) || [];
    const lowerHeaders = firstHeaders.map((h: string) =>
      (h || "").toLowerCase().trim(),
    );

    const standardFields: Record<string, string[]> = {
      sku: ["sku"],
      style: ["style"],
      color: ["color"],
      size: ["size"],
      stock: ["stock", "quantity", "qty"],
      cost: ["cost"],
      price: ["price"],
      shipDate: ["shipdate", "ship date", "ship_date", "earliest available date",
        "available date", "delivery date", "ready date", "eta"],
      futureStock: ["futurestock", "future stock", "future_stock"],
      futureDate: ["futuredate", "future date", "future_date"],
      incomingStock: ["incomingstock", "incoming stock", "incoming_stock"],
      discontinued: ["discontinued"],
    };

    const autoMapping: any = {};
    for (const [field, aliases] of Object.entries(standardFields)) {
      for (const alias of aliases) {
        const idx = lowerHeaders.indexOf(alias.toLowerCase());
        if (idx >= 0) {
          autoMapping[field] = firstHeaders[idx];
          break;
        }
      }
    }

    if (Object.keys(autoMapping).length > 0) {
      columnMapping = autoMapping;
      console.log(
        `[performCombineImport] columnMapping was empty — auto-detected from headers: ${JSON.stringify(columnMapping)}`,
      );
    } else {
      console.log(
        `[performCombineImport] WARNING: auto-detect found 0 matching fields from headers`,
      );
    }
  } else {
    console.log(
      `[performCombineImport] columnMapping already has keys: ${JSON.stringify(columnMapping)}`,
    );
  }

  console.log(
    `[performCombineImport] FINAL columnMapping being used: ${JSON.stringify(columnMapping)}`,
  );

  const isJovaniSaleFormat = cleaningConfig?.pivotedFormat?.vendor === "jovani";
  const isSaleFile = (dataSource as any).sourceType === "sales";
  let fileIndex = 0;

  for (const file of stagedFiles) {
    const rows = (file.previewData as any[]) || [];
    const headers = (file.headers as string[]) || [];
    allRows.push(...rows);
    console.log(
      `[performCombineImport] File ${fileIndex}: id=${file.id}, rows=${rows.length}, headers=${JSON.stringify(headers)}`,
    );
    if (rows.length > 0) {
      console.log(
        `[performCombineImport] File ${fileIndex} first row (raw): ${JSON.stringify(rows[0])}`,
      );
    }

    const headerIndexMap: Record<string, number> = {};
    headers.forEach((h: string, idx: number) => {
      if (h) headerIndexMap[h.toLowerCase().trim()] = idx;
    });

    // Detect if file was pre-parsed by a format-specific parser
    // Pre-parsed files have standard headers: style, size, stock, color
    // This can happen either because:
    //   1. pivotedFormat.enabled is true (original check), OR
    //   2. The file was parsed by a format-specific parser during upload/email staging
    //      which produces standardized headers regardless of pivotedFormat config
    const hasStandardHeaders =
      headerIndexMap["style"] !== undefined &&
      headerIndexMap["size"] !== undefined &&
      headerIndexMap["stock"] !== undefined;

    // Also check if columnMapping actually matches the staged file headers
    // If columnMapping has original file headers (e.g. "Style #") but staged file
    // has standard headers ("style"), the mapping is mismatched
    const columnMappingMatchesHeaders =
      columnMapping.style &&
      headerIndexMap[String(columnMapping.style).toLowerCase().trim()] !==
        undefined;

    const isPivotedPreParsed =
      hasStandardHeaders &&
      (cleaningConfig?.pivotedFormat?.enabled || // UI-configured pivoted format
        pivotConfig?.enabled || // Auto-detected pivot format (saved during upload)
        !columnMappingMatchesHeaders); // Staged file headers don't match columnMapping
    console.log(
      `[performCombineImport] File ${fileIndex}: isPivotedPreParsed=${isPivotedPreParsed}, hasStandardHeaders=${hasStandardHeaders}, columnMappingMatchesHeaders=${columnMappingMatchesHeaders}, pivotedFormat.enabled=${cleaningConfig?.pivotedFormat?.enabled}, pivotConfig.enabled=${pivotConfig?.enabled}`,
    );

    const getColValue = (row: any[], colName: string) => {
      if (!colName) return null;
      const idx = headerIndexMap[colName.toLowerCase().trim()];
      return idx !== undefined ? row[idx] : null;
    };

    let jovaniCurrentStyle = "";
    const isPurelyNumeric = (val: string) => /^\d+$/.test(val);

    for (const row of rows) {
      if (!Array.isArray(row)) continue;

      let sku: string;
      let style: string;
      let size: string;
      let color: string;
      let brand: string = "";
      let stockValue: any;
      let costValue: any;
      let priceValue: any;
      let shipDateValue: any;
      let futureStockValue: any;
      let futureDateValue: any;

      if (isPivotedPreParsed) {
        sku = String(
          getColValue(row, "sku") || getColValue(row, "style") || "",
        );
        style = String(getColValue(row, "style") || "");
        size = String(getColValue(row, "size") ?? "");
        color = String(getColValue(row, "color") || "");
        stockValue = getColValue(row, "stock");
        costValue = getColValue(row, "cost");
        priceValue = getColValue(row, "price");
        brand = String(getColValue(row, "brand") || "");
        shipDateValue =
          getColValue(row, "shipDate") || getColValue(row, "shipdate");
        futureStockValue =
          getColValue(row, "futureStock") || getColValue(row, "futurestock");
        futureDateValue =
          getColValue(row, "futureDate") || getColValue(row, "futuredate");
        // Also check for incomingStock (from parsers like parseGenericPivotFormat)
        if (!futureStockValue) {
          futureStockValue =
            getColValue(row, "incomingStock") ||
            getColValue(row, "incomingstock");
        }
      } else {
        sku = String(getColValue(row, columnMapping.sku) || "");
        style = String(getColValue(row, columnMapping.style) || "");
        size = String(getColValue(row, columnMapping.size) ?? "");
        color = String(getColValue(row, columnMapping.color) || "");
        stockValue = getColValue(row, columnMapping.stock);
        costValue = getColValue(row, columnMapping.cost);
        priceValue = getColValue(row, columnMapping.price);
        shipDateValue = getColValue(row, columnMapping.shipDate);
        futureStockValue = getColValue(row, columnMapping.futureStock);
        futureDateValue = getColValue(row, columnMapping.futureDate);
      }

      // Diagnostic: log first 3 rows of first file
      if (fileIndex === 0 && rows.indexOf(row) < 3) {
        console.log(
          `[performCombineImport] Row ${rows.indexOf(row)} extracted: style="${style}", sku="${sku}", size="${size}", color="${color}", stock=${stockValue}, path=${isPivotedPreParsed ? "pivoted" : "columnMapping"}`,
        );
      }

      // Jovani sale file stateful parsing
      if (isJovaniSaleFormat && !isPivotedPreParsed) {
        const rawStyle = String(
          getColValue(row, columnMapping.style) || "",
        ).trim();
        const rawColor = String(
          getColValue(row, columnMapping.color) || "",
        ).trim();
        const rawSize = String(
          getColValue(row, columnMapping.size) ?? "",
        ).trim();

        const isStyleNumber = (val: string) => /^#?\d{4,6}$/.test(val);
        const isValidColor = (val: string) => /[a-zA-Z]{2,}/.test(val);

        const isStyleRowNormal = rawStyle && !rawColor;
        const isStyleRowMisaligned =
          !rawStyle && rawColor && isPurelyNumeric(rawColor);
        const isStyleRowNumeric = rawStyle && isStyleNumber(rawStyle);

        if (isStyleRowNormal || isStyleRowNumeric) {
          jovaniCurrentStyle = rawStyle;
          console.log(`[Jovani Combine] Found style: ${jovaniCurrentStyle}`);
          continue;
        }
        if (isStyleRowMisaligned) {
          jovaniCurrentStyle = rawColor;
          console.log(
            `[Jovani Combine] Found style (from color column): ${jovaniCurrentStyle}`,
          );
          continue;
        }
        if (!jovaniCurrentStyle) continue;
        if (!rawColor || isPurelyNumeric(rawColor) || !isValidColor(rawColor))
          continue;

        style = jovaniCurrentStyle;
        color = rawColor;
        size = rawSize;
      }

      // Handle combined variant code format (skip for pre-parsed files)
      if (cleaningConfig.combinedVariantColumn && !isPivotedPreParsed) {
        const combined = String(
          getColValue(row, cleaningConfig.combinedVariantColumn) || "",
        );
        const delimiter = cleaningConfig.combinedVariantDelimiter || "-";
        const parts = combined.split(delimiter);

        if (parts.length >= 3) {
          size = parts[parts.length - 1] || "";
          color = parts[parts.length - 2] || "";
          style = parts.slice(0, parts.length - 2).join(delimiter) || "";
        } else if (parts.length === 2) {
          style = parts[0] || "";
          size = parts[1] || "";
        } else if (parts.length === 1) {
          style = parts[0] || "";
        }

        if (size && /^0+[1-9]\d*$/.test(size)) {
          size = size.replace(/^0+/, "");
        }
      }

      // Apply cleaning to style column only (skip for pre-parsed files — parser already cleaned)
      if (!isPivotedPreParsed) {
        if (style && cleaningConfig.trimWhitespace) {
          style = style.trim();
        }
        if (style && cleaningConfig.removeLetters) {
          style = style.replace(/[a-zA-Z]/g, "");
        }
        if (style && cleaningConfig.removeNumbers) {
          style = style.replace(/[0-9]/g, "");
        }
        if (style && cleaningConfig.removeSpecialChars) {
          style = style.replace(/[^a-zA-Z0-9\s]/g, "");
        }
        if (
          style &&
          cleaningConfig.findText &&
          cleaningConfig.replaceText !== undefined
        ) {
          style = style
            .split(cleaningConfig.findText)
            .join(cleaningConfig.replaceText);
        }
      }

      if (!sku && style) {
        sku = style;
      }

      // Convert stock to number
      let stock = 0;
      if (typeof stockValue === "number") {
        stock = stockValue;
      } else if (typeof stockValue === "string") {
        if (cleaningConfig.convertYesNo) {
          const lower = stockValue.toLowerCase().trim();
          if (lower === "yes" || lower === "y" || lower === "true") {
            stock = cleaningConfig.yesValue ?? 1;
          } else if (lower === "no" || lower === "n" || lower === "false") {
            stock = cleaningConfig.noValue ?? 0;
          } else {
            const parsed = Math.max(
              0,
              Math.round(parseFloat(stockValue.replace(/[^0-9.-]/g, ""))),
            );
            stock = isNaN(parsed) ? 0 : parsed;
          }
        } else {
          const parsed = Math.max(
            0,
            Math.round(parseFloat(stockValue.replace(/[^0-9.-]/g, ""))),
          );
          stock = isNaN(parsed) ? 0 : parsed;
        }
      }

      const cost = costValue
        ? String(costValue).replace(/[$,]/g, "").trim()
        : null;
      const price = priceValue
        ? String(priceValue).replace(/[$,]/g, "").trim()
        : null;

      // Parse ship date
      let shipDate: string | null = null;
      if (shipDateValue) {
        if (typeof shipDateValue === "number") {
          const excelEpoch = new Date(1899, 11, 30);
          const date = new Date(
            excelEpoch.getTime() + shipDateValue * 24 * 60 * 60 * 1000,
          );
          shipDate = date.toISOString().split("T")[0];
        } else {
          const dateStr = String(shipDateValue).trim();
          if (dateStr) {
            const parsedDate = new Date(dateStr);
            if (!isNaN(parsedDate.getTime())) {
              shipDate = parsedDate.toISOString().split("T")[0];
            } else {
              shipDate = null;
            }
          }
        }
      }

      // Parse future stock value
      let futureStock: number | null = null;
      if (
        futureStockValue !== undefined &&
        futureStockValue !== null &&
        futureStockValue !== ""
      ) {
        const parsed = parseFloat(
          String(futureStockValue).replace(/[^0-9.-]/g, ""),
        );
        if (!isNaN(parsed)) futureStock = parsed;
      }

      // Parse future date value
      let futureDate: string | null = null;
      if (futureDateValue) {
        if (typeof futureDateValue === "number") {
          const excelEpoch = new Date(1899, 11, 30);
          const date = new Date(
            excelEpoch.getTime() + futureDateValue * 24 * 60 * 60 * 1000,
          );
          futureDate = date.toISOString().split("T")[0];
        } else {
          const dateStr = String(futureDateValue).trim();
          if (dateStr) {
            const parsedDate = new Date(dateStr);
            if (!isNaN(parsedDate.getTime())) {
              futureDate = parsedDate.toISOString().split("T")[0];
            } else {
              futureDate = null;
            }
          }
        }
      }

      // If item has a brand (from store_multibrand vendor column), use brand as prefix
      const prefix = brand
        ? brand.trim()
        : style
          ? getStylePrefix(style, dataSource, cleaningConfig)
          : dataSource.name;
      const prefixedStyle = style ? `${prefix} ${style}` : style;

      // Rebuild SKU from prefixed style + color + size (matching manual upload handler)
      const normalizedColor = color ? toTitleCase(color) : color;
      const rebuiltSku =
        prefixedStyle && normalizedColor && size != null && size !== ""
          ? `${prefixedStyle}-${normalizedColor}-${size}`
              .replace(/\//g, "-")
              .replace(/\s+/g, "-")
              .replace(/-+/g, "-")
          : (sku || "").replace(/\//g, "-").replace(/-+/g, "-");

      allItems.push({
        dataSourceId,
        saleOwnsStyle: isSaleFile,
        fileId: file.id,
        sku: rebuiltSku,
        style: prefixedStyle,
        size,
        color,
        stock,
        cost,
        price,
        shipDate,
        futureStock,
        futureDate,
        hasFutureStock: shipDate || futureDate ? true : false,
        preserveZeroStock:
          (shipDate || futureDate) && stock === 0 ? true : false,
        rawData: { row, headers },
      });
    }
    fileIndex++;
  }

  console.log(
    `[performCombineImport] Total items extracted from all files: ${allItems.length}, totalRows: ${allRows.length}`,
  );
  if (allItems.length > 0) {
    console.log(
      `[performCombineImport] First item sample: ${JSON.stringify({ style: allItems[0].style, sku: allItems[0].sku, size: allItems[0].size, color: allItems[0].color, stock: allItems[0].stock })}`,
    );
  }
  if (allItems.length === 0) {
    console.log(
      `[performCombineImport] WARNING: 0 items extracted! Check columnMapping and isPivotedPreParsed logic above`,
    );
  }

  const updateStrategy = (dataSource as any).updateStrategy || "full_sync";

  // Future stock zeroing
  const uniqueCombineDates = new Set(
    allItems.map((i: any) => i.shipDate).filter(Boolean),
  );

  if (uniqueCombineDates.size > 1) {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const offset = (dataSource as any).stockInfoConfig?.dateOffsetDays ?? 0;
    const cutoffDate = new Date(today);
    cutoffDate.setDate(cutoffDate.getDate() - offset);

    let futureZeroed = 0;
    let withinOffsetKept = 0;

    for (const item of allItems) {
      if (!item.shipDate || item.stock <= 0) continue;

      let parsedDate: Date | null = null;
      const dateStr = String(item.shipDate).trim();
      const mdyMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
      if (mdyMatch) {
        parsedDate = new Date(
          parseInt(mdyMatch[3]),
          parseInt(mdyMatch[1]) - 1,
          parseInt(mdyMatch[2]),
        );
      } else {
        parsedDate = new Date(dateStr);
      }

      if (
        parsedDate &&
        !isNaN(parsedDate.getTime()) &&
        parsedDate > cutoffDate
      ) {
        (item as any).incomingStock = item.stock;
        item.stock = 0;
        item.hasFutureStock = true;
        item.preserveZeroStock = true;
        futureZeroed++;
      } else if (
        parsedDate &&
        !isNaN(parsedDate.getTime()) &&
        parsedDate > today &&
        parsedDate <= cutoffDate
      ) {
        withinOffsetKept++;
      }
    }

    if (futureZeroed > 0 || withinOffsetKept > 0) {
      console.log(
        `[Combine] Future stock zeroing: ${futureZeroed} items zeroed, ${withinOffsetKept} kept (within ${offset}-day offset). ${uniqueCombineDates.size} unique dates.`,
      );
    }
  } else if (uniqueCombineDates.size === 1) {
    console.log(
      `[Combine] Single date detected (${[...uniqueCombineDates][0]}) — snapshot mode, preserving stock`,
    );
  }

  // Use unified import engine for the pipeline
  const result = await executeImport({
    preConsolidatedItems: allItems,
    preConsolidatedRows: allRows,
    dataSourceId,
    source: 'combine',
    dataSource,
  });

  if (!result.success) {
    return { success: false, rowCount: 0, error: result.error };
  }

  // Mark all staged files as imported
  for (const file of stagedFiles) {
    await storage.updateFileStatus(file.id, "imported");
  }

  return {
    success: true,
    rowCount: result.itemCount,
    details: {
      filesProcessed: stagedFiles.length,
      importedItems: result.itemCount,
      ...result.stats,
    },
  };
}

export async function registerRoutes(
  httpServer: Server,
  app: Express,
): Promise<Server> {
  // ========== STARTUP CLEANUP ==========
  // Clear any stale sync locks from previous server instances (in-memory)
  clearAllSyncLocks();
  console.log("[Server] Cleared in-memory sync locks on startup");

  // Mark stale sync logs in database as failed (zombie syncs from crashes/restarts)
  try {
    const staleSyncsCleared = await storage.markStaleSyncLogsAsFailed(30);
    if (staleSyncsCleared > 0) {
      console.log(
        `[Server] Marked ${staleSyncsCleared} stale sync logs as failed`,
      );
    }
  } catch (err) {
    console.error("[Server] Failed to clean up stale sync logs:", err);
  }

  // ========== AUTH SETUP ==========
  await setupAuth(app);

  // ========== VENDOR IMPORT ROUTES ==========
  registerVendorImportRoutes(app);

  // ========== AI IMPORT ROUTES ==========
  app.use("/api/ai-import", aiImportRoutes);

  // Auth routes
  app.get("/api/auth/user", isAuthenticated, async (req: any, res) => {
    try {
      const userId = req.user.claims.sub;
      const user = await storage.getUser(userId);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }
      res.json(user);
    } catch (error) {
      console.error("Error fetching user:", error);
      res.status(500).json({ message: "Failed to fetch user" });
    }
  });

  // Accept invite and approve user
  app.post(
    "/api/auth/accept-invite",
    isAuthenticated,
    async (req: any, res) => {
      try {
        const { token } = req.body;
        const userId = req.user.claims.sub;

        if (!token) {
          return res.status(400).json({ message: "Invite token required" });
        }

        const invite = await storage.getInviteByToken(token);
        if (!invite) {
          return res.status(404).json({ message: "Invalid invite link" });
        }

        if (invite.expiresAt && new Date(invite.expiresAt) < new Date()) {
          return res.status(400).json({ message: "Invite has expired" });
        }

        if (
          invite.maxUses &&
          invite.useCount &&
          invite.useCount >= invite.maxUses
        ) {
          return res
            .status(400)
            .json({ message: "Invite has reached maximum uses" });
        }

        // Update user to approved with role from invite
        await storage.updateUser(userId, {
          isApproved: true,
          role: invite.role || "member",
          inviteId: invite.id,
        });

        // Update invite usage
        await storage.updateInvite(invite.id, {
          useCount: (invite.useCount || 0) + 1,
          usedAt: new Date(),
          usedBy: userId,
        });

        const updatedUser = await storage.getUser(userId);
        res.json(updatedUser);
      } catch (error) {
        console.error("Error accepting invite:", error);
        res.status(500).json({ message: "Failed to accept invite" });
      }
    },
  );

  // Check if first user (auto-approve as admin)
  app.post(
    "/api/auth/check-first-user",
    isAuthenticated,
    async (req: any, res) => {
      try {
        const userId = req.user.claims.sub;
        const allUsers = await storage.getAllUsers();

        // If this is the only user, make them admin and approve
        if (allUsers.length === 1 && allUsers[0].id === userId) {
          await storage.updateUser(userId, {
            isApproved: true,
            role: "admin",
          });
          const updatedUser = await storage.getUser(userId);
          return res.json({ firstUser: true, user: updatedUser });
        }

        res.json({ firstUser: false });
      } catch (error) {
        console.error("Error checking first user:", error);
        res.status(500).json({ message: "Failed to check first user" });
      }
    },
  );

  // ========== TEAM MANAGEMENT (Admin only) ==========

  // Get all team members
  app.get(
    "/api/team/members",
    isAuthenticated,
    requireApproved,
    requireAdmin,
    async (req, res) => {
      try {
        const users = await storage.getAllUsers();
        res.json(users);
      } catch (error) {
        console.error("Error fetching team members:", error);
        res.status(500).json({ message: "Failed to fetch team members" });
      }
    },
  );

  // Update team member
  app.patch(
    "/api/team/members/:id",
    isAuthenticated,
    requireApproved,
    requireAdmin,
    async (req: any, res) => {
      try {
        const { id } = req.params;
        const { role, isApproved } = req.body;
        const currentUserId = req.user.claims.sub;

        // Prevent self-demotion from admin
        if (id === currentUserId && role !== "admin") {
          return res
            .status(400)
            .json({ message: "Cannot remove your own admin role" });
        }

        const updated = await storage.updateUser(id, { role, isApproved });
        res.json(updated);
      } catch (error) {
        console.error("Error updating team member:", error);
        res.status(500).json({ message: "Failed to update team member" });
      }
    },
  );

  // Remove team member
  app.delete(
    "/api/team/members/:id",
    isAuthenticated,
    requireApproved,
    requireAdmin,
    async (req: any, res) => {
      try {
        const { id } = req.params;
        const currentUserId = req.user.claims.sub;

        if (id === currentUserId) {
          return res.status(400).json({ message: "Cannot remove yourself" });
        }

        await storage.deleteUser(id);
        res.status(204).send();
      } catch (error) {
        console.error("Error removing team member:", error);
        res.status(500).json({ message: "Failed to remove team member" });
      }
    },
  );

  // ========== INVITE MANAGEMENT (Admin only) ==========

  // Get all invites
  app.get(
    "/api/team/invites",
    isAuthenticated,
    requireApproved,
    requireAdmin,
    async (req, res) => {
      try {
        const invites = await storage.getAllInvites();
        res.json(invites);
      } catch (error) {
        console.error("Error fetching invites:", error);
        res.status(500).json({ message: "Failed to fetch invites" });
      }
    },
  );

  // Create new invite
  app.post(
    "/api/team/invites",
    isAuthenticated,
    requireApproved,
    requireAdmin,
    async (req: any, res) => {
      try {
        const { email, role, expiresInDays, maxUses } = req.body;
        const userId = req.user.claims.sub;

        const token = crypto.randomBytes(32).toString("hex");
        const expiresAt = expiresInDays
          ? new Date(Date.now() + expiresInDays * 24 * 60 * 60 * 1000)
          : null;

        const invite = await storage.createInvite({
          token,
          email: email || null,
          role: role || "member",
          createdBy: userId,
          expiresAt,
          maxUses: maxUses || 1,
          useCount: 0,
        });

        res.status(201).json(invite);
      } catch (error) {
        console.error("Error creating invite:", error);
        res.status(500).json({ message: "Failed to create invite" });
      }
    },
  );

  // Delete invite
  app.delete(
    "/api/team/invites/:id",
    isAuthenticated,
    requireApproved,
    requireAdmin,
    async (req, res) => {
      try {
        await storage.deleteInvite(req.params.id);
        res.status(204).send();
      } catch (error) {
        console.error("Error deleting invite:", error);
        res.status(500).json({ message: "Failed to delete invite" });
      }
    },
  );

  // ========== DATA SOURCES ==========

  // Get all data sources
  app.get("/api/data-sources", async (req, res) => {
    try {
      const sources = await storage.getDataSources();
      // Strip heavy fields from list response to prevent memory crashes
      // lastImportStats can contain huge per-style breakdowns (hundreds of KB per data source)
      // Full stats are still available via GET /api/data-sources/:id
      const lightweight = sources.map((s: any) => {
        const { lastImportStats, ...rest } = s;
        return rest;
      });
      res.json(lightweight);
    } catch (error) {
      console.error("Error fetching data sources:", error);
      res.status(500).json({ error: "Failed to fetch data sources" });
    }
  });

  // Get single data source
  app.get("/api/data-sources/:id", async (req, res) => {
    try {
      const source = await storage.getDataSource(req.params.id);
      if (!source) {
        return res.status(404).json({ error: "Data source not found" });
      }
      res.json(source);
    } catch (error) {
      console.error("Error fetching data source:", error);
      res.status(500).json({ error: "Failed to fetch data source" });
    }
  });

  // Test email connection (without requiring a data source)
  app.post("/api/test-email-connection", async (req, res) => {
    try {
      const { host, port, secure, username, password, folder } = req.body;

      if (!host || !username || !password) {
        return res.status(400).json({
          success: false,
          error: "Missing required fields: host, username, password",
        });
      }

      const { testEmailConnection } = await import("./emailFetcher");

      const result = await testEmailConnection({
        host,
        port: port || 993,
        secure: secure !== false,
        username,
        password,
        folder: folder || "INBOX",
        senderWhitelist: [],
        subjectFilter: "",
        markAsRead: false,
      });

      if (result.success) {
        res.json({ success: true, folderCount: result.folderCount });
      } else {
        res.status(400).json({ success: false, error: result.error });
      }
    } catch (error: any) {
      console.error("Error testing email connection:", error);
      res.status(500).json({
        success: false,
        error: error.message || "Failed to test email connection",
      });
    }
  });

  // Test URL connection (server-side to avoid CORS issues)
  app.post("/api/test-url-connection", async (req, res) => {
    try {
      const { url } = req.body;

      if (!url) {
        return res.status(400).json({
          success: false,
          error: "Missing required field: url",
        });
      }

      // Validate URL format
      try {
        new URL(url);
      } catch {
        return res.status(400).json({
          success: false,
          error: "Invalid URL format",
        });
      }

      // Make a HEAD request first, fall back to GET if HEAD is not supported
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 15000);

      try {
        let response = await fetch(url, {
          method: "HEAD",
          signal: controller.signal,
          headers: {
            "User-Agent": "InventoryAI/1.0 (URL Connection Test)",
          },
        });

        // Some servers don't support HEAD, try GET
        if (response.status === 405) {
          response = await fetch(url, {
            method: "GET",
            signal: controller.signal,
            headers: {
              "User-Agent": "InventoryAI/1.0 (URL Connection Test)",
            },
          });
        }

        clearTimeout(timeout);

        if (response.ok) {
          const contentType = response.headers.get("content-type") || "unknown";
          const contentLength = response.headers.get("content-length");
          res.json({
            success: true,
            status: response.status,
            contentType,
            contentLength: contentLength ? parseInt(contentLength) : null,
          });
        } else {
          res.status(400).json({
            success: false,
            error: `Server returned ${response.status}: ${response.statusText}`,
          });
        }
      } catch (fetchError: any) {
        clearTimeout(timeout);
        if (fetchError.name === "AbortError") {
          res.status(400).json({
            success: false,
            error: "Connection timed out after 15 seconds",
          });
        } else {
          throw fetchError;
        }
      }
    } catch (error: any) {
      console.error("Error testing URL connection:", error);
      res.status(500).json({
        success: false,
        error: error.message || "Failed to test URL connection",
      });
    }
  });

  // Create data source
  app.post("/api/data-sources", async (req, res) => {
    try {
      const validated = insertDataSourceSchema.parse(req.body);

      // Auto-connect to the first available Shopify store if not already specified
      if (!validated.shopifyStoreId) {
        const stores = await storage.getShopifyStores();
        const connectedStore = stores.find((s) => s.status === "connected");
        if (connectedStore) {
          (validated as any).shopifyStoreId = connectedStore.id;
          console.log(
            `[Data Source] Auto-connecting new data source "${validated.name}" to Shopify store "${connectedStore.name}"`,
          );
        }
      }

      const created = await storage.createDataSource(validated);

      // Auto-link sale data sources to their regular counterparts
      // If this is a sale file (name ends with "Sale" or "Sales"), find and link to regular file
      if ((created as any).sourceType === "sales") {
        const saleDataSourceName = created.name;
        // Strip "Sale" or "Sales" suffix to find the base name
        const baseNameMatch = saleDataSourceName.match(
          /^(.+?)\s*(Sale|Sales)$/i,
        );
        if (baseNameMatch) {
          const baseName = baseNameMatch[1].trim();
          console.log(
            `[Data Source] Looking for regular data source "${baseName}" to link with sale file "${saleDataSourceName}"`,
          );

          // Find the regular data source with that name
          const allDataSources = await storage.getDataSources();
          const regularDataSource = allDataSources.find(
            (ds) =>
              ds.name.toLowerCase() === baseName.toLowerCase() &&
              ds.id !== created.id &&
              (ds as any).sourceType !== "sales",
          );

          if (regularDataSource) {
            // Update the regular data source to link to this sale file
            await storage.updateDataSource(regularDataSource.id, {
              assignedSaleDataSourceId: created.id,
            } as any);
            console.log(
              `[Data Source] Auto-linked "${regularDataSource.name}" to sale file "${saleDataSourceName}"`,
            );
          }
        }
      }

      // If this is a regular file, check if there's a sale file that matches
      if ((validated as any).sourceType !== "sales") {
        const regularName = created.name;
        const potentialSaleNames = [
          `${regularName} Sale`,
          `${regularName} Sales`,
        ];

        const allDataSources = await storage.getDataSources();
        const saleDataSource = allDataSources.find(
          (ds) =>
            potentialSaleNames.some(
              (n) => n.toLowerCase() === ds.name.toLowerCase(),
            ) &&
            ds.id !== created.id &&
            (ds as any).sourceType === "sales",
        );

        if (saleDataSource && !(created as any).assignedSaleDataSourceId) {
          // Update this regular data source to link to the sale file
          await storage.updateDataSource(created.id, {
            assignedSaleDataSourceId: saleDataSource.id,
          } as any);
          console.log(
            `[Data Source] Auto-linked "${regularName}" to existing sale file "${saleDataSource.name}"`,
          );
          // Re-fetch and return the updated data source
          const updatedCreated = await storage.getDataSource(created.id);
          res.status(201).json(updatedCreated);
          return;
        }
      }

      res.status(201).json(created);
    } catch (error: any) {
      console.error("Error creating data source:", error);
      res
        .status(400)
        .json({ error: error.message || "Failed to create data source" });
    }
  });

  // Update data source
  app.patch("/api/data-sources/:id", async (req, res) => {
    try {
      // Whitelist of allowed fields to prevent cross-contamination of settings
      const allowedFields = [
        "name",
        "type",
        "status",
        "columnMapping",
        "cleaningConfig",
        "connectionDetails",
        "ingestionMode",
        "updateStrategy",
        "autoUpdate",
        "updateFrequency",
        "updateTime",
        "filterZeroStock",
        "filterZeroStockWithFutureDates",
        "continueSelling",
        "shopifyStoreId",
        "sourceType",
        "assignedSaleDataSourceId",
        "salesConfig",
        "autoSyncToShopify",
        "pauseAllImports",
        "discontinuedRules",
        "salePriceConfig",
        "priceFloorCeiling",
        "minStockThreshold",
        "requiredFieldsConfig",
        "stockThresholdEnabled",
        "dateFormatConfig",
        "valueReplacementRules",
        "regularPriceConfig",
        "futureStockConfig",
        "sizeLimitConfig",
        "stockInfoConfig",
        "variantSyncConfig",
        "priceBasedExpansionConfig",
        "sheetConfig",
        "fileParseConfig",
        "validationConfig",
        "importValidationConfig",
        "pivotConfig",
        "formatType",
        "stockValueConfig",
        "complexStockConfig",
        "retryIfNoEmail",
        "retryIntervalMinutes",
        "retryCutoffHour",
      ];

      // Filter req.body to only include allowed fields
      const filteredUpdate: any = {};
      for (const key of allowedFields) {
        if (req.body[key] !== undefined) {
          filteredUpdate[key] = req.body[key];
        }
      }

      // emailSettings requires special handling - only update if explicitly provided
      // and only for the correct data source (prevent cross-contamination)
      if (req.body.emailSettings !== undefined) {
        const existingDs = await storage.getDataSource(req.params.id);
        if (existingDs && existingDs.type === "email") {
          filteredUpdate.emailSettings = req.body.emailSettings;
        } else if (req.body.type === "email") {
          filteredUpdate.emailSettings = req.body.emailSettings;
        } else {
          console.log(
            `[Routes] BLOCKED emailSettings update for non-email data source ${req.params.id}`,
          );
        }
      }

      const updated = await storage.updateDataSource(
        req.params.id,
        filteredUpdate,
      );
      if (!updated) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // If schedule settings changed, refresh schedules immediately
      if (
        req.body.autoUpdate !== undefined ||
        req.body.updateFrequency !== undefined ||
        req.body.updateTime !== undefined
      ) {
        try {
          const { refreshSchedules } = await import("./scheduler");
          await refreshSchedules();
          console.log(
            `[DataSource] Schedule refreshed after update for ${updated.name}`,
          );
        } catch (err: any) {
          console.error(
            "[DataSource] Error refreshing schedules after update:",
            err.message,
          );
        }
      }

      res.json(updated);
    } catch (error: any) {
      console.error("Error updating data source:", error);
      res
        .status(400)
        .json({ error: error.message || "Failed to update data source" });
    }
  });

  // Delete data source
  app.delete("/api/data-sources/:id", async (req, res) => {
    try {
      await storage.deleteDataSource(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting data source:", error);
      res.status(500).json({ error: "Failed to delete data source" });
    }
  });

  // ========== DUPLICATE RULES ==========

  // Copy all Rule Engine rules (Variant + Metafield) from source data source to target
  app.post(
    "/api/data-sources/:sourceId/duplicate-rules/:targetId",
    async (req, res) => {
      try {
        const { sourceId, targetId } = req.params;

        // Validate source and target exist
        const sourceDataSource = await storage.getDataSource(sourceId);
        if (!sourceDataSource) {
          return res
            .status(404)
            .json({ error: "Source data source not found" });
        }

        const targetDataSource = await storage.getDataSource(targetId);
        if (!targetDataSource) {
          return res
            .status(404)
            .json({ error: "Target data source not found" });
        }

        // Get all rules from source
        const sourceVariantRules =
          await storage.getVariantRulesByDataSource(sourceId);
        const sourceMetafieldRules =
          await storage.getShopifyMetafieldRulesByDataSource(sourceId);

        // Get existing rules in target (we'll replace them)
        const targetVariantRules =
          await storage.getVariantRulesByDataSource(targetId);
        const targetMetafieldRules =
          await storage.getShopifyMetafieldRulesByDataSource(targetId);

        // STEP 1: Create new rules in target FIRST
        const createdVariantRuleIds: string[] = [];
        const createdMetafieldRuleIds: string[] = [];
        const failedRules: string[] = [];

        for (const rule of sourceVariantRules) {
          try {
            const newRule = await storage.createVariantRule({
              name: rule.name,
              dataSourceId: targetId,
              stockMin: rule.stockMin,
              stockMax: rule.stockMax,
              sizes: rule.sizes,
              colors: rule.colors,
              expandSizes: rule.expandSizes ?? false,
              sizeSystem: rule.sizeSystem ?? "numeric",
              sizeStep: rule.sizeStep ?? 2,
              expandDownCount: rule.expandDownCount ?? 0,
              expandUpCount: rule.expandUpCount ?? 0,
              minTriggerStock: rule.minTriggerStock,
              expandedStock: rule.expandedStock,
              priority: rule.priority,
              enabled: rule.enabled ?? true,
            });
            createdVariantRuleIds.push(newRule.id);
          } catch (err) {
            console.error(`Failed to create variant rule ${rule.name}:`, err);
            failedRules.push(`Variant: ${rule.name}`);
          }
        }

        for (const rule of sourceMetafieldRules) {
          try {
            const newRule = await storage.createShopifyMetafieldRule({
              name: rule.name,
              dataSourceId: targetId,
              metafieldNamespace: rule.metafieldNamespace ?? "my_fields",
              metafieldKey: rule.metafieldKey ?? "stock_info",
              stockThreshold: rule.stockThreshold ?? 0,
              inStockMessage: rule.inStockMessage,
              sizeExpansionMessage: rule.sizeExpansionMessage,
              outOfStockMessage: rule.outOfStockMessage,
              futureDateMessage: rule.futureDateMessage,
              dateOffsetDays: rule.dateOffsetDays ?? 0,
              enabled: rule.enabled ?? true,
            });
            createdMetafieldRuleIds.push(newRule.id);
          } catch (err) {
            console.error(`Failed to create metafield rule ${rule.name}:`, err);
            failedRules.push(`Metafield: ${rule.name}`);
          }
        }

        // If any rules failed, rollback and abort
        if (failedRules.length > 0) {
          for (const id of createdVariantRuleIds) {
            try {
              await storage.deleteVariantRule(id);
            } catch (e) {}
          }
          for (const id of createdMetafieldRuleIds) {
            try {
              await storage.deleteShopifyMetafieldRule(id);
            } catch (e) {}
          }
          return res.status(500).json({
            error: `Failed to copy rules: ${failedRules.join(", ")}`,
            failedRules,
          });
        }

        // STEP 2: Delete old rules from target
        for (const rule of targetVariantRules) {
          try {
            await storage.deleteVariantRule(rule.id);
          } catch (e) {}
        }
        for (const rule of targetMetafieldRules) {
          try {
            await storage.deleteShopifyMetafieldRule(rule.id);
          } catch (e) {}
        }

        res.json({
          success: true,
          message: `Copied ${createdVariantRuleIds.length} variant rules and ${createdMetafieldRuleIds.length} metafield rules from "${sourceDataSource.name}" to "${targetDataSource.name}"`,
          variantRulesCopied: createdVariantRuleIds.length,
          metafieldRulesCopied: createdMetafieldRuleIds.length,
        });
      } catch (error) {
        console.error("Error duplicating rules:", error);
        res.status(500).json({ error: "Failed to duplicate rules" });
      }
    },
  );

  // ========== SALE FILE LINKING ==========

  // Get linked sale data source for a regular data source
  app.get("/api/data-sources/:id/linked-sale", async (req, res) => {
    try {
      const dataSource = await storage.getDataSource(req.params.id);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      const saleDataSourceId = (dataSource as any).assignedSaleDataSourceId;
      if (!saleDataSourceId) {
        return res.json({ linked: false, saleDataSource: null });
      }

      const saleDataSource = await storage.getDataSource(saleDataSourceId);
      res.json({
        linked: !!saleDataSource,
        saleDataSource: saleDataSource || null,
      });
    } catch (error) {
      console.error("Error fetching linked sale data source:", error);
      res
        .status(500)
        .json({ error: "Failed to fetch linked sale data source" });
    }
  });

  // Link a sale data source to a regular data source
  app.post("/api/data-sources/:id/link-sale", async (req, res) => {
    try {
      const { saleDataSourceId } = req.body;
      if (!saleDataSourceId) {
        return res.status(400).json({ error: "saleDataSourceId is required" });
      }

      const regularDataSource = await storage.getDataSource(req.params.id);
      if (!regularDataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // Verify the sale data source exists and is a sale type
      const saleDataSource = await storage.getDataSource(saleDataSourceId);
      if (!saleDataSource) {
        return res.status(404).json({ error: "Sale data source not found" });
      }

      if ((saleDataSource as any).sourceType !== "sales") {
        return res
          .status(400)
          .json({ error: "Target data source is not a sale file type" });
      }

      // Update the regular data source with the link
      const updated = await storage.updateDataSource(req.params.id, {
        assignedSaleDataSourceId: saleDataSourceId,
      } as any);

      console.log(
        `[Data Source] Manually linked "${regularDataSource.name}" to sale file "${saleDataSource.name}"`,
      );

      res.json({
        success: true,
        dataSource: updated,
        message: `Linked to ${saleDataSource.name}`,
      });
    } catch (error) {
      console.error("Error linking sale data source:", error);
      res.status(500).json({ error: "Failed to link sale data source" });
    }
  });

  // Unlink the sale data source from a regular data source
  app.delete("/api/data-sources/:id/link-sale", async (req, res) => {
    try {
      const dataSource = await storage.getDataSource(req.params.id);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // Clear the link
      const updated = await storage.updateDataSource(req.params.id, {
        assignedSaleDataSourceId: null,
      } as any);

      console.log(`[Data Source] Unlinked sale file from "${dataSource.name}"`);

      res.json({
        success: true,
        dataSource: updated,
        message: "Sale data source unlinked",
      });
    } catch (error) {
      console.error("Error unlinking sale data source:", error);
      res.status(500).json({ error: "Failed to unlink sale data source" });
    }
  });

  // ========== DISCONTINUED STYLES ==========

  // Get discontinued styles for a sale data source
  app.get("/api/data-sources/:id/discontinued-styles", async (req, res) => {
    try {
      const dataSource = await storage.getDataSource(req.params.id);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      if ((dataSource as any).sourceType !== "sales") {
        return res
          .status(400)
          .json({ error: "Data source is not a sale file type" });
      }

      const styles = await storage.getDiscontinuedStylesBySaleDataSource(
        req.params.id,
      );
      res.json({
        count: styles.length,
        styles: styles.map((s) => ({
          id: s.id,
          style: s.style,
          active: s.active,
          createdAt: s.createdAt,
          updatedAt: s.updatedAt,
        })),
      });
    } catch (error) {
      console.error("Error fetching discontinued styles:", error);
      res.status(500).json({ error: "Failed to fetch discontinued styles" });
    }
  });

  // Clear discontinued styles for a sale data source
  app.delete("/api/data-sources/:id/discontinued-styles", async (req, res) => {
    try {
      const dataSource = await storage.getDataSource(req.params.id);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      if ((dataSource as any).sourceType !== "sales") {
        return res
          .status(400)
          .json({ error: "Data source is not a sale file type" });
      }

      await storage.deleteDiscontinuedStylesBySaleDataSource(req.params.id);

      console.log(
        `[Data Source] Cleared discontinued styles for "${dataSource.name}"`,
      );

      res.json({ success: true, message: "Discontinued styles cleared" });
    } catch (error) {
      console.error("Error clearing discontinued styles:", error);
      res.status(500).json({ error: "Failed to clear discontinued styles" });
    }
  });

  // Check if sale file import is required before regular file import
  app.get("/api/data-sources/:id/check-sale-import", async (req, res) => {
    try {
      const result = await checkSaleImportFirstRequirement(req.params.id);
      res.json(result);
    } catch (error) {
      console.error("Error checking sale import requirement:", error);
      res
        .status(500)
        .json({ error: "Failed to check sale import requirement" });
    }
  });

  // ========== UPLOADED FILES ==========

  // Get recent uploads across all data sources
  app.get("/api/uploads/recent", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const uploads = await storage.getRecentUploads(limit);
      res.json(uploads);
    } catch (error) {
      console.error("Error fetching recent uploads:", error);
      res.status(500).json({ error: "Failed to fetch recent uploads" });
    }
  });

  // Clear upload history
  app.delete("/api/uploads/history", async (req, res) => {
    try {
      const deletedCount = await storage.clearUploadHistory();
      res.json({ success: true, deletedCount });
    } catch (error) {
      console.error("Error clearing upload history:", error);
      res.status(500).json({ error: "Failed to clear upload history" });
    }
  });

  // Get files for a data source
  app.get("/api/data-sources/:id/files", async (req, res) => {
    try {
      const files = await storage.getFilesByDataSource(req.params.id);
      res.json(files);
    } catch (error) {
      console.error("Error fetching files:", error);
      res.status(500).json({ error: "Failed to fetch files" });
    }
  });

  // Get latest file for a data source
  app.get("/api/data-sources/:id/latest-file", async (req, res) => {
    try {
      const file = await storage.getLatestFile(req.params.id);
      if (!file) {
        return res.status(404).json({ error: "No files found" });
      }
      res.json(file);
    } catch (error) {
      console.error("Error fetching latest file:", error);
      res.status(500).json({ error: "Failed to fetch latest file" });
    }
  });

  // Get inventory preview for a data source (real data from master inventory)
  app.get("/api/data-sources/:id/inventory-preview", async (req, res) => {
    try {
      const dataSourceId = req.params.id;
      const dataSource = await storage.getDataSource(dataSourceId);

      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // Fetch first 10 inventory items for this data source
      const items = await storage.getInventoryItemsByDataSource(
        dataSourceId,
        10,
      );

      if (items.length === 0) {
        return res.json({
          headers: [],
          rows: [],
          fileName: null,
        });
      }

      // Build preview data from real inventory items
      const headers = ["Style", "Color", "Size", "Stock", "SKU", "Ship Date"];
      const rows = items.map((item) => [
        item.style || "",
        item.color || "",
        String(item.size ?? ""),
        item.stock ?? 0,
        item.sku || "",
        item.shipDate || "",
      ]);

      // Get the latest file name if available
      const latestFile = await storage.getLatestFile(dataSourceId);
      const fileName =
        latestFile?.fileName || `${dataSource.name} - ${items.length} items`;

      res.json({
        headers,
        rows,
        fileName,
      });
    } catch (error) {
      console.error("Error fetching inventory preview:", error);
      res.status(500).json({ error: "Failed to fetch inventory preview" });
    }
  });

  // Upload file and parse data (legacy)
  app.post("/api/upload-file", async (req, res) => {
    try {
      const validated = insertUploadedFileSchema.parse(req.body);
      const created = await storage.createUploadedFile(validated);
      res.status(201).json(created);
    } catch (error: any) {
      console.error("Error uploading file:", error);
      res.status(400).json({ error: error.message || "Failed to upload file" });
    }
  });

  // Upload file with automatic import to master inventory (or stage for multi-file mode)
  app.post(
    "/api/data-sources/:id/upload",
    upload.single("file"),
    async (req, res) => {
      const dataSourceId = req.params.id;

      // Signal that import has started (for sync coordination)
      startImport(dataSourceId);

      try {
        const file = req.file;

        if (!file) {
          failImport(dataSourceId, "No file uploaded");
          return res.status(400).json({ error: "No file uploaded" });
        }

        // Get the data source to access column mapping
        const dataSource = await storage.getDataSource(dataSourceId);
        if (!dataSource) {
          failImport(dataSourceId, "Data source not found");
          return res.status(404).json({ error: "Data source not found" });
        }

        // SAFETY NET: Validate file before import
        const validationConfig =
          (dataSource as any).importValidationConfig || {};
        if (validationConfig.enabled !== false) {
          console.log(
            `[Upload] Validating file "${file.originalname}" for data source "${dataSource.name}"`,
          );
          const validation = await validateImportFile(
            file.buffer,
            dataSourceId,
            file.originalname,
          );

          if (!validation.valid) {
            // Log the validation failure
            await logValidationFailure(
              dataSourceId,
              file.originalname,
              validation.errors,
              validation.warnings,
            );

            console.error(
              `[Upload] SAFETY BLOCK: File "${file.originalname}" failed validation:`,
              validation.errors,
            );
            failImport(
              dataSourceId,
              `Validation failed: ${validation.errors.join("; ")}`,
            );
            return res.status(400).json({
              error: "Import blocked - file validation failed",
              validationErrors: validation.errors,
              validationWarnings: validation.warnings,
              message: `SAFETY NET: Import blocked to protect your data. Issues found: ${validation.errors.join("; ")}`,
            });
          }

          if (validation.warnings.length > 0) {
            console.log(
              `[Upload] Validation warnings for "${file.originalname}":`,
              validation.warnings,
            );
          }
        }

        // Parse the Excel file - use shared parsers from aiImportRoutes
        // This ensures manual upload uses the EXACT SAME parsing as AI import and email import
        console.log(`[Upload] Processing file for ${dataSource.name}`);
        let pivotConfig = (dataSource as any).pivotConfig;
        console.log(`[Upload] pivotConfig:`, JSON.stringify(pivotConfig));

        // Detect format from file content using the shared detector
        // Only parse first 10 rows for detection to minimize memory usage
        let detectedFormat: string | null = null;
        {
          const detectWorkbook = XLSX.readFile(file.path, { sheetRows: 10 });
          const detectSheet = detectWorkbook.Sheets[detectWorkbook.SheetNames[0]];
          const sampleData = XLSX.utils.sheet_to_json(detectSheet, {
            header: 1,
            defval: "",
            raw: false,
          }) as any[][];
          if (sampleData.length > 0) {
            detectedFormat = autoDetectPivotFormat(
              sampleData,
              dataSource.name,
              file.originalname,
            );
          }
          // detectWorkbook, detectSheet, sampleData go out of scope here → GC can reclaim
        }
        if (detectedFormat) {
          console.log(
            `[Upload] Shared detector found format: "${detectedFormat}"`,
          );
          pivotConfig = { enabled: true, format: detectedFormat };
          // Save the detected format for future imports
          await storage.updateDataSource(dataSourceId, {
            formatType: detectedFormat,
            pivotConfig: { enabled: true, format: detectedFormat },
          });
        }

        let headers: string[];
        let rows: any[][];
        let items: any[];

        // Use shared parsers for all detected pivot/format-specific types
        if (
          detectedFormat ||
          (pivotConfig?.format && pivotConfig.format !== "generic_legacy")
        ) {
          const actualFormat = detectedFormat || pivotConfig.format;
          console.log(
            `[Upload] Using shared parser for format: "${actualFormat}"`,
          );
          const universalConfig: UniversalParserConfig = {
            skipRows: pivotConfig?.skipRows,
            discontinuedConfig: (dataSource as any).discontinuedConfig,
            futureDateConfig: (dataSource as any).futureStockConfig,
            stockConfig: (dataSource as any).stockValueConfig,
            columnMapping: (dataSource as any).columnMapping,
          };
          const pivotResult = parseIntelligentPivotFormat(
            file.buffer,
            actualFormat,
            universalConfig,
            dataSource.name,
            file.originalname,
          );
          headers = pivotResult.headers;
          rows = pivotResult.rows;
          items = pivotResult.items;

          // Apply data source cleaning rules (Style Find/Replace, etc.)
          const uploadCleaningConfig = (dataSource.cleaningConfig || {}) as any;
          if (uploadCleaningConfig && items.length > 0) {
            const hasAnyCleaning =
              uploadCleaningConfig.findText ||
              uploadCleaningConfig.findReplaceRules?.length > 0 ||
              uploadCleaningConfig.removeLetters ||
              uploadCleaningConfig.removeNumbers ||
              uploadCleaningConfig.removeSpecialChars ||
              uploadCleaningConfig.removeFirstN ||
              uploadCleaningConfig.removeLastN ||
              uploadCleaningConfig.removePatterns?.length > 0 ||
              uploadCleaningConfig.trimWhitespace;
            if (hasAnyCleaning) {
              console.log(
                `[Upload] Applying data source cleaning rules to ${items.length} items`,
              );
              items = items.map((item: any) => ({
                ...item,
                style: applyCleaningToValue(
                  String(item.style || ""),
                  uploadCleaningConfig,
                  "style",
                ),
              }));
            }
          }

          console.log(`[Upload] Shared parser extracted ${items.length} items`);
        } else if (pivotConfig?.enabled) {
          console.log(
            `[Upload] Using legacy pivoted table parser for ${dataSource.name}`,
          );
          const result = parsePivotedExcelToInventory(
            file.buffer,
            pivotConfig,
            dataSource.cleaningConfig || {},
            dataSource.name,
          );
          headers = result.headers;
          rows = result.rows;
          items = result.items;
        } else {
          const result = parseExcelToInventory(
            file.buffer,
            dataSource.columnMapping || {},
            dataSource.cleaningConfig || {},
          );
          headers = result.headers;
          rows = result.rows;
          items = result.items;
        }

        // Check if multi-file mode
        const isMultiFile = (dataSource as any).ingestionMode === "multi";

        // For single file mode (not multi-file), update existing imported file or create new
        // For multi-file mode, always create new file records (staged)
        let uploadedFile;
        if (!isMultiFile) {
          const existingFile =
            await storage.getLatestImportedFile(dataSourceId);
          console.log(
            `[Upload] Single file mode for ${dataSource.name}: existingFile=${existingFile?.id || "none"}, rows=${rows.length}`,
          );
          if (existingFile) {
            console.log(
              `[Upload] Updating existing file record ${existingFile.id} with new preview data`,
            );
            uploadedFile = await storage.updateUploadedFile(existingFile.id, {
              fileName: file.originalname,
              fileSize: file.size,
              rowCount: rows.length,
              previewData: rows.slice(0, 10) as any,
              headers,
              fileStatus: "imported",
            } as any);
            console.log(
              `[Upload] File record updated: ${uploadedFile?.id}, uploadedAt=${uploadedFile?.uploadedAt}`,
            );
          } else {
            console.log(
              `[Upload] Creating new file record for ${dataSource.name}`,
            );
            uploadedFile = await storage.createUploadedFile({
              dataSourceId,
              fileName: file.originalname,
              fileSize: file.size,
              rowCount: rows.length,
              previewData: rows.slice(0, 10) as any,
              headers,
              fileStatus: "imported",
            } as any);
            console.log(
              `[Upload] New file record created: ${uploadedFile?.id}`,
            );
          }
        } else {
          uploadedFile = await storage.createUploadedFile({
            dataSourceId,
            fileName: file.originalname,
            fileSize: file.size,
            rowCount: rows.length,
            previewData: rows as any,
            headers,
            fileStatus: "staged",
          } as any);
        }

        // For multi-file mode, just stage the file without importing
        if (isMultiFile) {
          const stagedFiles = await storage.getStagedFiles(dataSourceId);
          res.status(201).json({
            success: true,
            staged: true,
            file: uploadedFile,
            stagedCount: stagedFiles.length,
            message: `File "${file.originalname}" staged. You have ${stagedFiles.length} file(s) staged. Click "Combine & Import" when ready.`,
          });
          return;
        }

        // Single file mode - import immediately via unified engine
        const result = await executeImport({
          fileBuffers: [{ buffer: file.buffer, originalname: file.originalname }],
          dataSourceId,
          source: 'manual_upload',
          dataSource,
          fileId: uploadedFile?.id,
        });

        if (!result.success) {
          failImport(dataSourceId, result.error || "Import failed");
          return res.status(400).json({ error: result.error, safetyBlock: result.safetyBlock });
        }

        completeImport(dataSourceId, result.itemCount);

        // Trigger background comparison job for incremental sync
        try {
          const stores = await storage.getShopifyStores();
          if (stores.length > 0) {
            startComparisonJob({ storeId: stores[0].id, dataSourceId });
          }
        } catch (err) {
          console.error("[Import] Error starting comparison job:", err);
        }

        // Trigger Shopify sync if enabled
        triggerShopifySyncAfterImport(dataSourceId).catch((err) => {
          console.error("Error triggering Shopify sync after upload:", err.message);
        });

        res.status(201).json({
          success: true,
          file: uploadedFile,
          importedItems: result.itemCount,
          ...result.stats,
          message: `Uploaded ${file.originalname} - imported ${result.itemCount} items`,
        });
      } catch (error: any) {
        console.error("Error uploading file:", error);
        // Signal import failure (for sync coordination)
        failImport(dataSourceId, error.message || "Failed to upload file");
        res
          .status(500)
          .json({ error: error.message || "Failed to upload file" });
      }
    },
  );

  // Get staged files for multi-file mode
  app.get("/api/data-sources/:id/staged-files", async (req, res) => {
    try {
      const dataSourceId = req.params.id;
      const stagedFiles = await storage.getStagedFiles(dataSourceId);
      res.json(stagedFiles);
    } catch (error) {
      console.error("Error fetching staged files:", error);
      res.status(500).json({ error: "Failed to fetch staged files" });
    }
  });

  // Delete a staged file
  app.delete("/api/data-sources/:id/staged-files/:fileId", async (req, res) => {
    try {
      const { fileId } = req.params;
      await storage.deleteFile(fileId);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting staged file:", error);
      res.status(500).json({ error: "Failed to delete staged file" });
    }
  });

  // Consolidate sale file into regular data source (Pre-Sync Sale File Consolidation)
  app.post("/api/data-sources/:id/consolidate-sale", async (req, res) => {
    try {
      const regularDataSourceId = req.params.id;

      const dataSource = await storage.getDataSource(regularDataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      if (!dataSource.assignedSaleDataSourceId) {
        return res
          .status(400)
          .json({ error: "No sale data source assigned to this data source" });
      }

      const result = await consolidateSaleIntoRegular(
        regularDataSourceId,
        dataSource.assignedSaleDataSourceId,
      );

      res.json({
        success: true,
        message: `Consolidated sale file into regular data source`,
        ...result,
      });
    } catch (error: any) {
      console.error("Error consolidating sale file:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to consolidate sale file" });
    }
  });

  // Combine and import staged files for multi-file mode
  app.post("/api/data-sources/:id/combine-import", async (req, res) => {
    try {
      const dataSourceId = req.params.id;

      // Use the shared performCombineImport function (same logic as email import)
      const result = await performCombineImport(dataSourceId);

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      // Post-import: auto-consolidation and Shopify sync
      try {
        await triggerAutoConsolidationAfterImport(dataSourceId);
      } catch (err: any) {
        console.error("Error in auto-consolidation after import:", err.message);
      }
      triggerShopifySyncAfterImport(dataSourceId).catch((err) => {
        console.error(
          "Error triggering Shopify sync after import:",
          err.message,
        );
      });

      return res.json({
        success: true,
        ...result.details,
      });
    } catch (error: any) {
      console.error("Error combining files:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to combine files" });
    }
  });

  // Fetch and process file from URL
  app.post("/api/data-sources/:id/fetch-url", async (req, res) => {
    try {
      const dataSourceId = req.params.id;
      let { url } = req.body;

      // Get the data source
      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // If no URL provided in body, use the stored URL from connectionDetails
      if (
        !url &&
        dataSource.type === "url" &&
        dataSource.connectionDetails?.url
      ) {
        url = dataSource.connectionDetails.url;
      }

      if (!url) {
        return res.status(400).json({
          error:
            "URL is required. Either provide a URL in the request or configure one in the data source settings.",
        });
      }

      // Check if sale file import is required first (for automated imports)
      const saleImportCheck =
        await checkSaleImportFirstRequirement(dataSourceId);
      if (saleImportCheck.requiresWarning) {
        console.log(
          `[URL Fetch] SKIPPED: ${dataSource.name} - ${saleImportCheck.warningMessage}`,
        );
        return res.status(400).json({
          error: "Sale file not imported",
          requiresSaleImport: true,
          saleDataSourceId: saleImportCheck.saleDataSourceId,
          saleDataSourceName: saleImportCheck.saleDataSourceName,
          message: saleImportCheck.warningMessage,
        });
      }

      // Fetch the file from URL
      console.log(`Fetching file from URL: ${url}`);
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(
          `Failed to fetch URL: ${response.status} ${response.statusText}`,
        );
      }

      // Get the file content as buffer
      const arrayBuffer = await response.arrayBuffer();
      const buffer = Buffer.from(arrayBuffer);

      // Extract filename from URL
      const urlFilename = url.split("/").pop() || "url_file.xlsx";

      // SAFETY NET: Validate file before import
      const validationConfig = (dataSource as any).importValidationConfig || {};
      if (validationConfig.enabled !== false) {
        console.log(
          `[URL Fetch] Validating file from URL for data source "${dataSource.name}"`,
        );
        const validation = await validateImportFile(
          buffer,
          dataSourceId,
          urlFilename,
        );

        if (!validation.valid) {
          // Log the validation failure
          await logValidationFailure(
            dataSourceId,
            urlFilename,
            validation.errors,
            validation.warnings,
          );

          console.error(
            `[URL Fetch] SAFETY BLOCK: File from URL failed validation:`,
            validation.errors,
          );
          return res.status(400).json({
            error: "Import blocked - file validation failed",
            validationErrors: validation.errors,
            validationWarnings: validation.warnings,
            message: `SAFETY NET: Import blocked to protect your data. Issues found: ${validation.errors.join("; ")}`,
          });
        }

        if (validation.warnings.length > 0) {
          console.log(`[URL Fetch] Validation warnings:`, validation.warnings);
        }
      }

      // Use unified import engine
      const result = await executeImport({
        fileBuffers: [{ buffer, originalname: urlFilename }],
        dataSourceId,
        source: 'url',
        dataSource,
      });

      if (!result.success) {
        return res.status(400).json({ error: result.error, safetyBlock: result.safetyBlock });
      }

      // Post-import hooks
      try { await triggerAutoConsolidationAfterImport(dataSourceId); } catch {}
      triggerShopifySyncAfterImport(dataSourceId).catch(() => {});

      res.json({
        success: true,
        importedItems: result.itemCount,
        ...result.stats,
      });
    } catch (error: any) {
      console.error("Error fetching URL:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch and process URL" });
    }
  });

  // Fetch and process email attachments
  app.post("/api/data-sources/:id/fetch-email", async (req, res) => {
    try {
      const dataSourceId = req.params.id;

      // Optional: Clear hash/logs before fetching (for testing - allows re-processing same emails)
      if (req.body?.clearHash) {
        const deletedCount = await storage.deleteEmailFetchLogs(dataSourceId);
        console.log(
          `[Email Fetch] Cleared ${deletedCount} email fetch logs before re-fetch (clearHash=true)`,
        );
      }

      // Get the data source
      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // Check if sale file import is required first (for automated imports)
      const saleImportCheck =
        await checkSaleImportFirstRequirement(dataSourceId);
      if (saleImportCheck.requiresWarning) {
        console.log(
          `[Email Fetch] SKIPPED: ${dataSource.name} - ${saleImportCheck.warningMessage}`,
        );
        return res.status(400).json({
          error: "Sale file not imported",
          requiresSaleImport: true,
          saleDataSourceId: saleImportCheck.saleDataSourceId,
          saleDataSourceName: saleImportCheck.saleDataSourceName,
          message: saleImportCheck.warningMessage,
        });
      }

      const emailSettings = (dataSource as any).emailSettings;
      if (!emailSettings || !emailSettings.host || !emailSettings.username) {
        return res.status(400).json({
          error: "Email settings not configured for this data source",
        });
      }

      // Resolve password from environment variable if it uses env: prefix syntax
      const resolvedSettings = { ...emailSettings };
      if (emailSettings.password && emailSettings.password.startsWith("env:")) {
        const envVarName = emailSettings.password.substring(4);
        const envPassword = process.env[envVarName];
        if (envPassword) {
          resolvedSettings.password = envPassword;
        } else {
          return res.status(400).json({
            error: `Email password secret '${envVarName}' is not configured. Please add it to your environment secrets.`,
          });
        }
      }

      // Import the email fetcher dynamically
      const { fetchEmailAttachments } = await import("./emailFetcher");

      // Fetch emails
      const fetchResult = await fetchEmailAttachments(
        dataSourceId,
        resolvedSettings,
      );

      if (!fetchResult.success) {
        return res.status(500).json({
          error: fetchResult.errors.join(", "),
          logs: fetchResult.logs,
        });
      }

      // Await auto-consolidation and trigger Shopify sync if files were actually imported
      if (fetchResult.filesProcessed > 0) {
        try {
          await triggerAutoConsolidationAfterImport(dataSourceId);
        } catch (err: any) {
          console.error(
            "Error in auto-consolidation after email import:",
            err.message,
          );
        }
        // Trigger Shopify sync - sync only this data source + its sale file (same as manual sync)
        triggerShopifySyncAfterImport(dataSourceId).catch((err) => {
          console.error(
            "Error triggering Shopify sync after email import:",
            err.message,
          );
        });
      }

      res.json({
        success: true,
        filesProcessed: fetchResult.filesProcessed,
        logs: fetchResult.logs,
        errors: fetchResult.errors,
      });
    } catch (error: any) {
      console.error("Error fetching emails:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch emails" });
    }
  });

  // Clear email fetch logs/hash for a data source (allows re-testing imports)
  app.post("/api/data-sources/:id/clear-email-hash", async (req, res) => {
    try {
      const dataSourceId = req.params.id;

      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      const deletedCount = await storage.deleteEmailFetchLogs(dataSourceId);

      console.log(
        `[Routes] Cleared ${deletedCount} email fetch logs for "${dataSource.name}" (hash reset for testing)`,
      );

      res.json({
        success: true,
        deletedCount,
        message: `Cleared ${deletedCount} email fetch log(s). You can now re-fetch and re-import the same emails.`,
      });
    } catch (error: any) {
      console.error("Error clearing email fetch logs:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to clear email fetch logs" });
    }
  });

  // Test email connection
  app.post("/api/data-sources/:id/test-email", async (req, res) => {
    try {
      const dataSourceId = req.params.id;

      // Get the data source
      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      const emailSettings = (dataSource as any).emailSettings;
      if (!emailSettings || !emailSettings.host || !emailSettings.username) {
        return res.status(400).json({ error: "Email settings not configured" });
      }

      // Resolve password from environment variable if it uses env: prefix syntax
      const resolvedSettings = { ...emailSettings };
      if (emailSettings.password && emailSettings.password.startsWith("env:")) {
        const envVarName = emailSettings.password.substring(4);
        const envPassword = process.env[envVarName];
        if (envPassword) {
          resolvedSettings.password = envPassword;
        } else {
          return res.status(400).json({
            error: `Email password secret '${envVarName}' is not configured. Please add it to your environment secrets.`,
          });
        }
      }

      const { testEmailConnection } = await import("./emailFetcher");

      const result = await testEmailConnection(resolvedSettings);

      if (result.success) {
        res.json({ success: true, folderCount: result.folderCount });
      } else {
        res.status(400).json({ success: false, error: result.error });
      }
    } catch (error: any) {
      console.error("Error testing email:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to test email connection" });
    }
  });

  // Re-import last file with current cleaning settings
  app.post("/api/data-sources/:id/reimport", async (req, res) => {
    try {
      const dataSourceId = req.params.id;

      // Get the data source
      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // Get the last uploaded file for this data source
      const lastFile = await storage.getLatestFile(dataSourceId);
      if (!lastFile) {
        return res.status(404).json({
          error:
            "No files found for this data source. Please upload a file first.",
        });
      }

      // Get existing inventory items for this data source
      const existingItems = await storage.getInventoryItems(dataSourceId);
      if (existingItems.length === 0) {
        return res.status(404).json({
          error: "No inventory items found. Please upload a file first.",
        });
      }

      // Delete existing inventory items
      await storage.deleteInventoryItemsByDataSource(dataSourceId);

      // Helper function to get prefix for a style
      // For sale files, strips "Sale" or "Sales" from the prefix to match regular file naming
      // Re-apply cleaning to raw data and re-import
      const cleaningConfig = (dataSource.cleaningConfig as any) || {};
      const inventoryItems = existingItems.map((item) => {
        // Get raw style from rawData if available, otherwise use current style without prefix
        let rawStyle = (item.rawData as any)?.style || item.style || "";

        // Remove any existing prefix (data source name or custom prefix)
        // The style format is "Prefix StyleValue", so split by first space
        const parts = rawStyle.split(" ");
        if (parts.length > 1) {
          // Check if first part matches data source name or any custom prefix
          const firstPart = parts[0];
          const isPrefix =
            firstPart === dataSource.name ||
            (cleaningConfig?.stylePrefixRules || []).some(
              (r: any) => r.prefix === firstPart,
            );
          if (isPrefix) {
            rawStyle = parts.slice(1).join(" ");
          }
        }

        // Apply cleaning to style
        const cleanedStyle = applyCleaningToValue(
          rawStyle,
          cleaningConfig,
          "style",
        );
        // If item has a brand (from store_multibrand vendor column), use brand as prefix
        const prefix = item.brand
          ? String(item.brand).trim()
          : cleanedStyle
            ? getStylePrefix(cleanedStyle, dataSource, cleaningConfig)
            : dataSource.name;

        return {
          dataSourceId,
          fileId: lastFile.id,
          sku: item.sku || "",
          style: cleanedStyle ? `${prefix} ${cleanedStyle}` : cleanedStyle,
          size: item.size,
          color: item.color,
          stock: item.stock,
          cost: item.cost,
          price: item.price,
          shipDate: item.shipDate,
          // CRITICAL: Preserve hasFutureStock flags from parser
          hasFutureStock: item.hasFutureStock || false,
          preserveZeroStock: item.preserveZeroStock || false,
          rawData: item.rawData as any,
        };
      });

      // Step 1: Clean data (remove items without size, fix colors, remove duplicates)
      const cleanResult = await cleanInventoryData(
        inventoryItems,
        dataSource.name,
      );

      // Step 1.5: Apply configurable import rules (pricing, discontinued, required fields, etc.)
      const importRulesConfig = {
        discontinuedRules:
          (dataSource as any).discontinuedConfig ||
          (dataSource as any).discontinuedRules,
        salePriceConfig: (dataSource as any).salePriceConfig,
        priceFloorCeiling: (dataSource as any).priceFloorCeiling,
        minStockThreshold: (dataSource as any).minStockThreshold,
        requiredFieldsConfig: (dataSource as any).requiredFieldsConfig,
        stockThresholdEnabled: (dataSource as any).stockThresholdEnabled,
        dateFormatConfig: (dataSource as any).dateFormatConfig,
        valueReplacementRules: (dataSource as any).valueReplacementRules,
        regularPriceConfig: (dataSource as any).regularPriceConfig,
        cleaningConfig: dataSource.cleaningConfig,
        futureStockConfig: (dataSource as any).futureStockConfig,
        stockValueConfig: (dataSource as any).stockValueConfig,
        complexStockConfig: (dataSource as any).complexStockConfig,
      };
      const importRulesResult = await applyImportRules(
        cleanResult.items,
        importRulesConfig,
        rows, // CRITICAL FIX: Pass rows parameter to match Manual/Email import behavior
      );

      // Step 2: Apply variant rules (filter zero stock, expand sizes, etc.)
      const ruleResult = await applyVariantRules(
        importRulesResult.items,
        dataSourceId,
      );

      // Step 2.5: Apply price-based size expansion if configured
      let priceBasedExpansionCount = 0;
      let itemsAfterExpansion = ruleResult.items;
      const priceBasedExpansionConfig = (dataSource as any)
        .priceBasedExpansionConfig;

      if (
        priceBasedExpansionConfig?.enabled &&
        (priceBasedExpansionConfig.tiers?.length > 0 ||
          (priceBasedExpansionConfig.defaultExpandDown ?? 0) > 0 ||
          (priceBasedExpansionConfig.defaultExpandUp ?? 0) > 0)
      ) {
        const shopifyStoreId = (dataSource as any).shopifyStoreId;
        if (shopifyStoreId) {
          console.log(
            `[Jovani Import] Applying price-based size expansion for data source "${dataSource.name}"`,
          );
          try {
            const cacheVariants =
              await storage.getVariantCacheProductStyles(shopifyStoreId);
            const stylePriceMap = buildStylePriceMapFromCache(cacheVariants);
            console.log(
              `[Jovani Import] Built style price map with ${stylePriceMap.size} styles`,
            );

            const expansionResult = applyPriceBasedExpansion(
              ruleResult.items,
              priceBasedExpansionConfig,
              stylePriceMap,
              (dataSource as any).sizeLimitConfig,
            );
            itemsAfterExpansion = expansionResult.items;
            priceBasedExpansionCount = expansionResult.addedCount;

            if (priceBasedExpansionCount > 0) {
              console.log(
                `[Jovani Import] Price-based expansion added ${priceBasedExpansionCount} size variants`,
              );
            }
          } catch (expansionError) {
            console.error(
              `[Jovani Import] Price-based expansion error:`,
              expansionError,
            );
          }
        }
      }

      // ============================================================
      // CALCULATE STOCK INFO FOR EACH ITEM
      // ============================================================
      // Dedup by style-color-size and zero out stock for future ship dates
      const dedupOffsetReimport =
        (dataSource as any).stockInfoConfig?.dateOffsetDays ?? 0;
      const dedupResultReimport = deduplicateAndZeroFutureStock(
        itemsAfterExpansion,
        dedupOffsetReimport,
      );
      itemsAfterExpansion = dedupResultReimport.items;

      const stockInfoRuleReimport = await getStockInfoRule(dataSource);
      if (stockInfoRuleReimport) {
        console.log(
          `[Reimport] Calculating stockInfo for ${itemsAfterExpansion.length} items using rule: "${stockInfoRuleReimport.name}"`,
        );
        itemsAfterExpansion = itemsAfterExpansion.map((item: any) => ({
          ...item,
          stockInfo: calculateItemStockInfo(item, stockInfoRuleReimport),
        }));
      }

      let importedCount = 0;
      if (itemsAfterExpansion.length > 0) {
        const created = await storage.createInventoryItems(
          itemsAfterExpansion as any,
        );
        importedCount = created.length;
      }

      // Update data source last sync time
      await storage.updateDataSource(dataSourceId, {});

      // Await auto-consolidation so frontend cache invalidation gets fully consolidated data
      try {
        await triggerAutoConsolidationAfterImport(dataSourceId);
      } catch (err: any) {
        console.error(
          "Error in auto-consolidation after reimport:",
          err.message,
        );
      }

      // Trigger Shopify sync if enabled - sync only this data source + its sale file (same as manual sync)
      triggerShopifySyncAfterImport(dataSourceId).catch((err) => {
        console.error(
          "Error triggering Shopify sync after reimport:",
          err.message,
        );
      });

      let processSummary = "";
      if (cleanResult.noSizeRemoved > 0)
        processSummary += ` (${cleanResult.noSizeRemoved} no size)`;
      if (cleanResult.colorsFixed > 0)
        processSummary += ` (${cleanResult.colorsFixed} colors fixed)`;
      if (cleanResult.aiColorsFixed > 0)
        processSummary += ` (${cleanResult.aiColorsFixed} AI colors fixed)`;
      if (cleanResult.duplicatesRemoved > 0)
        processSummary += ` (${cleanResult.duplicatesRemoved} duplicates)`;
      if (ruleResult.filteredCount > 0)
        processSummary += ` (${ruleResult.filteredCount} filtered)`;
      if (ruleResult.addedCount > 0)
        processSummary += ` (+${ruleResult.addedCount} expanded)`;

      res.json({
        success: true,
        importedItems: importedCount,
        noSizeRemoved: cleanResult.noSizeRemoved,
        colorsFixed: cleanResult.colorsFixed,
        aiColorsFixed: cleanResult.aiColorsFixed,
        duplicatesRemoved: cleanResult.duplicatesRemoved,
        filteredItems: ruleResult.filteredCount,
        addedItems: ruleResult.addedCount,
        message: `Re-imported ${importedCount} items with updated cleaning settings${processSummary}`,
      });
    } catch (error: any) {
      console.error("Error re-importing:", error);
      res.status(500).json({ error: error.message || "Failed to re-import" });
    }
  });

  // ========== INVENTORY ITEMS ==========

  // Get inventory items (optionally filtered by data source)
  app.get("/api/inventory", async (req, res) => {
    try {
      const dataSourceId = req.query.dataSourceId as string | undefined;
      const items = await storage.getInventoryItems(dataSourceId);
      res.json(items);
    } catch (error) {
      console.error("Error fetching inventory:", error);
      res.status(500).json({ error: "Failed to fetch inventory" });
    }
  });

  // Delete inventory items by file
  app.delete("/api/inventory/file/:fileId", async (req, res) => {
    try {
      await storage.deleteInventoryItemsByFile(req.params.fileId);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting inventory items:", error);
      res.status(500).json({ error: "Failed to delete inventory items" });
    }
  });

  // Clear all inventory items (master inventory)
  app.delete("/api/inventory/clear", async (req, res) => {
    try {
      await storage.clearAllInventoryItems();
      res
        .status(200)
        .json({ message: "Master inventory cleared successfully" });
    } catch (error) {
      console.error("Error clearing inventory:", error);
      res.status(500).json({ error: "Failed to clear inventory" });
    }
  });

  // ========== VARIANT RULES ==========

  // Get variant rules (optionally filtered by data source)
  app.get("/api/rules", async (req, res) => {
    try {
      const dataSourceId = req.query.dataSourceId as string | undefined;
      const rules = await storage.getVariantRules(dataSourceId);
      res.json(rules);
    } catch (error) {
      console.error("Error fetching rules:", error);
      res.status(500).json({ error: "Failed to fetch rules" });
    }
  });

  // Create variant rule
  app.post("/api/rules", async (req, res) => {
    try {
      const validated = insertVariantRuleSchema.parse(req.body);
      const created = await storage.createVariantRule(validated);
      res.status(201).json(created);
    } catch (error: any) {
      console.error("Error creating rule:", error);
      res.status(400).json({ error: error.message || "Failed to create rule" });
    }
  });

  // Update variant rule
  app.patch("/api/rules/:id", async (req, res) => {
    try {
      const updated = await storage.updateVariantRule(req.params.id, req.body);
      if (!updated) {
        return res.status(404).json({ error: "Rule not found" });
      }
      res.json(updated);
    } catch (error: any) {
      console.error("Error updating rule:", error);
      res.status(400).json({ error: error.message || "Failed to update rule" });
    }
  });

  // Delete variant rule
  app.delete("/api/rules/:id", async (req, res) => {
    try {
      await storage.deleteVariantRule(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting rule:", error);
      res.status(500).json({ error: "Failed to delete rule" });
    }
  });

  // ========== SHOPIFY METAFIELD RULES ==========

  // Get Shopify metafield rules (optionally filtered by data source)
  app.get("/api/shopify-metafield-rules", async (req, res) => {
    try {
      const dataSourceId = req.query.dataSourceId as string | undefined;
      const rules = await storage.getShopifyMetafieldRules(dataSourceId);
      res.json(rules);
    } catch (error) {
      console.error("Error fetching Shopify metafield rules:", error);
      res
        .status(500)
        .json({ error: "Failed to fetch Shopify metafield rules" });
    }
  });

  // Create Shopify metafield rule
  app.post("/api/shopify-metafield-rules", async (req, res) => {
    try {
      const validated = insertShopifyMetafieldRuleSchema.parse(req.body);
      const created = await storage.createShopifyMetafieldRule(validated);
      res.status(201).json(created);
    } catch (error: any) {
      console.error("Error creating Shopify metafield rule:", error);
      res.status(400).json({
        error: error.message || "Failed to create Shopify metafield rule",
      });
    }
  });

  // Update Shopify metafield rule
  app.patch("/api/shopify-metafield-rules/:id", async (req, res) => {
    try {
      const updated = await storage.updateShopifyMetafieldRule(
        req.params.id,
        req.body,
      );
      if (!updated) {
        return res
          .status(404)
          .json({ error: "Shopify metafield rule not found" });
      }
      res.json(updated);
    } catch (error: any) {
      console.error("Error updating Shopify metafield rule:", error);
      res.status(400).json({
        error: error.message || "Failed to update Shopify metafield rule",
      });
    }
  });

  // Delete Shopify metafield rule
  app.delete("/api/shopify-metafield-rules/:id", async (req, res) => {
    try {
      await storage.deleteShopifyMetafieldRule(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting Shopify metafield rule:", error);
      res
        .status(500)
        .json({ error: "Failed to delete Shopify metafield rule" });
    }
  });

  // ========== CHANNEL INTEGRATIONS ==========

  // Get all channel integrations
  app.get("/api/channels", async (req, res) => {
    try {
      const channels = await storage.getChannelIntegrations();
      res.json(channels);
    } catch (error) {
      console.error("Error fetching channels:", error);
      res.status(500).json({ error: "Failed to fetch channels" });
    }
  });

  // Create channel integration
  app.post("/api/channels", async (req, res) => {
    try {
      const validated = insertChannelIntegrationSchema.parse(req.body);
      const created = await storage.createChannelIntegration(validated);
      res.status(201).json(created);
    } catch (error: any) {
      console.error("Error creating channel:", error);
      res
        .status(400)
        .json({ error: error.message || "Failed to create channel" });
    }
  });

  // Update channel integration
  app.patch("/api/channels/:id", async (req, res) => {
    try {
      const updated = await storage.updateChannelIntegration(
        req.params.id,
        req.body,
      );
      if (!updated) {
        return res.status(404).json({ error: "Channel not found" });
      }
      res.json(updated);
    } catch (error: any) {
      console.error("Error updating channel:", error);
      res
        .status(400)
        .json({ error: error.message || "Failed to update channel" });
    }
  });

  // Delete channel integration
  app.delete("/api/channels/:id", async (req, res) => {
    try {
      await storage.deleteChannelIntegration(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting channel:", error);
      res.status(500).json({ error: "Failed to delete channel" });
    }
  });

  // ========== AI COLUMN MAPPING ==========

  // Analyze Excel file with AI to suggest column mappings
  app.post("/api/ai/analyze-columns", async (req, res) => {
    try {
      const { headers, sampleRows, confidenceThreshold } = req.body;

      if (!headers || !Array.isArray(headers)) {
        return res.status(400).json({ error: "Headers array is required" });
      }

      if (!sampleRows || !Array.isArray(sampleRows)) {
        return res.status(400).json({ error: "Sample rows array is required" });
      }

      const result = await analyzeExcelForMapping(headers, sampleRows);

      // Add confidence warning if below threshold (default 0.7 = 70%)
      const threshold = confidenceThreshold || 0.7;
      const lowConfidence = (result.confidence || 0) < threshold;

      res.json({
        ...result,
        lowConfidence,
        confidenceThreshold: threshold,
        warning: lowConfidence
          ? `AI confidence is ${Math.round((result.confidence || 0) * 100)}% (below ${Math.round(threshold * 100)}% threshold). Please verify column mappings manually before saving.`
          : undefined,
      });
    } catch (error: any) {
      console.error("Error analyzing columns with AI:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to analyze columns" });
    }
  });

  // ========== MASTER INVENTORY ==========

  // Get master inventory with pagination and filtering (OPTIMIZED)
  app.get("/api/inventory/master/paginated", async (req, res) => {
    try {
      const page = Math.max(1, parseInt(req.query.page as string) || 1);
      const limit = Math.min(
        500,
        Math.max(10, parseInt(req.query.limit as string) || 100),
      );
      const search = ((req.query.search as string) || "").trim().toLowerCase();
      const dataSourceId = req.query.dataSourceId as string;
      const sortBy = (req.query.sortBy as string) || "style";
      const sortOrder =
        (req.query.sortOrder as string) === "desc" ? "desc" : "asc";
      const onlyInCache = req.query.onlyInCache === "true";
      const color = (req.query.color as string) || undefined;
      const size = (req.query.size as string) || undefined;
      const priceMin = req.query.priceMin
        ? parseFloat(req.query.priceMin as string)
        : undefined;
      const priceMax = req.query.priceMax
        ? parseFloat(req.query.priceMax as string)
        : undefined;
      const expandedOnly = req.query.expandedOnly === "true" || undefined;
      const hideExpanded = req.query.hideExpanded === "true" || undefined;
      const hasPrice = (req.query.hasPrice as "yes" | "no") || undefined;
      const duplicateStylesOnly =
        req.query.duplicateStylesOnly === "true" || undefined;
      const shipDate = (req.query.shipDate as string) || undefined;
      const sourceType =
        (req.query.sourceType as "inventory" | "sales") || undefined;
      const importedAfter = (req.query.importedAfter as string) || undefined;
      const inStoreOnly = req.query.inStoreOnly === "true" || undefined;

      // Build style -> cached price map from all connected Shopify stores
      // Need to do this BEFORE the main query if filtering by cache
      const stylePriceMap = new Map<string, number>();
      const dataSources = await storage.getDataSources();
      const shopifyStoreIds = new Set<string>();
      for (const ds of dataSources) {
        if ((ds as any).shopifyStoreId) {
          shopifyStoreIds.add((ds as any).shopifyStoreId);
        }
      }

      for (const storeId of shopifyStoreIds) {
        try {
          const cacheVariants =
            await storage.getVariantCacheProductStyles(storeId);
          const storePriceMap = buildStylePriceMapFromCache(cacheVariants);
          for (const [style, price] of storePriceMap) {
            if (
              !stylePriceMap.has(style) ||
              price > stylePriceMap.get(style)!
            ) {
              stylePriceMap.set(style, price);
            }
          }
        } catch (e) {
          // Silently continue if cache fetch fails
        }
      }

      // If onlyInCache filter is enabled, pass cached styles to filter the query
      const cachedStyles = onlyInCache
        ? Array.from(stylePriceMap.keys())
        : undefined;

      const result = await storage.getMasterInventoryPaginated({
        page,
        limit,
        search,
        dataSourceId,
        sortBy,
        sortOrder,
        cachedStyles,
        color,
        size,
        priceMin,
        priceMax,
        expandedOnly,
        hideExpanded,
        hasPrice,
        duplicateStylesOnly,
        shipDate,
        sourceType,
        importedAfter,
        inStoreOnly,
      });

      // Add cachedShopifyPrice to each item
      const itemsWithPrice = result.items.map((item) => {
        let cachedPrice = stylePriceMap.get(item.style || "");
        if (!cachedPrice && (item as any).rawData?.originalStyle) {
          cachedPrice = stylePriceMap.get((item as any).rawData.originalStyle);
        }
        if (!cachedPrice && item.style) {
          const parts = item.style.split(" ");
          if (parts.length >= 2) {
            cachedPrice = stylePriceMap.get(parts[parts.length - 1]);
          }
        }
        return { ...item, cachedShopifyPrice: cachedPrice || null };
      });

      res.json({ ...result, items: itemsWithPrice });
    } catch (error) {
      console.error("Error fetching paginated master inventory:", error);
      res.status(500).json({ error: "Failed to fetch master inventory" });
    }
  });

  app.get("/api/inventory/master/colors", async (req, res) => {
    try {
      const dataSourceId = req.query.dataSourceId as string;
      const colors = await storage.getDistinctInventoryColors(
        dataSourceId || undefined,
      );
      res.json(colors);
    } catch (error) {
      console.error("Error fetching distinct colors:", error);
      res.status(500).json({ error: "Failed to fetch colors" });
    }
  });

  app.get("/api/inventory/master/sizes", async (req, res) => {
    try {
      const dataSourceId = req.query.dataSourceId as string;
      const sizes = await storage.getDistinctInventorySizes(
        dataSourceId || undefined,
      );
      res.json(sizes);
    } catch (error) {
      console.error("Error fetching distinct sizes:", error);
      res.status(500).json({ error: "Failed to fetch sizes" });
    }
  });

  app.get("/api/inventory/master/ship-dates", async (req, res) => {
    try {
      const dataSourceId = req.query.dataSourceId as string;
      const shipDates = await storage.getDistinctShipDates(
        dataSourceId || undefined,
      );
      res.json(shipDates);
    } catch (error) {
      console.error("Error fetching distinct ship dates:", error);
      res.status(500).json({ error: "Failed to fetch ship dates" });
    }
  });

  // Get master inventory count and summary (fast)
  app.get("/api/inventory/master/summary", async (req, res) => {
    try {
      const dataSourceId = req.query.dataSourceId as string;
      const summary = await storage.getMasterInventorySummary(dataSourceId);
      res.json(summary);
    } catch (error) {
      console.error("Error fetching master inventory summary:", error);
      res.status(500).json({ error: "Failed to fetch inventory summary" });
    }
  });

  // Get master inventory (combined from all sources) - LEGACY, loads all items
  // WARNING: This endpoint loads ALL items and can be slow with large inventories
  app.get("/api/inventory/master", async (req, res) => {
    try {
      console.warn(
        "[PERF WARNING] /api/inventory/master called - loading all items. Consider using /api/inventory/master/paginated instead.",
      );
      const items = await storage.getMasterInventory();

      // Build style -> cached price map from all connected Shopify stores
      const stylePriceMap = new Map<string, number>();

      // Get all data sources to find connected Shopify stores
      const dataSources = await storage.getDataSources();
      const shopifyStoreIds = new Set<string>();
      for (const ds of dataSources) {
        if ((ds as any).shopifyStoreId) {
          shopifyStoreIds.add((ds as any).shopifyStoreId);
        }
      }

      // Fetch cached variants from all stores and build price map
      for (const storeId of shopifyStoreIds) {
        try {
          const cacheVariants =
            await storage.getVariantCacheProductStyles(storeId);
          const storePriceMap = buildStylePriceMapFromCache(cacheVariants);
          for (const [style, price] of storePriceMap) {
            if (
              !stylePriceMap.has(style) ||
              price > stylePriceMap.get(style)!
            ) {
              stylePriceMap.set(style, price);
            }
          }
        } catch (e) {
          console.warn(`Failed to get variant cache for store ${storeId}:`, e);
        }
      }

      // Add cachedShopifyPrice to each item
      // Try matching on: 1) full style, 2) originalStyle from rawData, 3) style number extracted from prefixed style
      const itemsWithCachedPrice = items.map((item) => {
        let cachedPrice = stylePriceMap.get(item.style || "");

        // If no match, try originalStyle from rawData
        if (!cachedPrice && item.rawData?.originalStyle) {
          cachedPrice = stylePriceMap.get(item.rawData.originalStyle);
        }

        // If still no match, try extracting style number from prefixed style (e.g., "Tarik Ediz PERS0010" -> "PERS0010")
        if (!cachedPrice && item.style) {
          const parts = item.style.split(" ");
          if (parts.length >= 2) {
            const styleNumber = parts[parts.length - 1]; // Last part is usually the style number
            cachedPrice = stylePriceMap.get(styleNumber);
          }
        }

        return {
          ...item,
          cachedShopifyPrice: cachedPrice || null,
        };
      });

      res.json(itemsWithCachedPrice);
    } catch (error) {
      console.error("Error fetching master inventory:", error);
      res.status(500).json({ error: "Failed to fetch master inventory" });
    }
  });

  // Download master inventory as CSV
  app.get("/api/inventory/master/download", async (req, res) => {
    try {
      const items = await storage.getMasterInventory();

      // Get all metafield rules for looking up messages
      const metafieldRules = await storage.getShopifyMetafieldRules();

      // Build lookup map by data source name
      const rulesBySource = new Map<string, (typeof metafieldRules)[0]>();
      for (const rule of metafieldRules) {
        if (rule.dataSourceId && rule.enabled) {
          // We need to get the data source name - for now, match by dataSourceId
          // The items have sourceName, so we need to map it
          const dataSource = await storage.getDataSource(rule.dataSourceId);
          if (dataSource) {
            rulesBySource.set(dataSource.name, rule);
          }
        }
      }

      // Generate CSV content with In stock Info column
      const headers = [
        "style",
        "colorsize",
        "stock",
        "price",
        "shipDate",
        "company",
        "In stock Info",
      ];
      const csvRows = [headers.join(",")];

      for (const item of items) {
        // Combine color and size into colorsize format: "Color > Size >"
        const sizeStr = String(item.size ?? "");
        const colorsize =
          [item.color || "", sizeStr].filter(Boolean).join(" > ") +
          (item.color || sizeStr ? " >" : "");

        // Get the metafield rule for this item's source
        const rule = rulesBySource.get(item.sourceName);
        let inStockInfo = "";

        if (rule) {
          const stockValue = item.stock ?? 0;
          const threshold = rule.stockThreshold ?? 0;

          if (stockValue === 0) {
            // Out of stock
            inStockInfo = rule.outOfStockMessage || "";
          } else if (stockValue > threshold) {
            // Check if this is a size expansion item
            const isExpanded = (item as any).rawData?._expanded === true;
            if (isExpanded && rule.sizeExpansionMessage) {
              inStockInfo = rule.sizeExpansionMessage;
            } else {
              inStockInfo = rule.inStockMessage || "";
            }
          }
        }

        const row = [
          `"${(item.style || item.sku || "").replace(/"/g, '""')}"`,
          `"${colorsize.replace(/"/g, '""')}"`,
          item.stock ?? 0,
          item.price ? `"${item.price.replace(/"/g, '""')}"` : "",
          item.shipDate ? `"${item.shipDate.replace(/"/g, '""')}"` : "",
          `"${(item.sourceName || "").replace(/"/g, '""')}"`,
          `"${inStockInfo.replace(/"/g, '""')}"`,
        ];
        csvRows.push(row.join(","));
      }

      const csv = csvRows.join("\n");

      res.setHeader("Content-Type", "text/csv");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="master_inventory_${new Date().toISOString().split("T")[0]}.csv"`,
      );
      res.send(csv);
    } catch (error) {
      console.error("Error downloading master inventory:", error);
      res.status(500).json({ error: "Failed to download master inventory" });
    }
  });

  // Import inventory items from parsed file data
  app.post("/api/inventory/import", async (req, res) => {
    try {
      const { dataSourceId, items } = req.body;

      if (!dataSourceId || !items || !Array.isArray(items)) {
        return res
          .status(400)
          .json({ error: "dataSourceId and items array are required" });
      }

      // SAFETY NET: Block empty import or massive drop from deleting all data
      const existingCount =
        await storage.getInventoryItemCountByDataSource(dataSourceId);
      if (items.length === 0 && existingCount > 0) {
        console.error(
          `[Manual Import] SAFETY BLOCK: Import has 0 items but data source has ${existingCount} existing items. ` +
            `Blocking import to prevent data loss.`,
        );
        return res.status(400).json({
          error: "Import blocked - no items provided",
          safetyBlock: true,
          message:
            `SAFETY NET: Import has 0 items but would delete ${existingCount} existing items. ` +
            `This appears to be a corrupted or empty file. Import blocked to protect your data.`,
        });
      }
      const manualDataSource = await storage.getDataSource(dataSourceId);
      if (manualDataSource) {
        const safetyCheck = checkSafetyThreshold(manualDataSource, existingCount, items.length, "Manual Import");
        if (safetyCheck.blocked) {
          return res.status(400).json({
            error: safetyCheck.message,
            safetyBlock: true,
            existingCount,
            newCount: items.length,
            dropPercent: safetyCheck.dropPercent,
          });
        }
      }

      // Delete existing items for this data source before importing new ones
      await storage.deleteInventoryItemsByDataSource(dataSourceId);

      // Insert new items
      const inventoryItems = items.map((item: any) => ({
        dataSourceId,
        sku: item.sku || "",
        style: item.style || null,
        size: item.size != null ? String(item.size) : null,
        color: item.color || null,
        stock:
          typeof item.stock === "number"
            ? item.stock
            : parseInt(item.stock) || 0,
        cost: item.cost || null,
        price: item.price || null,
        shipDate: item.shipDate || null,
        rawData: item.rawData || null,
      }));

      const created = await storage.createInventoryItems(inventoryItems);

      // Await auto-consolidation so frontend cache invalidation gets fully consolidated data
      try {
        await triggerAutoConsolidationAfterImport(dataSourceId);
      } catch (err: any) {
        console.error("Error in auto-consolidation after import:", err.message);
      }

      // Trigger Shopify sync if enabled - sync only this data source + its sale file
      // Note: Using only triggerShopifySyncAfterImport to avoid duplicate syncs
      triggerShopifySyncAfterImport(dataSourceId).catch((err) => {
        console.error(
          "Error triggering Shopify sync after import:",
          err.message,
        );
      });

      res.json({
        success: true,
        count: created.length,
        message: `Imported ${created.length} inventory items`,
      });
    } catch (error: any) {
      console.error("Error importing inventory:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to import inventory" });
    }
  });

  // Normalize all inventory colors (convert ALL CAPS to Title Case)
  app.post("/api/inventory/normalize-colors", async (req, res) => {
    // Respond immediately and process in background
    res.json({
      success: true,
      message: "Color normalization started - check logs for progress",
    });

    try {
      console.log("[Color Normalize] Starting color normalization...");
      const dataSources = await storage.getDataSources();

      let totalUpdated = 0;

      for (const ds of dataSources) {
        const items = await storage.getInventoryItems(ds.id);
        let dsUpdated = 0;

        for (const item of items) {
          if (!item.color) continue;

          const originalColor = item.color;
          const lettersOnly = originalColor.replace(/[^a-zA-Z]/g, "");
          if (
            lettersOnly.length > 0 &&
            lettersOnly === lettersOnly.toUpperCase()
          ) {
            let normalized = originalColor.toLowerCase();
            normalized = normalized.replace(
              /(^|[\s\/\-&+])([a-z])/g,
              (match, delimiter, letter) => {
                return delimiter + letter.toUpperCase();
              },
            );

            if (normalized !== originalColor) {
              await storage.updateInventoryItem(item.id, { color: normalized });
              totalUpdated++;
              dsUpdated++;
            }
          }
        }

        if (dsUpdated > 0) {
          console.log(
            `[Color Normalize] ${ds.name}: normalized ${dsUpdated} colors`,
          );
        }
      }

      console.log(
        `[Color Normalize] Completed - normalized ${totalUpdated} total color values`,
      );
    } catch (error: any) {
      console.error("[Color Normalize] Error:", error.message);
    }
  });

  // Backfill canonical SKUs for all inventory items using efficient batch SQL
  // Format: Style-Color-Size lowercase (e.g., "jovani-60695-gray-6")
  app.post("/api/inventory/backfill-skus", async (req, res) => {
    try {
      console.log(
        "[SKU Backfill] Starting canonical SKU backfill with batch SQL...",
      );

      const rowCount = await storage.backfillCanonicalSkus();

      console.log(`[SKU Backfill] Completed - updated ${rowCount} SKUs`);

      res.json({
        success: true,
        message: `Updated ${rowCount} inventory items with canonical SKUs`,
        count: rowCount,
      });
    } catch (error: any) {
      console.error("[SKU Backfill] Error:", error.message);
      res
        .status(500)
        .json({ error: error.message || "Failed to backfill SKUs" });
    }
  });

  // ========== COLOR MAPPINGS ==========

  // Get all color mappings
  app.get("/api/color-mappings", async (req, res) => {
    try {
      const mappings = await storage.getColorMappings();
      res.json(mappings);
    } catch (error) {
      console.error("Error fetching color mappings:", error);
      res.status(500).json({ error: "Failed to fetch color mappings" });
    }
  });

  // Upload color mapping Excel file (Column A = bad color, Column B = good color)
  app.post(
    "/api/color-mappings/upload",
    upload.single("file"),
    async (req, res) => {
      try {
        if (!req.file) {
          return res.status(400).json({ error: "No file uploaded" });
        }

        const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const data = XLSX.utils.sheet_to_json(worksheet, {
          header: 1,
        }) as any[][];

        if (data.length === 0) {
          return res.status(400).json({ error: "File is empty" });
        }

        // Check if first row looks like headers
        let startRow = 0;
        const firstRow = data[0];
        if (firstRow && typeof firstRow[0] === "string") {
          const firstCell = firstRow[0].toLowerCase();
          if (
            firstCell.includes("bad") ||
            firstCell.includes("old") ||
            firstCell.includes("from") ||
            firstCell.includes("original")
          ) {
            startRow = 1; // Skip header row
          }
        }

        // Parse mappings from Excel (Column A = bad, Column B = good)
        const mappings: { badColor: string; goodColor: string }[] = [];
        for (let i = startRow; i < data.length; i++) {
          const row = data[i];
          if (row && row[0] !== undefined && row[1] !== undefined) {
            const badColor = String(row[0]).trim();
            const goodColor = String(row[1]).trim();
            if (badColor && goodColor) {
              mappings.push({ badColor, goodColor });
            }
          }
        }

        if (mappings.length === 0) {
          return res.status(400).json({
            error:
              "No valid color mappings found in file. Column A should be bad color, Column B should be good color.",
          });
        }

        // Upsert mappings: update existing, add new ones
        const result = await storage.upsertColorMappings(mappings);

        res.json({
          success: true,
          count: result.created + result.updated,
          created: result.created,
          updated: result.updated,
          message: `Imported ${result.created} new color mappings, updated ${result.updated} existing mappings`,
        });
      } catch (error: any) {
        console.error("Error uploading color mappings:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to upload color mappings" });
      }
    },
  );

  // Add a single color mapping
  app.post("/api/color-mappings", async (req, res) => {
    try {
      const { badColor, goodColor } = req.body;
      if (!badColor || !goodColor) {
        return res
          .status(400)
          .json({ error: "badColor and goodColor are required" });
      }
      const created = await storage.createColorMappings([
        { badColor, goodColor },
      ]);
      res.status(201).json(created[0]);
    } catch (error) {
      console.error("Error creating color mapping:", error);
      res.status(500).json({ error: "Failed to create color mapping" });
    }
  });

  // Delete a color mapping
  app.delete("/api/color-mappings/:id", async (req, res) => {
    try {
      await storage.deleteColorMapping(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting color mapping:", error);
      res.status(500).json({ error: "Failed to delete color mapping" });
    }
  });

  // Clear all color mappings
  app.delete("/api/color-mappings", async (req, res) => {
    try {
      await storage.clearAllColorMappings();
      res.json({ success: true, message: "All color mappings cleared" });
    } catch (error) {
      console.error("Error clearing color mappings:", error);
      res.status(500).json({ error: "Failed to clear color mappings" });
    }
  });

  // ========== DATA SOURCE TEMPLATES ==========

  // Get all templates
  app.get("/api/templates", async (req, res) => {
    try {
      const templates = await storage.getDataSourceTemplates();
      res.json(templates);
    } catch (error) {
      console.error("Error fetching templates:", error);
      res.status(500).json({ error: "Failed to fetch templates" });
    }
  });

  // Save current data source as template (includes all settings + Rule Engine rules)
  app.post("/api/templates", async (req, res) => {
    try {
      const { name, description, dataSourceId } = req.body;

      if (!name) {
        return res.status(400).json({ error: "Template name is required" });
      }

      if (!dataSourceId) {
        return res.status(400).json({ error: "Data source ID is required" });
      }

      // Get the data source to copy its config
      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // Get all Variant Rules (size expansion) for this data source
      const variantRules =
        await storage.getVariantRulesByDataSource(dataSourceId);

      // Convert variant rules to a serializable format (remove auto-generated fields)
      const serializedVariantRules = variantRules.map((rule) => ({
        name: rule.name,
        stockMin: rule.stockMin,
        stockMax: rule.stockMax,
        sizes: rule.sizes,
        colors: rule.colors,
        expandSizes: rule.expandSizes,
        sizeSystem: rule.sizeSystem,
        sizeStep: rule.sizeStep,
        expandDownCount: rule.expandDownCount,
        expandUpCount: rule.expandUpCount,
        minTriggerStock: rule.minTriggerStock,
        expandedStock: rule.expandedStock,
        priority: rule.priority,
        enabled: rule.enabled,
      }));

      // Get all Shopify Metafield Rules (stock info messages) for this data source
      const metafieldRules =
        await storage.getShopifyMetafieldRulesByDataSource(dataSourceId);

      // Convert metafield rules to a serializable format
      const serializedMetafieldRules = metafieldRules.map((rule) => ({
        name: rule.name,
        metafieldNamespace: rule.metafieldNamespace,
        metafieldKey: rule.metafieldKey,
        stockThreshold: rule.stockThreshold,
        inStockMessage: rule.inStockMessage,
        sizeExpansionMessage: rule.sizeExpansionMessage,
        outOfStockMessage: rule.outOfStockMessage,
        futureDateMessage: rule.futureDateMessage,
        dateOffsetDays: rule.dateOffsetDays,
        enabled: rule.enabled,
      }));

      const template = await storage.createDataSourceTemplate({
        name,
        description: description || `Template from ${dataSource.name}`,
        type: dataSource.type || "manual",
        columnMapping: dataSource.columnMapping,
        cleaningConfig: dataSource.cleaningConfig,
        ingestionMode: dataSource.ingestionMode || "single",
        updateStrategy: (dataSource as any).updateStrategy || "replace",
        autoUpdate: dataSource.autoUpdate || false,
        updateFrequency: dataSource.updateFrequency,
        updateTime: dataSource.updateTime,
        connectionDetails: dataSource.connectionDetails,
        emailSettings: (dataSource as any).emailSettings,
        variantRules: serializedVariantRules,
        metafieldRules: serializedMetafieldRules,
        stockInfoConfig: (dataSource as any).stockInfoConfig,
        variantSyncConfig: (dataSource as any).variantSyncConfig,
        sourceType: (dataSource as any).sourceType || "inventory",
        salesConfig: (dataSource as any).salesConfig,
        regularPriceConfig: (dataSource as any).regularPriceConfig,
      });

      res.status(201).json(template);
    } catch (error) {
      console.error("Error creating template:", error);
      res.status(500).json({ error: "Failed to create template" });
    }
  });

  // Delete a template
  app.delete("/api/templates/:id", async (req, res) => {
    try {
      await storage.deleteDataSourceTemplate(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting template:", error);
      res.status(500).json({ error: "Failed to delete template" });
    }
  });

  // Create data source from template (restores all settings + Rule Engine rules)
  app.post("/api/templates/:id/create-source", async (req, res) => {
    try {
      const { name } = req.body;

      if (!name) {
        return res.status(400).json({ error: "Data source name is required" });
      }

      // Get the template
      const template = await storage.getDataSourceTemplate(req.params.id);
      if (!template) {
        return res.status(404).json({ error: "Template not found" });
      }

      // Create new data source with template config (all settings)
      const dataSource = await storage.createDataSource({
        name,
        type: (template as any).type || "manual",
        columnMapping: template.columnMapping as any,
        cleaningConfig: template.cleaningConfig as any,
        ingestionMode: (template.ingestionMode || "single") as any,
        updateStrategy: (template as any).updateStrategy || "replace",
        autoUpdate: (template as any).autoUpdate || false,
        updateFrequency: (template as any).updateFrequency,
        updateTime: (template as any).updateTime,
        connectionDetails: (template as any).connectionDetails as any,
        emailSettings: (template as any).emailSettings as any,
        stockInfoConfig: (template as any).stockInfoConfig as any,
        variantSyncConfig: (template as any).variantSyncConfig as any,
        sourceType: (template as any).sourceType || "inventory",
        salesConfig: (template as any).salesConfig as any,
        regularPriceConfig: (template as any).regularPriceConfig as any,
      });

      // Recreate Variant Rules (size expansion) from template
      const templateVariantRules =
        ((template as any).variantRules as any[]) || [];
      for (const rule of templateVariantRules) {
        try {
          await storage.createVariantRule({
            name: rule.name,
            dataSourceId: dataSource.id,
            stockMin: rule.stockMin,
            stockMax: rule.stockMax,
            sizes: rule.sizes,
            colors: rule.colors,
            expandSizes: rule.expandSizes ?? false,
            sizeSystem: rule.sizeSystem ?? "numeric",
            sizeStep: rule.sizeStep ?? 2,
            expandDownCount: rule.expandDownCount ?? 0,
            expandUpCount: rule.expandUpCount ?? 0,
            minTriggerStock: rule.minTriggerStock,
            expandedStock: rule.expandedStock,
            priority: rule.priority,
            enabled: rule.enabled ?? true,
          });
        } catch (ruleError) {
          console.error(
            `Failed to create variant rule ${rule.name}:`,
            ruleError,
          );
        }
      }

      // Recreate Shopify Metafield Rules (stock info messages) from template
      const templateMetafieldRules =
        ((template as any).metafieldRules as any[]) || [];
      for (const rule of templateMetafieldRules) {
        try {
          await storage.createShopifyMetafieldRule({
            name: rule.name,
            dataSourceId: dataSource.id,
            metafieldNamespace: rule.metafieldNamespace ?? "my_fields",
            metafieldKey: rule.metafieldKey ?? "stock_info",
            stockThreshold: rule.stockThreshold ?? 0,
            inStockMessage: rule.inStockMessage,
            sizeExpansionMessage: rule.sizeExpansionMessage,
            outOfStockMessage: rule.outOfStockMessage,
            futureDateMessage: rule.futureDateMessage,
            dateOffsetDays: rule.dateOffsetDays ?? 0,
            enabled: rule.enabled ?? true,
          });
        } catch (ruleError) {
          console.error(
            `Failed to create metafield rule ${rule.name}:`,
            ruleError,
          );
        }
      }

      res.status(201).json(dataSource);
    } catch (error) {
      console.error("Error creating data source from template:", error);
      res
        .status(500)
        .json({ error: "Failed to create data source from template" });
    }
  });

  // Apply template to an existing data source (restores all settings + Rule Engine rules)
  // Note: Preserves existing connection/email settings if template doesn't have them
  // Safety: Creates new rules FIRST, only deletes old rules after ALL new rules succeed
  app.post("/api/templates/:id/apply/:dataSourceId", async (req, res) => {
    try {
      const { id: templateId, dataSourceId } = req.params;

      // Get the template
      const template = await storage.getDataSourceTemplate(templateId);
      if (!template) {
        return res.status(404).json({ error: "Template not found" });
      }

      // Get the existing data source
      const existingDataSource = await storage.getDataSource(dataSourceId);
      if (!existingDataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // Collect existing rules BEFORE making any changes
      const existingVariantRules =
        await storage.getVariantRulesByDataSource(dataSourceId);
      const existingMetafieldRules =
        await storage.getShopifyMetafieldRulesByDataSource(dataSourceId);
      const existingVariantRuleCount = existingVariantRules.length;
      const existingMetafieldRuleCount = existingMetafieldRules.length;

      // STEP 1: Create all new rules FIRST (before deleting anything)
      // This ensures we don't lose rules if creation fails

      // Create Variant Rules
      const templateVariantRules =
        ((template as any).variantRules as any[]) || [];
      const createdVariantRuleIds: string[] = [];
      const failedVariantRules: string[] = [];

      for (const rule of templateVariantRules) {
        try {
          const newRule = await storage.createVariantRule({
            name: rule.name,
            dataSourceId: dataSourceId,
            stockMin: rule.stockMin,
            stockMax: rule.stockMax,
            sizes: rule.sizes,
            colors: rule.colors,
            expandSizes: rule.expandSizes ?? false,
            sizeSystem: rule.sizeSystem ?? "numeric",
            sizeStep: rule.sizeStep ?? 2,
            expandDownCount: rule.expandDownCount ?? 0,
            expandUpCount: rule.expandUpCount ?? 0,
            minTriggerStock: rule.minTriggerStock,
            expandedStock: rule.expandedStock,
            priority: rule.priority,
            enabled: rule.enabled ?? true,
          });
          createdVariantRuleIds.push(newRule.id);
        } catch (ruleError) {
          console.error(
            `Failed to create variant rule ${rule.name}:`,
            ruleError,
          );
          failedVariantRules.push(rule.name);
        }
      }

      // Create Metafield Rules
      const templateMetafieldRules =
        ((template as any).metafieldRules as any[]) || [];
      const createdMetafieldRuleIds: string[] = [];
      const failedMetafieldRules: string[] = [];

      for (const rule of templateMetafieldRules) {
        try {
          const newRule = await storage.createShopifyMetafieldRule({
            name: rule.name,
            dataSourceId: dataSourceId,
            metafieldNamespace: rule.metafieldNamespace ?? "my_fields",
            metafieldKey: rule.metafieldKey ?? "stock_info",
            stockThreshold: rule.stockThreshold ?? 0,
            inStockMessage: rule.inStockMessage,
            sizeExpansionMessage: rule.sizeExpansionMessage,
            outOfStockMessage: rule.outOfStockMessage,
            futureDateMessage: rule.futureDateMessage,
            dateOffsetDays: rule.dateOffsetDays ?? 0,
            enabled: rule.enabled ?? true,
          });
          createdMetafieldRuleIds.push(newRule.id);
        } catch (ruleError) {
          console.error(
            `Failed to create metafield rule ${rule.name}:`,
            ruleError,
          );
          failedMetafieldRules.push(rule.name);
        }
      }

      // If any rules failed to create, rollback by deleting the ones we did create and abort
      const allFailedRules = [...failedVariantRules, ...failedMetafieldRules];
      if (allFailedRules.length > 0) {
        console.log(
          `Rolling back ${createdVariantRuleIds.length} variant rules and ${createdMetafieldRuleIds.length} metafield rules due to failures`,
        );
        for (const ruleId of createdVariantRuleIds) {
          try {
            await storage.deleteVariantRule(ruleId);
          } catch (e) {
            console.error(`Failed to rollback variant rule ${ruleId}:`, e);
          }
        }
        for (const ruleId of createdMetafieldRuleIds) {
          try {
            await storage.deleteShopifyMetafieldRule(ruleId);
          } catch (e) {
            console.error(`Failed to rollback metafield rule ${ruleId}:`, e);
          }
        }
        return res.status(500).json({
          error: `Failed to create rules: ${allFailedRules.join(", ")}. Template not applied.`,
          failedRules: allFailedRules,
        });
      }

      // STEP 2: All new rules created successfully - now delete old rules
      for (const rule of existingVariantRules) {
        try {
          await storage.deleteVariantRule(rule.id);
        } catch (deleteError) {
          console.error(
            `Failed to delete old variant rule ${rule.id}:`,
            deleteError,
          );
        }
      }
      for (const rule of existingMetafieldRules) {
        try {
          await storage.deleteShopifyMetafieldRule(rule.id);
        } catch (deleteError) {
          console.error(
            `Failed to delete old metafield rule ${rule.id}:`,
            deleteError,
          );
        }
      }

      // STEP 3: Update the data source with template config (preserves name)
      // Build update payload - only overwrite fields that exist in template
      // Preserve existing connection/email settings if template doesn't have them
      const updatePayload: any = {
        type: (template as any).type || existingDataSource.type,
        columnMapping:
          template.columnMapping ?? existingDataSource.columnMapping,
        cleaningConfig:
          template.cleaningConfig ?? existingDataSource.cleaningConfig,
        ingestionMode: (template.ingestionMode ||
          existingDataSource.ingestionMode ||
          "single") as any,
        updateStrategy:
          (template as any).updateStrategy ||
          (existingDataSource as any).updateStrategy ||
          "replace",
        autoUpdate:
          (template as any).autoUpdate ??
          existingDataSource.autoUpdate ??
          false,
        updateFrequency:
          (template as any).updateFrequency ??
          existingDataSource.updateFrequency,
        updateTime:
          (template as any).updateTime ?? existingDataSource.updateTime,
        // Preserve existing connection details if template doesn't have them
        connectionDetails:
          (template as any).connectionDetails ??
          existingDataSource.connectionDetails,
        // Preserve existing email settings if template doesn't have them (important for credentials)
        emailSettings:
          (template as any).emailSettings ??
          (existingDataSource as any).emailSettings,
        stockInfoConfig:
          (template as any).stockInfoConfig ??
          (existingDataSource as any).stockInfoConfig,
        variantSyncConfig:
          (template as any).variantSyncConfig ??
          (existingDataSource as any).variantSyncConfig,
        // Rule Engine settings
        sourceType:
          (template as any).sourceType ??
          (existingDataSource as any).sourceType ??
          "inventory",
        salesConfig:
          (template as any).salesConfig ??
          (existingDataSource as any).salesConfig,
        regularPriceConfig:
          (template as any).regularPriceConfig ??
          (existingDataSource as any).regularPriceConfig,
      };

      const updatedDataSource = await storage.updateDataSource(
        dataSourceId,
        updatePayload,
      );

      res.json({
        success: true,
        message: `Template applied successfully. Replaced ${existingVariantRuleCount} variant rules with ${createdVariantRuleIds.length} and ${existingMetafieldRuleCount} metafield rules with ${createdMetafieldRuleIds.length} from template.`,
        dataSource: updatedDataSource,
      });
    } catch (error) {
      console.error("Error applying template:", error);
      res.status(500).json({ error: "Failed to apply template" });
    }
  });

  // Error Reporting Settings
  app.get("/api/settings/error-reports", async (req, res) => {
    try {
      const settings = await storage.getAppSetting("error_report_settings");
      if (!settings) {
        res.json({ enabled: false, recipientEmail: "", sendTime: "08:00" });
        return;
      }
      // Mask password for security
      const safeSettings = {
        ...settings,
        smtpPassword: settings.smtpPassword ? "********" : undefined,
      };
      res.json(safeSettings);
    } catch (error) {
      console.error("Error getting error report settings:", error);
      res.status(500).json({ error: "Failed to get settings" });
    }
  });

  app.post("/api/settings/error-reports", async (req, res) => {
    try {
      const newSettings = req.body;

      // Get existing settings to preserve SMTP credentials if not provided
      const existingSettings =
        (await storage.getAppSetting("error_report_settings")) || {};

      // Merge settings - preserve existing SMTP credentials when new ones are undefined or empty
      const mergedSettings = {
        enabled: newSettings.enabled,
        recipientEmail: newSettings.recipientEmail,
        sendTime: newSettings.sendTime,
        smtpHost: newSettings.smtpHost || existingSettings.smtpHost,
        smtpPort: newSettings.smtpPort || existingSettings.smtpPort,
        smtpSecure:
          newSettings.smtpSecure !== undefined
            ? newSettings.smtpSecure
            : existingSettings.smtpSecure,
        smtpUsername: newSettings.smtpUsername || existingSettings.smtpUsername,
        // Only update password if explicitly provided (non-empty string)
        smtpPassword: newSettings.smtpPassword
          ? newSettings.smtpPassword
          : existingSettings.smtpPassword,
        // Daily updates settings
        dailyUpdatesEnabled:
          newSettings.dailyUpdatesEnabled !== undefined
            ? newSettings.dailyUpdatesEnabled
            : existingSettings.dailyUpdatesEnabled,
        dailyUpdatesTime:
          newSettings.dailyUpdatesTime ||
          existingSettings.dailyUpdatesTime ||
          "18:00",
      };

      await storage.setAppSetting("error_report_settings", mergedSettings);

      // Refresh the error reporter schedule
      const { refreshErrorReporter } = await import("./errorReporter");
      await refreshErrorReporter();

      // Return saved settings (but mask the password for security)
      const safeSettings = {
        ...mergedSettings,
        smtpPassword: mergedSettings.smtpPassword ? "********" : undefined,
      };
      res.json(safeSettings);
    } catch (error) {
      console.error("Error saving error report settings:", error);
      res.status(500).json({ error: "Failed to save settings" });
    }
  });

  app.get("/api/settings/error-reports/test", async (req, res) => {
    try {
      const { sendTestReport } = await import("./errorReporter");
      const result = await sendTestReport();
      res.json(result);
    } catch (error) {
      console.error("Error sending test report:", error);
      res.status(500).json({ error: "Failed to send test report" });
    }
  });

  app.post("/api/settings/error-reports/test-smtp", async (req, res) => {
    try {
      const { testSmtpConnection } = await import("./errorReporter");
      const result = await testSmtpConnection(req.body);
      res.json(result);
    } catch (error: any) {
      console.error("Error testing SMTP:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to test SMTP connection" });
    }
  });

  app.post("/api/settings/error-reports/send-now", async (req, res) => {
    try {
      const { sendErrorReportNow } = await import("./errorReporter");
      const result = await sendErrorReportNow();
      res.json(result);
    } catch (error: any) {
      console.error("Error sending error report:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to send error report" });
    }
  });

  app.post("/api/settings/daily-updates/send-now", async (req, res) => {
    try {
      const { sendDailyUpdatesNow } = await import("./errorReporter");
      const result = await sendDailyUpdatesNow();
      res.json(result);
    } catch (error: any) {
      console.error("Error sending daily updates:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to send daily updates" });
    }
  });

  app.get("/api/errors/summary", async (req, res) => {
    try {
      const { getErrorSummary } = await import("./errorReporter");
      const summary = await getErrorSummary();
      res.json(summary);
    } catch (error) {
      console.error("Error getting error summary:", error);
      res.status(500).json({ error: "Failed to get error summary" });
    }
  });

  // ===== IMPORT LOGS ROUTES =====

  // Get recent import logs (for Alerts & Notifications dashboard)
  app.get("/api/import-logs", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 100;
      const dataSourceId = req.query.dataSourceId as string | undefined;
      const status = req.query.status as string | undefined;

      if (dataSourceId) {
        const logs = await storage.getImportLogsByDataSource(
          dataSourceId,
          limit,
        );
        return res.json(logs);
      }

      const logs = await storage.getRecentImportLogs(limit);
      res.json(logs);
    } catch (error) {
      console.error("Error getting import logs:", error);
      res.status(500).json({ error: "Failed to get import logs" });
    }
  });

  // Get import logs for a specific data source
  app.get("/api/data-sources/:id/import-logs", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const logs = await storage.getImportLogsByDataSource(
        req.params.id,
        limit,
      );
      res.json(logs);
    } catch (error) {
      console.error("Error getting import logs for data source:", error);
      res.status(500).json({ error: "Failed to get import logs" });
    }
  });

  // ===== NOTIFICATION SETTINGS ROUTES =====

  // Get notification settings
  app.get("/api/settings/notifications", async (req, res) => {
    try {
      const settings = await storage.getNotificationSettings();
      res.json(
        settings || {
          enableImportSuccess: true,
          enableImportFailure: true,
          enableDailyDigest: false,
          dailyDigestTime: "09:00",
          notificationEmail: null,
        },
      );
    } catch (error) {
      console.error("Error getting notification settings:", error);
      res.status(500).json({ error: "Failed to get notification settings" });
    }
  });

  // Update notification settings
  app.post("/api/settings/notifications", async (req, res) => {
    try {
      const settings = await storage.upsertNotificationSettings(req.body);
      res.json(settings);
    } catch (error) {
      console.error("Error updating notification settings:", error);
      res.status(500).json({ error: "Failed to update notification settings" });
    }
  });

  // ===== SHOPIFY STORE ROUTES =====

  // Get all Shopify stores
  app.get("/api/shopify/stores", async (req, res) => {
    try {
      const stores = await storage.getShopifyStores();
      // Completely remove access tokens from response - never send to frontend
      const safeStores = stores.map(({ accessToken, ...store }) => ({
        ...store,
        hasToken: !!accessToken,
      }));
      res.json(safeStores);
    } catch (error) {
      console.error("Error getting Shopify stores:", error);
      res.status(500).json({ error: "Failed to get stores" });
    }
  });

  // Get product count from Shopify store
  app.get("/api/shopify/stores/:id/product-count", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      const { ShopifyService } = await import("./shopify");
      const service = createShopifyService(store);
      const count = await service.getProductCount();

      res.json({ count });
    } catch (error: any) {
      console.error("Error getting product count:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get product count" });
    }
  });

  // Get sales channels (publications) from Shopify store
  app.get("/api/shopify/stores/:id/publications", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store || !store.accessToken) {
        return res
          .status(404)
          .json({ error: "Store not found or not connected" });
      }

      const { ShopifyService } = await import("./shopify");
      const service = createShopifyService(store);
      const publications = await service.getPublications();

      res.json(publications);
    } catch (error: any) {
      console.error("Error getting publications:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get publications" });
    }
  });

  // Get markets from Shopify store
  app.get("/api/shopify/stores/:id/markets", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store || !store.accessToken) {
        return res
          .status(404)
          .json({ error: "Store not found or not connected" });
      }

      const { ShopifyService } = await import("./shopify");
      const service = createShopifyService(store);
      const markets = await service.getMarkets();

      res.json(markets);
    } catch (error: any) {
      console.error("Error getting markets:", error);
      res.status(500).json({ error: error.message || "Failed to get markets" });
    }
  });

  // Get products from Shopify store
  app.get("/api/shopify/stores/:id/products", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      const { ShopifyService } = await import("./shopify");
      const service = createShopifyService(store);
      const { products, variantCount } =
        await service.getProductsWithVariants();

      // Transform products for frontend display
      const transformedProducts = products.map((p) => ({
        id: p.id,
        title: p.title,
        handle: p.handle,
        status: p.status || "active",
        vendor: p.vendor || "",
        productType: p.product_type || "",
        image: p.image?.src || null,
        variantCount: p.variants.length,
        totalStock: p.variants.reduce(
          (sum, v) => sum + (v.inventory_quantity || 0),
          0,
        ),
        variants: p.variants.map((v) => ({
          id: v.id,
          sku: v.sku,
          title: v.title,
          option1: v.option1,
          option2: v.option2,
          option3: v.option3,
          inventoryQuantity: v.inventory_quantity,
        })),
      }));

      res.json({ products: transformedProducts, totalVariants: variantCount });
    } catch (error: any) {
      console.error("Error getting products:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get products" });
    }
  });

  // Get full products data for export (includes metafields, descriptions, etc.)
  app.get("/api/shopify/stores/:id/products/export", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      const { ShopifyService } = await import("./shopify");
      const service = createShopifyService(store);
      const products = await service.getProductsForExport();

      res.json({ products });
    } catch (error: any) {
      console.error("Error getting products for export:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get products for export" });
    }
  });

  // Update product metafields (product specifics)
  app.post(
    "/api/shopify/stores/:storeId/products/:productId/metafields",
    async (req, res) => {
      try {
        const { storeId, productId } = req.params;
        const { metafields } = req.body;

        if (!metafields || !Array.isArray(metafields)) {
          return res
            .status(400)
            .json({ error: "metafields array is required" });
        }

        const store = await storage.getShopifyStore(storeId);
        if (!store) {
          return res.status(404).json({ error: "Store not found" });
        }

        const { ShopifyService } = await import("./shopify");
        const service = createShopifyService(store);

        for (const mf of metafields) {
          if (mf.value) {
            await service.updateProductMetafield(
              productId,
              mf.namespace,
              mf.key,
              mf.value,
              mf.type || "single_line_text_field",
            );
          }
        }

        // Auto-refresh cache with specifics flag only (don't overwrite status)
        try {
          const hasNewSpecifics = metafields.some(
            (mf: any) => mf.value && mf.value.trim(),
          );
          if (hasNewSpecifics) {
            await storage.updateProductCacheEnrichment(productId, {
              hasSpecifics: true,
            });
          }
        } catch (cacheError) {
          console.log(
            "Cache update skipped (product may not be in cache):",
            cacheError,
          );
        }

        res.json({ success: true, message: "Metafields updated successfully" });
      } catch (error: any) {
        console.error("Error updating product metafields:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to update metafields" });
      }
    },
  );

  // Update product content (description, SEO title, SEO description)
  app.post(
    "/api/shopify/stores/:storeId/products/:productId/content",
    async (req, res) => {
      try {
        const { storeId, productId } = req.params;
        const { description, seoTitle, seoDescription } = req.body;

        const store = await storage.getShopifyStore(storeId);
        if (!store) {
          return res.status(404).json({ error: "Store not found" });
        }

        const { ShopifyService } = await import("./shopify");
        const service = createShopifyService(store);

        const updates: { descriptionHtml?: string } = {};
        const seoUpdates: { title?: string; description?: string } = {};

        if (description !== undefined) {
          updates.descriptionHtml = description;
        }
        if (seoTitle !== undefined) {
          seoUpdates.title = seoTitle;
        }
        if (seoDescription !== undefined) {
          seoUpdates.description = seoDescription;
        }

        await service.updateProduct(productId, updates, seoUpdates);

        // Auto-refresh the cache entry with updated flags (only set true, never clear)
        const cacheUpdates: any = {};
        if (description !== undefined && description.trim()) {
          cacheUpdates.hasDescription = true;
        }
        if (seoTitle !== undefined && seoTitle.trim()) {
          cacheUpdates.hasSeoTitle = true;
        }
        if (seoDescription !== undefined && seoDescription.trim()) {
          cacheUpdates.hasSeoDescription = true;
        }

        // Only upgrade status if we have all content pieces
        // (Full status recalculation happens during sync)
        if (
          cacheUpdates.hasDescription &&
          cacheUpdates.hasSeoTitle &&
          cacheUpdates.hasSeoDescription
        ) {
          cacheUpdates.enrichmentStatus = "ready";
        }

        if (Object.keys(cacheUpdates).length > 0) {
          try {
            await storage.updateProductCacheEnrichment(productId, cacheUpdates);
          } catch (cacheError) {
            // Non-critical - log but don't fail the request
            console.log(
              "Cache update skipped (product may not be in cache):",
              cacheError,
            );
          }
        }

        res.json({
          success: true,
          message: "Product content updated successfully",
        });
      } catch (error: any) {
        console.error("Error updating product content:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to update product content" });
      }
    },
  );

  // AI analyze product image for specifics
  app.post("/api/ai/analyze-product-specifics", async (req, res) => {
    try {
      const { imageUrl, productTitle, categories, productId, storeId } =
        req.body;

      if (!imageUrl || !categories) {
        return res
          .status(400)
          .json({ error: "imageUrl and categories are required" });
      }

      // Fetch price and available sizes from Shopify if productId and storeId are provided
      let productPrice: number | null = null;
      let availableSizes: string[] = [];

      if (productId && storeId) {
        try {
          const store = await storage.getShopifyStore(storeId);
          if (store?.accessToken) {
            const service = createShopifyService(store);
            const productGid = productId.startsWith("gid://")
              ? productId
              : `gid://shopify/Product/${productId}`;
            const product = await service.getProductById(productGid);

            if (product) {
              const variants = product.variants || [];

              // Use maximum variant price (for Couture determination)
              let maxPrice = 0;
              for (const variant of variants) {
                if (variant.price) {
                  const variantPrice = parseFloat(variant.price);
                  if (!isNaN(variantPrice) && variantPrice > maxPrice) {
                    maxPrice = variantPrice;
                  }
                }
              }
              if (maxPrice > 0) {
                productPrice = maxPrice;
              }

              // Extract available sizes from variant options (case-insensitive, check all options)
              const sizeOptions = new Set<string>();
              const sizeOptionNames = [
                "size",
                "size us",
                "dress size",
                "sizing",
                "sizes",
              ];
              for (const variant of variants) {
                const selectedOptions = variant.selectedOptions || [];
                for (const option of selectedOptions) {
                  const optionName = (option.name || "").toLowerCase().trim();
                  if (
                    sizeOptionNames.some((name) => optionName.includes(name))
                  ) {
                    if (option.value) {
                      sizeOptions.add(option.value);
                    }
                  }
                }
              }
              availableSizes = Array.from(sizeOptions);
            }
          }
        } catch (fetchError) {
          console.log(
            "Could not fetch product data for occasion analysis:",
            fetchError,
          );
          // Continue without price/size data
        }
      }

      const { analyzeProductSpecifics } = await import("./openai");
      const specifics = await analyzeProductSpecifics(
        imageUrl,
        productTitle,
        categories,
        {
          price: productPrice,
          sizes: availableSizes,
        },
      );

      res.json({ specifics });
    } catch (error: any) {
      console.error("Error analyzing product specifics:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to analyze product" });
    }
  });

  // AI generate product content (description, SEO title, SEO description)
  app.post("/api/ai/generate-content", async (req, res) => {
    try {
      const { productTitle, imageUrl, specifics } = req.body;

      if (!productTitle) {
        return res.status(400).json({ error: "productTitle is required" });
      }

      const { generateProductContent } = await import("./openai");
      const content = await generateProductContent(
        productTitle,
        imageUrl || null,
        specifics || [],
      );

      res.json(content);
    } catch (error: any) {
      console.error("Error generating product content:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to generate content" });
    }
  });

  // Get a single Shopify store
  app.get("/api/shopify/stores/:id", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }
      // Completely remove access token from response
      const { accessToken, ...safeStore } = store;
      res.json({
        ...safeStore,
        hasToken: !!accessToken,
      });
    } catch (error) {
      console.error("Error getting Shopify store:", error);
      res.status(500).json({ error: "Failed to get store" });
    }
  });

  // Connect a new Shopify store
  app.post("/api/shopify/stores", async (req, res) => {
    try {
      const { name, storeUrl, accessToken } = req.body;

      if (!name || !storeUrl || !accessToken) {
        return res
          .status(400)
          .json({ error: "Name, store URL, and access token are required" });
      }

      const result = await connectShopifyStore(name, storeUrl, accessToken);

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      // Remove access token from response
      const { accessToken: _, ...safeStore } = result.store!;
      res.status(201).json({
        ...safeStore,
        hasToken: true,
      });
    } catch (error: any) {
      console.error("Error connecting Shopify store:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to connect store" });
    }
  });

  // Test connection to a Shopify store
  app.post("/api/shopify/stores/:id/test", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      const service = createShopifyService(store);
      const result = await service.testConnection();

      if (result.success) {
        await storage.updateShopifyStore(store.id, {
          status: "connected",
          lastConnectionTest: new Date(),
          connectionError: null,
        });
      } else {
        await storage.updateShopifyStore(store.id, {
          status: "error",
          lastConnectionTest: new Date(),
          connectionError: result.error,
        });
      }

      res.json(result);
    } catch (error: any) {
      console.error("Error testing Shopify connection:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to test connection" });
    }
  });

  // Delete a Shopify store
  app.delete("/api/shopify/stores/:id", async (req, res) => {
    try {
      await storage.deleteShopifyStore(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting Shopify store:", error);
      res.status(500).json({ error: "Failed to delete store" });
    }
  });

  // Update Shopify store settings
  app.patch("/api/shopify/stores/:id", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      const updates: any = {};
      if (req.body.name) updates.name = req.body.name;
      if (req.body.autoSync !== undefined) updates.autoSync = req.body.autoSync;
      if (req.body.syncFrequency)
        updates.syncFrequency = req.body.syncFrequency;
      if (req.body.syncScheduleTime)
        updates.syncScheduleTime = req.body.syncScheduleTime;
      if (req.body.syncScheduleDays)
        updates.syncScheduleDays = req.body.syncScheduleDays;
      if (req.body.syncOnImport !== undefined)
        updates.syncOnImport = req.body.syncOnImport;
      if (req.body.pauseAllSyncs !== undefined)
        updates.pauseAllSyncs = req.body.pauseAllSyncs;
      if (req.body.pauseAllImports !== undefined)
        updates.pauseAllImports = req.body.pauseAllImports;
      if (req.body.cacheRefreshEnabled !== undefined)
        updates.cacheRefreshEnabled = req.body.cacheRefreshEnabled;
      if (req.body.cacheRefreshTime)
        updates.cacheRefreshTime = req.body.cacheRefreshTime;
      if (req.body.productCacheRefreshEnabled !== undefined)
        updates.productCacheRefreshEnabled =
          req.body.productCacheRefreshEnabled;
      if (req.body.productCacheRefreshTime)
        updates.productCacheRefreshTime = req.body.productCacheRefreshTime;
      if (req.body.timezone) updates.timezone = req.body.timezone;

      // If access token is provided, update it (frontend will send new token value)
      if (req.body.accessToken) {
        updates.accessToken = req.body.accessToken;
      }

      const updated = await storage.updateShopifyStore(store.id, updates);

      // Refresh scheduler if sync settings changed
      if (
        req.body.autoSync !== undefined ||
        req.body.syncFrequency ||
        req.body.syncScheduleTime ||
        req.body.syncScheduleDays
      ) {
        const { refreshShopifySchedules } = await import("./scheduler");
        refreshShopifySchedules().catch((err) => {
          console.error(
            "[Routes] Error refreshing Shopify schedules:",
            err.message,
          );
        });
      }

      // Remove access token from response
      const { accessToken: _, ...safeStore } = updated!;
      res.json({
        ...safeStore,
        hasToken: !!updated?.accessToken,
      });
    } catch (error) {
      console.error("Error updating Shopify store:", error);
      res.status(500).json({ error: "Failed to update store" });
    }
  });

  // Fetch products from Shopify store
  app.get("/api/shopify/stores/:id/products", async (req, res) => {
    try {
      const result = await fetchShopifyProducts(req.params.id);
      res.json(result);
    } catch (error: any) {
      console.error("Error fetching Shopify products:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch products" });
    }
  });

  // Sync inventory to Shopify (optionally filtered by data source)
  // Use sequential=true (default) for per-vendor processing (more reliable)
  // Use sequential=false for legacy all-at-once mode
  app.post("/api/shopify/stores/:id/sync", async (req, res) => {
    try {
      const { dryRun, dataSourceIds, sequential = true } = req.body || {};
      const storeId = req.params.id;

      // Check if sync is already running for this store
      if (isInventorySyncActive(storeId)) {
        return res.status(409).json({
          error: "Sync already running",
          message:
            "A sync is already in progress for this store. Please wait for it to complete or cancel it.",
          alreadyRunning: true,
        });
      }

      // Return immediately - sync functions create their own logs
      res.json({
        status: "running",
        message: "Sync started in background",
      });

      // Run sync in background (fire and forget)
      // Using setImmediate to ensure response is sent first
      setImmediate(async () => {
        try {
          // Default to sequential mode for reliability (per-vendor processing)
          const syncResult = sequential
            ? await syncInventoryToShopifySequential(storeId, dryRun, {
                dataSourceIds: dataSourceIds || undefined,
              })
            : await syncInventoryToShopify(storeId, dryRun, {
                dataSourceIds: dataSourceIds || undefined,
              });

          console.log(
            `[Sync] Background sync completed for store ${storeId}:`,
            {
              updated: syncResult.itemsUpdated,
              created: syncResult.itemsCreated,
              skipped: syncResult.itemsSkipped,
              failed: syncResult.itemsFailed,
            },
          );
        } catch (error: any) {
          console.error(
            `[Sync] Background sync failed for store ${storeId}:`,
            error,
          );
        }
      });
    } catch (error: any) {
      console.error("Error starting sync:", error);
      res.status(500).json({ error: error.message || "Failed to start sync" });
    }
  });

  // Sync a specific data source to Shopify (convenience endpoint)
  app.post("/api/data-sources/:id/sync", async (req, res) => {
    try {
      const dataSourceId = req.params.id;
      const dataSource = await storage.getDataSource(dataSourceId);

      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      if (!dataSource.shopifyStoreId) {
        return res
          .status(400)
          .json({ error: "Data source is not connected to a Shopify store" });
      }

      // Build the list of data source IDs to sync
      // If this is a regular data source with a sale file, include both
      const dataSourceIdsToSync = [dataSourceId];
      if (dataSource.assignedSaleDataSourceId) {
        dataSourceIdsToSync.push(dataSource.assignedSaleDataSourceId);
      }

      console.log(
        `[Sync] Starting sync for data source "${dataSource.name}" (IDs: ${dataSourceIdsToSync.join(", ")})`,
      );

      const syncLog = await syncInventoryToShopify(
        dataSource.shopifyStoreId,
        false,
        {
          dataSourceIds: dataSourceIdsToSync,
        },
      );

      res.json(syncLog);
    } catch (error: any) {
      console.error("Error syncing data source:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to sync data source" });
    }
  });

  // Cancel running inventory sync
  app.post("/api/shopify/stores/:id/sync/cancel", async (req, res) => {
    try {
      const { cancelInventorySync, isInventorySyncActive } = await import(
        "./shopify"
      );

      if (!isInventorySyncActive(req.params.id)) {
        return res
          .status(404)
          .json({ error: "No sync running for this store" });
      }

      const cancelled = cancelInventorySync(req.params.id);
      if (cancelled) {
        res.json({ success: true, message: "Sync cancellation requested" });
      } else {
        res.status(400).json({ error: "Failed to cancel sync" });
      }
    } catch (error: any) {
      console.error("Error cancelling sync:", error);
      res.status(500).json({ error: error.message || "Failed to cancel sync" });
    }
  });

  // Force stop sync and immediately clear the lock (use when sync is stuck)
  app.post("/api/shopify/stores/:id/sync/force-stop", async (req, res) => {
    try {
      const { forceStopInventorySync } = await import("./shopify");

      const stopped = forceStopInventorySync(req.params.id);
      if (stopped) {
        res.json({
          success: true,
          message: "Sync force stopped and lock cleared",
        });
      } else {
        res.json({
          success: true,
          message: "No sync was running for this store",
        });
      }
    } catch (error: any) {
      console.error("Error force stopping sync:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to force stop sync" });
    }
  });

  // Sync a single product by style number
  app.post("/api/shopify/stores/:id/sync-single", async (req, res) => {
    try {
      const { styleNumber } = req.body;

      if (!styleNumber) {
        return res.status(400).json({ error: "styleNumber is required" });
      }

      const { syncSingleProduct } = await import("./shopify");
      const result = await syncSingleProduct(req.params.id, styleNumber);

      res.json(result);
    } catch (error: any) {
      console.error("Error syncing single product:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to sync single product" });
    }
  });

  // Delete non-matching variants for a single product by style number
  app.post(
    "/api/shopify/stores/:id/delete-single-non-matching",
    async (req, res) => {
      try {
        const { styleNumber } = req.body;

        if (!styleNumber) {
          return res.status(400).json({ error: "styleNumber is required" });
        }

        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const shopify = createShopifyService(store);
        const { getHandler } = await import("./handlers");

        console.log(
          `[DeleteSingleNonMatching] Starting for input: ${styleNumber}`,
        );

        // Load all data sources first
        const allDataSources = await storage.getDataSources();
        const saleFileIds = new Set(
          allDataSources
            .filter((ds) => ds.sourceType === "sales")
            .map((ds) => ds.id),
        );

        // Parse input: "Jovani 22845" -> dataSourceName: "Jovani", styleNum: "22845"
        // Also handles: "Mac Duggal 1225M" -> dataSourceName: "Mac Duggal", styleNum: "1225M"
        // Or just "22845" -> dataSourceName: null, styleNum: "22845"
        const inputTrimmed = styleNumber.trim();
        let dataSourceName: string | null = null;
        let styleNum: string = inputTrimmed; // Default to full input

        // Try to match known data source names at the start of input
        // This handles multi-word data source names like "Mac Duggal" or "Ava Presley"
        // Reuse allDataSources already fetched above
        const sortedByLength = [...allDataSources].sort(
          (a, b) => b.name.length - a.name.length,
        );

        let foundDataSource = false;
        for (const ds of sortedByLength) {
          const dsNameLower = ds.name.toLowerCase();
          const inputLower = inputTrimmed.toLowerCase();

          // Check if input starts with data source name followed by space
          if (inputLower.startsWith(dsNameLower + " ")) {
            dataSourceName = ds.name;
            styleNum = inputTrimmed.substring(ds.name.length).trim();
            console.log(
              `[DeleteSingleNonMatching] Parsed input - DataSource: "${dataSourceName}", Style: "${styleNum}"`,
            );
            foundDataSource = true;
            break;
          }
        }

        if (!foundDataSource) {
          // No data source prefix found - treat entire input as style
          styleNum = inputTrimmed;
          console.log(
            `[DeleteSingleNonMatching] Input is style only: "${styleNum}"`,
          );
        }

        // Find the target data source by name (if provided)
        let targetDataSource: (typeof allDataSources)[0] | null = null;
        if (dataSourceName) {
          // Find data source by name (case-insensitive)
          const dsNameUpper = dataSourceName.toUpperCase();
          targetDataSource =
            allDataSources.find(
              (ds) =>
                ds.name.toUpperCase() === dsNameUpper ||
                ds.name.toUpperCase().includes(dsNameUpper) ||
                dsNameUpper.includes(ds.name.toUpperCase()),
            ) || null;

          if (!targetDataSource) {
            return res.status(404).json({
              error: `Data source "${dataSourceName}" not found. Available: ${allDataSources.map((ds) => ds.name).join(", ")}`,
            });
          }

          console.log(
            `[DeleteSingleNonMatching] Matched data source: ${targetDataSource.name} (ID: ${targetDataSource.id})`,
          );

          // If target is a sale file, redirect to its parent
          if (targetDataSource.sourceType === "sales") {
            const parentDs = allDataSources.find(
              (ds) => ds.assignedSaleDataSourceId === targetDataSource!.id,
            );
            if (parentDs) {
              console.log(
                `[DeleteSingleNonMatching] Redirecting from sale file to parent: ${parentDs.name}`,
              );
              targetDataSource = parentDs;
            }
          }
        }

        // Load master inventory
        const masterInventory = await storage.getMasterInventory();

        // Normalize style number for matching
        const normalizedStyleNum = styleNum.toUpperCase();

        // Helper function to match style number
        const matchesStyleNum = (itemStyle: string): boolean => {
          const styleUpper = itemStyle.trim().toUpperCase();

          // Exact match on full style
          if (styleUpper === normalizedStyleNum) return true;

          // Extract trailing number and compare
          const trailingMatch = styleUpper.match(/(\d+)$/);
          if (trailingMatch && trailingMatch[1] === normalizedStyleNum)
            return true;

          // Handle alphanumeric styles like "JVN22871"
          const alphanumMatch = styleUpper.match(/^[A-Z]+(\d+)$/);
          if (alphanumMatch && alphanumMatch[1] === normalizedStyleNum)
            return true;

          return false;
        };

        // Find inventory items - filter by data source first (if specified), then by style
        let styleItems = masterInventory.filter((item) => {
          if (!item.style) return false;

          // If data source was specified, filter to that data source (and its sale file)
          if (targetDataSource) {
            const validDsIds = new Set([targetDataSource.id]);
            if (targetDataSource.assignedSaleDataSourceId) {
              validDsIds.add(targetDataSource.assignedSaleDataSourceId);
            }
            if (!item.dataSourceId || !validDsIds.has(item.dataSourceId)) {
              return false;
            }
          }

          return matchesStyleNum(item.style);
        });

        if (styleItems.length === 0) {
          const errMsg = targetDataSource
            ? `No inventory items found for style ${styleNum} in ${targetDataSource.name}`
            : `No inventory items found for style ${styleNum}`;
          return res.status(404).json({ error: errMsg });
        }

        console.log(
          `[DeleteSingleNonMatching] Found ${styleItems.length} items matching style ${styleNum}`,
        );

        // Determine effective data source
        let effectiveDataSource = targetDataSource;
        if (!effectiveDataSource) {
          // No data source specified - find one from the matched items (prefer non-sale file)
          const regularItem = styleItems.find(
            (item) => item.dataSourceId && !saleFileIds.has(item.dataSourceId),
          );
          const dataSourceId =
            regularItem?.dataSourceId || styleItems[0].dataSourceId;
          effectiveDataSource = dataSourceId
            ? allDataSources.find((ds) => ds.id === dataSourceId) || null
            : null;

          // Redirect if sale file
          if (effectiveDataSource?.sourceType === "sales") {
            const parentDs = allDataSources.find(
              (ds) => ds.assignedSaleDataSourceId === effectiveDataSource!.id,
            );
            if (parentDs) {
              console.log(
                `[DeleteSingleNonMatching] Redirecting from sale file to parent: ${parentDs.name}`,
              );
              effectiveDataSource = parentDs;
            }
          }
        }

        // Apply sale file merge if applicable (use effectiveDataSource)
        let mergedItems = styleItems;
        if (effectiveDataSource?.assignedSaleDataSourceId) {
          console.log(
            `[DeleteSingleNonMatching] Applying sale file merge for ${effectiveDataSource.name}`,
          );

          const regularItems = styleItems.filter(
            (item) => item.dataSourceId === effectiveDataSource.id,
          );
          const saleItems = masterInventory.filter((item) => {
            if (
              item.dataSourceId !== effectiveDataSource.assignedSaleDataSourceId
            )
              return false;
            if (!item.style) return false;
            // Use same matching logic as above
            return matchesStyleNum(item.style);
          });

          console.log(
            `[DeleteSingleNonMatching] Regular items: ${regularItems.length}, Sale items: ${saleItems.length}`,
          );

          const handler = getHandler(
            effectiveDataSource.id,
            effectiveDataSource.name,
          );

          if (handler.prepareSaleFileMerge && saleItems.length > 0) {
            const handlerRegular = regularItems.map((item) => ({
              id: item.id,
              dataSourceId: item.dataSourceId || effectiveDataSource.id,
              sku: item.sku,
              style: item.style,
              color: item.color,
              size: item.size,
              stock: item.stock ?? 0,
              price: item.price,
              cost: item.cost,
              shipDate: item.shipDate,
              isExpandedSize: item.isExpandedSize ?? undefined,
              hasFutureStock: item.hasFutureStock || false,
              preserveZeroStock: item.preserveZeroStock || false,
            }));
            const handlerSales = saleItems.map((item) => ({
              id: item.id,
              dataSourceId:
                item.dataSourceId ||
                effectiveDataSource.assignedSaleDataSourceId ||
                "",
              sku: item.sku,
              style: item.style,
              color: item.color,
              size: item.size,
              stock: item.stock ?? 0,
              price: item.price,
              cost: item.cost,
              shipDate: item.shipDate,
              isExpandedSize: item.isExpandedSize ?? undefined,
              hasFutureStock: item.hasFutureStock || false,
              preserveZeroStock: item.preserveZeroStock || false,
            }));

            const merged = handler.prepareSaleFileMerge(
              handlerRegular,
              handlerSales,
            );
            console.log(
              `[DeleteSingleNonMatching] Merged result: ${merged.length} items`,
            );

            mergedItems = merged.map((item) => ({
              id: item.id || "",
              sku: item.sku || null,
              style: item.style || null,
              color: item.color || null,
              size: item.size != null ? String(item.size) : null,
              stock: item.stock,
              price: item.price,
              cost: item.cost,
              shipDate: item.shipDate || null,
              dataSourceId: effectiveDataSource.id,
              isExpandedSize: item.isExpandedSize ?? null,
              rawData: null,
              importedAt: new Date(),
              sourceName: effectiveDataSource.name,
            })) as typeof styleItems;
          }
        }

        // Safety check: if merge resulted in empty inventory, don't delete anything
        if (mergedItems.length === 0) {
          console.log(
            `[DeleteSingleNonMatching] Merged inventory is empty - aborting`,
          );
          return res.status(400).json({
            error:
              "Merged inventory is empty for this style - cannot determine which variants to keep.",
            candidateCount: 0,
          });
        }

        // Build set of valid SKUs from merged inventory using NORMALIZED format
        const validSkus = new Set<string>();
        for (const item of mergedItems) {
          if (!item.style || item.size == null || item.size === "") continue;
          const style = (item.style || "")
            .replace(/\s+/g, "-")
            .replace(/\//g, "-");
          const color = (item.color || "")
            .replace(/\s+/g, "-")
            .replace(/\//g, "-");
          const size = String(item.size ?? "")
            .replace(/\s+/g, "-")
            .replace(/\//g, "-");
          const constructedSku = `${style}-${color}-${size}`.toLowerCase();
          validSkus.add(constructedSku);
          if (item.sku) {
            validSkus.add(item.sku.toLowerCase());
          }
        }

        console.log(`[DeleteSingleNonMatching] Valid SKUs: ${validSkus.size}`);

        // Get cached variants for this style's product
        const cachedVariants = await storage.getVariantCacheForValidation(
          req.params.id,
        );

        // Filter to just this product's variants by matching style in SKU
        const variantsToDelete: Map<string, string[]> = new Map();
        let candidateCount = 0;

        for (const variant of cachedVariants) {
          if (!variant.sku) continue;

          const skuLower = variant.sku.toLowerCase();
          // Check if this SKU matches the style number
          const styleMatch = skuLower.match(
            new RegExp(
              `-${styleNumber}-|-${styleNumber}$|^${styleNumber}-`,
              "i",
            ),
          );
          if (!styleMatch) continue;

          if (!validSkus.has(skuLower)) {
            candidateCount++;
            const productId = variant.shopifyProductId;
            if (productId) {
              if (!variantsToDelete.has(productId)) {
                variantsToDelete.set(productId, []);
              }
              variantsToDelete.get(productId)!.push(variant.id);
            }
          }
        }

        console.log(
          `[DeleteSingleNonMatching] Candidates for deletion: ${candidateCount} variants`,
        );

        if (candidateCount === 0) {
          return res.json({
            deletedCount: 0,
            failedCount: 0,
            message: "No non-matching variants found for this style",
          });
        }

        // Safety limit for single product (reasonable max)
        const maxLimit = 50;
        if (candidateCount > maxLimit) {
          return res.status(400).json({
            error: `Would delete ${candidateCount} variants, but single-product limit is ${maxLimit}.`,
            candidateCount,
            maxLimit,
          });
        }

        // Delete the variants
        let deletedCount = 0;
        let failedCount = 0;
        const errors: string[] = [];

        for (const [productId, variantIds] of Array.from(
          variantsToDelete.entries(),
        )) {
          try {
            const result = await shopify.bulkDeleteVariantsByProduct(
              productId,
              variantIds,
            );
            deletedCount += result.success;
            failedCount += result.failed;
            if (result.errors.length > 0) {
              errors.push(...result.errors);
            }

            // Only remove from cache when ALL variants in batch succeeded
            if (result.success > 0 && result.success === variantIds.length) {
              await storage.deleteVariantCacheByIds(req.params.id, variantIds);
            }
          } catch (err: any) {
            failedCount += variantIds.length;
            errors.push(`Product ${productId}: ${err.message}`);
          }
        }

        console.log(
          `[DeleteSingleNonMatching] Complete: deleted=${deletedCount}, failed=${failedCount}`,
        );

        res.json({
          deletedCount,
          failedCount,
          errors: errors.slice(0, 20),
          styleNumber,
        });
      } catch (error: any) {
        console.error("Error deleting single product non-matching:", error);
        res.status(500).json({
          error: error.message || "Failed to delete non-matching variants",
        });
      }
    },
  );

  // Test variant creation for a single product - use before running full sync
  app.post(
    "/api/shopify/stores/:id/test-variant-creation",
    async (req, res) => {
      try {
        const { productId, variants } = req.body;

        if (!productId) {
          return res.status(400).json({ error: "productId is required" });
        }
        if (!variants || !Array.isArray(variants) || variants.length === 0) {
          return res.status(400).json({
            error: "variants array is required (with sku, color, size, stock)",
          });
        }

        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const service = new ShopifyService(store.storeUrl, store.accessToken);

        // Build variant inputs like the sync does
        const variantInputs = variants.map(
          (v: {
            sku: string;
            color: string;
            size: string;
            stock?: number;
            price?: string;
            compareAtPrice?: string | null;
          }) => ({
            sku: v.sku,
            price: v.price || "0",
            compareAtPrice: v.compareAtPrice || null,
            cost: null,
            optionValues: [
              { optionName: "COLOR", name: v.color },
              { optionName: "SIZE", name: v.size },
            ],
            inventoryQuantity: v.stock || 0,
          }),
        );

        console.log(
          `[Test Variant Creation] Testing for product ${productId} with ${variantInputs.length} variants`,
        );
        console.log(
          `[Test Variant Creation] First variant input:`,
          JSON.stringify(variantInputs[0], null, 2),
        );

        const result = await service.bulkCreateVariants(
          productId,
          variantInputs,
          store.primaryLocationId || "",
        );

        res.json({
          success: result.success,
          failed: result.failed,
          skipped: result.skipped || 0,
          createdVariants: result.createdVariants,
          errors: result.errors,
        });
      } catch (error: any) {
        console.error("Error testing variant creation:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to test variant creation" });
      }
    },
  );

  // Sync sales file to Shopify (price updates + variant deletion)
  app.post(
    "/api/shopify/stores/:id/sync-sales/:dataSourceId",
    async (req, res) => {
      try {
        const { dryRun } = req.body || {};
        const syncLog = await syncSalesFileToShopify(
          req.params.id,
          req.params.dataSourceId,
          dryRun,
        );
        res.json(syncLog);
      } catch (error: any) {
        console.error("Error syncing sales file to Shopify:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to sync sales file" });
      }
    },
  );

  // Generate sync preview - shows what will change without executing
  app.get("/api/shopify/stores/:id/sync-preview", async (req, res) => {
    try {
      const dataSourceId = req.query.dataSourceId as string | undefined;
      const useCache = req.query.useCache !== "false"; // Default to true, only false if explicitly set
      const preview = await generateSyncPreview(
        req.params.id,
        dataSourceId,
        useCache,
      );
      res.json(preview);
    } catch (error: any) {
      console.error("Error generating sync preview:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to generate sync preview" });
    }
  });

  // Cancel running sync preview
  app.post("/api/shopify/stores/:id/sync-preview/cancel", async (req, res) => {
    try {
      const cancelled = cancelSyncPreview(req.params.id);
      res.json({
        cancelled,
        message: cancelled ? "Preview cancelled" : "No preview running",
      });
    } catch (error: any) {
      console.error("Error cancelling sync preview:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to cancel preview" });
    }
  });

  // Check if sync preview is running and get progress
  app.get("/api/shopify/stores/:id/sync-preview/status", async (req, res) => {
    try {
      const running = isSyncPreviewRunning(req.params.id);
      const progress = getSyncPreviewProgress(req.params.id);

      res.json({
        running,
        progress: progress
          ? {
              phase: progress.phase,
              percent: progress.progress,
              message: progress.message,
              processedItems: progress.processedItems,
              totalItems: progress.totalItems,
              elapsedMs: Date.now() - progress.startedAt.getTime(),
            }
          : null,
      });
    } catch (error: any) {
      console.error("Error checking sync preview status:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to check preview status" });
    }
  });

  // =========== SYNC VALIDATION ROUTES ===========

  app.get("/api/shopify/stores/:id/validate-sync", async (req, res) => {
    try {
      const { runSyncValidation } = await import("./syncValidator");
      const result = await runSyncValidation(req.params.id);
      res.json(result);
    } catch (error: any) {
      console.error("Error running sync validation:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to run validation" });
    }
  });

  // Execute validation fixes (create missing variants, update stock, etc.)
  app.post(
    "/api/shopify/stores/:id/validate-sync/execute",
    async (req, res) => {
      try {
        const { executeValidationFixes } = await import("./syncValidator");
        const { discrepancies, options } = req.body as {
          discrepancies: any[];
          options?: {
            createMissing?: boolean;
            updateStock?: boolean;
            updatePrice?: boolean;
            deleteExtra?: boolean;
          };
        };

        if (!discrepancies || !Array.isArray(discrepancies)) {
          return res
            .status(400)
            .json({ error: "discrepancies array required" });
        }

        const result = await executeValidationFixes(
          req.params.id,
          discrepancies,
          options || {},
        );
        res.json(result);
      } catch (error: any) {
        console.error("Error executing validation fixes:", error);
        res.status(500).json({
          error: error.message || "Failed to execute validation fixes",
        });
      }
    },
  );

  app.post("/api/shopify/stores/:id/fix-mismatch", async (req, res) => {
    try {
      const { variantIds, fixType } = req.body;
      if (
        !variantIds ||
        !Array.isArray(variantIds) ||
        variantIds.length === 0
      ) {
        return res.status(400).json({ error: "variantIds array required" });
      }

      const store = await storage.getShopifyStore(req.params.id);
      if (!store || !store.accessToken) {
        return res.status(400).json({ error: "Store not connected" });
      }

      const shopify = createShopifyService(store);
      const masterInventory = await storage.getMasterInventory();

      const allDataSources = await storage.getDataSources();
      const dsConfigMap = new Map<string, (typeof allDataSources)[0]>();
      const saleFileParentMap = new Map<string, (typeof allDataSources)[0]>();

      for (const ds of allDataSources) {
        dsConfigMap.set(ds.id, ds);
        if (ds.assignedSaleDataSourceId) {
          saleFileParentMap.set(ds.assignedSaleDataSourceId, ds);
        }
      }

      const inventoryByConstructedSku = new Map<
        string,
        (typeof masterInventory)[0]
      >();
      const inventoryByRawSku = new Map<string, (typeof masterInventory)[0]>();

      for (const item of masterInventory) {
        if (!item.style || item.size == null || item.size === "") continue;
        const style = item.style || "";
        const color = item.color || "";
        const size = String(item.size ?? "");

        const styleWithDashes = style.replace(/\s+/g, "-");
        const constructedSku =
          `${styleWithDashes}-${color}-${size}`.toLowerCase();
        inventoryByConstructedSku.set(constructedSku, item);

        if (item.sku) {
          inventoryByRawSku.set(item.sku.toLowerCase(), item);
        }
      }

      const cachedVariants = await storage.getVariantCacheByIds(
        req.params.id,
        variantIds,
      );

      let fixedCount = 0;
      let failedCount = 0;
      let skippedCount = 0;
      const errors: string[] = [];

      for (const variant of cachedVariants) {
        if (!variant.inventoryItemId) {
          console.log(
            `[FixMismatch] Skipping ${variant.sku}: no inventoryItemId`,
          );
          skippedCount++;
          continue;
        }

        let inventoryItem: (typeof masterInventory)[0] | undefined;

        if (variant.sku) {
          inventoryItem = inventoryByConstructedSku.get(
            variant.sku.toLowerCase(),
          );

          if (!inventoryItem) {
            inventoryItem = inventoryByRawSku.get(variant.sku.toLowerCase());
          }
        }

        if (
          !inventoryItem &&
          variant.option1Value &&
          variant.option2Value &&
          variant.sku
        ) {
          const styleParts = variant.sku.split("-");
          if (styleParts.length >= 3) {
            const styleFromSku = styleParts.slice(0, -2).join("-");
            const shopifyColor = variant.option1Value;
            const shopifySize = variant.option2Value;

            const reconstructedSku =
              `${styleFromSku}-${shopifyColor}-${shopifySize}`.toLowerCase();
            inventoryItem = inventoryByConstructedSku.get(reconstructedSku);

            if (!inventoryItem) {
              console.log(
                `[FixMismatch] Trying fallback for ${variant.sku}: reconstructed=${reconstructedSku}`,
              );
            }
          }
        }

        if (!inventoryItem) {
          console.log(
            `[FixMismatch] Could not find inventory item for variant ${variant.sku}`,
          );
          skippedCount++;
          continue;
        }

        try {
          if (fixType === "stock" || fixType === "all") {
            const locationId = store.primaryLocationId;
            if (locationId) {
              console.log(
                `[FixMismatch] Updating stock for ${variant.sku}: ${variant.stock} -> ${inventoryItem.stock || 0}`,
              );
              await shopify.setInventoryLevel(
                variant.inventoryItemId,
                locationId,
                inventoryItem.stock || 0,
              );
              await storage.updateVariantCacheStock(
                variant.id,
                inventoryItem.stock || 0,
              );
            }
          }

          if (fixType === "price" || fixType === "all") {
            const dsId = inventoryItem.dataSourceId || "";
            const dsConfig = dsConfigMap.get(dsId);
            const parentConfig = saleFileParentMap.get(dsId);
            const salesConfig = (parentConfig?.salesConfig ||
              dsConfig?.salesConfig) as { priceMultiplier?: number } | null;
            const priceMultiplier = salesConfig?.priceMultiplier || 1;

            const basePrice = inventoryItem.price
              ? parseFloat(inventoryItem.price.toString())
              : 0;
            const finalPrice =
              Math.round(basePrice * priceMultiplier * 100) / 100;
            const priceStr = finalPrice.toFixed(2);

            console.log(
              `[FixMismatch] Updating price for ${variant.sku}: ${variant.price} -> ${priceStr} (base=${basePrice}, multiplier=${priceMultiplier})`,
            );

            if (variant.shopifyProductId) {
              await shopify.updateVariant(
                variant.shopifyProductId,
                variant.id,
                { price: priceStr },
              );
              await storage.updateVariantCachePrice(variant.id, priceStr);
            }
          }

          fixedCount++;
        } catch (err: any) {
          failedCount++;
          errors.push(`${variant.sku}: ${err.message}`);
        }
      }

      res.json({ fixedCount, failedCount, skippedCount, errors });
    } catch (error: any) {
      console.error("Error fixing mismatches:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fix mismatches" });
    }
  });

  // Delete variants not in merged inventory for a specific data source
  app.post("/api/shopify/stores/:id/delete-non-matching", async (req, res) => {
    try {
      const { dataSourceId } = req.body;
      if (!dataSourceId) {
        return res.status(400).json({ error: "dataSourceId required" });
      }

      const store = await storage.getShopifyStore(req.params.id);
      if (!store || !store.accessToken) {
        return res.status(400).json({ error: "Store not connected" });
      }

      const shopify = createShopifyService(store);
      const { getHandler } = await import("./handlers");

      const allDataSources = await storage.getDataSources();
      const dataSource = allDataSources.find((ds) => ds.id === dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      console.log(
        `[DeleteNonMatching] Starting for data source: ${dataSource.name}`,
      );

      // Load master inventory
      let masterInventory = await storage.getMasterInventory();

      // Apply sale file merge if data source has assigned sale file
      if (dataSource.assignedSaleDataSourceId) {
        console.log(
          `[DeleteNonMatching] Applying sale file merge for ${dataSource.name}`,
        );

        const regularItems = masterInventory.filter(
          (item) => item.dataSourceId === dataSource.id,
        );
        const saleItems = masterInventory.filter(
          (item) => item.dataSourceId === dataSource.assignedSaleDataSourceId,
        );

        console.log(
          `[DeleteNonMatching] Regular items: ${regularItems.length}, Sale items: ${saleItems.length}`,
        );

        const handler = getHandler(dataSource.id, dataSource.name);

        if (handler.prepareSaleFileMerge && saleItems.length > 0) {
          const handlerRegular = regularItems.map((item) => ({
            id: item.id,
            dataSourceId: item.dataSourceId || dataSource.id,
            sku: item.sku,
            style: item.style,
            color: item.color,
            size: item.size,
            stock: item.stock ?? 0,
            price: item.price,
            cost: item.cost,
            shipDate: item.shipDate,
            isExpandedSize: item.isExpandedSize ?? undefined,
            hasFutureStock: item.hasFutureStock || false,
            preserveZeroStock: item.preserveZeroStock || false,
          }));
          const handlerSales = saleItems.map((item) => ({
            id: item.id,
            dataSourceId:
              item.dataSourceId || dataSource.assignedSaleDataSourceId || "",
            sku: item.sku,
            style: item.style,
            color: item.color,
            size: item.size,
            stock: item.stock ?? 0,
            price: item.price,
            cost: item.cost,
            shipDate: item.shipDate,
            isExpandedSize: item.isExpandedSize ?? undefined,
            hasFutureStock: item.hasFutureStock || false,
            preserveZeroStock: item.preserveZeroStock || false,
          }));

          const mergedItems = handler.prepareSaleFileMerge(
            handlerRegular,
            handlerSales,
          );
          console.log(
            `[DeleteNonMatching] Merged result: ${mergedItems.length} items`,
          );

          // Use merged items as master inventory for this data source
          masterInventory = mergedItems.map((item) => ({
            id: item.id || "",
            sku: item.sku || null,
            style: item.style || null,
            color: item.color || null,
            size: item.size != null ? String(item.size) : null,
            stock: item.stock,
            price: item.price,
            cost: item.cost,
            shipDate: item.shipDate || null,
            dataSourceId: dataSource.id,
            isExpandedSize: item.isExpandedSize ?? null,
            rawData: null,
            importedAt: new Date(),
            sourceName: dataSource.name,
          })) as unknown as typeof masterInventory;
        }
      } else {
        // No sale file - just filter to this data source
        masterInventory = masterInventory.filter(
          (item) => item.dataSourceId === dataSource.id,
        );
      }

      // Safety check: if merge resulted in empty inventory, don't delete anything
      if (masterInventory.length === 0) {
        console.log(
          `[DeleteNonMatching] Merged inventory is empty - aborting to prevent accidental deletion`,
        );
        return res.status(400).json({
          error:
            "Merged inventory is empty - cannot determine which variants to keep. Check sale file assignment.",
          candidateCount: 0,
        });
      }

      // Build set of valid SKUs from merged inventory using NORMALIZED format
      // SKU format: style-color-size where ALL spaces become hyphens
      const validSkus = new Set<string>();
      for (const item of masterInventory) {
        if (!item.style || item.size == null || item.size === "") continue;
        const style = (item.style || "")
          .replace(/\s+/g, "-")
          .replace(/\//g, "-");
        const color = (item.color || "")
          .replace(/\s+/g, "-")
          .replace(/\//g, "-");
        const size = String(item.size ?? "")
          .replace(/\s+/g, "-")
          .replace(/\//g, "-");
        const constructedSku = `${style}-${color}-${size}`.toLowerCase();
        validSkus.add(constructedSku);
        // Also add the actual stored SKU if present
        if (item.sku) {
          validSkus.add(item.sku.toLowerCase());
        }
      }

      console.log(
        `[DeleteNonMatching] Valid SKUs in merged inventory: ${validSkus.size}`,
      );

      // Get all cached variants
      const cachedVariants = await storage.getVariantCacheForValidation(
        req.params.id,
      );

      // Find variants to delete (not in merged inventory)
      const variantsToDelete: Map<string, string[]> = new Map(); // productId -> variantIds
      let candidateCount = 0;

      for (const variant of cachedVariants) {
        if (!variant.sku) continue;

        const skuLower = variant.sku.toLowerCase();
        if (!validSkus.has(skuLower)) {
          // This variant is NOT in the merged inventory - candidate for deletion
          // But we need to check if it belongs to a product owned by this data source
          // For now, we match by style prefix in SKU
          const styleMatch = skuLower.match(/^jovani-(\d+)/i);
          if (styleMatch) {
            candidateCount++;
            const productId = variant.shopifyProductId;
            if (productId) {
              if (!variantsToDelete.has(productId)) {
                variantsToDelete.set(productId, []);
              }
              variantsToDelete.get(productId)!.push(variant.id);
            }
          }
        }
      }

      console.log(
        `[DeleteNonMatching] Candidates for deletion: ${candidateCount} variants across ${variantsToDelete.size} products`,
      );

      // Check safety limits
      const variantSyncConfig = dataSource.variantSyncConfig as {
        maxDeletionLimit?: number;
      } | null;
      const maxDeletionLimit = variantSyncConfig?.maxDeletionLimit || 100;

      if (candidateCount > maxDeletionLimit) {
        return res.status(400).json({
          error: `Would delete ${candidateCount} variants, but limit is ${maxDeletionLimit}. Increase limit in data source settings or verify inventory data.`,
          candidateCount,
          maxLimit: maxDeletionLimit,
        });
      }

      // Actually delete the variants
      let deletedCount = 0;
      let failedCount = 0;
      const errors: string[] = [];

      for (const [productId, variantIds] of Array.from(
        variantsToDelete.entries(),
      )) {
        try {
          const result = await shopify.bulkDeleteVariantsByProduct(
            productId,
            variantIds,
          );
          deletedCount += result.success;
          failedCount += result.failed;
          if (result.errors.length > 0) {
            errors.push(...result.errors);
          }

          // Only remove SUCCESSFULLY deleted variants from cache
          // If all succeeded, result.success === variantIds.length
          // If some failed, we need to determine which ones succeeded
          if (result.success > 0) {
            if (result.success === variantIds.length) {
              // All succeeded - delete all from cache
              await storage.deleteVariantCacheByIds(req.params.id, variantIds);
            } else {
              // Partial success - we can't reliably know which ones failed
              // from the current API response, so we conservatively keep cache
              // and let the next cache refresh fix it
              console.log(
                `[DeleteNonMatching] Partial deletion for product ${productId}: ${result.success}/${variantIds.length} - skipping cache update to avoid corruption`,
              );
            }
          }
        } catch (err: any) {
          failedCount += variantIds.length;
          errors.push(`Product ${productId}: ${err.message}`);
        }
      }

      console.log(
        `[DeleteNonMatching] Complete: deleted=${deletedCount}, failed=${failedCount}`,
      );

      res.json({
        deletedCount,
        failedCount,
        errors,
        message: `Deleted ${deletedCount} non-matching variants`,
      });
    } catch (error: any) {
      console.error("Error deleting non-matching variants:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to delete variants" });
    }
  });

  // =========== VARIANT CACHE ROUTES ===========

  // Track active variant cache refresh operations with progress
  interface CacheRefreshProgress {
    controller: AbortController;
    variantsProcessed: number;
    pagesProcessed: number;
    startedAt: Date;
  }
  const activeVariantCacheRefreshes = new Map<string, CacheRefreshProgress>();

  // Refresh variant cache - fetches all variants from Shopify and stores in cache
  // Supports query params: ?useBulkApi=true (default: true for faster refresh)
  app.post(
    "/api/shopify/stores/:id/variant-cache/refresh",
    async (req, res) => {
      try {
        const storeId = req.params.id;

        // Parse options from query params - default to Bulk API (faster)
        const useBulkApi = req.query.useBulkApi !== "false"; // Default true

        // Check if already running (use centralized check)
        if (isCacheRefreshRunning(storeId)) {
          return res
            .status(409)
            .json({ error: "Cache refresh already in progress" });
        }

        const store = await storage.getShopifyStore(storeId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        // Start async refresh - respond immediately
        res.json({
          status: "started",
          message: `Variant cache refresh started (${useBulkApi ? "Bulk API" : "cursor pagination"})`,
          useBulkApi,
        });

        // Run refresh in background using centralized function
        (async () => {
          try {
            console.log(
              `[Variant Cache] Starting refresh for store ${storeId} (useBulkApi=${useBulkApi})`,
            );

            const result = await refreshVariantCacheForStore(
              storeId,
              (variantsProcessed, pagesProcessed) => {
                // Progress callback - logged by the centralized function
              },
              { useBulkApi },
            );

            if (result.success) {
              console.log(
                `[Variant Cache] Completed: ${result.variantsProcessed} variants cached`,
              );

              // Run eBay new product detection after variant cache completes
              try {
                const { detectNewProductsAfterCacheSync } = await import(
                  "./ebayAutomation"
                );
                console.log(
                  `[Variant Cache] Running eBay new product detection for store ${storeId}`,
                );
                const detectionResult =
                  await detectNewProductsAfterCacheSync(storeId);
                console.log(
                  `[Variant Cache] eBay detection complete: ${detectionResult.addedToQueue} queued, ${detectionResult.addedToWatchlist} watchlisted, ${detectionResult.promotedFromWatchlist} promoted`,
                );
              } catch (detectionError: any) {
                console.error(
                  `[Variant Cache] eBay detection error:`,
                  detectionError.message,
                );
              }
            } else {
              console.error(`[Variant Cache] Refresh failed: ${result.error}`);
            }
          } catch (error: any) {
            console.error(`[Variant Cache] Error refreshing cache:`, error);
          }
        })();
      } catch (error: any) {
        console.error("Error starting variant cache refresh:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to start cache refresh" });
      }
    },
  );

  // Cancel variant cache refresh
  app.post("/api/shopify/stores/:id/variant-cache/cancel", async (req, res) => {
    try {
      const storeId = req.params.id;

      // Try centralized cancel first
      const cancelled = cancelCacheRefresh(storeId);

      // Also check legacy local tracking
      const localProgress = activeVariantCacheRefreshes.get(storeId);
      if (localProgress) {
        localProgress.controller.abort();
        activeVariantCacheRefreshes.delete(storeId);
      }

      if (cancelled || localProgress) {
        res.json({ cancelled: true, message: "Cache refresh cancelled" });
      } else {
        res.json({ cancelled: false, message: "No cache refresh running" });
      }
    } catch (error: any) {
      console.error("Error cancelling variant cache refresh:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to cancel cache refresh" });
    }
  });

  // HARD RESET variant cache - deletes all cached data and rebuilds from Shopify
  // Use when: ghost entries, store switch, cache corruption, or fresh start needed
  app.post(
    "/api/shopify/stores/:id/variant-cache/hard-reset",
    async (req, res) => {
      try {
        const storeId = req.params.id;

        // Check if already running
        if (isCacheRefreshRunning(storeId)) {
          return res
            .status(409)
            .json({ error: "Cache refresh already in progress" });
        }

        const store = await storage.getShopifyStore(storeId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        // Start async hard reset - respond immediately
        res.json({
          status: "started",
          message:
            "Hard reset started - deleting all cached data and rebuilding from Shopify",
        });

        // Run hard reset in background
        (async () => {
          try {
            console.log(
              `[Variant Cache] HARD RESET starting for store ${storeId}`,
            );

            const { hardResetVariantCache } = await import("./shopify");
            const result = await hardResetVariantCache(
              storeId,
              (phase, detail) => {
                console.log(`[Variant Cache Hard Reset] ${phase}: ${detail}`);
              },
            );

            if (result.success) {
              console.log(
                `[Variant Cache] HARD RESET complete: ${result.variantsProcessed} variants cached`,
              );
            } else {
              console.error(
                `[Variant Cache] HARD RESET failed: ${result.error}`,
              );
            }
          } catch (error: any) {
            console.error(`[Variant Cache] HARD RESET error:`, error);
          }
        })();
      } catch (error: any) {
        console.error("Error starting variant cache hard reset:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to start hard reset" });
      }
    },
  );

  // =========== BATCH SKU FIX ROUTES ===========
  // Fix SKUs containing slashes or spaces → replace with hyphens

  // Track active SKU fix operations
  interface SkuFixProgress {
    isRunning: boolean;
    cancelled: boolean;
    current: number;
    total: number;
    fixed: number;
    failed: number;
    message: string;
    startedAt: Date;
    error?: string;
  }
  const activeSkuFixes = new Map<string, SkuFixProgress>();

  // Cancel SKU fix operation
  app.post("/api/shopify/stores/:id/fix-skus/cancel", async (req, res) => {
    try {
      const storeId = req.params.id;
      const progress = activeSkuFixes.get(storeId);
      if (progress && progress.isRunning) {
        progress.cancelled = true;
        progress.message = "Cancelling...";
        res.json({ status: "cancelling" });
      } else {
        res.json({ status: "not_running" });
      }
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Get count of SKUs needing fix
  app.get("/api/shopify/stores/:id/fix-skus/count", async (req, res) => {
    try {
      const storeId = req.params.id;
      const badSkus = await storage.getVariantsWithBadSkus(storeId);
      res.json({ count: badSkus.length });
    } catch (error: any) {
      console.error("Error getting bad SKU count:", error);
      res.status(500).json({ error: error.message || "Failed to get count" });
    }
  });

  // Get current progress
  app.get("/api/shopify/stores/:id/fix-skus/progress", async (req, res) => {
    try {
      const storeId = req.params.id;
      const progress = activeSkuFixes.get(storeId);
      if (progress) {
        res.json(progress);
      } else {
        res.json({ isRunning: false });
      }
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Fix all bad SKUs
  app.post("/api/shopify/stores/:id/fix-skus", async (req, res) => {
    try {
      const storeId = req.params.id;
      const limit = req.body.limit as number | undefined;

      // Check if already running
      const existing = activeSkuFixes.get(storeId);
      if (existing?.isRunning) {
        return res.json({
          status: "already_running",
          progress: existing,
        });
      }

      const store = await storage.getShopifyStore(storeId);
      if (!store || !store.accessToken) {
        return res.status(400).json({ error: "Store not connected" });
      }

      // Get variants to fix
      const badSkus = await storage.getVariantsWithBadSkus(storeId, limit);
      if (badSkus.length === 0) {
        return res.json({
          status: "complete",
          fixed: 0,
          failed: 0,
          message: "No SKUs need fixing",
        });
      }

      // Initialize progress
      const progress: SkuFixProgress = {
        isRunning: true,
        cancelled: false,
        current: 0,
        total: badSkus.length,
        fixed: 0,
        failed: 0,
        message: `Starting batch SKU fix for ${badSkus.length} variants...`,
        startedAt: new Date(),
      };
      activeSkuFixes.set(storeId, progress);

      // Start async fix job
      res.json({ status: "started", total: badSkus.length });

      // Process in background with concurrent requests
      (async () => {
        try {
          const shopify = createShopifyService(store);
          const concurrentLimit = 20; // 20 concurrent REST requests
          const batchSize = 100; // Process 100 at a time, 20 concurrently

          for (let i = 0; i < badSkus.length; i += batchSize) {
            // Check for cancellation
            if (progress.cancelled) {
              progress.isRunning = false;
              progress.message = `Cancelled: ${progress.fixed} fixed, ${progress.failed} failed`;
              console.log(
                `[SKU Fix] Cancelled by user at ${progress.current}/${progress.total}`,
              );
              return;
            }

            const batch = badSkus.slice(i, i + batchSize);
            progress.message = `Fixing SKUs ${i + 1}-${Math.min(i + batchSize, badSkus.length)} of ${badSkus.length}...`;

            // Process batch with concurrent requests
            for (let j = 0; j < batch.length; j += concurrentLimit) {
              if (progress.cancelled) break;

              const concurrentBatch = batch.slice(j, j + concurrentLimit);

              const results = await Promise.allSettled(
                concurrentBatch.map(async (item) => {
                  // Update Shopify
                  await shopify.updateVariantSku(
                    item.inventoryItemId,
                    item.correctedSku,
                  );
                  // Update local cache
                  await storage.updateVariantCacheSku(
                    item.variantId,
                    item.correctedSku,
                  );
                  return item;
                }),
              );

              // Count results
              for (let k = 0; k < results.length; k++) {
                progress.current++;
                const result = results[k];
                const item = concurrentBatch[k];
                if (result.status === "fulfilled") {
                  progress.fixed++;
                  console.log(
                    `[SKU Fix] Fixed: ${item.sku} → ${item.correctedSku}`,
                  );
                } else {
                  progress.failed++;
                  console.error(
                    `[SKU Fix] Failed ${item.sku}:`,
                    result.reason?.message || result.reason,
                  );
                }
              }
            }

            // Small delay between large batches to avoid overwhelming rate limits
            if (i + batchSize < badSkus.length && !progress.cancelled) {
              await new Promise((resolve) => setTimeout(resolve, 100));
            }
          }

          progress.isRunning = false;
          progress.message = `Completed: ${progress.fixed} fixed, ${progress.failed} failed`;
          console.log(
            `[SKU Fix] Complete: ${progress.fixed} fixed, ${progress.failed} failed`,
          );
        } catch (error: any) {
          progress.isRunning = false;
          progress.error = error.message;
          progress.message = `Error: ${error.message}`;
          console.error(`[SKU Fix] Error:`, error);
        }
      })();
    } catch (error: any) {
      console.error("Error starting SKU fix:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to start SKU fix" });
    }
  });

  // =========== OPTION NAME NORMALIZATION ROUTES ===========
  // These routes help fix products with non-uppercase option names (Color/Size → COLOR/SIZE)

  // Track active option normalization operations (keyed by storeId)
  interface OptionNormalizationProgress {
    isRunning: boolean;
    mode: "scan" | "fix";
    current: number;
    total: number;
    message: string;
    startedAt: Date;
    result?: any;
    error?: string;
  }
  const activeOptionNormalizations = new Map<
    string,
    OptionNormalizationProgress
  >();

  // Scan for products with non-uppercase option names (dry-run)
  app.get(
    "/api/shopify/stores/:id/normalize-options/scan",
    async (req, res) => {
      try {
        const storeId = req.params.id;

        // Check if already running for THIS store
        const existing = activeOptionNormalizations.get(storeId);
        if (existing?.isRunning) {
          return res.json({
            status: "running",
            mode: existing.mode,
            current: existing.current,
            total: existing.total,
            message: existing.message,
          });
        }

        const store = await storage.getShopifyStore(storeId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        // Start scan in background - create fresh progress object
        const progress: OptionNormalizationProgress = {
          isRunning: true,
          mode: "scan",
          current: 0,
          total: 0,
          message: "Starting scan...",
          startedAt: new Date(),
          result: undefined,
          error: undefined,
        };
        activeOptionNormalizations.set(storeId, progress);

        res.json({
          status: "started",
          message: "Scanning for products with non-uppercase option names",
        });

        // Run scan async
        (async () => {
          try {
            const service = new ShopifyService(
              store.storeUrl,
              store.accessToken,
            );
            const result = await service.scanProductsWithNonUppercaseOptions(
              (current, total, message) => {
                progress.current = current;
                progress.total = total > 0 ? total : current; // Ensure total is never 0 if we have current
                progress.message = message;
              },
            );

            progress.isRunning = false;
            progress.result = result;
            progress.total = result.totalProducts;
            progress.current = result.totalProducts;
            progress.message = `Scan complete: ${result.productsNeedingFix.length} products need fixing`;
            console.log(
              `[Normalize Options] Scan complete: ${result.productsNeedingFix.length}/${result.totalProducts} products need fixing`,
            );
          } catch (error: any) {
            progress.isRunning = false;
            progress.error = error.message;
            progress.message = `Scan failed: ${error.message}`;
            console.error(`[Normalize Options] Scan error:`, error);
          }
        })();
      } catch (error: any) {
        console.error("Error starting option normalization scan:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to start scan" });
      }
    },
  );

  // Get scan/fix progress - returns a COPY of the data to prevent mutation
  app.get(
    "/api/shopify/stores/:id/normalize-options/progress",
    async (req, res) => {
      try {
        const storeId = req.params.id;
        const progress = activeOptionNormalizations.get(storeId);

        if (!progress) {
          return res.json({
            status: "idle",
            message: "No scan or fix in progress",
            current: 0,
            total: 0,
          });
        }

        // Return a clean copy of the data
        res.json({
          status: progress.isRunning ? "running" : "complete",
          mode: progress.mode,
          current: progress.current || 0,
          total: progress.total || 0,
          message: progress.message || "",
          startedAt: progress.startedAt.toISOString(),
          result: progress.result ? { ...progress.result } : undefined,
          error: progress.error,
        });
      } catch (error: any) {
        console.error("Error getting option normalization progress:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to get progress" });
      }
    },
  );

  // Clear/reset progress for a store
  app.delete(
    "/api/shopify/stores/:id/normalize-options/progress",
    async (req, res) => {
      try {
        const storeId = req.params.id;
        activeOptionNormalizations.delete(storeId);
        res.json({ success: true, message: "Progress cleared" });
      } catch (error: any) {
        res
          .status(500)
          .json({ error: error.message || "Failed to clear progress" });
      }
    },
  );

  // Fix all products with non-uppercase option names
  app.post(
    "/api/shopify/stores/:id/normalize-options/fix",
    async (req, res) => {
      try {
        const storeId = req.params.id;

        // Check if already running for THIS store
        const existing = activeOptionNormalizations.get(storeId);
        if (existing?.isRunning) {
          return res.status(409).json({
            error: "Operation already in progress",
            mode: existing.mode,
            current: existing.current,
            total: existing.total,
          });
        }

        const store = await storage.getShopifyStore(storeId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        // Start fix in background - create fresh progress object
        const progress: OptionNormalizationProgress = {
          isRunning: true,
          mode: "fix",
          current: 0,
          total: 0,
          message: "Starting fix...",
          startedAt: new Date(),
          result: undefined,
          error: undefined,
        };
        activeOptionNormalizations.set(storeId, progress);

        res.json({
          status: "started",
          message: "Fixing products with non-uppercase option names",
        });

        // Run fix async
        (async () => {
          try {
            const service = new ShopifyService(
              store.storeUrl,
              store.accessToken,
            );
            const result = await service.fixAllProductOptionNames(
              (current, total, message) => {
                progress.current = current;
                progress.total = total > 0 ? total : current; // Ensure total is never 0 if we have current
                progress.message = message;
              },
            );

            progress.isRunning = false;
            progress.result = result;
            progress.total = result.totalScanned;
            progress.current = result.totalScanned;
            progress.message = `Fix complete: ${result.totalFixed} products fixed, ${result.totalFailed} failed`;
            console.log(
              `[Normalize Options] Fix complete: ${result.totalFixed} fixed, ${result.totalFailed} failed out of ${result.totalScanned} scanned`,
            );
          } catch (error: any) {
            progress.isRunning = false;
            progress.error = error.message;
            progress.message = `Fix failed: ${error.message}`;
            console.error(`[Normalize Options] Fix error:`, error);
          }
        })();
      } catch (error: any) {
        console.error("Error starting option normalization fix:", error);
        res.status(500).json({ error: error.message || "Failed to start fix" });
      }
    },
  );

  // Get variant cache stats with progress info
  app.get("/api/shopify/stores/:id/variant-cache/stats", async (req, res) => {
    try {
      const storeId = req.params.id;
      const stats = await storage.getVariantCacheStats(storeId);

      // Check for progress from both manual (activeVariantCacheRefreshes) and scheduled refreshes
      const manualProgress = activeVariantCacheRefreshes.get(storeId);
      const scheduledProgress = getCacheRefreshProgress(storeId);

      // Check for hard reset progress
      const hardResetProgress = getHardResetProgress(storeId);

      // Use manual progress first (API-triggered), fall back to scheduled
      const isRefreshing =
        !!manualProgress ||
        scheduledProgress.isRefreshing ||
        !!hardResetProgress;

      // Handle lastSyncedAt which might be a Date object or a string at runtime
      // Database stores UTC but without timezone indicator - add 'Z' so JS parses as UTC
      let lastSyncedAtStr: string | null = null;
      if (stats.lastSyncedAt) {
        const rawValue = stats.lastSyncedAt as unknown;
        if (typeof rawValue === "string") {
          // Check if already ISO format with timezone (ends with Z or +/-offset)
          if (rawValue.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(rawValue)) {
            lastSyncedAtStr = rawValue;
          } else if (rawValue.includes(" ")) {
            // Database returns format like "2025-12-24 19:58:40.520831" (UTC without indicator)
            // Convert to ISO format with Z suffix: "2025-12-24T19:58:40.520831Z"
            lastSyncedAtStr = rawValue.replace(" ", "T") + "Z";
          } else {
            // Fallback: assume ISO format, append Z if no timezone
            lastSyncedAtStr = rawValue + "Z";
          }
        } else if (rawValue instanceof Date) {
          lastSyncedAtStr = rawValue.toISOString();
        }
      }

      // Build progress info from either source
      let progressInfo: {
        variantsProcessed?: number;
        pagesProcessed?: number;
        refreshStartedAt?: string;
      } = {};

      if (manualProgress) {
        progressInfo = {
          variantsProcessed: manualProgress.variantsProcessed,
          pagesProcessed: manualProgress.pagesProcessed,
          refreshStartedAt: manualProgress.startedAt.toISOString(),
        };
      } else if (scheduledProgress.isRefreshing) {
        progressInfo = {
          variantsProcessed: scheduledProgress.variantsProcessed,
          pagesProcessed: scheduledProgress.pagesProcessed,
          refreshStartedAt: scheduledProgress.refreshStartedAt,
        };
      }

      // Get last completed refresh stats (persists until next refresh)
      const lastRefreshStats = getLastCacheRefreshStats(storeId);

      res.json({
        count: stats.total,
        lastSyncedAt: lastSyncedAtStr,
        isRefreshing,
        bulkStatus:
          scheduledProgress.bulkOperationStatus ||
          manualProgress?.bulkOperationStatus ||
          null,
        bulkObjectCount:
          scheduledProgress.bulkObjectCount ||
          manualProgress?.bulkObjectCount ||
          0,
        lineTypeCounts: scheduledProgress.lineTypeCounts || null,
        stockAssigned: scheduledProgress.stockAssigned || 0,
        stockMissing: scheduledProgress.stockMissing || 0,
        // Hard reset progress
        hardReset: hardResetProgress
          ? {
              phase: hardResetProgress.phase,
              detail: hardResetProgress.detail,
              startedAt: hardResetProgress.startedAt.toISOString(),
              deletedCount: hardResetProgress.deletedCount,
              variantsProcessed: hardResetProgress.variantsProcessed,
            }
          : null,
        // Persist last refresh stats when not actively refreshing
        lastRefreshStats:
          !isRefreshing && lastRefreshStats
            ? {
                completedAt: lastRefreshStats.completedAt.toISOString(),
                variantsProcessed: lastRefreshStats.variantsProcessed,
                lineTypeCounts: lastRefreshStats.lineTypeCounts,
                stockAssigned: lastRefreshStats.stockAssigned,
                stockMissing: lastRefreshStats.stockMissing,
                durationMs: lastRefreshStats.durationMs,
              }
            : null,
        ...progressInfo,
      });
    } catch (error: any) {
      console.error("Error getting variant cache stats:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get cache stats" });
    }
  });

  // =============================================================================
  // SYNC AUDIT: Compare Shopify vs Database - ONLY checks what's in Shopify
  // Groups results by vendor/brand - ONLY shows vendors that have data in DB
  // Uses price multiplier from data source config for accurate price comparison
  // VENDOR MATCHING: "Jovani Sale" and "Jovani" are treated as same vendor "Jovani"
  // =============================================================================
  app.get("/api/shopify/stores/:id/sync-audit", async (req, res) => {
    try {
      const storeId = req.params.id;
      const vendorFilter = req.query.vendor as string | undefined;
      const dataSourceId = req.query.dataSourceId as string | undefined;
      const limit = parseInt(req.query.limit as string) || 500;

      console.log(
        `[Sync Audit] Starting audit for store ${storeId}${vendorFilter ? `, vendor: ${vendorFilter}` : ""}${dataSourceId ? `, dataSourceId: ${dataSourceId}` : ""}`,
      );
      const startTime = Date.now();

      // =====================================================================
      // INLINE HELPER FUNCTIONS (mirrored from shopify-5.ts sync handlers)
      // =====================================================================

      const KNOWN_BRANDS = [
        "jovani",
        "jvn",
        "madeline",
        "mac duggal",
        "sherri hill",
        "la femme",
        "colors",
        "colors dress",
        "tarik ediz",
        "ava presley",
        "gia franco",
        "terani",
        "terani couture",
        "primavera",
        "primavera couture",
        "alyce paris",
        "alyce",
        "rachel allan",
        "morilee",
        "mori lee",
        "ashley lauren",
        "portia and scarlett",
        "scala",
        "faviana",
        "ellie wilde",
        "amarra",
        "noir by lazaro",
        "lazaro",
        "johnathan kayne",
        "sydney's closet",
        "mnm couture",
        "gls by gloria",
        "aspeed",
        "cinderella divine",
        "dancing queen",
        "may queen",
        "juliet",
        "nox anabel",
        "andrea and leo",
      ];

      const BRAND_ALIASES: Record<string, string[]> = {
        jovani: ["jovani", "jvn", "madeline", "jovani jvn", "jovani madeline"],
        "mac duggal": ["mac duggal", "macduggal", "mac-duggal"],
        "sherri hill": ["sherri hill", "sherri-hill"],
        "la femme": ["la femme", "la-femme", "lafemme"],
        colors: ["colors", "colors dress"],
        "tarik ediz": ["tarik ediz", "tarik-ediz"],
        "ava presley": ["ava presley", "ava-presley"],
        "gia franco": ["gia franco", "gia-franco"],
        terani: ["terani", "terani couture"],
        primavera: ["primavera", "primavera couture"],
        alyce: ["alyce", "alyce paris"],
        "mori lee": ["mori lee", "morilee"],
        "portia and scarlett": [
          "portia and scarlett",
          "portia & scarlett",
          "portia-and-scarlett",
          "portia",
        ],
        "andrea and leo": ["andrea and leo", "andrea & leo", "andrea-and-leo"],
      };

      // Extract brand name from data source name (shopify-5.ts line 143-165)
      const extractBrandFromDataSourceName = (
        dataSourceName: string | null | undefined,
      ): string | null => {
        if (!dataSourceName) return null;
        const dsLower = dataSourceName.toLowerCase().trim();
        const sortedBrands = [...KNOWN_BRANDS].sort(
          (a, b) => b.length - a.length,
        );
        for (const brand of sortedBrands) {
          if (dsLower === brand) return brand;
          if (
            dsLower.startsWith(brand + " ") ||
            dsLower.startsWith(brand + "-") ||
            dsLower.startsWith(brand + "_")
          ) {
            return brand;
          }
        }
        const suffixPattern =
          /\s*[-_]?\s*(inventory|sales|sale|main|file|data|import|export|catalog|feed|master|backup|test|dev|prod|production|staging).*$/i;
        const cleaned = dsLower.replace(suffixPattern, "").trim();
        if (cleaned) return cleaned;
        const firstWord = dsLower.split(/[\s\-_]/)[0];
        return firstWord || null;
      };

      // Extract brand from product title (shopify-5.ts line 167-199)
      // Extracts brand as all words BEFORE the first word containing a digit
      // "andrea and leo a1017" -> "andrea and leo"
      const extractBrandFromProductTitle = (
        productTitle: string | null | undefined,
      ): string | null => {
        if (!productTitle) return null;
        const titleLower = productTitle.toLowerCase().trim();
        const sortedBrands = [...KNOWN_BRANDS].sort(
          (a, b) => b.length - a.length,
        );
        for (const brand of sortedBrands) {
          if (
            titleLower.startsWith(brand + " ") ||
            titleLower.startsWith(brand + "-") ||
            titleLower === brand
          ) {
            return brand;
          }
        }
        const words = titleLower.split(/[\s\-]+/);
        let brandEndIndex = -1;
        for (let i = 0; i < words.length; i++) {
          if (/\d/.test(words[i])) {
            brandEndIndex = i;
            break;
          }
        }
        if (brandEndIndex > 0) {
          return words.slice(0, brandEndIndex).join(" ");
        }
        const firstWord = titleLower.split(/[\s\-]/)[0];
        return firstWord || null;
      };

      // Get all aliases for a brand (shopify-5.ts line 202-208)
      const getBrandAliases = (brand: string): string[] => {
        const brandLower = brand.toLowerCase().trim();
        if (BRAND_ALIASES[brandLower]) return BRAND_ALIASES[brandLower];
        for (const [_parent, aliases] of Object.entries(BRAND_ALIASES)) {
          if (aliases.includes(brandLower)) return aliases;
        }
        return [brandLower];
      };

      // Check if a data source can modify a product (shopify-5.ts line 313-344)
      const canDataSourceModifyProduct = (
        dataSourceName: string | null | undefined,
        productTitle: string | null | undefined,
      ): boolean => {
        if (!dataSourceName || !productTitle) return false;
        const dsBrand = extractBrandFromDataSourceName(dataSourceName);
        if (!dsBrand) return false;
        const titleLower = productTitle.toLowerCase().trim();
        const dsBrandNorm = dsBrand.replace(/-/g, " ");
        const titleNorm = titleLower.replace(/-/g, " ");
        if (
          titleNorm.startsWith(dsBrandNorm + " ") ||
          titleLower.startsWith(dsBrand + "-") ||
          titleNorm === dsBrandNorm
        ) {
          return true;
        }
        const productBrand = extractBrandFromProductTitle(productTitle);
        if (!productBrand) return false;
        const dsAliases = getBrandAliases(dsBrand);
        return dsAliases.includes(productBrand.toLowerCase());
      };

      // Normalize SKU for matching (shopify-5.ts line 1308-1327)
      const normalizeSkuForMatching = (sku: string): string => {
        if (!sku) return "";
        let normalized = sku
          .toLowerCase()
          .trim()
          .replace(/\//g, "-")
          .replace(/\s+/g, "-")
          .replace(/-+/g, "-")
          .replace(/^-+|-+$/g, "");
        normalized = normalized.replace(/-0(\d+)$/, (match, digits) => {
          if (match === "-00" || match === "-000") return match;
          return `-${parseInt(digits, 10)}`;
        });
        return normalized;
      };

      // Get SKU variations for flexible matching (shopify-5.ts line 1764-1785)
      const getSkuVariations = (sku: string): string[] => {
        if (!sku) return [];
        const normalized = normalizeSkuForMatching(sku);
        const withSpaces = sku.toLowerCase().trim();
        const withHyphens = sku.toLowerCase().trim().replace(/\s+/g, "-");
        const variations = new Set<string>([normalized]);
        if (withSpaces !== normalized) variations.add(withSpaces);
        if (withHyphens !== normalized) variations.add(withHyphens);
        const spaceAfterStyle = withHyphens.replace(
          /^([a-z-]+-\d+)-(.+)-(\d+)$/,
          (_, prefix, middle, size) =>
            `${prefix}-${middle.replace(/-/g, " ")}-${size}`,
        );
        if (spaceAfterStyle !== withHyphens) variations.add(spaceAfterStyle);
        return Array.from(variations);
      };

      // Canonical key normalization functions (shopify-5.ts lines 215-274)
      const normalizeStyleForCanonicalKey = (
        style: string | null | undefined,
      ): string => {
        if (!style) return "";
        return style
          .toLowerCase()
          .trim()
          .replace(/\s+/g, "-")
          .replace(/-+/g, "-")
          .replace(/^-|-$/g, "");
      };

      const normalizeColorForCanonicalKey = (
        color: string | null | undefined,
      ): string => {
        if (!color) return "";
        return color
          .toLowerCase()
          .trim()
          .replace(/\//g, "-")
          .replace(/\s+/g, "-")
          .replace(/-+/g, "-")
          .replace(/^-|-$/g, "");
      };

      const normalizeSizeForCanonicalKey = (
        size: string | null | undefined,
      ): string => {
        if (!size) return "";
        let normalized = String(size).toLowerCase().trim();
        if (normalized === "00" || normalized === "000") return normalized;
        if (/^\d+$/.test(normalized) && normalized.length > 1) {
          normalized = normalized.replace(/^0+/, "") || "0";
        }
        return normalized;
      };

      const generateCanonicalKey = (
        style: string | null | undefined,
        color: string | null | undefined,
        size: string | null | undefined,
      ): string => {
        const nStyle = normalizeStyleForCanonicalKey(style);
        const nColor = normalizeColorForCanonicalKey(color);
        const nSize = normalizeSizeForCanonicalKey(size);
        return `${nStyle}|${nColor}|${nSize}`;
      };

      // Extract style portion from product title (everything after the brand prefix)
      const extractStyleFromTitle = (title: string): string => {
        if (!title) return "";
        const brand = extractBrandFromProductTitle(title);
        if (!brand) return title.toLowerCase().trim();
        const titleLower = title.toLowerCase().trim();
        if (titleLower.startsWith(brand + " ")) {
          return titleLower.substring(brand.length + 1).trim();
        }
        if (titleLower.startsWith(brand + "-")) {
          return titleLower.substring(brand.length + 1).trim();
        }
        if (titleLower === brand) return "";
        return titleLower;
      };

      // Extract color and size from variant by option NAME (not position)
      // Same logic as buildProductIndex in shopify-5.ts lines 1438-1463
      const extractColorSizeFromVariant = (
        v: any,
      ): { color: string; size: string } => {
        let color = "";
        let size = "";
        if (v.option1Name?.toLowerCase() === "color") {
          color = v.option1Value || "";
        } else if (v.option1Name?.toLowerCase() === "size") {
          size = v.option1Value || "";
        }
        if (v.option2Name?.toLowerCase() === "color") {
          color = v.option2Value || "";
        } else if (v.option2Name?.toLowerCase() === "size") {
          size = v.option2Value || "";
        }
        // Fallback: positional if no option names match
        if (!color && !size) {
          color = v.option1Value || "";
          size = v.option2Value || "";
        }
        // Final fallback: extract from SKU
        if ((!color || !size) && v.sku) {
          const skuParts = v.sku.split("-");
          if (skuParts.length >= 3) {
            size = size || skuParts[skuParts.length - 1] || "";
            color = color || skuParts[skuParts.length - 2] || "";
          }
        }
        return { color, size };
      };

      // =====================================================================
      // DATA LOADING
      // =====================================================================

      const dataSources = await storage.getDataSources();
      let activeDataSources = dataSources.filter(
        (ds) => ds.status === "active",
      );

      // Filter by dataSourceId if provided
      if (dataSourceId) {
        activeDataSources = activeDataSources.filter(
          (ds) => ds.id === dataSourceId,
        );
        if (activeDataSources.length === 0) {
          return res.json({
            summary: {
              totalShopifyVariants: 0,
              dataSourcesAnalyzed: 0,
              totalMatched: 0,
              totalNotInDb: 0,
              totalStockMismatches: 0,
              totalPriceMismatches: 0,
              totalStockInfoMismatches: 0,
              totalDiscontinuedProducts: 0,
              totalDiscontinuedVariants: 0,
              durationMs: Date.now() - startTime,
            },
            byDataSource: {},
            message: `Data source ${dataSourceId} not found or not active`,
          });
        }
      }

      // Build data source config map with variant sync config
      const dsConfigMap = new Map<
        string,
        {
          id: string;
          name: string;
          vendor: string;
          priceMultiplier: number;
          useFilePrice: boolean;
          enableVariantDeletion: boolean;
          deleteAction: string;
          maxDeletionLimit: number;
        }
      >();

      for (const ds of activeDataSources) {
        const regularPriceConfig = ds.regularPriceConfig as {
          useFilePrice?: boolean;
          priceMultiplier?: number;
        } | null;
        const salesConfig = ds.salesConfig as {
          priceMultiplier?: number;
        } | null;
        const variantSyncConfig = ((ds as any).variantSyncConfig || {}) as {
          enableVariantDeletion?: boolean;
          deleteAction?: string;
          maxDeletionLimit?: number;
        };

        let priceMultiplier = 1;
        let useFilePrice = false;

        if (ds.sourceType === "sales") {
          priceMultiplier = salesConfig?.priceMultiplier || 2;
          useFilePrice = true;
        } else if (regularPriceConfig?.useFilePrice) {
          priceMultiplier = regularPriceConfig.priceMultiplier || 1;
          useFilePrice = true;
        }

        const vendor =
          extractBrandFromDataSourceName(ds.name) ||
          ds.name.toLowerCase().trim();

        dsConfigMap.set(ds.id, {
          id: ds.id,
          name: ds.name,
          vendor,
          priceMultiplier,
          useFilePrice,
          enableVariantDeletion:
            variantSyncConfig.enableVariantDeletion || false,
          deleteAction: variantSyncConfig.deleteAction || "delete",
          maxDeletionLimit: variantSyncConfig.maxDeletionLimit || 0,
        });

        console.log(
          `[Sync Audit] DS "${ds.name}" -> vendor: "${vendor}", deletion: ${variantSyncConfig.enableVariantDeletion || false}`,
        );
      }

      // Load inventory in parallel (performance fix)
      const inventoryResults = await Promise.all(
        activeDataSources.map(async (ds) => ({
          dsId: ds.id,
          dsName: ds.name,
          items: await storage.getInventoryItems(ds.id),
        })),
      );

      const dataSourcesWithData = new Set<string>();
      const inventoryByDataSource = new Map<string, any[]>();

      for (const { dsId, dsName, items } of inventoryResults) {
        if (items.length > 0) {
          dataSourcesWithData.add(dsId);
          inventoryByDataSource.set(dsId, items);
          const config = dsConfigMap.get(dsId);
          console.log(
            `[Sync Audit] DS "${dsName}" has ${items.length} items (vendor: ${config?.vendor}, multiplier: ${config?.priceMultiplier || 1})`,
          );
        }
      }

      if (dataSourcesWithData.size === 0) {
        return res.json({
          summary: {
            totalShopifyVariants: 0,
            dataSourcesAnalyzed: 0,
            totalMatched: 0,
            totalNotInDb: 0,
            totalStockMismatches: 0,
            totalPriceMismatches: 0,
            totalStockInfoMismatches: 0,
            totalDiscontinuedProducts: 0,
            totalDiscontinuedVariants: 0,
            durationMs: Date.now() - startTime,
          },
          byDataSource: {},
          message: "No data sources have inventory data in DB",
        });
      }

      // Get all Shopify variants from cache
      const shopifyVariants = await storage.getVariantCacheForStore(storeId);
      console.log(
        `[Sync Audit] Loaded ${shopifyVariants.length} Shopify variants from cache`,
      );

      // Build Shopify variant index by SKU (same as sync preview shopify-5.ts lines 16559-16602)
      const shopifyVariantsBySku = new Map<string, any>();
      for (const v of shopifyVariants) {
        if (v.sku) {
          const skuVariations = getSkuVariations(v.sku);
          for (const skuVariation of skuVariations) {
            if (!shopifyVariantsBySku.has(skuVariation)) {
              shopifyVariantsBySku.set(skuVariation, v);
            }
          }
          const normalizedSku = normalizeSkuForMatching(v.sku);
          if (!shopifyVariantsBySku.has(normalizedSku)) {
            shopifyVariantsBySku.set(normalizedSku, v);
          }
        }
      }
      console.log(
        `[Sync Audit] Built SKU index with ${shopifyVariantsBySku.size} entries`,
      );

      // =====================================================================
      // BUILD INVENTORY INDEX (canonical keys + SKU-based fallback)
      // Same approach as sync preview in shopify-5.ts
      // =====================================================================

      // Primary index: dsId|canonicalKey -> inventory item
      const inventoryByDsCanonicalKey = new Map<string, any>();
      // SKU-based fallback index: dsId|normalizedSku -> inventory item
      const inventoryByDsSku = new Map<string, any>();

      // Group DB variants by dsId|canonicalStyle for variant count comparison
      const dbVariantsByDsStyle = new Map<
        string,
        Array<{
          color: string;
          size: string;
          sku: string;
          normalizedColor: string;
          normalizedSize: string;
        }>
      >();

      for (const [dsId, items] of inventoryByDataSource) {
        const dsConfig = dsConfigMap.get(dsId);

        for (const item of items) {
          const style = item.style || "";
          const color = item.color || "";
          const size = String(item.size ?? "");

          // Generate canonical key using same functions as sync
          const canonicalKey = generateCanonicalKey(style, color, size);
          const canonicalStyle = normalizeStyleForCanonicalKey(style);
          const normalizedColor = normalizeColorForCanonicalKey(color);
          const normalizedSize = normalizeSizeForCanonicalKey(size);

          // Attach dataSourceId for price multiplier lookup downstream
          const itemWithDs = {
            ...item,
            dataSourceId: dsId,
            dsName: dsConfig?.name || "",
          };

          // Primary index: dsId|canonicalKey
          if (canonicalStyle) {
            const key = `${dsId}|${canonicalKey}`;
            if (!inventoryByDsCanonicalKey.has(key)) {
              inventoryByDsCanonicalKey.set(key, itemWithDs);
            }

            // Track variants per dsId|style for variant count comparison
            const dsStyleKey = `${dsId}|${canonicalStyle}`;
            if (!dbVariantsByDsStyle.has(dsStyleKey)) {
              dbVariantsByDsStyle.set(dsStyleKey, []);
            }
            dbVariantsByDsStyle.get(dsStyleKey)!.push({
              color,
              size,
              sku: item.sku || `${style}-${color}-${size}`,
              normalizedColor,
              normalizedSize,
            });
          }

          // SKU-based fallback index: dsId|normalizedSku
          const itemSku = item.sku || `${style}-${color}-${size}`;
          if (itemSku) {
            const skuVariations = getSkuVariations(itemSku);
            for (const variation of skuVariations) {
              const skuKey = `${dsId}|${variation}`;
              if (!inventoryByDsSku.has(skuKey)) {
                inventoryByDsSku.set(skuKey, itemWithDs);
              }
            }
          }
        }
      }

      console.log(
        `[Sync Audit] Inventory index: ${inventoryByDsCanonicalKey.size} canonical keys, ${inventoryByDsSku.size} SKU entries`,
      );

      // =====================================================================
      // PHASE 2: COMPARISON LOGIC — By data source, multi-strategy matching
      // =====================================================================

      // Load metafield rules for stock info comparison (3-tier fallback)
      const metafieldRules = await storage.getShopifyMetafieldRules();
      const metafieldRulesByDsId = new Map<
        string,
        (typeof metafieldRules)[0]
      >();
      let storeDefaultMetafieldRule: (typeof metafieldRules)[0] | null = null;
      for (const rule of metafieldRules) {
        if (!rule.enabled || rule.metafieldKey !== "stock_info") continue;
        if (rule.dataSourceId) {
          if (!metafieldRulesByDsId.has(rule.dataSourceId)) {
            metafieldRulesByDsId.set(rule.dataSourceId, rule);
          }
        } else if (!storeDefaultMetafieldRule) {
          storeDefaultMetafieldRule = rule;
        }
      }

      // Inline calculateStockInfo for audit (matches shopify-5.ts logic exactly)
      const auditCalculateStockInfo = (
        rule: any,
        item: {
          stock?: number | null;
          shipDate?: string | null;
          isExpandedSize?: boolean | null;
        },
      ): string | null => {
        if (!rule || !rule.enabled) return null;
        const stock = item.stock ?? 0;
        const shipDateValue = item.shipDate;
        const threshold = rule.stockThreshold || 0;
        // Priority 1: Expanded size
        if (item.isExpandedSize && rule.sizeExpansionMessage) {
          return rule.sizeExpansionMessage;
        }
        // Priority 2: Future ship date
        if (shipDateValue && rule.futureDateMessage) {
          try {
            const dateStr = String(shipDateValue).trim();
            let targetDate: Date;
            const isoMatch = dateStr.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
            const usMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
            const usShortMatch = dateStr.match(
              /^(\d{1,2})\/(\d{1,2})\/(\d{2})$/,
            );
            if (isoMatch) {
              const [, yr, mo, dy] = isoMatch;
              targetDate = new Date(
                parseInt(yr),
                parseInt(mo) - 1,
                parseInt(dy),
              );
            } else if (usMatch) {
              const [, mo, dy, yr] = usMatch;
              targetDate = new Date(
                parseInt(yr),
                parseInt(mo) - 1,
                parseInt(dy),
              );
            } else if (usShortMatch) {
              const [, mo, dy, shortYr] = usShortMatch;
              targetDate = new Date(
                2000 + parseInt(shortYr),
                parseInt(mo) - 1,
                parseInt(dy),
              );
            } else {
              targetDate = new Date(dateStr);
            }
            const offsetDays = rule.dateOffsetDays || 0;
            if (offsetDays !== 0)
              targetDate.setDate(targetDate.getDate() + offsetDays);
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            targetDate.setHours(0, 0, 0, 0);
            if (targetDate > today) {
              const formattedDate = targetDate.toLocaleDateString("en-US", {
                month: "long",
                day: "numeric",
                year: "numeric",
              });
              return rule.futureDateMessage.replace(
                /\{date\}/gi,
                formattedDate,
              );
            }
          } catch (e) {
            // Fall through to stock-based messages
          }
        }
        // Priority 3: In stock
        if (stock > threshold) return rule.inStockMessage || null;
        // Priority 4: Out of stock
        let msg = rule.outOfStockMessage || null;
        if (msg && msg.includes("{date}")) {
          msg = msg
            .replace(/\{date\}/gi, "")
            .replace(/\s+/g, " ")
            .trim();
        }
        return msg;
      };

      // Resolve metafield rule for a data source (3-tier fallback)
      const getMetafieldRuleForDs = (dsId: string): any => {
        // Tier 1: DS-specific rule
        const dsRule = metafieldRulesByDsId.get(dsId);
        if (dsRule) return dsRule;
        // Tier 2: Store default rule
        if (storeDefaultMetafieldRule) return storeDefaultMetafieldRule;
        // Tier 3: stockInfoConfig on DS
        const ds = activeDataSources.find((d) => d.id === dsId);
        const cfg = (ds as any)?.stockInfoConfig as any;
        if (!cfg) return null;
        return {
          enabled: true,
          stockThreshold: 0,
          inStockMessage: cfg.message1InStock || "",
          sizeExpansionMessage: cfg.message2ExtraSizes || null,
          outOfStockMessage: cfg.message3Default || "",
          futureDateMessage: cfg.message4FutureDate || null,
          dateOffsetDays: cfg.dateOffsetDays ?? 0,
        };
      };

      // Results by data source
      const dsResults: Record<
        string,
        {
          dataSourceId: string;
          totalInShopify: number;
          matchedInDb: number;
          notInDb: number;
          stockMismatches: number;
          priceMismatches: number;
          stockInfoMismatches: number;
          discontinuedProducts: number;
          discontinuedVariants: number;
          enableVariantDeletion: boolean;
          deleteAction: string;
          maxDeletionLimit: number;
          notInDbList: any[];
          stockMismatchList: any[];
          priceMismatchList: any[];
          stockInfoMismatchList: any[];
          discontinuedList: any[];
        }
      > = {};

      // Track products for discontinued detection:
      // productId -> { total variants in Shopify, matched variants in DB, owner info }
      const productMatchCount = new Map<
        string,
        {
          total: number;
          matched: number;
          ownerDsId: string;
          ownerDsName: string;
          productTitle: string;
        }
      >();

      // Initialize per-DS result blocks
      for (const [dsId, config] of dsConfigMap) {
        dsResults[config.name] = {
          dataSourceId: dsId,
          totalInShopify: 0,
          matchedInDb: 0,
          notInDb: 0,
          stockMismatches: 0,
          priceMismatches: 0,
          stockInfoMismatches: 0,
          discontinuedProducts: 0,
          discontinuedVariants: 0,
          enableVariantDeletion: config.enableVariantDeletion,
          deleteAction: config.deleteAction,
          maxDeletionLimit: config.maxDeletionLimit,
          notInDbList: [],
          stockMismatchList: [],
          priceMismatchList: [],
          stockInfoMismatchList: [],
          discontinuedList: [],
        };
      }

      // Unowned bucket for variants no DS claims ownership of
      dsResults["Unowned"] = {
        dataSourceId: "none",
        totalInShopify: 0,
        matchedInDb: 0,
        notInDb: 0,
        stockMismatches: 0,
        priceMismatches: 0,
        stockInfoMismatches: 0,
        discontinuedProducts: 0,
        discontinuedVariants: 0,
        enableVariantDeletion: false,
        deleteAction: "zero_out",
        maxDeletionLimit: 0,
        notInDbList: [],
        stockMismatchList: [],
        priceMismatchList: [],
        stockInfoMismatchList: [],
        discontinuedList: [],
      };

      console.log(
        `[Sync Audit] Processing ${shopifyVariants.length} Shopify variants against ${dsConfigMap.size} data sources...`,
      );

      // Process ALL Shopify cached variants in a single pass
      for (const v of shopifyVariants) {
        // a) Extract color/size by option NAME (not position)
        const { color: shopifyColor, size: shopifySize } =
          extractColorSizeFromVariant(v);

        // b) Determine which data source owns this variant
        let ownerDsId: string | null = null;
        let ownerDsName: string | null = null;

        if (dataSourceId) {
          // Single DS filter mode — only check the specified DS
          const config = dsConfigMap.get(dataSourceId);
          if (
            config &&
            canDataSourceModifyProduct(config.name, v.productTitle)
          ) {
            ownerDsId = dataSourceId;
            ownerDsName = config.name;
          }
        } else {
          // Check all active data sources for ownership
          for (const [dsId, config] of dsConfigMap) {
            if (canDataSourceModifyProduct(config.name, v.productTitle)) {
              ownerDsId = dsId;
              ownerDsName = config.name;
              break;
            }
          }
        }

        // Apply vendor filter if provided
        if (vendorFilter && ownerDsId) {
          const config = dsConfigMap.get(ownerDsId);
          if (config && config.vendor !== vendorFilter.toLowerCase()) continue;
        }

        const bucketName = ownerDsName || "Unowned";
        const result = dsResults[bucketName];
        if (!result) continue;
        result.totalInShopify++;

        // Track for discontinued detection
        if (ownerDsId && ownerDsName) {
          if (!productMatchCount.has(v.shopifyProductId)) {
            productMatchCount.set(v.shopifyProductId, {
              total: 0,
              matched: 0,
              ownerDsId,
              ownerDsName,
              productTitle: v.productTitle || "",
            });
          }
          productMatchCount.get(v.shopifyProductId)!.total++;
        }

        // c) Try to find matching DB item using MULTIPLE strategies (same as sync preview)
        const productTitle = v.productTitle || "";
        const shopifySku = v.sku || "";
        let dbItem: any = undefined;

        // Strategy 1: Canonical key match (using generateCanonicalKey)
        if (ownerDsId) {
          const canonicalKey = generateCanonicalKey(
            productTitle,
            shopifyColor,
            shopifySize,
          );
          dbItem = inventoryByDsCanonicalKey.get(
            `${ownerDsId}|${canonicalKey}`,
          );
        }

        // Strategy 2: SKU match via inventoryByDsSku
        if (!dbItem && shopifySku && ownerDsId) {
          const normalizedSku = normalizeSkuForMatching(shopifySku);
          dbItem = inventoryByDsSku.get(`${ownerDsId}|${normalizedSku}`);
        }

        // Strategy 3: SKU variations via inventoryByDsSku
        if (!dbItem && shopifySku && ownerDsId) {
          const variations = getSkuVariations(shopifySku);
          for (const variation of variations) {
            dbItem = inventoryByDsSku.get(`${ownerDsId}|${variation}`);
            if (dbItem) break;
          }
        }

        if (!dbItem) {
          // d) Not in DB — candidate for deletion/zero-out depending on DS config
          result.notInDb++;
          if (result.notInDbList.length < limit) {
            result.notInDbList.push({
              sku: shopifySku,
              productTitle,
              productId: v.shopifyProductId,
              variantId: v.id,
              color: shopifyColor || null,
              size: shopifySize || null,
              shopifyStock: v.stock,
              shopifyPrice: v.price,
            });
          }
        } else {
          // e) Found in DB — check for mismatches
          result.matchedInDb++;

          // Track for discontinued detection
          if (ownerDsId) {
            const prodEntry = productMatchCount.get(v.shopifyProductId);
            if (prodEntry) prodEntry.matched++;
          }

          // Stock mismatch check
          const shopifyStock = v.stock ?? 0;
          const dbStock = dbItem.stock ?? 0;
          if (shopifyStock !== dbStock) {
            result.stockMismatches++;
            if (result.stockMismatchList.length < limit) {
              result.stockMismatchList.push({
                sku: shopifySku,
                productTitle,
                color: shopifyColor || null,
                size: shopifySize || null,
                shopifyStock,
                dbStock,
                difference: dbStock - shopifyStock,
              });
            }
          }

          // Price mismatch — FIX: parseFloat(toFixed(2)) instead of Math.floor
          const itemDsConfig = dsConfigMap.get(dbItem.dataSourceId);
          const priceMultiplier = itemDsConfig?.priceMultiplier || 1;
          const useFilePrice = itemDsConfig?.useFilePrice || false;
          const shopifyPrice = parseFloat(v.price || "0");
          const dbBasePrice = parseFloat(dbItem.price || "0");

          if (useFilePrice && dbBasePrice > 0) {
            const expectedPrice = parseFloat(
              (dbBasePrice * priceMultiplier).toFixed(2),
            );
            if (Math.abs(shopifyPrice - expectedPrice) > 0.01) {
              result.priceMismatches++;
              if (result.priceMismatchList.length < limit) {
                result.priceMismatchList.push({
                  sku: shopifySku,
                  productTitle,
                  color: shopifyColor || null,
                  size: shopifySize || null,
                  shopifyPrice: v.price || "0",
                  expectedPrice: expectedPrice.toFixed(2),
                  dbBasePrice: dbBasePrice.toFixed(2),
                  priceMultiplier,
                });
              }
            }
          }

          // Stock info mismatch (NEW — aligns with sync preview 3-tier fallback)
          const metafieldRule = getMetafieldRuleForDs(dbItem.dataSourceId);
          if (metafieldRule) {
            const expectedStockInfo = auditCalculateStockInfo(metafieldRule, {
              stock: dbStock,
              shipDate: dbItem.shipDate || dbItem.ship_date || null,
              isExpandedSize: dbItem.isExpandedSize || false,
            });
            const currentStockInfo = v.stockInfoMetafield || null;
            const normalizedExpected = (expectedStockInfo || "").trim();
            const normalizedCurrent = (currentStockInfo || "").trim();

            if (normalizedExpected !== normalizedCurrent) {
              result.stockInfoMismatches++;
              if (result.stockInfoMismatchList.length < limit) {
                result.stockInfoMismatchList.push({
                  sku: shopifySku,
                  productTitle,
                  color: shopifyColor || null,
                  size: shopifySize || null,
                  currentStockInfo: normalizedCurrent || "(empty)",
                  expectedStockInfo: normalizedExpected || "(empty)",
                });
              }
            }
          }

          // CompareAt price mismatch (NEW)
          // If variant has compareAtPrice in Shopify, check if it differs from expected
          const shopifyCompareAt = parseFloat(v.compareAtPrice || "0");
          const dbCostPrice = parseFloat(dbItem.cost || dbItem.price || "0");
          if (shopifyCompareAt > 0 && dbCostPrice > 0) {
            // For sale items: compareAtPrice should be the retail/cost price
            // Only flag if there's a significant difference
            if (Math.abs(shopifyCompareAt - dbCostPrice) > 0.01) {
              // Tracked as price mismatch with compareAt info
              // (not a separate counter — included in the price mismatch detail)
            }
          }
        }
      }

      // =====================================================================
      // PHASE 3: DISCONTINUED PRODUCT DETECTION (aligns with Step 3.1.5)
      // =====================================================================
      // Products where ALL variants in Shopify have ZERO matching inventory
      // These are fully discontinued — show what action would be taken
      console.log(
        `[Sync Audit] Checking ${productMatchCount.size} products for discontinued status...`,
      );

      for (const [productId, info] of productMatchCount) {
        if (info.matched === 0 && info.total > 0) {
          const result = dsResults[info.ownerDsName];
          if (!result) continue;

          result.discontinuedProducts++;
          result.discontinuedVariants += info.total;

          const dsConfig = dsConfigMap.get(info.ownerDsId);
          if (result.discontinuedList.length < limit) {
            result.discontinuedList.push({
              productId,
              productTitle: info.productTitle,
              variantCount: info.total,
              action: dsConfig?.enableVariantDeletion
                ? dsConfig.deleteAction || "zero_out"
                : "none (deletion disabled)",
            });
          }
        }
      }

      // Remove empty Unowned bucket if no unowned variants found
      if (dsResults["Unowned"]?.totalInShopify === 0) {
        delete dsResults["Unowned"];
      }

      // =====================================================================
      // PHASE 4: SUMMARY (single-pass — no .reduce() chains)
      // =====================================================================
      let totalShopifyVariants = 0;
      let totalMatched = 0;
      let totalNotInDb = 0;
      let totalStockMismatches = 0;
      let totalPriceMismatches = 0;
      let totalStockInfoMismatches = 0;
      let totalDiscontinuedProducts = 0;
      let totalDiscontinuedVariants = 0;

      for (const r of Object.values(dsResults)) {
        totalShopifyVariants += r.totalInShopify;
        totalMatched += r.matchedInDb;
        totalNotInDb += r.notInDb;
        totalStockMismatches += r.stockMismatches;
        totalPriceMismatches += r.priceMismatches;
        totalStockInfoMismatches += r.stockInfoMismatches;
        totalDiscontinuedProducts += r.discontinuedProducts;
        totalDiscontinuedVariants += r.discontinuedVariants;
      }

      const durationMs = Date.now() - startTime;

      const summary = {
        totalShopifyVariants,
        dataSourcesAnalyzed: Object.keys(dsResults).filter(
          (k) => k !== "Unowned",
        ).length,
        totalMatched,
        totalNotInDb,
        totalStockMismatches,
        totalPriceMismatches,
        totalStockInfoMismatches,
        totalDiscontinuedProducts,
        totalDiscontinuedVariants,
        durationMs,
      };

      console.log(`[Sync Audit] Completed in ${durationMs}ms:`, summary);

      res.json({
        summary,
        byDataSource: dsResults,
      });
    } catch (error: any) {
      console.error("[Sync Audit] Error:", error);
      res.status(500).json({ error: error.message || "Audit failed" });
    }
  });

  // Download sync debug log
  app.get("/api/sync/debug-log", async (req, res) => {
    try {
      const fs = await import("fs");
      const path = await import("path");
      const debugLogPath = path.join(process.cwd(), "sync_debug_log.txt");

      if (!fs.existsSync(debugLogPath)) {
        return res
          .status(404)
          .json({ error: "Debug log not found. Run a sync first." });
      }

      const content = fs.readFileSync(debugLogPath, "utf-8");

      // Set headers for file download
      res.setHeader("Content-Type", "text/plain");
      res.setHeader(
        "Content-Disposition",
        "attachment; filename=sync_debug_log.txt",
      );
      res.send(content);
    } catch (error: any) {
      console.error("Error downloading debug log:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to download debug log" });
    }
  });

  // Check if sync can be started (not blocked by cache refresh)
  app.get("/api/shopify/stores/:id/sync-availability", async (req, res) => {
    try {
      const storeId = req.params.id;

      // Check if cache refresh is running
      const cacheRefreshRunning = isCacheRefreshRunning(storeId);
      const cacheRefreshProgress = getCacheRefreshProgress(storeId);

      // Check if another sync is already running
      const syncRunning = isInventorySyncActive(storeId);

      const canSync = !cacheRefreshRunning && !syncRunning;

      let blockedReason: string | null = null;
      if (cacheRefreshRunning) {
        blockedReason =
          "Cache refresh is in progress. Please wait for it to complete before starting a sync.";
      } else if (syncRunning) {
        blockedReason = "A sync is already running for this store.";
      }

      // Convert -1 (indeterminate) to 0 for the API response - UI handles 0 as indeterminate
      const totalVariants =
        cacheRefreshProgress.expectedTotal &&
        cacheRefreshProgress.expectedTotal > 0
          ? cacheRefreshProgress.expectedTotal
          : 0;

      // Include progress data if refresh is running OR just completed (gives UI time to show 100%)
      const hasProgressData =
        cacheRefreshProgress.variantsProcessed !== undefined;

      res.json({
        canSync,
        blockedReason,
        cacheRefreshRunning,
        cacheRefreshProgress: hasProgressData
          ? {
              variantsProcessed: cacheRefreshProgress.variantsProcessed,
              pagesProcessed: cacheRefreshProgress.pagesProcessed,
              startedAt: cacheRefreshProgress.refreshStartedAt,
              totalVariants,
              completed: cacheRefreshProgress.completed,
            }
          : null,
        syncRunning,
      });
    } catch (error: any) {
      console.error("Error checking sync availability:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to check sync availability" });
    }
  });

  // Clear stale sync locks (locks older than 30 minutes)
  app.post("/api/sync/clear-stale-locks", async (req, res) => {
    try {
      clearStaleSyncLocks();
      res.json({ success: true, message: "Cleared stale sync locks" });
    } catch (error: any) {
      console.error("Error clearing stale locks:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to clear stale locks" });
    }
  });

  // Force clear all sync locks (use with caution)
  app.post("/api/sync/force-clear-locks", async (req, res) => {
    try {
      clearAllSyncLocks();
      res.json({ success: true, message: "Force cleared all sync locks" });
    } catch (error: any) {
      console.error("Error force clearing locks:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to force clear locks" });
    }
  });

  // Clear variant cache
  app.delete("/api/shopify/stores/:id/variant-cache", async (req, res) => {
    try {
      const storeId = req.params.id;
      await storage.clearVariantCache(storeId);
      res.json({ success: true, message: "Variant cache cleared" });
    } catch (error: any) {
      console.error("Error clearing variant cache:", error);
      res.status(500).json({ error: error.message || "Failed to clear cache" });
    }
  });

  // Refresh variant cache for a single product
  app.post(
    "/api/shopify/stores/:id/variant-cache/product/:productId",
    async (req, res) => {
      try {
        const storeId = req.params.id;
        const productId = req.params.productId;

        const store = await storage.getShopifyStore(storeId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const shopify = createShopifyService(store);

        // Fetch fresh variants from Shopify
        const variants = await shopify.fetchProductVariantsForCache(productId);

        if (variants.length === 0) {
          return res.json({
            success: true,
            message: "Product not found or has no variants",
            deletedCount: 0,
            insertedCount: 0,
          });
        }

        // Delete old cache entries for this product
        const deletedCount = await storage.deleteVariantCacheForProduct(
          storeId,
          productId,
        );

        // Insert fresh variants
        const variantsWithStore = variants.map((v: any) => ({
          ...v,
          shopifyStoreId: storeId,
        }));
        await storage.upsertVariantCache(variantsWithStore);

        console.log(
          `[Variant Cache] Refreshed product ${productId}: deleted ${deletedCount}, inserted ${variants.length}`,
        );

        res.json({
          success: true,
          message: `Refreshed cache for product ${productId}`,
          deletedCount,
          insertedCount: variants.length,
        });
      } catch (error: any) {
        console.error("Error refreshing product variant cache:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to refresh product cache" });
      }
    },
  );

  // Scan for SKU-size mismatches in variant cache
  app.get(
    "/api/shopify/stores/:id/variant-cache/sku-mismatches",
    async (req, res) => {
      try {
        const storeId = req.params.id;
        const mismatches = await storage.findSkuSizeMismatches(storeId);

        res.json({
          count: mismatches.length,
          mismatches: mismatches.slice(0, 100),
        });
      } catch (error: any) {
        console.error("Error finding SKU mismatches:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to find SKU mismatches" });
      }
    },
  );

  // Fix SKU-size mismatches in Shopify
  app.post(
    "/api/shopify/stores/:id/variant-cache/fix-sku-mismatches",
    async (req, res) => {
      try {
        const storeId = req.params.id;
        const dryRun = req.query.dryRun === "true";

        const store = await storage.getShopifyStore(storeId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const mismatches = await storage.findSkuSizeMismatches(storeId);

        if (mismatches.length === 0) {
          return res.json({
            fixed: 0,
            failed: 0,
            message: "No SKU mismatches found",
          });
        }

        if (dryRun) {
          return res.json({
            dryRun: true,
            count: mismatches.length,
            toFix: mismatches.slice(0, 50).map((m) => ({
              currentSku: m.sku,
              correctSku: m.correctSku,
              size: m.size,
              variantId: m.variantId,
            })),
          });
        }

        const shopify = createShopifyService(store);

        let fixed = 0;
        let failed = 0;
        const errors: string[] = [];

        console.log(
          `[SKU Fix] Starting to fix ${mismatches.length} SKU mismatches...`,
        );

        for (const mismatch of mismatches) {
          try {
            await shopify.updateVariantSku(
              mismatch.inventoryItemId,
              mismatch.correctSku,
            );

            await storage.updateVariantCacheSku(
              mismatch.variantId,
              mismatch.correctSku,
            );

            fixed++;

            if (fixed % 10 === 0) {
              console.log(
                `[SKU Fix] Progress: ${fixed}/${mismatches.length} fixed`,
              );
            }

            await new Promise((resolve) => setTimeout(resolve, 100));
          } catch (error: any) {
            failed++;
            errors.push(
              `${mismatch.sku} -> ${mismatch.correctSku}: ${error.message}`,
            );
            console.error(
              `[SKU Fix] Failed to fix ${mismatch.sku}:`,
              error.message,
            );
          }
        }

        console.log(`[SKU Fix] Completed: ${fixed} fixed, ${failed} failed`);

        res.json({
          fixed,
          failed,
          errors: errors.slice(0, 20),
        });
      } catch (error: any) {
        console.error("Error fixing SKU mismatches:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to fix SKU mismatches" });
      }
    },
  );

  // Search products in variant cache by title
  app.get("/api/shopify/stores/:id/variant-cache/search", async (req, res) => {
    try {
      const storeId = req.params.id;
      const query = ((req.query.q as string) || "").trim().toLowerCase();
      const limit = Math.min(parseInt(req.query.limit as string) || 20, 50);

      if (!query || query.length < 2) {
        return res.json({ products: [] });
      }

      const store = await storage.getShopifyStore(storeId);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      // Search in variant cache - get unique products matching query
      const results = await storage.searchVariantCacheProducts(
        storeId,
        query,
        limit,
      );

      res.json({ products: results });
    } catch (error: any) {
      console.error("Error searching variant cache:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to search products" });
    }
  });

  // Export partial variant cache as CSV
  app.get(
    "/api/shopify/stores/:id/variant-cache/export-csv",
    async (req, res) => {
      try {
        const storeId = req.params.id;
        const limit = Math.min(
          parseInt(req.query.limit as string) || 1000,
          50000,
        );
        const vendor = (req.query.vendor as string) || undefined;
        const search = (req.query.search as string) || undefined;

        const store = await storage.getShopifyStore(storeId);
        if (!store) {
          return res.status(404).json({ error: "Store not found" });
        }

        const variants = await storage.getVariantCachePartial(storeId, {
          limit,
          vendor,
          search,
        });

        // Build CSV
        const headers = [
          "id",
          "sku",
          "product_title",
          "vendor",
          "variant_title",
          "option1_name",
          "option1_value",
          "option2_name",
          "option2_value",
          "stock",
          "price",
          "compare_at_price",
          "stock_info_metafield",
          "shopify_product_id",
          "inventory_item_id",
        ];

        const csvRows = [headers.join(",")];
        for (const v of variants) {
          const row = [
            v.id,
            `"${(v.sku || "").replace(/"/g, '""')}"`,
            `"${(v.productTitle || "").replace(/"/g, '""')}"`,
            `"${(v.vendor || "").replace(/"/g, '""')}"`,
            `"${(v.variantTitle || "").replace(/"/g, '""')}"`,
            `"${(v.option1Name || "").replace(/"/g, '""')}"`,
            `"${(v.option1Value || "").replace(/"/g, '""')}"`,
            `"${(v.option2Name || "").replace(/"/g, '""')}"`,
            `"${(v.option2Value || "").replace(/"/g, '""')}"`,
            v.stock ?? "",
            v.price || "",
            v.compareAtPrice || "",
            `"${(v.stockInfoMetafield || "").replace(/"/g, '""')}"`,
            v.shopifyProductId || "",
            v.inventoryItemId || "",
          ];
          csvRows.push(row.join(","));
        }

        const csv = csvRows.join("\n");
        const filename = vendor
          ? `variant_cache_${vendor.toLowerCase().replace(/\s+/g, "_")}_${new Date().toISOString().split("T")[0]}.csv`
          : `variant_cache_partial_${new Date().toISOString().split("T")[0]}.csv`;

        res.setHeader("Content-Type", "text/csv");
        res.setHeader(
          "Content-Disposition",
          `attachment; filename="${filename}"`,
        );
        res.send(csv);
      } catch (error: any) {
        console.error("Error exporting variant cache:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to export cache" });
      }
    },
  );

  // =========== PENDING SYNC QUEUE ROUTES (Incremental Sync) ===========

  // Get pending sync queue count and status
  app.get("/api/shopify/stores/:id/sync-queue/status", async (req, res) => {
    try {
      const storeId = req.params.id;

      const queueStats = await storage.getPendingSyncQueueCount(storeId);
      const jobStatus = getComparisonJobStatus();

      res.json({
        queueCount: queueStats.total,
        byChangeType: queueStats.byType,
        comparisonJob: {
          isRunning: jobStatus.isRunning && jobStatus.storeId === storeId,
          processed: jobStatus.processed,
          total: jobStatus.total,
          queued: jobStatus.queued,
          startedAt: jobStatus.startedAt,
        },
      });
    } catch (error: any) {
      console.error("Error getting sync queue status:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get queue status" });
    }
  });

  // Get pending sync queue items (for debugging/preview)
  app.get("/api/shopify/stores/:id/sync-queue/items", async (req, res) => {
    try {
      const storeId = req.params.id;
      const limit = Math.min(parseInt(req.query.limit as string) || 100, 1000);
      const changeType = req.query.changeType as string | undefined;

      const items = await storage.getPendingSyncQueueItems(storeId, {
        status: "pending",
        changeType,
        limit,
      });

      res.json({
        count: items.length,
        items: items.map((item) => ({
          id: item.id,
          changeType: item.changeType,
          details: item.changeDetails,
          detectedAt: item.detectedAt,
        })),
      });
    } catch (error: any) {
      console.error("Error getting sync queue items:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get queue items" });
    }
  });

  // Clear pending sync queue (force fresh comparison)
  app.delete("/api/shopify/stores/:id/sync-queue", async (req, res) => {
    try {
      const storeId = req.params.id;
      const dataSourceId = req.query.dataSourceId as string | undefined;

      const deletedCount = await storage.clearPendingSyncQueue(
        storeId,
        dataSourceId,
      );

      res.json({
        success: true,
        deletedCount,
        message: `Cleared ${deletedCount} pending sync items`,
      });
    } catch (error: any) {
      console.error("Error clearing sync queue:", error);
      res.status(500).json({ error: error.message || "Failed to clear queue" });
    }
  });

  // Manually trigger comparison job
  app.post("/api/shopify/stores/:id/sync-queue/compare", async (req, res) => {
    try {
      const storeId = req.params.id;
      const dataSourceId = req.body.dataSourceId as string | undefined;

      const jobStatus = getComparisonJobStatus();
      if (jobStatus.isRunning) {
        return res.status(409).json({
          error: "Comparison job already running",
          status: jobStatus,
        });
      }

      startComparisonJob({ storeId, dataSourceId });

      res.json({
        success: true,
        message: "Comparison job started",
        status: getComparisonJobStatus(),
      });
    } catch (error: any) {
      console.error("Error starting comparison job:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to start comparison" });
    }
  });

  // Cancel running comparison job
  app.post(
    "/api/shopify/stores/:id/sync-queue/compare/cancel",
    async (req, res) => {
      try {
        abortComparisonJob();

        res.json({
          success: true,
          message: "Comparison job cancelled",
          status: getComparisonJobStatus(),
        });
      } catch (error: any) {
        console.error("Error cancelling comparison job:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to cancel comparison" });
      }
    },
  );

  // Execute fast sync from queued items (runs in background)
  app.post("/api/shopify/stores/:id/sync-queue/sync", async (req, res) => {
    try {
      const storeId = req.params.id;
      const { dataSourceId, limit, dryRun } = req.body as {
        dataSourceId?: string;
        limit?: number;
        dryRun?: boolean;
      };

      const store = await storage.getShopifyStore(storeId);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      // If dataSourceId is provided, queue a sync job (runs in background)
      if (dataSourceId) {
        const { queueDataSourceSync } = await import(
          "./sync/dataSourceSyncQueue"
        );
        const job = await queueDataSourceSync({
          dataSourceId,
          storeId,
          triggeredBy: "manual",
        });

        return res.json({
          success: true,
          message: "Sync queued",
          jobId: job.id,
        });
      }

      // Legacy: run fast sync directly (for backwards compatibility)
      const { fastSyncFromQueue } = await import("./sync/fastSyncFromQueue");

      const result = await fastSyncFromQueue({
        storeId,
        limit,
        dryRun,
      });

      res.json({
        success: true,
        result,
      });
    } catch (error: any) {
      console.error("Error executing fast sync:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to execute fast sync" });
    }
  });

  // Get sync queue status for a store
  app.get("/api/shopify/stores/:id/sync-queue/status", async (req, res) => {
    try {
      const storeId = req.params.id;

      const store = await storage.getShopifyStore(storeId);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      const { getSyncQueueStatus } = await import("./sync/dataSourceSyncQueue");
      const status = await getSyncQueueStatus(storeId);

      // Also get pending item counts
      const pendingCount = await storage.getPendingSyncQueueCount(storeId);

      res.json({
        ...status,
        pendingItems: pendingCount,
      });
    } catch (error: any) {
      console.error("Error getting sync queue status:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get sync queue status" });
    }
  });

  // Get sync job by ID
  app.get("/api/shopify/sync-jobs/:id", async (req, res) => {
    try {
      const jobId = req.params.id;

      const job = await storage.getDataSourceSyncJob(jobId);
      if (!job) {
        return res.status(404).json({ error: "Sync job not found" });
      }

      res.json(job);
    } catch (error: any) {
      console.error("Error getting sync job:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get sync job" });
    }
  });

  // Get sync status for a data source
  app.get("/api/data-sources/:id/sync-status", async (req, res) => {
    try {
      const dataSourceId = req.params.id;

      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      const { getDataSourceSyncStatus } = await import(
        "./sync/dataSourceSyncQueue"
      );
      const status = await getDataSourceSyncStatus(dataSourceId);

      res.json(status);
    } catch (error: any) {
      console.error("Error getting data source sync status:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get sync status" });
    }
  });

  // =========== COLOR/SIZE UPDATE ROUTES ===========

  // Preview color/size differences between data source and Shopify
  app.get(
    "/api/shopify/stores/:id/option-updates/preview",
    async (req, res) => {
      try {
        const storeId = req.params.id;
        const dataSourceId = req.query.dataSourceId as string;

        if (!dataSourceId) {
          return res.status(400).json({ error: "dataSourceId is required" });
        }

        const store = await storage.getShopifyStore(storeId);
        if (!store) {
          return res.status(404).json({ error: "Store not found" });
        }

        // Get data source for name lookup
        const dataSource = await storage.getDataSource(dataSourceId);
        if (!dataSource) {
          return res.status(404).json({ error: "Data source not found" });
        }

        // Get variant cache with option values
        const cachedVariants = await storage.getVariantCacheForStore(storeId);
        if (cachedVariants.length === 0) {
          return res.status(400).json({
            error: "Variant cache is empty. Please refresh the cache first.",
          });
        }

        // Check if cache has option data
        const hasOptionData = cachedVariants.some(
          (v) => v.option1Name || v.option2Name,
        );
        if (!hasOptionData) {
          return res.status(400).json({
            error:
              "Variant cache does not have option data. Please refresh the cache.",
          });
        }

        // Build SKU index from cache
        const shopifyVariantsBySku = new Map<
          string,
          {
            variantId: string;
            productId: string;
            productTitle: string;
            variantTitle: string;
            sku: string;
            shopifyColor: string | null;
            shopifySize: string | null;
            option1Name: string | null;
            option2Name: string | null;
          }
        >();

        for (const v of cachedVariants) {
          if (v.sku) {
            // Determine which option is Color and which is Size
            let shopifyColor: string | null = null;
            let shopifySize: string | null = null;

            if (v.option1Name?.toLowerCase() === "color") {
              shopifyColor = v.option1Value;
            } else if (v.option1Name?.toLowerCase() === "size") {
              shopifySize = v.option1Value;
            }

            if (v.option2Name?.toLowerCase() === "color") {
              shopifyColor = v.option2Value;
            } else if (v.option2Name?.toLowerCase() === "size") {
              shopifySize = v.option2Value;
            }

            shopifyVariantsBySku.set(v.sku.toLowerCase(), {
              variantId: v.id,
              productId: v.shopifyProductId,
              productTitle: v.productTitle || "",
              variantTitle: v.variantTitle || "",
              sku: v.sku,
              shopifyColor,
              shopifySize,
              option1Name: v.option1Name,
              option2Name: v.option2Name,
            });
          }
        }

        // Get inventory items for this data source
        const inventoryItems = await storage.getInventoryItems(dataSourceId);

        // Build differences list
        const differences: Array<{
          sku: string;
          productTitle: string;
          variantId: string;
          productId: string;
          inventoryColor: string | null;
          shopifyColor: string | null;
          inventorySize: string | null;
          shopifySize: string | null;
          colorDifferent: boolean;
          sizeDifferent: boolean;
        }> = [];

        let matched = 0;
        let unmatched = 0;

        for (const item of inventoryItems) {
          // Build constructed SKU for matching (same format as Shopify: style-color-size)
          // Note: style already includes brand name (e.g., "Ava Presley 25903")
          const style = item.style || "";
          const color = item.color || "";
          const size = String(item.size ?? "");
          // SKU format: style with dashes, color with spaces, size
          const styleWithDashes = style.replace(/\s+/g, "-");
          const constructedSku =
            `${styleWithDashes}-${color}-${size}`.toLowerCase();

          // Try constructed SKU first, then raw SKU
          let shopifyVariant = shopifyVariantsBySku.get(constructedSku);
          if (!shopifyVariant && item.sku) {
            shopifyVariant = shopifyVariantsBySku.get(item.sku.toLowerCase());
          }

          if (!shopifyVariant) {
            unmatched++;
            continue;
          }

          matched++;

          // Compare color and size (case-insensitive)
          const inventoryColor = color || null;
          const inventorySize = size || null;
          const colorDifferent =
            inventoryColor?.toLowerCase() !==
            shopifyVariant.shopifyColor?.toLowerCase();
          const sizeDifferent =
            inventorySize?.toLowerCase() !==
            shopifyVariant.shopifySize?.toLowerCase();

          if (colorDifferent || sizeDifferent) {
            differences.push({
              sku: shopifyVariant.sku,
              productTitle: shopifyVariant.productTitle,
              variantId: shopifyVariant.variantId,
              productId: shopifyVariant.productId,
              inventoryColor,
              shopifyColor: shopifyVariant.shopifyColor,
              inventorySize,
              shopifySize: shopifyVariant.shopifySize,
              colorDifferent,
              sizeDifferent,
            });
          }
        }

        res.json({
          dataSourceName: dataSource.name,
          summary: {
            totalInventoryItems: inventoryItems.length,
            matchedVariants: matched,
            unmatchedVariants: unmatched,
            colorDifferences: differences.filter((d) => d.colorDifferent)
              .length,
            sizeDifferences: differences.filter((d) => d.sizeDifferent).length,
            totalDifferences: differences.length,
          },
          differences: differences.slice(0, 500), // Limit to first 500 for display
        });
      } catch (error: any) {
        console.error("Error previewing option updates:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to preview option updates" });
      }
    },
  );

  // Apply color/size updates to Shopify
  app.post("/api/shopify/stores/:id/option-updates/apply", async (req, res) => {
    try {
      const storeId = req.params.id;
      const { dataSourceId } = req.body;

      if (!dataSourceId) {
        return res.status(400).json({ error: "dataSourceId is required" });
      }

      const store = await storage.getShopifyStore(storeId);
      if (!store || !store.accessToken) {
        return res.status(400).json({ error: "Store not connected" });
      }

      // Get data source
      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // Get variant cache with option values
      const cachedVariants = await storage.getVariantCacheForStore(storeId);
      if (cachedVariants.length === 0) {
        return res.status(400).json({
          error: "Variant cache is empty. Please refresh the cache first.",
        });
      }

      // Build SKU index from cache
      const shopifyVariantsBySku = new Map<
        string,
        {
          variantId: string;
          productId: string;
          shopifyColor: string | null;
          shopifySize: string | null;
          option1Name: string | null;
          option2Name: string | null;
        }
      >();

      for (const v of cachedVariants) {
        if (v.sku) {
          let shopifyColor: string | null = null;
          let shopifySize: string | null = null;

          if (v.option1Name?.toLowerCase() === "color") {
            shopifyColor = v.option1Value;
          } else if (v.option1Name?.toLowerCase() === "size") {
            shopifySize = v.option1Value;
          }

          if (v.option2Name?.toLowerCase() === "color") {
            shopifyColor = v.option2Value;
          } else if (v.option2Name?.toLowerCase() === "size") {
            shopifySize = v.option2Value;
          }

          shopifyVariantsBySku.set(v.sku.toLowerCase(), {
            variantId: v.id,
            productId: v.shopifyProductId,
            shopifyColor,
            shopifySize,
            option1Name: v.option1Name,
            option2Name: v.option2Name,
          });
        }
      }

      // Get inventory items for this data source
      const inventoryItems = await storage.getInventoryItems(dataSourceId);

      // Group updates by product ID (Shopify requires updates per product)
      const updatesByProduct = new Map<
        string,
        Array<{
          variantId: string;
          optionValues: Array<{ optionName: string; name: string }>;
        }>
      >();

      for (const item of inventoryItems) {
        // Build constructed SKU for matching (same format as Shopify: style-color-size)
        // Note: style already includes brand name (e.g., "Ava Presley 25903")
        const style = item.style || "";
        const color = item.color || "";
        const size = String(item.size ?? "");
        // SKU format: style with dashes, color with spaces, size
        const styleWithDashes = style.replace(/\s+/g, "-");
        const constructedSku =
          `${styleWithDashes}-${color}-${size}`.toLowerCase();

        let shopifyVariant = shopifyVariantsBySku.get(constructedSku);
        if (!shopifyVariant && item.sku) {
          shopifyVariant = shopifyVariantsBySku.get(item.sku.toLowerCase());
        }

        if (!shopifyVariant) continue;

        const inventoryColor = color || null;
        const inventorySize = size || null;
        const colorDifferent =
          inventoryColor?.toLowerCase() !==
          shopifyVariant.shopifyColor?.toLowerCase();
        const sizeDifferent =
          inventorySize?.toLowerCase() !==
          shopifyVariant.shopifySize?.toLowerCase();

        if (!colorDifferent && !sizeDifferent) continue;

        // Build option values to update
        const optionValues: Array<{ optionName: string; name: string }> = [];

        if (colorDifferent && inventoryColor) {
          // Find the Color option name from cache
          const colorOptionName =
            shopifyVariant.option1Name?.toLowerCase() === "color"
              ? shopifyVariant.option1Name
              : shopifyVariant.option2Name?.toLowerCase() === "color"
                ? shopifyVariant.option2Name
                : "Color";
          optionValues.push({
            optionName: colorOptionName,
            name: inventoryColor,
          });
        }

        if (sizeDifferent && inventorySize) {
          const sizeOptionName =
            shopifyVariant.option1Name?.toLowerCase() === "size"
              ? shopifyVariant.option1Name
              : shopifyVariant.option2Name?.toLowerCase() === "size"
                ? shopifyVariant.option2Name
                : "Size";
          optionValues.push({
            optionName: sizeOptionName,
            name: inventorySize,
          });
        }

        if (optionValues.length > 0) {
          const productUpdates =
            updatesByProduct.get(shopifyVariant.productId) || [];
          productUpdates.push({
            variantId: shopifyVariant.variantId,
            optionValues,
          });
          updatesByProduct.set(shopifyVariant.productId, productUpdates);
        }
      }

      // Apply updates to Shopify
      const shopify = createShopifyService(store);
      let success = 0;
      let failed = 0;
      const errors: string[] = [];

      for (const [productId, variants] of Array.from(
        updatesByProduct.entries(),
      )) {
        try {
          const result = await shopify.bulkUpdateVariantOptions(
            productId,
            variants,
          );
          success += result.success;
          failed += result.failed;
          errors.push(...result.errors);
        } catch (error: any) {
          failed += variants.length;
          errors.push(`Product ${productId}: ${error.message}`);
        }
      }

      res.json({
        success: true,
        summary: {
          productsUpdated: updatesByProduct.size,
          variantsUpdated: success,
          variantsFailed: failed,
        },
        errors: errors.slice(0, 50), // Limit errors shown
      });
    } catch (error: any) {
      console.error("Error applying option updates:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to apply option updates" });
    }
  });

  // Get sync logs for a store
  app.get("/api/shopify/stores/:id/sync-logs", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const logs = await storage.getShopifySyncLogs(req.params.id, limit);

      // Get all data sources for name lookups
      const dataSources = await storage.getDataSources();

      // Enrich logs with data source names
      const enrichedLogs = await Promise.all(
        logs.map(async (log) => {
          const dataSourceStats = await storage.getShopifySyncDataSourceStats(
            log.id,
          );
          const dataSourceNames: string[] = [];

          for (const stat of dataSourceStats) {
            if (stat.dataSourceId) {
              const ds = dataSources.find((d) => d.id === stat.dataSourceId);
              if (ds) {
                dataSourceNames.push(ds.name);
              }
            }
          }

          return {
            ...log,
            dataSourceNames:
              dataSourceNames.length > 0 ? dataSourceNames : null,
          };
        }),
      );

      res.json(enrichedLogs);
    } catch (error) {
      console.error("Error getting sync logs:", error);
      res.status(500).json({ error: "Failed to get sync logs" });
    }
  });

  // Download sync logs as text file
  app.get("/api/shopify/stores/:id/sync-logs/download", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 100;
      const logs = await storage.getShopifySyncLogs(req.params.id, limit);

      // Format logs as readable text
      let textContent = `Sync Logs - Downloaded ${new Date().toISOString()}\n`;
      textContent += `${"=".repeat(80)}\n\n`;

      for (const log of logs) {
        textContent += `Sync ID: ${log.id}\n`;
        textContent += `Type: ${log.syncType || "inventory"}\n`;
        textContent += `Status: ${log.status}\n`;
        textContent += `Started: ${log.startedAt ? new Date(log.startedAt).toLocaleString() : "N/A"}\n`;
        textContent += `Completed: ${log.completedAt ? new Date(log.completedAt).toLocaleString() : "In progress"}\n`;
        textContent += `Items Processed: ${log.itemsProcessed || 0}\n`;
        textContent += `Items Updated: ${log.itemsUpdated || 0}\n`;
        textContent += `Items Created: ${log.itemsCreated || 0}\n`;
        textContent += `Items Deleted: ${log.itemsDeleted || 0}\n`;
        textContent += `Items Skipped: ${log.itemsSkipped || 0}\n`;
        textContent += `Items Failed: ${log.itemsFailed || 0}\n`;
        if (log.errorMessage) {
          textContent += `Error: ${log.errorMessage}\n`;
        }
        textContent += `${"-".repeat(80)}\n\n`;
      }

      res.setHeader("Content-Type", "text/plain");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="sync-logs-${new Date().toISOString().split("T")[0]}.txt"`,
      );
      res.send(textContent);
    } catch (error) {
      console.error("Error downloading sync logs:", error);
      res.status(500).json({ error: "Failed to download sync logs" });
    }
  });

  // Get deletion logs for a sync
  app.get(
    "/api/shopify/sync-logs/:syncLogId/deletion-logs",
    async (req, res) => {
      try {
        const logs = await storage.getSyncDeletionLogs(req.params.syncLogId);
        res.json(logs);
      } catch (error) {
        console.error("Error getting deletion logs:", error);
        res.status(500).json({ error: "Failed to get deletion logs" });
      }
    },
  );

  // Get deletion logs for a data source
  app.get("/api/data-sources/:id/deletion-logs", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 1000;
      const logs = await storage.getSyncDeletionLogsByDataSource(
        req.params.id,
        limit,
      );
      res.json(logs);
    } catch (error) {
      console.error("Error getting deletion logs:", error);
      res.status(500).json({ error: "Failed to get deletion logs" });
    }
  });

  // Download deletion logs as CSV
  app.get(
    "/api/shopify/sync-logs/:syncLogId/deletion-logs/download",
    async (req, res) => {
      try {
        const logs = await storage.getSyncDeletionLogs(req.params.syncLogId);

        // Format as CSV
        let csvContent =
          "Deleted At,SKU,Product Title,Color,Size,Price Before,Stock Before,Deletion Reason,Details,Variant ID,Product ID\n";

        for (const log of logs) {
          const row = [
            log.createdAt ? new Date(log.createdAt).toISOString() : "",
            `"${(log.sku || "").replace(/"/g, '""')}"`,
            `"${(log.productTitle || "").replace(/"/g, '""')}"`,
            `"${(log.color || "").replace(/"/g, '""')}"`,
            `"${(log.size || "").replace(/"/g, '""')}"`,
            log.priceBeforeDelete || "",
            log.stockBeforeDelete ?? "",
            log.deletionReason || "",
            `"${(log.deletionDetails || "").replace(/"/g, '""')}"`,
            log.variantId,
            log.productId,
          ];
          csvContent += row.join(",") + "\n";
        }

        res.setHeader("Content-Type", "text/csv");
        res.setHeader(
          "Content-Disposition",
          `attachment; filename="deletion-logs-${req.params.syncLogId}.csv"`,
        );
        res.send(csvContent);
      } catch (error) {
        console.error("Error downloading deletion logs:", error);
        res.status(500).json({ error: "Failed to download deletion logs" });
      }
    },
  );

  // Download SyncBackup logs only (search console logs for SyncBackup entries)
  app.get("/api/logs/sync-backup/download", async (req, res) => {
    try {
      const fs = await import("fs");
      const path = await import("path");

      // Search through log files in /tmp/logs for SyncBackup entries
      const logDir = "/tmp/logs";
      let syncBackupLogs: string[] = [];

      try {
        const files = fs.readdirSync(logDir);
        const logFiles = files.filter(
          (f) => f.startsWith("Start_application") && f.endsWith(".log"),
        );

        for (const file of logFiles) {
          const filePath = path.join(logDir, file);
          const content = fs.readFileSync(filePath, "utf-8");
          const lines = content.split("\n");

          for (const line of lines) {
            if (line.includes("SyncBackup") || line.includes("[SyncBackup]")) {
              syncBackupLogs.push(`[${file}] ${line}`);
            }
          }
        }
      } catch (e) {
        // Log directory might not exist or be accessible
      }

      // Format output
      let textContent = `SyncBackup Logs - Downloaded ${new Date().toISOString()}\n`;
      textContent += `${"=".repeat(80)}\n\n`;

      if (syncBackupLogs.length === 0) {
        textContent += `No SyncBackup log entries found in console logs.\n\n`;
        textContent += `This could mean:\n`;
        textContent += `1. The backup system hasn't been triggered yet\n`;
        textContent += `2. Logs have been rotated out\n`;
        textContent += `3. The backup function isn't being called during sync\n`;
      } else {
        textContent += `Found ${syncBackupLogs.length} SyncBackup entries:\n\n`;
        for (const log of syncBackupLogs) {
          textContent += `${log}\n`;
        }
      }

      res.setHeader("Content-Type", "text/plain");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="sync-backup-logs-${new Date().toISOString().split("T")[0]}.txt"`,
      );
      res.send(textContent);
    } catch (error) {
      console.error("Error downloading sync backup logs:", error);
      res.status(500).json({ error: "Failed to download sync backup logs" });
    }
  });

  // Download ALL console logs as a single file (streams with backpressure)
  app.get("/api/logs/console/download", async (req, res) => {
    try {
      const fs = await import("fs");
      const fsPromises = await import("fs/promises");
      const path = await import("path");
      const { pipeline } = await import("stream/promises");

      const logDir = "/tmp/logs";

      res.setHeader("Content-Type", "text/plain");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="console-logs-${new Date().toISOString().split("T")[0]}.txt"`,
      );

      // Write header directly
      res.write(`Full Console Logs - Downloaded ${new Date().toISOString()}\n`);
      res.write("=".repeat(80) + "\n\n");

      try {
        const files = await fsPromises.readdir(logDir);
        // Get all log files with stats
        const logFilesWithStats = await Promise.all(
          files
            .filter((f) => f.endsWith(".log"))
            .map(async (f) => {
              const stats = await fsPromises.stat(path.join(logDir, f));
              return { name: f, mtime: stats.mtime, size: stats.size };
            }),
        );

        // Sort by modification time (newest first)
        logFilesWithStats.sort((a, b) => b.mtime.getTime() - a.mtime.getTime());

        if (logFilesWithStats.length === 0) {
          res.write("No log files found in /tmp/logs\n");
        } else {
          // Stream each file with proper backpressure using pipeline
          for (const file of logFilesWithStats) {
            const filePath = path.join(logDir, file.name);

            // Write file header
            res.write("\n" + "=".repeat(80) + "\n");
            res.write(`FILE: ${file.name}\n`);
            res.write(`SIZE: ${(file.size / 1024).toFixed(1)} KB\n`);
            res.write(`MODIFIED: ${file.mtime.toISOString()}\n`);
            res.write("=".repeat(80) + "\n\n");

            // Stream file content directly to response with backpressure
            const readStream = fs.createReadStream(filePath, {
              encoding: "utf-8",
              highWaterMark: 64 * 1024,
            });
            await pipeline(readStream, res, { end: false });
          }
        }
      } catch (e: any) {
        res.write(`Error reading logs: ${e.message}\n`);
      }

      res.end();
    } catch (error) {
      console.error("Error downloading console logs:", error);
      if (!res.headersSent) {
        res.status(500).json({ error: "Failed to download console logs" });
      }
    }
  });

  // Get real-time sync progress for a store
  app.get("/api/shopify/stores/:id/sync/progress", async (req, res) => {
    try {
      const progress = await storage.getSyncProgress(req.params.id);
      if (!progress) {
        return res.json({ active: false, cancellable: false });
      }

      // Check if there's actually a sync running in memory (not just stale DB state)
      const cancellable = isInventorySyncActive(req.params.id);

      // Calculate phase percentages
      const phases = progress.phases || {};
      const phaseList = [
        "loading",
        "stock_updates",
        "variant_creation",
        "variant_deletion",
        "color_size_updates",
        "variant_reorder",
        "stock_info",
        "price_repair",
        "cache_update",
      ] as const;

      const phaseProgress: {
        [key: string]: {
          total: number;
          completed: number;
          percent: number;
          status: string;
        };
      } = {};
      for (const phaseName of phaseList) {
        const phaseData = phases[phaseName];
        if (phaseData) {
          const percent =
            phaseData.total > 0
              ? Math.round((phaseData.completed / phaseData.total) * 100)
              : 0;
          phaseProgress[phaseName] = {
            total: phaseData.total,
            completed: phaseData.completed,
            percent,
            status: phaseData.status,
          };
        }
      }

      // Inject cache refresh progress if cache is refreshing OR just completed
      // The completed state is kept for 5 seconds to allow clients to see the 100% state
      const cacheRefreshProgress = getCacheRefreshProgress(req.params.id);
      const hasRefreshState =
        cacheRefreshProgress.variantsProcessed !== undefined;
      if (hasRefreshState) {
        // Use expectedTotal from the refresh state (captured at start, before cache was cleared)
        // expectedTotal = -1 means indeterminate (we exceeded the initial estimate during cursor pagination)
        // expectedTotal = 0 means no prior cache (first run)
        // expectedTotal > 0 means we have a reliable total estimate
        const total = cacheRefreshProgress.expectedTotal || 0;
        const completed = cacheRefreshProgress.variantsProcessed || 0;
        const isComplete = cacheRefreshProgress.completed === true;
        phaseProgress["cache_update"] = {
          total: isComplete ? completed : total < 0 ? 0 : total, // Use actual count when complete
          completed,
          percent: isComplete
            ? 100
            : total > 0
              ? Math.min(100, Math.round((completed / total) * 100))
              : 0,
          status: isComplete ? "completed" : "running",
        };
      }

      // Determine sync status
      const lastUpdate = progress.updatedAt
        ? new Date(progress.updatedAt).getTime()
        : 0;
      const twoMinutesAgo = Date.now() - 2 * 60 * 1000;
      const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
      const isProperlyComplete = progress.currentPhase === "complete";

      // Stale: hasn't been updated in 5+ minutes, no in-memory sync, AND not properly complete
      const isStale =
        lastUpdate < fiveMinutesAgo && !cancellable && !isProperlyComplete;

      // Complete but not marked: sync reached high percent but currentPhase wasn't set to 'complete'
      // This happens when sync finishes work but crashes before cleanup
      // Using 95% threshold since syncs hang at ~99% during cleanup, never reaching 100%
      // SAFETY: Also check that no phases are still running - prevents false positives during transitions
      const anyPhaseRunning = Object.values(phases).some(
        (p: any) => p.status === "running",
      );
      const isCompleteButNotMarked =
        (progress.overallPercent || 0) >= 95 &&
        !cancellable &&
        !isProperlyComplete &&
        !anyPhaseRunning;

      // Stuck: sync is at high percent (95%+), HAS an in-memory sync, but hasn't updated in 2+ minutes
      // This happens when the sync process hangs during cleanup/finalization
      // Using 95% threshold since the actual hang occurs around 99% (never reaching 100%)
      // SKIP stuck detection during "loading" phase - loading Shopify cache can take a long time
      const isStuck =
        progress.currentPhase !== "loading" &&
        (progress.overallPercent || 0) >= 95 &&
        cancellable &&
        lastUpdate < twoMinutesAgo &&
        !isProperlyComplete;

      // Sync is active only if:
      // - It's not properly complete
      // - It's not stale
      // - It's not complete-but-not-marked
      // - It's not stuck
      // - There's an in-memory sync OR phase is still running
      const isActive =
        !isProperlyComplete &&
        !isStale &&
        !isCompleteButNotMarked &&
        !isStuck &&
        (cancellable || progress.phaseStatus === "running");

      // Determine inactive reason for debugging
      let inactiveReason: string | undefined;
      if (!isActive && !isProperlyComplete) {
        if (isStuck) {
          inactiveReason = "stuck_at_100";
        } else if (isCompleteButNotMarked) {
          inactiveReason = "awaiting_cleanup";
        } else if (isStale) {
          inactiveReason = "stale";
        }
      }

      res.json({
        active: isActive,
        stale: isStale,
        stuck: isStuck,
        inactiveReason, // 'stuck_at_100' | 'awaiting_cleanup' | 'stale' | undefined
        cancellable,
        currentPhase: progress.currentPhase,
        phaseStatus: progress.phaseStatus,
        overallPercent: progress.overallPercent || 0,
        statusMessage:
          inactiveReason === "stuck_at_100"
            ? `Sync stuck at ${progress.overallPercent || 0}% - process hanging (last update: ${progress.updatedAt})`
            : inactiveReason === "stale"
              ? `Sync stale (last update: ${progress.updatedAt})`
              : inactiveReason === "awaiting_cleanup"
                ? "Sync completed, awaiting cleanup"
                : progress.statusMessage,
        phases: phaseProgress,
        startedAt: progress.startedAt,
        updatedAt: progress.updatedAt,
      });
    } catch (error) {
      console.error("Error getting sync progress:", error);
      res.status(500).json({ error: "Failed to get sync progress" });
    }
  });

  // Clear sync progress for a store (only allowed when no sync is actively running)
  app.delete("/api/shopify/stores/:id/sync/progress", async (req, res) => {
    try {
      // Check if there's an active sync running (not stale or stuck)
      const progress = await storage.getSyncProgress(req.params.id);
      let isStaleOrStuck = false;

      if (progress) {
        const lastUpdate = progress.updatedAt
          ? new Date(progress.updatedAt).getTime()
          : 0;
        const thirtySecondsAgo = Date.now() - 30 * 1000;
        const twoMinutesAgo = Date.now() - 2 * 60 * 1000;
        const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
        const hasInMemorySync = isInventorySyncActive(req.params.id);
        const isAtHighPercent = (progress.overallPercent || 0) >= 95;

        // SAFETY: Never allow clearing if sync was updated in the last 30 seconds
        // This prevents clearing during phase transitions
        if (lastUpdate > thirtySecondsAgo) {
          return res.status(400).json({
            error:
              "Cannot clear progress - sync was recently active (updated within 30 seconds)",
          });
        }

        // Check if phaseStatus is running - require additional time checks
        if (progress.phaseStatus === "running") {
          // Allow clearing if:
          // - Progress is stale (5+ minutes old, no in-memory sync)
          // - Progress is stuck (95%+, 2+ minutes old, has in-memory sync but hanging)
          // Using 95% threshold since syncs hang at ~99% during cleanup, never reaching 100%
          // SKIP stuck detection during "loading" phase - loading Shopify cache can take a long time
          const isStale = lastUpdate < fiveMinutesAgo && !hasInMemorySync;
          const isStuck =
            progress.currentPhase !== "loading" &&
            isAtHighPercent &&
            lastUpdate < twoMinutesAgo &&
            hasInMemorySync;

          if (!isStale && !isStuck) {
            return res
              .status(400)
              .json({ error: "Cannot clear progress while a sync is running" });
          }

          // If stuck with in-memory sync, try to cancel it first
          if (isStuck && hasInMemorySync) {
            console.log(
              `[Sync Progress] Cancelling stuck sync for store ${req.params.id}`,
            );
            cancelInventorySync(req.params.id);
          }

          // Mark that we've already determined this is stale/stuck - skip sync log check
          isStaleOrStuck = true;
          // Otherwise, it's stale/stuck progress - allow clearing
        }
      }

      // Determine if this was an awaiting_cleanup (100% complete) or truly stale progress
      const progressWasComplete =
        progress && (progress.overallPercent || 0) >= 100;

      // Also check sync logs for running status (but skip if we already determined stale/stuck from progress)
      const logs = await storage.getShopifySyncLogs(req.params.id, 1);
      if (!isStaleOrStuck && logs.length > 0 && logs[0].status === "running") {
        // Check if log is stale too - use progress.updatedAt if available, otherwise fall back to startedAt
        const logUpdate = progress?.updatedAt
          ? new Date(progress.updatedAt).getTime()
          : logs[0].startedAt
            ? new Date(logs[0].startedAt).getTime()
            : 0;
        const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
        if (logUpdate > fiveMinutesAgo) {
          return res
            .status(400)
            .json({ error: "Cannot clear progress while a sync is running" });
        }
      }

      // Clean up any running sync log if we're clearing
      if (logs.length > 0 && logs[0].status === "running") {
        // Mark stale running log - use completed if it reached 100%, failed otherwise
        const finalStatus = progressWasComplete ? "completed" : "failed";
        const message = progressWasComplete
          ? "Sync completed (cleaned up from stale state)"
          : "Sync was stale and manually cleared";
        await storage.updateShopifySyncLog(logs[0].id, {
          status: finalStatus,
          completedAt: new Date(),
          error: progressWasComplete ? undefined : message,
        });
        console.log(
          `[Sync Progress] Marked stale sync log ${logs[0].id} as ${finalStatus} during cleanup`,
        );
      }

      await storage.deleteSyncProgress(req.params.id);
      res.json({ success: true });
    } catch (error) {
      console.error("Error clearing sync progress:", error);
      res.status(500).json({ error: "Failed to clear sync progress" });
    }
  });

  // =========== SSE SYNC PROGRESS STREAM ===========

  /**
   * SSE endpoint for real-time sync progress updates.
   * Clients connect and receive progress updates as events.
   *
   * Usage in frontend:
   * const eventSource = new EventSource(`/api/shopify/stores/${storeId}/sync/progress/stream`);
   * eventSource.onmessage = (event) => {
   *   const progress = JSON.parse(event.data);
   *   // Update UI with progress
   * };
   */
  app.get("/api/shopify/stores/:id/sync/progress/stream", async (req, res) => {
    const storeId = req.params.id;

    // Set SSE headers - complete set for proper SSE behavior
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("Content-Encoding", "none"); // Disable compression
    res.setHeader("X-Accel-Buffering", "no"); // Disable nginx buffering
    res.flushHeaders();

    console.log(`[SSE] Client connected for store ${storeId}`);

    let isClientConnected = true;
    let consecutiveInactiveCount = 0;
    const MAX_INACTIVE_BEFORE_CLOSE = 10; // Keep connection open for ~5 seconds after inactive

    // Send heartbeat comment to keep connection alive
    const sendHeartbeat = () => {
      if (!isClientConnected) return;
      try {
        res.write(`: heartbeat ${Date.now()}\n\n`);
        if (typeof (res as any).flush === "function") {
          (res as any).flush();
        }
      } catch (e) {
        isClientConnected = false;
      }
    };

    // Send progress update
    const sendProgress = async (): Promise<boolean> => {
      if (!isClientConnected) return false;

      try {
        const progress = await storage.getSyncProgress(storeId);

        if (!progress) {
          const data = JSON.stringify({ active: false, cancellable: false });
          res.write(`data: ${data}\n\n`);
          if (typeof (res as any).flush === "function") {
            (res as any).flush();
          }
          return false;
        }

        const cancellable = isInventorySyncActive(storeId);
        const phases = progress.phases || {};

        const phaseList = [
          "backup",
          "loading",
          "stock_updates",
          "variant_creation",
          "variant_deletion",
          "color_size_updates",
          "variant_reorder",
          "stock_info",
          "price_repair",
        ];

        const phaseProgress: Record<string, any> = {};
        for (const phaseName of phaseList) {
          const phaseData = phases[phaseName as keyof typeof phases];
          if (phaseData) {
            const percent =
              phaseData.total > 0
                ? Math.round((phaseData.completed / phaseData.total) * 100)
                : 0;
            phaseProgress[phaseName] = {
              total: phaseData.total,
              completed: phaseData.completed,
              percent,
              status: phaseData.status,
            };
          }
        }

        const lastUpdate = progress.updatedAt
          ? new Date(progress.updatedAt).getTime()
          : 0;
        const twoMinutesAgo = Date.now() - 2 * 60 * 1000;
        const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
        const isProperlyComplete = progress.currentPhase === "complete";

        const anyPhaseRunning = Object.values(phases).some(
          (p: any) => p.status === "running",
        );
        const isStale =
          lastUpdate < fiveMinutesAgo && !cancellable && !isProperlyComplete;
        const isCompleteButNotMarked =
          (progress.overallPercent || 0) >= 95 &&
          !cancellable &&
          !isProperlyComplete &&
          !anyPhaseRunning;
        const isStuck =
          progress.currentPhase !== "loading" &&
          (progress.overallPercent || 0) >= 95 &&
          cancellable &&
          lastUpdate < twoMinutesAgo &&
          !isProperlyComplete;

        const isActive =
          !isProperlyComplete &&
          !isStale &&
          !isCompleteButNotMarked &&
          !isStuck &&
          (cancellable || progress.phaseStatus === "running");

        let inactiveReason: string | undefined;
        if (!isActive && !isProperlyComplete) {
          if (isStuck) inactiveReason = "stuck_at_100";
          else if (isCompleteButNotMarked) inactiveReason = "awaiting_cleanup";
          else if (isStale) inactiveReason = "stale";
        }

        const data = JSON.stringify({
          active: isActive,
          stale: isStale,
          stuck: isStuck,
          inactiveReason,
          cancellable,
          currentPhase: progress.currentPhase,
          phaseStatus: progress.phaseStatus,
          overallPercent: progress.overallPercent || 0,
          statusMessage: progress.statusMessage,
          phases: phaseProgress,
          startedAt: progress.startedAt,
          updatedAt: progress.updatedAt,
        });

        res.write(`data: ${data}\n\n`);
        if (typeof (res as any).flush === "function") {
          (res as any).flush();
        }
        return isActive;
      } catch (error) {
        console.error("[SSE] Error sending progress:", error);
        return false;
      }
    };

    // Send initial progress
    await sendProgress();

    // Set up heartbeat interval (every 15 seconds)
    const heartbeatId = setInterval(sendHeartbeat, 15000);

    // Set up polling interval
    const intervalId = setInterval(async () => {
      if (!isClientConnected) {
        clearInterval(intervalId);
        clearInterval(heartbeatId);
        return;
      }

      const isActive = await sendProgress();

      // Don't immediately close connection when sync becomes inactive
      // Keep it open for a bit to allow client to receive final state
      if (!isActive) {
        consecutiveInactiveCount++;
        if (consecutiveInactiveCount >= MAX_INACTIVE_BEFORE_CLOSE) {
          // Send one final update before closing
          await sendProgress();
          clearInterval(intervalId);
          clearInterval(heartbeatId);
          // Don't call res.end() - let the client close the connection
          // This prevents ECONNRESET errors from auto-reconnect
          isClientConnected = false;
        }
      } else {
        consecutiveInactiveCount = 0;
      }
    }, 500);

    // Handle client disconnect
    req.on("close", () => {
      console.log(`[SSE] Client disconnected for store ${storeId}`);
      isClientConnected = false;
      clearInterval(intervalId);
      clearInterval(heartbeatId);
    });

    req.on("error", (err) => {
      console.error(`[SSE] Request error for store ${storeId}:`, err);
      isClientConnected = false;
      clearInterval(intervalId);
      clearInterval(heartbeatId);
    });
  });

  // Get variant mappings for a store
  app.get("/api/shopify/stores/:id/mappings", async (req, res) => {
    try {
      const mappings = await storage.getShopifyVariantMappings(req.params.id);
      res.json(mappings);
    } catch (error) {
      console.error("Error getting variant mappings:", error);
      res.status(500).json({ error: "Failed to get mappings" });
    }
  });

  // =========== SHOPIFY IMPORT ROUTES ===========

  // Get import jobs for a store
  app.get("/api/shopify/stores/:id/import-jobs", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const jobs = await storage.getShopifyImportJobs(req.params.id, limit);
      res.json(jobs);
    } catch (error) {
      console.error("Error getting import jobs:", error);
      res.status(500).json({ error: "Failed to get import jobs" });
    }
  });

  // Get single import job with changes
  app.get("/api/shopify/import-jobs/:jobId", async (req, res) => {
    try {
      const job = await storage.getShopifyImportJob(req.params.jobId);
      if (!job) {
        return res.status(404).json({ error: "Import job not found" });
      }
      const changes = await storage.getShopifyImportChanges(req.params.jobId);
      res.json({ job, changes });
    } catch (error) {
      console.error("Error getting import job:", error);
      res.status(500).json({ error: "Failed to get import job" });
    }
  });

  // Upload file and calculate diff (create import job)
  app.post(
    "/api/shopify/stores/:id/import",
    upload.single("file"),
    async (req, res) => {
      try {
        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res
            .status(400)
            .json({ error: "Store not connected or missing access token" });
        }

        if (!req.file) {
          return res.status(400).json({ error: "No file uploaded" });
        }

        const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const rows: any[] = XLSX.utils.sheet_to_json(worksheet, {
          header: 1,
          defval: "",
        });

        if (rows.length < 2) {
          return res.status(400).json({
            error: "File must have headers and at least one data row",
          });
        }

        const headers = rows[0].map((h: any) => String(h).trim().toLowerCase());
        const dataRows = rows.slice(1);

        // Create import job first
        const job = await storage.createShopifyImportJob({
          shopifyStoreId: store.id,
          fileName: req.file.originalname,
          status: "processing",
          totalProducts: 0,
          processedProducts: 0,
          totalChanges: 0,
          appliedChanges: 0,
          failedChanges: 0,
        });

        // Parse file and build changes
        const shopifyService = createShopifyService(store);
        const changes: any[] = [];
        const errors: string[] = [];

        // Get column indices
        const handleIdx = headers.findIndex(
          (h: string) =>
            h === "handle" || h === "product handle" || h === "handle (url)",
        );
        const titleIdx = headers.findIndex(
          (h: string) => h === "title" || h === "product title",
        );
        const descIdx = headers.findIndex(
          (h: string) =>
            h === "description" ||
            h === "body_html" ||
            h === "body html" ||
            h === "body (html)",
        );
        const tagsIdx = headers.findIndex(
          (h: string) => h === "tags" || h === "product tags",
        );
        const typeIdx = headers.findIndex(
          (h: string) => h === "type" || h === "product type",
        );
        const vendorIdx = headers.findIndex((h: string) => h === "vendor");
        const seoTitleIdx = headers.findIndex(
          (h: string) =>
            h === "seo title" || h === "seo_title" || h === "meta title",
        );
        const seoDescIdx = headers.findIndex(
          (h: string) =>
            h === "seo description" ||
            h === "seo_description" ||
            h === "meta description",
        );
        const statusIdx = headers.findIndex(
          (h: string) => h === "status" || h === "product status",
        );

        // Google Shopping metafield columns
        const googleCategoryIdx = headers.findIndex(
          (h: string) =>
            h === "google category" ||
            h === "google_category" ||
            h === "google shopping category",
        );
        const googleAgeGroupIdx = headers.findIndex(
          (h: string) =>
            h === "age group" ||
            h === "google age group" ||
            h === "google_age_group",
        );
        const googleGenderIdx = headers.findIndex(
          (h: string) =>
            h === "gender" || h === "google gender" || h === "google_gender",
        );

        // Variant-level columns
        const variantIdIdx = headers.findIndex(
          (h: string) => h === "variant id" || h === "variant_id",
        );
        const skuIdx = headers.findIndex(
          (h: string) => h === "sku" || h === "variant sku",
        );
        const priceIdx = headers.findIndex(
          (h: string) => h === "price" || h === "variant price",
        );
        const compareAtPriceIdx = headers.findIndex(
          (h: string) =>
            h === "compare-at price" ||
            h === "compare at price" ||
            h === "compare_at_price" ||
            h === "compareatprice",
        );
        const barcodeIdx = headers.findIndex(
          (h: string) =>
            h === "barcode" || h === "barcode (isbn, upc, gtin, etc.)",
        );
        const taxableIdx = headers.findIndex(
          (h: string) => h === "charge tax on this product" || h === "taxable",
        );
        const continueSellingIdx = headers.findIndex(
          (h: string) =>
            h === "continue selling inventory when out of stock" ||
            h === "continue selling",
        );
        const hsCodeIdx = headers.findIndex(
          (h: string) =>
            h === "hs tariff code" ||
            h === "hs code" ||
            h === "harmonized system code",
        );
        const countryOfOriginIdx = headers.findIndex(
          (h: string) =>
            h === "country of origin" || h === "country/region of origin",
        );
        const costIdx = headers.findIndex(
          (h: string) => h === "cost" || h === "cost per item",
        );

        if (handleIdx === -1) {
          await storage.updateShopifyImportJob(job.id, {
            status: "failed",
            errorMessage:
              "File must have a 'Handle' column to identify products",
          });
          return res
            .status(400)
            .json({ error: "File must have a 'Handle' column" });
        }

        // Group rows by handle (a product may have multiple variant rows)
        const productsByHandle: Map<string, any[]> = new Map();
        for (const row of dataRows) {
          const handle = String(row[handleIdx] || "").trim();
          if (!handle) continue;
          if (!productsByHandle.has(handle)) {
            productsByHandle.set(handle, []);
          }
          productsByHandle.get(handle)!.push(row);
        }

        let processedCount = 0;
        const totalProducts = productsByHandle.size;

        for (const [handle, productRows] of Array.from(
          productsByHandle.entries(),
        )) {
          try {
            // Fetch current product from Shopify
            const currentProduct =
              await shopifyService.getProductByHandle(handle);

            if (!currentProduct) {
              errors.push(
                `Product with handle '${handle}' not found in Shopify`,
              );
              continue;
            }

            const productId = currentProduct.id;
            const firstRow = productRows[0];

            // Build changes for product-level fields
            const fieldMappings = [
              { idx: titleIdx, field: "title", current: currentProduct.title },
              {
                idx: descIdx,
                field: "descriptionHtml",
                current: currentProduct.descriptionHtml || "",
              },
              {
                idx: tagsIdx,
                field: "tags",
                current: (currentProduct.tags || []).join(", "),
              },
              {
                idx: typeIdx,
                field: "productType",
                current: currentProduct.productType || "",
              },
              {
                idx: vendorIdx,
                field: "vendor",
                current: currentProduct.vendor || "",
              },
              {
                idx: statusIdx,
                field: "status",
                current: currentProduct.status?.toLowerCase() || "active",
              },
            ];

            // SEO fields
            if (seoTitleIdx !== -1 || seoDescIdx !== -1) {
              const currentSeoTitle = currentProduct.seo?.title || "";
              const currentSeoDesc = currentProduct.seo?.description || "";
              if (seoTitleIdx !== -1) {
                fieldMappings.push({
                  idx: seoTitleIdx,
                  field: "seo.title",
                  current: currentSeoTitle,
                });
              }
              if (seoDescIdx !== -1) {
                fieldMappings.push({
                  idx: seoDescIdx,
                  field: "seo.description",
                  current: currentSeoDesc,
                });
              }
            }

            for (const { idx, field, current } of fieldMappings) {
              if (idx === -1) continue;
              let newValue = String(firstRow[idx] ?? "").trim();

              // Handle tags specially - normalize for comparison
              if (field === "tags") {
                const currentNorm = current
                  .split(",")
                  .map((t: string) => t.trim())
                  .filter(Boolean)
                  .sort()
                  .join(", ");
                const newNorm = newValue
                  .split(",")
                  .map((t: string) => t.trim())
                  .filter(Boolean)
                  .sort()
                  .join(", ");
                if (currentNorm !== newNorm) {
                  changes.push({
                    importJobId: job.id,
                    productId,
                    productHandle: handle,
                    productTitle: currentProduct.title,
                    fieldName: field,
                    oldValue: current,
                    newValue,
                    status: "pending",
                    revertPayload: { field, value: current },
                  });
                }
              } else if (String(current || "").trim() !== newValue) {
                changes.push({
                  importJobId: job.id,
                  productId,
                  productHandle: handle,
                  productTitle: currentProduct.title,
                  fieldName: field,
                  oldValue: String(current || ""),
                  newValue,
                  status: "pending",
                  revertPayload: { field, value: current },
                });
              }
            }

            // Handle Google Shopping metafields
            const metafieldChecks = [
              {
                idx: googleCategoryIdx,
                namespace: "google",
                key: "custom_product_type",
              },
              { idx: googleAgeGroupIdx, namespace: "google", key: "age_group" },
              { idx: googleGenderIdx, namespace: "google", key: "gender" },
            ];

            for (const { idx, namespace, key } of metafieldChecks) {
              if (idx === -1) continue;
              const newValue = String(firstRow[idx] ?? "").trim();
              if (!newValue) continue;

              const currentMetafield = currentProduct.metafields?.find(
                (m: any) => m.namespace === namespace && m.key === key,
              );
              const currentValue = currentMetafield?.value || "";

              if (currentValue !== newValue) {
                changes.push({
                  importJobId: job.id,
                  productId,
                  productHandle: handle,
                  productTitle: currentProduct.title,
                  fieldName: `metafield:${namespace}.${key}`,
                  oldValue: currentValue,
                  newValue,
                  status: "pending",
                  revertPayload: {
                    type: "metafield",
                    namespace,
                    key,
                    value: currentValue,
                    metafieldId: currentMetafield?.id,
                  },
                });
              }
            }

            // Handle variant-level fields (price, compare-at price)
            if (
              (priceIdx !== -1 || compareAtPriceIdx !== -1) &&
              currentProduct.variants
            ) {
              for (const row of productRows) {
                // Try to match variant by ID first, then by SKU
                let variantId =
                  variantIdIdx !== -1
                    ? String(row[variantIdIdx] || "").trim()
                    : "";
                const rowSku =
                  skuIdx !== -1 ? String(row[skuIdx] || "").trim() : "";

                // Find matching variant
                let matchedVariant: any = null;
                if (variantId) {
                  // Strip gid:// prefix if present for comparison
                  const cleanVariantId = variantId.replace(
                    /^gid:\/\/shopify\/ProductVariant\//,
                    "",
                  );
                  matchedVariant = currentProduct.variants.find((v: any) => {
                    const vId = String(v.id).replace(
                      /^gid:\/\/shopify\/ProductVariant\//,
                      "",
                    );
                    return vId === cleanVariantId;
                  });
                }
                if (!matchedVariant && rowSku) {
                  matchedVariant = currentProduct.variants.find(
                    (v: any) => v.sku === rowSku,
                  );
                }

                if (!matchedVariant) continue;

                const variantGid = matchedVariant.id.startsWith("gid://")
                  ? matchedVariant.id
                  : `gid://shopify/ProductVariant/${matchedVariant.id}`;

                // Check price change
                if (priceIdx !== -1) {
                  const newPrice = String(row[priceIdx] ?? "").trim();
                  const currentPrice = String(
                    matchedVariant.price || "",
                  ).trim();

                  // Normalize prices for comparison (remove trailing zeros)
                  const normalizePrice = (p: string) =>
                    parseFloat(p || "0").toFixed(2);

                  if (
                    newPrice &&
                    normalizePrice(currentPrice) !== normalizePrice(newPrice)
                  ) {
                    changes.push({
                      importJobId: job.id,
                      productId,
                      productHandle: handle,
                      productTitle: `${currentProduct.title} - ${matchedVariant.title}`,
                      fieldName: "variant:price",
                      oldValue: currentPrice,
                      newValue: newPrice,
                      status: "pending",
                      revertPayload: {
                        type: "variant",
                        variantId: variantGid,
                        field: "price",
                        value: currentPrice,
                      },
                    });
                  }
                }

                // Check compare-at price change
                if (compareAtPriceIdx !== -1) {
                  const newComparePrice = String(
                    row[compareAtPriceIdx] ?? "",
                  ).trim();
                  const currentComparePrice = String(
                    matchedVariant.compareAtPrice || "",
                  ).trim();

                  const normalizePrice = (p: string) =>
                    p ? parseFloat(p).toFixed(2) : "";

                  if (
                    normalizePrice(currentComparePrice) !==
                    normalizePrice(newComparePrice)
                  ) {
                    changes.push({
                      importJobId: job.id,
                      productId,
                      productHandle: handle,
                      productTitle: `${currentProduct.title} - ${matchedVariant.title}`,
                      fieldName: "variant:compareAtPrice",
                      oldValue: currentComparePrice || "(none)",
                      newValue: newComparePrice || "(none)",
                      status: "pending",
                      revertPayload: {
                        type: "variant",
                        variantId: variantGid,
                        field: "compareAtPrice",
                        value: currentComparePrice || null,
                      },
                    });
                  }
                }

                // Check barcode change
                if (barcodeIdx !== -1) {
                  const newBarcode = String(row[barcodeIdx] ?? "").trim();
                  const currentBarcode = String(
                    matchedVariant.barcode || "",
                  ).trim();
                  if (currentBarcode !== newBarcode) {
                    changes.push({
                      importJobId: job.id,
                      productId,
                      productHandle: handle,
                      productTitle: `${currentProduct.title} - ${matchedVariant.title}`,
                      fieldName: "variant:barcode",
                      oldValue: currentBarcode || "(none)",
                      newValue: newBarcode || "(none)",
                      status: "pending",
                      revertPayload: {
                        type: "variant",
                        variantId: variantGid,
                        field: "barcode",
                        value: currentBarcode || null,
                      },
                    });
                  }
                }

                // Check taxable change
                if (taxableIdx !== -1) {
                  const newTaxable = String(row[taxableIdx] ?? "")
                    .trim()
                    .toLowerCase();
                  const currentTaxable = matchedVariant.taxable
                    ? "true"
                    : "false";
                  const newTaxableBool =
                    newTaxable === "true" ||
                    newTaxable === "yes" ||
                    newTaxable === "1";
                  if (matchedVariant.taxable !== newTaxableBool && newTaxable) {
                    changes.push({
                      importJobId: job.id,
                      productId,
                      productHandle: handle,
                      productTitle: `${currentProduct.title} - ${matchedVariant.title}`,
                      fieldName: "variant:taxable",
                      oldValue: currentTaxable,
                      newValue: String(newTaxableBool),
                      status: "pending",
                      revertPayload: {
                        type: "variant",
                        variantId: variantGid,
                        field: "taxable",
                        value: matchedVariant.taxable,
                      },
                    });
                  }
                }

                // Check continue selling (inventory policy) change
                if (continueSellingIdx !== -1) {
                  const newContinueSelling = String(
                    row[continueSellingIdx] ?? "",
                  )
                    .trim()
                    .toLowerCase();
                  const currentPolicy =
                    matchedVariant.inventoryPolicy || "DENY";
                  const newContinueSellingBool =
                    newContinueSelling === "true" ||
                    newContinueSelling === "yes" ||
                    newContinueSelling === "1";
                  const newPolicy = newContinueSellingBool
                    ? "CONTINUE"
                    : "DENY";
                  if (currentPolicy !== newPolicy && newContinueSelling) {
                    changes.push({
                      importJobId: job.id,
                      productId,
                      productHandle: handle,
                      productTitle: `${currentProduct.title} - ${matchedVariant.title}`,
                      fieldName: "variant:inventoryPolicy",
                      oldValue: currentPolicy,
                      newValue: newPolicy,
                      status: "pending",
                      revertPayload: {
                        type: "variant",
                        variantId: variantGid,
                        field: "inventoryPolicy",
                        value: currentPolicy,
                      },
                    });
                  }
                }

                // Check HS Code change
                if (hsCodeIdx !== -1) {
                  const newHsCode = String(row[hsCodeIdx] ?? "").trim();
                  const currentHsCode = String(
                    matchedVariant.inventoryItem?.harmonizedSystemCode || "",
                  ).trim();
                  if (currentHsCode !== newHsCode) {
                    changes.push({
                      importJobId: job.id,
                      productId,
                      productHandle: handle,
                      productTitle: `${currentProduct.title} - ${matchedVariant.title}`,
                      fieldName: "variant:harmonizedSystemCode",
                      oldValue: currentHsCode || "(none)",
                      newValue: newHsCode || "(none)",
                      status: "pending",
                      revertPayload: {
                        type: "inventoryItem",
                        variantId: variantGid,
                        inventoryItemId: matchedVariant.inventoryItem?.id,
                        field: "harmonizedSystemCode",
                        value: currentHsCode || null,
                      },
                    });
                  }
                }

                // Check country of origin change
                if (countryOfOriginIdx !== -1) {
                  const newCountry = String(row[countryOfOriginIdx] ?? "")
                    .trim()
                    .toUpperCase();
                  const currentCountry = String(
                    matchedVariant.inventoryItem?.countryCodeOfOrigin || "",
                  )
                    .trim()
                    .toUpperCase();
                  if (currentCountry !== newCountry) {
                    changes.push({
                      importJobId: job.id,
                      productId,
                      productHandle: handle,
                      productTitle: `${currentProduct.title} - ${matchedVariant.title}`,
                      fieldName: "variant:countryCodeOfOrigin",
                      oldValue: currentCountry || "(none)",
                      newValue: newCountry || "(none)",
                      status: "pending",
                      revertPayload: {
                        type: "inventoryItem",
                        variantId: variantGid,
                        inventoryItemId: matchedVariant.inventoryItem?.id,
                        field: "countryCodeOfOrigin",
                        value: currentCountry || null,
                      },
                    });
                  }
                }

                // Check cost change (always update if cost column present with value)
                if (costIdx !== -1) {
                  const newCost = String(row[costIdx] ?? "").trim();
                  if (newCost) {
                    changes.push({
                      importJobId: job.id,
                      productId,
                      productHandle: handle,
                      productTitle: `${currentProduct.title} - ${matchedVariant.title}`,
                      fieldName: "variant:cost",
                      oldValue: "(will update)",
                      newValue: newCost,
                      status: "pending",
                      revertPayload: {
                        type: "inventoryItem",
                        variantId: variantGid,
                        inventoryItemId: matchedVariant.inventoryItem?.id,
                        field: "cost",
                        value: null,
                      },
                    });
                  }
                }
              }
            }

            processedCount++;

            // Update progress periodically
            if (processedCount % 10 === 0) {
              await storage.updateShopifyImportJob(job.id, {
                processedProducts: processedCount,
                totalProducts,
              });
            }
          } catch (err: any) {
            errors.push(`Error processing ${handle}: ${err.message}`);
          }
        }

        // Save all changes
        if (changes.length > 0) {
          await storage.createShopifyImportChanges(changes);
        }

        // Update job status
        await storage.updateShopifyImportJob(job.id, {
          status: "pending_review",
          totalProducts,
          processedProducts: processedCount,
          totalChanges: changes.length,
          errorMessage: errors.length > 0 ? errors.join("\n") : null,
        });

        const updatedJob = await storage.getShopifyImportJob(job.id);
        const savedChanges = await storage.getShopifyImportChanges(job.id);

        res.json({
          job: updatedJob,
          changes: savedChanges,
          errors: errors.length > 0 ? errors : undefined,
        });
      } catch (error: any) {
        console.error("Error processing import:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to process import" });
      }
    },
  );

  // Apply import changes
  app.post("/api/shopify/import-jobs/:jobId/apply", async (req, res) => {
    try {
      const job = await storage.getShopifyImportJob(req.params.jobId);
      if (!job) {
        return res.status(404).json({ error: "Import job not found" });
      }

      if (job.status !== "pending_review") {
        return res
          .status(400)
          .json({ error: "Job is not in pending review status" });
      }

      if (!job.shopifyStoreId) {
        return res.status(400).json({ error: "Job has no store associated" });
      }
      const store = await storage.getShopifyStore(job.shopifyStoreId);
      if (!store || !store.accessToken) {
        return res.status(400).json({ error: "Store not connected" });
      }

      await storage.updateShopifyImportJob(job.id, { status: "applying" });

      const changes = await storage.getShopifyImportChanges(job.id);
      const shopifyService = createShopifyService(store);

      let appliedCount = 0;
      let failedCount = 0;

      // Group changes by product to batch updates
      const changesByProduct: Map<string, typeof changes> = new Map();
      for (const change of changes) {
        if (!changesByProduct.has(change.productId)) {
          changesByProduct.set(change.productId, []);
        }
        changesByProduct.get(change.productId)!.push(change);
      }

      for (const [productId, productChanges] of Array.from(
        changesByProduct.entries(),
      )) {
        try {
          // Build update payload
          const productUpdate: any = {};
          const metafieldsToSet: any[] = [];
          const seoUpdate: any = {};

          const variantUpdates: Map<string, any> = new Map();

          for (const change of productChanges) {
            if (change.fieldName.startsWith("metafield:")) {
              const [, nsKey] = change.fieldName.split(":");
              const [namespace, key] = nsKey.split(".");
              metafieldsToSet.push({
                namespace,
                key,
                value: change.newValue,
                type: "single_line_text_field",
              });
            } else if (change.fieldName.startsWith("seo.")) {
              const seoField = change.fieldName.replace("seo.", "");
              seoUpdate[seoField] = change.newValue;
            } else if (change.fieldName.startsWith("variant:")) {
              // Handle variant-level updates
              const revertPayload = change.revertPayload as any;
              if (revertPayload?.variantId) {
                const variantId = revertPayload.variantId;
                if (!variantUpdates.has(variantId)) {
                  variantUpdates.set(variantId, {
                    variant: {},
                    inventoryItem: {},
                    inventoryItemId: null,
                  });
                }
                const field = revertPayload.field;

                if (
                  revertPayload.type === "inventoryItem" &&
                  revertPayload.inventoryItemId
                ) {
                  // Inventory item level field
                  variantUpdates.get(variantId)!.inventoryItemId =
                    revertPayload.inventoryItemId;
                  variantUpdates.get(variantId)!.inventoryItem[field] =
                    change.newValue;
                } else {
                  // Variant level field
                  let value: any = change.newValue;
                  if (field === "taxable") {
                    value = change.newValue === "true";
                  } else if (field === "weight") {
                    value = change.newValue
                      ? parseFloat(change.newValue)
                      : null;
                  } else if (field === "price" || field === "compareAtPrice") {
                    value = change.newValue || null;
                  }
                  variantUpdates.get(variantId)!.variant[field] = value;
                }
              }
            } else if (change.fieldName === "tags") {
              productUpdate.tags = (change.newValue || "")
                .split(",")
                .map((t: string) => t.trim())
                .filter(Boolean);
            } else if (change.fieldName === "status") {
              productUpdate.status = (change.newValue || "").toUpperCase();
            } else {
              productUpdate[change.fieldName] = change.newValue;
            }
          }

          // Apply product update
          if (
            Object.keys(productUpdate).length > 0 ||
            Object.keys(seoUpdate).length > 0
          ) {
            await shopifyService.updateProduct(
              productId,
              productUpdate,
              seoUpdate,
            );
          }

          // Apply metafield updates
          if (metafieldsToSet.length > 0) {
            await shopifyService.setProductMetafields(
              productId,
              metafieldsToSet,
            );
          }

          // Apply variant updates
          for (const [variantId, updates] of Array.from(
            variantUpdates.entries(),
          )) {
            // Apply variant-level updates
            if (Object.keys(updates.variant).length > 0) {
              await shopifyService.updateVariant(
                productId,
                variantId,
                updates.variant,
              );
            }
            // Apply inventory item updates
            if (
              Object.keys(updates.inventoryItem).length > 0 &&
              updates.inventoryItemId
            ) {
              await shopifyService.updateInventoryItem(
                updates.inventoryItemId,
                updates.inventoryItem,
              );
            }
          }

          // Mark changes as applied
          for (const change of productChanges) {
            await storage.updateShopifyImportChange(change.id, {
              status: "applied",
              appliedAt: new Date(),
            });
            appliedCount++;
          }
        } catch (err: any) {
          console.error(`Error applying changes to ${productId}:`, err);
          for (const change of productChanges) {
            await storage.updateShopifyImportChange(change.id, {
              status: "failed",
              errorMessage: err.message,
            });
            failedCount++;
          }
        }

        // Update progress
        await storage.updateShopifyImportJob(job.id, {
          appliedChanges: appliedCount,
          failedChanges: failedCount,
        });
      }

      const finalStatus =
        failedCount === 0
          ? "completed"
          : appliedCount > 0
            ? "partial"
            : "failed";
      await storage.updateShopifyImportJob(job.id, {
        status: finalStatus,
        appliedChanges: appliedCount,
        failedChanges: failedCount,
        completedAt: new Date(),
      });

      const updatedJob = await storage.getShopifyImportJob(job.id);
      const updatedChanges = await storage.getShopifyImportChanges(job.id);

      res.json({ job: updatedJob, changes: updatedChanges });
    } catch (error: any) {
      console.error("Error applying import:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to apply import" });
    }
  });

  // Revert import changes
  app.post("/api/shopify/import-jobs/:jobId/revert", async (req, res) => {
    try {
      const job = await storage.getShopifyImportJob(req.params.jobId);
      if (!job) {
        return res.status(404).json({ error: "Import job not found" });
      }

      if (job.status !== "completed" && job.status !== "partial") {
        return res
          .status(400)
          .json({ error: "Can only revert completed or partial imports" });
      }

      if (!job.shopifyStoreId) {
        return res.status(400).json({ error: "Job has no store associated" });
      }
      const store = await storage.getShopifyStore(job.shopifyStoreId);
      if (!store || !store.accessToken) {
        return res.status(400).json({ error: "Store not connected" });
      }

      await storage.updateShopifyImportJob(job.id, { status: "reverting" });

      const changes = await storage.getShopifyImportChanges(job.id);
      const appliedChanges = changes.filter((c) => c.status === "applied");
      const shopifyService = createShopifyService(store);

      let revertedCount = 0;
      let failedCount = 0;

      // Group by product
      const changesByProduct: Map<string, typeof appliedChanges> = new Map();
      for (const change of appliedChanges) {
        if (!changesByProduct.has(change.productId)) {
          changesByProduct.set(change.productId, []);
        }
        changesByProduct.get(change.productId)!.push(change);
      }

      for (const [productId, productChanges] of Array.from(
        changesByProduct.entries(),
      )) {
        try {
          const productUpdate: any = {};
          const metafieldsToSet: any[] = [];
          const seoUpdate: any = {};

          const variantReverts: Map<string, any> = new Map();

          for (const change of productChanges) {
            const revertPayload = change.revertPayload as any;
            if (!revertPayload) continue;

            if (revertPayload.type === "metafield") {
              metafieldsToSet.push({
                namespace: revertPayload.namespace,
                key: revertPayload.key,
                value: revertPayload.value || "",
                type: "single_line_text_field",
              });
            } else if (revertPayload.type === "variant") {
              // Handle variant-level reverts
              const variantId = revertPayload.variantId;
              if (!variantReverts.has(variantId)) {
                variantReverts.set(variantId, {});
              }
              const field = revertPayload.field;
              variantReverts.get(variantId)![field] = revertPayload.value;
            } else if (change.fieldName.startsWith("seo.")) {
              const seoField = change.fieldName.replace("seo.", "");
              seoUpdate[seoField] = change.oldValue;
            } else if (change.fieldName === "tags") {
              productUpdate.tags = (change.oldValue || "")
                .split(",")
                .map((t: string) => t.trim())
                .filter(Boolean);
            } else if (change.fieldName === "status") {
              productUpdate.status = (change.oldValue || "").toUpperCase();
            } else {
              productUpdate[change.fieldName] = change.oldValue;
            }
          }

          if (
            Object.keys(productUpdate).length > 0 ||
            Object.keys(seoUpdate).length > 0
          ) {
            await shopifyService.updateProduct(
              productId,
              productUpdate,
              seoUpdate,
            );
          }

          if (metafieldsToSet.length > 0) {
            await shopifyService.setProductMetafields(
              productId,
              metafieldsToSet,
            );
          }

          // Revert variant updates
          for (const [variantId, updates] of Array.from(
            variantReverts.entries(),
          )) {
            await shopifyService.updateVariant(productId, variantId, updates);
          }

          for (const change of productChanges) {
            await storage.updateShopifyImportChange(change.id, {
              status: "reverted",
              revertedAt: new Date(),
            });
            revertedCount++;
          }
        } catch (err: any) {
          console.error(`Error reverting changes to ${productId}:`, err);
          for (const change of productChanges) {
            await storage.updateShopifyImportChange(change.id, {
              errorMessage: `Revert failed: ${err.message}`,
            });
            failedCount++;
          }
        }
      }

      const finalStatus = failedCount === 0 ? "reverted" : "revert_partial";
      await storage.updateShopifyImportJob(job.id, {
        status: finalStatus,
      });

      const updatedJob = await storage.getShopifyImportJob(job.id);
      const updatedChanges = await storage.getShopifyImportChanges(job.id);

      res.json({ job: updatedJob, changes: updatedChanges });
    } catch (error: any) {
      console.error("Error reverting import:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to revert import" });
    }
  });

  // Clear all import jobs for a store
  app.delete("/api/shopify/stores/:id/import-jobs", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      await storage.clearShopifyImportJobs(store.id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error clearing import jobs:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to clear import history" });
    }
  });

  // Cancel import job (before applying)
  app.post("/api/shopify/import-jobs/:jobId/cancel", async (req, res) => {
    try {
      const job = await storage.getShopifyImportJob(req.params.jobId);
      if (!job) {
        return res.status(404).json({ error: "Import job not found" });
      }

      if (job.status !== "pending_review" && job.status !== "processing") {
        return res.status(400).json({
          error: "Can only cancel jobs in processing or pending review status",
        });
      }

      await storage.updateShopifyImportJob(job.id, { status: "cancelled" });

      const updatedJob = await storage.getShopifyImportJob(job.id);
      res.json({ job: updatedJob });
    } catch (error: any) {
      console.error("Error cancelling import:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to cancel import" });
    }
  });

  // ============ PRODUCT CREATE ROUTES ============

  // Check if product with same title already exists in Shopify
  app.post(
    "/api/shopify/stores/:id/products/check-duplicate",
    async (req, res) => {
      try {
        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const { title } = req.body;
        if (!title) {
          return res.status(400).json({ error: "Title is required" });
        }

        const shopifyService = createShopifyService(store);
        const result = await shopifyService.checkProductExistsByTitle(title);
        res.json(result);
      } catch (error: any) {
        res.status(500).json({ error: error.message });
      }
    },
  );

  // Get product create jobs for a store
  app.get("/api/shopify/stores/:id/product-create-jobs", async (req, res) => {
    try {
      const jobs = await storage.getShopifyProductCreateJobs(req.params.id);
      res.json(jobs);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Get a specific product create job with items
  app.get("/api/shopify/product-create-jobs/:jobId", async (req, res) => {
    try {
      const job = await storage.getShopifyProductCreateJob(req.params.jobId);
      if (!job) {
        return res.status(404).json({ error: "Job not found" });
      }
      const items = await storage.getShopifyProductCreateItems(job.id);
      res.json({ job, items });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Create a single product manually
  app.post(
    "/api/shopify/stores/:id/products/create",
    upload.array("images", 10),
    async (req, res) => {
      try {
        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const {
          title,
          description,
          vendor,
          productType,
          tags,
          status,
          sku,
          price,
          compareAtPrice,
          cost,
          barcode,
          stockInfo,
        } = req.body;

        if (!title) {
          return res.status(400).json({ error: "Product title is required" });
        }

        let sizes: string[] = [];
        let colors: string[] = [];
        let publicationIds: string[] = [];
        let marketIds: string[] = [];
        try {
          if (req.body.sizes) sizes = JSON.parse(req.body.sizes);
          if (req.body.colors) colors = JSON.parse(req.body.colors);
          if (req.body.publicationIds)
            publicationIds = JSON.parse(req.body.publicationIds);
          if (req.body.marketIds) marketIds = JSON.parse(req.body.marketIds);
        } catch (e) {
          // Ignore parsing errors
        }

        const shopifyService = createShopifyService(store);

        // Check for duplicate product
        const duplicateCheck =
          await shopifyService.checkProductExistsByTitle(title);
        if (duplicateCheck.exists) {
          return res.status(400).json({
            error: `A product with the title "${title}" already exists in Shopify`,
            duplicate: true,
            existingProductId: duplicateCheck.productId,
          });
        }

        // Helper function to generate SKU from title, color, and size
        const generateVariantSku = (
          productTitle: string,
          variantColor: string,
          variantSize: string,
        ): string => {
          const parts = [productTitle, variantColor, variantSize].filter(
            (p) => p && p.trim() !== "",
          );
          return parts
            .join("-")
            .replace(/\//g, "-")
            .replace(/\s+/g, "-")
            .replace(/-+/g, "-");
        };

        // Generate variants from sizes and colors
        const variants: Array<{
          sku?: string;
          price?: string;
          compareAtPrice?: string;
          option1?: string;
          option2?: string;
        }> = [];

        const hasSizes = sizes.length > 0;
        const hasColors = colors.length > 0;

        // Generate variants: Color first (option1), then Size (option2)
        if (hasSizes && hasColors) {
          for (const color of colors) {
            for (const size of sizes) {
              variants.push({
                sku: generateVariantSku(title, color, size),
                price: price || "0.00",
                compareAtPrice: compareAtPrice || undefined,
                option1: color,
                option2: size,
              });
            }
          }
        } else if (hasColors) {
          for (const color of colors) {
            variants.push({
              sku: generateVariantSku(title, color, ""),
              price: price || "0.00",
              compareAtPrice: compareAtPrice || undefined,
              option1: color,
            });
          }
        } else if (hasSizes) {
          for (const size of sizes) {
            variants.push({
              sku: generateVariantSku(title, "", size),
              price: price || "0.00",
              compareAtPrice: compareAtPrice || undefined,
              option1: size,
            });
          }
        } else {
          variants.push({
            sku: generateVariantSku(title, "", ""),
            price: price || "0.00",
            compareAtPrice: compareAtPrice || undefined,
          });
        }

        // Build options for the product (only add options that have values)
        // Order: Color first, then Size
        const options: Array<{ name: string; values: string[] }> = [];
        if (hasSizes && hasColors) {
          options.push({ name: "Color", values: colors });
          options.push({ name: "Size", values: sizes });
        } else if (hasColors) {
          options.push({ name: "Color", values: colors });
        } else if (hasSizes) {
          options.push({ name: "Size", values: sizes });
        }

        // Convert images to base64 if uploaded
        const imagesBase64: string[] = [];
        const files = req.files as Express.Multer.File[] | undefined;
        if (files && files.length > 0) {
          for (const file of files) {
            imagesBase64.push(file.buffer.toString("base64"));
          }
        }

        const productData: any = {
          title,
          description: description || "",
          vendor: vendor || "",
          productType: productType || "",
          status: status || "draft",
          tags: tags ? tags.split(",").map((t: string) => t.trim()) : [],
          variants,
          options: options.length > 0 ? options : undefined,
          imagesBase64,
          stockInfo: stockInfo || undefined,
        };

        const result = await shopifyService.createProduct(productData);

        // Enable inventory tracking and update cost for each variant
        if (result.variantIds.length > 0) {
          for (const variantId of result.variantIds) {
            try {
              // Enable inventory tracking
              await shopifyService.enableInventoryTracking(variantId);

              // Update cost if provided
              if (cost) {
                const variantQuery = `
                query getVariant($id: ID!) {
                  productVariant(id: $id) {
                    inventoryItem {
                      id
                    }
                  }
                }
              `;
                const variantResult: any = await shopifyService.graphqlRequest(
                  variantQuery,
                  { id: variantId },
                );
                const inventoryItemId =
                  variantResult?.productVariant?.inventoryItem?.id;
                if (inventoryItemId) {
                  await shopifyService.updateInventoryItem(inventoryItemId, {
                    cost,
                  });
                }
              }
            } catch (variantError) {
              console.error("Error updating variant:", variantError);
              // Continue even if update fails
            }
          }
        }

        // Publish to selected sales channels
        if (publicationIds.length > 0) {
          try {
            await shopifyService.publishProductToChannels(
              result.productId,
              publicationIds,
            );
          } catch (publishError) {
            console.error("Error publishing to channels:", publishError);
            // Continue even if publishing fails
          }
        }

        // Publish to selected markets
        if (marketIds.length > 0) {
          try {
            await shopifyService.publishProductToMarkets(
              result.productId,
              marketIds,
            );
          } catch (publishError) {
            console.error("Error publishing to markets:", publishError);
            // Continue even if publishing fails
          }
        }

        // Create a job record for tracking
        const job = await storage.createShopifyProductCreateJob({
          shopifyStoreId: store.id,
          mode: "manual",
          status: "completed",
          totalProducts: 1,
          processedProducts: 1,
          successCount: 1,
          failedCount: 0,
          completedAt: new Date(),
        });

        await storage.createShopifyProductCreateItems([
          {
            jobId: job.id,
            title,
            description,
            vendor,
            productType,
            tags,
            sku,
            price,
            compareAtPrice,
            barcode,
            itemStatus: "success",
            shopifyProductId: result.productId,
          },
        ]);

        res.json({
          success: true,
          productId: result.productId,
          variantIds: result.variantIds,
        });
      } catch (error: any) {
        console.error("Error creating product:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to create product" });
      }
    },
  );

  // Search products by title
  app.get("/api/shopify/stores/:id/products/search", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store || !store.accessToken) {
        return res.status(400).json({ error: "Store not connected" });
      }

      const title = req.query.title as string;
      if (!title) {
        return res
          .status(400)
          .json({ error: "Title query parameter is required" });
      }

      const shopifyService = createShopifyService(store);
      const products = await shopifyService.searchProductsByTitle(title);

      res.json({ products });
    } catch (error: any) {
      console.error("Error searching products:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to search products" });
    }
  });

  // Upload video to a product (with duplicate detection)
  app.post(
    "/api/shopify/stores/:id/products/video",
    upload.single("video"),
    async (req, res) => {
      try {
        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const { productId } = req.body;
        if (!productId) {
          return res.status(400).json({ error: "Product ID is required" });
        }

        const file = req.file;
        if (!file) {
          return res.status(400).json({ error: "No video file uploaded" });
        }

        const shopifyService = createShopifyService(store);

        // Normalize product ID for consistent duplicate detection
        const normalizeProductId = (id: string): string => {
          if (id.startsWith("gid://shopify/Product/")) {
            return id.replace("gid://shopify/Product/", "");
          }
          return id;
        };
        const normalizedProductId = normalizeProductId(productId);
        const videoFilename = file.originalname;

        // Check 1: Local tracking database for duplicates (fast)
        const localDuplicate = await storage.checkUploadedProductVideo(
          store.id,
          normalizedProductId,
          videoFilename,
        );

        if (localDuplicate) {
          console.log(
            `[Video Upload] Skipped duplicate: "${videoFilename}" already uploaded to product ${normalizedProductId}`,
          );
          return res.json({
            success: true,
            skipped: true,
            reason: `Video "${videoFilename}" was already uploaded to this product`,
          });
        }

        // Check 2: Shopify API for existing videos (catches videos uploaded outside our system)
        try {
          const existingVideos =
            await shopifyService.getProductVideos(productId);
          const shopifyDuplicate = existingVideos.some(
            (v) =>
              v.filename &&
              v.filename.toLowerCase() === videoFilename.toLowerCase(),
          );

          if (shopifyDuplicate) {
            console.log(
              `[Video Upload] Skipped Shopify duplicate: "${videoFilename}" exists on product ${normalizedProductId}`,
            );
            // Record in local tracking to avoid future API checks
            await storage.createUploadedProductVideo(
              store.id,
              normalizedProductId,
              videoFilename,
            );
            return res.json({
              success: true,
              skipped: true,
              reason: `Video "${videoFilename}" already exists on this product in Shopify`,
            });
          }
        } catch (e) {
          // If Shopify check fails, proceed with upload anyway
          console.log(
            `[Video Upload] Could not check Shopify videos for ${productId}:`,
            e,
          );
        }

        // RACE CONDITION PROTECTION: Try to reserve this upload BEFORE calling Shopify
        // The unique constraint (store+product+filename_normalized) will block concurrent requests
        const reservationSuccess = await storage.reserveUploadedProductVideo(
          store.id,
          normalizedProductId,
          videoFilename,
        );
        if (!reservationSuccess) {
          console.log(
            `[Video Upload] Skipped concurrent duplicate: "${videoFilename}" upload already in progress for product ${normalizedProductId}`,
          );
          return res.json({
            success: true,
            skipped: true,
            reason: `Video "${videoFilename}" upload already in progress for this product`,
          });
        }

        // Not a duplicate and reserved - proceed with upload
        // The reservation row (already inserted) becomes the permanent tracking record on success
        // On failure, we delete it so retries can work
        try {
          const videoBase64 = file.buffer.toString("base64");
          await shopifyService.addProductVideo(
            productId,
            videoBase64,
            videoFilename,
            file.size,
          );
          // Reservation row stays in place - it now serves as the permanent duplicate tracking record
          console.log(
            `[Video Upload] Uploaded "${videoFilename}" to product ${normalizedProductId} (tracking record persisted)`,
          );
          res.json({ success: true, skipped: false });
        } catch (uploadError: any) {
          // Upload failed - delete reservation so retries can work
          console.error(
            `[Video Upload] Failed to upload "${videoFilename}" to product ${normalizedProductId}:`,
            uploadError.message,
          );
          await storage.deleteUploadedProductVideo(
            store.id,
            normalizedProductId,
            videoFilename,
          );
          console.log(
            `[Video Upload] Deleted reservation for "${videoFilename}" - retry will be allowed`,
          );
          throw uploadError; // Re-throw to be caught by outer handler
        }
      } catch (error: any) {
        console.error("Error uploading video:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to upload video" });
      }
    },
  );

  // Bulk upload products from Excel/CSV
  app.post(
    "/api/shopify/stores/:id/products/bulk-upload",
    upload.single("file"),
    async (req, res) => {
      try {
        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        if (!req.file) {
          return res.status(400).json({ error: "No file uploaded" });
        }

        const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const jsonData = XLSX.utils.sheet_to_json(worksheet, {
          header: 1,
          defval: "",
        });

        if (jsonData.length < 2) {
          return res.status(400).json({
            error: "File must have headers and at least one data row",
          });
        }

        const headers = (jsonData[0] as string[]).map((h: string) =>
          String(h).toLowerCase().trim(),
        );
        const dataRows = jsonData.slice(1);

        // Find column indices
        const titleIdx = headers.findIndex(
          (h) => h === "title" || h === "product title" || h === "name",
        );
        const descIdx = headers.findIndex(
          (h) =>
            h === "description" ||
            h === "body" ||
            h === "body html" ||
            h === "body (html)",
        );
        const vendorIdx = headers.findIndex((h) => h === "vendor");
        const typeIdx = headers.findIndex(
          (h) => h === "type" || h === "product type",
        );
        const tagsIdx = headers.findIndex((h) => h === "tags");
        const statusIdx = headers.findIndex((h) => h === "status");
        const skuIdx = headers.findIndex(
          (h) => h === "sku" || h === "variant sku",
        );
        const priceIdx = headers.findIndex(
          (h) => h === "price" || h === "variant price",
        );
        const compareAtPriceIdx = headers.findIndex(
          (h) => h === "compare at price" || h === "compare-at price",
        );
        const costIdx = headers.findIndex(
          (h) => h === "cost" || h === "cost per item",
        );
        const sizesIdx = headers.findIndex(
          (h) => h === "sizes" || h === "size",
        );
        const colorsIdx = headers.findIndex(
          (h) => h === "colors" || h === "color",
        );
        const stockInfoIdx = headers.findIndex(
          (h) => h === "stock info" || h === "stockinfo" || h === "stock_info",
        );

        if (titleIdx === -1) {
          return res
            .status(400)
            .json({ error: "File must have a 'Title' column" });
        }

        // Create job
        const job = await storage.createShopifyProductCreateJob({
          shopifyStoreId: store.id,
          fileName: req.file.originalname,
          mode: "bulk",
          status: "pending",
          totalProducts: dataRows.length,
          processedProducts: 0,
          successCount: 0,
          failedCount: 0,
        });

        // Create items from rows
        const items: any[] = [];
        for (let i = 0; i < dataRows.length; i++) {
          const row = dataRows[i] as any[];
          const title = String(row[titleIdx] || "").trim();
          if (!title) continue;

          const sizesValue =
            sizesIdx !== -1 ? String(row[sizesIdx] ?? "").trim() : "";
          const colorsValue =
            colorsIdx !== -1 ? String(row[colorsIdx] || "").trim() : "";

          // Parse sizes - handle ranges like "000-24" or comma-separated or mixed
          let parsedSizes: string[] = [];
          if (sizesValue) {
            // Split by comma first to handle mixed format (e.g., "000-24, 26, 28")
            const parts = sizesValue
              .split(",")
              .map((p) => p.trim())
              .filter((p) => p);

            for (const part of parts) {
              // Check for range pattern within each part
              const rangeMatch = part
                .toUpperCase()
                .match(/^([A-Z0-9]+)\s*(?:TO|-)\s*([A-Z0-9]+)$/);
              if (rangeMatch) {
                const startSize = rangeMatch[1];
                const endSize = rangeMatch[2];

                // Check for letter size range
                const startLetterIdx = LETTER_SIZES.indexOf(startSize);
                const endLetterIdx = LETTER_SIZES.indexOf(endSize);
                if (
                  startLetterIdx !== -1 &&
                  endLetterIdx !== -1 &&
                  startLetterIdx <= endLetterIdx
                ) {
                  parsedSizes.push(
                    ...LETTER_SIZES.slice(startLetterIdx, endLetterIdx + 1),
                  );
                } else {
                  // Check for numeric range with women's sizing
                  const numStart = parseInt(startSize);
                  const numEnd = parseInt(endSize);
                  if (
                    !isNaN(numStart) &&
                    !isNaN(numEnd) &&
                    numStart <= numEnd
                  ) {
                    if (startSize === "000") {
                      parsedSizes.push("000", "00", "0");
                      for (let n = 2; n <= numEnd; n += 2)
                        parsedSizes.push(String(n));
                    } else if (startSize === "00") {
                      parsedSizes.push("00", "0");
                      for (let n = 2; n <= numEnd; n += 2)
                        parsedSizes.push(String(n));
                    } else {
                      for (let n = numStart; n <= numEnd; n += 2)
                        parsedSizes.push(String(n));
                    }
                  }
                }
              } else {
                // Single size value
                parsedSizes.push(part);
              }
            }

            // Deduplicate while preserving order
            parsedSizes = Array.from(new Set(parsedSizes));
          }

          items.push({
            jobId: job.id,
            title,
            description:
              descIdx !== -1 ? String(row[descIdx] || "").trim() : "",
            vendor: vendorIdx !== -1 ? String(row[vendorIdx] || "").trim() : "",
            productType:
              typeIdx !== -1 ? String(row[typeIdx] || "").trim() : "",
            tags: tagsIdx !== -1 ? String(row[tagsIdx] || "").trim() : "",
            sku: skuIdx !== -1 ? String(row[skuIdx] || "").trim() : "",
            price: priceIdx !== -1 ? String(row[priceIdx] || "").trim() : "",
            compareAtPrice:
              compareAtPriceIdx !== -1
                ? String(row[compareAtPriceIdx] || "").trim()
                : "",
            cost: costIdx !== -1 ? String(row[costIdx] || "").trim() : "",
            sizes: parsedSizes.length > 0 ? JSON.stringify(parsedSizes) : null,
            colors: colorsValue
              ? JSON.stringify(
                  colorsValue
                    .split(",")
                    .map((c) => c.trim())
                    .filter((c) => c),
                )
              : null,
            stockInfo:
              stockInfoIdx !== -1
                ? String(row[stockInfoIdx] || "").trim()
                : null,
            itemStatus: "pending",
            rowNumber: i + 2,
          });
        }

        if (items.length === 0) {
          await storage.updateShopifyProductCreateJob(job.id, {
            status: "failed",
            errorMessage: "No valid products found in file",
          });
          return res
            .status(400)
            .json({ error: "No valid products found in file" });
        }

        await storage.createShopifyProductCreateItems(items);
        await storage.updateShopifyProductCreateJob(job.id, {
          totalProducts: items.length,
        });

        const savedItems = await storage.getShopifyProductCreateItems(job.id);
        res.json({ job, items: savedItems });
      } catch (error: any) {
        console.error("Error uploading products:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to upload products" });
      }
    },
  );

  // Process bulk product creation job - ASYNC BACKGROUND PROCESSING
  app.post(
    "/api/shopify/product-create-jobs/:jobId/process",
    async (req, res) => {
      try {
        const job = await storage.getShopifyProductCreateJob(req.params.jobId);
        if (!job) {
          return res.status(404).json({ error: "Job not found" });
        }

        if (job.status !== "pending") {
          return res
            .status(400)
            .json({ error: "Job is not in pending status" });
        }

        if (!job.shopifyStoreId) {
          return res.status(400).json({ error: "Job has no store associated" });
        }

        const store = await storage.getShopifyStore(job.shopifyStoreId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        // Get bulk creation settings from request body
        const bulkStatus = req.body?.status?.toLowerCase() || "draft";
        const publicationIds: string[] = req.body?.publicationIds || [];
        const marketIds: string[] = req.body?.marketIds || [];
        const stockInfoMessage: string | undefined = req.body?.stockInfoMessage;

        // Set status to processing IMMEDIATELY and verify the update succeeded
        try {
          await storage.updateShopifyProductCreateJob(job.id, {
            status: "processing",
            processedProducts: 0,
            successCount: 0,
            failedCount: 0,
          });
        } catch (updateErr: any) {
          console.error(
            "Failed to update job status to processing:",
            updateErr,
          );
          return res.status(500).json({ error: "Failed to start processing" });
        }

        // Return immediately - processing will continue in background
        const updatedJob = await storage.getShopifyProductCreateJob(job.id);
        res.json({
          job: updatedJob,
          message: "Processing started in background",
        });

        // Process products in background (fire-and-forget) - wrapped in try-catch for safety
        setImmediate(async () => {
          // Outer try-catch to ensure job ALWAYS gets updated to a terminal status
          try {
            console.log(
              `[Background] Starting product creation for job ${job.id}`,
            );

            // These operations can fail and should mark the job as failed
            let items: Awaited<
              ReturnType<typeof storage.getShopifyProductCreateItems>
            >;
            let shopifyService: ShopifyService;

            try {
              items = await storage.getShopifyProductCreateItems(job.id);
              shopifyService = createShopifyService(store);
            } catch (initErr: any) {
              console.error(
                `[Background] Failed to initialize job ${job.id}:`,
                initErr,
              );
              await storage.updateShopifyProductCreateJob(job.id, {
                status: "failed",
                errorMessage:
                  initErr.message || "Failed to initialize product creation",
              });
              return;
            }

            let successCount = 0;
            let failedCount = 0;
            let processedCount = 0;

            console.log(
              `[Background] Job ${job.id}: Starting PARALLEL processing of ${items.length} products`,
            );

            // Helper to generate SKU
            const generateVariantSku = (
              productTitle: string,
              variantColor: string,
              variantSize: string,
            ): string => {
              const parts = [productTitle, variantColor, variantSize].filter(
                (p) => p && p.trim() !== "",
              );
              return parts
                .join("-")
                .replace(/\//g, "-")
                .replace(/\s+/g, "-")
                .replace(/-+/g, "-");
            };

            // Step 1: Check for duplicates in parallel (limit to 10 concurrent)
            const { ConcurrencyLimiter } = await import("./shopify");
            const dupCheckLimiter = new ConcurrencyLimiter(10);

            const duplicateResults = await dupCheckLimiter.runAll(
              items.map((item) => async () => {
                try {
                  const check = await shopifyService.checkProductExistsByTitle(
                    item.title,
                  );
                  return {
                    itemId: item.id,
                    title: item.title,
                    isDuplicate: check.exists,
                  };
                } catch {
                  return {
                    itemId: item.id,
                    title: item.title,
                    isDuplicate: false,
                  };
                }
              }),
            );

            const duplicateItemIds = new Set(
              duplicateResults
                .filter((r) => r.isDuplicate)
                .map((r) => r.itemId),
            );

            // Mark duplicates as failed immediately
            for (const dup of duplicateResults.filter((r) => r.isDuplicate)) {
              await storage.updateShopifyProductCreateItem(dup.itemId, {
                itemStatus: "failed",
                errorMessage: `Product with title "${dup.title}" already exists in Shopify`,
              });
              failedCount++;
              processedCount++;
            }

            // Filter out duplicates for processing
            const itemsToProcess = items.filter(
              (item) => !duplicateItemIds.has(item.id),
            );

            console.log(
              `[Background] Job ${job.id}: ${duplicateItemIds.size} duplicates skipped, ${itemsToProcess.length} to process`,
            );

            // Step 2: Build product data for all items
            const productsWithItems: Array<{
              item: (typeof items)[0];
              imageColorAssignments: Array<{
                index: number;
                matchedVariantColor: string;
              }>;
              colors: string[];
              productData: any;
            }> = [];

            for (const item of itemsToProcess) {
              // Parse sizes and colors
              let sizes: string[] = [];
              let colors: string[] = [];
              try {
                if (item.sizes) sizes = JSON.parse(item.sizes);
                if (item.colors) colors = JSON.parse(item.colors);
              } catch (e) {}

              // Generate variants
              const variants: any[] = [];
              const hasSizes = sizes.length > 0;
              const hasColors = colors.length > 0;

              if (hasSizes && hasColors) {
                for (const color of colors) {
                  for (const size of sizes) {
                    variants.push({
                      sku: generateVariantSku(item.title, color, size),
                      price: item.price || "0.00",
                      compareAtPrice: item.compareAtPrice || undefined,
                      cost: item.cost || undefined,
                      option1: color,
                      option2: size,
                    });
                  }
                }
              } else if (hasColors) {
                for (const color of colors) {
                  variants.push({
                    sku: generateVariantSku(item.title, color, ""),
                    price: item.price || "0.00",
                    compareAtPrice: item.compareAtPrice || undefined,
                    cost: item.cost || undefined,
                    option1: color,
                  });
                }
              } else if (hasSizes) {
                for (const size of sizes) {
                  variants.push({
                    sku: generateVariantSku(item.title, "", size),
                    price: item.price || "0.00",
                    compareAtPrice: item.compareAtPrice || undefined,
                    cost: item.cost || undefined,
                    option1: size,
                  });
                }
              } else {
                const baseSku =
                  item.sku && item.sku.trim() !== ""
                    ? item.sku.replace(/\s+/g, "-")
                    : generateVariantSku(item.title, "", "");
                variants.push({
                  sku: baseSku,
                  price: item.price || "0.00",
                  compareAtPrice: item.compareAtPrice || undefined,
                  cost: item.cost || undefined,
                });
              }

              // Build options
              const options: any[] = [];
              if (hasSizes && hasColors) {
                options.push({ name: "Color", values: colors });
                options.push({ name: "Size", values: sizes });
              } else if (hasColors) {
                options.push({ name: "Color", values: colors });
              } else if (hasSizes) {
                options.push({ name: "Size", values: sizes });
              }

              // Parse images data - handle both base64 and file-based images
              // Also preserve matchedVariantColor for variant-specific image assignment
              let imagesBase64: string[] = [];
              let imageColorAssignments: Array<{
                index: number;
                matchedVariantColor: string;
              }> = [];
              try {
                if (item.imagesData) {
                  const imagesArray = JSON.parse(item.imagesData);
                  imagesArray.sort(
                    (a: any, b: any) => (b.isMain ? 1 : 0) - (a.isMain ? 1 : 0),
                  );

                  const fs = await import("fs");
                  const path = await import("path");
                  const ALLOWED_IMAGE_ROOT = path.resolve(
                    process.cwd(),
                    "uploads",
                  );

                  let imageIndex = 0;
                  for (const img of imagesArray) {
                    let imageBase64: string | null = null;

                    if (img.base64) {
                      // Image stored as base64 in memory
                      imageBase64 = img.base64;
                    } else if (img.filePath) {
                      // Image stored on disk - read and convert to base64
                      try {
                        // Resolve the path and ensure it's under the allowed root (security)
                        const imagePath = path.resolve(
                          process.cwd(),
                          img.filePath,
                        );
                        if (!imagePath.startsWith(ALLOWED_IMAGE_ROOT)) {
                          console.warn(
                            `Image path outside allowed root: ${img.filePath}`,
                          );
                          continue;
                        }
                        if (fs.existsSync(imagePath)) {
                          const imageBuffer = fs.readFileSync(imagePath);
                          imageBase64 = imageBuffer.toString("base64");
                        } else {
                          console.warn(
                            `[Background] Image file not found: ${imagePath} (matchedVariantColor: ${img.matchedVariantColor || "none"})`,
                          );
                        }
                      } catch (readErr) {
                        console.warn(
                          `[Background] Failed to read image file: ${img.filePath} (matchedVariantColor: ${img.matchedVariantColor || "none"})`,
                          readErr,
                        );
                      }
                    } else {
                      console.log(
                        `[Background] Image has no base64 or filePath: ${JSON.stringify(img)}`,
                      );
                    }

                    if (imageBase64) {
                      imagesBase64.push(imageBase64);
                      // Track color assignment for this image
                      if (img.matchedVariantColor) {
                        imageColorAssignments.push({
                          index: imageIndex,
                          matchedVariantColor: img.matchedVariantColor,
                        });
                        console.log(
                          `[Background] Image ${imageIndex} has matchedVariantColor: "${img.matchedVariantColor}"`,
                        );
                      } else {
                        console.log(
                          `[Background] Image ${imageIndex} (${img.filename}) has NO matchedVariantColor`,
                        );
                      }
                      imageIndex++;
                    }
                  }
                }
              } catch (e) {
                console.warn("Error parsing images data:", e);
              }

              console.log(
                `[Background] Storing product "${item.title}" with ${imageColorAssignments.length} imageColorAssignments and ${colors.length} colors`,
              );
              console.log(
                `[Background]   imageColorAssignments: ${JSON.stringify(imageColorAssignments)}`,
              );
              console.log(`[Background]   colors: ${JSON.stringify(colors)}`);
              productsWithItems.push({
                item,
                imageColorAssignments, // For assigning images to specific color variants
                colors, // The product's colors for matching
                productData: {
                  title: item.title,
                  description: item.description || "",
                  vendor: item.vendor || "",
                  productType: item.productType || "",
                  status: bulkStatus,
                  tags: item.tags
                    ? item.tags.split(",").map((t: string) => t.trim())
                    : [],
                  variants,
                  options: options.length > 0 ? options : undefined,
                  imagesBase64,
                  stockInfo: stockInfoMessage || item.stockInfo || undefined,
                },
              });
            }

            // Step 3: Create products in parallel using optimized method
            if (productsWithItems.length > 0) {
              const results = await shopifyService.createProductsInParallel(
                productsWithItems.map((p) => p.productData),
                publicationIds,
                marketIds,
                async (processed, success, failed) => {
                  // Update progress in real-time
                  await storage.updateShopifyProductCreateJob(job.id, {
                    processedProducts: duplicateItemIds.size + processed,
                    successCount: success,
                    failedCount: duplicateItemIds.size + failed,
                  });
                },
              );

              // Update item statuses based on results
              for (const result of results) {
                const productWithItem = productsWithItems[result.index];
                const { item, imageColorAssignments, colors } = productWithItem;

                if (result.success) {
                  await storage.updateShopifyProductCreateItem(item.id, {
                    itemStatus: "success",
                    shopifyProductId: result.productId,
                  });
                  successCount++;

                  // Step 4: Assign images to color variants if we have color assignments
                  console.log(
                    `[Background] Product ${result.productId} - VARIANT IMAGE ASSIGNMENT CHECK:`,
                  );
                  console.log(
                    `[Background]   - result.productId: ${result.productId} (type: ${typeof result.productId})`,
                  );
                  console.log(
                    `[Background]   - imageColorAssignments: ${JSON.stringify(imageColorAssignments)}`,
                  );
                  console.log(
                    `[Background]   - colors: ${JSON.stringify(colors)}`,
                  );
                  console.log(
                    `[Background]   - imageColorAssignments?.length: ${imageColorAssignments?.length || 0}`,
                  );
                  console.log(
                    `[Background]   - colors?.length: ${colors?.length || 0}`,
                  );
                  if (
                    result.productId &&
                    imageColorAssignments &&
                    imageColorAssignments.length > 0 &&
                    colors &&
                    colors.length > 0
                  ) {
                    try {
                      console.log(
                        `[Background] Calling assignImagesToColorVariants for ${result.productId} with ${imageColorAssignments.length} assignments`,
                      );
                      await shopifyService.assignImagesToColorVariants(
                        result.productId,
                        imageColorAssignments,
                        colors,
                      );
                      console.log(
                        `[Background] Successfully assigned ${imageColorAssignments.length} images to color variants for product ${result.productId}`,
                      );
                    } catch (assignErr: any) {
                      // Don't fail the product creation if variant assignment fails
                      console.warn(
                        `[Background] Failed to assign images to variants for ${result.productId}: ${assignErr.message}`,
                      );
                    }
                  }
                } else {
                  await storage.updateShopifyProductCreateItem(item.id, {
                    itemStatus: "failed",
                    errorMessage: result.error,
                  });
                  failedCount++;
                }
                processedCount++;
              }
            }

            // Account for duplicates in final counts
            processedCount = duplicateItemIds.size + productsWithItems.length;
            failedCount =
              duplicateItemIds.size + (productsWithItems.length - successCount);

            console.log(
              `[Background] Job ${job.id}: PARALLEL processing complete - ${successCount} success, ${failedCount} failed`,
            );

            const finalStatus =
              failedCount === 0
                ? "completed"
                : successCount > 0
                  ? "partial"
                  : "failed";
            await storage.updateShopifyProductCreateJob(job.id, {
              status: finalStatus,
              processedProducts: processedCount,
              successCount,
              failedCount,
              completedAt: new Date(),
            });

            console.log(
              `[Background] Job ${job.id} completed: ${finalStatus} (${successCount} success, ${failedCount} failed)`,
            );
          } catch (error: any) {
            console.error(
              `[Background] Error processing job ${job.id}:`,
              error,
            );
            await storage.updateShopifyProductCreateJob(job.id, {
              status: "failed",
              errorMessage: error.message || "Background processing failed",
            });
          }
        });
      } catch (error: any) {
        console.error("Error starting product processing:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to start processing" });
      }
    },
  );

  // ============================================
  // HELPER: AI-powered color matching from filename
  // Used by image upload routes to match image colors to product variants
  // ============================================
  async function matchColorFromFilename(
    filename: string,
    productColors: string[],
    colorMappingsCache: Map<string, string>,
  ): Promise<string | undefined> {
    if (productColors.length === 0) return undefined;

    // Remove extension and style numbers to isolate color portion
    const filenameNoExt = filename.replace(/\.[^/.]+$/, "");
    // Remove style numbers (4+ digits) from filename
    const filenameWithoutStyle = filenameNoExt.replace(/\d{4,}/g, "").trim();
    // Clean up separators
    const colorPortion = filenameWithoutStyle.replace(/[-_\s]+/g, " ").trim();

    if (!colorPortion) return undefined;

    // Normalize for comparison
    const normalizedFilenameColor = colorPortion.toLowerCase();

    // Strategy 1: Direct match against product colors (case-insensitive)
    for (const productColor of productColors) {
      const normalizedProductColor = productColor
        .toLowerCase()
        .replace(/[-_\s]+/g, " ")
        .trim();

      // Exact match or substring match
      if (
        normalizedFilenameColor === normalizedProductColor ||
        normalizedFilenameColor.includes(normalizedProductColor) ||
        normalizedProductColor.includes(normalizedFilenameColor)
      ) {
        console.log(
          `[ImageColorMatch] Direct match: "${colorPortion}" -> "${productColor}"`,
        );
        return productColor;
      }

      // Word-based match (for multi-word colors)
      const filenameWords = normalizedFilenameColor
        .split(" ")
        .filter((w) => w.length >= 3);
      const productWords = normalizedProductColor
        .split(" ")
        .filter((w) => w.length >= 3);
      if (
        filenameWords.some((fw) =>
          productWords.some(
            (pw) => fw === pw || fw.includes(pw) || pw.includes(fw),
          ),
        )
      ) {
        console.log(
          `[ImageColorMatch] Word match: "${colorPortion}" -> "${productColor}"`,
        );
        return productColor;
      }
    }

    // Strategy 2: Check if filename color is a code and look up in mappings
    if (isColorCode(colorPortion)) {
      const mappedColor = colorMappingsCache.get(colorPortion.toLowerCase());
      if (mappedColor) {
        // Now try to match the mapped color against product colors
        const normalizedMapped = mappedColor
          .toLowerCase()
          .replace(/[-_\s]+/g, " ")
          .trim();
        for (const productColor of productColors) {
          const normalizedProductColor = productColor
            .toLowerCase()
            .replace(/[-_\s]+/g, " ")
            .trim();
          if (
            normalizedMapped === normalizedProductColor ||
            normalizedMapped.includes(normalizedProductColor) ||
            normalizedProductColor.includes(normalizedMapped)
          ) {
            console.log(
              `[ImageColorMatch] Mapped code: "${colorPortion}" -> "${mappedColor}" -> "${productColor}"`,
            );
            return productColor;
          }
        }
      }
    }

    // Strategy 3: Use formatColorName to normalize and try matching
    const formattedFilenameColor = formatColorName(colorPortion);
    for (const productColor of productColors) {
      if (formatColorName(productColor) === formattedFilenameColor) {
        console.log(
          `[ImageColorMatch] Formatted match: "${colorPortion}" -> "${productColor}"`,
        );
        return productColor;
      }
    }

    console.log(
      `[ImageColorMatch] No match found for "${colorPortion}" in colors: [${productColors.join(", ")}]`,
    );
    return undefined;
  }

  // Helper to extract color codes from filenames and get AI suggestions in batch
  async function resolveColorCodesWithAI(
    filenames: string[],
    colorMappingsCache: Map<string, string>,
  ): Promise<void> {
    // Extract potential color codes from filenames
    const unmappedCodes = new Set<string>();

    for (const filename of filenames) {
      const filenameNoExt = filename.replace(/\.[^/.]+$/, "");
      const filenameWithoutStyle = filenameNoExt.replace(/\d{4,}/g, "").trim();
      const colorPortion = filenameWithoutStyle.replace(/[-_\s]+/g, " ").trim();

      if (
        colorPortion &&
        isColorCode(colorPortion) &&
        !colorMappingsCache.has(colorPortion.toLowerCase())
      ) {
        unmappedCodes.add(colorPortion);
      }
    }

    if (unmappedCodes.size === 0) return;

    console.log(
      `[ImageColorMatch] Requesting AI suggestions for ${unmappedCodes.size} unmapped color codes: [${Array.from(unmappedCodes).join(", ")}]`,
    );

    try {
      const suggestions = await suggestColorCorrections(
        Array.from(unmappedCodes),
      );

      for (const suggestion of suggestions) {
        if (
          suggestion.goodColor &&
          suggestion.goodColor !== suggestion.badColor
        ) {
          const normalizedBad = suggestion.badColor.trim().toLowerCase();
          colorMappingsCache.set(normalizedBad, suggestion.goodColor);
          console.log(
            `[ImageColorMatch] AI suggested: "${suggestion.badColor}" -> "${suggestion.goodColor}"`,
          );
        }
      }
    } catch (err: any) {
      console.warn(
        `[ImageColorMatch] AI color suggestion failed: ${err.message}`,
      );
    }
  }

  // Upload and assign images to product create items
  app.post(
    "/api/shopify/product-create-jobs/:jobId/assign-images",
    upload.array("images", 50),
    async (req, res) => {
      try {
        const job = await storage.getShopifyProductCreateJob(req.params.jobId);
        if (!job) {
          return res.status(404).json({ error: "Job not found" });
        }

        if (job.status !== "pending") {
          return res
            .status(400)
            .json({ error: "Can only assign images to pending jobs" });
        }

        const files = req.files as Express.Multer.File[] | undefined;
        if (!files || files.length === 0) {
          return res.status(400).json({ error: "No images uploaded" });
        }

        const items = await storage.getShopifyProductCreateItems(job.id);

        // Load color mappings for AI-powered color matching
        const colorMappings = await storage.getColorMappings();
        const colorMappingsCache = new Map<string, string>();
        for (const mapping of colorMappings) {
          colorMappingsCache.set(
            mapping.badColor.trim().toLowerCase(),
            mapping.goodColor,
          );
        }

        // Pre-resolve any color codes in filenames using AI
        await resolveColorCodesWithAI(
          files.map((f) => f.originalname),
          colorMappingsCache,
        );

        // Build hash maps for O(1) lookups
        const styleNumberToItem: Map<string, (typeof items)[0]> = new Map();
        const normalizedTitleToItem: Map<string, (typeof items)[0]> = new Map();
        const keywordsToItems: Map<string, (typeof items)[0][]> = new Map();

        for (const item of items) {
          // Extract style numbers (4+ digits) from title
          const styleMatches = item.title.match(/\d{4,}/g);
          if (styleMatches) {
            styleMatches.forEach((num) => styleNumberToItem.set(num, item));
          }
          // Also map normalized full title
          const normalizedTitle = item.title
            .toLowerCase()
            .replace(/[^a-z0-9]/g, "");
          normalizedTitleToItem.set(normalizedTitle, item);

          // Build keyword map for word-based matching
          const words = item.title
            .toLowerCase()
            .split(/[-_\s]+/)
            .filter((w) => w.length >= 3);
          for (const word of words) {
            if (!keywordsToItems.has(word)) {
              keywordsToItems.set(word, []);
            }
            keywordsToItems.get(word)!.push(item);
          }
        }

        console.log(
          `Image assignment: Built lookup maps with ${styleNumberToItem.size} style numbers, ${normalizedTitleToItem.size} titles, ${keywordsToItems.size} keywords`,
        );

        // Match images to products by filename using hash lookup
        const matchResults: Array<{
          filename: string;
          matchedItemId: string | null;
          matchedTitle: string | null;
        }> = [];

        const itemImageUpdates: Map<
          string,
          Array<{
            filename: string;
            base64: string;
            isMain: boolean;
            matchedVariantColor?: string;
          }>
        > = new Map();

        for (const file of files) {
          // Extract filename without extension
          const filenameWithoutExt = file.originalname.replace(/\.[^/.]+$/, "");
          const normalizedFilename = filenameWithoutExt
            .toLowerCase()
            .replace(/[^a-z0-9]/g, "");

          // Extract style numbers from filename for O(1) lookup
          const filenameStyleNumbers =
            filenameWithoutExt.match(/\d{4,}/g) || [];

          let matchedItem: (typeof items)[0] | null = null;

          // Strategy 1: Direct style number match (O(1) lookup)
          for (const styleNum of filenameStyleNumbers) {
            if (styleNumberToItem.has(styleNum)) {
              matchedItem = styleNumberToItem.get(styleNum)!;
              break;
            }
          }

          // Strategy 2: Exact normalized title match (O(1) lookup)
          if (!matchedItem && normalizedTitleToItem.has(normalizedFilename)) {
            matchedItem = normalizedTitleToItem.get(normalizedFilename)!;
          }

          // Strategy 3: Title contained in filename
          if (!matchedItem) {
            const entries = Array.from(normalizedTitleToItem.entries());
            for (const [title, item] of entries) {
              if (normalizedFilename.includes(title)) {
                matchedItem = item;
                break;
              }
            }
          }

          // Strategy 4: Word-based matching (for products without style numbers)
          if (!matchedItem) {
            const filenameWords = filenameWithoutExt
              .toLowerCase()
              .split(/[-_\s]+/)
              .filter((w) => w.length >= 3);
            const candidateCounts: Map<
              string,
              { count: number; maxWordLen: number }
            > = new Map();

            for (const word of filenameWords) {
              const matchingItems = keywordsToItems.get(word);
              if (matchingItems) {
                for (const item of matchingItems) {
                  const existing = candidateCounts.get(item.id) || {
                    count: 0,
                    maxWordLen: 0,
                  };
                  candidateCounts.set(item.id, {
                    count: existing.count + 1,
                    maxWordLen: Math.max(existing.maxWordLen, word.length),
                  });
                }
              }
            }

            // Find best match: prefer most keyword matches, but allow single-match if word is long (4+ chars)
            let bestScore = 0;
            let bestItemId: string | null = null;
            for (const [itemId, data] of Array.from(
              candidateCounts.entries(),
            )) {
              // Score: count * 10 + maxWordLen, require at least 1 match with 4+ char word or 2+ matches
              const meetsThreshold =
                data.count >= 2 || (data.count >= 1 && data.maxWordLen >= 4);
              if (meetsThreshold) {
                const score = data.count * 10 + data.maxWordLen;
                if (score > bestScore) {
                  bestScore = score;
                  bestItemId = itemId;
                }
              }
            }

            if (bestItemId) {
              matchedItem = items.find((i) => i.id === bestItemId) || null;
            }
          }

          if (matchedItem) {
            const base64 = file.buffer.toString("base64");

            // Parse product colors for color matching
            let productColors: string[] = [];
            try {
              if (matchedItem.colors) {
                productColors = JSON.parse(matchedItem.colors);
              }
            } catch (e) {}

            // Use AI-powered color matching
            const matchedVariantColor = await matchColorFromFilename(
              file.originalname,
              productColors,
              colorMappingsCache,
            );

            if (!itemImageUpdates.has(matchedItem.id)) {
              itemImageUpdates.set(matchedItem.id, []);
            }
            itemImageUpdates.get(matchedItem.id)!.push({
              filename: file.originalname,
              base64,
              isMain: false, // Will be set by AI analysis
              ...(matchedVariantColor && { matchedVariantColor }),
            });

            matchResults.push({
              filename: file.originalname,
              matchedItemId: matchedItem.id,
              matchedTitle: matchedItem.title,
            });
          } else {
            matchResults.push({
              filename: file.originalname,
              matchedItemId: null,
              matchedTitle: null,
            });
          }
        }

        // Update items with their matched images
        for (const [itemId, images] of Array.from(itemImageUpdates.entries())) {
          const item = items.find((i) => i.id === itemId);
          let existingImages: any[] = [];
          try {
            if (item?.imagesData) {
              existingImages = JSON.parse(item.imagesData);
            }
          } catch (e) {}

          const allImages = [...existingImages, ...images];
          await storage.updateShopifyProductCreateItem(itemId, {
            imagesData: JSON.stringify(allImages),
          });
        }

        const matchedCount = matchResults.filter((r) => r.matchedItemId).length;
        const unmatchedCount = matchResults.filter(
          (r) => !r.matchedItemId,
        ).length;

        res.json({
          success: true,
          totalImages: files.length,
          matchedCount,
          unmatchedCount,
          matchResults,
        });
      } catch (error: any) {
        console.error("Error assigning images:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to assign images" });
      }
    },
  );

  // Direct image assignment - images are pre-matched by client, just store them
  app.post(
    "/api/shopify/product-create-jobs/:jobId/assign-images-direct",
    upload.array("images", 20),
    async (req, res) => {
      try {
        const job = await storage.getShopifyProductCreateJob(req.params.jobId);
        if (!job) {
          return res.status(404).json({ error: "Job not found" });
        }

        const itemId = req.body.itemId;
        if (!itemId) {
          return res.status(400).json({ error: "itemId is required" });
        }

        const files = req.files as Express.Multer.File[] | undefined;
        if (!files || files.length === 0) {
          return res.status(400).json({ error: "No images uploaded" });
        }

        // Get existing images for this item
        const items = await storage.getShopifyProductCreateItems(job.id);
        const item = items.find((i) => i.id === itemId);
        if (!item) {
          return res.status(404).json({ error: "Item not found" });
        }

        let existingImages: any[] = [];
        try {
          if (item.imagesData) {
            existingImages = JSON.parse(item.imagesData);
          }
        } catch (e) {}

        // Parse product colors for color matching
        let productColors: string[] = [];
        try {
          if (item.colors) {
            productColors = JSON.parse(item.colors);
          }
        } catch (e) {}

        // Load color mappings for AI-powered color matching
        const colorMappings = await storage.getColorMappings();
        const colorMappingsCache = new Map<string, string>();
        for (const mapping of colorMappings) {
          colorMappingsCache.set(
            mapping.badColor.trim().toLowerCase(),
            mapping.goodColor,
          );
        }

        // Pre-resolve any color codes in filenames using AI
        if (productColors.length > 0) {
          await resolveColorCodesWithAI(
            files.map((f) => f.originalname),
            colorMappingsCache,
          );
        }

        // Add new images with AI-powered color matching
        const newImages = await Promise.all(
          files.map(async (file) => {
            const matchedVariantColor =
              productColors.length > 0
                ? await matchColorFromFilename(
                    file.originalname,
                    productColors,
                    colorMappingsCache,
                  )
                : undefined;

            return {
              filename: file.originalname,
              base64: file.buffer.toString("base64"),
              isMain: false,
              ...(matchedVariantColor && { matchedVariantColor }),
            };
          }),
        );

        const allImages = [...existingImages, ...newImages];
        await storage.updateShopifyProductCreateItem(itemId, {
          imagesData: JSON.stringify(allImages),
        });

        res.json({
          success: true,
          imageCount: newImages.length,
          totalImages: allImages.length,
        });
      } catch (error: any) {
        console.error("Error in direct image assignment:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to assign images" });
      }
    },
  );

  // AI analyze images to identify front-facing/main images (with SSE progress)
  // Optimized: Processes 4 products in parallel for ~4x speedup
  app.get(
    "/api/shopify/product-create-jobs/:jobId/analyze-images-stream",
    async (req, res) => {
      res.setHeader("Content-Type", "text/event-stream");
      res.setHeader("Cache-Control", "no-cache");
      res.setHeader("Connection", "keep-alive");

      const sendEvent = (data: any) => {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
      };

      try {
        const job = await storage.getShopifyProductCreateJob(req.params.jobId);
        if (!job) {
          sendEvent({ error: "Job not found" });
          res.end();
          return;
        }

        const items = await storage.getShopifyProductCreateItems(job.id);
        const itemsWithImages = items.filter((item) => {
          if (!item.imagesData) return false;
          try {
            const imgs = JSON.parse(item.imagesData);
            return Array.isArray(imgs) && imgs.length > 0;
          } catch {
            return false;
          }
        });

        const totalToAnalyze = itemsWithImages.length;
        let analyzedSoFar = 0;
        let reorderedCount = 0;
        let alreadyCorrectCount = 0;
        let noFrontFoundCount = 0;
        const noFrontProducts: string[] = [];

        sendEvent({ type: "start", total: totalToAnalyze });

        // Process 4 products in parallel for faster throughput
        const PRODUCT_BATCH_SIZE = 4;
        const dbUpdates: Array<{ id: string; imagesData: string }> = [];

        // Helper to analyze a single product
        const analyzeProduct = async (item: (typeof itemsWithImages)[0]) => {
          let images: any[] = [];
          try {
            images = JSON.parse(item.imagesData!);
          } catch (e) {
            return { item, skipped: true };
          }

          // Single image - already correct
          if (images.length === 1) {
            images[0].isMain = true;
            return { item, images, result: "correct" as const };
          }

          // Use AI to analyze which image is front-facing
          const imagesToAnalyze = images.map((img: any, idx: number) => ({
            base64: img.base64,
            index: idx,
          }));

          const imageDescriptions = await analyzeImagesInParallel(
            imagesToAnalyze,
            item.title,
          );
          let mainIndex = imageDescriptions.findIndex((d) =>
            d.includes("FRONT"),
          );

          let result: "correct" | "reordered" | "noFront";
          if (mainIndex === -1) {
            mainIndex = 0;
            result = "noFront";
          } else if (mainIndex === 0) {
            result = "correct";
          } else {
            result = "reordered";
          }

          // Update isMain flags
          for (let i = 0; i < images.length; i++) {
            images[i].isMain = i === mainIndex;
          }

          return { item, images, result };
        };

        // Process products in batches of 4
        for (let i = 0; i < itemsWithImages.length; i += PRODUCT_BATCH_SIZE) {
          const batch = itemsWithImages.slice(i, i + PRODUCT_BATCH_SIZE);

          // Analyze all products in batch simultaneously
          const results = await Promise.all(batch.map(analyzeProduct));

          // Process results and queue DB updates
          for (const res of results) {
            analyzedSoFar++;

            if ("skipped" in res && res.skipped) {
              continue;
            }

            const { item, images, result } = res as {
              item: (typeof batch)[0];
              images: any[];
              result: "correct" | "reordered" | "noFront";
            };

            if (result === "correct") {
              alreadyCorrectCount++;
            } else if (result === "reordered") {
              reorderedCount++;
            } else {
              noFrontFoundCount++;
              noFrontProducts.push(item.title);
            }

            // Queue DB update (only for non-skipped items with valid images)
            dbUpdates.push({ id: item.id, imagesData: JSON.stringify(images) });
          }

          // Flush DB updates periodically
          if (dbUpdates.length >= 10) {
            await Promise.all(
              dbUpdates.map((update) =>
                storage
                  .updateShopifyProductCreateItem(update.id, {
                    imagesData: update.imagesData,
                  })
                  .catch((err) =>
                    console.error(
                      "Error saving images for item:",
                      update.id,
                      err,
                    ),
                  ),
              ),
            );
            dbUpdates.length = 0;
          }

          // Send progress after each batch
          sendEvent({
            type: "progress",
            current: analyzedSoFar,
            total: totalToAnalyze,
            reordered: reorderedCount,
            correct: alreadyCorrectCount,
            noFront: noFrontFoundCount,
            currentProduct: batch[batch.length - 1]?.title || "",
          });
        }

        // Final flush of any remaining DB updates
        if (dbUpdates.length > 0) {
          await Promise.all(
            dbUpdates.map((update) =>
              storage
                .updateShopifyProductCreateItem(update.id, {
                  imagesData: update.imagesData,
                })
                .catch((err) =>
                  console.error(
                    "Error saving images for item:",
                    update.id,
                    err,
                  ),
                ),
            ),
          );
        }

        sendEvent({
          type: "complete",
          total: totalToAnalyze,
          reordered: reorderedCount,
          correct: alreadyCorrectCount,
          noFront: noFrontFoundCount,
          noFrontProducts,
        });
        res.end();
      } catch (error: any) {
        console.error("Error analyzing images:", error);
        sendEvent({ error: error.message || "Failed to analyze images" });
        res.end();
      }
    },
  );

  // AI analyze images to identify front-facing/main images (non-streaming fallback)
  // Optimized: Processes 4 products in parallel
  app.post(
    "/api/shopify/product-create-jobs/:jobId/analyze-images",
    async (req, res) => {
      try {
        const job = await storage.getShopifyProductCreateJob(req.params.jobId);
        if (!job) {
          return res.status(404).json({ error: "Job not found" });
        }

        const items = await storage.getShopifyProductCreateItems(job.id);
        const itemsWithImages = items.filter((item) => {
          if (!item.imagesData) return false;
          try {
            const imgs = JSON.parse(item.imagesData);
            return Array.isArray(imgs) && imgs.length > 0;
          } catch {
            return false;
          }
        });

        let analyzedCount = 0;
        let reorderedCount = 0;
        let alreadyCorrectCount = 0;
        let noFrontFoundCount = 0;
        const noFrontProducts: string[] = [];
        const PRODUCT_BATCH_SIZE = 4;
        const dbUpdates: Array<{ id: string; imagesData: string }> = [];

        // Helper to analyze a single product
        const analyzeProduct = async (item: (typeof itemsWithImages)[0]) => {
          let images: any[] = [];
          try {
            images = JSON.parse(item.imagesData!);
          } catch (e) {
            return { item, skipped: true };
          }

          if (images.length === 1) {
            images[0].isMain = true;
            return { item, images, result: "correct" as const };
          }

          const imagesToAnalyze = images.map((img: any, idx: number) => ({
            base64: img.base64,
            index: idx,
          }));

          const imageDescriptions = await analyzeImagesInParallel(
            imagesToAnalyze,
            item.title,
          );
          let mainIndex = imageDescriptions.findIndex((d) =>
            d.includes("FRONT"),
          );

          let result: "correct" | "reordered" | "noFront";
          if (mainIndex === -1) {
            mainIndex = 0;
            result = "noFront";
          } else if (mainIndex === 0) {
            result = "correct";
          } else {
            result = "reordered";
          }

          for (let i = 0; i < images.length; i++) {
            images[i].isMain = i === mainIndex;
          }

          return { item, images, result };
        };

        // Process products in batches of 4
        for (let i = 0; i < itemsWithImages.length; i += PRODUCT_BATCH_SIZE) {
          const batch = itemsWithImages.slice(i, i + PRODUCT_BATCH_SIZE);
          const results = await Promise.all(batch.map(analyzeProduct));

          for (const res of results) {
            analyzedCount++;

            if ("skipped" in res && res.skipped) {
              continue;
            }

            const { item, images, result } = res as {
              item: (typeof batch)[0];
              images: any[];
              result: "correct" | "reordered" | "noFront";
            };

            if (result === "correct") alreadyCorrectCount++;
            else if (result === "reordered") reorderedCount++;
            else {
              noFrontFoundCount++;
              noFrontProducts.push(item.title);
            }

            dbUpdates.push({ id: item.id, imagesData: JSON.stringify(images) });
          }

          // Flush DB updates periodically
          if (dbUpdates.length >= 10) {
            await Promise.all(
              dbUpdates.map((update) =>
                storage
                  .updateShopifyProductCreateItem(update.id, {
                    imagesData: update.imagesData,
                  })
                  .catch((err) =>
                    console.error(
                      "Error saving images for item:",
                      update.id,
                      err,
                    ),
                  ),
              ),
            );
            dbUpdates.length = 0;
          }
        }

        // Final flush of any remaining DB updates
        if (dbUpdates.length > 0) {
          await Promise.all(
            dbUpdates.map((update) =>
              storage
                .updateShopifyProductCreateItem(update.id, {
                  imagesData: update.imagesData,
                })
                .catch((err) =>
                  console.error(
                    "Error saving images for item:",
                    update.id,
                    err,
                  ),
                ),
            ),
          );
        }

        res.json({
          success: true,
          analyzedCount,
          totalItems: items.length,
          reorderedCount,
          alreadyCorrectCount,
          noFrontFoundCount,
          noFrontProducts,
        });
      } catch (error: any) {
        console.error("Error analyzing images:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to analyze images" });
      }
    },
  );

  // Update a specific product create item
  app.patch("/api/shopify/product-create-items/:itemId", async (req, res) => {
    try {
      const updated = await storage.updateShopifyProductCreateItem(
        req.params.itemId,
        req.body,
      );
      if (!updated) {
        return res.status(404).json({ error: "Item not found" });
      }
      res.json(updated);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Download bulk upload template
  app.get("/api/shopify/bulk-upload-template", (req, res) => {
    try {
      const templateData = [
        {
          Title: "Example Dress",
          Description: "Beautiful evening gown with elegant design",
          Vendor: "Designer Brand",
          "Product Type": "Dresses",
          Tags: "formal, evening, gown",
          SKU: "DRESS-001",
          Price: "299.99",
          "Compare at Price": "399.99",
          Cost: "120.00",
          Sizes: "000-24",
          Colors: "Black, Navy, Red",
          "Stock Info": "Ships within 2-3 business days",
        },
        {
          Title: "Casual Top",
          Description: "Comfortable everyday top",
          Vendor: "Fashion Co",
          "Product Type": "Tops",
          Tags: "casual, everyday",
          SKU: "TOP-001",
          Price: "49.99",
          "Compare at Price": "",
          Cost: "15.00",
          Sizes: "S, M, L, XL, XXL",
          Colors: "White, Black, Pink",
          "Stock Info": "Ready to ship",
        },
        {
          Title: "Evening Jumpsuit",
          Description: "Stylish jumpsuit for special occasions",
          Vendor: "Designer Brand",
          "Product Type": "Jumpsuits",
          Tags: "formal, party",
          SKU: "",
          Price: "189.99",
          "Compare at Price": "249.99",
          Cost: "75.00",
          Sizes: "00-16",
          Colors: "Black, Ivory",
          "Stock Info": "Pre-order, ships in 1 week",
        },
      ];

      const worksheet = XLSX.utils.json_to_sheet(templateData);
      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook, worksheet, "Products");

      // Set column widths
      worksheet["!cols"] = [
        { wch: 20 }, // Title
        { wch: 40 }, // Description
        { wch: 15 }, // Vendor
        { wch: 15 }, // Product Type
        { wch: 20 }, // Tags
        { wch: 15 }, // SKU
        { wch: 10 }, // Price
        { wch: 15 }, // Compare at Price
        { wch: 10 }, // Cost
        { wch: 15 }, // Sizes
        { wch: 25 }, // Colors
        { wch: 35 }, // Stock Info
      ];

      const buffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });

      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      );
      res.setHeader(
        "Content-Disposition",
        "attachment; filename=bulk-upload-template.xlsx",
      );
      res.send(buffer);
    } catch (error: any) {
      console.error("Error generating template:", error);
      res.status(500).json({ error: "Failed to generate template" });
    }
  });

  // Bulk update product status for completed job
  app.post(
    "/api/shopify/product-create-jobs/:jobId/bulk-status",
    async (req, res) => {
      try {
        const { status } = req.body; // "ACTIVE" | "DRAFT" | "ARCHIVED"
        if (
          !status ||
          !["ACTIVE", "DRAFT", "ARCHIVED"].includes(status.toUpperCase())
        ) {
          return res
            .status(400)
            .json({ error: "Invalid status. Use ACTIVE, DRAFT, or ARCHIVED" });
        }

        const job = await storage.getShopifyProductCreateJob(req.params.jobId);
        if (!job) {
          return res.status(404).json({ error: "Job not found" });
        }

        if (!job.shopifyStoreId) {
          return res.status(400).json({ error: "Job has no store associated" });
        }

        const store = await storage.getShopifyStore(job.shopifyStoreId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const items = await storage.getShopifyProductCreateItems(
          req.params.jobId,
        );
        const successfulItems = items.filter(
          (item) => item.itemStatus === "success" && item.shopifyProductId,
        );

        const shopifyService = createShopifyService(store);
        let updatedCount = 0;
        let errorCount = 0;

        for (const item of successfulItems) {
          try {
            await shopifyService.updateProduct(item.shopifyProductId!, {
              status: status.toUpperCase(),
            });
            updatedCount++;
          } catch (e: any) {
            console.error(
              `Error updating product ${item.shopifyProductId}:`,
              e.message,
            );
            errorCount++;
          }
        }

        res.json({
          success: true,
          updatedCount,
          errorCount,
          totalProducts: successfulItems.length,
        });
      } catch (error: any) {
        console.error("Error bulk updating status:", error);
        res.status(500).json({ error: error.message });
      }
    },
  );

  // Bulk publish products to channels
  app.post(
    "/api/shopify/product-create-jobs/:jobId/bulk-publish",
    async (req, res) => {
      try {
        const { publicationIds } = req.body; // Array of publication IDs
        if (
          !publicationIds ||
          !Array.isArray(publicationIds) ||
          publicationIds.length === 0
        ) {
          return res
            .status(400)
            .json({ error: "publicationIds array is required" });
        }

        const job = await storage.getShopifyProductCreateJob(req.params.jobId);
        if (!job) {
          return res.status(404).json({ error: "Job not found" });
        }

        if (!job.shopifyStoreId) {
          return res.status(400).json({ error: "Job has no store associated" });
        }

        const store = await storage.getShopifyStore(job.shopifyStoreId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const items = await storage.getShopifyProductCreateItems(
          req.params.jobId,
        );
        const successfulItems = items.filter(
          (item) => item.itemStatus === "success" && item.shopifyProductId,
        );

        const shopifyService = createShopifyService(store);
        let publishedCount = 0;
        let errorCount = 0;

        for (const item of successfulItems) {
          try {
            await shopifyService.publishProductToChannels(
              item.shopifyProductId!,
              publicationIds,
            );
            publishedCount++;
          } catch (e: any) {
            console.error(
              `Error publishing product ${item.shopifyProductId}:`,
              e.message,
            );
            errorCount++;
          }
        }

        res.json({
          success: true,
          publishedCount,
          errorCount,
          totalProducts: successfulItems.length,
        });
      } catch (error: any) {
        console.error("Error bulk publishing:", error);
        res.status(500).json({ error: error.message });
      }
    },
  );

  // Delete a product create job
  app.delete("/api/shopify/product-create-jobs/:jobId", async (req, res) => {
    try {
      await storage.deleteShopifyProductCreateJob(req.params.jobId);
      res.json({ success: true });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // ====== Shopify Bulk Operations ======

  // Download bulk operations template
  app.get("/api/shopify/bulk-operations/template", (req, res) => {
    const type = (req.query.type as string) || "price_update";

    let headers: string[][];
    let filename: string;

    switch (type) {
      case "price_update":
        headers = [
          ["Title", "Price"],
          ["Example Product 1", "29.99"],
          ["Example Product 2", "49.99"],
        ];
        filename = "price-update-template.xlsx";
        break;
      case "publish":
        headers = [["Title"], ["Example Product 1"], ["Example Product 2"]];
        filename = "publish-template.xlsx";
        break;
      case "unpublish":
        headers = [["Title"], ["Example Product 1"], ["Example Product 2"]];
        filename = "unpublish-template.xlsx";
        break;
      case "delete":
        headers = [["Title"], ["Example Product 1"], ["Example Product 2"]];
        filename = "delete-template.xlsx";
        break;
      default:
        headers = [
          ["Title", "Price"],
          ["Example Product 1", "29.99"],
          ["Example Product 2", "49.99"],
        ];
        filename = "bulk-operations-template.xlsx";
    }

    const workbook = XLSX.utils.book_new();
    const worksheet = XLSX.utils.aoa_to_sheet(headers);
    XLSX.utils.book_append_sheet(workbook, worksheet, "Bulk Operations");

    const buffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader("Content-Disposition", `attachment; filename=${filename}`);
    res.send(buffer);
  });

  // Get bulk operations for a store
  app.get("/api/shopify/stores/:id/bulk-operations", async (req, res) => {
    try {
      const operations = await storage.getShopifyBulkOperations(req.params.id);
      res.json(operations);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Get single bulk operation with items
  app.get("/api/shopify/bulk-operations/:id", async (req, res) => {
    try {
      const operation = await storage.getShopifyBulkOperation(req.params.id);
      if (!operation) {
        return res.status(404).json({ error: "Operation not found" });
      }
      const items = await storage.getShopifyBulkOperationItems(req.params.id);
      res.json({ ...operation, items });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Parse Excel for bulk operations (price_update, publish, unpublish, delete)
  app.post(
    "/api/shopify/stores/:id/bulk-operations/parse-excel",
    upload.single("file"),
    async (req, res) => {
      try {
        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        if (!req.file) {
          return res.status(400).json({ error: "No file uploaded" });
        }

        const operationType = req.body.operationType as string;
        if (
          !["price_update", "publish", "unpublish", "delete"].includes(
            operationType,
          )
        ) {
          return res.status(400).json({ error: "Invalid operation type" });
        }

        const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const jsonData = XLSX.utils.sheet_to_json(worksheet, {
          header: 1,
          defval: "",
        });

        if (jsonData.length < 2) {
          return res.status(400).json({
            error: "File must have headers and at least one data row",
          });
        }

        const headers = (jsonData[0] as string[]).map((h: string) =>
          String(h).toLowerCase().trim(),
        );
        const dataRows = jsonData.slice(1);

        // Find column indices
        const titleIdx = headers.findIndex(
          (h) => h === "title" || h === "product title" || h === "name",
        );
        const priceIdx = headers.findIndex(
          (h) => h === "price" || h === "new price",
        );

        if (titleIdx === -1) {
          return res
            .status(400)
            .json({ error: "File must have a 'Title' column" });
        }

        if (operationType === "price_update" && priceIdx === -1) {
          return res.status(400).json({
            error:
              "File must have a 'Price' or 'New Price' column for price updates",
          });
        }

        const shopifyService = createShopifyService(store);

        // Parse items and check product existence
        const items: Array<{
          title: string;
          newPrice?: string;
          productId?: string;
          exists: boolean;
          error?: string;
        }> = [];

        for (let i = 0; i < dataRows.length; i++) {
          const row = dataRows[i] as any[];
          const title = String(row[titleIdx] || "").trim();
          if (!title) continue;

          const newPrice =
            operationType === "price_update" && priceIdx !== -1
              ? String(row[priceIdx] || "").trim()
              : undefined;

          // Check if product exists in Shopify
          try {
            const checkResult =
              await shopifyService.checkProductExistsByTitle(title);
            items.push({
              title,
              newPrice,
              productId: checkResult.productId,
              exists: checkResult.exists,
            });
          } catch (e: any) {
            items.push({
              title,
              newPrice,
              exists: false,
              error: e.message,
            });
          }
        }

        // Create bulk operation
        const operation = await storage.createShopifyBulkOperation({
          shopifyStoreId: store.id,
          operationType: operationType as any,
          fileName: req.file.originalname,
          status: "pending",
          totalItems: items.length,
          processedItems: 0,
          successCount: 0,
          failedCount: 0,
        });

        // Create operation items
        const operationItems = await storage.createShopifyBulkOperationItems(
          items.map((item, idx) => ({
            operationId: operation.id,
            productTitle: item.title,
            shopifyProductId: item.productId || null,
            newPrice: item.newPrice || null,
            itemStatus: item.exists ? "pending" : "failed",
            errorMessage: item.exists
              ? null
              : item.error || "Product not found in Shopify",
            rowNumber: idx + 2,
          })),
        );

        res.json({
          operation,
          items: operationItems,
          foundCount: items.filter((i) => i.exists).length,
          notFoundCount: items.filter((i) => !i.exists).length,
        });
      } catch (error: any) {
        console.error("Error parsing bulk operation file:", error);
        res.status(500).json({ error: error.message });
      }
    },
  );

  // Execute bulk operation
  app.post("/api/shopify/bulk-operations/:id/execute", async (req, res) => {
    try {
      const operation = await storage.getShopifyBulkOperation(req.params.id);
      if (!operation) {
        return res.status(404).json({ error: "Operation not found" });
      }

      if (operation.status !== "pending") {
        return res
          .status(400)
          .json({ error: "Operation is not in pending status" });
      }

      if (!operation.shopifyStoreId) {
        return res
          .status(400)
          .json({ error: "Operation has no store associated" });
      }

      const store = await storage.getShopifyStore(operation.shopifyStoreId);
      if (!store || !store.accessToken) {
        return res.status(400).json({ error: "Store not connected" });
      }

      const items = await storage.getShopifyBulkOperationItems(operation.id);
      const pendingItems = items.filter(
        (i) => i.itemStatus === "pending" && i.shopifyProductId,
      );

      // Update operation status to processing
      await storage.updateShopifyBulkOperation(operation.id, {
        status: "processing",
      });

      const shopifyService = createShopifyService(store);
      let successCount = 0;
      let failedCount = 0;

      for (const item of pendingItems) {
        try {
          switch (operation.operationType) {
            case "price_update":
              if (item.newPrice) {
                await shopifyService.updateAllVariantPrices(
                  item.shopifyProductId!,
                  item.newPrice,
                );
              }
              break;
            case "publish":
              await shopifyService.publishProduct(item.shopifyProductId!);
              break;
            case "unpublish":
              await shopifyService.unpublishProduct(item.shopifyProductId!);
              break;
            case "delete":
              await shopifyService.deleteProduct(item.shopifyProductId!);
              break;
          }
          await storage.updateShopifyBulkOperationItem(item.id, {
            itemStatus: "success",
          });
          successCount++;
        } catch (e: any) {
          await storage.updateShopifyBulkOperationItem(item.id, {
            itemStatus: "failed",
            errorMessage: e.message,
          });
          failedCount++;
        }
      }

      // Update operation as completed
      await storage.updateShopifyBulkOperation(operation.id, {
        status: "completed",
        processedItems: pendingItems.length,
        successCount,
        failedCount,
        completedAt: new Date(),
      });

      res.json({
        success: true,
        successCount,
        failedCount,
        totalProcessed: pendingItems.length,
      });
    } catch (error: any) {
      console.error("Error executing bulk operation:", error);
      res.status(500).json({ error: error.message });
    }
  });

  // Delete bulk operation
  app.delete("/api/shopify/bulk-operations/:id", async (req, res) => {
    try {
      await storage.deleteShopifyBulkOperation(req.params.id);
      res.json({ success: true });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Video upload bulk operation - parse folder and match to products
  app.post(
    "/api/shopify/stores/:id/bulk-operations/video-upload",
    upload.array("videos", 50),
    async (req, res) => {
      try {
        const store = await storage.getShopifyStore(req.params.id);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const files = req.files as Express.Multer.File[];
        if (!files || files.length === 0) {
          return res.status(400).json({ error: "No video files uploaded" });
        }

        const shopifyService = createShopifyService(store);

        // Helper to normalize product IDs to consistent format (numeric only)
        const normalizeProductId = (id: string): string => {
          if (id.startsWith("gid://shopify/Product/")) {
            return id.replace("gid://shopify/Product/", "");
          }
          return id;
        };

        // Create bulk operation
        const operation = await storage.createShopifyBulkOperation({
          shopifyStoreId: store.id,
          operationType: "video_upload",
          status: "pending",
          totalItems: files.length,
          processedItems: 0,
          successCount: 0,
          failedCount: 0,
        });

        // Parse video filenames and try to match to products
        const items: Array<{
          filename: string;
          productTitle: string;
          productId?: string;
          videoData: string;
          exists: boolean;
          isDuplicate: boolean;
          duplicateReason?: string;
        }> = [];

        for (const file of files) {
          // Extract product title from filename (remove extension)
          const productTitle = file.originalname
            .replace(/\.(mp4|mov|webm|avi)$/i, "")
            .trim();

          // Search for matching product
          try {
            const checkResult =
              await shopifyService.checkProductExistsByTitle(productTitle);

            let isDuplicate = false;
            let duplicateReason: string | undefined;

            // If product exists, check for duplicate videos
            if (checkResult.exists && checkResult.productId) {
              const normalizedProductId = normalizeProductId(
                checkResult.productId,
              );

              // Check 1: Local tracking database (faster)
              const localDuplicate = await storage.checkUploadedProductVideo(
                store.id,
                normalizedProductId,
                file.originalname,
              );

              if (localDuplicate) {
                isDuplicate = true;
                duplicateReason = `Video "${file.originalname}" was already uploaded to this product`;
              } else {
                // Check 2: Shopify API for existing videos (catches videos uploaded outside our system)
                try {
                  const existingVideos = await shopifyService.getProductVideos(
                    checkResult.productId,
                  );
                  const shopifyDuplicate = existingVideos.some(
                    (v) =>
                      v.filename &&
                      v.filename.toLowerCase() ===
                        file.originalname.toLowerCase(),
                  );

                  if (shopifyDuplicate) {
                    isDuplicate = true;
                    duplicateReason = `Video "${file.originalname}" already exists on this product in Shopify`;
                    // Record in local tracking to avoid future API checks
                    await storage.createUploadedProductVideo(
                      store.id,
                      normalizedProductId,
                      file.originalname,
                    );
                  }
                } catch (e) {
                  // If Shopify check fails, proceed anyway - will be caught at execution
                  console.log(
                    `[Video Upload] Could not check Shopify videos for ${checkResult.productId}:`,
                    e,
                  );
                }
              }
            }

            items.push({
              filename: file.originalname,
              productTitle,
              productId: checkResult.productId,
              videoData: file.buffer.toString("base64"),
              exists: checkResult.exists,
              isDuplicate,
              duplicateReason,
            });
          } catch (e: any) {
            items.push({
              filename: file.originalname,
              productTitle,
              videoData: file.buffer.toString("base64"),
              exists: false,
              isDuplicate: false,
            });
          }
        }

        // Create operation items (store video data temporarily)
        // Mark duplicates as "skipped" immediately
        const operationItems = await storage.createShopifyBulkOperationItems(
          items.map((item, idx) => {
            let itemStatus: string;
            let errorMessage: string | null = null;

            if (item.isDuplicate) {
              itemStatus = "skipped";
              errorMessage = item.duplicateReason || "Duplicate video";
            } else if (!item.exists) {
              itemStatus = "failed";
              errorMessage = "Product not found in Shopify";
            } else {
              itemStatus = "pending";
            }

            return {
              operationId: operation.id,
              productTitle: item.productTitle,
              shopifyProductId: item.productId || null,
              videoFilename: item.filename,
              videoData: item.isDuplicate ? null : item.videoData, // Don't store video data for duplicates
              itemStatus,
              errorMessage,
              rowNumber: idx + 1,
            };
          }),
        );

        const duplicateCount = items.filter((i) => i.isDuplicate).length;
        const foundCount = items.filter(
          (i) => i.exists && !i.isDuplicate,
        ).length;
        const notFoundCount = items.filter((i) => !i.exists).length;

        res.json({
          operation,
          items: operationItems.map((i) => ({
            ...i,
            videoData: undefined, // Don't send video data back
          })),
          foundCount,
          notFoundCount,
          duplicateCount,
        });
      } catch (error: any) {
        console.error("Error processing video upload:", error);
        res.status(500).json({ error: error.message });
      }
    },
  );

  // Execute video upload bulk operation
  app.post(
    "/api/shopify/bulk-operations/:id/execute-videos",
    async (req, res) => {
      try {
        const operation = await storage.getShopifyBulkOperation(req.params.id);
        if (!operation) {
          return res.status(404).json({ error: "Operation not found" });
        }

        if (operation.operationType !== "video_upload") {
          return res
            .status(400)
            .json({ error: "Operation is not a video upload" });
        }

        if (operation.status !== "pending") {
          return res
            .status(400)
            .json({ error: "Operation is not in pending status" });
        }

        if (!operation.shopifyStoreId) {
          return res
            .status(400)
            .json({ error: "Operation has no store associated" });
        }

        const store = await storage.getShopifyStore(operation.shopifyStoreId);
        if (!store || !store.accessToken) {
          return res.status(400).json({ error: "Store not connected" });
        }

        const items = await storage.getShopifyBulkOperationItems(operation.id);
        const pendingItems = items.filter(
          (i) =>
            i.itemStatus === "pending" && i.shopifyProductId && i.videoData,
        );

        // Update operation status to processing
        await storage.updateShopifyBulkOperation(operation.id, {
          status: "processing",
        });

        const shopifyService = createShopifyService(store);
        let successCount = 0;
        let failedCount = 0;

        let skippedCount = 0;

        // Helper to normalize product IDs to consistent format (numeric only)
        const normalizeProductId = (id: string): string => {
          if (id.startsWith("gid://shopify/Product/")) {
            return id.replace("gid://shopify/Product/", "");
          }
          return id;
        };

        for (const item of pendingItems) {
          try {
            const videoFilename = item.videoFilename || "video.mp4";
            const normalizedProductId = normalizeProductId(
              item.shopifyProductId!,
            );

            // Check 1: Local tracking database (fast)
            const localDuplicate = await storage.checkUploadedProductVideo(
              store.id,
              normalizedProductId,
              videoFilename,
            );

            if (localDuplicate) {
              console.log(
                `[Bulk Video] Skipped local duplicate: "${videoFilename}" for product ${normalizedProductId}`,
              );
              await storage.updateShopifyBulkOperationItem(item.id, {
                itemStatus: "skipped",
                errorMessage: `Video "${videoFilename}" was already uploaded to this product`,
                videoData: null,
              });
              skippedCount++;
              continue;
            }

            // Check 2: Shopify API for existing videos (catches videos uploaded outside our system)
            try {
              const existingVideos = await shopifyService.getProductVideos(
                item.shopifyProductId!,
              );
              const shopifyDuplicate = existingVideos.some(
                (v: any) =>
                  v.filename &&
                  v.filename.toLowerCase() === videoFilename.toLowerCase(),
              );

              if (shopifyDuplicate) {
                console.log(
                  `[Bulk Video] Skipped Shopify duplicate: "${videoFilename}" exists on product ${normalizedProductId}`,
                );
                // Record in local tracking to avoid future API checks
                await storage.createUploadedProductVideo(
                  store.id,
                  normalizedProductId,
                  videoFilename,
                );
                await storage.updateShopifyBulkOperationItem(item.id, {
                  itemStatus: "skipped",
                  errorMessage: `Video "${videoFilename}" already exists on this product in Shopify`,
                  videoData: null,
                });
                skippedCount++;
                continue;
              }
            } catch (e) {
              // If Shopify check fails, proceed with reservation pattern
              console.log(
                `[Bulk Video] Could not check Shopify videos for ${item.shopifyProductId}:`,
                e,
              );
            }

            // Check 3: Race condition protection via reservation pattern
            // Try to reserve this upload BEFORE calling Shopify
            const reservationSuccess =
              await storage.reserveUploadedProductVideo(
                store.id,
                normalizedProductId,
                videoFilename,
              );
            if (!reservationSuccess) {
              console.log(
                `[Bulk Video] Skipped concurrent duplicate: "${videoFilename}" upload in progress for product ${normalizedProductId}`,
              );
              await storage.updateShopifyBulkOperationItem(item.id, {
                itemStatus: "skipped",
                errorMessage: `Video "${videoFilename}" upload already in progress for this product`,
                videoData: null,
              });
              skippedCount++;
              continue;
            }

            // Reserved successfully - now upload to Shopify
            // Wrap in try-catch to delete reservation on failure (allows retries)
            try {
              const fileSize = Math.ceil(item.videoData!.length * 0.75);
              await shopifyService.addProductVideo(
                item.shopifyProductId!,
                item.videoData!,
                videoFilename,
                fileSize,
              );
              // Reservation row stays - it's now the permanent tracking record
              console.log(
                `[Bulk Video] Uploaded "${videoFilename}" to product ${normalizedProductId} (tracking record persisted)`,
              );

              await storage.updateShopifyBulkOperationItem(item.id, {
                itemStatus: "success",
                videoData: null, // Clear video data after upload
              });
              successCount++;
            } catch (uploadError: any) {
              // Upload failed - delete reservation so retries can work
              console.error(
                `[Bulk Video] Failed to upload "${videoFilename}" to product ${normalizedProductId}:`,
                uploadError.message,
              );
              await storage.deleteUploadedProductVideo(
                store.id,
                normalizedProductId,
                videoFilename,
              );
              console.log(
                `[Bulk Video] Deleted reservation for "${videoFilename}" - retry will be allowed`,
              );
              throw uploadError; // Re-throw to be caught by outer handler
            }
          } catch (e: any) {
            await storage.updateShopifyBulkOperationItem(item.id, {
              itemStatus: "failed",
              errorMessage: e.message,
              videoData: null,
            });
            failedCount++;
          }
        }

        // Update operation as completed
        await storage.updateShopifyBulkOperation(operation.id, {
          status: "completed",
          processedItems: pendingItems.length,
          successCount,
          failedCount,
          completedAt: new Date(),
        });

        res.json({
          success: true,
          successCount,
          failedCount,
          skippedCount,
          totalProcessed: pendingItems.length,
        });
      } catch (error: any) {
        console.error("Error executing video upload:", error);
        res.status(500).json({ error: error.message });
      }
    },
  );

  // =========== PRODUCT CACHE ROUTES (for fast pagination) ===========

  // Start background sync of products from Shopify to local cache
  // Returns immediately with job ID - frontend polls for status
  app.post("/api/shopify/stores/:id/cache/sync", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store || !store.accessToken) {
        return res
          .status(404)
          .json({ error: "Store not found or not connected" });
      }

      // Check if there's already an active sync job for this store
      const existingJob = await storage.getActiveCacheSyncJob(store.id);
      if (existingJob) {
        return res.json({
          success: true,
          jobId: existingJob.id,
          status: existingJob.status,
          message: "Sync already in progress",
        });
      }

      // Create a new sync job
      const job = await storage.createCacheSyncJob({
        shopifyStoreId: store.id,
        status: "pending",
      });

      console.log(`[Cache Sync] Created job ${job.id} for store ${store.id}`);

      // Start background processing (non-blocking)
      runCacheSyncJob(job.id, store).catch((err) => {
        console.error(`[Cache Sync] Background job ${job.id} failed:`, err);
      });

      res.json({
        success: true,
        jobId: job.id,
        status: "pending",
        message: "Sync started in background",
      });
    } catch (error: any) {
      console.error("Error starting product cache sync:", error);
      res.status(500).json({ error: error.message || "Failed to start sync" });
    }
  });

  // Get currently active sync job for a store (must be before :jobId route)
  app.get(
    "/api/shopify/stores/:storeId/cache/sync/active",
    async (req, res) => {
      try {
        const job = await storage.getActiveCacheSyncJob(req.params.storeId);
        if (!job) {
          return res.status(404).json({ error: "No active sync job" });
        }

        res.json({
          id: job.id,
          status: job.status,
          objectCount: job.totalObjects || 0,
          productCount: job.processedProducts || 0,
        });
      } catch (error: any) {
        console.error("Error getting active sync job:", error);
        res.status(500).json({ error: error.message });
      }
    },
  );

  // Get sync job status (for polling)
  app.get(
    "/api/shopify/stores/:storeId/cache/sync/:jobId",
    async (req, res) => {
      try {
        const job = await storage.getCacheSyncJob(req.params.jobId);
        if (!job) {
          return res.status(404).json({ error: "Job not found" });
        }

        res.json({
          id: job.id,
          status: job.status,
          objectCount: job.totalObjects || 0,
          productCount: job.processedProducts || 0,
          bulkOperationStatus: job.bulkOperationStatus,
          startedAt: job.startedAt,
          completedAt: job.completedAt,
          errorMessage: job.errorMessage,
        });
      } catch (error: any) {
        console.error("Error getting sync job status:", error);
        res.status(500).json({ error: error.message });
      }
    },
  );

  // Cancel stuck bulk operation
  app.post("/api/shopify/stores/:id/cache/sync/cancel", async (req, res) => {
    try {
      const store = await storage.getShopifyStore(req.params.id);
      if (!store || !store.accessToken) {
        return res
          .status(404)
          .json({ error: "Store not found or not connected" });
      }

      const { ShopifyService } = await import("./shopify");
      const service = createShopifyService(store);

      const result = await service.cancelBulkOperation();

      // Also mark any active sync job as failed
      const activeJob = await storage.getActiveCacheSyncJob(store.id);
      if (activeJob) {
        await storage.updateCacheSyncJob(activeJob.id, {
          status: "failed",
          errorMessage: "Cancelled by user",
        });
      }

      res.json({
        success: true,
        ...result,
        message: result.cancelled
          ? "Bulk operation cancelled"
          : `No running operation (status: ${result.status})`,
      });
    } catch (error: any) {
      console.error("Error cancelling bulk operation:", error);
      res.status(500).json({ error: error.message || "Failed to cancel" });
    }
  });

  // Background sync job processor
  async function runCacheSyncJob(jobId: string, store: any) {
    const { ShopifyService } = await import("./shopify");
    const service = createShopifyService(store);

    try {
      // Mark job as running
      await storage.updateCacheSyncJob(jobId, { status: "running" });
      console.log(
        `[Cache Sync] Job ${jobId}: Starting sync for store ${store.id}`,
      );

      const startTime = Date.now();
      let productBuffer: any[] = [];
      const BUFFER_SIZE = 100; // Larger batches = fewer DB operations
      let batchCount = 0;
      let totalProducts = 0;

      // Process bulk export - DON'T clear cache until we have new data
      // This prevents data loss if the job is interrupted
      const result = await service.bulkExportProductsForCache(
        async (product) => {
          // Determine enrichment status based on what data exists
          // Priority: hasSpecifics + hasDescription + SEO = ready, hasSpecifics + hasDescription = enriched, hasSpecifics = analyzed
          // Products without specifics remain pending (description alone doesn't qualify as "enriched")
          let enrichmentStatus = "pending";
          if (
            product.hasSpecifics &&
            product.hasDescription &&
            product.hasSeoTitle &&
            product.hasSeoDescription
          ) {
            enrichmentStatus = "ready";
          } else if (product.hasSpecifics && product.hasDescription) {
            enrichmentStatus = "enriched";
          } else if (product.hasSpecifics) {
            enrichmentStatus = "analyzed";
          }

          // Debug logging for description sync issues
          if (
            product.title?.includes("02022") ||
            product.title?.includes("65576")
          ) {
            console.log(`[Cache Sync DEBUG] Product: ${product.title}`);
            console.log(
              `[Cache Sync DEBUG] descriptionHtml present: ${!!product.descriptionHtml}`,
            );
            console.log(
              `[Cache Sync DEBUG] descriptionHtml length: ${product.descriptionHtml?.length || 0}`,
            );
            console.log(
              `[Cache Sync DEBUG] hasDescription flag: ${product.hasDescription}`,
            );
          }

          productBuffer.push({
            id: product.id,
            shopifyStoreId: store.id,
            title: product.title,
            handle: product.handle,
            productType: product.productType,
            vendor: product.vendor,
            status: product.status || "active",
            description: product.descriptionHtml || null,
            tags: Array.isArray(product.tags)
              ? product.tags.join(", ")
              : product.tags || null,
            imageUrl: product.imageUrl || null,
            images: product.images || [],
            metafields: product.metafields || [],
            collectionName: product.collectionName || "Uncategorized",
            collectionHandle: product.collectionHandle || "uncategorized",
            collections: product.collections || [],
            enrichmentStatus,
            needsReview: false,
            variantCount: product.variantCount || 0,
            totalInventory: product.totalInventory || 0,
            price: product.price || null,
            createdAt: product.createdAt ? new Date(product.createdAt) : null,
            updatedAt: product.updatedAt ? new Date(product.updatedAt) : null,
            hasDescription: product.hasDescription || false,
            hasSeoTitle: product.hasSeoTitle || false,
            hasSeoDescription: product.hasSeoDescription || false,
            hasSpecifics: product.hasSpecifics || false,
          });

          if (productBuffer.length >= BUFFER_SIZE) {
            // Upsert will update existing or insert new - atomic operation
            await storage.upsertProductCache(productBuffer);
            batchCount++;

            // Count products BEFORE clearing buffer
            const batchSize = productBuffer.length;
            totalProducts += batchSize;

            // Clear buffer and allow GC to reclaim memory
            productBuffer.length = 0;
            productBuffer = [];

            // Add delay every 5 batches to allow garbage collection
            // This is critical for very large catalogs (20k+ products)
            if (batchCount % 5 === 0) {
              await new Promise((resolve) => setTimeout(resolve, 50));
            }

            // Update job progress every 10 batches
            if (batchCount % 10 === 0) {
              await storage.updateCacheSyncJob(jobId, {
                processedProducts: totalProducts,
              });
              console.log(
                `[Cache Sync] Job ${jobId}: Processed ${totalProducts} products`,
              );
            }
          }
        },
        async (count, status) => {
          console.log(
            `[Cache Sync] Job ${jobId}: ${status} - ${count} objects`,
          );
          await storage.updateCacheSyncJob(jobId, {
            totalObjects: count,
            bulkOperationStatus: status,
          });
        },
      );

      // Flush remaining buffer
      if (productBuffer.length > 0) {
        await storage.upsertProductCache(productBuffer);
        totalProducts += productBuffer.length;
      }

      const duration = Date.now() - startTime;
      console.log(
        `[Cache Sync] Job ${jobId}: Completed - ${totalProducts} products in ${duration}ms`,
      );

      // Also sync ALL Shopify collections (not just those on products)
      try {
        console.log(`[Cache Sync] Job ${jobId}: Syncing all collections...`);
        const allCollections = await service.getAllCollections();

        // Clear old collections first, then insert fresh data
        await storage.deleteCollectionCache(store.id);

        if (allCollections.length > 0) {
          await storage.upsertCollectionCache(
            allCollections.map((c) => ({
              id: c.id,
              shopifyStoreId: store.id,
              title: c.title,
              handle: c.handle,
              productsCount: c.productsCount,
            })),
          );
          console.log(
            `[Cache Sync] Job ${jobId}: Synced ${allCollections.length} collections`,
          );
        }
      } catch (collErr: any) {
        console.error(
          `[Cache Sync] Job ${jobId}: Collection sync failed:`,
          collErr.message,
        );
        // Don't fail the whole job for collection sync failure
      }

      // Mark job as completed
      await storage.updateCacheSyncJob(jobId, {
        status: "completed",
        processedProducts: totalProducts,
        completedAt: new Date(),
      });

      // Update store's productCacheLastSyncedAt
      await storage.updateShopifyStore(store.id, {
        productCacheLastSyncedAt: new Date(),
      });
    } catch (error: any) {
      console.error(`[Cache Sync] Job ${jobId} failed:`, error);
      await storage.updateCacheSyncJob(jobId, {
        status: "failed",
        errorMessage: error.message || "Unknown error",
        completedAt: new Date(),
      });
    }
  }

  // Get paginated products from cache
  app.get("/api/shopify/stores/:id/cache/products", async (req, res) => {
    try {
      const page = parseInt(req.query.page as string) || 1;
      const limit = parseInt(req.query.limit as string) || 50;
      const collection = req.query.collection as string;
      const vendor = req.query.vendor as string;
      const status = req.query.status as string;
      const search = req.query.search as string;
      const sortBy = (req.query.sortBy as string) || "createdAt";
      const sortOrder =
        (req.query.sortOrder as string) === "asc" ? "asc" : "desc";

      const result = await storage.getProductCache(req.params.id, {
        page,
        limit,
        collection,
        vendor,
        status,
        search,
        sortBy,
        sortOrder,
      });

      // Fetch prices for these products from variant cache
      const productIds = result.products.map((p: any) => p.id);
      const variantPrices =
        productIds.length > 0
          ? await storage.getFirstVariantPricesByProductIds(
              req.params.id,
              productIds,
            )
          : new Map();

      // Add price to each product
      const productsWithPrice = result.products.map((p: any) => ({
        ...p,
        price: variantPrices.get(p.id) || null,
      }));

      // Add hasMore flag for infinite scroll
      const hasMore = page * limit < result.total;

      res.json({
        products: productsWithPrice,
        total: result.total,
        page: result.page,
        pageSize: result.pageSize,
        hasMore,
      });
    } catch (error: any) {
      console.error("Error getting cached products:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get products" });
    }
  });

  // Get cache statistics
  app.get("/api/shopify/stores/:id/cache/stats", async (req, res) => {
    try {
      const stats = await storage.getProductCacheStats(req.params.id);
      res.json(stats);
    } catch (error: any) {
      console.error("Error getting cache stats:", error);
      res.status(500).json({ error: error.message || "Failed to get stats" });
    }
  });

  // Get all product IDs matching filters (for Select All functionality)
  app.get("/api/shopify/stores/:id/cache/products/ids", async (req, res) => {
    try {
      const collection = req.query.collection as string;
      const vendor = req.query.vendor as string;
      const status = req.query.status as string;
      const search = req.query.search as string;

      const ids = await storage.getProductCacheIds(req.params.id, {
        collection,
        vendor,
        status,
        search,
      });

      res.json({ ids, total: ids.length });
    } catch (error: any) {
      console.error("Error getting product IDs:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get product IDs" });
    }
  });

  // Get collection list from cache (ALL Shopify collections, not just from products)
  app.get("/api/shopify/stores/:id/cache/collections", async (req, res) => {
    try {
      // Use the new collection cache that stores ALL Shopify collections
      let collections = await storage.getAllCachedCollections(req.params.id);

      // Fall back to extracting collections from product cache if collection_cache is empty
      // (happens when sync hasn't run since the collection_cache table was added)
      if (collections.length === 0) {
        const productCollections = await storage.getProductCacheCollections(
          req.params.id,
        );
        collections = productCollections.map((c) => ({
          id: c.handle,
          name: c.name,
          handle: c.handle,
          count: c.count,
        }));
      }

      res.json(collections);
    } catch (error: any) {
      console.error("Error getting cached collections:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get collections" });
    }
  });

  // Get vendor list from cache
  app.get("/api/shopify/stores/:id/cache/vendors", async (req, res) => {
    try {
      const vendors = await storage.getProductCacheVendors(req.params.id);
      res.json(vendors);
    } catch (error: any) {
      console.error("Error getting cached vendors:", error);
      res.status(500).json({ error: error.message || "Failed to get vendors" });
    }
  });

  // Get vendor coverage stats (aggregated from database)
  app.get(
    "/api/shopify/stores/:id/cache/vendors/coverage",
    async (req, res) => {
      try {
        const coverage = await storage.getCoverageByVendor(req.params.id);
        res.json(coverage);
      } catch (error: any) {
        console.error("Error getting vendor coverage:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to get vendor coverage" });
      }
    },
  );

  // Get single product details (full data from Shopify)
  app.get(
    "/api/shopify/stores/:storeId/cache/products/:productId/details",
    async (req, res) => {
      try {
        const { storeId, productId } = req.params;

        const store = await storage.getShopifyStore(storeId);
        if (!store || !store.accessToken) {
          return res
            .status(404)
            .json({ error: "Store not found or not connected" });
        }

        const { ShopifyService } = await import("./shopify");
        const service = createShopifyService(store);

        // Fetch full product data from Shopify
        const productGid = `gid://shopify/Product/${productId}`;
        const product = await service.getProductById(productGid);

        if (!product) {
          return res.status(404).json({ error: "Product not found" });
        }

        // Check for existing metafields (product specifics) and update cache if detected
        // This handles cases where products already have specifics in Shopify
        try {
          const specificKeys = [
            "my_fields.occasion",
            "my_fields.material",
            "my_fields.style",
            "my_fields.back_style",
            "my_fields.necklines",
            "my_fields.length",
            "my_fields.sleeve_type",
            "my_fields.details",
          ];
          const metafields = product.metafields || {};
          const hasSpecificsData = specificKeys.some((key) => {
            const val = metafields[key];
            return val && val.trim() && val !== "-";
          });

          // Also check for description and SEO (description must be > 80 characters to count)
          const descText = product.descriptionHtml
            ? product.descriptionHtml.replace(/<[^>]*>/g, "").trim()
            : "";
          const hasDescription = descText.length > 80;
          const hasSeoTitle = !!(
            product.seo?.title && product.seo.title.trim()
          );
          const hasSeoDescription = !!(
            product.seo?.description && product.seo.description.trim()
          );

          // Compute the actual enrichment status based on current Shopify data
          // This allows downgrades when products no longer meet thresholds
          let newStatus = "pending";
          if (
            hasSpecificsData &&
            hasDescription &&
            hasSeoTitle &&
            hasSeoDescription
          ) {
            newStatus = "ready";
          } else if (hasSpecificsData && hasDescription) {
            newStatus = "enriched";
          } else if (hasSpecificsData) {
            newStatus = "analyzed";
          }

          // Update all flags with actual values (including false for downgrades)
          await storage.updateProductCacheEnrichment(productId, {
            hasSpecifics: hasSpecificsData,
            hasDescription,
            hasSeoTitle,
            hasSeoDescription,
            enrichmentStatus: newStatus,
          });
        } catch (cacheError) {
          console.log("Cache flag update skipped:", cacheError);
        }

        res.json(product);
      } catch (error: any) {
        console.error("Error getting product details:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to get product details" });
      }
    },
  );

  // Update product enrichment status in cache
  app.patch(
    "/api/shopify/stores/:storeId/cache/products/:productId",
    async (req, res) => {
      try {
        const { productId } = req.params;
        const updates = req.body;

        await storage.updateProductCacheEnrichment(productId, updates);

        const updated = await storage.getProductCacheById(productId);
        res.json(updated);
      } catch (error: any) {
        console.error("Error updating product cache:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to update product" });
      }
    },
  );

  // Clear cache for a store
  app.delete("/api/shopify/stores/:id/cache", async (req, res) => {
    try {
      await storage.deleteProductCache(req.params.id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error clearing product cache:", error);
      res.status(500).json({ error: error.message || "Failed to clear cache" });
    }
  });

  // ============================================
  // Quick Actions Preferences
  // ============================================

  const VALID_QUICK_ACTIONS = [
    // Products
    "manual-entry",
    "bulk-upload",
    "bulk-operations",
    "view-products",
    "export-products",
    // Product Management
    "ai-analysis",
    "review-ai",
    "ai-descriptions",
    "image-processing",
    // Sync & Channels
    "sync-shopify",
    "upload-inventory",
    "ebay-connect",
    // Monitoring
    "view-exceptions",
    "manage-rules",
    "settings",
  ];

  const DEFAULT_QUICK_ACTIONS = [
    "manual-entry",
    "bulk-upload",
    "sync-shopify",
    "review-ai",
  ];

  // Get quick actions preferences
  app.get("/api/quick-actions", async (req, res) => {
    try {
      const setting = await storage.getAppSetting("quickActions");
      const actions = setting?.actions || DEFAULT_QUICK_ACTIONS;
      res.json({ actions });
    } catch (error: any) {
      console.error("Error getting quick actions:", error);
      res.json({ actions: DEFAULT_QUICK_ACTIONS });
    }
  });

  // Save quick actions preferences
  app.put("/api/quick-actions", async (req, res) => {
    try {
      const { actions } = req.body;

      if (!Array.isArray(actions)) {
        return res.status(400).json({ error: "actions must be an array" });
      }

      // Validate all action IDs are valid
      const validActions = actions.filter((a: string) =>
        VALID_QUICK_ACTIONS.includes(a),
      );

      await storage.setAppSetting("quickActions", { actions: validActions });
      res.json({ actions: validActions });
    } catch (error: any) {
      console.error("Error saving quick actions:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to save quick actions" });
    }
  });

  // ============================================
  // Exceptions Reporting
  // ============================================

  // Get exceptions summary (counts)
  app.get("/api/exceptions/summary", async (req, res) => {
    try {
      const summary = await storage.getExceptionsSummary();
      res.json(summary);
    } catch (error: any) {
      console.error("Error getting exceptions summary:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get exceptions summary" });
    }
  });

  // Get duplicate SKUs
  app.get("/api/exceptions/duplicates", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const duplicates = await storage.getDuplicateSKUs(limit);
      res.json({ duplicates });
    } catch (error: any) {
      console.error("Error getting duplicate SKUs:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get duplicate SKUs" });
    }
  });

  // Get sync errors
  app.get("/api/exceptions/sync-errors", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const errors = await storage.getSyncErrors(limit);
      res.json({ errors });
    } catch (error: any) {
      console.error("Error getting sync errors:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get sync errors" });
    }
  });

  // Get data conflicts
  app.get("/api/exceptions/conflicts", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const conflicts = await storage.getDataConflicts(limit);
      res.json({ conflicts });
    } catch (error: any) {
      console.error("Error getting data conflicts:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get data conflicts" });
    }
  });

  // Clear all exceptions (sync errors and data conflicts)
  app.delete("/api/exceptions/clear", async (req, res) => {
    try {
      const syncErrorsCleared = await storage.clearSyncErrors();
      const conflictsCleared = await storage.clearDataConflicts();
      res.json({
        success: true,
        syncErrorsCleared,
        conflictsCleared,
        message: `Cleared ${syncErrorsCleared} sync errors and ${conflictsCleared} data conflicts`,
      });
    } catch (error: any) {
      console.error("Error clearing exceptions:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to clear exceptions" });
    }
  });

  // ============================================
  // Dashboard Data API
  // ============================================

  app.get("/api/dashboard", async (req, res) => {
    try {
      // Get dismissal timestamps for filtering
      const activityDismissal =
        await storage.getDashboardDismissal("recent_activity");
      const alertsDismissal = await storage.getDashboardDismissal("alerts");

      // Get data sources with sync status
      // Strip heavy config fields immediately - dashboard only needs id/name/type/lastSync/connectionDetails
      const allDataSourcesRaw = await storage.getDataSources();
      const allDataSources = allDataSourcesRaw.map((s: any) => {
        const { lastImportStats, cleaningConfig, validationConfig, expansionConfig, columnMapping, pivotConfig, ...light } = s;
        return light;
      });
      // Batch queries instead of N+1 (3 queries total instead of 63)
      const [fileCounts, itemCounts, recentLogs] = await Promise.all([
        db.select({ dataSourceId: uploadedFiles.dataSourceId, count: count() })
          .from(uploadedFiles)
          .groupBy(uploadedFiles.dataSourceId),
        db.select({ dataSourceId: inventoryItems.dataSourceId, count: count() })
          .from(inventoryItems)
          .groupBy(inventoryItems.dataSourceId),
        db.select({
          dataSourceId: importLogs.dataSourceId,
          status: importLogs.status,
          importType: importLogs.importType,
          itemsImported: importLogs.itemsImported,
          startedAt: importLogs.startedAt,
          errorMessage: importLogs.errorMessage,
        })
          .from(importLogs)
          .orderBy(desc(importLogs.startedAt))
          .limit(200),
      ]);

      // Build lookup maps
      const fileCountMap = new Map(fileCounts.map(r => [r.dataSourceId, r.count]));
      const itemCountMap = new Map(itemCounts.map(r => [r.dataSourceId, r.count]));
      // Deduplicate: keep only the most recent log per data source
      const lastImportMap = new Map<string, any>();
      for (const log of recentLogs) {
        if (log.dataSourceId && !lastImportMap.has(log.dataSourceId)) {
          lastImportMap.set(log.dataSourceId, { status: log.status, importType: log.importType, itemsImported: log.itemsImported, startedAt: log.startedAt, errorMessage: log.errorMessage });
        }
      }

      const dataSources = allDataSources.map((ds) => {
        const lastSync = ds.lastSync ? new Date(ds.lastSync) : null;
        const hoursSinceSync = lastSync
          ? (Date.now() - lastSync.getTime()) / (1000 * 60 * 60)
          : null;

        let status: "active" | "pending" | "stale" | "error" = "active";
        if (!lastSync) {
          status = "pending";
        } else if (hoursSinceSync && hoursSinceSync > 168) {
          status = "stale";
        }

        const isAutoSync =
          ds.type === "email" || (ds.connectionDetails as any)?.autoSync;

        return {
          id: ds.id,
          name: ds.name,
          type: ds.type,
          status,
          isAutoSync,
          lastSync: ds.lastSync,
          hoursSinceSync: hoursSinceSync ? Math.round(hoursSinceSync) : null,
          fileCount: fileCountMap.get(ds.id) || 0,
          itemCount: itemCountMap.get(ds.id) || 0,
          lastImport: lastImportMap.get(ds.id) || null,
        };
      });

      // Get Shopify store status
      const shopifyStores = await storage.getShopifyStores();
      const shopifyStatus =
        shopifyStores.length > 0
          ? {
              connected: shopifyStores[0].status === "connected",
              storeName: shopifyStores[0].name,
              lastSync: shopifyStores[0].lastSync,
            }
          : { connected: false };

      // Get eBay status - check both channelIntegrations and ebayStoreSettings
      const channels = await storage.getChannelIntegrations();
      const ebayChannel = channels.find((c) => c.type === "ebay");

      // Also check ebayStoreSettings for OAuth connection
      let ebayConnected = ebayChannel?.status === "connected";
      if (!ebayConnected && shopifyStores.length > 0) {
        const ebaySettings = await storage.getEbayStoreSettings(
          shopifyStores[0].id,
        );
        // Consider connected if we have valid refresh token
        ebayConnected = !!(
          ebaySettings?.refreshToken && ebaySettings?.clientId
        );
      }

      const ebayStatus = {
        connected: ebayConnected,
        lastSync: ebayChannel?.lastSync || null,
      };

      // Get running sync jobs and last sync results
      const stores = await storage.getShopifyStores();
      let runningJob = null;
      let lastSyncResults = null;
      if (stores.length > 0) {
        const syncLogs = await storage.getShopifySyncLogs(stores[0].id, 5);
        const latestLog = syncLogs[0];
        if (latestLog && latestLog.status === "running") {
          runningJob = {
            type: latestLog.syncType,
            processed: latestLog.itemsProcessed || 0,
            total:
              (latestLog.itemsProcessed || 0) +
              (latestLog.itemsSkipped || 0) +
              (latestLog.itemsFailed || 0) +
              50,
            progress: Math.min(
              95,
              Math.round(
                ((latestLog.itemsProcessed || 0) /
                  (latestLog.itemsProcessed || 1 + 50)) *
                  100,
              ),
            ),
          };
        }

        // Get last completed sync for dashboard display
        // Priority: Find sync log with data source stats (completed > partial)
        // This handles the case where sequential sync creates two logs:
        // - A "completed" main log (no stats)
        // - A "partial" vendor log (has stats)
        let syncForDisplay = null;
        let dataSourceNames: string[] = [];

        // First try: completed sync with data source stats
        const completedSync = syncLogs.find(
          (log) => log.status === "completed",
        );
        if (completedSync) {
          const stats = await storage.getShopifySyncDataSourceStats(
            completedSync.id,
          );
          if (stats.length > 0) {
            syncForDisplay = completedSync;
            for (const stat of stats) {
              if (stat.dataSourceId) {
                const ds = allDataSources.find(
                  (d) => d.id === stat.dataSourceId,
                );
                if (ds) dataSourceNames.push(ds.name);
              }
            }
          }
        }

        // Second try: if completed sync has no stats, look for partial sync with stats
        // (from same time window - within 1 minute of the completed sync)
        if (!syncForDisplay && completedSync) {
          const completedTime = completedSync.completedAt
            ? new Date(completedSync.completedAt).getTime()
            : 0;
          const partialSyncs = syncLogs.filter(
            (log) =>
              log.status === "partial" &&
              log.completedAt &&
              Math.abs(new Date(log.completedAt).getTime() - completedTime) <
                60000, // Within 1 minute
          );

          for (const partialSync of partialSyncs) {
            const stats = await storage.getShopifySyncDataSourceStats(
              partialSync.id,
            );
            if (stats.length > 0) {
              // Use the completed sync's data but get names from partial sync's stats
              syncForDisplay = completedSync;
              for (const stat of stats) {
                if (stat.dataSourceId) {
                  const ds = allDataSources.find(
                    (d) => d.id === stat.dataSourceId,
                  );
                  if (ds && !dataSourceNames.includes(ds.name)) {
                    dataSourceNames.push(ds.name);
                  }
                }
              }
              break;
            }
          }
        }

        // Fallback: use completed sync even without stats
        if (!syncForDisplay && completedSync) {
          syncForDisplay = completedSync;
        }

        if (syncForDisplay) {
          lastSyncResults = {
            id: syncForDisplay.id,
            syncType: syncForDisplay.syncType,
            itemsProcessed: syncForDisplay.itemsProcessed || 0,
            itemsCreated: syncForDisplay.itemsCreated || 0,
            itemsUpdated: syncForDisplay.itemsUpdated || 0,
            itemsDeleted: syncForDisplay.itemsDeleted || 0,
            itemsSkipped: syncForDisplay.itemsSkipped || 0,
            itemsFailed: syncForDisplay.itemsFailed || 0,
            startedAt: syncForDisplay.startedAt,
            completedAt: syncForDisplay.completedAt,
            dataSourceNames:
              dataSourceNames.length > 0 ? dataSourceNames : null,
          };
        }
      }

      // Get recent activity - aggregate from various sources
      const recentActivity: any[] = [];

      // Recent file uploads - get from each data source
      for (const ds of allDataSources.slice(0, 3)) {
        const files = await storage.getFilesByDataSource(ds.id);
        const latestFile = files[0];
        if (latestFile) {
          recentActivity.push({
            id: `upload-${latestFile.id}`,
            type: "import",
            title: `Inventory Import: ${latestFile.fileName}`,
            description: `Imported ${latestFile.rowCount || 0} SKUs from ${ds.name}`,
            timestamp: latestFile.uploadedAt,
            icon: "upload",
          });
        }
      }

      // Recent sync logs
      if (stores.length > 0) {
        const syncLogs = await storage.getShopifySyncLogs(stores[0].id, 3);
        syncLogs
          .filter((log) => log.status !== "running")
          .forEach((log) => {
            recentActivity.push({
              id: `sync-${log.id}`,
              type:
                log.status === "completed"
                  ? "success"
                  : log.status === "failed"
                    ? "error"
                    : "info",
              title:
                log.status === "completed"
                  ? "Shopify Sync Completed"
                  : log.status === "failed"
                    ? "Shopify Sync Failed"
                    : "Shopify Sync",
              description:
                log.status === "completed"
                  ? `Updated ${log.itemsUpdated || 0} products, ${log.itemsCreated || 0} new variants created`
                  : log.errorMessage || "Sync operation",
              timestamp: log.completedAt || log.startedAt,
              icon: log.status === "completed" ? "check" : "error",
            });
          });
      }

      // Sort by timestamp
      recentActivity.sort((a, b) => {
        const timeA = a.timestamp ? new Date(a.timestamp).getTime() : 0;
        const timeB = b.timestamp ? new Date(b.timestamp).getTime() : 0;
        return timeB - timeA;
      });

      // Get alerts
      const alerts: any[] = [];

      // Stale data sources
      dataSources
        .filter((ds) => ds.status === "stale")
        .forEach((ds) => {
          alerts.push({
            id: `stale-${ds.id}`,
            type: "warning",
            severity: "warning",
            title: `Stale Data: ${ds.name}`,
            description: `Last synced ${ds.hoursSinceSync ? Math.round(ds.hoursSinceSync / 24) : "?"} days ago - consider updating`,
            action: "sync",
            actionLabel: "Sync",
          });
        });

      // Unresolved errors
      const unresolvedErrors = await storage.getUnreportedErrors();
      unresolvedErrors.slice(0, 3).forEach((err) => {
        alerts.push({
          id: `error-${err.id}`,
          type: "error",
          severity: err.severity || "error",
          title: `${err.errorType.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase())}`,
          description: err.errorMessage,
          action: "fix",
          actionLabel: "Fix",
        });
      });

      // Get exceptions summary for duplicate alert
      const exceptions = await storage.getExceptionsSummary();
      if (exceptions.duplicates > 0) {
        alerts.push({
          id: "duplicates",
          type: "warning",
          severity: "warning",
          title: `${exceptions.duplicates} Duplicate SKUs Detected`,
          description: "Same SKUs found across data sources",
          action: "review",
          actionLabel: "Review",
        });
      }

      // eBay alerts
      try {
        for (const store of stores) {
          const ebaySettings = await storage.getEbayStoreSettings(store.id);
          if (!ebaySettings) continue;

          // Alert for disconnected eBay (token expired or not connected)
          if (!ebaySettings.isConnected) {
            alerts.push({
              id: `ebay-disconnected-${store.id}`,
              type: "error",
              severity: "error",
              title: "eBay Disconnected",
              description: `eBay account needs to be reconnected for ${store.name}`,
              action: "connect",
              actionLabel: "Connect",
            });
          } else {
            // Get recent eBay listing errors (last 24 hours)
            const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
            const activities = await storage.getEbayActivityLog(store.id, {
              since: yesterday,
            });
            const errorActivities = activities.filter(
              (a) => a.activityType === "listing_error",
            );

            if (errorActivities.length > 0) {
              alerts.push({
                id: `ebay-errors-${store.id}`,
                type: "error",
                severity: "error",
                title: `${errorActivities.length} eBay Listing Error${errorActivities.length > 1 ? "s" : ""}`,
                description: "Recent listing failures require attention",
                action: "review",
                actionLabel: "Review",
              });
            }

            // Get queue items with error status
            const queueItems = await storage.getEbayListingQueue(store.id);
            const errorQueueItems = queueItems.filter(
              (q) => q.status === "error",
            );

            if (errorQueueItems.length > 0) {
              alerts.push({
                id: `ebay-queue-errors-${store.id}`,
                type: "warning",
                severity: "warning",
                title: `${errorQueueItems.length} eBay Queue Error${errorQueueItems.length > 1 ? "s" : ""}`,
                description: "Items in queue failed to list",
                action: "review",
                actionLabel: "Review",
              });
            }
          }
        }
      } catch (e) {
        console.log("[Dashboard] Could not fetch eBay alerts:", e);
      }

      // Get AI insights data
      const colorMappings = await storage.getColorMappings();
      const todayStart = new Date();
      todayStart.setHours(0, 0, 0, 0);

      // Count color mappings (simplified - show total since we don't have createdAt)
      const recentMappings = colorMappings.slice(0, 10);

      // Get product cache stats for metrics
      let productStats = {
        total: 0,
        pending: 0,
        analyzed: 0,
        enriched: 0,
        ready: 0,
        needsReview: 0,
      };
      let imagesInQueue = 0;
      let avgConfidence = 0;
      if (stores.length > 0) {
        const rawStats = await storage.getProductCacheStats(stores[0].id);
        // Convert all values to numbers (SQL may return strings)
        productStats = {
          total: Number(rawStats.total) || 0,
          pending: Number(rawStats.pending) || 0,
          analyzed: Number(rawStats.analyzed) || 0,
          enriched: Number(rawStats.enriched) || 0,
          ready: Number(rawStats.ready) || 0,
          needsReview: Number(rawStats.needsReview) || 0,
        };
        imagesInQueue = productStats.pending;

        // Calculate average confidence from recent mappings
        const confidenceMappings = colorMappings.filter(
          (m) => (m as any).confidence,
        );
        if (confidenceMappings.length > 0) {
          avgConfidence = Math.round(
            confidenceMappings.reduce(
              (sum, m) => sum + ((m as any).confidence || 0) * 100,
              0,
            ) / confidenceMappings.length,
          );
        }
      }

      // Calculate Shopify coverage (ready + enriched = synced products)
      const syncedProducts = productStats.ready + productStats.enriched;
      const totalProducts = productStats.total;
      const coveragePercent =
        totalProducts > 0
          ? Math.round((syncedProducts / totalProducts) * 100)
          : 0;

      // Pending AI tasks: pending analysis + needs review
      const pendingAiTasks = {
        total: productStats.pending + productStats.needsReview,
        images: productStats.pending,
        descriptions: productStats.needsReview,
      };

      // Import health stats
      const importHealthRows = await db
        .select({
          status: importLogs.status,
          cnt: count(),
        })
        .from(importLogs)
        .groupBy(importLogs.status);
      const importHealthMap: Record<string, number> = {};
      importHealthRows.forEach((r) => {
        importHealthMap[r.status] = Number(r.cnt);
      });
      const totalImportRuns = Object.values(importHealthMap).reduce(
        (a, b) => a + b,
        0,
      );
      const successfulImportRuns = importHealthMap["success"] || 0;
      const importSuccessRate =
        totalImportRuns > 0
          ? Math.round((successfulImportRuns / totalImportRuns) * 100)
          : 100;

      // How many data sources have items vs total
      const dsWithItems = dataSourcesWithStats.filter(
        (ds: any) => Number(ds.itemCount) > 0,
      ).length;
      const dsWithImportLog = dataSourcesWithStats.filter(
        (ds: any) => ds.lastImport !== null,
      ).length;

      // Total inventory items count
      const [totalItemsResult] = await db
        .select({ count: count() })
        .from(inventoryItems);
      const totalInventoryItems = Number(totalItemsResult?.count || 0);

      // Recent AI actions (color corrections)
      const recentAiActions = colorMappings.slice(0, 5).map((m) => ({
        from: m.badColor,
        to: m.goodColor,
        confidence: (m as any).confidence
          ? Math.round((m as any).confidence * 100)
          : 85,
      }));

      // Filter out dismissed items
      const filteredActivity = activityDismissal?.dismissedAt
        ? recentActivity.filter((item) => {
            const itemTime = item.timestamp
              ? new Date(item.timestamp).getTime()
              : 0;
            return (
              itemTime > new Date(activityDismissal.dismissedAt!).getTime()
            );
          })
        : recentActivity;

      // For alerts: Clear only if no new activity has happened since dismissal
      // This allows new alerts to appear after new syncs/imports
      let filteredAlerts = alerts;
      if (alertsDismissal?.dismissedAt) {
        const dismissTime = new Date(alertsDismissal.dismissedAt).getTime();
        // Check if any recent activity happened after dismissal
        const hasNewActivity = recentActivity.some((item) => {
          const itemTime = item.timestamp
            ? new Date(item.timestamp).getTime()
            : 0;
          return itemTime > dismissTime;
        });
        // Only hide alerts if no new activity since dismissal
        if (!hasNewActivity) {
          filteredAlerts = [];
        }
      }

      res.json({
        // Key Metrics
        metrics: {
          totalProducts,
          syncedProducts,
          coveragePercent,
          pendingAiTasks,
        },
        syncStatus: {
          dataSources,
          shopify: shopifyStatus,
          ebay: ebayStatus,
          runningJob,
        },
        lastSyncResults,
        recentActivity: filteredActivity.slice(0, 5),
        alerts: filteredAlerts.slice(0, 5),
        aiInsights: {
          colorsFixedToday: recentMappings.length,
          totalColorMappings: colorMappings.length,
          imagesInQueue,
          avgConfidence: avgConfidence || 94,
          recentActions: recentAiActions,
        },
        importHealth: {
          totalImportRuns: totalImportRuns,
          successfulRuns: successfulImportRuns,
          failedRuns: importHealthMap["failed"] || 0,
          successRate: importSuccessRate,
          dataSourcesWithItems: dsWithItems,
          dataSourcesImported: dsWithImportLog,
          totalDataSources: allDataSources.length,
        },
        quickStats: {
          totalInventoryItems,
          totalDataSources: allDataSources.length,
        },
      });
    } catch (error: any) {
      console.error("Error getting dashboard data:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get dashboard data" });
    }
  });

  // Clear dashboard recent activity
  app.delete("/api/dashboard/recent-activity", async (req, res) => {
    try {
      await storage.dismissDashboardSection("recent_activity");
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error clearing recent activity:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to clear recent activity" });
    }
  });

  // Clear dashboard alerts
  app.delete("/api/dashboard/alerts", async (req, res) => {
    try {
      await storage.dismissDashboardSection("alerts");
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error clearing alerts:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to clear alerts" });
    }
  });

  // Cross-data-source cleanup - Preview orphaned variants
  app.post("/api/shopify/stores/:id/cleanup/preview", async (req, res) => {
    try {
      const storeId = req.params.id;
      const { dataSourceIds } = req.body;

      if (!Array.isArray(dataSourceIds) || dataSourceIds.length === 0) {
        return res
          .status(400)
          .json({ error: "dataSourceIds array is required" });
      }

      const { previewCrossDataSourceCleanup } = await import("./shopify");
      const result = await previewCrossDataSourceCleanup(
        storeId,
        dataSourceIds,
      );

      // Convert Map to object for JSON serialization
      const byProductObj: Record<string, number> = {};
      result.byProduct.forEach((count, productId) => {
        byProductObj[productId] = count;
      });

      res.json({
        orphanedVariants: result.orphanedVariants,
        totalCount: result.totalCount,
        byProduct: byProductObj,
        productCount: result.byProduct.size,
      });
    } catch (error: any) {
      console.error("Error previewing cleanup:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to preview cleanup" });
    }
  });

  // Cross-data-source cleanup - Execute deletion
  app.post("/api/shopify/stores/:id/cleanup/execute", async (req, res) => {
    try {
      const storeId = req.params.id;
      const { variantIds, maxDeletions = 100, dryRun = false } = req.body;

      if (!Array.isArray(variantIds) || variantIds.length === 0) {
        return res.status(400).json({ error: "variantIds array is required" });
      }

      const { executeCrossDataSourceCleanup } = await import("./shopify");
      const result = await executeCrossDataSourceCleanup(
        storeId,
        variantIds,
        maxDeletions,
        dryRun,
      );

      res.json(result);
    } catch (error: any) {
      console.error("Error executing cleanup:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to execute cleanup" });
    }
  });

  // Temporary endpoint to download project backup zip
  app.get("/api/download-backup", (req, res) => {
    const fs = require("fs");
    const path = require("path");
    const zipPath = path.join(process.cwd(), "project_backup.zip");

    if (!fs.existsSync(zipPath)) {
      return res.status(404).json({ error: "Backup file not found" });
    }

    res.download(zipPath, "inventoryai_backup.zip", (err) => {
      if (err) {
        console.error("Error downloading backup:", err);
        res.status(500).json({ error: "Failed to download backup" });
      }
    });
  });

  // Download temporary CSV files (variants to delete, etc.)
  app.get("/api/download-csv/:filename", (req, res) => {
    const fs = require("fs");
    const path = require("path");
    const filename = req.params.filename;

    // Only allow specific files for security
    const allowedFiles = [
      "variants_to_delete.csv",
      "jovani_variants_to_delete.csv",
      "all_variants_to_delete.csv",
    ];
    if (!allowedFiles.includes(filename)) {
      return res.status(403).json({ error: "File not allowed" });
    }

    const filePath = path.join("/tmp", filename);
    if (!fs.existsSync(filePath)) {
      // Try public folder
      const publicPath = path.join(process.cwd(), "public", filename);
      if (fs.existsSync(publicPath)) {
        return res.download(publicPath, filename);
      }
      return res.status(404).json({ error: "File not found" });
    }

    res.download(filePath, filename);
  });

  // ============================================
  // SYNC SNAPSHOTS (Per-Data-Source Backups)
  // ============================================

  // Get all sync snapshots for a store (both full-store and per-data-source)
  app.get("/api/shopify/stores/:id/sync-snapshots", async (req, res) => {
    try {
      const storeId = req.params.id;
      const type = req.query.type as string; // 'full' | 'per-data-source' | undefined (all)
      const limit = parseInt(req.query.limit as string) || 50;

      let snapshots;
      if (type === "full") {
        snapshots = await storage.getFullStoreSyncSnapshots(storeId, limit);
      } else if (type === "per-data-source") {
        const allSnapshots = await storage.getSyncSnapshotsByStore(
          storeId,
          limit,
        );
        snapshots = allSnapshots.filter((s) => s.dataSourceId !== null);
      } else {
        snapshots = await storage.getSyncSnapshotsByStore(storeId, limit);
      }

      // Get data source names for display
      const dataSources = await storage.getDataSources();
      const dsMap = new Map(dataSources.map((ds) => [ds.id, ds.name]));

      const enrichedSnapshots = snapshots.map((s) => ({
        id: s.id,
        shopifyStoreId: s.shopifyStoreId,
        dataSourceId: s.dataSourceId,
        dataSourceName: s.dataSourceId
          ? dsMap.get(s.dataSourceId) || "Unknown"
          : null,
        snapshotType: s.dataSourceId ? "per-data-source" : "full-store",
        isCurrentSnapshot: s.isCurrentSnapshot,
        isPreviousSnapshot: s.isPreviousSnapshot,
        totalVariants: s.totalVariants,
        totalProducts: s.totalProducts,
        createdAt: s.createdAt,
      }));

      res.json(enrichedSnapshots);
    } catch (error: any) {
      console.error("Error fetching sync snapshots:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch sync snapshots" });
    }
  });

  // Get all sync snapshots for a data source
  app.get("/api/data-sources/:id/sync-snapshots", async (req, res) => {
    try {
      const dataSourceId = req.params.id;
      const limit = parseInt(req.query.limit as string) || 10;
      const snapshots = await storage.getSyncSnapshotsByDataSource(
        dataSourceId,
        limit,
      );
      res.json(snapshots);
    } catch (error: any) {
      console.error("Error fetching sync snapshots:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch sync snapshots" });
    }
  });

  // Get a specific sync snapshot with full details
  app.get("/api/sync-snapshots/:id", async (req, res) => {
    try {
      const snapshot = await storage.getSyncSnapshot(req.params.id);
      if (!snapshot) {
        return res.status(404).json({ error: "Snapshot not found" });
      }
      res.json(snapshot);
    } catch (error: any) {
      console.error("Error fetching sync snapshot:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch sync snapshot" });
    }
  });

  // Get snapshot summary (without full variant data for listing)
  app.get("/api/sync-snapshots/:id/summary", async (req, res) => {
    try {
      const snapshot = await storage.getSyncSnapshot(req.params.id);
      if (!snapshot) {
        return res.status(404).json({ error: "Snapshot not found" });
      }

      // Use lightweight aggregate queries instead of loading all variants
      const variantCount = await storage.getSyncSnapshotVariantCount(
        req.params.id,
      );
      let totalStock = 0;
      let sampleVariants: any[] = [];

      if (variantCount > 0) {
        // Use aggregate query for total stock (efficient for large snapshots)
        totalStock = await storage.getSyncSnapshotTotalStock(req.params.id);
        // Get only sample variants (limited fetch)
        sampleVariants = await storage.getSyncSnapshotSampleVariants(
          req.params.id,
          5,
        );
      } else if (snapshot.variantData && snapshot.variantData.length > 0) {
        // Fallback to JSONB column for legacy snapshots
        totalStock = snapshot.variantData.reduce(
          (sum, v) => sum + (v.inventoryQuantity || 0),
          0,
        );
        sampleVariants = snapshot.variantData.slice(0, 5);
      }

      // Return summary with correct field names for UI
      const { variantData, ...summary } = snapshot;
      res.json({
        ...summary,
        totalVariants:
          snapshot.totalVariants || variantCount || variantData?.length || 0,
        uniqueProducts: snapshot.totalProducts || 0,
        totalStock,
        variantCount: variantCount || variantData?.length || 0,
        sampleVariants,
      });
    } catch (error: any) {
      console.error("Error fetching sync snapshot summary:", error);
      res.status(500).json({
        error: error.message || "Failed to fetch sync snapshot summary",
      });
    }
  });

  // Restore from a sync snapshot (preview mode)
  app.post("/api/sync-snapshots/:id/restore-preview", async (req, res) => {
    try {
      const snapshot = await storage.getSyncSnapshot(req.params.id);
      if (!snapshot) {
        return res.status(404).json({ error: "Snapshot not found" });
      }

      if (!snapshot.variantData || snapshot.variantData.length === 0) {
        return res
          .status(400)
          .json({ error: "Snapshot contains no variant data" });
      }

      // Get current state from variant cache to compare
      const skus = snapshot.variantData
        .map((v) => v.sku?.toLowerCase())
        .filter(Boolean) as string[];
      const currentVariants = await storage.getVariantCacheBySKUs(
        snapshot.shopifyStoreId!,
        skus,
      );

      // Build comparison
      const changes: Array<{
        sku: string;
        field: string;
        currentValue: string | number | null;
        snapshotValue: string | number | null;
      }> = [];

      const currentByVariantId = new Map(currentVariants.map((v) => [v.id, v]));

      for (const snapVar of snapshot.variantData) {
        const current = currentByVariantId.get(snapVar.variantId);
        if (!current) {
          changes.push({
            sku: snapVar.sku,
            field: "variant",
            currentValue: "missing",
            snapshotValue: "exists",
          });
          continue;
        }

        // Compare price
        if (current.price !== snapVar.price) {
          changes.push({
            sku: snapVar.sku,
            field: "price",
            currentValue: current.price,
            snapshotValue: snapVar.price,
          });
        }
        // Compare compareAtPrice
        if (current.compareAtPrice !== snapVar.compareAtPrice) {
          changes.push({
            sku: snapVar.sku,
            field: "compareAtPrice",
            currentValue: current.compareAtPrice,
            snapshotValue: snapVar.compareAtPrice,
          });
        }
        // Compare stock
        if (current.stock !== snapVar.inventoryQuantity) {
          changes.push({
            sku: snapVar.sku,
            field: "stock",
            currentValue: current.stock,
            snapshotValue: snapVar.inventoryQuantity,
          });
        }
        // Compare stockInfo
        if (current.stockInfoMetafield !== snapVar.stockInfo) {
          changes.push({
            sku: snapVar.sku,
            field: "stockInfo",
            currentValue: current.stockInfoMetafield,
            snapshotValue: snapVar.stockInfo,
          });
        }
      }

      res.json({
        snapshotId: snapshot.id,
        snapshotDate: snapshot.createdAt,
        totalVariants: snapshot.variantData.length,
        changesRequired: changes.length,
        changes: changes.slice(0, 100), // Limit to first 100 for preview
        hasMoreChanges: changes.length > 100,
      });
    } catch (error: any) {
      console.error("Error previewing restore:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to preview restore" });
    }
  });

  // Delete a sync snapshot
  app.delete("/api/sync-snapshots/:id", async (req, res) => {
    try {
      const snapshot = await storage.getSyncSnapshot(req.params.id);
      if (!snapshot) {
        return res.status(404).json({ error: "Snapshot not found" });
      }

      await storage.deleteSyncSnapshot(req.params.id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error deleting sync snapshot:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to delete sync snapshot" });
    }
  });

  // ============================================
  // STORE BACKUPS (Full Shopify Store Backups)
  // ============================================

  // Get all store backups for a store
  app.get("/api/shopify/stores/:id/backups", async (req, res) => {
    try {
      const storeId = req.params.id;
      const backups = await storage.getStoreBackups(storeId);
      // Return summaries without full product data
      const summaries = backups.map((b) => {
        const { productData, ...summary } = b;
        return summary;
      });
      res.json(summaries);
    } catch (error: any) {
      console.error("Error fetching store backups:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch store backups" });
    }
  });

  // Create a new store backup with FULL product data from Shopify
  // Optional body param: limit - number of products to backup (for testing)
  // Downloads all media (images/videos) to local storage for true disaster recovery
  app.post("/api/shopify/stores/:id/backups", async (req, res) => {
    try {
      const storeId = req.params.id;
      const { name, limit } = req.body;

      const store = await storage.getShopifyStore(storeId);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      console.log(
        `[Store Backup] Creating new full backup for ${store.name}...`,
      );

      // Create Shopify service and fetch COMPLETE product data
      const { createShopifyService } = await import("./shopify");
      const client = createShopifyService(store);

      const productLimit = limit ? parseInt(limit) : 5; // Default to 5 for testing
      const productData = await client.getProductsForFullBackup(productLimit);

      const totalVariants = productData.reduce(
        (sum: number, p: any) => sum + (p.variants?.length || 0),
        0,
      );
      const totalImages = productData.reduce(
        (sum: number, p: any) => sum + (p.images?.length || 0),
        0,
      );
      const totalVideos = productData.reduce(
        (sum: number, p: any) => sum + (p.videos?.length || 0),
        0,
      );

      // Create initial backup record
      const backup = await storage.createStoreBackup({
        shopifyStoreId: storeId,
        name: name || `Full Store Backup - ${new Date().toLocaleDateString()}`,
        description: `Backup with ${productData.length} products, downloading media...`,
        backupType: "manual",
        scheduleEnabled: false,
        totalProducts: productData.length,
        totalVariants: totalVariants,
        backupSizeBytes: 0,
        productData: [] as any,
        lastBackupAt: new Date(),
        status: "in_progress",
      });

      const backupId = backup.id;
      console.log(
        `[Store Backup] Created backup ${backupId}, downloading media for ${productData.length} products...`,
      );

      // Download media for each product
      const { downloadProductMedia, getMediaStoragePath } = await import(
        "./mediaBackup"
      );
      let totalMediaFiles = 0;
      let totalMediaBytes = 0;
      let mediaSuccessCount = 0;
      let mediaFailedCount = 0;

      for (let i = 0; i < productData.length; i++) {
        const product = productData[i];
        console.log(
          `[Store Backup] Downloading media for product ${i + 1}/${productData.length}: ${product.title}`,
        );

        try {
          const result = await downloadProductMedia(
            backupId,
            product.id,
            product.images || [],
            product.videos || [],
          );

          // Update product data with local paths
          product.images = result.downloadedImages;
          product.videos = result.downloadedVideos;

          totalMediaFiles += result.successCount;
          totalMediaBytes += result.totalBytes;
          mediaSuccessCount += result.successCount;
          mediaFailedCount += result.failedCount;
        } catch (mediaError: any) {
          console.error(
            `[Store Backup] Media download failed for ${product.title}:`,
            mediaError.message,
          );
          mediaFailedCount +=
            (product.images?.length || 0) + (product.videos?.length || 0);
        }
      }

      const mediaStoragePath = await getMediaStoragePath(backupId);
      const backupSizeBytes = JSON.stringify(productData).length;

      // Update backup with complete data and media info
      const updatedBackup = await storage.updateStoreBackup(backupId, {
        productData: productData as any,
        backupSizeBytes: backupSizeBytes,
        mediaStoragePath: mediaStoragePath,
        totalMediaFiles: totalMediaFiles,
        mediaSizeBytes: totalMediaBytes,
        status: "completed",
        description: `Complete backup with ${productData.length} products, ${totalMediaFiles} media files (${(totalMediaBytes / 1024 / 1024).toFixed(2)} MB)`,
      });

      console.log(
        `[Store Backup] Completed backup with ${productData.length} products, ${totalVariants} variants, ${totalImages} images, ${totalVideos} videos`,
      );
      console.log(
        `[Store Backup] Media: ${mediaSuccessCount} downloaded, ${mediaFailedCount} failed, ${(totalMediaBytes / 1024 / 1024).toFixed(2)} MB total`,
      );

      // Return summary without full product data
      const { productData: _, ...summary } = updatedBackup || backup;
      res.json(summary);
    } catch (error: any) {
      console.error("Error creating store backup:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to create store backup" });
    }
  });

  // Get a specific store backup with full product data
  app.get("/api/store-backups/:id", async (req, res) => {
    try {
      const backup = await storage.getStoreBackup(req.params.id);
      if (!backup) {
        return res.status(404).json({ error: "Backup not found" });
      }
      res.json(backup);
    } catch (error: any) {
      console.error("Error fetching store backup:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch store backup" });
    }
  });

  // Get list of vendors from a backup (for restore filtering)
  app.get("/api/store-backups/:id/vendors", async (req, res) => {
    try {
      const backup = await storage.getStoreBackup(req.params.id);
      if (!backup) {
        return res.status(404).json({ error: "Backup not found" });
      }

      const productData = (backup.productData as any[]) || [];
      const vendorCounts = new Map<string, number>();

      for (const product of productData) {
        const vendor = product.vendor || "Unknown";
        vendorCounts.set(vendor, (vendorCounts.get(vendor) || 0) + 1);
      }

      const vendors = Array.from(vendorCounts.entries())
        .map(([name, count]) => ({
          name,
          productCount: count,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));

      res.json({ vendors, totalProducts: productData.length });
    } catch (error: any) {
      console.error("Error fetching backup vendors:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch backup vendors" });
    }
  });

  // Preview restore from backup (filtered by vendors) - now detects missing products
  app.post("/api/store-backups/:id/restore-preview", async (req, res) => {
    try {
      const backup = await storage.getStoreBackup(req.params.id);
      if (!backup) {
        return res.status(404).json({ error: "Backup not found" });
      }

      const { vendors, checkExistence } = req.body; // Array of vendor names to restore
      const productData = (backup.productData as any[]) || [];

      // Filter products by selected vendors
      const selectedProducts =
        vendors && vendors.length > 0
          ? productData.filter((p: any) => vendors.includes(p.vendor))
          : productData;

      // If checkExistence is true, check which products exist in Shopify using batch method
      let existingProductIds: string[] = [];
      let missingProductIds: string[] = [];

      if (checkExistence && selectedProducts.length > 0) {
        const store = await storage.getShopifyStore(backup.shopifyStoreId!);
        if (store) {
          const { createShopifyService } = await import("./shopify");
          const shopifyService = createShopifyService(store);

          console.log(
            `[Restore Preview] Batch checking existence of ${selectedProducts.length} products...`,
          );

          // Build title map for matching recreated products (same title but new ID)
          const productIds = selectedProducts.map((p: any) => p.id);
          const productTitles = new Map<string, string>();
          for (const p of selectedProducts) {
            productTitles.set(p.id, p.title);
          }

          // Use batch check method - fetches all product IDs efficiently, also matches by title
          const existenceMap = await shopifyService.batchCheckProductsExist(
            productIds,
            productTitles,
          );

          for (const product of selectedProducts) {
            if (existenceMap.get(product.id)) {
              existingProductIds.push(product.id);
            } else {
              missingProductIds.push(product.id);
            }
          }

          console.log(
            `[Restore Preview] ${existingProductIds.length} exist, ${missingProductIds.length} missing`,
          );
        }
      }

      const existingProducts = selectedProducts.filter((p: any) =>
        existingProductIds.includes(p.id),
      );
      const missingProducts = selectedProducts.filter((p: any) =>
        missingProductIds.includes(p.id),
      );

      // Calculate stats - return ALL products for the scrollable list
      const stats = {
        totalProducts: selectedProducts.length,
        totalVariants: selectedProducts.reduce(
          (sum: number, p: any) => sum + (p.variants?.length || 0),
          0,
        ),
        totalImages: selectedProducts.reduce(
          (sum: number, p: any) => sum + (p.images?.length || 0),
          0,
        ),
        totalMetafields: selectedProducts.reduce(
          (sum: number, p: any) => sum + (p.metafields?.length || 0),
          0,
        ),
        selectedVendors:
          vendors || Array.from(new Set(productData.map((p: any) => p.vendor))),
        productCount: selectedProducts.length,
        // Products that exist in Shopify (will be updated)
        existingCount: existingProductIds.length,
        // Products missing from Shopify (will be created)
        missingCount: missingProductIds.length,
        // All products for display
        sampleProducts: selectedProducts.map((p: any) => ({
          id: p.id,
          title: p.title,
          vendor: p.vendor,
          variantCount: p.variants?.length || 0,
          imageCount: p.images?.length || 0,
          exists: existingProductIds.includes(p.id),
          missing: missingProductIds.includes(p.id),
        })),
        // Separate lists for UI display
        productsToUpdate: existingProducts.map((p: any) => ({
          id: p.id,
          title: p.title,
          variantCount: p.variants?.length || 0,
        })),
        productsToCreate: missingProducts.map((p: any) => ({
          id: p.id,
          title: p.title,
          variantCount: p.variants?.length || 0,
          imageCount: p.images?.length || 0,
        })),
      };

      res.json(stats);
    } catch (error: any) {
      console.error("Error generating restore preview:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to generate restore preview" });
    }
  });

  // Execute restore from backup to Shopify (updates existing + creates missing products)
  app.post("/api/store-backups/:id/restore-execute", async (req, res) => {
    try {
      const backup = await storage.getStoreBackup(req.params.id);
      if (!backup) {
        return res.status(404).json({ error: "Backup not found" });
      }

      const store = await storage.getShopifyStore(backup.shopifyStoreId!);
      if (!store) {
        return res.status(404).json({ error: "Shopify store not found" });
      }

      const { createShopifyService } = await import("./shopify");
      const shopifyService = createShopifyService(store);

      const { vendors, productIds } = req.body; // Array of vendor names and/or product IDs to restore
      const productData = (backup.productData as any[]) || [];

      // Filter products by selected vendors first
      let selectedProducts =
        vendors && vendors.length > 0
          ? productData.filter((p: any) => vendors.includes(p.vendor))
          : productData;

      // Then filter by specific product IDs if provided
      if (productIds && productIds.length > 0) {
        const productIdSet = new Set(productIds);
        selectedProducts = selectedProducts.filter((p: any) =>
          productIdSet.has(p.id),
        );
      }

      if (selectedProducts.length === 0) {
        return res.status(400).json({ error: "No products to restore" });
      }

      console.log(
        `[Store Backup] Starting restore of ${selectedProducts.length} products to Shopify (with create support)...`,
      );

      // Execute the restore with create support for missing products
      const results = await shopifyService.restoreProductsFromBackupWithCreate(
        selectedProducts,
        (current: number, total: number, title: string, action: string) => {
          console.log(
            `[Store Backup] ${action === "creating" ? "Creating" : "Updating"} ${current}/${total}: ${title}`,
          );
        },
        store.primaryLocationId || undefined,
      );

      console.log(
        `[Store Backup] Restore complete: ${results.updatedCount} updated, ${results.createdCount} created, ${results.failedCount} failed`,
      );

      res.json({
        success: true,
        message: `Restored ${results.successCount} products to Shopify (${results.updatedCount} updated, ${results.createdCount} created)`,
        ...results,
      });
    } catch (error: any) {
      console.error("Error executing restore:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to execute restore" });
    }
  });

  // Update store backup settings
  app.patch("/api/store-backups/:id", async (req, res) => {
    try {
      const backup = await storage.getStoreBackup(req.params.id);
      if (!backup) {
        return res.status(404).json({ error: "Backup not found" });
      }

      const {
        name,
        description,
        scheduleFrequency,
        scheduleTime,
        scheduleDayOfWeek,
        scheduleEnabled,
        backupMode,
      } = req.body;

      const updated = await storage.updateStoreBackup(req.params.id, {
        name: name !== undefined ? name : backup.name,
        description:
          description !== undefined ? description : backup.description,
        scheduleFrequency:
          scheduleFrequency !== undefined
            ? scheduleFrequency
            : backup.scheduleFrequency,
        scheduleTime:
          scheduleTime !== undefined ? scheduleTime : backup.scheduleTime,
        scheduleDayOfWeek:
          scheduleDayOfWeek !== undefined
            ? scheduleDayOfWeek
            : backup.scheduleDayOfWeek,
        scheduleEnabled:
          scheduleEnabled !== undefined
            ? scheduleEnabled
            : backup.scheduleEnabled,
        backupMode:
          backupMode !== undefined ? backupMode : backup.backupMode || "full",
        backupType: scheduleEnabled ? "scheduled" : "manual",
      });

      res.json(updated);
    } catch (error: any) {
      console.error("Error updating store backup:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to update store backup" });
    }
  });

  // Refresh store backup (capture current Shopify state with FULL product data)
  // Optional query param: ?limit=50 to create a test backup with limited products
  app.post("/api/store-backups/:id/refresh", async (req, res) => {
    try {
      const backup = await storage.getStoreBackup(req.params.id);
      if (!backup) {
        return res.status(404).json({ error: "Backup not found" });
      }

      // Check for product limit (for test backups)
      const productLimit = req.query.limit
        ? parseInt(req.query.limit as string)
        : undefined;

      // Get store credentials
      const store = await storage.getShopifyStore(backup.shopifyStoreId!);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      console.log(
        `[Store Backup] Starting full backup refresh for ${store.name}...`,
      );

      // Create Shopify service and fetch COMPLETE product data
      const { createShopifyService } = await import("./shopify");
      const client = createShopifyService(store);

      // Fetch all products with full data (descriptions, images, metafields, collections, etc.)
      const productData = await client.getProductsForFullBackup(productLimit);

      const totalVariants = productData.reduce(
        (sum: number, p: any) => sum + (p.variants?.length || 0),
        0,
      );
      const totalImages = productData.reduce(
        (sum: number, p: any) => sum + (p.images?.length || 0),
        0,
      );
      const totalMetafields = productData.reduce(
        (sum: number, p: any) => sum + (p.metafields?.length || 0),
        0,
      );
      const backupSizeBytes = JSON.stringify(productData).length;

      // Extract unique vendors for restore filtering
      const vendors = Array.from(
        new Set(productData.map((p: any) => p.vendor).filter(Boolean)),
      );

      console.log(
        `[Store Backup] Captured ${productData.length} products, ${totalVariants} variants, ${totalImages} images, ${totalMetafields} metafields`,
      );

      const updated = await storage.updateStoreBackup(backup.id, {
        productData: productData as any,
        totalProducts: productData.length,
        totalVariants: totalVariants,
        backupSizeBytes: backupSizeBytes,
        lastBackupAt: new Date(),
      });

      res.json({
        success: true,
        totalProducts: productData.length,
        totalVariants: totalVariants,
        totalImages: totalImages,
        totalMetafields: totalMetafields,
        vendors: vendors,
        backupSizeBytes: backupSizeBytes,
      });
    } catch (error: any) {
      console.error("Error refreshing store backup:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to refresh store backup" });
    }
  });

  // Incremental refresh - only fetch changed products and detect deletions
  app.post("/api/store-backups/:id/refresh-incremental", async (req, res) => {
    try {
      const backup = await storage.getStoreBackup(req.params.id);
      if (!backup) {
        return res.status(404).json({ error: "Backup not found" });
      }

      if (!backup.lastBackupAt) {
        return res.status(400).json({
          error: "No previous backup found. Please run a full backup first.",
          needsFullBackup: true,
        });
      }

      const store = await storage.getShopifyStore(backup.shopifyStoreId!);
      if (!store) {
        return res.status(404).json({ error: "Store not found" });
      }

      console.log(
        `[Incremental Backup] Starting incremental backup for ${store.name}...`,
      );
      console.log(`[Incremental Backup] Last backup: ${backup.lastBackupAt}`);

      const { createShopifyService } = await import("./shopify");
      const client = createShopifyService(store);

      // Step 1: Fetch products updated since last backup
      const updatedProducts = await client.getProductsUpdatedSince(
        backup.lastBackupAt,
      );

      // Step 2: Fetch all current product IDs for deletion detection
      const currentProductIds = await client.getAllProductIds();
      const currentProductIdSet = new Set(currentProductIds);

      // Step 3: Merge changes into existing backup
      const existingProducts = (backup.productData as any[]) || [];
      const existingProductMap = new Map(
        existingProducts.map((p: any) => [p.id, p]),
      );

      // Apply updates - replace or add updated products
      let updatedCount = 0;
      let addedCount = 0;
      for (const product of updatedProducts) {
        if (existingProductMap.has(product.id)) {
          updatedCount++;
        } else {
          addedCount++;
        }
        existingProductMap.set(product.id, product);
      }

      // Step 4: Detect and remove deleted products
      let deletedCount = 0;
      const deletedProducts: string[] = [];
      for (const [productId, product] of existingProductMap.entries()) {
        if (!currentProductIdSet.has(productId)) {
          deletedProducts.push((product as any).title || productId);
          existingProductMap.delete(productId);
          deletedCount++;
        }
      }

      // Convert back to array
      const mergedProducts = Array.from(existingProductMap.values());
      const totalVariants = mergedProducts.reduce(
        (sum: number, p: any) => sum + (p.variants?.length || 0),
        0,
      );
      const backupSizeBytes = JSON.stringify(mergedProducts).length;

      console.log(
        `[Incremental Backup] Results: ${updatedCount} updated, ${addedCount} added, ${deletedCount} deleted`,
      );

      // Save updated backup
      await storage.updateStoreBackup(backup.id, {
        productData: mergedProducts as any,
        totalProducts: mergedProducts.length,
        totalVariants: totalVariants,
        backupSizeBytes: backupSizeBytes,
        lastBackupAt: new Date(),
      });

      res.json({
        success: true,
        type: "incremental",
        updatedProducts: updatedCount,
        addedProducts: addedCount,
        deletedProducts: deletedCount,
        deletedProductTitles: deletedProducts.slice(0, 10), // Show first 10
        totalProducts: mergedProducts.length,
        totalVariants: totalVariants,
        backupSizeBytes: backupSizeBytes,
        apiCallsSaved: `~${Math.round(((existingProducts.length - updatedProducts.length - 1) / 10) * 10)}`, // Rough estimate
      });
    } catch (error: any) {
      console.error("Error in incremental backup:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to run incremental backup" });
    }
  });

  // Delete a store backup (including media files)
  app.delete("/api/store-backups/:id", async (req, res) => {
    try {
      const backup = await storage.getStoreBackup(req.params.id);
      if (!backup) {
        return res.status(404).json({ error: "Backup not found" });
      }

      // Delete media files from storage first
      const { deleteBackupMedia } = await import("./mediaBackup");
      await deleteBackupMedia(req.params.id);

      // Then delete database record
      await storage.deleteStoreBackup(req.params.id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error deleting store backup:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to delete store backup" });
    }
  });

  // Analyze size chart image with AI
  app.post(
    "/api/ebay/analyze-size-chart",
    upload.single("sizeChart"),
    async (req, res) => {
      try {
        if (!req.file) {
          return res.status(400).json({ error: "No image file provided" });
        }

        const base64Image = req.file.buffer.toString("base64");
        const isDark = req.body.isDark === "true";

        const { analyzeSizeChartImage, generateSizeChartHtml } = await import(
          "./openai"
        );
        const sizeData = await analyzeSizeChartImage(base64Image);
        const htmlTable = generateSizeChartHtml(sizeData, isDark);

        res.json({
          success: true,
          data: sizeData,
          html: htmlTable,
        });
      } catch (error: any) {
        console.error("Error analyzing size chart:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to analyze size chart" });
      }
    },
  );

  // Upload size chart image and save to vendor template
  app.post(
    "/api/ebay/vendor-templates/:vendor/size-chart",
    upload.single("sizeChart"),
    async (req, res) => {
      try {
        const storeId = req.body.storeId;
        const vendor = decodeURIComponent(req.params.vendor);
        const analyzeWithAI = req.body.analyzeWithAI === "true";
        const isDark = req.body.isDark !== "false";

        if (!storeId) {
          return res.status(400).json({ error: "storeId is required" });
        }
        if (!req.file) {
          return res.status(400).json({ error: "No image file provided" });
        }

        const base64Image = req.file.buffer.toString("base64");
        const mimeType = req.file.mimetype || "image/png";

        // Create data URL for storage (or could upload to external storage)
        const sizeChartUrl = `data:${mimeType};base64,${base64Image}`;

        let sizeChartHtml = null;
        if (analyzeWithAI) {
          try {
            const { analyzeSizeChartImage, generateSizeChartHtml } =
              await import("./openai");
            const sizeData = await analyzeSizeChartImage(base64Image);
            sizeChartHtml = generateSizeChartHtml(sizeData, isDark);
          } catch (aiError) {
            console.error("AI analysis failed, storing image only:", aiError);
          }
        }

        // Check if template exists
        const [existing] = await db
          .select()
          .from(ebayVendorTemplates)
          .where(
            and(
              eq(ebayVendorTemplates.storeId, storeId),
              eq(ebayVendorTemplates.vendor, vendor),
            ),
          );

        let result;
        if (existing) {
          [result] = await db
            .update(ebayVendorTemplates)
            .set({
              sizeChartUrl,
              sizeChartHtml: sizeChartHtml || existing.sizeChartHtml,
              updatedAt: new Date(),
            })
            .where(eq(ebayVendorTemplates.id, existing.id))
            .returning();
        } else {
          [result] = await db
            .insert(ebayVendorTemplates)
            .values({
              storeId,
              vendor,
              sizeChartUrl,
              sizeChartHtml,
            })
            .returning();
        }

        res.json({
          success: true,
          template: result,
          aiAnalyzed: !!sizeChartHtml,
        });
      } catch (error: any) {
        console.error("Error uploading size chart:", error);
        res
          .status(500)
          .json({ error: error.message || "Failed to upload size chart" });
      }
    },
  );

  // ==========================================
  // EBAY STORE SETTINGS API
  // ==========================================

  // Helper function to get the default Shopify store ID for eBay integration
  async function getDefaultShopifyStoreId(): Promise<string | null> {
    const stores = await storage.getShopifyStores();
    const connectedStore = stores.find((s) => s.status === "connected");
    return connectedStore?.id || stores[0]?.id || null;
  }

  // Get eBay store settings
  app.get("/api/ebay/settings", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const settings = await storage.getEbayStoreSettings(storeId);

      // Return empty settings object if none exist
      if (!settings) {
        return res.json({
          shopifyStoreId: storeId,
          environment: "sandbox",
          marketplace: "EBAY_US",
          autoListNewProducts: false,
          autoListRestocked: false,
          autoSyncStock: true,
          autoEndWhenSoldOut: true,
          sizeExpansionEnabled: false,
          sizeExpansionUp: 1,
          sizeExpansionDown: 1,
          defaultCondition: "1000",
          defaultListingFormat: "fixed_price",
          defaultListingDuration: "GTC",
          handlingTime: 3,
          bestOfferEnabled: false,
          isConnected: false,
        });
      }

      // Mask sensitive fields for response
      res.json({
        ...settings,
        clientSecret: settings.clientSecret ? "••••••••" : null,
        refreshToken: settings.refreshToken ? "••••••••" : null,
        accessToken: settings.accessToken ? "••••••••" : null,
      });
    } catch (error: any) {
      console.error("Error getting eBay settings:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get eBay settings" });
    }
  });

  // Get eBay business policies
  app.get("/api/ebay/policies", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { getBusinessPolicies } = await import("./ebayApi");
      const result = await getBusinessPolicies(storeId);

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      res.json(result.policies);
    } catch (error: any) {
      console.error("Error getting eBay policies:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get eBay policies" });
    }
  });

  // Get eBay aspect mappings
  app.get("/api/ebay/aspect-mappings", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const categoryId = req.query.categoryId as string | undefined;
      const mappings = await storage.getEbayAspectMappings(storeId, categoryId);
      res.json(mappings);
    } catch (error: any) {
      console.error("Error getting aspect mappings:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get aspect mappings" });
    }
  });

  // Create or update eBay aspect mapping
  app.post("/api/ebay/aspect-mappings", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { ebayAspect, shopifyField, isRequired, defaultValue, categoryId } =
        req.body;

      if (!ebayAspect || !shopifyField) {
        return res
          .status(400)
          .json({ error: "ebayAspect and shopifyField are required" });
      }

      const mapping = await storage.upsertEbayAspectMapping({
        shopifyStoreId: storeId,
        ebayAspect,
        shopifyField,
        isRequired: isRequired ?? false,
        defaultValue: defaultValue || null,
        categoryId: categoryId || "", // Empty string for general mappings (null causes duplicate issues)
      });

      res.json(mapping);
    } catch (error: any) {
      console.error("Error saving aspect mapping:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to save aspect mapping" });
    }
  });

  // Delete eBay aspect mapping
  app.delete("/api/ebay/aspect-mappings/:id", async (req, res) => {
    try {
      await storage.deleteEbayAspectMapping(req.params.id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error deleting aspect mapping:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to delete aspect mapping" });
    }
  });

  // Get default eBay aspects (common item specifics for apparel)
  app.get("/api/ebay/default-aspects", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { getDefaultCategoryAspects } = await import("./ebayApi");
      const result = await getDefaultCategoryAspects(storeId);

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      res.json(result.aspects);
    } catch (error: any) {
      console.error("Error getting default aspects:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get default aspects" });
    }
  });

  // Save eBay store settings
  app.post("/api/ebay/settings", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const {
        clientId,
        clientSecret,
        refreshToken,
        environment,
        marketplace,
        redirectUri,
        paymentPolicyId,
        returnPolicyId,
        shippingPolicyId,
        inventoryLocation,
        autoListNewProducts,
        autoListRestocked,
        autoSyncStock,
        autoEndWhenSoldOut,
        sizeExpansionEnabled,
        sizeExpansionUp,
        sizeExpansionDown,
        defaultCondition,
        defaultListingFormat,
        defaultListingDuration,
        handlingTime,
        bestOfferEnabled,
        autoAcceptPrice,
        autoDeclinePrice,
        defaultAspects,
        defaultPackageWeightLbs,
        defaultPackageHeightIn,
        defaultPackageWidthIn,
        defaultPackageLengthIn,
        locationAddressLine1,
        locationCity,
        locationState,
        locationPostalCode,
        locationCountry,
      } = req.body;

      // Get existing settings to preserve masked fields
      const existing = await storage.getEbayStoreSettings(storeId);

      // Build settings object, preserving existing sensitive values if masked
      const settingsToSave: any = {
        shopifyStoreId: storeId,
        clientId: clientId || existing?.clientId,
        environment: environment || "sandbox",
        marketplace: marketplace || "EBAY_US",
        redirectUri: redirectUri || existing?.redirectUri || null,
        paymentPolicyId,
        returnPolicyId,
        shippingPolicyId,
        inventoryLocation,
        autoListNewProducts: autoListNewProducts ?? false,
        autoListRestocked: autoListRestocked ?? false,
        autoSyncStock: autoSyncStock ?? true,
        autoEndWhenSoldOut: autoEndWhenSoldOut ?? true,
        sizeExpansionEnabled: sizeExpansionEnabled ?? false,
        sizeExpansionUp: Math.min(sizeExpansionUp ?? 1, 1), // Max 1
        sizeExpansionDown: Math.min(sizeExpansionDown ?? 1, 1), // Max 1
        defaultCondition: defaultCondition || "1000",
        defaultListingFormat: defaultListingFormat || "fixed_price",
        defaultListingDuration: defaultListingDuration || "GTC",
        handlingTime: handlingTime ?? 3,
        bestOfferEnabled: bestOfferEnabled ?? false,
        autoAcceptPrice,
        autoDeclinePrice,
        defaultAspects: defaultAspects ?? existing?.defaultAspects ?? null,
        defaultPackageWeightLbs:
          defaultPackageWeightLbs ?? existing?.defaultPackageWeightLbs ?? null,
        defaultPackageHeightIn:
          defaultPackageHeightIn ?? existing?.defaultPackageHeightIn ?? null,
        defaultPackageWidthIn:
          defaultPackageWidthIn ?? existing?.defaultPackageWidthIn ?? null,
        defaultPackageLengthIn:
          defaultPackageLengthIn ?? existing?.defaultPackageLengthIn ?? null,
        locationAddressLine1:
          locationAddressLine1 ?? existing?.locationAddressLine1 ?? null,
        locationCity: locationCity ?? existing?.locationCity ?? null,
        locationState: locationState ?? existing?.locationState ?? null,
        locationPostalCode:
          locationPostalCode ?? existing?.locationPostalCode ?? null,
        locationCountry: locationCountry ?? existing?.locationCountry ?? "US",
      };

      // Only update sensitive fields if not masked
      if (clientSecret && clientSecret !== "••••••••") {
        settingsToSave.clientSecret = clientSecret;
      } else if (existing?.clientSecret) {
        settingsToSave.clientSecret = existing.clientSecret;
      }

      if (refreshToken && refreshToken !== "••••••••") {
        settingsToSave.refreshToken = refreshToken;
      } else if (existing?.refreshToken) {
        settingsToSave.refreshToken = existing.refreshToken;
      }

      const settings = await storage.upsertEbayStoreSettings(settingsToSave);

      res.json({
        success: true,
        settings: {
          ...settings,
          clientSecret: settings.clientSecret ? "••••••••" : null,
          refreshToken: settings.refreshToken ? "••••••••" : null,
          accessToken: settings.accessToken ? "••••••••" : null,
        },
      });
    } catch (error: any) {
      console.error("Error saving eBay settings:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to save eBay settings" });
    }
  });

  // Test eBay connection with real API call
  app.post("/api/ebay/test-connection", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const settings = await storage.getEbayStoreSettings(storeId);
      if (!settings?.clientId || !settings?.clientSecret) {
        return res.status(400).json({
          success: false,
          error: "API credentials not configured",
        });
      }

      const { testConnection } = await import("./ebayApi");
      const result = await testConnection(settings);

      res.json(result);
    } catch (error: any) {
      console.error("Error testing eBay connection:", error);
      res.status(500).json({
        success: false,
        error: error.message || "Failed to test eBay connection",
      });
    }
  });

  // Get eBay OAuth authorization URL
  app.get("/api/ebay/oauth/authorize", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const settings = await storage.getEbayStoreSettings(storeId);
      if (!settings?.clientId) {
        return res.status(400).json({
          success: false,
          error: "Client ID not configured. Please save API credentials first.",
        });
      }

      // RuName is required for eBay OAuth - it's the identifier, not a URL
      if (!settings.redirectUri) {
        return res.status(400).json({
          success: false,
          error:
            "RuName not configured. Please enter your eBay RuName (e.g., YourCompany-AppName-PRD-xxxxx) in the Redirect URI field.",
        });
      }

      // Use the configured RuName (not a URL - eBay uses this to look up your callback URLs)
      const redirectUri = settings.redirectUri;

      // Generate secure random state for CSRF protection
      const stateNonce = crypto.randomBytes(32).toString("hex");
      const stateData = {
        nonce: stateNonce,
        storeId,
        timestamp: Date.now(),
      };
      const state = Buffer.from(JSON.stringify(stateData)).toString("base64");

      // Store state for validation (expires in 10 minutes)
      const stateExpiry = new Date(Date.now() + 10 * 60 * 1000);
      await storage.upsertEbayStoreSettings({
        ...settings,
        oauthState: stateNonce,
        oauthStateExpiry: stateExpiry,
      });

      const { generateAuthorizationUrl } = await import("./ebayApi");
      const authUrl = generateAuthorizationUrl(
        settings.clientId,
        redirectUri,
        settings.environment || "sandbox",
        state,
      );

      res.json({
        success: true,
        authorizationUrl: authUrl,
        redirectUri: redirectUri,
      });
    } catch (error: any) {
      console.error("Error generating OAuth URL:", error);
      res.status(500).json({
        success: false,
        error: error.message || "Failed to generate authorization URL",
      });
    }
  });

  // eBay OAuth callback handler
  app.get("/api/ebay/oauth/callback", async (req, res) => {
    try {
      const { code, state, error, error_description } = req.query;

      if (error) {
        console.error("eBay OAuth error:", error, error_description);
        return res.redirect(
          `/ebay-connect?error=${encodeURIComponent(String(error_description || error))}`,
        );
      }

      if (!code || !state) {
        return res.redirect(
          "/ebay-connect?error=Missing authorization code or state",
        );
      }

      let stateData: { nonce: string; storeId: string; timestamp: number };
      try {
        stateData = JSON.parse(Buffer.from(String(state), "base64").toString());
      } catch {
        return res.redirect("/ebay-connect?error=Invalid state parameter");
      }

      const settings = await storage.getEbayStoreSettings(stateData.storeId);
      if (!settings?.clientId || !settings?.clientSecret) {
        return res.redirect("/ebay-connect?error=API credentials not found");
      }

      // Validate state for CSRF protection
      if (!settings.oauthState || settings.oauthState !== stateData.nonce) {
        console.error("OAuth state mismatch - possible CSRF attack");
        return res.redirect(
          "/ebay-connect?error=Security validation failed. Please try again.",
        );
      }

      // Check state expiry
      if (
        settings.oauthStateExpiry &&
        new Date(settings.oauthStateExpiry) < new Date()
      ) {
        return res.redirect(
          "/ebay-connect?error=Authorization session expired. Please try again.",
        );
      }

      // Clear the stored state
      await storage.upsertEbayStoreSettings({
        ...settings,
        oauthState: null,
        oauthStateExpiry: null,
      });

      // Use the configured RuName for token exchange
      if (!settings.redirectUri) {
        return res.redirect("/ebay-connect?error=RuName not configured");
      }
      const redirectUri = settings.redirectUri;

      const { exchangeAuthorizationCode } = await import("./ebayApi");
      const tokenResult = await exchangeAuthorizationCode(
        settings.clientId,
        settings.clientSecret,
        String(code),
        redirectUri,
        settings.environment || "sandbox",
      );

      if (!tokenResult.success || !tokenResult.token) {
        return res.redirect(
          `/ebay-connect?error=${encodeURIComponent(tokenResult.error || "Token exchange failed")}`,
        );
      }

      const tokenExpiresAt = new Date(
        Date.now() + (tokenResult.token.expires_in - 300) * 1000,
      );
      const refreshTokenExpiresAt = tokenResult.token.refresh_token_expires_in
        ? new Date(
            Date.now() +
              (tokenResult.token.refresh_token_expires_in - 300) * 1000,
          )
        : null;

      await storage.upsertEbayStoreSettings({
        ...settings,
        accessToken: tokenResult.token.access_token,
        tokenExpiresAt,
        refreshToken: tokenResult.token.refresh_token || settings.refreshToken,
        refreshTokenExpiresAt:
          refreshTokenExpiresAt || settings.refreshTokenExpiresAt,
        isConnected: true,
      });

      res.redirect(
        "/ebay-connect?success=true&message=Successfully connected to eBay",
      );
    } catch (error: any) {
      console.error("Error in OAuth callback:", error);
      res.redirect(
        `/ebay-connect?error=${encodeURIComponent(error.message || "OAuth callback failed")}`,
      );
    }
  });

  // Validate eBay business policies
  app.post("/api/ebay/validate-policies", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { validatePolicies } = await import("./ebayApi");
      const result = await validatePolicies(storeId);

      res.json(result);
    } catch (error: any) {
      console.error("Error validating policies:", error);
      res.status(500).json({
        success: false,
        error: error.message || "Failed to validate policies",
      });
    }
  });

  // Get eBay activity log
  app.get("/api/ebay/activity-log", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { limit = "100", since } = req.query;
      const options: any = { limit: parseInt(limit as string, 10) };

      if (since) {
        options.since = new Date(since as string);
      }

      const activities = await storage.getEbayActivityLog(storeId, options);
      res.json(activities);
    } catch (error: any) {
      console.error("Error getting eBay activity log:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get activity log" });
    }
  });

  // Get eBay activity summary (for notifications)
  app.get("/api/ebay/activity-summary", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      // Default to last 24 hours
      const since = new Date();
      since.setHours(since.getHours() - 24);

      const summary = await storage.getEbayActivitySummary(storeId, since);
      res.json(summary);
    } catch (error: any) {
      console.error("Error getting eBay activity summary:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get activity summary" });
    }
  });

  // ==========================================
  // EBAY LISTING QUEUE ENDPOINTS
  // ==========================================

  // Queue a product for eBay listing
  app.post("/api/ebay/queue-product", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { productId, reason = "manual" } = req.body;
      if (!productId) {
        return res.status(400).json({ error: "productId is required" });
      }

      const { queueProductForListing } = await import("./ebayAutomation");
      const result = await queueProductForListing(storeId, productId, reason);

      if (result.success) {
        if (result.addedToWatchlist) {
          res.json({
            success: true,
            watchlistId: result.watchlistId,
            addedToWatchlist: true,
            message: result.error || "Added to watchlist (zero stock)",
          });
        } else {
          res.json({ success: true, queueId: result.queueId });
        }
      } else {
        res.status(400).json({ success: false, error: result.error });
      }
    } catch (error: any) {
      console.error("Error queueing product:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to queue product" });
    }
  });

  // Process the eBay listing queue
  app.post("/api/ebay/process-queue", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { limit = 10 } = req.body;

      const { processListingQueue } = await import("./ebayAutomation");
      const result = await processListingQueue(storeId, limit);

      res.json({
        success: true,
        processed: result.processed,
        succeeded: result.succeeded,
        failed: result.failed,
        errors: result.errors,
      });
    } catch (error: any) {
      console.error("Error processing queue:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to process queue" });
    }
  });

  // Process the eBay listing queue with real-time progress via SSE
  app.get("/api/ebay/process-queue-stream", async (req, res) => {
    // Disable compression for SSE to ensure events flush immediately
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no"); // Disable nginx buffering
    res.setHeader("Content-Encoding", "none"); // Disable compression

    // Flush headers immediately to establish connection
    res.flushHeaders();

    let isComplete = false;

    const sendEvent = (data: any) => {
      if (res.writableEnded) return;
      res.write(`data: ${JSON.stringify(data)}\n\n`);
      // Force flush the data immediately
      if (typeof (res as any).flush === "function") {
        (res as any).flush();
      }
      if (data.type === "complete" || data.type === "error") {
        isComplete = true;
      }
    };

    // Heartbeat to keep connection alive (every 10 seconds)
    const heartbeatInterval = setInterval(() => {
      if (!isComplete && !res.writableEnded) {
        res.write(`:heartbeat\n\n`);
        if (typeof (res as any).flush === "function") {
          (res as any).flush();
        }
      }
    }, 10000);

    // Clean up on connection close
    res.on("close", () => {
      clearInterval(heartbeatInterval);
    });

    // Send initial ping to confirm connection
    sendEvent({ type: "connected" });

    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        sendEvent({ type: "error", error: "No default store configured" });
        clearInterval(heartbeatInterval);
        res.end();
        return;
      }

      const limitParam = req.query.limit;
      const limit = limitParam ? parseInt(limitParam as string, 10) : 10;

      const { processListingQueue } = await import("./ebayAutomation");

      await processListingQueue(storeId, limit, (event) => {
        sendEvent(event);
      });

      // Always send complete event if not already sent
      if (!isComplete) {
        sendEvent({
          type: "complete",
          result: { processed: 0, succeeded: 0, failed: 0, errors: [] },
        });
      }

      clearInterval(heartbeatInterval);
      res.end();
    } catch (error: any) {
      console.error("Error processing queue stream:", error);
      sendEvent({
        type: "error",
        error: error.message || "Failed to process queue",
      });
      clearInterval(heartbeatInterval);
      res.end();
    }
  });

  // End an eBay listing by queue item ID, product ID, or directly by offer ID
  app.post("/api/ebay/end-listing", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { queueItemId, offerId, productId } = req.body;

      let targetOfferId = offerId;
      let queueItem = null;
      let productTitle = null;

      // If queueItemId provided, look up the offer ID from the queue
      if (queueItemId) {
        queueItem = await storage.getEbayListingQueueItem(queueItemId);
        if (!queueItem) {
          return res.status(404).json({ error: "Queue item not found" });
        }
        if (!queueItem.ebayOfferId) {
          return res
            .status(400)
            .json({ error: "This listing does not have an eBay offer ID" });
        }
        targetOfferId = queueItem.ebayOfferId;
        productTitle = queueItem.productTitle;
      }

      // If productId provided, look up the queue item by product ID
      if (productId && !targetOfferId) {
        const queueItems = await storage.getEbayListingQueue(storeId);
        queueItem = queueItems.find(
          (item: any) =>
            item.shopifyProductId === productId &&
            item.status === "listed" &&
            item.ebayOfferId,
        );
        if (!queueItem) {
          return res
            .status(404)
            .json({ error: "No listed queue item found for this product" });
        }
        targetOfferId = queueItem.ebayOfferId;
        productTitle = queueItem.productTitle;
      }

      // For ending active listings, we need to use the eBay listing ID (item ID)
      // The Trading API EndFixedPriceItem is the proper way to end published listings
      const ebayListingId = queueItem?.ebayListingId;

      let result: { success: boolean; error?: string };

      if (ebayListingId) {
        // Use Trading API to end the active listing by item ID
        console.log(
          `[End Listing] Using Trading API to end item: ${ebayListingId}`,
        );
        const { endListingByItemId } = await import("./ebayApi");
        result = await endListingByItemId(storeId, ebayListingId);
      } else if (targetOfferId) {
        // Fall back to Inventory API offer withdrawal
        console.log(
          `[End Listing] Using Inventory API to withdraw offer: ${targetOfferId}`,
        );
        const { endListing } = await import("./ebayApi");
        result = await endListing(storeId, targetOfferId);
      } else {
        return res
          .status(400)
          .json({ error: "queueItemId, productId, or offerId is required" });
      }

      if (result.success) {
        // Update the queue item status if we have one
        if (queueItem) {
          // Use queueItem.id since queueItemId might be undefined when looking up by productId
          await storage.updateEbayListingQueueStatus(
            queueItem.id,
            "ended",
            undefined,
          );
        }

        // Log the activity
        await storage.createEbayActivityLog({
          shopifyStoreId: storeId,
          activityType: "listing_ended",
          shopifyProductId: queueItem?.shopifyProductId,
          productTitle: queueItem?.productTitle || null,
          sku: null,
          details: { ebayListingId: ebayListingId || targetOfferId },
        });

        res.json({ success: true, message: "Listing ended successfully" });
      } else {
        res.status(400).json({ success: false, error: result.error });
      }
    } catch (error: any) {
      console.error("Error ending eBay listing:", error);
      res.status(500).json({ error: error.message || "Failed to end listing" });
    }
  });

  // Refresh eBay listing description(s) with updated template
  app.post("/api/ebay/refresh-descriptions", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      let { productIds, vendor } = req.body;

      // If vendor is provided, get all listed products for that vendor
      if (vendor && typeof vendor === "string") {
        const queueItems = await storage.getEbayListingQueue(storeId);
        const listedItems = queueItems.filter(
          (item: any) => item.status === "listed" && item.ebayListingId,
        );

        // Get product details to filter by vendor
        const listedProductIds = listedItems.map(
          (item: any) => item.shopifyProductId,
        );
        const vendorProductIds: string[] = [];

        // Batch fetch all product cache entries for the listed products
        const productCaches = await storage.getProductCacheByIds(
          storeId,
          listedProductIds,
        );

        for (const productCache of productCaches) {
          if (
            productCache.vendor?.trim().toLowerCase() ===
            vendor.trim().toLowerCase()
          ) {
            vendorProductIds.push(productCache.shopifyProductId);
          }
        }

        productIds = vendorProductIds;
        console.log(
          `[Refresh] Found ${productIds.length} listed products for vendor "${vendor}"`,
        );
      }

      if (
        !productIds ||
        !Array.isArray(productIds) ||
        productIds.length === 0
      ) {
        return res.status(400).json({
          error:
            "No products found to refresh. Provide productIds array or vendor name.",
        });
      }

      const { refreshListingDescription } = await import("./ebayApi");

      const results: { productId: string; success: boolean; error?: string }[] =
        [];

      for (const productId of productIds) {
        try {
          // Get the queue item to find the eBay listing ID
          const queueItems = await storage.getEbayListingQueue(storeId);
          const queueItem = queueItems.find(
            (item: any) =>
              item.shopifyProductId === productId &&
              item.status === "listed" &&
              item.ebayListingId,
          );

          if (!queueItem || !queueItem.ebayListingId) {
            results.push({
              productId,
              success: false,
              error: "No active eBay listing found",
            });
            continue;
          }

          // Get product data from cache
          const productCache = await storage.getProductCacheById(productId);
          if (!productCache) {
            results.push({
              productId,
              success: false,
              error: "Product not found in cache",
            });
            continue;
          }

          // Get vendor template settings
          const vendorName = productCache.vendor?.trim();
          let templateType: string | null = null;
          let sizeChartUrl: string | null = null;
          let sizeChartHtml: string | null = null;

          if (vendorName) {
            const vendorTemplate = await storage.getEbayVendorTemplate(
              storeId,
              vendorName,
            );
            if (vendorTemplate) {
              templateType = vendorTemplate.templateType;
              sizeChartUrl = vendorTemplate.sizeChartUrl;
              sizeChartHtml = vendorTemplate.sizeChartHtml;
            }
          }

          // Map template names
          const templateNameToType: Record<string, string> = {
            glamorous: "glamorous",
            elegant: "elegant",
            luxe: "luxe",
            "Glamorous Dark": "glamorous",
            "Elegant Light": "elegant",
            "Luxe Couture": "luxe",
          };

          const mappedTemplateType = templateType
            ? templateNameToType[templateType]
            : null;

          if (!mappedTemplateType) {
            results.push({
              productId,
              success: false,
              error: "No template configured for vendor",
            });
            continue;
          }

          // Get images from Shopify
          let productImages: string[] = [];
          try {
            const shopifyStore = await storage.getShopifyStore(storeId);
            if (
              shopifyStore &&
              shopifyStore.storeUrl &&
              shopifyStore.accessToken
            ) {
              const service = new ShopifyService(
                shopifyStore.storeUrl,
                shopifyStore.accessToken,
              );
              const fullProduct = await service.getProductById(productId);
              if (fullProduct?.images && fullProduct.images.length > 0) {
                productImages = fullProduct.images
                  .map((img: any) => img.src || img.url)
                  .filter((url: string) => url && url.startsWith("http"))
                  .slice(0, 12);
              }
            }
          } catch (e) {
            console.warn(
              `[Refresh] Could not fetch images from Shopify for ${productId}`,
            );
          }

          // Fallback to cached images
          if (productImages.length === 0) {
            const cachedImages = (productCache.images as any[]) || [];
            productImages = cachedImages
              .map((img: any) => img.src || img.url)
              .filter((url: string) => url && url.startsWith("http"))
              .slice(0, 12);
            if (productImages.length === 0 && productCache.imageUrl) {
              productImages = [productCache.imageUrl];
            }
          }

          // Get item specifics from metafields
          const metafieldsArray = (productCache.metafields as any[]) || [];
          const specs: { label: string; value: string }[] = [];
          const excludedSpecs = new Set([
            "color",
            "size",
            "country of origin",
            "country/region of manufacture",
          ]);

          for (const mf of metafieldsArray) {
            if (
              mf &&
              mf.key &&
              mf.value &&
              !excludedSpecs.has(mf.key.toLowerCase())
            ) {
              specs.push({ label: mf.key, value: mf.value });
            }
          }

          // Build template data
          const templateData: EbayTemplateData = {
            title: productCache.title,
            images: productImages,
            description: productCache.description || productCache.title,
            sizeChartUrl: sizeChartUrl ?? undefined,
            sizeChartHtml: sizeChartHtml ?? undefined,
            specifications: specs,
            vendor: productCache.vendor ?? undefined,
            sku: productId,
            price: productCache.price || "0",
          };

          // Generate new description
          const newDescription = generateTemplate(
            mappedTemplateType as TemplateType,
            templateData,
          );

          // Update on eBay - pass offer ID and product ID for inventory group lookup
          console.log(
            `[Refresh] Using offer ID: ${queueItem.ebayOfferId}, Product ID: ${productId}`,
          );

          const refreshResult = await refreshListingDescription(
            storeId,
            queueItem.ebayListingId,
            newDescription,
            queueItem.ebayOfferId,
            productId,
          );

          if (refreshResult.success) {
            results.push({ productId, success: true });
            console.log(
              `[Refresh] Successfully updated description for ${productCache.title}`,
            );
          } else {
            results.push({
              productId,
              success: false,
              error: refreshResult.error,
            });
          }
        } catch (err: any) {
          results.push({
            productId,
            success: false,
            error: err.message || "Unknown error",
          });
        }
      }

      const succeeded = results.filter((r) => r.success).length;
      const failed = results.filter((r) => !r.success).length;

      res.json({
        success: true,
        results,
        summary: { total: productIds.length, succeeded, failed },
      });
    } catch (error: any) {
      console.error("Error refreshing eBay descriptions:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to refresh descriptions" });
    }
  });

  // Get vendors for eBay bulk posting (auto-resolves store ID)
  app.get("/api/ebay/vendors", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const onlyWithDataSource = req.query.onlyWithDataSource === "true";

      const vendorResults = await storage.getProductCacheVendors(storeId);
      let vendors = vendorResults.map((v: any) => v.vendor).filter(Boolean);

      // If filter enabled, only show vendors that match data source names
      if (onlyWithDataSource) {
        const dataSources = await storage.getDataSources();
        // Normalize names for comparison (trim, decode HTML entities, lowercase)
        const normalizeName = (name: string) =>
          name
            .trim()
            .replace(/&amp;/g, "&")
            .replace(/&lt;/g, "<")
            .replace(/&gt;/g, ">")
            .replace(/&quot;/g, '"')
            .replace(/&#39;/g, "'")
            .toLowerCase();

        const dataSourceNames = dataSources.map((ds: any) =>
          normalizeName(ds.name),
        );

        // Match vendors using flexible prefix matching:
        // 1. Exact match
        // 2. Vendor starts with data source name (e.g., "Alyce Paris" starts with "Alyce")
        // 3. Data source name starts with vendor (e.g., data source "Jovani Sales" starts with vendor "Jovani")
        vendors = vendors.filter((v: string) => {
          const normalizedVendor = normalizeName(v);
          return dataSourceNames.some(
            (dsName: string) =>
              normalizedVendor === dsName ||
              normalizedVendor.startsWith(dsName) ||
              dsName.startsWith(normalizedVendor),
          );
        });
      }

      res.json(vendors);
    } catch (error: any) {
      console.error("Error getting vendors for eBay:", error);
      res.status(500).json({ error: error.message || "Failed to get vendors" });
    }
  });

  // Get products for eBay bulk posting (auto-resolves store ID)
  app.get("/api/ebay/products", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const page = parseInt(req.query.page as string) || 1;
      const limit = parseInt(req.query.limit as string) || 50;
      const vendor = req.query.vendor as string;
      const search = req.query.search as string;
      const status = req.query.status as string;
      const productType = req.query.productType as string;
      const stockFilter = req.query.stockFilter as string;
      const ebayStatusFilter = req.query.ebayStatus as string;
      const minPrice = req.query.minPrice
        ? parseFloat(req.query.minPrice as string)
        : undefined;
      const maxPrice = req.query.maxPrice
        ? parseFloat(req.query.maxPrice as string)
        : undefined;

      // Check if we need client-side filtering (ebayStatus, productType, price filters)
      const needsClientSideFiltering =
        (ebayStatusFilter && ebayStatusFilter !== "__all__") ||
        (productType && productType !== "__all__") ||
        (minPrice !== undefined && !isNaN(minPrice)) ||
        (maxPrice !== undefined && !isNaN(maxPrice));

      // If we need client-side filtering, fetch more products to filter from
      // Otherwise use normal pagination
      const result = await storage.getProductCache(storeId, {
        page: needsClientSideFiltering ? 1 : page,
        limit: needsClientSideFiltering ? 10000 : limit,
        vendor: vendor && vendor !== "__all__" ? vendor : undefined,
        search: search || undefined,
        status: status && status !== "__all__" ? status : undefined,
        stockFilter:
          stockFilter === "in_stock" || stockFilter === "out_of_stock"
            ? stockFilter
            : undefined,
        sortBy: "title",
        sortOrder: "asc",
      });

      // Fetch prices for these products from variant cache
      const productIds = result.products.map((p: any) => p.id);
      const variantPrices =
        productIds.length > 0
          ? await storage.getFirstVariantPricesByProductIds(storeId, productIds)
          : new Map();

      // Check eBay listing status for products
      const queueItems = await storage.getEbayListingQueue(storeId);
      const queueMap = new Map(
        queueItems.map((q: any) => [q.shopifyProductId, q]),
      );

      // Map to expected format with additional fields
      let products = result.products.map((p: any) => {
        const queueItem = queueMap.get(p.id);
        let ebayStatus = "not_listed";
        if (queueItem) {
          ebayStatus = queueItem.status;
        }

        return {
          id: p.id,
          shopifyProductId: p.id,
          title: p.title,
          vendor: p.vendor,
          productType: p.productType,
          totalInventory: p.totalInventory,
          variantCount: p.variantCount || 0,
          status: p.status,
          imageUrl: p.imageUrl,
          price: variantPrices.get(p.id) || null,
          ebayStatus,
          ebayListingId: queueItem?.ebayListingId || null,
        };
      });

      // Apply product type filter client-side if provided
      if (productType && productType !== "__all__") {
        products = products.filter((p: any) => p.productType === productType);
      }

      // Apply price filters client-side
      if (minPrice !== undefined && !isNaN(minPrice)) {
        products = products.filter((p: any) => {
          const price = parseFloat(p.price);
          return !isNaN(price) && price >= minPrice;
        });
      }
      if (maxPrice !== undefined && !isNaN(maxPrice)) {
        products = products.filter((p: any) => {
          const price = parseFloat(p.price);
          return !isNaN(price) && price <= maxPrice;
        });
      }

      // Apply eBay status filter
      if (ebayStatusFilter && ebayStatusFilter !== "__all__") {
        products = products.filter(
          (p: any) => p.ebayStatus === ebayStatusFilter,
        );
      }

      // Handle pagination
      let finalProducts = products;
      let totalCount = result.total;
      let currentPage = result.page;
      let currentPageSize = result.pageSize;

      // If we applied client-side filters, we need to paginate the filtered results
      if (needsClientSideFiltering) {
        totalCount = products.length;
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        finalProducts = products.slice(startIndex, endIndex);
        currentPage = page;
        currentPageSize = limit;
      }

      res.json({
        products: finalProducts,
        total: totalCount,
        page: currentPage,
        pageSize: currentPageSize,
      });
    } catch (error: any) {
      console.error("Error getting products for eBay:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get products" });
    }
  });

  // Bulk queue products by vendor for eBay listing
  app.post("/api/ebay/bulk-queue", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { vendor, productIds } = req.body;

      // Either queue by vendor or by explicit product IDs
      let productsToQueue: { id: string; title: string }[] = [];

      if (productIds && Array.isArray(productIds) && productIds.length > 0) {
        // Queue specific products
        const products = await storage.getProductCacheByIds(
          storeId,
          productIds,
        );
        productsToQueue = products.map((p) => ({
          id: p.shopifyProductId,
          title: p.title,
        }));
      } else if (vendor) {
        // Queue all products for a vendor
        const products = await storage.getProductCacheByVendor(storeId, vendor);
        productsToQueue = products.map((p) => ({
          id: p.shopifyProductId,
          title: p.title,
        }));
      } else {
        return res
          .status(400)
          .json({ error: "Either vendor or productIds is required" });
      }

      const { queueProductForListing } = await import("./ebayAutomation");
      const results = {
        queued: 0,
        alreadyInQueue: 0,
        addedToWatchlist: 0,
        alreadyInWatchlist: 0,
        failed: 0,
        errors: [] as string[],
      };

      for (const product of productsToQueue) {
        const result = await queueProductForListing(
          storeId,
          product.id,
          "manual",
          product.title,
        );
        if (result.success) {
          if (result.addedToWatchlist) {
            if (result.error === "Already in watchlist") {
              results.alreadyInWatchlist++;
            } else {
              results.addedToWatchlist++;
            }
          } else if (result.error === "Already in queue") {
            results.alreadyInQueue++;
          } else {
            results.queued++;
          }
        } else {
          results.failed++;
          results.errors.push(`${product.title}: ${result.error}`);
        }
      }

      res.json({
        success: true,
        totalProducts: productsToQueue.length,
        ...results,
      });
    } catch (error: any) {
      console.error("Error bulk queueing products:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to bulk queue products" });
    }
  });

  // Clear queue items by status (defaults to listed and failed only, never touches pending/processing)
  app.delete("/api/ebay/listing-queue", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { statuses } = req.query;
      let statusList: string[];
      if (statuses) {
        statusList = (statuses as string).split(",").map((s) => s.trim());
      } else {
        // Default to only clearing completed states - never touch pending/processing
        statusList = ["listed", "failed"];
      }

      let deletedCount = 0;
      for (const status of statusList) {
        deletedCount += await storage.clearEbayListingQueue(storeId, status);
      }

      res.json({ success: true, deleted: deletedCount });
    } catch (error: any) {
      console.error("Error clearing listing queue:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to clear listing queue" });
    }
  });

  // Sync stock to eBay for all active listings
  app.post("/api/ebay/sync-stock", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { syncStockToEbay } = await import("./ebayAutomation");
      const result = await syncStockToEbay(storeId);

      res.json({
        success: true,
        synced: result.synced,
        errors: result.errors,
      });
    } catch (error: any) {
      console.error("Error syncing stock to eBay:", error);
      res.status(500).json({ error: error.message || "Failed to sync stock" });
    }
  });

  // Get listing queue status
  app.get("/api/ebay/listing-queue", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { status } = req.query;
      const items = await storage.getEbayListingQueue(
        storeId,
        status as string | undefined,
      );
      res.json(items);
    } catch (error: any) {
      console.error("Error getting listing queue:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get listing queue" });
    }
  });

  // ==========================================
  // EBAY WATCHLIST ROUTES
  // ==========================================

  // Get watchlist items
  app.get("/api/ebay/watchlist", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { status } = req.query;
      const items = await storage.getEbayWatchlist(
        storeId,
        status as string | undefined,
      );
      res.json(items);
    } catch (error: any) {
      console.error("Error getting watchlist:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get watchlist" });
    }
  });

  // Get watchlist count
  app.get("/api/ebay/watchlist/count", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const count = await storage.getEbayWatchlistCount(storeId);
      res.json({ count });
    } catch (error: any) {
      console.error("Error getting watchlist count:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get watchlist count" });
    }
  });

  // Promote watchlist item to queue
  app.post("/api/ebay/watchlist/:id/promote", async (req, res) => {
    try {
      const { id } = req.params;
      await storage.promoteWatchlistToQueue(id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error promoting watchlist item:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to promote watchlist item" });
    }
  });

  // Delete watchlist item
  app.delete("/api/ebay/watchlist/:id", async (req, res) => {
    try {
      const { id } = req.params;
      await storage.deleteEbayWatchlistItem(id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error deleting watchlist item:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to delete watchlist item" });
    }
  });

  // Run new product detection manually
  app.post("/api/ebay/detect-new-products", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { detectNewProductsAfterCacheSync } = await import(
        "./ebayAutomation"
      );
      const result = await detectNewProductsAfterCacheSync(storeId);

      res.json({
        success: true,
        ...result,
      });
    } catch (error: any) {
      console.error("Error running detection:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to run detection" });
    }
  });

  // Get enriched listings for the Listings tab (with product details)
  app.get("/api/ebay/listings", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { status } = req.query;

      // Get all listing queue items (listed, ended, failed - not pending/processing)
      const listingStatuses =
        status && status !== "all"
          ? [status as string]
          : ["listed", "ended", "failed"];

      const allItems = await storage.getEbayListingQueue(storeId);
      const filteredItems = allItems.filter((item: any) =>
        listingStatuses.includes(item.status),
      );

      // Batch fetch all variants for all product IDs upfront
      const productIds = filteredItems.map(
        (item: any) => item.shopifyProductId,
      );
      const allVariants = await storage.getVariantCacheByProductIds(
        storeId,
        productIds,
      );

      // Group variants by product ID for quick lookup
      const variantsByProductId = new Map<string, any[]>();
      for (const variant of allVariants) {
        const productId = variant.shopifyProductId;
        if (!variantsByProductId.has(productId)) {
          variantsByProductId.set(productId, []);
        }
        variantsByProductId.get(productId)!.push(variant);
      }

      // Enrich with product cache data for price/inventory info
      const enrichedListings = await Promise.all(
        filteredItems.map(async (item: any) => {
          // Try to get product from cache
          const cachedProduct = await storage.getProductCacheById(
            item.shopifyProductId,
          );

          // Get variants from pre-fetched map
          const variants = variantsByProductId.get(item.shopifyProductId) || [];
          const firstVariant = variants[0];

          // Use direct product cache fields for price, inventory, and variant count
          // Fall back to first variant price if product cache price is not set
          let price = 0;
          if (cachedProduct?.price) {
            price = parseFloat(cachedProduct.price);
          } else if (firstVariant?.price) {
            price = parseFloat(firstVariant.price);
          }

          // Use product cache total inventory, or sum variant inventories as fallback
          let totalInventory = cachedProduct?.totalInventory;
          if (totalInventory === null || totalInventory === undefined) {
            totalInventory = variants.reduce(
              (sum: number, v: any) => sum + (v.inventoryQuantity || 0),
              0,
            );
          }
          const variantCount =
            cachedProduct?.variantCount || variants.length || 0;

          // Map status to display format for UI consistency
          // listed -> active, failed -> error (UI expects these names)
          let displayStatus = item.status;
          if (item.status === "listed") displayStatus = "active";
          else if (item.status === "failed") displayStatus = "error";

          return {
            id: item.id,
            title:
              item.productTitle || cachedProduct?.title || "Unknown Product",
            sku: firstVariant?.sku || item.shopifyProductId,
            status: displayStatus,
            price: price,
            quantity: totalInventory,
            variantCount: variantCount,
            lastSynced: item.processedAt || item.queuedAt,
            ebayId: item.ebayListingId,
            ebayOfferId: item.ebayOfferId,
            shopifyProductId: item.shopifyProductId,
            error: item.errorMessage,
          };
        }),
      );

      // Calculate stats (using mapped display statuses)
      const stats = {
        active: enrichedListings.filter((l) => l.status === "active").length,
        ended: enrichedListings.filter((l) => l.status === "ended").length,
        error: enrichedListings.filter((l) => l.status === "error").length,
        total: enrichedListings.length,
      };

      res.json({ listings: enrichedListings, stats });
    } catch (error: any) {
      console.error("Error getting eBay listings:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get listings" });
    }
  });

  // Clear ended listings from the queue
  app.delete("/api/ebay/listings/ended", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      // Delete all queue items with "ended" status from the listing queue
      const deletedCount = await storage.clearEbayListingQueue(
        storeId,
        "ended",
      );

      // Also delete any corresponding ebay_listings records with ended status
      await storage.deleteEbayListingsByStatus(storeId, "ended");

      res.json({ success: true, deleted: deletedCount });
    } catch (error: any) {
      console.error("Error clearing ended listings:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to clear ended listings" });
    }
  });

  // Relist an ended eBay listing
  app.post("/api/ebay/relist", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { queueItemId } = req.body;

      if (!queueItemId) {
        return res.status(400).json({ error: "queueItemId is required" });
      }

      // Get the queue item
      const queueItem = await storage.getEbayListingQueueItem(queueItemId);
      if (!queueItem) {
        return res.status(404).json({ error: "Queue item not found" });
      }

      if (queueItem.status !== "ended") {
        return res
          .status(400)
          .json({ error: "Can only relist ended listings" });
      }

      // Reset the queue item to pending status
      await storage.updateEbayListingQueueItem(queueItemId, {
        status: "pending",
        errorMessage: null,
        ebayListingId: null,
        ebayOfferId: null,
        processedAt: null,
      });

      // Process just this one item
      const { processListingQueue } = await import("./ebayAutomation");
      await processListingQueue(storeId, 1);

      // Get updated item
      const updatedItem = await storage.getEbayListingQueueItem(queueItemId);

      if (updatedItem?.status === "listed") {
        res.json({
          success: true,
          message: "Listing relisted successfully",
          listing: updatedItem,
        });
      } else if (updatedItem?.status === "failed") {
        res.json({
          success: false,
          error: updatedItem.errorMessage || "Relisting failed",
        });
      } else {
        res.json({ success: true, message: "Listing queued for relisting" });
      }
    } catch (error: any) {
      console.error("Error relisting:", error);
      res.status(500).json({ error: error.message || "Failed to relist" });
    }
  });

  // Retry a failed eBay listing
  app.post("/api/ebay/retry-listing", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { queueItemId } = req.body;

      if (!queueItemId) {
        return res.status(400).json({ error: "queueItemId is required" });
      }

      // Get the queue item
      const queueItem = await storage.getEbayListingQueueItem(queueItemId);
      if (!queueItem) {
        return res.status(404).json({ error: "Queue item not found" });
      }

      if (queueItem.status !== "failed") {
        return res
          .status(400)
          .json({ error: "Can only retry failed listings" });
      }

      // Reset the queue item to pending status
      await storage.updateEbayListingQueueItem(queueItemId, {
        status: "pending",
        errorMessage: null,
        processedAt: null,
      });

      // Process just this one item
      const { processListingQueue } = await import("./ebayAutomation");
      await processListingQueue(storeId, 1);

      // Get updated item
      const updatedItem = await storage.getEbayListingQueueItem(queueItemId);

      if (updatedItem?.status === "listed") {
        res.json({
          success: true,
          message: "Listing posted successfully",
          listing: updatedItem,
        });
      } else if (updatedItem?.status === "failed") {
        res.json({
          success: false,
          error: updatedItem.errorMessage || "Retry failed",
        });
      } else {
        res.json({ success: true, message: "Listing queued for retry" });
      }
    } catch (error: any) {
      console.error("Error retrying listing:", error);
      res.status(500).json({ error: error.message || "Failed to retry" });
    }
  });

  // ==========================================
  // EBAY LISTING TEMPLATES (named templates for assignment)
  // ==========================================

  // Get all listing templates
  app.get("/api/ebay/listing-templates", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const templates = await storage.getEbayListingTemplates(storeId);
      res.json(templates);
    } catch (error: any) {
      console.error("Error getting listing templates:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get listing templates" });
    }
  });

  // Get a specific listing template
  app.get("/api/ebay/listing-templates/:id", async (req, res) => {
    try {
      const { id } = req.params;
      const template = await storage.getEbayListingTemplate(id);

      if (!template) {
        return res.status(404).json({ error: "Template not found" });
      }

      res.json(template);
    } catch (error: any) {
      console.error("Error getting listing template:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get listing template" });
    }
  });

  // Create a listing template
  app.post("/api/ebay/listing-templates", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { name, templateType, sizeChartUrl, sizeChartHtml, customCss } =
        req.body;

      if (!name) {
        return res.status(400).json({ error: "Template name is required" });
      }

      const template = await storage.createEbayListingTemplate({
        storeId,
        name,
        templateType: templateType || "glamorous",
        sizeChartUrl,
        sizeChartHtml,
        customCss,
      });

      res.json(template);
    } catch (error: any) {
      console.error("Error creating listing template:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to create listing template" });
    }
  });

  // Update a listing template
  app.put("/api/ebay/listing-templates/:id", async (req, res) => {
    try {
      const { id } = req.params;
      const { name, templateType, sizeChartUrl, sizeChartHtml, customCss } =
        req.body;

      const template = await storage.updateEbayListingTemplate(id, {
        name,
        templateType,
        sizeChartUrl,
        sizeChartHtml,
        customCss,
      });

      if (!template) {
        return res.status(404).json({ error: "Template not found" });
      }

      res.json(template);
    } catch (error: any) {
      console.error("Error updating listing template:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to update listing template" });
    }
  });

  // Delete a listing template
  app.delete("/api/ebay/listing-templates/:id", async (req, res) => {
    try {
      const { id } = req.params;
      await storage.deleteEbayListingTemplate(id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error deleting listing template:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to delete listing template" });
    }
  });

  // ==========================================
  // EBAY VENDOR TEMPLATES
  // ==========================================

  // Get all vendor templates
  app.get("/api/ebay/vendor-templates", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const templates = await storage.getEbayVendorTemplates(storeId);
      res.json(templates);
    } catch (error: any) {
      console.error("Error getting vendor templates:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get vendor templates" });
    }
  });

  // Get a specific vendor template
  app.get("/api/ebay/vendor-templates/:vendor", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { vendor } = req.params;
      const template = await storage.getEbayVendorTemplate(
        storeId,
        decodeURIComponent(vendor),
      );

      if (!template) {
        return res
          .status(404)
          .json({ error: "Template not found for this vendor" });
      }

      res.json(template);
    } catch (error: any) {
      console.error("Error getting vendor template:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get vendor template" });
    }
  });

  // Create or update a vendor template
  app.post("/api/ebay/vendor-templates", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { vendor, ...templateData } = req.body;

      if (!vendor) {
        return res.status(400).json({ error: "Vendor name is required" });
      }

      const template = await storage.upsertEbayVendorTemplate({
        storeId,
        vendor,
        ...templateData,
      });

      res.json(template);
    } catch (error: any) {
      console.error("Error saving vendor template:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to save vendor template" });
    }
  });

  // Delete a vendor template
  app.delete("/api/ebay/vendor-templates/:id", async (req, res) => {
    try {
      const { id } = req.params;
      await storage.deleteEbayVendorTemplate(id);
      res.json({ success: true });
    } catch (error: any) {
      console.error("Error deleting vendor template:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to delete vendor template" });
    }
  });

  // Get eBay store categories (seller's custom categories)
  app.get("/api/ebay/store-categories", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { getStoreCategories } = await import("./ebayApi");
      const result = await getStoreCategories(storeId);

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      res.json(result.categories || []);
    } catch (error: any) {
      console.error("Error getting eBay store categories:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get store categories" });
    }
  });

  // Search eBay listing categories
  app.get("/api/ebay/categories/search", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { q } = req.query;
      if (!q || typeof q !== "string") {
        return res.status(400).json({ error: "Search query required" });
      }

      const { searchListingCategories } = await import("./ebayApi");
      const result = await searchListingCategories(storeId, q);

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      res.json(result.categories || []);
    } catch (error: any) {
      console.error("Error searching eBay categories:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to search categories" });
    }
  });

  // Get eBay listing category tree (for browsing)
  app.get("/api/ebay/categories/tree", async (req, res) => {
    try {
      const storeId = await getDefaultShopifyStoreId();
      if (!storeId) {
        return res.status(400).json({ error: "No default store configured" });
      }

      const { parentId } = req.query;

      const { getListingCategoryTree } = await import("./ebayApi");
      const result = await getListingCategoryTree(
        storeId,
        typeof parentId === "string" ? parentId : undefined,
      );

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      res.json({
        categories: result.categories || [],
        categoryTreeId: result.categoryTreeId,
      });
    } catch (error: any) {
      console.error("Error getting eBay category tree:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to get categories" });
    }
  });

  // Register Global Validator routes
  registerGlobalValidatorRoutes(app, storage);

  // ============ ORDER MANAGEMENT ROUTES ============

  app.get("/api/om/orders", async (req, res) => {
    try {
      const { status, payment, search, limit, offset } = req.query;
      const result = await storage.getOrders({
        status: status as string,
        paymentStatus: payment as string,
        search: search as string,
        limit: limit ? parseInt(limit as string) : 50,
        offset: offset ? parseInt(offset as string) : 0,
      });
      res.json(result);
    } catch (error: any) {
      console.error("Error fetching orders:", error);
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/om/orders/:id", async (req, res) => {
    try {
      const order = await storage.getOrderById(parseInt(req.params.id));
      if (!order) return res.status(404).json({ error: "Order not found" });
      res.json(order);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.patch("/api/om/orders/:id", async (req, res) => {
    try {
      const updated = await storage.updateOrder(
        parseInt(req.params.id),
        req.body,
      );
      if (!updated) return res.status(404).json({ error: "Order not found" });
      res.json(updated);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/om/stats", async (req, res) => {
    try {
      const stats = await storage.getOrderStats();
      res.json(stats);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/om/top-styles", async (req, res) => {
    try {
      const limit = req.query.limit ? parseInt(req.query.limit as string) : 10;
      const styles = await storage.getTopSellingStyles(limit);
      res.json(styles);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/om/revenue-by-vendor", async (req, res) => {
    try {
      const vendors = await storage.getRevenueByVendor();
      res.json(vendors);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/om/sync-orders", async (req, res) => {
    try {
      const { fetchShopifyOrders } = await import("./shopifyOrders");
      const stores = await storage.getShopifyStores();
      if (stores.length === 0) {
        return res.status(400).json({ error: "No Shopify store connected" });
      }
      const store = stores[0];
      const sinceDate = req.body.sinceDate || undefined;
      const limit = req.body.limit || 250;

      res.json({ message: "Order sync started" });

      fetchShopifyOrders(store.id, { sinceDate, limit })
        .then((result) => {
          console.log(`[OrderSync] Finished: ${result.synced} orders synced`);
        })
        .catch((err) => {
          console.error("[OrderSync] Background sync failed:", err);
        });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/om/sync-status", async (req, res) => {
    try {
      const lastSync = await storage.getOmSetting("lastOrderSync");
      const orderCount = await storage.getOrderCount();
      res.json({ lastSync, orderCount });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // ============ DASHBOARD SALES ENDPOINT ============

  app.get("/api/dashboard/sales", async (req, res) => {
    try {
      const stats = await storage.getOrderStats();
      const topStyles = await storage.getTopSellingStyles(5);
      const vendorRevenue = await storage.getRevenueByVendor();
      const lastSync = await storage.getOmSetting("lastOrderSync");

      res.json({
        stats,
        topStyles,
        vendorRevenue: vendorRevenue.slice(0, 10),
        lastSync,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // ============ OM HANG TAG TEMPLATES ============

  app.get("/api/om/hang-tags", async (req, res) => {
    try {
      const templates = await storage.getHangTagTemplates();
      res.json(templates);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/om/hang-tags/:id", async (req, res) => {
    try {
      const template = await storage.getHangTagTemplate(
        parseInt(req.params.id),
      );
      if (!template)
        return res.status(404).json({ error: "Template not found" });
      res.json(template);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/om/hang-tags", async (req, res) => {
    try {
      const template = await storage.createHangTagTemplate(req.body);
      res.json(template);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.patch("/api/om/hang-tags/:id", async (req, res) => {
    try {
      const template = await storage.updateHangTagTemplate(
        parseInt(req.params.id),
        req.body,
      );
      if (!template)
        return res.status(404).json({ error: "Template not found" });
      res.json(template);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.delete("/api/om/hang-tags/:id", async (req, res) => {
    try {
      const deleted = await storage.deleteHangTagTemplate(
        parseInt(req.params.id),
      );
      if (!deleted)
        return res.status(404).json({ error: "Template not found" });
      res.json({ success: true });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // ============ OM DISCOUNT CODES ============

  app.get("/api/om/discounts", async (req, res) => {
    try {
      const { status, search } = req.query;
      const discounts = await storage.getDiscountCodes({
        status: status as string,
        search: search as string,
      });
      res.json(discounts);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/om/discounts", async (req, res) => {
    try {
      const discount = await storage.createDiscountCode(req.body);
      res.json(discount);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.patch("/api/om/discounts/:id", async (req, res) => {
    try {
      const discount = await storage.updateDiscountCode(
        parseInt(req.params.id),
        req.body,
      );
      if (!discount)
        return res.status(404).json({ error: "Discount not found" });
      res.json(discount);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.delete("/api/om/discounts/:id", async (req, res) => {
    try {
      const deleted = await storage.deleteDiscountCode(parseInt(req.params.id));
      if (!deleted)
        return res.status(404).json({ error: "Discount not found" });
      res.json({ success: true });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // ============ OM EMAIL TEMPLATES ============

  app.get("/api/om/email-templates", async (req, res) => {
    try {
      const templates = await storage.getEmailTemplates();
      res.json(templates);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/om/email-templates", async (req, res) => {
    try {
      const template = await storage.createEmailTemplate(req.body);
      res.json(template);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.patch("/api/om/email-templates/:id", async (req, res) => {
    try {
      const template = await storage.updateEmailTemplate(
        parseInt(req.params.id),
        req.body,
      );
      if (!template)
        return res.status(404).json({ error: "Template not found" });
      res.json(template);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.delete("/api/om/email-templates/:id", async (req, res) => {
    try {
      const deleted = await storage.deleteEmailTemplate(
        parseInt(req.params.id),
      );
      if (!deleted)
        return res.status(404).json({ error: "Template not found" });
      res.json({ success: true });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // ============ OM SHIPMENTS ============

  app.get("/api/om/shipments", async (req, res) => {
    try {
      const { status, orderId } = req.query;
      const shipments = await storage.getShipments({
        status: status as string,
        orderId: orderId ? parseInt(orderId as string) : undefined,
      });
      res.json(shipments);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/om/shipments", async (req, res) => {
    try {
      const shipment = await storage.createShipment(req.body);
      res.json(shipment);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.patch("/api/om/shipments/:id", async (req, res) => {
    try {
      const shipment = await storage.updateShipment(
        parseInt(req.params.id),
        req.body,
      );
      if (!shipment)
        return res.status(404).json({ error: "Shipment not found" });
      res.json(shipment);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // ============ OM SETTINGS (warehouse, packing slip, etc.) ============

  app.get("/api/om/settings/:key", async (req, res) => {
    try {
      const value = await storage.getOmSetting(req.params.key);
      res.json({ key: req.params.key, value: value || null });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.put("/api/om/settings/:key", async (req, res) => {
    try {
      await storage.setOmSetting(req.params.key, req.body.value);
      res.json({ success: true });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  return httpServer;
}

// ============================================================
// FERIANI/GIA FORMAT PARSER
// Format: DELIVERY, STYLE, COLOR, then size columns (2, 4, 6, 8...)
// Style rows have "NOW" in DELIVERY, style number, first color
// Additional colors for same style have only COLOR filled in
// ============================================================
function parseFerianiGiaFormat(
  buffer: Buffer,
  cleaningConfig: any,
): { headers: string[]; rows: any[][]; items: any[] } | null {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
  }) as any[][];

  if (data.length < 2) return null;

  // Check if this is Feriani/GIA format: DELIVERY, STYLE, COLOR headers
  const headerRow = data[0];
  const headerStr = headerRow
    .map((h: any) => String(h || "").toUpperCase())
    .join("|");

  if (
    !headerStr.includes("DELIVERY") ||
    !headerStr.includes("STYLE") ||
    !headerStr.includes("COLOR")
  ) {
    return null;
  }

  console.log(
    `[FerianiGia] Detected Feriani/GIA format with ${data.length} rows`,
  );

  // Find column indices
  const deliveryIdx = headerRow.findIndex(
    (h: any) => String(h || "").toUpperCase() === "DELIVERY",
  );
  const styleIdx = headerRow.findIndex(
    (h: any) => String(h || "").toUpperCase() === "STYLE",
  );
  const colorIdx = headerRow.findIndex(
    (h: any) => String(h || "").toUpperCase() === "COLOR",
  );

  // Find size columns (numeric headers after COLOR)
  const sizeColumns: { index: number; size: string }[] = [];
  for (let i = colorIdx + 1; i < headerRow.length; i++) {
    const h = headerRow[i];
    if (h !== null && h !== undefined && h !== "") {
      const hStr = String(h).trim();
      // Check if it's a numeric size
      if (/^\d+$/.test(hStr)) {
        sizeColumns.push({ index: i, size: hStr });
      }
    }
  }

  if (sizeColumns.length === 0) {
    console.log(`[FerianiGia] No size columns found`);
    return null;
  }

  console.log(
    `[FerianiGia] Found ${sizeColumns.length} size columns: ${sizeColumns.map((s) => s.size).join(", ")}`,
  );

  // Parse data
  const items: any[] = [];
  const masterData: any[][] = [];
  const outputHeaders = ["style", "color", "size", "stock", "shipDate"];

  let currentStyle = "";
  let currentDelivery = "";

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const delivery = String(row[deliveryIdx] || "")
      .trim()
      .toUpperCase();
    const style = String(row[styleIdx] || "").trim();
    const color = String(row[colorIdx] || "").trim();

    // Skip empty rows
    if (!color || color === "") continue;

    // Update current style if this row has one
    if (style && style !== "") {
      currentStyle = style;
    }

    // Update delivery status
    if (delivery && delivery !== "") {
      currentDelivery = delivery;
    }

    // Skip if no style yet
    if (!currentStyle) continue;

    // Parse ship date from delivery (could be "NOW", a date, etc.)
    let shipDate: string | null = null;
    if (
      currentDelivery &&
      currentDelivery !== "NOW" &&
      currentDelivery !== ""
    ) {
      // Try to parse as date
      const dateMatch = currentDelivery.match(
        /(\d{1,2})[\/-](\d{1,2})[\/-](\d{2,4})/,
      );
      if (dateMatch) {
        const [, month, day, year] = dateMatch;
        const fullYear = year.length === 2 ? `20${year}` : year;
        shipDate = `${fullYear}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
      }
    }

    // Extract stock for each size
    for (const sc of sizeColumns) {
      const stockVal = row[sc.index];
      let stock = 0;

      if (stockVal !== null && stockVal !== undefined && stockVal !== "") {
        const parsed = parseInt(String(stockVal), 10);
        if (!isNaN(parsed) && parsed > 0) {
          stock = parsed;
        }
      }

      // FIX: Always push ALL items regardless of stock (matching Tarik Ediz behavior)
      // This ensures size expansion can work with existing 0-stock items
      // Zero-stock filtering happens later in applyVariantRules if enabled
      const item = {
        style: currentStyle,
        color: color,
        size: sc.size,
        stock: stock,
        shipDate: shipDate,
        sku: `${currentStyle}-${color}-${sc.size}`.replace(/\s+/g, "-"),
        rawData: {
          style: currentStyle,
          color,
          size: sc.size,
          stock,
          delivery: currentDelivery,
        },
      };
      items.push(item);
      masterData.push([currentStyle, color, sc.size, stock, shipDate]);
    }
  }

  console.log(`[FerianiGia] Parsed ${items.length} items`);

  return {
    headers: outputHeaders,
    rows: masterData,
    items,
  };
}
