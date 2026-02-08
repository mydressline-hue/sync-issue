/**
 * AI Import Routes - Complete Universal Parser Implementation
 *
 * ALL 23+ vendor file formats supported:
 * - ROW formats (standard one-row-per-variant)
 * - PIVOT formats (sizes as column headers)
 * - Special formats (PR date headers, Sherri Hill alternating, OTS, etc.)
 *
 * Features:
 * - Auto-detection of format type from content and filename
 * - Configurable discontinued detection
 * - Configurable future date detection
 * - Text-to-numeric stock mapping
 * - Multi-brand file support
 * - Skip rows for title headers
 */

import { Router, Request, Response } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
import { storage } from "./storage";
import { detectFileFormat } from "./aiFormatDetection";
import {
  parseWithEnhancedConfig,
  EnhancedImportConfig,
} from "./enhancedImportProcessor";
import {
  validateImportResults,
  ValidationConfig,
  PostImportValidationResult,
  captureSourceChecksums,
  SourceChecksums,
  DataSourceRules,
  LastImportStats,
} from "./importValidator";
import {
  applyImportRules,
  applyVariantRules,
  applyPriceBasedExpansion,
  buildStylePriceMapFromCache,
  formatColorName,
  isValidShipDate,
} from "./inventoryProcessing";
import {
  filterDiscontinuedStyles,
  removeDiscontinuedInventoryItems,
} from "./importUtils";

const router = Router();
const upload = multer({ storage: multer.memoryStorage() });

// ============================================================
// TYPE DEFINITIONS
// ============================================================

interface PivotItem {
  style: string;
  color: string;
  size: string;
  stock: number;
  price?: number;
  discontinued?: boolean;
  shipDate?: string;
  incomingStock?: number;
  brand?: string;
}

interface DiscontinuedConfig {
  method:
    | "status_column"
    | "filename"
    | "d_flag"
    | "cl_prefix"
    | "keyword"
    | "none";
  column?: string | number;
  values?: string[];
  prefixValues?: string[];
  activeValues?: string[];
  invertLogic?: boolean;
}

interface FutureDateConfig {
  method:
    | "excel_serial"
    | "adjacent_columns"
    | "headers_as_dates"
    | "text_date"
    | "delivery_column"
    | "dual_columns"
    | "none";
  dateColumn?: string | number;
  futureStockColumn?: string;
  immediateStockColumn?: string;
  adjacentOffset?: number;
  nowValue?: string;
  dateFormat?: "excel" | "mm/dd/yyyy" | "yyyy-mm-dd" | "text";
}

interface StockConfig {
  type: "numeric" | "text" | "pivot";
  column?: string | number;
  textMappings?: Record<string, number>;
}

interface BrandDetectionConfig {
  enabled: boolean;
  sourceColumn: string | number;
  extractionMethod: "prefix" | "contains" | "regex";
  knownBrands?: string[];
  regex?: string;
}

interface UniversalParserConfig {
  skipRows?: number;
  discontinuedConfig?: DiscontinuedConfig;
  futureDateConfig?: FutureDateConfig;
  stockConfig?: StockConfig;
  brandDetection?: BrandDetectionConfig;
  pivotConfig?: any;
  columnMapping?: Record<string, string>;
}

// ============================================================
// UTILITY FUNCTIONS
// ============================================================

function excelSerialToDate(serial: number): string {
  if (!serial || serial < 40000 || serial > 55000) return "";
  const excelEpoch = new Date(1899, 11, 30);
  const jsDate = new Date(excelEpoch.getTime() + serial * 24 * 60 * 60 * 1000);
  return jsDate.toISOString().split("T")[0];
}

function parseStockValue(
  value: any,
  textMappings?:
    | Record<string, number>
    | Array<{ text: string; value: number }>,
): number {
  if (value === null || value === undefined || value === "") return 0;
  if (typeof value === "number") return Math.max(0, Math.floor(value));

  const strVal = String(value).trim().toLowerCase();

  const defaultMappings: Record<string, number> = {
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
    "": 0,
  };

  // Handle textMappings - can be either object or array format
  if (textMappings) {
    // Array format: [{ text: "Yes", value: 3 }]
    if (Array.isArray(textMappings)) {
      for (const mapping of textMappings) {
        if (mapping.text && mapping.text.toLowerCase().trim() === strVal) {
          return mapping.value;
        }
      }
    }
    // Object format: { "yes": 3 }
    else if (textMappings[strVal] !== undefined) {
      return textMappings[strVal];
    }
  }

  if (defaultMappings[strVal] !== undefined) return defaultMappings[strVal];

  const parsed = parseInt(strVal, 10);
  return isNaN(parsed) ? 0 : Math.max(0, parsed);
}

// ============================================================
// AUTO-DETECT PIVOT FORMAT
// ============================================================

function autoDetectPivotFormat(
  data: any[][],
  dataSourceName?: string,
  filename?: string,
): string | null {
  const nameUpper = (dataSourceName || "").toUpperCase();
  const fileUpper = (filename || "").toUpperCase();
  const combinedName = nameUpper + " " + fileUpper;

  // Check by name patterns
  if (
    combinedName.includes("JOVANI") &&
    (combinedName.includes("SALE") || fileUpper.includes("SALE"))
  )
    return "jovani_sale";
  if (combinedName.includes("FERIANI")) return "feriani";
  if (
    combinedName.includes("GIA") &&
    (combinedName.includes("FRANCO") || combinedName.includes("INV"))
  )
    return "feriani";
  if (
    combinedName.includes("TARIK") ||
    combinedName.includes("EDIZ") ||
    combinedName.includes("LISTINVENTORY")
  )
    return "tarik_ediz";
  if (combinedName.includes("SHERRI") || combinedName.includes("HILL"))
    return "sherri_hill";
  if (combinedName.includes("ALYCE")) return "generic_pivot";
  if (combinedName.includes("INESS") || combinedName.includes("COLETTE"))
    return "generic_pivot";
  if (
    combinedName.includes("PR-1") ||
    combinedName.includes("PR-2") ||
    combinedName.includes("PRINCESA")
  )
    return "pr_date_headers";
  if (combinedName.includes("GRN") || combinedName.includes("INVOICE"))
    return "grn_invoice";
  if (combinedName.includes("STORE") && combinedName.includes("INVENTORY"))
    return "store_multibrand";
  if (combinedName.includes("OTS") || fileUpper.includes("OTS_"))
    return "ots_format";

  if (data.length < 2) return null;

  // Check by content
  const firstRowText = String(data[0]?.[0] || "").toLowerCase();
  if (
    firstRowText.includes("up-to-date") ||
    firstRowText.includes("inventory report")
  )
    return "tarik_ediz";
  if (firstRowText.includes("grn") || firstRowText.includes("invoice"))
    return "grn_invoice";

  const headerRow = data[0];
  if (!headerRow) return null;

  const headers = headerRow.map((h: any) =>
    String(h || "")
      .toUpperCase()
      .trim(),
  );
  const headersLower = headerRow.map((h: any) =>
    String(h || "")
      .toLowerCase()
      .trim(),
  );
  const headerStr = headers.join("|");

  // OTS format detection
  if (headersLower.some((h: string) => /^ots\d+$/.test(h))) return "ots_format";

  // Sherri Hill
  if (headerStr.includes("SPECIAL DATE")) return "sherri_hill";

  // Feriani/Gia
  if (
    headerStr.includes("DELIVERY") &&
    headerStr.includes("STYLE") &&
    headerStr.includes("COLOR")
  )
    return "feriani";

  // PR Date Headers
  const dateHeaders = headers.filter((h: string) => /^4\d{4}$/.test(h));
  if (dateHeaders.length >= 3) return "pr_date_headers";

  // Generic Pivot
  const sizePattern =
    /^(000|00|OOO|OO|0|2|4|6|8|10|12|14|16|18|20|22|24|26|28|30)$/i;
  const sizeColumns = headers.filter((h: string) => sizePattern.test(h));

  if (sizeColumns.length >= 5) {
    if (headers.some((h: string) => h.includes("STYLE")))
      return "generic_pivot";
    const cell0 = String(headerRow[0] || "").trim();
    const cell1 = String(headerRow[1] || "").trim();
    if ((cell0 === "" || sizePattern.test(cell0)) && sizePattern.test(cell1))
      return "jovani_sale";
    return "generic_pivot";
  }

  return null;
}

// ============================================================
// INTELLIGENT PIVOT FORMAT PARSER
// ============================================================

function parseIntelligentPivotFormat(
  buffer: Buffer,
  formatType: string,
  config: UniversalParserConfig,
  dataSourceName?: string,
  filename?: string,
): { headers: string[]; rows: any[][]; items: PivotItem[] } {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rawData = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
  }) as any[][];

  const skipRows = config.skipRows || 0;
  const data = skipRows > 0 ? rawData.slice(skipRows) : rawData;

  const detectedFormat = autoDetectPivotFormat(data, dataSourceName, filename);
  console.log(
    `[IntelligentPivot] Requested: ${formatType}, Auto-detected: ${detectedFormat}`,
  );

  const actualFormat = detectedFormat || formatType;
  let items: PivotItem[] = [];

  switch (actualFormat) {
    case "feriani":
    case "pivot_grouped":
      items = parseFerianiFormat(data, config);
      break;
    case "jovani_sale":
    case "jovani":
    case "pivot_interleaved":
      items = parseJovaniSaleFormat(data, config);
      break;
    case "tarik_ediz":
      items = parseTarikEdizFormat(data, config);
      break;
    case "sherri_hill":
    case "pivot_alternating":
      items = parseSherriHillFormat(data, config);
      break;
    case "generic_pivot":
      items = parseGenericPivotFormat(data, config, filename);
      break;
    case "pr_date_headers":
      items = parsePRDateHeaderFormat(data, config);
      break;
    case "grn_invoice":
      items = parseGRNInvoiceFormat(rawData, config);
      break;
    case "store_multibrand":
      items = parseStoreMultibrandFormat(data, config);
      break;
    case "ots_format":
      items = parseOTSFormat(data, config);
      break;
    default:
      console.log(
        `[IntelligentPivot] Unknown format ${actualFormat}, trying parsers...`,
      );
      items = parseRowFormat(data, config, filename);
      if (items.length === 0)
        items = parseGenericPivotFormat(data, config, filename);
      if (items.length === 0) items = parseFerianiFormat(data, config);
      if (items.length === 0) items = parseJovaniSaleFormat(data, config);
  }

  console.log(
    `[IntelligentPivot] Parsed ${items.length} items using ${actualFormat} format`,
  );

  return {
    headers: [
      "style",
      "color",
      "size",
      "stock",
      "price",
      "discontinued",
      "shipDate",
    ],
    rows: items.map((i) => [
      i.style,
      i.color,
      i.size,
      i.stock,
      i.price || 0,
      i.discontinued,
      i.shipDate,
    ]),
    items,
  };
}

// ============================================================
// PARSER: FERIANI / GIA FRANCO FORMAT
// ============================================================

function parseFerianiFormat(
  data: any[][],
  config: UniversalParserConfig,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 2) return items;

  let headerRowIdx = 0;
  for (let i = 0; i < Math.min(5, data.length); i++) {
    const row = data[i];
    const rowStr = row.map((c: any) => String(c || "").toUpperCase()).join("|");
    if (rowStr.includes("STYLE") && rowStr.includes("COLOR")) {
      headerRowIdx = i;
      break;
    }
  }

  const headerRow = data[headerRowIdx];
  const headers = headerRow.map((h: any) =>
    String(h || "")
      .toUpperCase()
      .trim(),
  );

  const deliveryIdx = headers.findIndex((h: string) => h.includes("DELIVERY"));
  const styleIdx = headers.findIndex((h: string) => h.includes("STYLE"));
  const colorIdx = headers.findIndex((h: string) => h.includes("COLOR"));

  const sizePattern = /^(0|2|4|6|8|10|12|14|16|18|20|22|24|26|28|30)$/;
  const sizeColumns: { index: number; size: string }[] = [];

  for (let i = Math.max(colorIdx + 1, 3); i < headers.length; i++) {
    const h = String(headerRow[i] ?? "").trim();
    if (sizePattern.test(h)) sizeColumns.push({ index: i, size: h });
  }

  if (styleIdx === -1 || colorIdx === -1 || sizeColumns.length === 0)
    return items;

  let currentStyle = "",
    currentDelivery = "";

  for (let rowIdx = headerRowIdx + 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const styleVal = String(row[styleIdx] ?? "").trim();
    const colorVal = String(row[colorIdx] ?? "").trim();
    const deliveryVal =
      deliveryIdx >= 0 ? String(row[deliveryIdx] ?? "").trim() : "";

    if (styleVal) {
      currentStyle = styleVal;
      currentDelivery = deliveryVal;
    }

    if (!colorVal || !currentStyle) continue;

    let shipDate: string | undefined;
    const delivery = currentDelivery || deliveryVal;
    if (delivery && delivery.toUpperCase() !== "NOW") shipDate = delivery;

    for (const sc of sizeColumns) {
      const stock = parseStockValue(
        row[sc.index],
        config.stockConfig?.textMappings,
      );
      // FIX: Always push ALL items regardless of stock (matching Tarik Ediz behavior)
      // This ensures size expansion can work with existing 0-stock items
      // Zero-stock filtering happens later in applyVariantRules if enabled
      items.push({
        style: currentStyle,
        color: colorVal,
        size: sc.size,
        stock,
        shipDate,
      });
    }
  }

  return items;
}

// ============================================================
// PARSER: JOVANI SALE FORMAT (FIXED)
// ============================================================

function parseJovaniSaleFormat(
  data: any[][],
  config: UniversalParserConfig,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 2) return items;

  const headerRow = data[0];

  // Find size columns - sizes start at index 1
  const sizePattern = /^(00|0|2|4|6|8|10|12|14|16|18|20|22|24)$/;
  const sizeColumns: { index: number; size: string }[] = [];

  for (let i = 1; i < headerRow.length; i++) {
    const h = String(headerRow[i] ?? "").trim();
    if (sizePattern.test(h)) sizeColumns.push({ index: i, size: h });
  }

  if (sizeColumns.length === 0) return items;

  // Style patterns: #02861, JVN04759, 04859, AL02665, etc.
  const stylePattern = /^#?\d{4,6}$|^#?\d{5}[A-Z]?$|^[A-Z]{2,3}\d{4,6}$/i;

  let currentStyle = "";
  let currentPrice = 0;

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.every((c: any) => !c && c !== 0)) continue;

    const cell0 = String(row[0] ?? "").trim();
    const cell1 = row[1];

    // Check if this is a style row
    if (stylePattern.test(cell0)) {
      currentStyle = cell0.replace(/^#/, "");
      currentPrice =
        typeof cell1 === "number" ? cell1 : parseFloat(String(cell1 || "0"));
      continue;
    }

    // This is a color row if cell0 has text
    if (!currentStyle) continue;
    if (!cell0 || /^#?\d+$/.test(cell0)) continue;

    const colorVal = cell0;

    for (const sc of sizeColumns) {
      const stock = parseStockValue(
        row[sc.index],
        config.stockConfig?.textMappings,
      );
      if (stock > 0) {
        items.push({
          style: currentStyle,
          color: colorVal,
          size: sc.size,
          stock,
          price: currentPrice || undefined,
        });
      }
    }
  }

  return items;
}

// ============================================================
// PARSER: TARIK EDIZ FORMAT
// ============================================================

function parseTarikEdizFormat(
  data: any[][],
  config: UniversalParserConfig,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 5) return items;

  let dataStartIdx = 0;
  const stylePattern = /^\d{2}[A-Z]{2,}/i;

  for (let i = 0; i < Math.min(15, data.length); i++) {
    const cell = String(data[i]?.[0] || "").trim();
    if (stylePattern.test(cell) || cell === "D" || cell === "01") {
      dataStartIdx = i;
      break;
    }
  }

  const sizePattern = /^(0|2|4|6|8|10|12|14|16|18|20|22|24)$/;
  let currentStyle = "";
  let currentSizeHeaders: string[] = [];

  for (let rowIdx = dataStartIdx; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 5) continue;

    const cell0 = String(row[0] ?? "").trim();

    if (stylePattern.test(cell0) || /^\d{5,}$/.test(cell0)) {
      currentStyle = cell0;
      currentSizeHeaders = [];
      for (let i = 13; i < Math.min(30, row.length); i++) {
        const h = String(row[i] ?? "").trim();
        if (sizePattern.test(h)) currentSizeHeaders.push(h);
      }
      continue;
    }

    const isDiscontinued = cell0 === "D";
    if (!currentStyle || currentSizeHeaders.length === 0) continue;

    const colorVal = String(row[11] ?? row[3] ?? "").trim();
    if (!colorVal) continue;

    for (let i = 0; i < currentSizeHeaders.length; i++) {
      const stock = parseStockValue(
        row[13 + i],
        config.stockConfig?.textMappings,
      );
      items.push({
        style: currentStyle,
        color: colorVal,
        size: currentSizeHeaders[i],
        stock,
        discontinued: isDiscontinued,
      });
    }
  }

  return items;
}

// ============================================================
// PARSER: SHERRI HILL FORMAT
// ============================================================

function parseSherriHillFormat(
  data: any[][],
  config: UniversalParserConfig,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 2) return items;

  // DEBUG: Log stockConfig to diagnose text mappings issue
  console.log(`[SherriHill] stockConfig:`, JSON.stringify(config.stockConfig));
  console.log(
    `[SherriHill] textMappings:`,
    JSON.stringify(config.stockConfig?.textMappings),
  );

  const headerRow = data[0];
  const sizePattern =
    /^(OO0|OOO|OO|0|2|4|6|8|10|12|14|16|18|20|22|24|26|28|30)$/i;
  const sizeColumns: { index: number; size: string; dateIndex: number }[] = [];

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

  if (sizeColumns.length === 0) return items;

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const style = String(row[0] ?? "").trim();
    const color = String(row[1] ?? "").trim();
    if (!style || !color) continue;

    for (const sc of sizeColumns) {
      const stock = parseStockValue(
        row[sc.index],
        config.stockConfig?.textMappings,
      );
      const dateVal = row[sc.dateIndex];
      let shipDate: string | undefined;

      if (
        dateVal &&
        dateVal !== "&ndash;" &&
        dateVal !== "&ndash; " &&
        dateVal !== "–"
      ) {
        if (typeof dateVal === "number" && dateVal > 40000) {
          shipDate = excelSerialToDate(dateVal);
        }
      }

      if (stock > 0 || (shipDate && isValidShipDate(shipDate))) {
        items.push({ style, color, size: sc.size, stock, shipDate });
      }
    }
  }

  return items;
}

// ============================================================
// PARSER: GENERIC PIVOT FORMAT (Alyce, INESS, Styles Available)
// ============================================================

function parseGenericPivotFormat(
  data: any[][],
  config: UniversalParserConfig,
  filename?: string,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 2) return items;

  let headerRowIdx = 0;
  const sizePattern =
    /^(000|00|OOO|OO|0|2|4|6|8|10|12|14|16|18|20|22|24|26|28|30)$/i;

  for (let i = 0; i < Math.min(5, data.length); i++) {
    const row = data[i];
    const sizeCount = row.filter((c: any) =>
      sizePattern.test(String(c ?? "").trim()),
    ).length;
    if (sizeCount >= 5) {
      headerRowIdx = i;
      break;
    }
  }

  const headerRow = data[headerRowIdx];
  const headers = headerRow.map((h: any) => String(h ?? "").trim());
  const headersUpper = headers.map((h: string) => h.toUpperCase());
  const headersLower = headers.map((h: string) => h.toLowerCase());

  const styleIdx = headersUpper.findIndex(
    (h: string) => h.includes("STYLE") || h === "CODE" || h === "ITEM",
  );
  const colorIdx = headersUpper.findIndex(
    (h: string) => h.includes("COLOR") && !h.includes("CODE"),
  );
  const dateIdx = headersUpper.findIndex(
    (h: string) =>
      h.includes("DATE") ||
      h.includes("ETA") ||
      h.includes("DUE") ||
      h.includes("AVAILABLE"),
  );

  // For discontinued: first check user-mapped column, then fallback to auto-detect
  let statusIdx = -1;
  if (config.columnMapping?.discontinued) {
    const mappedCol = config.columnMapping.discontinued.toLowerCase().trim();
    statusIdx = headersLower.findIndex((h: string) => h === mappedCol);
  }
  if (statusIdx === -1) {
    statusIdx = headersUpper.findIndex(
      (h: string) =>
        h.includes("STATUS") ||
        h.includes("DISCONTINUED") ||
        h.includes("ACTIVE"),
    );
  }

  // Use configured keywords (from UI) or fallback to defaults
  // Check both 'keywords' (new UI format) and 'values' (old format)
  const configKeywords =
    (config.discontinuedConfig as any)?.keywords ||
    config.discontinuedConfig?.values;
  const discontinuedKeywords = configKeywords?.length
    ? configKeywords.map((v: string) => v.toLowerCase().trim())
    : ["discontinued", "disc", "inactive", "d", "no", "n", "false", "0", "cl"];

  const sizeColumns: { index: number; size: string }[] = [];
  for (let i = 0; i < headers.length; i++) {
    const h = String(headerRow[i] ?? "").trim();
    if (sizePattern.test(h)) {
      let normalizedSize = h;
      if (h.toUpperCase() === "OOO") normalizedSize = "000";
      else if (h.toUpperCase() === "OO") normalizedSize = "00";
      sizeColumns.push({ index: i, size: normalizedSize });
    }
  }

  if (styleIdx === -1 || sizeColumns.length === 0) return items;

  const isFileDiscontinued = filename
    ? filename.toLowerCase().includes("discontinued")
    : false;

  for (let rowIdx = headerRowIdx + 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const style = String(row[styleIdx] ?? "").trim();
    const color = colorIdx >= 0 ? String(row[colorIdx] ?? "").trim() : "";
    if (!style) continue;

    let shipDate: string | undefined;
    if (dateIdx >= 0) {
      const dateVal = row[dateIdx];
      if (dateVal && typeof dateVal === "number" && dateVal > 40000) {
        shipDate = excelSerialToDate(dateVal);
      }
    }

    let isDiscontinued = isFileDiscontinued;
    if (statusIdx >= 0 && !isDiscontinued) {
      const statusVal = String(row[statusIdx] ?? "")
        .toLowerCase()
        .trim();
      isDiscontinued = discontinuedKeywords.some(
        (k) =>
          statusVal === k || statusVal.includes(k) || statusVal.startsWith(k),
      );
    }

    for (const sc of sizeColumns) {
      const stock = parseStockValue(
        row[sc.index],
        config.stockConfig?.textMappings,
      );
      if (
        stock > 0 ||
        (shipDate && isValidShipDate(shipDate)) ||
        isDiscontinued
      ) {
        items.push({
          style,
          color: color || "DEFAULT",
          size: sc.size,
          stock,
          shipDate,
          discontinued: isDiscontinued,
        });
      }
    }
  }

  return items;
}

// ============================================================
// PARSER: PR DATE HEADERS FORMAT (PR-1, PR-2)
// ============================================================

function parsePRDateHeaderFormat(
  data: any[][],
  config: UniversalParserConfig,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 2) return items;

  const headerRow = data[0];
  const headers = headerRow.map((h: any) => String(h ?? "").trim());

  const styleIdx = headers.findIndex(
    (h: string) =>
      h.toLowerCase().includes("product") || h.toLowerCase().includes("code"),
  );
  const availableIdx = headers.findIndex((h: string) =>
    h.toLowerCase().includes("available"),
  );

  // FIX: Detect date columns - support BOTH Excel serial numbers AND human-readable dates (M/D/YY)
  const dateColumns: { index: number; date: string }[] = [];
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];

    // Check for Excel serial date numbers (e.g., "46068")
    if (/^4\d{4}$/.test(h)) {
      const dateStr = excelSerialToDate(parseInt(h, 10));
      if (dateStr) dateColumns.push({ index: i, date: dateStr });
      continue;
    }

    // FIX: Check for human-readable date strings (M/D/YYYY, MM/DD/YYYY, M/D/YY, MM/DD/YY)
    const dateMatch = h.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
    if (dateMatch) {
      const month = dateMatch[1].padStart(2, "0");
      const day = dateMatch[2].padStart(2, "0");
      let year = dateMatch[3];
      if (year.length === 2) {
        year = (parseInt(year, 10) >= 50 ? "19" : "20") + year;
      }
      const dateStr = `${year}-${month}-${day}`;
      dateColumns.push({ index: i, date: dateStr });
    }
  }

  console.log(
    `[PRDateHeaders-AI] Found columns: style=${styleIdx}, available=${availableIdx}, dateColumns=${dateColumns.length}`,
  );
  if (dateColumns.length > 0) {
    console.log(
      `[PRDateHeaders-AI] Date columns: ${dateColumns.map((d) => d.date).join(", ")}`,
    );
  }

  if (styleIdx === -1) return items;

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 2) continue;

    const rawCode = String(row[styleIdx] ?? "").trim();
    if (!rawCode) continue;

    // FIX: Split composite product code: STYLE-COLOR-SIZE (e.g., "PS26322E-IVBH-06")
    const parts = rawCode.split("-");
    let style = rawCode;
    let color = "";
    let extractedSize = "";

    if (parts.length >= 3) {
      // Last part = size, second-to-last = color, rest = style
      extractedSize = parts[parts.length - 1] || "";
      color = parts[parts.length - 2] || "";
      style = parts.slice(0, parts.length - 2).join("-") || "";
    } else if (parts.length === 2) {
      style = parts[0] || "";
      extractedSize = parts[1] || "";
    }

    // FIX: Normalize leading-zero sizes: "06" → "6", but preserve "0" and "00"
    if (extractedSize && /^0+[1-9]\d*$/.test(extractedSize)) {
      extractedSize = extractedSize.replace(/^0+/, "");
    }

    const currentStock =
      availableIdx >= 0
        ? parseStockValue(row[availableIdx], config.stockConfig?.textMappings)
        : 0;
    if (currentStock > 0) {
      const size = extractedSize || "ONE SIZE";
      // FIX: Use extracted color instead of hardcoded "DEFAULT"
      const sku = `${style}-${color || "DEFAULT"}-${size}`
        .replace(/\//g, "-")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-");

      items.push({
        style,
        color: color || "DEFAULT",
        size,
        stock: currentStock,
      });
    }

    for (const dc of dateColumns) {
      const futureStock = parseStockValue(
        row[dc.index],
        config.stockConfig?.textMappings,
      );
      if (futureStock > 0) {
        const size = extractedSize || "ONE SIZE";
        const sku = `${style}-${color || "DEFAULT"}-${size}`
          .replace(/\//g, "-")
          .replace(/\s+/g, "-")
          .replace(/-+/g, "-");

        items.push({
          style,
          color: color || "DEFAULT",
          size,
          stock: 0,
          incomingStock: futureStock,
          shipDate: dc.date,
        });
      }
    }
  }

  console.log(
    `[PRDateHeaders-AI] Parsed ${items.length} items (${items.filter((i) => i.shipDate).length} future stock)`,
  );
  return items;
}

// ============================================================
// PARSER: GRN-INVOICE FORMAT
// ============================================================

function parseGRNInvoiceFormat(
  rawData: any[][],
  config: UniversalParserConfig,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (rawData.length < 3) return items;

  let headerRowIdx = 0;
  for (let i = 0; i < Math.min(5, rawData.length); i++) {
    const row = rawData[i];
    const rowStr = row.map((c: any) => String(c || "").toLowerCase()).join("|");
    if (rowStr.includes("code") && rowStr.includes("color")) {
      headerRowIdx = i;
      break;
    }
  }

  const data = rawData.slice(headerRowIdx);
  if (data.length < 2) return items;

  const headerRow = data[0];
  const headersLower = headerRow.map((h: any) =>
    String(h ?? "")
      .toLowerCase()
      .trim(),
  );

  const codeIdx = headersLower.findIndex((h: string) => h === "code");
  const colorIdx = headersLower.findIndex((h: string) => h === "color");

  const sizePattern = /^(000|00|0|02|04|06|08|10|12|14|16|18|20|22|24)$/i;
  const sizeColumns: { index: number; size: string }[] = [];

  for (let i = 0; i < headerRow.length; i++) {
    const h = String(headerRow[i] ?? "").trim();
    if (sizePattern.test(h)) {
      let normalizedSize = h;
      if (/^0\d$/.test(h)) normalizedSize = h.replace(/^0/, "");
      sizeColumns.push({ index: i, size: normalizedSize });
    }
  }

  if (codeIdx === -1 || sizeColumns.length === 0) return items;

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const code = String(row[codeIdx] ?? "").trim();
    const color = colorIdx >= 0 ? String(row[colorIdx] ?? "").trim() : "";
    if (!code) continue;

    for (const sc of sizeColumns) {
      const stock = parseStockValue(
        row[sc.index],
        config.stockConfig?.textMappings,
      );
      if (stock > 0) {
        items.push({
          style: code,
          color: color || "DEFAULT",
          size: sc.size,
          stock,
        });
      }
    }
  }

  return items;
}

// ============================================================
// PARSER: OTS FORMAT (NEW)
// ============================================================

function parseOTSFormat(
  data: any[][],
  config: UniversalParserConfig,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 2) return items;

  const headerRow = data[0];
  const headers = headerRow.map((h: any) =>
    String(h ?? "")
      .trim()
      .toLowerCase(),
  );

  const styleIdx = headers.findIndex((h: string) => h === "style");
  const colorIdx = headers.findIndex((h: string) => h === "color");
  const priceIdx = headers.findIndex((h: string) => h.includes("price"));
  const sizeCompIdx = headers.findIndex(
    (h: string) => h.includes("size_whole") || h.includes("size"),
  );

  // Find OTS columns (ots1, ots2, etc.)
  const otsColumns: { index: number; num: number }[] = [];
  for (let i = 0; i < headers.length; i++) {
    const match = headers[i].match(/^ots(\d+)$/);
    if (match) otsColumns.push({ index: i, num: parseInt(match[1], 10) });
  }
  otsColumns.sort((a, b) => a.num - b.num);

  if (styleIdx === -1 || otsColumns.length === 0) return items;

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const style = String(row[styleIdx] ?? "").trim();
    const color = colorIdx >= 0 ? String(row[colorIdx] ?? "").trim() : "";
    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;
    if (!style) continue;

    // Parse sizes from size_whole_comp column
    let sizes: string[] = [];
    if (sizeCompIdx >= 0) {
      const sizeStr = String(row[sizeCompIdx] ?? "");
      sizes = sizeStr
        .trim()
        .split(/\s+/)
        .filter((s: string) => /^\d+$/.test(s));
    }
    if (sizes.length === 0)
      sizes = ["2", "4", "6", "8", "10", "12", "14", "16", "18"];

    // Map OTS columns to sizes
    for (let i = 0; i < Math.min(otsColumns.length, sizes.length); i++) {
      const stock = parseStockValue(
        row[otsColumns[i].index],
        config.stockConfig?.textMappings,
      );
      if (stock > 0) {
        items.push({
          style,
          color: color || "DEFAULT",
          size: sizes[i],
          stock,
          price,
        });
      }
    }
  }

  return items;
}

// ============================================================
// PARSER: STORE MULTI-BRAND FORMAT
// ============================================================

function parseStoreMultibrandFormat(
  data: any[][],
  config: UniversalParserConfig,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 2) return items;

  const headerRow = data[0];
  const headers = headerRow.map((h: any) => String(h ?? "").trim());
  const headersLower = headers.map((h: string) => h.toLowerCase());

  const productNameIdx = headersLower.findIndex(
    (h: string) => h.includes("product") && h.includes("name"),
  );
  const styleIdx = headersLower.findIndex((h: string) => h === "style");
  const colorIdx = headersLower.findIndex((h: string) => h === "color");
  const sizeIdx = headersLower.findIndex((h: string) => h === "size");
  const stockIdx = headersLower.findIndex(
    (h: string) => h === "stock" || h.includes("qty"),
  );
  const priceIdx = headersLower.findIndex((h: string) => h === "price");

  if (styleIdx === -1) return items;

  const knownBrands = [
    "Jovani",
    "Sherri Hill",
    "Mac Duggal",
    "MacDuggal",
    "Terani",
    "Tarik Ediz",
    "Feriani",
    "Gia Franco",
    "Alyce",
    "Portia",
    "Mon Cheri",
    "Morilee",
    "Jadore",
    "Lara",
    "Johnathan Kayne",
    "Rachel Allan",
    "Colors Dress",
    "Colette",
    "Marsoni",
    "Cameron Blake",
    "La Femme",
    "MGNY",
    "Nicoletta",
    "Montage",
    "Tony Bowls",
  ];

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const style = String(row[styleIdx] ?? "").trim();
    if (!style) continue;

    const productName =
      productNameIdx >= 0 ? String(row[productNameIdx] ?? "").trim() : "";
    const color = colorIdx >= 0 ? String(row[colorIdx] ?? "").trim() : "";
    const size = sizeIdx >= 0 ? String(row[sizeIdx] ?? "").trim() : "ONE SIZE";
    const stock =
      stockIdx >= 0
        ? parseStockValue(row[stockIdx], config.stockConfig?.textMappings)
        : 0;
    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;

    let brand: string | undefined;
    if (productName) {
      const nameLower = productName.toLowerCase();
      for (const b of knownBrands) {
        if (nameLower.includes(b.toLowerCase())) {
          brand = b;
          break;
        }
      }
    }

    items.push({ style, color: color || "DEFAULT", size, stock, price, brand });
  }

  return items;
}

// ============================================================
// PARSER: ROW FORMAT (Standard one-row-per-variant)
// ============================================================

function parseRowFormat(
  data: any[][],
  config: UniversalParserConfig,
  filename?: string,
): PivotItem[] {
  const items: PivotItem[] = [];
  if (data.length < 2) return items;

  const headerRow = data[0];
  const headers = headerRow.map((h: any) => String(h ?? "").trim());
  const headersLower = headers.map((h: string) => h.toLowerCase());

  const findColumn = (patterns: string[]): number => {
    for (const p of patterns) {
      const idx = headersLower.findIndex(
        (h: string) => h === p || h.includes(p),
      );
      if (idx !== -1) return idx;
    }
    return -1;
  };

  const styleIdx = findColumn([
    "style",
    "style#",
    "item",
    "product_id",
    "product",
    "code",
    "sku",
  ]);
  const colorIdx = findColumn([
    "color",
    "colour",
    "_color_name",
    "color_descript",
  ]);
  const sizeIdx = findColumn(["size", "_size", "sizename"]);
  const stockIdx = findColumn([
    "stock",
    "qty",
    "quantity",
    "available",
    "onhand",
    "ats_qty",
    "opentosale",
    "inventory",
    "_inventory_level",
    "immediate stock",
  ]);
  const priceIdx = findColumn([
    "price",
    "wholesale",
    "cost",
    "line price",
    "msrp",
    "_price",
  ]);
  const dateIdx = findColumn([
    "eta",
    "ship",
    "date",
    "arrival",
    "expected",
    "future ship",
  ]);

  // For discontinued: first check user-mapped column, then fallback to auto-detect
  let statusIdx = -1;
  if (config.columnMapping?.discontinued) {
    const mappedCol = config.columnMapping.discontinued.toLowerCase().trim();
    statusIdx = headersLower.findIndex((h: string) => h === mappedCol);
  }
  if (statusIdx === -1) {
    statusIdx = findColumn(["status", "discontinued", "active", "_status"]);
  }

  if (styleIdx === -1) return items;

  // Use configured keywords (from UI) or fallback to defaults
  // Check both 'keywords' (new UI format) and 'values' (old format)
  const configKeywords =
    (config.discontinuedConfig as any)?.keywords ||
    config.discontinuedConfig?.values;
  const discontinuedPatterns = configKeywords?.length
    ? configKeywords.map((v: string) => v.toLowerCase().trim())
    : ["discontinued", "disc", "inactive", "d", "no", "n", "false", "0"];

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 2) continue;

    const style = String(row[styleIdx] ?? "").trim();
    if (!style) continue;

    const color =
      colorIdx >= 0 ? String(row[colorIdx] ?? "").trim() : "DEFAULT";
    const size = sizeIdx >= 0 ? String(row[sizeIdx] ?? "").trim() : "ONE SIZE";
    const stock =
      stockIdx >= 0
        ? parseStockValue(row[stockIdx], config.stockConfig?.textMappings)
        : 0;
    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;

    let shipDate: string | undefined;
    if (dateIdx >= 0) {
      const dateVal = row[dateIdx];
      if (dateVal && typeof dateVal === "number" && dateVal > 40000) {
        shipDate = excelSerialToDate(dateVal);
      } else if (dateVal && typeof dateVal === "string" && dateVal.trim()) {
        shipDate = dateVal.trim();
      }
    }

    let discontinued = false;
    if (statusIdx >= 0) {
      const statusVal = String(row[statusIdx] ?? "")
        .toLowerCase()
        .trim();
      discontinued = discontinuedPatterns.some(
        (p) =>
          statusVal === p || statusVal.startsWith(p) || statusVal.includes(p),
      );
    }

    if (stock > 0 || (shipDate && isValidShipDate(shipDate)) || discontinued) {
      items.push({ style, color, size, stock, price, shipDate, discontinued });
    }
  }

  return items;
}

// ============================================================
// FORMAT DETECTION ENDPOINT
// ============================================================

router.post("/analyze", upload.any(), async (req: Request, res: Response) => {
  try {
    const files = req.files as Express.Multer.File[];
    if (!files || files.length === 0)
      return res.status(400).json({ error: "No file uploaded" });

    // Consolidate multiple files if in multi-file mode
    let rawData: any[][] = [];
    let primaryFile = files[0];

    if (files.length > 1 && req.body.multiFileMode === "true") {
      console.log(
        `[AIImport] Multi-file analyze: consolidating ${files.length} files`,
      );
      let headerRow: any[] | null = null;

      for (const file of files) {
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
          rawData.push(...data.slice(1));
        }
      }
      console.log(
        `[AIImport] Consolidated ${rawData.length} total rows from ${files.length} files`,
      );
    } else {
      const workbook = XLSX.read(primaryFile.buffer, { type: "buffer" });
      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      rawData = XLSX.utils.sheet_to_json(sheet, {
        header: 1,
        defval: "",
      }) as any[][];
    }

    let dataSourceName = "";
    if (req.body.dataSourceId) {
      const ds = await storage.getDataSource(req.body.dataSourceId);
      dataSourceName = ds?.name || "";
    }

    const pivotFormat = autoDetectPivotFormat(
      rawData,
      dataSourceName,
      primaryFile.originalname,
    );

    if (pivotFormat) {
      return res.json({
        detection: {
          success: true,
          formatType: pivotFormat.includes("pivot")
            ? pivotFormat
            : `pivot_${pivotFormat}`,
          formatConfidence: 95,
          confidence: 95,
          columnMapping: {},
          suggestedColumnMapping: {},
          pivotConfig: { enabled: true, format: pivotFormat },
          notes: [`Auto-detected ${pivotFormat} pivot format`],
          warnings: [],
          columns: [],
          detectedPatterns: {
            hasDiscontinuedIndicators: false,
            hasDateColumns: false,
            hasTextStockValues: false,
            hasPriceColumn: false,
          },
        },
      });
    }

    // Try AI detection, but fall back to basic detection if it fails
    let analysisResult;
    try {
      analysisResult = await detectFileFormat(
        primaryFile.buffer,
        primaryFile.originalname,
        req.body.dataSourceId || undefined,
      );
    } catch (aiError: any) {
      console.error(
        "[AIImport] AI analysis failed, using basic detection:",
        aiError.message,
      );
      analysisResult = null;
    }

    // If AI failed or returned invalid result, provide basic detection
    if (!analysisResult || !analysisResult.suggestedColumnMapping) {
      const headers = rawData[0] || [];
      const headersLower = headers.map((h: any) =>
        String(h || "").toLowerCase(),
      );

      // Basic column mapping
      const basicMapping: any = {};
      const styleIdx = headersLower.findIndex(
        (h: string) => h.includes("style") || h === "code" || h === "item",
      );
      const colorIdx = headersLower.findIndex((h: string) =>
        h.includes("color"),
      );
      const sizeIdx = headersLower.findIndex((h: string) => h.includes("size"));
      const stockIdx = headersLower.findIndex(
        (h: string) =>
          h.includes("stock") || h.includes("qty") || h.includes("available"),
      );
      const priceIdx = headersLower.findIndex((h: string) =>
        h.includes("price"),
      );

      if (styleIdx >= 0) basicMapping.style = headers[styleIdx];
      if (colorIdx >= 0) basicMapping.color = headers[colorIdx];
      if (sizeIdx >= 0) basicMapping.size = headers[sizeIdx];
      if (stockIdx >= 0) basicMapping.stock = headers[stockIdx];
      if (priceIdx >= 0) basicMapping.price = headers[priceIdx];

      return res.json({
        detection: {
          success: true,
          formatType: "row",
          formatConfidence: 60,
          confidence: 60,
          columnMapping: basicMapping,
          suggestedColumnMapping: basicMapping,
          pivotConfig: null,
          notes: ["Used basic column detection (AI unavailable)"],
          warnings: [],
          columns: headers.map((h: any, i: number) => ({
            headerName: String(h || ""),
            columnIndex: i,
          })),
          detectedPatterns: {
            hasDiscontinuedIndicators: false,
            hasDateColumns: false,
            hasTextStockValues: false,
            hasPriceColumn: priceIdx >= 0,
          },
        },
      });
    }

    res.json({ detection: analysisResult });
  } catch (error: any) {
    console.error("[AIImport] Analysis error:", error);
    res.status(500).json({
      detection: {
        success: false,
        error: error.message || "Analysis failed",
        formatType: "row",
        formatConfidence: 0,
        suggestedColumnMapping: {},
        columnMapping: {},
        columns: [],
        notes: [],
        warnings: [error.message || "Analysis failed"],
        detectedPatterns: {
          hasDiscontinuedIndicators: false,
          hasDateColumns: false,
          hasTextStockValues: false,
          hasPriceColumn: false,
        },
      },
    });
  }
});

// ============================================================
// PREVIEW ENDPOINT
// ============================================================

router.post(
  "/preview",
  upload.single("file"),
  async (req: Request, res: Response) => {
    try {
      if (!req.file) return res.status(400).json({ error: "No file uploaded" });

      const dataSourceId = req.body.dataSourceId;
      const configOverride = req.body.config
        ? JSON.parse(req.body.config)
        : null;

      let dataSource = null;
      let enhancedConfig: any = {};

      if (dataSourceId) {
        dataSource = await storage.getDataSource(dataSourceId);
        if (dataSource) {
          enhancedConfig = {
            formatType: (dataSource as any).formatType,
            columnMapping: dataSource.columnMapping,
            pivotConfig: (dataSource as any).pivotConfig,
            discontinuedConfig:
              (dataSource as any).discontinuedConfig ||
              (dataSource as any).discontinuedRules,
            futureStockConfig: (dataSource as any).futureStockConfig,
            // CRITICAL FIX: stockValueConfig column doesn't exist in schema!
            // Fall back to cleaningConfig.stockTextMappings which is where UI saves the data
            stockValueConfig:
              (dataSource as any).stockValueConfig ||
              (dataSource.cleaningConfig?.stockTextMappings?.length > 0
                ? { textMappings: dataSource.cleaningConfig.stockTextMappings }
                : undefined),
          };
          // DEBUG: Log what's being loaded for stock text mappings
          console.log(
            `[AIImport] dataSource.stockValueConfig:`,
            JSON.stringify((dataSource as any).stockValueConfig),
          );
          console.log(
            `[AIImport] dataSource.cleaningConfig?.stockTextMappings:`,
            JSON.stringify(dataSource.cleaningConfig?.stockTextMappings),
          );
          console.log(
            `[AIImport] enhancedConfig.stockValueConfig:`,
            JSON.stringify(enhancedConfig.stockValueConfig),
          );
        }
      }

      const config: EnhancedImportConfig = {
        formatType:
          configOverride?.formatType || enhancedConfig.formatType || "row",
        columnMapping:
          configOverride?.columnMapping || enhancedConfig.columnMapping || {},
        pivotConfig: configOverride?.pivotConfig || enhancedConfig.pivotConfig,
        discontinuedConfig:
          configOverride?.discontinuedConfig ||
          enhancedConfig.discontinuedConfig,
        futureStockConfig:
          configOverride?.futureStockConfig || enhancedConfig.futureStockConfig,
        stockValueConfig:
          configOverride?.stockValueConfig || enhancedConfig.stockValueConfig,
      };

      const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      const rawData = XLSX.utils.sheet_to_json(sheet, {
        header: 1,
        defval: "",
      }) as any[][];

      const detectedPivotFormat = autoDetectPivotFormat(
        rawData,
        dataSource?.name,
        req.file.originalname,
      );
      const isPivotFormat =
        config.formatType?.startsWith("pivot") ||
        config.formatType === "pivoted" ||
        detectedPivotFormat !== null;

      let parseResult: any;

      if (isPivotFormat) {
        const actualFormat =
          detectedPivotFormat || config.formatType || "pivot_interleaved";
        const universalConfig: UniversalParserConfig = {
          skipRows: config.pivotConfig?.skipRows,
          discontinuedConfig: config.discontinuedConfig as any,
          futureDateConfig: config.futureStockConfig as any,
          stockConfig: config.stockValueConfig as any,
          columnMapping: config.columnMapping,
        };

        const pivotResult = parseIntelligentPivotFormat(
          req.file.buffer,
          actualFormat,
          universalConfig,
          dataSource?.name,
          req.file.originalname,
        );

        parseResult = {
          success: true,
          items: pivotResult.items,
          stats: {
            totalRows: pivotResult.rows.length,
            validItems: pivotResult.items.length,
            discontinuedItems: pivotResult.items.filter(
              (i: any) => i.discontinued,
            ).length,
            futureStockItems: pivotResult.items.filter((i: any) => i.shipDate)
              .length,
          },
          warnings: [],
        };
      } else {
        parseResult = await parseWithEnhancedConfig(
          req.file.buffer,
          config,
          dataSourceId,
        );
      }

      if (!parseResult.success) {
        return res.status(400).json({
          success: false,
          error: "Failed to parse file",
          warnings: parseResult.warnings,
        });
      }

      // Calculate unique styles
      const uniqueStyles = new Set(
        parseResult.items.map((item: any) => item.style),
      ).size;

      // Calculate stats in the format frontend expects
      const stats = {
        totalRows: parseResult.stats?.totalRows || parseResult.items.length,
        totalItems: parseResult.items.length,
        validItems: parseResult.stats?.validItems || parseResult.items.length,
        discontinuedItems:
          parseResult.stats?.discontinuedItems ||
          parseResult.items.filter((i: any) => i.discontinued).length,
        futureStockItems:
          parseResult.stats?.futureStockItems ||
          parseResult.items.filter((i: any) => i.shipDate).length,
        saleItems: parseResult.items.filter((i: any) => i.salePrice || i.price)
          .length,
        expandedSizes: parseResult.items.filter((i: any) => i.isExpandedSize)
          .length,
        complexStockParsed: 0,
      };

      // Wrap response in the structure frontend expects
      res.json({
        success: true,
        preview: {
          stats,
          sampleItems: parseResult.items.slice(0, 100),
          uniqueStyles,
        },
        warnings: parseResult.warnings || [],
      });
    } catch (error: any) {
      console.error("[AIImport] Preview error:", error);
      res
        .status(500)
        .json({ success: false, error: error.message || "Preview failed" });
    }
  },
);

// ============================================================
// EXECUTE IMPORT ENDPOINT
// ============================================================

router.post("/execute", upload.any(), async (req: Request, res: Response) => {
  try {
    console.log("[AIImport] /execute route HIT - request received");
    const files = req.files as Express.Multer.File[];
    if (!files || files.length === 0)
      return res.status(400).json({ error: "No file uploaded" });

    const dataSourceId = req.body.dataSourceId;
    const overrideConfig = req.body.config ? JSON.parse(req.body.config) : null;

    if (!dataSourceId)
      return res.status(400).json({ error: "Data source ID required" });

    const dataSource = await storage.getDataSource(dataSourceId);
    if (!dataSource)
      return res.status(404).json({ error: "Data source not found" });

    // DEBUG: Log stockInfoConfig from BOTH sources
    console.log(`[AIImport] Loaded dataSource "${dataSource.name}"`);
    console.log(
      `[AIImport]   - DB stockInfoConfig: ${(dataSource as any).stockInfoConfig ? JSON.stringify((dataSource as any).stockInfoConfig) : "NULL"}`,
    );
    console.log(
      `[AIImport]   - overrideConfig.stockInfoConfig: ${overrideConfig?.stockInfoConfig ? JSON.stringify(overrideConfig.stockInfoConfig) : "NULL"}`,
    );
    console.log(
      `[AIImport]   - overrideConfig keys: ${overrideConfig ? Object.keys(overrideConfig).join(", ") : "NO overrideConfig"}`,
    );

    const enhancedConfig: any = {
      formatType: (dataSource as any).formatType,
      columnMapping: dataSource.columnMapping,
      pivotConfig: (dataSource as any).pivotConfig,
      discontinuedConfig:
        (dataSource as any).discontinuedConfig ||
        (dataSource as any).discontinuedRules,
      futureStockConfig: (dataSource as any).futureStockConfig,
      // CRITICAL FIX: stockValueConfig column doesn't exist in schema!
      // Fall back to cleaningConfig.stockTextMappings which is where UI saves the data
      stockValueConfig:
        (dataSource as any).stockValueConfig ||
        (dataSource.cleaningConfig?.stockTextMappings?.length > 0
          ? { textMappings: dataSource.cleaningConfig.stockTextMappings }
          : undefined),
      cleaningConfig: (dataSource as any).cleaningConfig,
    };

    const config: EnhancedImportConfig = {
      formatType:
        overrideConfig?.formatType || enhancedConfig.formatType || "row",
      columnMapping:
        overrideConfig?.columnMapping || enhancedConfig.columnMapping || {},
      pivotConfig: overrideConfig?.pivotConfig || enhancedConfig.pivotConfig,
      discontinuedConfig:
        overrideConfig?.discontinuedConfig || enhancedConfig.discontinuedConfig,
      futureStockConfig:
        overrideConfig?.futureStockConfig || enhancedConfig.futureStockConfig,
      stockValueConfig:
        overrideConfig?.stockValueConfig || enhancedConfig.stockValueConfig,
      cleaningConfig: (dataSource as any).cleaningConfig,
    };

    // Consolidate multiple files if in multi-file mode
    let rawData: any[][] = [];
    const primaryFile = files[0];

    if (files.length > 1 && req.body.multiFileMode === "true") {
      console.log(
        `[AIImport] Multi-file execute: consolidating ${files.length} files`,
      );
      let headerRow: any[] | null = null;

      for (const file of files) {
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
          rawData.push(...data.slice(1));
        }
      }
      console.log(
        `[AIImport] Consolidated ${rawData.length} total rows from ${files.length} files`,
      );
    } else {
      const workbook = XLSX.read(primaryFile.buffer, { type: "buffer" });
      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      rawData = XLSX.utils.sheet_to_json(sheet, {
        header: 1,
        defval: "",
      }) as any[][];
    }

    // CRITICAL FIX: For multi-file mode, create a consolidated buffer for pivot parsing
    // This ensures ALL files' data is parsed, not just the first file
    let consolidatedBuffer: Buffer;
    if (files.length > 1 && req.body.multiFileMode === "true") {
      // Create a new workbook from consolidated rawData
      const newWorkbook = XLSX.utils.book_new();
      const newSheet = XLSX.utils.aoa_to_sheet(rawData);
      XLSX.utils.book_append_sheet(newWorkbook, newSheet, "Consolidated");
      consolidatedBuffer = Buffer.from(
        XLSX.write(newWorkbook, { type: "buffer", bookType: "xlsx" }),
      );
      console.log(
        `[AIImport] Created consolidated buffer (${consolidatedBuffer.length} bytes) from ${rawData.length} rows`,
      );
    } else {
      consolidatedBuffer = primaryFile.buffer;
    }

    const detectedPivotFormat = autoDetectPivotFormat(
      rawData,
      dataSource.name,
      primaryFile.originalname,
    );
    const isPivotFormat =
      config.formatType?.startsWith("pivot") ||
      config.formatType === "pivoted" ||
      detectedPivotFormat !== null;

    let parseResult: any;

    if (isPivotFormat) {
      const actualFormat =
        detectedPivotFormat || config.formatType || "pivot_interleaved";
      const universalConfig: UniversalParserConfig = {
        skipRows: config.pivotConfig?.skipRows,
        discontinuedConfig: config.discontinuedConfig as any,
        futureDateConfig: config.futureStockConfig as any,
        stockConfig: config.stockValueConfig as any,
        columnMapping: config.columnMapping,
      };

      // Use consolidated buffer (contains ALL files' data in multi-file mode)
      const pivotResult = parseIntelligentPivotFormat(
        consolidatedBuffer,
        actualFormat,
        universalConfig,
        dataSource.name,
        primaryFile.originalname,
      );

      parseResult = {
        success: true,
        items: pivotResult.items,
        stats: {
          totalRows: pivotResult.rows.length,
          totalItems: pivotResult.items.length,
          discontinuedItems: pivotResult.items.filter(
            (i: any) => i.discontinued,
          ).length,
          futureStockItems: pivotResult.items.filter((i: any) => i.shipDate)
            .length,
        },
        warnings: [],
      };
    } else {
      parseResult = await parseWithEnhancedConfig(
        primaryFile.buffer,
        config,
        dataSourceId,
      );
    }

    if (!parseResult.success) {
      return res.status(400).json({
        success: false,
        error: "Failed to parse file",
        warnings: parseResult.warnings,
      });
    }

    // ============================================================
    // APPLY IMPORT RULES (same as regular imports)
    // Handles: discontinued detection, sale pricing, date parsing,
    // stock text mappings, value replacements, etc.
    // ============================================================
    console.log(`[AIImport] Applying import rules for ${dataSource.name}...`);

    // BUG FIX: Check overrideConfig FIRST for configs (from UI), then fall back to DB
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
      // Add stockValueConfig for text-to-number mappings
      // CRITICAL FIX: stockValueConfig column doesn't exist in schema!
      // Fall back to cleaningConfig.stockTextMappings which is where UI saves the data
      stockValueConfig:
        overrideConfig?.stockValueConfig ||
        (dataSource as any).stockValueConfig ||
        (dataSource.cleaningConfig?.stockTextMappings?.length > 0
          ? { textMappings: dataSource.cleaningConfig.stockTextMappings }
          : undefined),
      // Add complexStockConfig for pattern-based stock parsing
      complexStockConfig:
        overrideConfig?.complexStockConfig ||
        (dataSource as any).complexStockConfig,
    };

    const importRulesResult = await applyImportRules(
      parseResult.items,
      importRulesConfig,
      rawData, // Pass raw data rows for context
    );

    console.log(
      `[AIImport] Import rules applied: ${importRulesResult.stats.discontinuedFiltered} discontinued, ${importRulesResult.stats.datesParsed} dates parsed`,
    );

    // ============================================================
    // APPLY GLOBAL COLOR MAPPINGS (CRITICAL - same as regular imports)
    // This transforms raw color codes like "BLL" → "Blue"
    // Also rebuilds SKUs with corrected colors
    // ============================================================
    console.log(
      `[AIImport] Applying global color mappings for ${dataSource.name}...`,
    );

    let itemsWithMappedColors = importRulesResult.items;
    let colorsFixed = 0;

    try {
      const colorMappings = await storage.getColorMappings();
      const colorMap = new Map<string, string>();

      for (const mapping of colorMappings) {
        const normalizedBad = mapping.badColor.trim().toLowerCase();
        colorMap.set(normalizedBad, mapping.goodColor);
      }

      console.log(
        `[AIImport] Loaded ${colorMappings.length} global color mappings`,
      );

      if (colorMap.size > 0) {
        itemsWithMappedColors = importRulesResult.items.map((item: any) => {
          const color = String(item.color || "").trim();
          const normalizedColor = color.toLowerCase();
          const mappedColor = colorMap.get(normalizedColor);

          if (mappedColor && mappedColor.toLowerCase() !== normalizedColor) {
            colorsFixed++;
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
          // No mapping, but still format the color name consistently
          return { ...item, color: formatColorName(color) };
        });

        if (colorsFixed > 0) {
          console.log(
            `[AIImport] Fixed ${colorsFixed} colors using global mappings`,
          );
        }
      }
    } catch (colorMapError: any) {
      console.error(`[AIImport] Error applying color mappings:`, colorMapError);
      // Continue without color mapping if there's an error
    }

    // ============================================================
    // CRITICAL FIX: APPLY STYLE PREFIX BEFORE VARIANT RULES
    // This ensures sizeLimitConfig prefix override patterns work correctly
    // The prefix must match the PREFIXED style (e.g., "INESS 12345")
    // ============================================================
    console.log(
      `[AIImport] Applying style prefix "${dataSource.name}" to items...`,
    );

    const cleaningConfig = (dataSource.cleaningConfig || {}) as any;
    const getStylePrefixForAI = (style: string): string => {
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
    };

    // Apply prefix to all items
    const itemsWithPrefix = itemsWithMappedColors.map((item: any) => {
      const rawStyle = String(item.style || "").trim();
      const prefix = rawStyle ? getStylePrefixForAI(rawStyle) : dataSource.name;
      const prefixedStyle = rawStyle ? `${prefix} ${rawStyle}` : rawStyle;
      // Rebuild SKU with prefixed style
      const prefixedSku =
        prefixedStyle && item.color && item.size
          ? `${prefixedStyle}-${item.color}-${item.size}`
              .replace(/\//g, "-")
              .replace(/\s+/g, "-")
              .replace(/-+/g, "-")
          : item.sku;
      return {
        ...item,
        style: prefixedStyle,
        sku: prefixedSku,
      };
    });

    console.log(`[AIImport] Applied prefix to ${itemsWithPrefix.length} items`);

    // ============================================================
    // APPLY VARIANT RULES (same as regular imports)
    // Handles: size limits, zero stock filtering, size expansion,
    // isExpandedSize flag setting
    // ============================================================
    console.log(`[AIImport] Applying variant rules for ${dataSource.name}...`);

    // BUG FIX: Pass filterZeroStock from overrideConfig if present
    const variantRulesConfigOverride =
      overrideConfig?.filterZeroStock !== undefined
        ? {
            filterZeroStock: overrideConfig.filterZeroStock,
            filterZeroStockWithFutureDates:
              overrideConfig?.filterZeroStockWithFutureDates,
          }
        : undefined;

    const variantRulesResult = await applyVariantRules(
      itemsWithPrefix, // Use prefixed items
      dataSourceId,
      variantRulesConfigOverride,
    );

    console.log(
      `[AIImport] Variant rules applied: ${variantRulesResult.addedCount} sizes expanded, ${variantRulesResult.filteredCount} filtered, ${variantRulesResult.sizeFiltered || 0} size-limited`,
    );

    // ============================================================
    // APPLY PRICE-BASED SIZE EXPANSION (same as regular imports)
    // Expands sizes based on price tiers from Shopify
    // ============================================================
    let priceBasedExpansionCount = 0;
    let itemsAfterExpansion = variantRulesResult.items;
    // BUG FIX: Check overrideConfig FIRST for these configs (from UI), then fall back to DB
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
        console.log(
          `[AIImport] Applying price-based size expansion for "${dataSource.name}"...`,
        );
        try {
          // Get cached variant prices from Shopify
          const cacheVariants =
            await storage.getVariantCacheProductStyles(shopifyStoreId);
          const stylePriceMap = buildStylePriceMapFromCache(cacheVariants);
          console.log(
            `[AIImport] Built style price map with ${stylePriceMap.size} styles`,
          );

          // Apply price-based expansion
          const expansionResult = applyPriceBasedExpansion(
            variantRulesResult.items,
            priceBasedExpansionConfig,
            stylePriceMap,
            sizeLimitConfig,
          );
          itemsAfterExpansion = expansionResult.items;
          priceBasedExpansionCount = expansionResult.addedCount;

          if (priceBasedExpansionCount > 0) {
            console.log(
              `[AIImport] Price-based expansion added ${priceBasedExpansionCount} size variants`,
            );
          }
        } catch (expansionError) {
          console.error(
            `[AIImport] Price-based expansion error:`,
            expansionError,
          );
          // Continue without expansion if there's an error
        }
      } else {
        console.log(
          `[AIImport] Price-based expansion enabled but no Shopify store linked - skipping`,
        );
      }
    }

    // ============================================================
    // FILTER DISCONTINUED STYLES (same as regular imports)
    // If this is a regular file with linked sale file, filter discontinued
    // ============================================================
    const isSaleFile = (dataSource as any).sourceType === "sales";
    const linkedSaleDataSourceId = (dataSource as any).assignedSaleDataSourceId;
    let discontinuedStylesFiltered = 0;
    let discontinuedItemsRemoved = 0;
    let itemsToImport = itemsAfterExpansion;

    if (!isSaleFile && linkedSaleDataSourceId) {
      console.log(
        `[AIImport] Regular file "${dataSource.name}" - checking for discontinued styles from sale file`,
      );

      try {
        // First, remove any existing inventory items that have discontinued styles
        discontinuedItemsRemoved = await removeDiscontinuedInventoryItems(
          dataSourceId,
          linkedSaleDataSourceId,
        );
        if (discontinuedItemsRemoved > 0) {
          console.log(
            `[AIImport] Removed ${discontinuedItemsRemoved} existing inventory items with discontinued styles`,
          );
        }

        // Then, filter out items from this import that have discontinued styles
        const filterResult = await filterDiscontinuedStyles(
          dataSourceId,
          itemsAfterExpansion,
          linkedSaleDataSourceId,
        );
        itemsToImport = filterResult.items;
        discontinuedStylesFiltered = filterResult.removedCount;

        if (discontinuedStylesFiltered > 0) {
          console.log(
            `[AIImport] Filtered out ${discontinuedStylesFiltered} items with ${filterResult.discontinuedStyles.length} discontinued styles: ${filterResult.discontinuedStyles.slice(0, 3).join(", ")}${filterResult.discontinuedStyles.length > 3 ? "..." : ""}`,
          );
        }
      } catch (discontinuedError) {
        console.error(
          `[AIImport] Discontinued filtering error:`,
          discontinuedError,
        );
        // Continue without filtering if there's an error
      }
    }

    // Use the final processed items for saving
    const processedItems = itemsToImport;

    const file = await storage.createUploadedFile({
      dataSourceId,
      fileName:
        files.length > 1
          ? `${files.length} files consolidated`
          : primaryFile.originalname,
      status: "completed",
      rowCount: processedItems.length,
      processedAt: new Date(),
    });

    // ============================================================
    // GET STOCK INFO RULE FOR MESSAGE CALCULATION
    // Priority: (1) overrideConfig stockInfoConfig (from UI), (2) DB stockInfoConfig, (3) Rule Engine metafield rules
    // ============================================================
    let stockInfoRule: any = null;
    try {
      // BUG FIX: Check overrideConfig FIRST (sent from UI), THEN fall back to DB
      // This ensures newly configured settings are used immediately without race conditions
      const stockInfoConfig =
        overrideConfig?.stockInfoConfig || (dataSource as any).stockInfoConfig;

      console.log(
        `[AIImport] stockInfoConfig source: ${overrideConfig?.stockInfoConfig ? "overrideConfig (UI)" : (dataSource as any).stockInfoConfig ? "database" : "NONE"}`,
      );

      // Check if stockInfoConfig has ANY actual messages configured
      const hasStockInfoMessages =
        stockInfoConfig &&
        (stockInfoConfig.message1InStock ||
          stockInfoConfig.message2ExtraSizes ||
          stockInfoConfig.message3Default ||
          stockInfoConfig.message4FutureDate);

      if (hasStockInfoMessages) {
        // Use AI Importer settings - these take priority
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
        console.log(
          `[AIImport] Using AI Importer stockInfoConfig: inStock="${stockInfoRule.inStockMessage}"`,
        );
      } else {
        // PRIORITY 2: Fallback to Rule Engine metafield rules
        const metafieldRules =
          await storage.getShopifyMetafieldRulesByDataSource(dataSourceId);

        // Use first enabled rule
        const activeDbRule = metafieldRules.find(
          (r: any) => r.enabled !== false,
        );

        if (activeDbRule) {
          // Normalize database rule to handle both snake_case and camelCase
          stockInfoRule = {
            id: activeDbRule.id,
            name: activeDbRule.name || "Rule Engine Metafield Rule",
            stockThreshold:
              activeDbRule.stockThreshold ?? activeDbRule.stock_threshold ?? 0,
            inStockMessage:
              activeDbRule.inStockMessage ||
              activeDbRule.in_stock_message ||
              "",
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
          console.log(
            `[AIImport] Using Rule Engine metafield rule: inStock="${stockInfoRule.inStockMessage}"`,
          );
        } else {
          console.log(
            `[AIImport] No stockInfoConfig AND no metafield rules - stockInfo will be null`,
          );
        }
      }
    } catch (ruleError) {
      console.error(`[AIImport] Failed to get stock info rules:`, ruleError);
    }

    // ============================================================
    // CALCULATE STOCK INFO FOR EACH ITEM
    // ============================================================
    const calculateItemStockInfo = (item: any): string | null => {
      if (!stockInfoRule) {
        return null;
      }

      const stock = item.stock || 0;
      const shipDate = item.shipDate;
      const isExpandedSize = item.isExpandedSize || false;
      const threshold = stockInfoRule.stockThreshold || 0;

      // Priority 1: Expanded size
      if (isExpandedSize && stockInfoRule.sizeExpansionMessage) {
        return stockInfoRule.sizeExpansionMessage;
      }

      // Priority 2: Has future date - check BEFORE stock check!
      if (shipDate && stockInfoRule.futureDateMessage) {
        try {
          const dateStr = String(shipDate).trim();
          let targetDate: Date;

          // Parse ISO format: YYYY-MM-DD
          const isoMatch = dateStr.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
          // Parse US format: M/D/YYYY
          const usMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
          // Parse US short format: M/D/YY
          const usShortMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2})$/);

          if (isoMatch) {
            const [, year, month, day] = isoMatch;
            targetDate = new Date(
              parseInt(year),
              parseInt(month) - 1,
              parseInt(day),
            );
          } else if (usMatch) {
            const [, month, day, year] = usMatch;
            targetDate = new Date(
              parseInt(year),
              parseInt(month) - 1,
              parseInt(day),
            );
          } else if (usShortMatch) {
            const [, month, day, shortYear] = usShortMatch;
            targetDate = new Date(
              2000 + parseInt(shortYear),
              parseInt(month) - 1,
              parseInt(day),
            );
          } else {
            targetDate = new Date(dateStr);
          }

          // Add offset days
          const offsetDays = stockInfoRule.dateOffsetDays || 0;
          if (offsetDays !== 0) {
            targetDate.setDate(targetDate.getDate() + offsetDays);
          }

          // Check if future
          const today = new Date();
          today.setHours(0, 0, 0, 0);
          targetDate.setHours(0, 0, 0, 0);

          if (targetDate > today) {
            const formattedDate = targetDate.toLocaleDateString("en-US", {
              month: "long",
              day: "numeric",
              year: "numeric",
            });
            return stockInfoRule.futureDateMessage.replace(
              /\{date\}/gi,
              formattedDate,
            );
          }
        } catch (e) {
          console.error(`[AIImport] Failed to parse date: ${shipDate}`, e);
        }
      }

      // Priority 3: In stock
      if (stock > threshold) {
        return stockInfoRule.inStockMessage;
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
    };

    // Map items for saving WITH stockInfo calculated
    // BUG FIX: Only include fields that exist in the inventoryItems schema
    // REMOVED: salePrice (not in schema, could cause type issues)
    // ADDED: rawData (was missing)
    console.log(
      `[AIImport] STOCK INFO: ${stockInfoRule ? `Rule="${stockInfoRule.name}" inStock="${stockInfoRule.inStockMessage}"` : "NO RULE - stockInfo will be null"}`,
    );

    const itemsToSave = processedItems.map((item: any) => {
      const calculatedStockInfo = calculateItemStockInfo(item);
      return {
        dataSourceId,
        fileId: file.id,
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
        // NOTE: salePrice removed - not in inventory_items schema
        shipDate: item.shipDate,
        discontinued: item.discontinued || false,
        isExpandedSize: item.isExpandedSize || false,
        stockInfo: calculatedStockInfo, // Calculate stock info message
        rawData: item.rawData || null, // BUG FIX: Was missing from original mapping
      };
    });

    // Log summary of stockInfo assignments
    const itemsWithStockInfo = itemsToSave.filter((i) => i.stockInfo).length;
    console.log(
      `[AIImport] stockInfo: ${itemsWithStockInfo}/${itemsToSave.length} items have messages`,
    );

    // DEBUG: Show first 3 items with their stockInfo
    console.log(`[AIImport] Sample items being saved:`);
    itemsToSave.slice(0, 3).forEach((item, i) => {
      console.log(
        `  Item ${i + 1}: sku="${item.sku}" stock=${item.stock} stockInfo="${item.stockInfo}"`,
      );
    });

    // ============================================================
    // BUG FIX: Respect updateStrategy setting
    // - "full_sync": Delete all existing items, then insert new (default)
    // - "replace": Upsert items (create new, update existing by SKU)
    // ============================================================
    const updateStrategy = dataSource.updateStrategy || "full_sync";
    console.log(`[AIImport] Using update strategy: ${updateStrategy}`);

    if (updateStrategy === "replace") {
      // Replace/Upsert: Create new items, update existing by SKU
      console.log(
        `[AIImport] Upserting ${itemsToSave.length} items (create new, update existing)`,
      );
      await storage.upsertInventoryItems(itemsToSave, dataSourceId);
    } else {
      // Full Sync (default): Delete all existing, then insert new
      console.log(
        `[AIImport] Full sync: deleting existing items and inserting ${itemsToSave.length} new items`,
      );
      await storage.deleteInventoryItemsByDataSource(dataSourceId);
      await storage.createInventoryItems(itemsToSave);
    }

    await storage.updateDataSource(dataSourceId, { lastSync: new Date() });

    // ============================================================
    // ALWAYS SAVE IMPORT STATS (for checksum validation)
    // These are the expected counts AFTER all rules are applied
    // Includes product-level data for detailed validation
    // ============================================================
    try {
      const styles = new Set<string>();
      const colors = new Set<string>();
      const skuSet = new Set<string>();
      let totalStock = 0;
      let itemsWithPrice = 0;
      let itemsWithShipDate = 0;
      let itemsDiscontinued = 0;
      let itemsExpanded = 0;

      // Get the style prefix for stats metadata (used for reference only)
      const stylePrefix = dataSource.name
        ? String(dataSource.name).toUpperCase().trim()
        : "";

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

      // NOTE: Styles are already prefixed at this point (prefix applied before applyVariantRules)
      // So we use item.style directly instead of re-prefixing

      // FETCH GLOBAL COLOR MAPPINGS to apply same transformations as DB
      const colorMappings = await storage.getColorMappings();
      const colorMap = new Map<string, string>();
      for (const mapping of colorMappings) {
        const normalizedBad = mapping.badColor.trim().toLowerCase();
        colorMap.set(normalizedBad, mapping.goodColor);
      }
      console.log(
        `[AIImport] Loaded ${colorMappings.length} global color mappings for validation stats`,
      );

      // Helper to apply color mapping (same logic as cleanInventoryData)
      const applyColorMapping = (rawColor: string): string => {
        const color = rawColor.trim();
        const normalizedColor = color.toLowerCase();
        const mappedColor = colorMap.get(normalizedColor);
        if (mappedColor && mappedColor.toLowerCase() !== normalizedColor) {
          // Format: capitalize first letter of each word
          return mappedColor
            .split(" ")
            .map(
              (word) =>
                word.charAt(0).toUpperCase() + word.slice(1).toLowerCase(),
            )
            .join(" ")
            .toUpperCase();
        }
        // No mapping found, format the original
        return color.toUpperCase();
      };

      for (const item of itemsToSave) {
        // Style is already prefixed (prefix applied before applyVariantRules)
        const prefixedStyle = String(item.style || "")
          .toUpperCase()
          .trim();

        // Color with mapping applied (same as what gets saved to DB)
        const rawColor = String(item.color || "");
        const mappedColor = applyColorMapping(rawColor);

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
        if (item.price && parseFloat(String(item.price)) > 0) itemsWithPrice++;
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
        if (sku && product.skus.length < 50) product.skus.push(sku); // Keep first 50 SKUs per style
      }

      // Convert product summary to serializable format (convert Sets to Arrays)
      const productData: Record<
        string,
        {
          variantCount: number;
          colors: string[];
          sizes: string[];
          totalStock: number;
          hasDiscontinued: boolean;
          hasFutureDate: boolean;
          expandedCount: number;
          skus: string[];
        }
      > = {};

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

      const importStats = {
        importedAt: new Date().toISOString(),
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
        `[AIImport] Saving import stats: items=${importStats.itemCount}, styles=${importStats.uniqueStyles}, colors=${importStats.uniqueColors}, products=${Object.keys(productData).length}, prefix="${stylePrefix}"`,
      );
      await storage.updateDataSource(dataSourceId, {
        lastImportStats: importStats,
      } as any);
    } catch (statsErr: any) {
      console.warn(
        `[AIImport] Could not save import stats: ${statsErr.message}`,
      );
    }

    // ============================================================
    // POST-IMPORT VALIDATION (Enhanced with Checksums)
    // Wrapped in defensive try-catch - validation errors NEVER break import
    // ============================================================

    let validationResult: PostImportValidationResult | null = null;

    // Outer try-catch ensures validation can NEVER break the import
    try {
      const validationConfig = (dataSource as any).validationConfig as
        | ValidationConfig
        | undefined;

      if (validationConfig && validationConfig.enabled === true) {
        try {
          console.log(
            `[AIImport] Running post-import validation for ${dataSource.name}...`,
          );

          // 1. Capture source checksums from parsed items
          const itemsForChecksum = parseResult?.items || itemsToSave || [];
          if (itemsForChecksum.length === 0) {
            console.warn(`[AIImport] No items to validate`);
          } else {
            const sourceChecksums: SourceChecksums =
              captureSourceChecksums(itemsForChecksum);
            console.log(
              `[AIImport] Source checksums: ${sourceChecksums.rawRowCount} rows, ${sourceChecksums.rawTotalStock} total stock, ${sourceChecksums.rawUniqueStyles} styles`,
            );

            // 2. Extract data source rules
            const dataSourceRules: DataSourceRules = {
              skipDiscontinued:
                (dataSource as any).discontinuedRules?.skipDiscontinued ||
                (dataSource as any).discontinuedConfig?.skipDiscontinued ||
                false,
              filterZeroStock: (dataSource as any).filterZeroStock || false,
              priceExpansionEnabled:
                (dataSource as any).priceBasedExpansionConfig?.enabled || false,
              sizeExpansionEnabled: (variantRulesResult.addedCount || 0) > 0,
            };

            // 3. Get last import stats for historical comparison (may not exist)
            const lastImportStats: LastImportStats | undefined = (
              dataSource as any
            ).lastImportStats;

            // 4. Run enhanced validation
            validationResult = await validateImportResults(
              dataSourceId,
              itemsToSave,
              validationConfig,
              sourceChecksums,
              dataSourceRules,
              lastImportStats,
            );

            console.log(
              `[AIImport] Validation complete: ${validationResult.passedChecks}/${validationResult.totalChecks} checks passed (${validationResult.accuracy}%)`,
            );

            if (!validationResult.passed) {
              console.warn(
                `[AIImport] Validation FAILED for ${dataSource.name}`,
              );
            }

            // NOTE: We do NOT save lastImportStats here anymore
            // Stats are saved earlier from itemsToSave (FINAL items after all rules)
            // The old code was incorrectly saving SOURCE checksums which caused validation mismatches
          }
        } catch (innerError: any) {
          console.error(`[AIImport] Validation inner error:`, innerError);
          validationResult = {
            passed: false,
            totalChecks: 0,
            passedChecks: 0,
            failedChecks: 0,
            accuracy: 0,
            checksumResults: [],
            distributionResults: [],
            deltaResults: [],
            countResults: [],
            ruleResults: [],
            spotCheckResults: [],
            importStats: {
              itemCount: itemsToSave?.length || 0,
              totalStock: 0,
              uniqueStyles: 0,
              uniqueColors: 0,
              itemsWithPrice: 0,
              itemsWithShipDate: 0,
              itemsDiscontinued: 0,
              itemsExpanded: 0,
            },
          };
        }
      }
    } catch (outerError: any) {
      console.error(`[AIImport] Validation outer error:`, outerError);
      // Completely ignore - validation errors should never affect import
      validationResult = null;
    }

    res.json({
      success: true,
      fileId: file.id,
      itemCount: itemsToSave.length,
      stockInfoCount: itemsWithStockInfo, // DEBUG: How many items got stockInfo calculated
      // DEBUG: Show first item's data and rule info so user can see what's happening
      debug: {
        firstItemWithShipDate: itemsToSave.find((i) => i.shipDate)
          ? {
              sku: itemsToSave.find((i) => i.shipDate)?.sku,
              shipDate: itemsToSave.find((i) => i.shipDate)?.shipDate,
              stockInfo: itemsToSave.find((i) => i.shipDate)?.stockInfo,
              stock: itemsToSave.find((i) => i.shipDate)?.stock,
            }
          : null,
        processedItemsWithShipDate: processedItems.filter(
          (i: any) => i.shipDate,
        ).length,
        stockInfoRule: stockInfoRule
          ? {
              name: stockInfoRule.name,
              futureDateMessage: stockInfoRule.futureDateMessage,
              outOfStockMessage: stockInfoRule.outOfStockMessage,
              inStockMessage: stockInfoRule.inStockMessage,
            }
          : null,
        ruleSource: stockInfoRule
          ? stockInfoRule.id === "config-rule"
            ? "stockInfoConfig"
            : "metafieldRules"
          : "none",
      },
      stats: {
        ...parseResult.stats,
        // Add rule processing stats
        totalParsed: parseResult.items.length,
        afterImportRules: importRulesResult.items.length,
        afterVariantRules: variantRulesResult.items.length,
        afterPriceExpansion: itemsAfterExpansion.length,
        afterDiscontinuedFilter: processedItems.length,
        finalCount: itemsToSave.length,
        stockInfoAssigned: itemsWithStockInfo, // DEBUG: visible in response
        // Detailed counts
        discontinuedFiltered: importRulesResult.stats.discontinuedFiltered,
        datesParsed: importRulesResult.stats.datesParsed,
        colorsFixed: colorsFixed, // NEW: number of colors fixed by global mappings
        sizesExpanded: variantRulesResult.addedCount || 0,
        sizeFiltered: variantRulesResult.sizeFiltered || 0,
        zeroStockFiltered: variantRulesResult.filteredCount || 0,
        priceBasedExpansion: priceBasedExpansionCount,
        discontinuedStylesFiltered: discontinuedStylesFiltered,
        discontinuedItemsRemoved: discontinuedItemsRemoved,
      },
      warnings: parseResult.warnings || [],
      validation: validationResult,
    });
  } catch (error: any) {
    console.error("[AIImport] Execute error:", error);
    res
      .status(500)
      .json({ success: false, error: error.message || "Import failed" });
  }
});

// ============================================================
// SAVE CONFIGURATION ENDPOINT
// ============================================================

router.post(
  "/save-config/:dataSourceId?",
  async (req: Request, res: Response) => {
    try {
      // Accept dataSourceId from URL params OR body for compatibility
      const dataSourceId = req.params.dataSourceId || req.body.dataSourceId;
      const {
        formatType,
        columnMapping,
        pivotConfig,
        discontinuedConfig,
        futureStockConfig,
        stockValueConfig,
        cleaningConfig,
        regularPriceConfig,
        filterZeroStock,
        filterZeroStockWithFutureDates,
        validationConfig,
        stockInfoConfig,
        // BUG FIX: These fields were MISSING - frontend sends them but backend wasn't saving them
        complexStockConfig,
        columnSaleConfig,
        priceExpansionConfig,
        priceBasedExpansionConfig, // Frontend might send either name
        sizeLimitConfig, // BUG FIX: Was missing - size limits weren't being saved
      } = req.body;

      if (!dataSourceId)
        return res.status(400).json({ error: "Data source ID required" });

      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource)
        return res.status(404).json({ error: "Data source not found" });

      const updateData: any = {};
      if (formatType !== undefined) updateData.formatType = formatType;
      if (columnMapping !== undefined) updateData.columnMapping = columnMapping;
      if (pivotConfig !== undefined) updateData.pivotConfig = pivotConfig;
      if (discontinuedConfig !== undefined)
        updateData.discontinuedRules = discontinuedConfig;
      if (futureStockConfig !== undefined)
        updateData.futureStockConfig = futureStockConfig;
      if (stockValueConfig !== undefined)
        updateData.stockValueConfig = stockValueConfig;
      if (cleaningConfig !== undefined)
        updateData.cleaningConfig = cleaningConfig;
      if (regularPriceConfig !== undefined)
        updateData.regularPriceConfig = regularPriceConfig;
      if (filterZeroStock !== undefined)
        updateData.filterZeroStock = filterZeroStock;
      if (filterZeroStockWithFutureDates !== undefined)
        updateData.filterZeroStockWithFutureDates =
          filterZeroStockWithFutureDates;
      if (validationConfig !== undefined)
        updateData.validationConfig = validationConfig;
      if (stockInfoConfig !== undefined)
        updateData.stockInfoConfig = stockInfoConfig;
      // BUG FIX: Save the missing config fields
      if (complexStockConfig !== undefined)
        updateData.complexStockConfig = complexStockConfig;
      if (columnSaleConfig !== undefined)
        updateData.salePriceConfig = columnSaleConfig; // Map to DB field name
      // Handle both possible names for price expansion config
      const priceExpConfig = priceExpansionConfig || priceBasedExpansionConfig;
      if (priceExpConfig !== undefined)
        updateData.priceBasedExpansionConfig = priceExpConfig;
      // BUG FIX: Save sizeLimitConfig
      if (sizeLimitConfig !== undefined)
        updateData.sizeLimitConfig = sizeLimitConfig;

      console.log(
        `[AIImport] save-config: stockInfoConfig=${stockInfoConfig ? "YES" : "NO"}, priceExpansionConfig=${priceExpConfig ? "YES" : "NO"}, complexStockConfig=${complexStockConfig ? "YES" : "NO"}, sizeLimitConfig=${sizeLimitConfig ? "YES" : "NO"}`,
      );

      await storage.updateDataSource(dataSourceId, updateData);

      // NOTE: stockInfoConfig is saved to data_sources table
      // During Shopify sync, the system will:
      // 1. First check shopify_metafield_rules table (Rule Engine rules)
      // 2. Fall back to stockInfoConfig on data source if no rules exist
      // We do NOT sync to shopify_metafield_rules here to avoid overwriting Rule Engine rules

      res.json({ success: true, message: "Configuration saved" });
    } catch (error: any) {
      console.error("[AIImport] Save config error:", error);
      res.status(500).json({
        success: false,
        error: error.message || "Failed to save configuration",
      });
    }
  },
);

// ============================================================
// VALIDATE DATABASE DATA (without file upload)
// ============================================================
router.post(
  "/validate-db/:dataSourceId",
  async (req: Request, res: Response) => {
    try {
      const { dataSourceId } = req.params;
      const { validationConfig } = req.body;

      console.log(
        `[AIImport] Validating DB data for data source: ${dataSourceId}`,
      );

      // Get data source
      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res
          .status(404)
          .json({ success: false, error: "Data source not found" });
      }

      // Get all inventory items for this data source
      const inventoryItems =
        await storage.getInventoryItemsByDataSource(dataSourceId);
      if (!inventoryItems || inventoryItems.length === 0) {
        return res.json({
          success: true,
          passed: false,
          itemCount: 0,
          styleCount: 0,
          totalStock: 0,
          totalChecks: 1,
          passedChecks: 0,
          failedChecks: 1,
          results: [
            {
              name: "Data Exists",
              passed: false,
              message: "No inventory items found for this data source",
            },
          ],
        });
      }

      // Calculate stats from DB data
      const dbItemCount = inventoryItems.length;
      const dbUniqueStyles = new Set(
        inventoryItems.map((i: any) =>
          String(i.style || "")
            .toUpperCase()
            .trim(),
        ),
      ).size;
      const dbUniqueColors = new Set(
        inventoryItems
          .map((i: any) =>
            String(i.color || "")
              .toUpperCase()
              .trim(),
          )
          .filter(Boolean),
      ).size;
      const dbUniqueSizes = new Set(
        inventoryItems
          .map((i: any) => String(i.size || "").trim())
          .filter(Boolean),
      ).size;
      const dbTotalStock = inventoryItems.reduce(
        (sum: number, i: any) => sum + (Number(i.stock) || 0),
        0,
      );
      const dbItemsWithPrice = inventoryItems.filter(
        (i: any) => i.price && Number(i.price) > 0,
      ).length;
      const dbItemsWithStock = inventoryItems.filter(
        (i: any) => Number(i.stock) > 0,
      ).length;
      const dbItemsDiscontinued = inventoryItems.filter(
        (i: any) => i.discontinued,
      ).length;
      const dbItemsWithShipDate = inventoryItems.filter(
        (i: any) => i.shipDate,
      ).length;
      const dbItemsExpanded = inventoryItems.filter(
        (i: any) => i.isExpandedSize,
      ).length;

      const results: Array<{
        name: string;
        passed: boolean;
        message: string;
        category: string;
      }> = [];

      // Run validation checks based on config
      const config = validationConfig || {};

      // ============================================================
      // TRUE CHECKSUM VALIDATION - Compare DB vs Expected (lastImportStats)
      // ============================================================
      if (config.checksumRules?.enabled) {
        const rules = config.checksumRules;
        const expected = (dataSource as any).lastImportStats;
        const tolerance = rules.tolerancePercent || 0;

        if (!expected) {
          results.push({
            name: "Checksum Baseline",
            passed: false,
            message:
              "No previous import stats found - run an import first to establish baseline",
            category: "checksum",
          });
        } else {
          console.log(
            `[AIImport] Checksum validation - Expected: ${JSON.stringify(expected)}`,
          );
          console.log(
            `[AIImport] Checksum validation - DB: items=${dbItemCount}, stock=${dbTotalStock}, styles=${dbUniqueStyles}`,
          );

          // Helper function to check with tolerance
          const withinTolerance = (
            actual: number,
            expected: number,
          ): boolean => {
            if (expected === 0) return actual === 0;
            const diff = Math.abs((actual - expected) / expected) * 100;
            return diff <= tolerance;
          };

          // Verify Item Count
          if (rules.verifyItemCount !== false) {
            const expectedCount = expected.itemCount || 0;
            const passed = withinTolerance(dbItemCount, expectedCount);
            const diff =
              expectedCount > 0
                ? (
                    ((dbItemCount - expectedCount) / expectedCount) *
                    100
                  ).toFixed(1)
                : "N/A";
            results.push({
              name: "✓ Item Count Match",
              passed,
              message: passed
                ? `DB: ${dbItemCount} = Expected: ${expectedCount} (${tolerance}% tolerance)`
                : `MISMATCH! DB: ${dbItemCount} vs Expected: ${expectedCount} (${diff}% diff, tolerance: ${tolerance}%)`,
              category: "checksum",
            });
          }

          // Verify Total Stock
          if (rules.verifyTotalStock !== false) {
            const expectedStock = expected.totalStock || 0;
            const passed = withinTolerance(dbTotalStock, expectedStock);
            const diff =
              expectedStock > 0
                ? (
                    ((dbTotalStock - expectedStock) / expectedStock) *
                    100
                  ).toFixed(1)
                : "N/A";
            results.push({
              name: "✓ Total Stock Match",
              passed,
              message: passed
                ? `DB: ${dbTotalStock} = Expected: ${expectedStock} (${tolerance}% tolerance)`
                : `MISMATCH! DB: ${dbTotalStock} vs Expected: ${expectedStock} (${diff}% diff, tolerance: ${tolerance}%)`,
              category: "checksum",
            });
          }

          // Verify Style Count
          if (rules.verifyStyleCount !== false) {
            const expectedStyles = expected.uniqueStyles || 0;
            const passed = withinTolerance(dbUniqueStyles, expectedStyles);
            const diff =
              expectedStyles > 0
                ? (
                    ((dbUniqueStyles - expectedStyles) / expectedStyles) *
                    100
                  ).toFixed(1)
                : "N/A";
            results.push({
              name: "✓ Style Count Match",
              passed,
              message: passed
                ? `DB: ${dbUniqueStyles} = Expected: ${expectedStyles} (${tolerance}% tolerance)`
                : `MISMATCH! DB: ${dbUniqueStyles} vs Expected: ${expectedStyles} (${diff}% diff, tolerance: ${tolerance}%)`,
              category: "checksum",
            });
          }

          // Verify Color Count
          if (rules.verifyColorCount) {
            const expectedColors = expected.uniqueColors || 0;
            const passed = withinTolerance(dbUniqueColors, expectedColors);
            results.push({
              name: "✓ Color Count Match",
              passed,
              message: passed
                ? `DB: ${dbUniqueColors} = Expected: ${expectedColors}`
                : `MISMATCH! DB: ${dbUniqueColors} vs Expected: ${expectedColors}`,
              category: "checksum",
            });
          }
        }
      }

      // ============================================================
      // DISTRIBUTION CHECKS
      // ============================================================
      if (config.distributionRules?.enabled) {
        const rules = config.distributionRules;

        if (rules.minPercentWithStock !== undefined) {
          const percentWithStock = (dbItemsWithStock / dbItemCount) * 100;
          const passed = percentWithStock >= rules.minPercentWithStock;
          results.push({
            name: "Min % with Stock",
            passed,
            message: `${percentWithStock.toFixed(1)}% have stock (min: ${rules.minPercentWithStock}%)`,
            category: "distribution",
          });
        }

        if (rules.maxPercentWithStock !== undefined) {
          const percentWithStock = (dbItemsWithStock / dbItemCount) * 100;
          const passed = percentWithStock <= rules.maxPercentWithStock;
          results.push({
            name: "Max % with Stock",
            passed,
            message: `${percentWithStock.toFixed(1)}% have stock (max: ${rules.maxPercentWithStock}%)`,
            category: "distribution",
          });
        }

        if (rules.minPercentWithPrice !== undefined) {
          const percentWithPrice = (dbItemsWithPrice / dbItemCount) * 100;
          const passed = percentWithPrice >= rules.minPercentWithPrice;
          results.push({
            name: "Min % with Price",
            passed,
            message: `${percentWithPrice.toFixed(1)}% have price (min: ${rules.minPercentWithPrice}%)`,
            category: "distribution",
          });
        }

        if (rules.minPercentWithShipDate !== undefined) {
          const percentWithShipDate = (dbItemsWithShipDate / dbItemCount) * 100;
          const passed = percentWithShipDate >= rules.minPercentWithShipDate;
          results.push({
            name: "Min % with Ship Date",
            passed,
            message: `${percentWithShipDate.toFixed(1)}% have ship date (min: ${rules.minPercentWithShipDate}%)`,
            category: "distribution",
          });
        }
      }

      // ============================================================
      // COUNT CHECKS
      // ============================================================
      if (config.countRules?.enabled) {
        const rules = config.countRules;

        if (rules.minItems !== undefined) {
          const passed = dbItemCount >= rules.minItems;
          results.push({
            name: "Minimum Items",
            passed,
            message: `${dbItemCount} items (min: ${rules.minItems})`,
            category: "count",
          });
        }

        if (rules.maxItems !== undefined) {
          const passed = dbItemCount <= rules.maxItems;
          results.push({
            name: "Maximum Items",
            passed,
            message: `${dbItemCount} items (max: ${rules.maxItems})`,
            category: "count",
          });
        }

        if (rules.minStyles !== undefined) {
          const passed = dbUniqueStyles >= rules.minStyles;
          results.push({
            name: "Minimum Styles",
            passed,
            message: `${dbUniqueStyles} styles (min: ${rules.minStyles})`,
            category: "count",
          });
        }

        if (rules.maxStyles !== undefined) {
          const passed = dbUniqueStyles <= rules.maxStyles;
          results.push({
            name: "Maximum Styles",
            passed,
            message: `${dbUniqueStyles} styles (max: ${rules.maxStyles})`,
            category: "count",
          });
        }
      }

      // ============================================================
      // DELTA CHECKS (compare to lastImportStats)
      // ============================================================
      if (config.deltaRules?.enabled) {
        const rules = config.deltaRules;
        const lastStats = (dataSource as any).lastImportStats;

        if (!lastStats) {
          results.push({
            name: "Historical Comparison",
            passed: true,
            message: "No previous import to compare against",
            category: "delta",
          });
        } else {
          if (rules.maxItemCountChange !== undefined && lastStats.itemCount) {
            const change =
              ((dbItemCount - lastStats.itemCount) / lastStats.itemCount) * 100;
            const passed = Math.abs(change) <= rules.maxItemCountChange;
            results.push({
              name: "Item Count Change",
              passed,
              message: `${change >= 0 ? "+" : ""}${change.toFixed(1)}% from last import (max: ±${rules.maxItemCountChange}%)`,
              category: "delta",
            });
          }

          if (rules.maxTotalStockChange !== undefined && lastStats.totalStock) {
            const change =
              ((dbTotalStock - lastStats.totalStock) / lastStats.totalStock) *
              100;
            const passed = Math.abs(change) <= rules.maxTotalStockChange;
            results.push({
              name: "Total Stock Change",
              passed,
              message: `${change >= 0 ? "+" : ""}${change.toFixed(1)}% from last import (max: ±${rules.maxTotalStockChange}%)`,
              category: "delta",
            });
          }

          if (
            rules.maxStyleCountChange !== undefined &&
            lastStats.uniqueStyles
          ) {
            const change =
              ((dbUniqueStyles - lastStats.uniqueStyles) /
                lastStats.uniqueStyles) *
              100;
            const passed = Math.abs(change) <= rules.maxStyleCountChange;
            results.push({
              name: "Style Count Change",
              passed,
              message: `${change >= 0 ? "+" : ""}${change.toFixed(1)}% from last import (max: ±${rules.maxStyleCountChange}%)`,
              category: "delta",
            });
          }
        }
      }

      // ============================================================
      // SPOT CHECKS
      // ============================================================
      if (config.spotChecks && config.spotChecks.length > 0) {
        for (const check of config.spotChecks) {
          if (!check.style) continue;

          const checkStyleUpper = String(check.style).toUpperCase().trim();
          const checkColorUpper = check.color
            ? String(check.color).toUpperCase().trim()
            : null;
          const checkSize = check.size ? String(check.size).trim() : null;

          const matching = inventoryItems.filter((i: any) => {
            const itemStyle = String(i.style || "")
              .toUpperCase()
              .trim();
            let match =
              itemStyle === checkStyleUpper ||
              itemStyle.includes(checkStyleUpper);
            if (checkColorUpper) {
              const itemColor = String(i.color || "")
                .toUpperCase()
                .trim();
              match =
                match &&
                (itemColor === checkColorUpper ||
                  itemColor.includes(checkColorUpper));
            }
            if (checkSize) {
              const itemSize = String(i.size || "").trim();
              match = match && itemSize === checkSize;
            }
            return match;
          });

          let passed = false;
          let message = "";

          switch (check.expectedCondition) {
            case "exists":
              passed = matching.length > 0;
              message = passed
                ? `Found ${matching.length} records`
                : "Not found in DB";
              break;
            case "stock_gt_0":
              passed = matching.some((i: any) => Number(i.stock) > 0);
              message = passed ? "Has stock > 0" : "No stock found";
              break;
            case "has_price":
              passed = matching.some(
                (i: any) => i.price && Number(i.price) > 0,
              );
              message = passed ? "Has price" : "No price found";
              break;
            case "is_discontinued":
              passed = matching.some((i: any) => i.discontinued);
              message = passed ? "Is discontinued" : "Not discontinued";
              break;
            case "has_future_date":
              passed = matching.some((i: any) => i.shipDate);
              message = passed
                ? `Has ship date: ${matching.find((i: any) => i.shipDate)?.shipDate}`
                : "No ship date";
              break;
          }

          results.push({
            name: `Spot: ${check.style}${check.color ? "/" + check.color : ""}${check.size ? "/" + check.size : ""}`,
            passed,
            message,
            category: "spot",
          });
        }
      }

      // If no checks configured, add basic data check
      if (results.length === 0) {
        results.push({
          name: "Data Exists",
          passed: dbItemCount > 0,
          message: `${dbItemCount} items found in database`,
          category: "basic",
        });
      }

      const passedChecks = results.filter((r) => r.passed).length;
      const totalChecks = results.length;
      const allPassed = passedChecks === totalChecks;

      console.log(
        `[AIImport] DB validation complete: ${passedChecks}/${totalChecks} checks passed`,
      );

      // ============================================================
      // DETAILED DIFF - Find missing/extra styles
      // ============================================================
      // ============================================================
      // DETAILED DIFF - Product Level Validation
      // ============================================================
      let detailedDiff: {
        // Style level
        missingStyles: string[];
        extraStyles: string[];
        missingStyleCount: number;
        extraStyleCount: number;
        // Color level
        missingColors: string[];
        extraColors: string[];
        missingColorCount: number;
        extraColorCount: number;
        // Product level issues
        productIssues: Array<{
          style: string;
          issue: string;
          expected: string;
          actual: string;
          severity: "error" | "warning" | "info";
        }>;
        // Summary counts
        hasProductData: boolean;
        totalProductsChecked: number;
        productsWithIssues: number;
        // Global stats
        styleListCount: number;
        dbStyleCount: number;
      } | null = null;

      const expectedStats = (dataSource as any).lastImportStats;

      // Build DB grouped data
      const dbByStyle: Record<
        string,
        {
          variantCount: number;
          colors: Set<string>;
          sizes: Set<string>;
          totalStock: number;
          discontinuedCount: number;
          futureDateCount: number;
          expandedCount: number;
          skus: string[];
        }
      > = {};

      const dbStyleSet = new Set<string>();
      const dbColorSet = new Set<string>();

      for (const item of inventoryItems) {
        const style = String(item.style || "")
          .toUpperCase()
          .trim();
        const color = String(item.color || "")
          .toUpperCase()
          .trim();
        const size = String(item.size || "").trim();
        const sku = String(item.sku || "")
          .toUpperCase()
          .trim();

        if (style) dbStyleSet.add(style);
        if (color) dbColorSet.add(color);

        if (!dbByStyle[style]) {
          dbByStyle[style] = {
            variantCount: 0,
            colors: new Set(),
            sizes: new Set(),
            totalStock: 0,
            discontinuedCount: 0,
            futureDateCount: 0,
            expandedCount: 0,
            skus: [],
          };
        }

        const product = dbByStyle[style];
        product.variantCount++;
        if (color) product.colors.add(color);
        if (size) product.sizes.add(size);
        product.totalStock += Number(item.stock) || 0;
        if (item.discontinued) product.discontinuedCount++;
        if (item.shipDate) product.futureDateCount++;
        if (item.isExpandedSize) product.expandedCount++;
        if (sku && product.skus.length < 20) product.skus.push(sku);
      }

      console.log(
        `[AIImport] Checking detailedDiff - productData exists: ${!!expectedStats?.productData}`,
      );

      // Style/Color level comparison
      const missingStyles: string[] = [];
      const extraStyles: string[] = [];
      const missingColors: string[] = [];
      const extraColors: string[] = [];

      if (expectedStats?.styleList) {
        const expectedStyleSet = new Set(
          expectedStats.styleList.map((s: string) =>
            String(s).toUpperCase().trim(),
          ),
        );
        for (const style of expectedStyleSet) {
          if (!dbStyleSet.has(style)) missingStyles.push(style);
        }
        for (const style of dbStyleSet) {
          if (!expectedStyleSet.has(style)) extraStyles.push(style);
        }
      }

      if (expectedStats?.colorList) {
        const expectedColorSet = new Set(
          expectedStats.colorList.map((c: string) =>
            String(c).toUpperCase().trim(),
          ),
        );
        for (const color of expectedColorSet) {
          if (!dbColorSet.has(color)) missingColors.push(color);
        }
        for (const color of dbColorSet) {
          if (!expectedColorSet.has(color)) extraColors.push(color);
        }
      }

      // Product-level validation
      const productIssues: Array<{
        style: string;
        issue: string;
        expected: string;
        actual: string;
        severity: "error" | "warning" | "info";
      }> = [];

      const expectedProductData = expectedStats?.productData || {};
      const hasProductData = Object.keys(expectedProductData).length > 0;
      let productsWithIssues = 0;

      if (hasProductData) {
        // Check each expected product against DB
        for (const [style, expected] of Object.entries(expectedProductData) as [
          string,
          any,
        ][]) {
          const dbProduct = dbByStyle[style];
          let hasIssue = false;

          if (!dbProduct) {
            // Product completely missing from DB
            productIssues.push({
              style,
              issue: "MISSING_PRODUCT",
              expected: `${expected.variantCount} variants`,
              actual: "Not in DB",
              severity: "error",
            });
            hasIssue = true;
          } else {
            // Compare variant counts
            const expectedVariants = expected.variantCount || 0;
            const dbVariants = dbProduct.variantCount || 0;
            const variantDiff = dbVariants - expectedVariants;

            if (variantDiff !== 0) {
              const expandedInDb = dbProduct.expandedCount || 0;
              const expandedExpected = expected.expandedCount || 0;

              if (variantDiff > 0) {
                // More variants in DB than expected
                if (expandedInDb > expandedExpected) {
                  // Extra due to size expansion
                  productIssues.push({
                    style,
                    issue: "EXTRA_EXPANDED_SIZES",
                    expected: `${expectedVariants} variants (${expandedExpected} expanded)`,
                    actual: `${dbVariants} variants (${expandedInDb} expanded, +${variantDiff} extra)`,
                    severity: "info",
                  });
                } else {
                  // Extra variants not from expansion
                  productIssues.push({
                    style,
                    issue: "EXTRA_VARIANTS",
                    expected: `${expectedVariants} variants`,
                    actual: `${dbVariants} variants (+${variantDiff} extra)`,
                    severity: "warning",
                  });
                }
                hasIssue = true;
              } else {
                // Fewer variants in DB than expected
                productIssues.push({
                  style,
                  issue: "MISSING_VARIANTS",
                  expected: `${expectedVariants} variants`,
                  actual: `${dbVariants} variants (${Math.abs(variantDiff)} missing)`,
                  severity: "error",
                });
                hasIssue = true;
              }
            }

            // Compare stock
            const expectedStock = expected.totalStock || 0;
            const dbStock = dbProduct.totalStock || 0;
            const stockDiff = Math.abs(dbStock - expectedStock);
            const stockPctDiff =
              expectedStock > 0 ? (stockDiff / expectedStock) * 100 : 0;

            if (stockPctDiff > 10) {
              // More than 10% stock difference
              productIssues.push({
                style,
                issue: "STOCK_MISMATCH",
                expected: `${expectedStock} total stock`,
                actual: `${dbStock} total stock (${stockDiff > 0 ? (dbStock > expectedStock ? "+" : "-") + stockDiff : "0"})`,
                severity: stockPctDiff > 50 ? "error" : "warning",
              });
              hasIssue = true;
            }

            // Check discontinued flags
            if (expected.hasDiscontinued && dbProduct.discontinuedCount === 0) {
              productIssues.push({
                style,
                issue: "DISCONTINUED_FLAG_LOST",
                expected: "Has discontinued items",
                actual: "No discontinued items in DB",
                severity: "warning",
              });
              hasIssue = true;
            }

            // Check future dates
            if (expected.hasFutureDate && dbProduct.futureDateCount === 0) {
              productIssues.push({
                style,
                issue: "FUTURE_DATE_LOST",
                expected: "Has future date items",
                actual: "No future date items in DB",
                severity: "warning",
              });
              hasIssue = true;
            }

            // Check colors per product
            const expectedColors = new Set(expected.colors || []);
            const dbColors = dbProduct.colors;
            const missingProductColors: string[] = [];
            const extraProductColors: string[] = [];

            for (const color of expectedColors) {
              if (!dbColors.has(color)) missingProductColors.push(color);
            }
            for (const color of dbColors) {
              if (!expectedColors.has(color)) extraProductColors.push(color);
            }

            if (missingProductColors.length > 0) {
              productIssues.push({
                style,
                issue: "MISSING_COLORS",
                expected: `Colors: ${Array.from(expectedColors).join(", ")}`,
                actual: `Missing: ${missingProductColors.join(", ")}`,
                severity: "warning",
              });
              hasIssue = true;
            }
          }

          if (hasIssue) productsWithIssues++;

          // Limit total issues to prevent huge payloads
          if (productIssues.length >= 100) break;
        }

        // Check for products in DB that weren't expected
        for (const style of Object.keys(dbByStyle)) {
          if (!expectedProductData[style] && productIssues.length < 100) {
            productIssues.push({
              style,
              issue: "UNEXPECTED_PRODUCT",
              expected: "Not in import",
              actual: `${dbByStyle[style].variantCount} variants in DB`,
              severity: "warning",
            });
            productsWithIssues++;
          }
        }
      }

      detailedDiff = {
        // Style level
        missingStyles: missingStyles.slice(0, 50),
        extraStyles: extraStyles.slice(0, 50),
        missingStyleCount: missingStyles.length,
        extraStyleCount: extraStyles.length,
        // Color level
        missingColors: missingColors.slice(0, 30),
        extraColors: extraColors.slice(0, 30),
        missingColorCount: missingColors.length,
        extraColorCount: extraColors.length,
        // Product level
        productIssues: productIssues.slice(0, 50),
        hasProductData,
        totalProductsChecked: Object.keys(expectedProductData).length,
        productsWithIssues,
        // Global stats
        styleListCount: expectedStats?.styleList?.length || 0,
        dbStyleCount: dbStyleSet.size,
      };

      console.log(
        `[AIImport] Detailed diff: ${missingStyles.length} missing styles, ${extraStyles.length} extra styles, ${productIssues.length} product issues`,
      );

      res.json({
        success: true,
        passed: allPassed,
        // DB Stats
        itemCount: dbItemCount,
        styleCount: dbUniqueStyles,
        colorCount: dbUniqueColors,
        sizeCount: dbUniqueSizes,
        totalStock: dbTotalStock,
        itemsWithPrice: dbItemsWithPrice,
        itemsWithStock: dbItemsWithStock,
        itemsDiscontinued: dbItemsDiscontinued,
        itemsWithShipDate: dbItemsWithShipDate,
        itemsExpanded: dbItemsExpanded,
        // Expected Stats (from last import)
        expectedStats: expectedStats || null,
        // Detailed Diff (styles missing/extra)
        detailedDiff,
        // Check Results
        totalChecks,
        passedChecks,
        failedChecks: totalChecks - passedChecks,
        results,
      });
    } catch (error: any) {
      console.error("[AIImport] Validate DB error:", error);
      res.status(500).json({
        success: false,
        error: error.message || "Failed to validate database",
      });
    }
  },
);

/**
 * Shared multi-file import function — used by BOTH the /execute route handler
 * AND the email fetcher. This ensures identical processing for "Import 2 Files"
 * button and email-based imports.
 *
 * Takes raw file buffers, consolidates them, parses with parseIntelligentPivotFormat,
 * applies the full import pipeline, and saves to DB.
 */
export async function executeAIImport(
  fileBuffers: { buffer: Buffer; originalname: string }[],
  dataSourceId: string,
  overrideConfig?: any,
): Promise<{ success: boolean; itemCount: number; error?: string; fileId?: string; stats?: any }> {
  const dataSource = await storage.getDataSource(dataSourceId);
  if (!dataSource) {
    return { success: false, itemCount: 0, error: "Data source not found" };
  }

  console.log(`[AIImport:shared] Loaded dataSource "${dataSource.name}", files=${fileBuffers.length}`);

  const enhancedConfig: any = {
    formatType: (dataSource as any).formatType,
    columnMapping: dataSource.columnMapping,
    pivotConfig: (dataSource as any).pivotConfig,
    discontinuedConfig:
      (dataSource as any).discontinuedConfig ||
      (dataSource as any).discontinuedRules,
    futureStockConfig: (dataSource as any).futureStockConfig,
    stockValueConfig:
      (dataSource as any).stockValueConfig ||
      (dataSource.cleaningConfig?.stockTextMappings?.length > 0
        ? { textMappings: dataSource.cleaningConfig.stockTextMappings }
        : undefined),
    cleaningConfig: (dataSource as any).cleaningConfig,
  };

  const config: EnhancedImportConfig = {
    formatType:
      overrideConfig?.formatType || enhancedConfig.formatType || "row",
    columnMapping:
      overrideConfig?.columnMapping || enhancedConfig.columnMapping || {},
    pivotConfig: overrideConfig?.pivotConfig || enhancedConfig.pivotConfig,
    discontinuedConfig:
      overrideConfig?.discontinuedConfig || enhancedConfig.discontinuedConfig,
    futureStockConfig:
      overrideConfig?.futureStockConfig || enhancedConfig.futureStockConfig,
    stockValueConfig:
      overrideConfig?.stockValueConfig || enhancedConfig.stockValueConfig,
    cleaningConfig: (dataSource as any).cleaningConfig,
  };

  // Step 1: Consolidate multiple files
  let rawData: any[][] = [];
  const primaryFile = fileBuffers[0];

  if (fileBuffers.length > 1) {
    console.log(`[AIImport:shared] Consolidating ${fileBuffers.length} files`);
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
        rawData.push(...data.slice(1));
      }
    }
    console.log(`[AIImport:shared] Consolidated ${rawData.length} total rows from ${fileBuffers.length} files`);
  } else {
    const workbook = XLSX.read(primaryFile.buffer, { type: "buffer" });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    rawData = XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      defval: "",
    }) as any[][];
  }

  // Step 2: Create consolidated buffer for pivot parsing
  let consolidatedBuffer: Buffer;
  if (fileBuffers.length > 1) {
    const newWorkbook = XLSX.utils.book_new();
    const newSheet = XLSX.utils.aoa_to_sheet(rawData);
    XLSX.utils.book_append_sheet(newWorkbook, newSheet, "Consolidated");
    consolidatedBuffer = Buffer.from(
      XLSX.write(newWorkbook, { type: "buffer", bookType: "xlsx" }),
    );
    console.log(`[AIImport:shared] Created consolidated buffer (${consolidatedBuffer.length} bytes)`);
  } else {
    consolidatedBuffer = primaryFile.buffer;
  }

  // Step 3: Auto-detect format and parse
  const detectedPivotFormat = autoDetectPivotFormat(
    rawData,
    dataSource.name,
    primaryFile.originalname,
  );
  const isPivotFormat =
    config.formatType?.startsWith("pivot") ||
    config.formatType === "pivoted" ||
    detectedPivotFormat !== null;

  let parseResult: any;

  if (isPivotFormat) {
    const actualFormat =
      detectedPivotFormat || config.formatType || "pivot_interleaved";
    const universalConfig: UniversalParserConfig = {
      skipRows: config.pivotConfig?.skipRows,
      discontinuedConfig: config.discontinuedConfig as any,
      futureDateConfig: config.futureStockConfig as any,
      stockConfig: config.stockValueConfig as any,
      columnMapping: config.columnMapping,
    };

    const pivotResult = parseIntelligentPivotFormat(
      consolidatedBuffer,
      actualFormat,
      universalConfig,
      dataSource.name,
      primaryFile.originalname,
    );

    parseResult = {
      success: true,
      items: pivotResult.items,
      stats: {
        totalRows: pivotResult.rows.length,
        totalItems: pivotResult.items.length,
        discontinuedItems: pivotResult.items.filter((i: any) => i.discontinued).length,
        futureStockItems: pivotResult.items.filter((i: any) => i.shipDate).length,
      },
      warnings: [],
    };
  } else {
    parseResult = await parseWithEnhancedConfig(
      primaryFile.buffer,
      config,
      dataSourceId,
    );
  }

  if (!parseResult.success) {
    return { success: false, itemCount: 0, error: "Failed to parse file" };
  }

  console.log(`[AIImport:shared] Parsed ${parseResult.items.length} items`);

  // Step 4: Apply import rules
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
    cleaningConfig:
      overrideConfig?.cleaningConfig || (dataSource as any).cleaningConfig,
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
    parseResult.items,
    importRulesConfig,
    rawData,
  );

  console.log(`[AIImport:shared] After import rules: ${importRulesResult.items.length} items`);

  // Step 5: Apply global color mappings
  let itemsWithMappedColors = importRulesResult.items;
  let colorsFixed = 0;

  try {
    const colorMappings = await storage.getColorMappings();
    const colorMap = new Map<string, string>();
    for (const mapping of colorMappings) {
      const normalizedBad = mapping.badColor.trim().toLowerCase();
      colorMap.set(normalizedBad, mapping.goodColor);
    }

    if (colorMap.size > 0) {
      itemsWithMappedColors = importRulesResult.items.map((item: any) => {
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

      if (colorsFixed > 0) {
        console.log(`[AIImport:shared] Fixed ${colorsFixed} colors using global mappings`);
      }
    }
  } catch (colorMapError: any) {
    console.error(`[AIImport:shared] Error applying color mappings:`, colorMapError);
  }

  // Step 6: Apply style prefix
  const cleaningConfig = (dataSource.cleaningConfig || {}) as any;
  const getStylePrefixForAI = (style: string): string => {
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
  };

  const itemsWithPrefix = itemsWithMappedColors.map((item: any) => {
    const rawStyle = String(item.style || "").trim();
    const prefix = rawStyle ? getStylePrefixForAI(rawStyle) : dataSource.name;
    const prefixedStyle = rawStyle ? `${prefix} ${rawStyle}` : rawStyle;
    const prefixedSku =
      prefixedStyle && item.color && item.size
        ? `${prefixedStyle}-${item.color}-${item.size}`
            .replace(/\//g, "-")
            .replace(/\s+/g, "-")
            .replace(/-+/g, "-")
        : item.sku;
    return {
      ...item,
      style: prefixedStyle,
      sku: prefixedSku,
    };
  });

  console.log(`[AIImport:shared] Applied prefix to ${itemsWithPrefix.length} items`);

  // Step 7: Apply variant rules
  const variantRulesConfigOverride =
    overrideConfig?.filterZeroStock !== undefined
      ? {
          filterZeroStock: overrideConfig.filterZeroStock,
          filterZeroStockWithFutureDates:
            overrideConfig?.filterZeroStockWithFutureDates,
        }
      : undefined;

  const variantRulesResult = await applyVariantRules(
    itemsWithPrefix,
    dataSourceId,
    variantRulesConfigOverride,
  );

  console.log(`[AIImport:shared] After variant rules: ${variantRulesResult.items.length} items`);

  // Step 8: Price-based size expansion
  let priceBasedExpansionCount = 0;
  let itemsAfterExpansion = variantRulesResult.items;
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
        const cacheVariants = await storage.getVariantCacheProductStyles(shopifyStoreId);
        const stylePriceMap = buildStylePriceMapFromCache(cacheVariants);
        const expansionResult = applyPriceBasedExpansion(
          variantRulesResult.items,
          priceBasedExpansionConfig,
          stylePriceMap,
          sizeLimitConfig,
        );
        itemsAfterExpansion = expansionResult.items;
        priceBasedExpansionCount = expansionResult.addedCount;
        if (priceBasedExpansionCount > 0) {
          console.log(`[AIImport:shared] Price-based expansion added ${priceBasedExpansionCount} size variants`);
        }
      } catch (expansionError) {
        console.error(`[AIImport:shared] Price-based expansion error:`, expansionError);
      }
    }
  }

  // Step 9: Filter discontinued styles
  const isSaleFile = (dataSource as any).sourceType === "sales";
  const linkedSaleDataSourceId = (dataSource as any).assignedSaleDataSourceId;
  let discontinuedStylesFiltered = 0;
  let discontinuedItemsRemoved = 0;
  let itemsToImport = itemsAfterExpansion;

  if (!isSaleFile && linkedSaleDataSourceId) {
    try {
      discontinuedItemsRemoved = await removeDiscontinuedInventoryItems(
        dataSourceId,
        linkedSaleDataSourceId,
      );
      const filterResult = await filterDiscontinuedStyles(
        dataSourceId,
        itemsAfterExpansion,
        linkedSaleDataSourceId,
      );
      itemsToImport = filterResult.items;
      discontinuedStylesFiltered = filterResult.removedCount;
      if (discontinuedStylesFiltered > 0) {
        console.log(`[AIImport:shared] Filtered out ${discontinuedStylesFiltered} discontinued items`);
      }
    } catch (discontinuedError) {
      console.error(`[AIImport:shared] Discontinued filtering error:`, discontinuedError);
    }
  }

  const processedItems = itemsToImport;

  // Step 10: Create file record
  const file = await storage.createUploadedFile({
    dataSourceId,
    fileName:
      fileBuffers.length > 1
        ? `${fileBuffers.length} files consolidated`
        : primaryFile.originalname,
    status: "completed",
    rowCount: processedItems.length,
    processedAt: new Date(),
  });

  // Step 11: Calculate stockInfo
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
        await storage.getShopifyMetafieldRulesByDataSource(dataSourceId);
      const activeDbRule = metafieldRules.find((r: any) => r.enabled !== false);
      if (activeDbRule) {
        stockInfoRule = {
          id: activeDbRule.id,
          name: activeDbRule.name || "Rule Engine Metafield Rule",
          stockThreshold: activeDbRule.stockThreshold ?? activeDbRule.stock_threshold ?? 0,
          inStockMessage: activeDbRule.inStockMessage || activeDbRule.in_stock_message || "",
          sizeExpansionMessage: activeDbRule.sizeExpansionMessage || activeDbRule.size_expansion_message || null,
          outOfStockMessage: activeDbRule.outOfStockMessage || activeDbRule.out_of_stock_message || "",
          futureDateMessage: activeDbRule.futureDateMessage || activeDbRule.future_date_message || null,
          dateOffsetDays: activeDbRule.dateOffsetDays ?? activeDbRule.date_offset_days ?? 0,
          enabled: true,
        };
      }
    }
  } catch (ruleError) {
    console.error(`[AIImport:shared] Failed to get stock info rules:`, ruleError);
  }

  const calcStockInfo = (item: any): string | null => {
    if (!stockInfoRule) return null;
    const stock = item.stock || 0;
    const shipDate = item.shipDate;
    const isExpandedSize = item.isExpandedSize || false;
    const threshold = stockInfoRule.stockThreshold || 0;

    if (isExpandedSize && stockInfoRule.sizeExpansionMessage) {
      return stockInfoRule.sizeExpansionMessage;
    }
    if (shipDate && stockInfoRule.futureDateMessage) {
      try {
        const dateStr = String(shipDate).trim();
        let targetDate: Date;
        const isoMatch = dateStr.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
        const usMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
        const usShortMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2})$/);
        if (isoMatch) {
          targetDate = new Date(parseInt(isoMatch[1]), parseInt(isoMatch[2]) - 1, parseInt(isoMatch[3]));
        } else if (usMatch) {
          targetDate = new Date(parseInt(usMatch[3]), parseInt(usMatch[1]) - 1, parseInt(usMatch[2]));
        } else if (usShortMatch) {
          targetDate = new Date(2000 + parseInt(usShortMatch[3]), parseInt(usShortMatch[1]) - 1, parseInt(usShortMatch[2]));
        } else {
          targetDate = new Date(dateStr);
        }
        const offsetDays = stockInfoRule.dateOffsetDays || 0;
        if (offsetDays !== 0) {
          targetDate.setDate(targetDate.getDate() + offsetDays);
        }
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        targetDate.setHours(0, 0, 0, 0);
        if (targetDate > today) {
          const formattedDate = targetDate.toLocaleDateString("en-US", {
            month: "long", day: "numeric", year: "numeric",
          });
          return stockInfoRule.futureDateMessage.replace(/\{date\}/gi, formattedDate);
        }
      } catch (e) { /* ignore date parse errors */ }
    }
    if (stock > threshold) {
      return stockInfoRule.inStockMessage;
    }
    let outOfStockMsg = stockInfoRule.outOfStockMessage;
    if (outOfStockMsg && outOfStockMsg.includes("{date}")) {
      outOfStockMsg = outOfStockMsg.replace(/\{date\}/gi, "").replace(/\s+/g, " ").trim();
    }
    return outOfStockMsg || null;
  };

  // Step 12: Map items for saving
  const itemsToSave = processedItems.map((item: any) => ({
    dataSourceId,
    fileId: file.id,
    sku:
      item.sku ||
      `${item.style}-${item.color}-${item.size}`.toUpperCase().replace(/\s+/g, "-"),
    style: item.style,
    color: item.color || "",
    size: item.size || "",
    stock: item.stock || 0,
    price: item.price,
    cost: item.cost,
    shipDate: item.shipDate,
    discontinued: item.discontinued || false,
    isExpandedSize: item.isExpandedSize || false,
    stockInfo: calcStockInfo(item),
    rawData: item.rawData || null,
  }));

  // Step 13: Save to database
  const updateStrategy = dataSource.updateStrategy || "full_sync";
  console.log(`[AIImport:shared] Saving ${itemsToSave.length} items (strategy=${updateStrategy})`);

  if (updateStrategy === "replace") {
    await storage.upsertInventoryItems(itemsToSave, dataSourceId);
  } else {
    await storage.deleteInventoryItemsByDataSource(dataSourceId);
    await storage.createInventoryItems(itemsToSave);
  }

  await storage.updateDataSource(dataSourceId, { lastSync: new Date() });

  console.log(`[AIImport:shared] DONE: ${itemsToSave.length} items saved for "${dataSource.name}"`);

  return {
    success: true,
    itemCount: itemsToSave.length,
    fileId: file.id,
    stats: {
      totalParsed: parseResult.items.length,
      afterImportRules: importRulesResult.items.length,
      afterVariantRules: variantRulesResult.items.length,
      afterPriceExpansion: itemsAfterExpansion.length,
      afterDiscontinuedFilter: processedItems.length,
      finalCount: itemsToSave.length,
      colorsFixed,
      priceBasedExpansion: priceBasedExpansionCount,
      discontinuedStylesFiltered,
      discontinuedItemsRemoved,
    },
  };
}

export default router;
