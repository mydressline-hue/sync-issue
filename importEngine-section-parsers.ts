// ============ SECTION 1: TYPES & VENDOR PARSERS ============
//
// Extracted from aiImportRoutes (6).ts
// Contains: interfaces, helper functions, and all 10 vendor-specific parsers.
// These are INTERNAL to the import engine — only types are exported.

import { isValidShipDate } from "./inventoryProcessing";

// ============================================================
// TYPE DEFINITIONS
// ============================================================

export interface PivotItem {
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

export interface DiscontinuedConfig {
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

export interface FutureDateConfig {
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

export interface StockConfig {
  type: "numeric" | "text" | "pivot";
  column?: string | number;
  textMappings?: Record<string, number>;
}

export interface BrandDetectionConfig {
  enabled: boolean;
  sourceColumn: string | number;
  extractionMethod: "prefix" | "contains" | "regex";
  knownBrands?: string[];
  regex?: string;
}

export interface UniversalParserConfig {
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
    "\u2013": 0,
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
// SHARED COLUMN RESOLUTION HELPER
// ============================================================
// Used by ALL parsers to resolve column indices.
// Checks user-mapped override (from UI dropdown) FIRST,
// then falls back to auto-detection using pattern matching.

function resolveColumnIndex(
  config: UniversalParserConfig,
  headersLower: string[],
  field: string,
  autoPatterns: string[],
): number {
  // 1. Check user-mapped column override first (from UI dropdown selection)
  if (config.columnMapping?.[field]) {
    const mappedCol = config.columnMapping[field].toLowerCase().trim();
    const idx = headersLower.findIndex((h: string) => h === mappedCol);
    if (idx !== -1) return idx;
  }
  // 2. Fall back to auto-detection using pattern matching
  for (const p of autoPatterns) {
    const idx = headersLower.findIndex(
      (h: string) => h === p || h.includes(p),
    );
    if (idx !== -1) return idx;
  }
  return -1;
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

  const headersLower = headerRow.map((h: any) =>
    String(h || "").toLowerCase().trim(),
  );
  const deliveryIdx = headers.findIndex((h: string) => h.includes("DELIVERY"));
  const styleIdx = resolveColumnIndex(config, headersLower, "style", ["style"]);
  const colorIdx = resolveColumnIndex(config, headersLower, "color", ["color"]);
  const priceIdx = resolveColumnIndex(config, headersLower, "price", [
    "price", "wholesale", "cost", "msrp", "line price",
  ]);

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

    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;

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
        price,
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

  // For price: check if user mapped a specific price column (overrides default column 1)
  let priceColIdx = 1; // Default: price is in column 1 for Jovani format
  if (config.columnMapping?.price) {
    const headersLower = headerRow.map((h: any) => String(h || "").toLowerCase().trim());
    const userIdx = resolveColumnIndex(config, headersLower, "price", []);
    if (userIdx >= 0) priceColIdx = userIdx;
  }

  // Style patterns: #02861, JVN04759, 04859, AL02665, etc.
  const stylePattern = /^#?\d{4,6}$|^#?\d{5}[A-Z]?$|^[A-Z]{2,3}\d{4,6}$/i;

  let currentStyle = "";
  let currentPrice = 0;

  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.every((c: any) => !c && c !== 0)) continue;

    const cell0 = String(row[0] ?? "").trim();
    const priceCell = row[priceColIdx];

    // Check if this is a style row
    if (stylePattern.test(cell0)) {
      currentStyle = cell0.replace(/^#/, "");
      currentPrice =
        typeof priceCell === "number" ? priceCell : parseFloat(String(priceCell || "0"));
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

  // For price: check if a header row exists before data
  let priceIdx = -1;

  // Helper to detect if first cell is a date (DD/MM/YYYY or similar)
  const isDateString = (val: string): boolean => {
    return (
      /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(val) ||
      /^\d{4}-\d{2}-\d{2}$/.test(val) ||
      /^\d{1,2}-\d{1,2}-\d{4}$/.test(val)
    );
  };

  // Helper to detect Excel serial date numbers (dates stored as raw numbers)
  const isExcelSerialDate = (val: any): boolean => {
    if (typeof val !== "number") return false;
    // Excel serial dates for years 2020-2035 range from ~43831 to ~49400
    return val > 43000 && val < 50000;
  };

  // Helper to convert Excel serial number to ISO date string
  const excelSerialToISO = (serial: number): string => {
    // Excel epoch is Jan 1, 1900 (with the Lotus 1-2-3 leap year bug)
    const excelEpoch = new Date(1899, 11, 30);
    const date = new Date(excelEpoch.getTime() + serial * 86400000);
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const d = String(date.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  };

  // Helper to parse date string to ISO format
  const parseDateToISO = (val: string): string | null => {
    const ddmmyyyy = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (ddmmyyyy) {
      const [, day, month, year] = ddmmyyyy;
      return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(val)) return val;
    return null;
  };

  // Find style header rows and data start
  // Style header rows: have size numbers in columns 13+ and product name in column 7
  let currentStyle = "";
  let sizeHeaders: { index: number; size: string }[] = [];

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    if (!row || row.length === 0) continue;

    const firstCell = String(row[0] ?? "").trim();

    // Price column detection from header-like rows
    if (i < 15 && priceIdx < 0 && row.length > 5) {
      const possibleHeaders = row.map((h: any) => String(h || "").toLowerCase().trim());
      priceIdx = resolveColumnIndex(config, possibleHeaders, "price", [
        "price", "wholesale", "cost", "msrp", "line price",
      ]);
    }

    // Style header detection: column 13 has a number (size) AND column 7 exists (product name)
    // OR style pattern like "98XX..." in first cell
    const stylePattern = /^\d{2}[A-Z]{2,}/i;
    const col13val = row[13] !== null && row[13] !== undefined ? String(row[13]).trim() : "";
    const isStyleRow = (col13val.match(/^\d+$/) && row[7]) || stylePattern.test(firstCell) || /^\d{5,}$/.test(firstCell);

    if (isStyleRow && !firstCell.match(/^D$/i) && !isDateString(firstCell) && !isExcelSerialDate(row[0])) {
      currentStyle = firstCell;

      // Extract size headers from this row (any non-empty value in cols 13+)
      sizeHeaders = [];
      for (let j = 13; j < row.length; j++) {
        if (row[j] !== null && row[j] !== undefined && row[j] !== "") {
          sizeHeaders.push({ index: j, size: String(row[j]) });
        }
      }
      continue;
    }

    // Data rows: "D" for current inventory OR date string for future ship dates
    // NOTE: In Tarik Ediz format, "D" means CURRENT STOCK (not discontinued)
    const isCurrentStock = firstCell === "D";
    const isFutureShipDate = isDateString(firstCell);
    const isSerialDate = isExcelSerialDate(row[0]);

    if ((isCurrentStock || isFutureShipDate || isSerialDate) && row[11] && currentStyle) {
      const color = String(row[11]).trim();
      let shipDate: string | null = null;

      if (isFutureShipDate) {
        shipDate = parseDateToISO(firstCell);
      } else if (isSerialDate) {
        shipDate = excelSerialToISO(row[0] as number);
      }

      const price =
        priceIdx >= 0
          ? parseFloat(String(row[priceIdx] || "0")) || undefined
          : undefined;

      // Extract stock values for each size
      for (const sh of sizeHeaders) {
        const stockRaw = row[sh.index];
        const stockNum =
          stockRaw !== null && stockRaw !== undefined && !isNaN(Number(stockRaw))
            ? Number(stockRaw)
            : 0;

        // Include ALL items: stock > 0 OR has future ship date
        if (stockNum > 0 || shipDate) {
          const item: any = {
            style: currentStyle,
            color,
            size: sh.size,
            stock: stockNum,
            price,
            // "D" = current stock, NOT discontinued. Do not set discontinued flag.
            shipDate: shipDate || undefined,
          };

          // Set future stock flags (critical for downstream processing)
          if (shipDate) {
            item.hasFutureStock = true;
            if (stockNum === 0) {
              item.preserveZeroStock = true;
            }
          }

          items.push(item);
        }
      }
    }
  }

  console.log(
    `[TarikEdiz] Parsed ${items.length} items (including future ship date items)`,
  );
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
  const headersLowerSH = headerRow.map((h: any) => String(h || "").toLowerCase().trim());

  const priceIdx = resolveColumnIndex(config, headersLowerSH, "price", [
    "price", "wholesale", "cost", "msrp", "line price",
  ]);

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

    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;

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
        dateVal !== "\u2013"
      ) {
        if (typeof dateVal === "number" && dateVal > 40000) {
          shipDate = excelSerialToDate(dateVal);
        }
      }

      if (stock > 0 || (shipDate && isValidShipDate(shipDate))) {
        items.push({ style, color, size: sc.size, stock, price, shipDate });
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
  const headersLower = headers.map((h: string) => h.toLowerCase());

  const styleIdx = resolveColumnIndex(config, headersLower, "style", [
    "style", "code", "item",
  ]);
  const colorIdx = resolveColumnIndex(config, headersLower, "color", [
    "color", "colour",
  ]);
  const dateIdx = resolveColumnIndex(config, headersLower, "shipDate", [
    "date", "eta", "due", "available",
  ]);
  const statusIdx = resolveColumnIndex(config, headersLower, "discontinued", [
    "status", "discontinued", "active",
  ]);
  const priceIdx = resolveColumnIndex(config, headersLower, "price", [
    "price", "wholesale", "cost", "msrp", "line price",
  ]);

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

    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;

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
          price,
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

  const headersLower = headers.map((h: string) => h.toLowerCase());

  const styleIdx = resolveColumnIndex(config, headersLower, "style", [
    "product", "code",
  ]);
  const availableIdx = resolveColumnIndex(config, headersLower, "stock", [
    "available",
  ]);
  const priceIdx = resolveColumnIndex(config, headersLower, "price", [
    "price", "wholesale", "cost", "msrp", "line price",
  ]);

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

    // FIX: Normalize leading-zero sizes: "06" -> "6", but preserve "0" and "00"
    if (extractedSize && /^0+[1-9]\d*$/.test(extractedSize)) {
      extractedSize = extractedSize.replace(/^0+/, "");
    }

    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;

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
        price,
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
          price,
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

  const codeIdx = resolveColumnIndex(config, headersLower, "style", ["code"]);
  const colorIdx = resolveColumnIndex(config, headersLower, "color", ["color"]);
  const priceIdx = resolveColumnIndex(config, headersLower, "price", [
    "price", "wholesale", "cost", "msrp", "line price",
  ]);

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

    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;

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
          price,
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

  const styleIdx = resolveColumnIndex(config, headers, "style", ["style"]);
  const colorIdx = resolveColumnIndex(config, headers, "color", ["color"]);
  const priceIdx = resolveColumnIndex(config, headers, "price", [
    "price", "wholesale", "cost", "msrp", "line price",
  ]);

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
  // Direct vendor/brand column (e.g., "Vendor", "Brand", "Designer", "Vendor Name")
  const vendorIdx = headersLower.findIndex(
    (h: string) =>
      h.includes("vendor") || h.includes("brand") || h.includes("designer") ||
      h.includes("manufacturer"),
  );
  const styleIdx = resolveColumnIndex(config, headersLower, "style", ["style"]);
  const colorIdx = resolveColumnIndex(config, headersLower, "color", ["color"]);
  const sizeIdx = resolveColumnIndex(config, headersLower, "size", ["size"]);
  const stockIdx = resolveColumnIndex(config, headersLower, "stock", [
    "stock", "qty", "quantity",
  ]);
  const priceIdx = resolveColumnIndex(config, headersLower, "price", [
    "price", "wholesale", "cost", "msrp", "line price",
  ]);

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
    // Priority 1: Direct vendor/brand column (e.g., "Jovani" in Column A)
    if (vendorIdx >= 0) {
      const vendorVal = String(row[vendorIdx] ?? "").trim();
      if (vendorVal) brand = vendorVal;
    }
    // Priority 2: Extract brand from product name by matching known brands
    if (!brand && productName) {
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

  const styleIdx = resolveColumnIndex(config, headersLower, "style", [
    "style", "style#", "item", "product_id", "product", "code", "sku",
  ]);
  const colorIdx = resolveColumnIndex(config, headersLower, "color", [
    "color", "colour", "_color_name", "color_descript",
  ]);
  const sizeIdx = resolveColumnIndex(config, headersLower, "size", [
    "size", "_size", "sizename",
  ]);
  const stockIdx = resolveColumnIndex(config, headersLower, "stock", [
    "stock", "qty", "quantity", "available", "onhand", "ats_qty",
    "opentosale", "inventory", "_inventory_level", "immediate stock",
  ]);
  const priceIdx = resolveColumnIndex(config, headersLower, "price", [
    "price", "wholesale", "cost", "line price", "msrp", "_price",
  ]);
  const dateIdx = resolveColumnIndex(config, headersLower, "shipDate", [
    "eta", "ship", "date", "arrival", "expected", "future ship",
  ]);
  const statusIdx = resolveColumnIndex(config, headersLower, "discontinued", [
    "status", "discontinued", "active", "_status",
  ]);

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
