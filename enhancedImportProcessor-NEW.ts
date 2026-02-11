/**
 * Enhanced Import Processor
 *
 * Handles:
 * 1. Row-based and pivoted formats
 * 2. Complex stock cell parsing (text with embedded values/dates)
 * 3. Column-based sale detection (sale price column → apply multiplier)
 * 4. Price-based size expansion
 * 5. Future stock/ship date extraction
 * 6. Discontinued detection from patterns
 */

import * as XLSX from "xlsx";
import {
  ComplexStockPattern,
  ColumnBasedSaleConfig,
  PriceBasedExpansionTier,
} from "./aiFormatDetection";
import { parseGroupedPivotData, GroupedPivotConfig } from "./universalParser";

// ============================================================
// TYPES
// ============================================================

export interface EnhancedImportConfig {
  formatType:
    | "row"
    | "pivoted"
    | "pivot_interleaved"
    | "pivot_grouped"
    | "pivot_alternating";

  // Column mapping
  columnMapping: {
    style?: string;
    color?: string;
    size?: string;
    stock?: string;
    price?: string;
    salePrice?: string;
    cost?: string;
    sku?: string;
    shipDate?: string;
    status?: string;
  };

  // Pivoted format config
  pivotConfig?: {
    enabled: boolean;
    styleColumn: string;
    colorColumn?: string;
    sizeColumns: string[];
    priceColumn?: string;
  };

  // Complex stock cell parsing
  complexStockConfig?: {
    enabled: boolean;
    stockColumn: string;
    patterns: ComplexStockPattern[];
  };

  // Column-based sale detection
  columnSaleConfig?: ColumnBasedSaleConfig;

  // Price-based size expansion
  priceExpansionConfig?: {
    enabled: boolean;
    tiers: PriceBasedExpansionTier[];
    sizeOrder?: string[];
  };

  // Discontinued detection
  discontinuedConfig?: {
    enabled: boolean;
    mode: "keyword" | "column_value" | "stock_text";
    keywords?: string[];
    column?: string;
  };

  // Future stock config
  futureStockConfig?: {
    enabled: boolean;
    mode: "column_has_date" | "embedded_in_stock";
    dateColumn?: string;
    useDateAsShipDate: boolean;
  };

  // Simple stock text mappings
  stockValueConfig?: {
    textMappings: Array<{ text: string; value: number }>;
  };

  // Grouped pivot config (from universal parser AI analysis)
  groupedPivotConfig?: GroupedPivotConfig;

  // Style normalization
  styleNormalization?: {
    removePrefixes?: string[];
    removeSuffixes?: string[];
    removeLeadingZeros?: boolean;
  };

  // From data source
  cleaningConfig?: any;
  sheetConfig?: any;
  fileParseConfig?: any;
}

export interface ParsedInventoryItem {
  style: string;
  color: string;
  size: string;
  stock: number;
  price?: number;
  salePrice?: number;
  cost?: number;
  sku?: string;
  shipDate?: string;
  discontinued?: boolean;
  specialOrder?: boolean;
  isSaleItem?: boolean;
  priceSource?: "regular" | "sale" | "shopify";
  compareAtPrice?: number;
  rawStockValue?: string;
  isExpandedSize?: boolean; // True if this size was created by expansion
  expandedFrom?: string; // Original size this was expanded from
}

export interface ParseResult {
  success: boolean;
  items: ParsedInventoryItem[];
  stats: {
    totalRows: number;
    validItems: number;
    skippedRows: number;
    discontinuedItems: number;
    futureStockItems: number;
    saleItems: number;
    complexStockParsed: number;
  };
  warnings: string[];
}

// ============================================================
// CRITICAL FIX: CSV DETECTION AND PARSING
// Prevents scientific notation conversion (e.g., "1921E0136" -> 1.921e+139)
// ============================================================

function isCSVBuffer(buffer: Buffer): boolean {
  // Check for UTF-16 BOM (Terani files use this)
  if (buffer[0] === 0xff && buffer[1] === 0xfe) {
    return true; // UTF-16 LE CSV
  }
  if (buffer[0] === 0xfe && buffer[1] === 0xff) {
    return true; // UTF-16 BE CSV
  }
  // Check for common Excel signatures (ZIP format for xlsx)
  if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
    return false; // This is an xlsx file (ZIP format)
  }
  // Check for old Excel format (xls)
  if (buffer[0] === 0xd0 && buffer[1] === 0xcf) {
    return false; // This is an xls file
  }
  // Check first 1000 bytes for CSV-like content
  const sample = buffer
    .slice(0, Math.min(1000, buffer.length))
    .toString("utf8");
  const hasCommas = (sample.match(/,/g) || []).length > 2;
  const hasNewlines = (sample.match(/[\r\n]/g) || []).length > 0;
  return hasCommas && hasNewlines;
}

function parseCSVAsText(buffer: Buffer): any[][] {
  let content: string;

  // Handle UTF-16 encoding (used by Terani)
  if (buffer[0] === 0xff && buffer[1] === 0xfe) {
    content = buffer.toString("utf16le").replace(/^\ufeff/, "");
  } else if (buffer[0] === 0xfe && buffer[1] === 0xff) {
    content = buffer
      .swap16()
      .toString("utf16le")
      .replace(/^\ufeff/, "");
  } else {
    content = buffer.toString("utf8").replace(/^\ufeff/, "");
  }

  const lines = content.split(/\r?\n/);
  const rows: any[][] = [];

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
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === "," && !inQuotes) {
        row.push(current.trim());
        current = "";
      } else {
        current += char;
      }
    }
    row.push(current.trim());
    rows.push(row);
  }

  return rows;
}

// ============================================================
// SIZE ORDER FOR EXPANSION
// ============================================================

const DEFAULT_SIZE_ORDER = [
  "00",
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
  "XXS",
  "XS",
  "S",
  "M",
  "L",
  "XL",
  "XXL",
  "XXXL",
  "2XL",
  "3XL",
  "4XL",
  "5XL",
  "0P",
  "2P",
  "4P",
  "6P",
  "8P",
  "10P",
  "12P",
  "14P",
  "16P",
  "0R",
  "2R",
  "4R",
  "6R",
  "8R",
  "10R",
  "12R",
  "14R",
  "16R",
];

// ============================================================
// COMPLEX STOCK PARSER
// ============================================================

interface ComplexStockResult {
  stock: number;
  shipDate?: string;
  discontinued?: boolean;
  specialOrder?: boolean;
  matched: boolean;
  patternName?: string;
}

function parseComplexStockCell(
  value: string,
  patterns: ComplexStockPattern[],
): ComplexStockResult {
  const result: ComplexStockResult = {
    stock: 0,
    matched: false,
  };

  if (!value || typeof value !== "string") {
    return result;
  }

  const normalizedValue = value.trim().toLowerCase();

  for (const pattern of patterns) {
    try {
      const regex = new RegExp(pattern.pattern, "i");
      const match = normalizedValue.match(regex);

      if (match) {
        result.matched = true;
        result.patternName = pattern.name;

        // Extract stock
        if (pattern.extractStock) {
          if (pattern.extractStock.startsWith("$")) {
            const groupNum = parseInt(pattern.extractStock.substring(1));
            result.stock = parseInt(match[groupNum] || "0") || 0;
          } else {
            result.stock = parseInt(pattern.extractStock) || 0;
          }
        }

        // Extract date
        if (pattern.extractDate && pattern.extractDate.startsWith("$")) {
          const groupNum = parseInt(pattern.extractDate.substring(1));
          if (match[groupNum]) {
            result.shipDate = match[groupNum];
          }
        }

        // Mark discontinued
        if (pattern.markDiscontinued) {
          result.discontinued = true;
        }

        // Mark special order
        if (pattern.markSpecialOrder) {
          result.specialOrder = true;
        }

        break; // Use first matching pattern
      }
    } catch (e) {
      // Invalid regex, skip
      continue;
    }
  }

  return result;
}

// ============================================================
// SIMPLE STOCK TEXT MAPPING
// ============================================================

function mapStockText(
  value: string,
  mappings: Array<{ text: string; value: number }>,
): number | null {
  if (!value || typeof value !== "string") {
    return null;
  }

  const normalized = value.trim().toUpperCase();

  for (const mapping of mappings) {
    if (normalized === mapping.text.toUpperCase()) {
      return mapping.value;
    }
  }

  // Try to parse as number
  const num = parseFloat(value);
  if (!isNaN(num)) {
    return Math.floor(num);
  }

  return null;
}

// ============================================================
// PRICE-BASED SIZE EXPANSION
// ============================================================

function expandSizes(
  item: ParsedInventoryItem,
  tiers: PriceBasedExpansionTier[],
  sizeOrder: string[],
): ParsedInventoryItem[] {
  const price = item.price || 0;

  // Find matching tier
  let tier: PriceBasedExpansionTier | null = null;
  for (const t of tiers) {
    const minOk = price >= t.minPrice;
    const maxOk = t.maxPrice === undefined || price <= t.maxPrice;
    if (minOk && maxOk) {
      tier = t;
      break;
    }
  }

  if (!tier || (tier.expandDown === 0 && tier.expandUp === 0)) {
    return [item];
  }

  const currentSizeUpper = item.size.toUpperCase();
  const currentIndex = sizeOrder.findIndex(
    (s) => s.toUpperCase() === currentSizeUpper,
  );

  if (currentIndex === -1) {
    // Size not in order list, can't expand
    return [item];
  }

  const result: ParsedInventoryItem[] = [];

  // Expand down
  for (let i = tier.expandDown; i > 0; i--) {
    const newIndex = currentIndex - i;
    if (newIndex >= 0) {
      result.push({
        ...item,
        size: sizeOrder[newIndex],
        stock: 0, // Expanded sizes start with 0 stock
        isExpandedSize: true, // Mark as expanded for UI highlighting
        expandedFrom: item.size, // Track original size
      });
    }
  }

  // Original size (not expanded)
  result.push({ ...item, isExpandedSize: false });

  // Expand up
  for (let i = 1; i <= tier.expandUp; i++) {
    const newIndex = currentIndex + i;
    if (newIndex < sizeOrder.length) {
      result.push({
        ...item,
        size: sizeOrder[newIndex],
        stock: 0, // Expanded sizes start with 0 stock
        isExpandedSize: true, // Mark as expanded for UI highlighting
        expandedFrom: item.size, // Track original size
      });
    }
  }

  return result;
}

// ============================================================
// DATE PARSING
// ============================================================

function parseDate(value: any): string | undefined {
  if (!value) return undefined;

  // Already a Date object
  if (value instanceof Date) {
    return value.toISOString().split("T")[0];
  }

  const str = String(value).trim();
  if (!str) return undefined;

  // Try various formats
  const patterns = [
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/, // MM/DD/YYYY or M/D/YYYY
    /^(\d{1,2})\/(\d{1,2})\/(\d{2})$/, // MM/DD/YY
    /^(\d{4})-(\d{2})-(\d{2})$/, // YYYY-MM-DD
  ];

  for (const pattern of patterns) {
    const match = str.match(pattern);
    if (match) {
      let year = match[3] || match[1];
      let month = match[1] || match[2];
      let day = match[2] || match[3];

      // Handle 2-digit year
      if (year.length === 2) {
        year = "20" + year;
      }

      // Ensure proper format
      month = month.padStart(2, "0");
      day = day.padStart(2, "0");

      return `${year}-${month}-${day}`;
    }
  }

  return undefined;
}

// ============================================================
// MAIN PARSE FUNCTION
// ============================================================

export async function parseWithEnhancedConfig(
  buffer: Buffer,
  config: EnhancedImportConfig,
  dataSourceId?: string,
): Promise<ParseResult> {
  const result: ParseResult = {
    success: false,
    items: [],
    stats: {
      totalRows: 0,
      validItems: 0,
      skippedRows: 0,
      discontinuedItems: 0,
      futureStockItems: 0,
      saleItems: 0,
      complexStockParsed: 0,
    },
    warnings: [],
  };

  try {
    // CRITICAL FIX: For CSV files, parse as text to prevent scientific notation
    let rawData: any[][];
    if (isCSVBuffer(buffer)) {
      console.log(
        "[EnhancedImportProcessor] Detected CSV - using text parser to prevent scientific notation",
      );
      rawData = parseCSVAsText(buffer);
    } else {
      // Parse Excel
      const workbook = XLSX.read(buffer, { type: "buffer", cellDates: true });
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      rawData = XLSX.utils.sheet_to_json(sheet, {
        header: 1,
        defval: "",
      }) as any[][];
    }

    if (rawData.length < 2) {
      result.warnings.push("File has no data rows");
      return result;
    }

    const headers = rawData[0].map((h: any) => String(h || "").trim());
    const rows = rawData.slice(1);
    result.stats.totalRows = rows.length;

    console.log(`[EnhancedParser] Headers found: ${headers.join(", ")}`);
    console.log(
      `[EnhancedParser] Total rows (excluding header): ${rows.length}`,
    );

    // Build header index map (case-insensitive for robust matching)
    const headerIndex: Record<string, number> = {};
    const headerIndexLower: Record<string, number> = {};
    headers.forEach((h, i) => {
      headerIndex[h] = i;
      headerIndexLower[h.toLowerCase()] = i;
    });

    // Helper to find column index (case-insensitive)
    const findColumnIndex = (
      columnName: string | undefined,
    ): number | undefined => {
      if (!columnName) return undefined;
      // Try exact match first
      if (headerIndex[columnName] !== undefined) return headerIndex[columnName];
      // Try case-insensitive match
      if (headerIndexLower[columnName.toLowerCase()] !== undefined)
        return headerIndexLower[columnName.toLowerCase()];
      return undefined;
    };

    // Get column indices from mapping
    const mapping = config.columnMapping || {};
    console.log(`[EnhancedParser] Column mapping keys: ${Object.keys(mapping).join(", ")}`);

    const styleIdx = findColumnIndex(mapping.style);
    const colorIdx = findColumnIndex(mapping.color);
    const sizeIdx = findColumnIndex(mapping.size);
    const stockIdx = findColumnIndex(mapping.stock);
    const priceIdx = findColumnIndex(mapping.price);
    const salePriceIdx = findColumnIndex(mapping.salePrice);
    const costIdx = findColumnIndex(mapping.cost);
    const skuIdx = findColumnIndex(mapping.sku);
    const shipDateIdx = findColumnIndex(mapping.shipDate);
    const statusIdx = findColumnIndex(mapping.status);

    // Log warnings for columns that couldn't be found
    if (mapping.style && styleIdx === undefined)
      console.warn(
        `[EnhancedParser] WARNING: Style column "${mapping.style}" not found in headers`,
      );
    if (mapping.color && colorIdx === undefined)
      console.warn(
        `[EnhancedParser] WARNING: Color column "${mapping.color}" not found in headers`,
      );
    if (mapping.size && sizeIdx === undefined)
      console.warn(
        `[EnhancedParser] WARNING: Size column "${mapping.size}" not found in headers`,
      );
    if (mapping.stock && stockIdx === undefined)
      console.warn(
        `[EnhancedParser] WARNING: Stock column "${mapping.stock}" not found in headers`,
      );
    if (mapping.price && priceIdx === undefined)
      console.warn(
        `[EnhancedParser] WARNING: Price column "${mapping.price}" not found in headers`,
      );

    console.log(
      `[EnhancedParser] Column indices - style:${styleIdx}, color:${colorIdx}, size:${sizeIdx}, stock:${stockIdx}, price:${priceIdx}`,
    );

    // Check if this is any pivot format type
    const isPivotFormat =
      config.formatType?.startsWith("pivot") || config.formatType === "pivoted";

    // Process based on format type
    if (config.formatType === "pivot_grouped" && config.groupedPivotConfig?.enabled) {
      // Grouped pivot format — style as section header, color rows below
      console.log(`[EnhancedParser] Using grouped pivot parser`);
      const groupedResult = parseGroupedPivotData(rawData, config.groupedPivotConfig);
      result.items = groupedResult.items.map((item: any) => ({
        style: String(item.style || ""),
        color: String(item.color || ""),
        size: String(item.size || ""),
        stock: item.stock || 0,
        price: item.price,
      }));
      result.stats.totalRows = rawData.length;
      console.log(`[EnhancedParser] Grouped pivot extracted ${result.items.length} items`);
    } else if (isPivotFormat && config.pivotConfig?.enabled) {
      // Pivoted format parsing
      const pivotCfg = config.pivotConfig;
      const pStyleIdx = headerIndex[pivotCfg.styleColumn];
      const pColorIdx = pivotCfg.colorColumn
        ? headerIndex[pivotCfg.colorColumn]
        : undefined;
      const pPriceIdx = pivotCfg.priceColumn
        ? headerIndex[pivotCfg.priceColumn]
        : undefined;
      const sizeColumnIndices = pivotCfg.sizeColumns
        .map((s) => ({ size: s, idx: headerIndex[s] }))
        .filter((x) => x.idx !== undefined);

      for (const row of rows) {
        const style = String(row[pStyleIdx] || "").trim();
        if (!style) {
          result.stats.skippedRows++;
          continue;
        }

        const color =
          pColorIdx !== undefined ? String(row[pColorIdx] || "").trim() : "";
        const price =
          pPriceIdx !== undefined
            ? parseFloat(row[pPriceIdx]) || undefined
            : undefined;

        for (const sizeCol of sizeColumnIndices) {
          const stockVal = row[sizeCol.idx];
          let stock = 0;

          // Parse stock value
          if (typeof stockVal === "number") {
            stock = Math.floor(stockVal);
          } else if (typeof stockVal === "string") {
            const parsed = parseFloat(stockVal);
            if (!isNaN(parsed)) {
              stock = Math.floor(parsed);
            } else if (config.stockValueConfig?.textMappings) {
              const mapped = mapStockText(
                stockVal,
                config.stockValueConfig.textMappings,
              );
              if (mapped !== null) stock = mapped;
            }
          }

          // Add all items - don't filter zero stock here, let variant rules handle that
          const item: ParsedInventoryItem = {
            style,
            color,
            size: sizeCol.size,
            stock,
            price,
          };
          result.items.push(item);
        }
      }
    } else {
      // Row-based format parsing
      let firstFewRows = 0;
      for (const row of rows) {
        const style =
          styleIdx !== undefined ? String(row[styleIdx] || "").trim() : "";
        if (!style) {
          result.stats.skippedRows++;
          // Log first few skipped rows to help diagnose
          if (firstFewRows < 3) {
            console.log(
              `[EnhancedParser] Skipped row (no style) - raw data: ${JSON.stringify(row.slice(0, 6))}...`,
            );
            firstFewRows++;
          }
          continue;
        }

        const color =
          colorIdx !== undefined ? String(row[colorIdx] ?? "").trim() : "";
        // Use ?? instead of || to preserve size "0" (0 is falsy but valid size)
        const size =
          sizeIdx !== undefined ? String(row[sizeIdx] ?? "").trim() : "";

        // Parse stock - handle complex cells
        let stock = 0;
        let shipDate: string | undefined;
        let discontinued = false;
        let specialOrder = false;
        let rawStockValue: string | undefined;

        if (stockIdx !== undefined) {
          const stockVal = row[stockIdx];
          rawStockValue = String(stockVal || "");

          // Try complex stock parsing first
          if (
            config.complexStockConfig?.enabled &&
            config.complexStockConfig.patterns.length > 0
          ) {
            const complexResult = parseComplexStockCell(
              rawStockValue,
              config.complexStockConfig.patterns,
            );
            if (complexResult.matched) {
              stock = complexResult.stock;
              shipDate = complexResult.shipDate;
              discontinued = complexResult.discontinued || false;
              specialOrder = complexResult.specialOrder || false;
              result.stats.complexStockParsed++;
            }
          }

          // If not matched by complex patterns, try simple parsing
          if (
            !config.complexStockConfig?.enabled ||
            result.stats.complexStockParsed === 0
          ) {
            if (typeof stockVal === "number") {
              stock = Math.floor(stockVal);
            } else if (typeof stockVal === "string") {
              // Try text mappings
              if (config.stockValueConfig?.textMappings) {
                const mapped = mapStockText(
                  stockVal,
                  config.stockValueConfig.textMappings,
                );
                if (mapped !== null) {
                  stock = mapped;
                } else {
                  const parsed = parseFloat(stockVal);
                  stock = !isNaN(parsed) ? Math.floor(parsed) : 0;
                }
              } else {
                const parsed = parseFloat(stockVal);
                stock = !isNaN(parsed) ? Math.floor(parsed) : 0;
              }
            }
          }
        }

        // Parse prices
        const regularPrice =
          priceIdx !== undefined
            ? parseFloat(row[priceIdx]) || undefined
            : undefined;
        const salePrice =
          salePriceIdx !== undefined
            ? parseFloat(row[salePriceIdx]) || undefined
            : undefined;
        const cost =
          costIdx !== undefined
            ? parseFloat(row[costIdx]) || undefined
            : undefined;

        // Parse ship date from dedicated column if not from complex stock
        if (!shipDate && shipDateIdx !== undefined) {
          shipDate = parseDate(row[shipDateIdx]);
        }

        // Check discontinued from status column
        if (
          !discontinued &&
          statusIdx !== undefined &&
          config.discontinuedConfig?.enabled
        ) {
          const statusVal = String(row[statusIdx] || "")
            .trim()
            .toUpperCase();
          if (config.discontinuedConfig.keywords) {
            discontinued = config.discontinuedConfig.keywords.some(
              (k) => statusVal === k.toUpperCase(),
            );
          }
        }

        // Determine final price and sale status
        let finalPrice = regularPrice;
        let isSaleItem = false;
        let compareAtPrice: number | undefined;
        let priceSource: ParsedInventoryItem["priceSource"] = "regular";

        // Column-based sale detection
        if (
          config.columnSaleConfig?.enabled &&
          salePrice !== undefined &&
          salePrice > 0
        ) {
          if (
            !config.columnSaleConfig.onlyWhenSalePricePresent ||
            salePrice > 0
          ) {
            isSaleItem = true;
            priceSource = "sale";
            // Apply multiplier to sale price
            finalPrice = salePrice * config.columnSaleConfig.multiplier;
            // Compare-at will be set from Shopify later if useShopifyAsCompareAt is true
            // For now, use regular price as fallback compare-at
            if (regularPrice && regularPrice > finalPrice) {
              compareAtPrice = regularPrice;
            }
          }
        }

        // Auto-generate SKU from style-color-size if not mapped (matching old system behavior)
        const rawSku =
          skuIdx !== undefined ? String(row[skuIdx] || "").trim() : "";
        const generatedSku =
          rawSku ||
          `${style}-${color}-${size}`
            .replace(/^-+|-+$/g, "")
            .replace(/--+/g, "-");

        const item: ParsedInventoryItem = {
          style,
          color,
          size,
          stock,
          price: finalPrice,
          salePrice,
          cost,
          sku: generatedSku || style, // Fallback to style if still empty
          shipDate,
          discontinued,
          specialOrder,
          isSaleItem,
          priceSource,
          compareAtPrice,
          rawStockValue,
        };

        // Track stats
        if (discontinued) result.stats.discontinuedItems++;
        if (shipDate) result.stats.futureStockItems++;
        if (isSaleItem) result.stats.saleItems++;

        result.items.push(item);

        // Log first few items to verify parsing
        if (result.items.length <= 3) {
          console.log(
            `[EnhancedParser] Sample item ${result.items.length}: style="${item.style}", color="${item.color}", size="${item.size}", stock=${item.stock}, price=${item.price}`,
          );
        }
      }
    }

    // Apply style normalization
    if (config.styleNormalization) {
      const norm = config.styleNormalization;
      result.items = result.items.map((item) => {
        let style = item.style.toUpperCase();

        if (norm.removePrefixes) {
          for (const prefix of norm.removePrefixes) {
            if (style.startsWith(prefix.toUpperCase())) {
              style = style.slice(prefix.length);
            }
          }
        }

        if (norm.removeSuffixes) {
          for (const suffix of norm.removeSuffixes) {
            if (style.endsWith(suffix.toUpperCase())) {
              style = style.slice(0, -suffix.length);
            }
          }
        }

        if (norm.removeLeadingZeros) {
          style = style.replace(/^0+/, "") || "0";
        }

        return { ...item, style };
      });
    }

    // Apply price-based size expansion
    if (
      config.priceExpansionConfig?.enabled &&
      config.priceExpansionConfig.tiers.length > 0
    ) {
      console.log(
        `[EnhancedParser] Applying price-based size expansion with ${config.priceExpansionConfig.tiers.length} tier(s)`,
      );
      const sizeOrder =
        config.priceExpansionConfig.sizeOrder || DEFAULT_SIZE_ORDER;
      const expandedItems: ParsedInventoryItem[] = [];

      for (const item of result.items) {
        const expanded = expandSizes(
          item,
          config.priceExpansionConfig.tiers,
          sizeOrder,
        );
        expandedItems.push(...expanded);
      }

      // Track how many were added
      const addedCount = expandedItems.length - result.items.length;
      const expandedCount = expandedItems.filter(
        (i) => i.isExpandedSize,
      ).length;

      if (addedCount > 0) {
        console.log(
          `[EnhancedParser] Price expansion: ${result.items.length} items → ${expandedItems.length} items (+${addedCount} expanded sizes)`,
        );
        result.warnings.push(
          `Price-based expansion added ${addedCount} size variants`,
        );
      } else {
        console.log(
          `[EnhancedParser] Price expansion: No sizes expanded (items may not match tier criteria or size order)`,
        );
      }

      result.items = expandedItems;
      (result.stats as any).expandedSizes = expandedCount;
    }

    result.stats.validItems = result.items.length;
    result.success = true;

    console.log(`[EnhancedParser] Parsing complete:`);
    console.log(`[EnhancedParser]   - Total rows: ${result.stats.totalRows}`);
    console.log(`[EnhancedParser]   - Valid items: ${result.stats.validItems}`);
    console.log(
      `[EnhancedParser]   - Skipped rows: ${result.stats.skippedRows}`,
    );
    console.log(`[EnhancedParser]   - Sale items: ${result.stats.saleItems}`);
    console.log(
      `[EnhancedParser]   - Discontinued: ${result.stats.discontinuedItems}`,
    );
  } catch (error: any) {
    console.error(`[EnhancedParser] Parse error:`, error);
    result.warnings.push(`Parse error: ${error.message}`);
  }

  return result;
}

// ============================================================
// SHOPIFY PRICE LOOKUP (for compare-at)
// ============================================================

export interface ShopifyPriceLookup {
  style: string;
  price: number;
}

export function applyShopifyPricesForCompareAt(
  items: ParsedInventoryItem[],
  shopifyPrices: Map<string, number>,
  columnSaleConfig?: ColumnBasedSaleConfig,
): { items: ParsedInventoryItem[]; updatedCount: number } {
  let updatedCount = 0;

  if (!columnSaleConfig?.useShopifyAsCompareAt) {
    return { items, updatedCount };
  }

  const updatedItems = items.map((item) => {
    if (item.isSaleItem) {
      // Normalize style for lookup
      const styleKey = item.style.toUpperCase().trim();
      const shopifyPrice = shopifyPrices.get(styleKey);

      if (shopifyPrice && shopifyPrice > (item.price || 0)) {
        updatedCount++;
        return {
          ...item,
          compareAtPrice: shopifyPrice,
        };
      }
    }
    return item;
  });

  return { items: updatedItems, updatedCount };
}

// ============================================================
// EXPORTS
// ============================================================

export {
  parseComplexStockCell,
  mapStockText,
  expandSizes,
  parseDate,
  DEFAULT_SIZE_ORDER,
};
