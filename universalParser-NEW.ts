/**
 * universalParser.ts — AI-powered format detection for any file layout
 *
 * Replaces the need for hardcoded parsers. Sends the first 20 rows of a file
 * to the AI model, which returns a structured extraction config. A deterministic
 * extractor then uses that config on ALL rows.
 *
 * Handles:
 *   - Row format (each row = one item)
 *   - Pivot format (sizes as column headers, one row per style+color)
 *   - Grouped pivot (style as section header, color rows below — e.g., Feriani)
 *
 * The returned config is compatible with EnhancedImportConfig so that
 * parseWithEnhancedConfig can process the data without new hardcoded parsers.
 */

import { openai } from "./openai";

// ============================================================
// TYPES
// ============================================================

export interface GroupedPivotConfig {
  enabled: boolean;
  /** How to detect a style header row */
  styleDetectionMethod: "single_cell" | "pattern" | "column_count";
  /** Regex pattern to match style header rows (for "pattern" method) */
  stylePattern?: string;
  /** Column index that holds the style value in header rows */
  styleColumn: number;
  /** Column index that holds color in data rows */
  colorColumn: number;
  /** Column index where size columns start */
  sizeStartColumn: number;
  /** Size labels for each column (e.g., ["00","0","2","4","6","8"]) */
  sizeLabels: string[];
  /** Optional price column index */
  priceColumn?: number;
  /** Row index where data begins (after file title/header rows) */
  dataStartRow: number;
  /** Patterns for rows to skip (totals, subtotals, empty sections) */
  skipPatterns?: string[];
}

export interface UniversalAnalysisResult {
  formatType: "row" | "pivot" | "pivot_grouped";
  confidence: number;
  /** 0-based row index of the header row */
  headerRowIndex: number;
  /** 0-based row index where data starts */
  dataStartRow: number;
  /** Column name → field mapping (for row and simple pivot) */
  columnMapping: Record<string, string>;
  /** Pivot config (for simple pivot) */
  pivotConfig?: {
    enabled: boolean;
    styleColumn: string;
    colorColumn?: string;
    sizeColumns: string[];
    priceColumn?: string;
  };
  /** Grouped pivot config */
  groupedPivotConfig?: GroupedPivotConfig;
  notes: string[];
}

// ============================================================
// AI ANALYSIS PROMPT
// ============================================================

const ANALYSIS_SYSTEM_PROMPT = `You are an expert at analyzing spreadsheet data layouts for inventory management systems.

Your task: Given the first rows of an Excel/CSV file, determine its format and return a JSON extraction config.

There are exactly 3 format types:

## FORMAT 1: "row"
Each row is one inventory item with columns for style, color, size, stock, etc.
Example:
  Row 0: Style | Color | Size | Stock | Price
  Row 1: 12345 | Blue  | 6    | 3     | 299
  Row 2: 12345 | Blue  | 8    | 1     | 299
  Row 3: 12345 | Red   | 6    | 0     | 299

## FORMAT 2: "pivot"
Sizes are column headers. Each row has style+color, and stock values in a grid.
Example:
  Row 0: Style | Color | 00 | 0 | 2 | 4 | 6 | 8 | 10 | 12 | Price
  Row 1: 12345 | Blue  | 0  | 1 | 3 | 2 | 1 | 0 | 0  | 0  | 299
  Row 2: 12345 | Red   | 2  | 0 | 1 | 1 | 0 | 0 | 0  | 0  | 299

## FORMAT 3: "pivot_grouped"
Style is a section header row (often a single value in the row). Below it are color rows with stock per size.
Example:
  Row 0: (empty or title row)
  Row 1: (empty or title row)
  Row 2: (header) | 00 | 0 | 2 | 4 | 6 | 8 | Price
  Row 3: STYLE 12345
  Row 4: Blue    | 0  | 1 | 3 | 2 | 1 | 0  | 299
  Row 5: Red     | 2  | 0 | 1 | 1 | 0 | 0  | 299
  Row 6: STYLE 12346
  Row 7: Black   | 1  | 2 | 0 | 3 | 0 | 1  | 350

Key indicators of grouped pivot:
- Some rows have a single non-empty cell (the style number)
- Below those, rows have color name + numeric stock values across columns
- The pattern repeats: style header, then 1+ color rows, then next style header

## IMPORTANT RULES
1. Look for title rows at the top (company name, report title, date) — these are NOT data. Set headerRowIndex and dataStartRow AFTER these.
2. Sizes in pivot/grouped formats are usually: 00, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24 OR XS, S, M, L, XL, XXL OR similar patterns.
3. If you see numeric column headers that look like dress sizes, it's likely a pivot format.
4. Return ONLY valid JSON. No explanation text.

## RESPONSE FORMAT

For "row" format:
{
  "formatType": "row",
  "confidence": 90,
  "headerRowIndex": 0,
  "dataStartRow": 1,
  "columnMapping": {
    "style": "Style Number",
    "color": "Color",
    "size": "Size",
    "stock": "Available Qty",
    "price": "Wholesale Price",
    "cost": "Cost",
    "shipDate": "Ship Date",
    "status": "Status"
  },
  "notes": ["Standard row format with clear headers"]
}

For "pivot" format:
{
  "formatType": "pivot",
  "confidence": 85,
  "headerRowIndex": 0,
  "dataStartRow": 1,
  "columnMapping": {},
  "pivotConfig": {
    "enabled": true,
    "styleColumn": "Style",
    "colorColumn": "Color",
    "sizeColumns": ["00", "0", "2", "4", "6", "8", "10", "12"],
    "priceColumn": "Price"
  },
  "notes": ["Pivot format with sizes as column headers"]
}

For "pivot_grouped" format:
{
  "formatType": "pivot_grouped",
  "confidence": 80,
  "headerRowIndex": 2,
  "dataStartRow": 3,
  "columnMapping": {},
  "groupedPivotConfig": {
    "enabled": true,
    "styleDetectionMethod": "single_cell",
    "styleColumn": 0,
    "colorColumn": 0,
    "sizeStartColumn": 1,
    "sizeLabels": ["00", "0", "2", "4", "6", "8", "10", "12"],
    "priceColumn": 9,
    "dataStartRow": 3,
    "skipPatterns": ["total", "subtotal"]
  },
  "notes": ["Grouped pivot - style as section header, color rows below"]
}

Only include fields that are actually present in the data. Omit optional fields if not applicable.`;

// ============================================================
// AI ANALYSIS FUNCTION
// ============================================================

/**
 * Sends the first rows of a file to the AI model for format detection.
 * Returns a structured config that can be used for extraction.
 */
export async function analyzeFileWithAI(
  rawData: any[][],
  filename?: string,
): Promise<UniversalAnalysisResult | null> {
  try {
    // Take first 25 rows for analysis (enough to see patterns)
    const sampleRows = rawData.slice(0, 25);

    // Format as a readable table for the AI
    const table = sampleRows
      .map((row, i) => {
        const cells = row.map((cell: any) => {
          if (cell === null || cell === undefined || cell === "") return "(empty)";
          return String(cell).substring(0, 50); // Truncate long values
        });
        return `Row ${i}: ${cells.join(" | ")}`;
      })
      .join("\n");

    const userMessage = `Filename: ${filename || "unknown"}\nTotal rows in file: ${rawData.length}\n\nFirst ${sampleRows.length} rows:\n${table}`;

    console.log(`[UniversalParser] Sending ${sampleRows.length} rows to AI for analysis...`);

    const response = await openai.chat.completions.create({
      model: "gpt-4o",
      messages: [
        { role: "system", content: ANALYSIS_SYSTEM_PROMPT },
        { role: "user", content: userMessage },
      ],
      response_format: { type: "json_object" },
      temperature: 0,
      max_tokens: 1500,
    });

    const content = response.choices?.[0]?.message?.content;
    if (!content) {
      console.error("[UniversalParser] AI returned empty response");
      return null;
    }

    const parsed = JSON.parse(content) as UniversalAnalysisResult;
    console.log(
      `[UniversalParser] AI detected format: ${parsed.formatType} (confidence: ${parsed.confidence}%)`,
    );
    if (parsed.notes?.length > 0) {
      console.log(`[UniversalParser] Notes: ${parsed.notes.join(", ")}`);
    }

    // Validate the response has required fields
    if (!parsed.formatType || !["row", "pivot", "pivot_grouped"].includes(parsed.formatType)) {
      console.error(`[UniversalParser] Invalid formatType: ${parsed.formatType}`);
      return null;
    }

    return parsed;
  } catch (error: any) {
    console.error(`[UniversalParser] AI analysis failed: ${error.message}`);
    return null;
  }
}

// ============================================================
// GROUPED PIVOT EXTRACTOR
// ============================================================

/**
 * Deterministic extractor for grouped pivot formats.
 * Style is a section header row, followed by color rows with stock per size.
 */
export function parseGroupedPivotData(
  rawData: any[][],
  config: GroupedPivotConfig,
): { items: any[]; rows: any[][]; headers: string[] } {
  const items: any[] = [];
  let currentStyle = "";
  const gc = config;

  // Extract headers from the header row (the row before dataStartRow)
  const headerRow = gc.dataStartRow > 0 ? rawData[gc.dataStartRow - 1] : [];
  const headers = (headerRow || []).map((h: any) => String(h || "").trim());

  console.log(`[GroupedPivot] Starting extraction from row ${gc.dataStartRow}`);
  console.log(`[GroupedPivot] Size labels: ${gc.sizeLabels.join(", ")}`);
  console.log(`[GroupedPivot] Style detection: ${gc.styleDetectionMethod}`);

  for (let i = gc.dataStartRow; i < rawData.length; i++) {
    const row = rawData[i];
    if (!row) continue;

    // Skip completely empty rows
    const nonEmptyCells = row.filter(
      (cell: any) => cell !== null && cell !== undefined && String(cell).trim() !== "",
    );
    if (nonEmptyCells.length === 0) continue;

    // Check skip patterns (totals, subtotals)
    if (gc.skipPatterns && gc.skipPatterns.length > 0) {
      const firstCell = String(row[gc.styleColumn] || "").trim().toLowerCase();
      if (gc.skipPatterns.some((p) => firstCell.includes(p.toLowerCase()))) {
        continue;
      }
    }

    // Determine if this is a style header row or a data (color) row
    if (isStyleHeaderRow(row, gc)) {
      currentStyle = String(row[gc.styleColumn] || "").trim();
      // Clean up style: remove common prefixes like "STYLE", "STYLE#", etc.
      currentStyle = currentStyle
        .replace(/^style\s*#?\s*/i, "")
        .replace(/^item\s*#?\s*/i, "")
        .trim();
      continue;
    }

    // This is a data (color) row
    if (!currentStyle) continue;

    const color = String(row[gc.colorColumn] || "").trim();
    if (!color) continue;

    const price =
      gc.priceColumn !== undefined
        ? parseFloat(String(row[gc.priceColumn] || "0")) || undefined
        : undefined;

    // Extract stock for each size column
    for (let j = 0; j < gc.sizeLabels.length; j++) {
      const colIdx = gc.sizeStartColumn + j;
      if (colIdx >= row.length) continue;

      const stockVal = row[colIdx];
      let stock = 0;

      if (typeof stockVal === "number") {
        stock = Math.floor(stockVal);
      } else if (typeof stockVal === "string") {
        const parsed = parseFloat(stockVal);
        if (!isNaN(parsed)) {
          stock = Math.floor(parsed);
        }
      }

      items.push({
        style: currentStyle,
        color,
        size: gc.sizeLabels[j],
        stock,
        price,
      });
    }
  }

  console.log(`[GroupedPivot] Extracted ${items.length} items from ${rawData.length - gc.dataStartRow} data rows`);

  return {
    items,
    rows: rawData.slice(gc.dataStartRow),
    headers,
  };
}

/**
 * Determines if a row is a style header (section break) or a data row.
 */
function isStyleHeaderRow(row: any[], gc: GroupedPivotConfig): boolean {
  const firstCell = String(row[gc.styleColumn] || "").trim();
  if (!firstCell) return false;

  switch (gc.styleDetectionMethod) {
    case "single_cell": {
      // Style rows typically have a value in the first column and empty/zero in size columns
      let emptySizeCols = 0;
      for (let j = 0; j < gc.sizeLabels.length; j++) {
        const colIdx = gc.sizeStartColumn + j;
        if (colIdx >= row.length) { emptySizeCols++; continue; }
        const val = row[colIdx];
        if (val === null || val === undefined || String(val).trim() === "" || val === 0) {
          emptySizeCols++;
        }
      }
      // If most size columns are empty, this is likely a style header
      return emptySizeCols >= gc.sizeLabels.length * 0.8;
    }

    case "pattern": {
      if (!gc.stylePattern) return false;
      try {
        return new RegExp(gc.stylePattern, "i").test(firstCell);
      } catch {
        return false;
      }
    }

    case "column_count": {
      // Count non-empty cells — style rows usually have only 1-2 non-empty cells
      const nonEmpty = row.filter(
        (cell: any) => cell !== null && cell !== undefined && String(cell).trim() !== "" && cell !== 0,
      );
      return nonEmpty.length <= 2;
    }

    default:
      return false;
  }
}

// ============================================================
// CONVENIENCE: Convert UniversalAnalysisResult to EnhancedImportConfig
// ============================================================

/**
 * Converts the AI analysis result into an EnhancedImportConfig
 * that parseWithEnhancedConfig can consume directly.
 */
export function toEnhancedConfig(analysis: UniversalAnalysisResult): any {
  const config: any = {
    formatType: analysis.formatType === "pivot" ? "pivoted" : analysis.formatType,
    columnMapping: analysis.columnMapping || {},
  };

  if (analysis.pivotConfig) {
    config.pivotConfig = analysis.pivotConfig;
  }

  if (analysis.groupedPivotConfig) {
    config.groupedPivotConfig = analysis.groupedPivotConfig;
  }

  return config;
}
