import * as XLSX from "xlsx";
import * as fs from "fs";
import { storage } from "./storage";
import Anthropic from "@anthropic-ai/sdk";
import { validateImportFile, logValidationFailure } from "./importValidator";
import { isSizeAllowed, SizeLimitConfig } from "./sizeUtils";
import {
  cleanInventoryData,
  applyVariantRules,
  isColorCode,
  formatColorName,
  applyImportRules,
  applyPriceBasedExpansion,
  buildStylePriceMapFromCache,
} from "./inventoryProcessing";
import { startImport, completeImport, failImport } from "./importState";

// Re-export shared processing functions for backward compatibility
export {
  cleanInventoryData,
  applyVariantRules,
  isColorCode,
  formatColorName,
  applyImportRules,
  applyPriceBasedExpansion,
  buildStylePriceMapFromCache,
};

/**
 * Check if a sale file has discontinued styles registered.
 * Used to warn when importing regular files before their linked sale file.
 *
 * @param saleDataSourceId - ID of the sale data source to check
 * @returns Object with check results
 */
export async function checkSaleFileHasStyles(
  saleDataSourceId: string,
): Promise<{
  hasStyles: boolean;
  styleCount: number;
}> {
  const styles = await storage.getDiscontinuedStyles(saleDataSourceId);
  return {
    hasStyles: styles.length > 0,
    styleCount: styles.length,
  };
}

/**
 * Check if regular data source requires sale file import first and warn if not imported.
 * Returns warning message if applicable, null otherwise.
 */
export async function checkSaleImportFirstRequirement(
  dataSourceId: string,
): Promise<{
  requiresWarning: boolean;
  warningMessage: string | null;
  saleDataSourceId: string | null;
  saleDataSourceName: string | null;
}> {
  const dataSource = await storage.getDataSource(dataSourceId);
  if (!dataSource) {
    return {
      requiresWarning: false,
      warningMessage: null,
      saleDataSourceId: null,
      saleDataSourceName: null,
    };
  }

  // Only check for regular files with assigned sale file and requireSaleImportFirst enabled
  if (
    dataSource.sourceType === "sales" ||
    !dataSource.assignedSaleDataSourceId ||
    dataSource.requireSaleImportFirst === false
  ) {
    return {
      requiresWarning: false,
      warningMessage: null,
      saleDataSourceId: null,
      saleDataSourceName: null,
    };
  }

  const saleDataSource = await storage.getDataSource(
    dataSource.assignedSaleDataSourceId,
  );
  if (!saleDataSource) {
    return {
      requiresWarning: false,
      warningMessage: null,
      saleDataSourceId: null,
      saleDataSourceName: null,
    };
  }

  const { hasStyles, styleCount } = await checkSaleFileHasStyles(
    dataSource.assignedSaleDataSourceId,
  );

  if (!hasStyles) {
    return {
      requiresWarning: true,
      warningMessage: `The linked sale file "${saleDataSource.name}" has not been imported yet (0 discontinued styles registered). Import the sale file first to ensure proper filtering of discontinued items.`,
      saleDataSourceId: dataSource.assignedSaleDataSourceId,
      saleDataSourceName: saleDataSource.name,
    };
  }

  console.log(
    `[checkSaleImportFirstRequirement] Sale file "${saleDataSource.name}" has ${styleCount} styles registered - OK`,
  );
  return {
    requiresWarning: false,
    warningMessage: null,
    saleDataSourceId: null,
    saleDataSourceName: null,
  };
}

export interface TemplateValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
  missingColumns: string[];
  extraColumns: string[];
  columnChanges: boolean;
}

const LETTER_SIZES = [
  "XXS",
  "XS",
  "S",
  "M",
  "L",
  "XL",
  "2XL",
  "3XL",
  "4XL",
  "5XL",
];
const LETTER_SIZE_MAP: Record<string, number> = {
  XXS: 0,
  XS: 1,
  S: 2,
  M: 3,
  L: 4,
  XL: 5,
  "2XL": 6,
  "3XL": 7,
  "4XL": 8,
  "5XL": 9,
  XXL: 6,
  XXXL: 7,
  XXXXL: 8,
  XXXXXL: 9,
};

const NUMERIC_SIZES = [
  "000",
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
];
const NUMERIC_SIZE_MAP: Record<string, number> = {};
NUMERIC_SIZES.forEach((size, index) => {
  NUMERIC_SIZE_MAP[size] = index;
});

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

// Known 2-4 letter color ABBREVIATION CODES that AI should correct
// IMPORTANT: These are ONLY terse abbreviations, NOT real color words
// Real color words (tan, nude, rose, mint, etc.) are in VALID_COLOR_WORDS
const COLOR_ABBREVIATION_CODES = new Set([
  // Single colors - 2-3 letter codes ONLY
  "BK",
  "BLK",
  "BLA", // Black
  "WH",
  "WT",
  "WHT", // White
  "NV",
  "NVY", // Navy
  "BL",
  "BLU", // Blue
  "GN",
  "GR",
  "GRN", // Green
  "RD", // Red (short form only)
  "GY",
  "GRY", // Gray
  "PK",
  "PNK", // Pink
  "PP",
  "PRP",
  "PUR", // Purple
  "OG",
  "OR",
  "ORG", // Orange
  "YL",
  "YLW",
  "YEL", // Yellow
  "BR",
  "BRN",
  "BRWN", // Brown
  "BGE", // Beige
  "CRM",
  "CR", // Cream
  "IV",
  "IVR", // Ivory (NOT IVY - that's a word)
  "GD",
  "GLD", // Gold
  "SV",
  "SLV", // Silver
  "MU",
  "MLT",
  "MULT", // Multi
  "BH",
  "BLSH", // Blush
  "CH",
  "CHP",
  "CHAM", // Champagne
  "ND",
  "NUD", // Nude (NOT NUDE - that's a word)
  "EM",
  "EME",
  "EMER", // Emerald
  "BU",
  "BUR",
  "BURG", // Burgundy
  "AQ",
  "AQU", // Aqua
  "TQ",
  "TRQ",
  "TURQ", // Turquoise
  "CO",
  "CRL", // Coral
  "TL",
  "TEL", // Teal
  "SG",
  "SAG", // Sage
  "LV",
  "LAV", // Lavender
  "MV",
  "MAU",
  "MAUV", // Mauve
  "PL",
  "PLM", // Plum (NOT PLUM - that's a word)
  "OL",
  "OLV",
  "OLIV", // Olive
  "CY",
  "CYN", // Cyan
  "MG",
  "MGT",
  "MGTA", // Magenta
  "FU",
  "FUS",
  "FUSC", // Fuschia
  "PH",
  "PCH", // Peach (NOT PEACH - that's a word)
  "MN",
  "MNT", // Mint (NOT MINT - that's a word)
  "RS",
  "ROS", // Rose (NOT ROSE - that's a word)
  "CB",
  "COB",
  "COBT", // Cobalt
  "AB",
  "ABM", // AB/Multicolor
  "SL",
  "SLT", // Slate
  "CHR",
  "CHAR", // Charcoal
  "NE",
  "NEO", // Neon (NOT NEON - that's a word)
  "BRZ",
  "BZ", // Bronze
  "CPR",
  "CP", // Copper
  "RG", // Rose Gold
]);

// Valid color WORDS that should NEVER be sent to AI for correction
// These are real English color names - even in uppercase they don't need correction
const VALID_COLOR_WORDS = new Set([
  "black",
  "white",
  "red",
  "blue",
  "green",
  "yellow",
  "orange",
  "purple",
  "pink",
  "brown",
  "gray",
  "grey",
  "beige",
  "cream",
  "ivory",
  "tan",
  "gold",
  "silver",
  "bronze",
  "copper",
  "navy",
  "teal",
  "coral",
  "sage",
  "blush",
  "nude",
  "champagne",
  "burgundy",
  "maroon",
  "wine",
  "merlot",
  "plum",
  "mauve",
  "lavender",
  "lilac",
  "violet",
  "indigo",
  "cobalt",
  "royal",
  "sky",
  "baby",
  "mint",
  "olive",
  "forest",
  "lime",
  "emerald",
  "jade",
  "hunter",
  "turquoise",
  "aqua",
  "cyan",
  "magenta",
  "fuchsia",
  "fuschia",
  "hot",
  "rose",
  "peach",
  "apricot",
  "salmon",
  "rust",
  "brick",
  "terra",
  "camel",
  "mocha",
  "coffee",
  "chocolate",
  "espresso",
  "taupe",
  "sand",
  "charcoal",
  "slate",
  "ash",
  "pewter",
  "gunmetal",
  "steel",
  "pearl",
  "opal",
  "crystal",
  "diamond",
  "platinum",
  "neon",
  "bright",
  "light",
  "dark",
  "deep",
  "pale",
  "soft",
  "dusty",
  "multi",
  "multicolor",
  "rainbow",
  "ombre",
  "gradient",
  "floral",
  "print",
  "pattern",
  "stripe",
  "polka",
  // Additional fashion colors
  "blush",
  "ballet",
  "bubblegum",
  "carnation",
  "cherry",
  "cranberry",
  "crimson",
  "garnet",
  "raspberry",
  "ruby",
  "scarlet",
  "strawberry",
  "watermelon",
  "hibiscus",
  "cerise",
  "vermillion",
  "azure",
  "cerulean",
  "denim",
  "midnight",
  "ocean",
  "peacock",
  "periwinkle",
  "sapphire",
  "steel",
  "stormy",
  "twilight",
  "celery",
  "fern",
  "grass",
  "kelly",
  "moss",
  "pistachio",
  "seafoam",
  "shamrock",
  "spring",
  "tiffany",
  "wintergreen",
  "amber",
  "butterscotch",
  "canary",
  "citrine",
  "dandelion",
  "dijon",
  "goldenrod",
  "honey",
  "lemon",
  "marigold",
  "mustard",
  "saffron",
  "sunflower",
  "burnt",
  "carrot",
  "clementine",
  "mango",
  "papaya",
  "pumpkin",
  "sunset",
  "tangerine",
  "terracotta",
  "amethyst",
  "aubergine",
  "eggplant",
  "grape",
  "heather",
  "orchid",
  "mulberry",
  "thistle",
  "wisteria",
  "bone",
  "ecru",
  "eggshell",
  "linen",
  "natural",
  "off",
  "vanilla",
  "winter",
  "black",
  "ebony",
  "jet",
  "onyx",
  "raven",
  "alabaster",
  "chalk",
  "coconut",
  "frost",
  "ghost",
  "snow",
]);

/**
 * Check if a color value is an abbreviation code that needs AI correction.
 * Returns true ONLY for actual abbreviation codes (BLK, NVY, RD/BK, etc.)
 * Returns false for valid color words (Blush, Navy, Coral, Nude, Rose, etc.) even in uppercase
 *
 * IMPORTANT: Checks VALID_COLOR_WORDS first to prevent real color words from being
 * incorrectly identified as codes (e.g., NUDE, ROSE, MINT should NOT go to AI)
 */

/**
 * Normalize color value by removing unwanted spaces around delimiters.
 * This prevents duplicates like "Fuscia/Ombre" vs "Fuscia/ Ombre".
 *
 * Examples:
 *   "Fuscia/ Ombre" -> "Fuscia/Ombre"
 *   "Light  Blue" -> "Light Blue"
 *   "Red / White" -> "Red/White"
 *   "Navy - Blue" -> "Navy-Blue"
 */
export function normalizeColorValue(color: string): string {
  if (!color) return color;

  let result = color.trim();

  // Collapse multiple spaces into single space
  result = result.replace(/\s{2,}/g, " ");

  // Remove spaces around slashes: "Fuscia/ Ombre" -> "Fuscia/Ombre", "Red / White" -> "Red/White"
  result = result.replace(/\s*\/\s*/g, "/");

  // Remove spaces around hyphens: "Navy - Blue" -> "Navy-Blue"
  result = result.replace(/\s*-\s*/g, "-");

  // Remove spaces around ampersands: "Black & White" is OK, but "Black  &  White" -> "Black & White"
  // We keep single spaces around & for readability but collapse multiples
  result = result
    .replace(/\s*&\s*/g, " & ")
    .replace(/\s{2,}/g, " ")
    .trim();

  return result;
}

async function suggestColorCorrections(
  colors: string[],
): Promise<Array<{ badColor: string; goodColor: string; confidence: number }>> {
  if (colors.length === 0) return [];

  const prompt = `You are an expert at converting color ABBREVIATION CODES to full color names.

IMPORTANT: You are ONLY receiving abbreviation codes (like BLK, NVY, RD/BK). 
You will NOT receive valid color words like "Blush", "Navy", "Coral".

COLOR CODES TO CONVERT:
${colors.map((c) => `- "${c}"`).join("\n")}

STANDARD ABBREVIATION CODE REFERENCE:
- BK, BLK, BLA → Black
- WH, WT, WHT → White
- NV, NVY → Navy
- BL, BLU → Blue
- GN, GR, GRN → Green
- RD → Red
- GY, GRY → Gray
- PK, PNK → Pink
- PP, PRP, PUR → Purple
- OG, OR, ORG → Orange
- YL, YLW, YEL → Yellow
- BR, BRN, BRWN → Brown
- BGE → Beige
- CRM, CR → Cream
- IV, IVR → Ivory
- GD, GLD → Gold
- SV, SLV → Silver
- BH, BLSH → Blush
- CH, CHP, CHAM → Champagne
- ND, NUD → Nude
- EM, EME, EMER → Emerald
- BU, BUR, BURG → Burgundy
- AQ, AQU → Aqua
- TQ, TRQ, TURQ → Turquoise
- CO, CRL → Coral
- TL, TEL → Teal
- SG, SAG → Sage
- MU, MLT, MULT → Multi/Multicolor
- RD/BLK → Red/Black
- GN/WH → Green/White
- GY/BN → Gray/Brown

Return JSON with "corrections" array:
{"corrections": [{"badColor": "original", "goodColor": "Standard Name", "confidence": 0.0-1.0}]}

Confidence 0.9+ for codes in reference list, 0.7-0.9 for similar patterns, below 0.7 if uncertain.`;

  try {
    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 1000,
      messages: [
        {
          role: "user",
          content: prompt,
        },
      ],
      system:
        "You are a color abbreviation expert. Always respond with valid JSON only, no markdown formatting.",
    });

    const content =
      response.content[0]?.type === "text" ? response.content[0].text : "{}";
    // Remove any markdown code blocks if present
    const cleanedContent = content.replace(/```json\n?|\n?```/g, "").trim();
    const parsed = JSON.parse(cleanedContent);
    return parsed.corrections || parsed.results || [];
  } catch (err) {
    console.error("Error suggesting color corrections:", err);
    return [];
  }
}

export function applyCleaningToValue(
  value: string,
  config: any,
  field: string,
): string {
  if (!value || typeof value !== "string") return value;

  let cleaned = value;

  if (config?.trimWhitespace) {
    cleaned = cleaned.trim();
  }

  if (config?.removeLetters) {
    cleaned = cleaned.replace(/[a-zA-Z]/g, "");
  }

  if (config?.removeNumbers) {
    cleaned = cleaned.replace(/[0-9]/g, "");
  }

  if (config?.removeSpecialChars) {
    cleaned = cleaned.replace(/[^a-zA-Z0-9\s]/g, "");
  }

  if (config?.removeFirstN && config.removeFirstN > 0) {
    cleaned = cleaned.substring(config.removeFirstN);
  }

  if (config?.removeLastN && config.removeLastN > 0) {
    cleaned = cleaned.substring(0, cleaned.length - config.removeLastN);
  }

  if (config?.findText && config.findText.length > 0) {
    const regex = new RegExp(config.findText, "gi");
    cleaned = cleaned.replace(regex, config.replaceText || "");
  }

  // Process multiple find/replace rules (array)
  if (config?.findReplaceRules && Array.isArray(config.findReplaceRules)) {
    for (const rule of config.findReplaceRules) {
      if (rule.find && rule.find.length > 0) {
        const regex = new RegExp(rule.find, "gi");
        cleaned = cleaned.replace(regex, rule.replace || "");
      }
    }
  }

  if (config?.removePatterns && Array.isArray(config.removePatterns)) {
    for (const pattern of config.removePatterns) {
      if (pattern && pattern.length > 0) {
        const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const regex = new RegExp(escaped, "gi");
        cleaned = cleaned.replace(regex, "");
      }
    }
  }

  return cleaned.trim();
}

// Sheet and file parse configuration types
export interface SheetConfig {
  sheetName?: string;
  sheetIndex?: number;
}

export interface FileParseConfig {
  delimiter?: string;
  encoding?: string;
  hasHeaderRow?: boolean;
}

/**
 * Helper to select the correct sheet from a workbook based on config
 */
function selectSheet(
  workbook: XLSX.WorkBook,
  sheetConfig?: SheetConfig,
): { sheet: XLSX.WorkSheet; sheetName: string } {
  let sheetName = workbook.SheetNames[0]; // Default to first sheet

  if (sheetConfig?.sheetName) {
    // Try to find sheet by name
    if (workbook.SheetNames.includes(sheetConfig.sheetName)) {
      sheetName = sheetConfig.sheetName;
    } else {
      console.log(
        `[SheetConfig] Sheet "${sheetConfig.sheetName}" not found, using first sheet "${sheetName}"`,
      );
    }
  } else if (sheetConfig?.sheetIndex !== undefined) {
    // Use sheet by index
    if (
      sheetConfig.sheetIndex >= 0 &&
      sheetConfig.sheetIndex < workbook.SheetNames.length
    ) {
      sheetName = workbook.SheetNames[sheetConfig.sheetIndex];
    } else {
      console.log(
        `[SheetConfig] Sheet index ${sheetConfig.sheetIndex} out of range, using first sheet "${sheetName}"`,
      );
    }
  }

  console.log(`[SheetConfig] Using sheet: "${sheetName}"`);
  return { sheet: workbook.Sheets[sheetName], sheetName };
}

/**
 * Get list of available sheet names from a workbook
 */
export function getAvailableSheets(buffer: Buffer): string[] {
  try {
    const workbook = XLSX.read(buffer, { type: "buffer", cellNF: true });
    return workbook.SheetNames;
  } catch (error) {
    console.error("[getAvailableSheets] Error reading workbook:", error);
    return [];
  }
}

/**
 * Convert encoding name to XLSX codepage number
 */
function getCodepage(encoding: string): number | undefined {
  const codepageMap: { [key: string]: number } = {
    "utf-8": 65001,
    utf8: 65001,
    latin1: 1252,
    "iso-8859-1": 1252,
    "windows-1252": 1252,
    ascii: 20127,
    "utf-16": 1200,
    "utf-16le": 1200,
    "utf-16be": 1201,
  };
  return codepageMap[encoding.toLowerCase()];
}

/**
 * Check if a buffer is a CSV file (not Excel)
 * Used to prevent XLSX from corrupting CSV values
 */
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

/**
 * Parse CSV buffer to array of arrays (preserving all values as strings)
 * Handles UTF-16 and UTF-8 BOMs, auto-detects delimiter
 */
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

/**
 * Parse CSV content with custom delimiter
 */
export function parseCSVWithDelimiter(
  content: string,
  delimiter: string = ",",
): any[][] {
  const lines = content.split(/\r?\n/);
  const rows: any[][] = [];

  for (const line of lines) {
    if (!line.trim()) continue;

    // Handle quoted values with the delimiter inside
    const row: any[] = [];
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
      } else if (char === delimiter && !inQuotes) {
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

export function parseExcelToInventory(
  buffer: Buffer,
  columnMapping: any,
  cleaningConfig: any,
  sheetConfig?: SheetConfig,
  fileParseConfig?: FileParseConfig,
): { headers: string[]; rows: any[][]; items: any[] } {
  // CRITICAL FIX: Check for CSV first to prevent XLSX from corrupting values
  // Style numbers like "1921E0136" would become scientific notation if parsed by XLSX
  let rawData: any[][];

  if (isCSVBuffer(buffer)) {
    console.log(
      "[Email Import] Detected CSV file - using text parser to preserve values",
    );
    rawData = parseCSVAsText(buffer);
  } else {
    // Parse workbook with encoding option if specified
    const readOptions: XLSX.ParsingOptions = { type: "buffer" };
    if (fileParseConfig?.encoding) {
      readOptions.codepage = getCodepage(fileParseConfig.encoding);
    }

    const workbook = XLSX.read(buffer, readOptions);
    const { sheet } = selectSheet(workbook, sheetConfig);
    rawData = XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      raw: false,
    }) as any[][];
  }

  if (rawData.length === 0) {
    return { headers: [], rows: [], items: [] };
  }

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

  // Helper to get column value by name
  const getColIndex = (colName: string) => {
    if (!colName) return -1;
    return headers.findIndex(
      (h) => h && h.toString().toLowerCase() === colName.toLowerCase(),
    );
  };

  const styleColIdx = getColIndex(columnMapping?.style || "");
  const colorColIdx = getColIndex(columnMapping?.color || "");
  const sizeColIdx = getColIndex(columnMapping?.size || "");
  const stockColIdx = getColIndex(columnMapping?.stock || "");
  const priceColIdx = getColIndex(columnMapping?.price || "");
  const skuColIdx = getColIndex(columnMapping?.sku || "");
  // CRITICAL FIX: Add missing column mappings to match Manual import
  const shipDateColIdx = getColIndex(columnMapping?.shipDate || "");
  const costColIdx = getColIndex(columnMapping?.cost || "");
  const discontinuedColIdx = getColIndex(columnMapping?.discontinued || "");

  // Check if this is a Jovani-style sale file that needs stateful parsing
  // Jovani sale files have style/color interleaved where:
  // - Style row: STYLE has value, COLOR is empty or missing
  // - Color row: COLOR has value (letters), SIZE has value, STOCK has value
  // - When COLOR is purely numeric, it's a new style (misaligned row)
  console.log(
    "[Import] cleaningConfig.pivotedFormat:",
    JSON.stringify(cleaningConfig?.pivotedFormat),
  );
  const isJovaniSaleFormat = cleaningConfig?.pivotedFormat?.vendor === "jovani";

  if (isJovaniSaleFormat) {
    console.log("[Import] Using Jovani sale file stateful parser");
    return parseJovaniSaleFile(
      dataRows,
      headers,
      styleColIdx,
      colorColIdx,
      sizeColIdx,
      stockColIdx,
      priceColIdx,
      skuColIdx,
      cleaningConfig,
    );
  } else {
    console.log("[Import] Using standard stateless parser");
  }

  // Standard stateless parsing for other files
  const items = dataRows
    .map((row: any[]) => {
      const getColValue = (colIdx: number) =>
        colIdx >= 0 ? row[colIdx] : null;

      let sku = String(getColValue(skuColIdx) || "").trim();
      let style = String(getColValue(styleColIdx) || "").trim();
      // FIX: Use ?? instead of || to preserve numeric 0 (valid size like "0" or "00")
      // The || operator treats 0 as falsy, so 0 || "" returns "" instead of 0
      let size = String(getColValue(sizeColIdx) ?? "").trim();
      let color = String(getColValue(colorColIdx) || "").trim();
      let stockValue = getColValue(stockColIdx);

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
      }

      style = applyCleaningToValue(
        String(style || ""),
        cleaningConfig,
        "style",
      );

      // Normalize multiple spaces to single space in style field
      style = style.replace(/\s+/g, " ").trim();

      // Apply size transformations if configured
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

      if (!sku && style) {
        sku = style;
      }

      let stock = 0;
      let stockMapped = false;

      // First, try stock text mappings (e.g., "SOLD OUT" → 0, "Very Low" → 1)
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

      // Check skip rule: skip rows when specified columns have certain values
      let shouldSkip = false;
      if (
        cleaningConfig?.skipRule?.enabled &&
        cleaningConfig.skipRule.conditions?.length > 0
      ) {
        // All conditions must match to trigger skip
        const allConditionsMatch = cleaningConfig.skipRule.conditions.every(
          (condition: { column: string; value: string }) => {
            const colIdx = headers.findIndex(
              (h) =>
                h.toLowerCase() === condition.column.toLowerCase() ||
                h === condition.column,
            );
            if (colIdx === -1) return false;
            const cellValue = String(row[colIdx] || "")
              .trim()
              .toLowerCase();
            return cellValue === condition.value.toLowerCase();
          },
        );

        if (allConditionsMatch) {
          // If skipUnlessContinueSelling is true, we mark for skip (actual skip happens later based on continueSelling setting)
          shouldSkip = true;
        }
      }

      // Handle ship date: First try conditional rule, then fall back to direct column mapping
      let shipDate: string | null = null;

      // Method 1: Conditional ship date rule (when column X = value Y, use column Z)
      if (cleaningConfig?.shipDateRule?.enabled) {
        const condColIdx = headers.findIndex(
          (h) =>
            h.toLowerCase() ===
              cleaningConfig.shipDateRule.conditionColumn.toLowerCase() ||
            h === cleaningConfig.shipDateRule.conditionColumn,
        );
        const dateColIdx = headers.findIndex(
          (h) =>
            h.toLowerCase() ===
              cleaningConfig.shipDateRule.dateColumn.toLowerCase() ||
            h === cleaningConfig.shipDateRule.dateColumn,
        );

        if (condColIdx !== -1 && dateColIdx !== -1) {
          const condValue = String(row[condColIdx] || "")
            .trim()
            .toLowerCase();
          if (
            condValue ===
            cleaningConfig.shipDateRule.conditionValue.toLowerCase()
          ) {
            shipDate = String(row[dateColIdx] || "").trim();
          }
        }
      }

      // Method 2: Direct column mapping (matching Manual import behavior)
      // Only if conditional rule didn't set a date
      if (!shipDate && shipDateColIdx !== -1) {
        const shipDateValue = row[shipDateColIdx];
        if (shipDateValue) {
          // Handle Excel date serial numbers
          if (typeof shipDateValue === "number" && shipDateValue > 0) {
            // Excel stores dates as days since 1899-12-30
            const excelEpoch = new Date(1899, 11, 30);
            const date = new Date(
              excelEpoch.getTime() + shipDateValue * 24 * 60 * 60 * 1000,
            );
            shipDate = date.toISOString().split("T")[0]; // Store as YYYY-MM-DD
          } else {
            // Already a string, try to parse and normalize
            const dateStr = String(shipDateValue).trim();
            if (dateStr && dateStr.toLowerCase() !== "n/a") {
              const parsedDate = new Date(dateStr);
              if (!isNaN(parsedDate.getTime())) {
                shipDate = parsedDate.toISOString().split("T")[0]; // Store as YYYY-MM-DD
              }
            }
          }
        }
      }

      // Parse cost from column mapping (matching Manual import)
      const cost =
        costColIdx !== -1 && row[costColIdx]
          ? String(row[costColIdx]).replace(/[$,]/g, "").trim()
          : null;

      // CRITICAL FIX: Parse price from column mapping (matching Manual import)
      // This is required for price-based expansion to work correctly
      const price =
        priceColIdx !== -1 && row[priceColIdx]
          ? String(row[priceColIdx]).replace(/[$,]/g, "").trim()
          : null;

      // CRITICAL FIX: Parse salePrice from column mapping (matching Manual import)
      const salePriceColIdx = getColIndex(columnMapping?.salePrice || "");
      const salePrice =
        salePriceColIdx !== -1 && row[salePriceColIdx]
          ? String(row[salePriceColIdx]).replace(/[$,]/g, "").trim()
          : null;

      // Parse discontinued from column mapping (matching Manual import)
      const discontinued =
        discontinuedColIdx !== -1 && row[discontinuedColIdx]
          ? (() => {
              const val = String(row[discontinuedColIdx]).trim();
              return val.length > 0;
            })()
          : false;

      return {
        sku: sku || "",
        style: style || null,
        // FIX: Preserve "0" as valid size (matching Manual import check)
        size: size != null && size !== "" ? String(size) : null,
        color: color || null,
        stock,
        cost,
        price, // CRITICAL FIX: Add price field (matching Manual import)
        shipDate,
        discontinued,
        // CRITICAL FIX: Set hasFutureStock flag for items with ship dates (matching manual upload parser)
        // This ensures items with future ship dates are preserved even with 0 stock
        hasFutureStock: shipDate ? true : false,
        preserveZeroStock: shipDate && stock === 0 ? true : false,
        salePrice, // CRITICAL FIX: Add salePrice field (matching Manual import)
        shouldSkip,
        skipUnlessContinueSelling:
          cleaningConfig?.skipRule?.skipUnlessContinueSelling ?? false,
        rawData: Object.fromEntries(headers.map((h, i) => [h, row[i]])),
      };
    })
    .filter((item) => item.sku);

  return { headers, rows: dataRows, items };
}

/**
 * Stateful parser for Jovani sale files where styles and colors are interleaved.
 *
 * File structure:
 * - Style header row: STYLE column has value, COLOR column is empty
 * - Color/variant rows: COLOR column has non-numeric value, SIZE has value
 * - Misaligned style row: STYLE empty, COLOR has purely numeric value (e.g., "1012")
 *
 * This parser maintains currentStyle context and correctly assigns variants.
 */
function parseJovaniSaleFile(
  dataRows: any[][],
  headers: string[],
  styleColIdx: number,
  colorColIdx: number,
  sizeColIdx: number,
  stockColIdx: number,
  priceColIdx: number,
  skuColIdx: number,
  cleaningConfig: any,
): { headers: string[]; rows: any[][]; items: any[] } {
  const items: any[] = [];
  let currentStyle = "";
  let stylesFound = 0;
  let variantsFound = 0;

  for (const row of dataRows) {
    const styleValue = String(row[styleColIdx] || "").trim();
    const colorValue = String(row[colorColIdx] || "").trim();
    // FIX: Use ?? to preserve numeric 0 (valid size)
    const sizeValue = String(row[sizeColIdx] ?? "").trim();
    const stockValue = row[stockColIdx];
    const priceValue = row[priceColIdx];

    // Check if this is a style header row:
    // 1. STYLE column has value AND COLOR column is empty
    // 2. OR: STYLE is empty but COLOR is purely numeric (misaligned style row)
    // 3. OR: STYLE is a 4-6 digit number (style header even if color has a value)
    const isPurelyNumeric = (val: string) => /^\d+$/.test(val);
    const isStyleNumber = (val: string) => /^#?\d{4,6}$/.test(val);

    const isStyleRowNormal = styleValue && !colorValue;
    const isStyleRowMisaligned =
      !styleValue && colorValue && isPurelyNumeric(colorValue);
    const isStyleRowNumeric = styleValue && isStyleNumber(styleValue);

    if (isStyleRowNormal || isStyleRowNumeric) {
      // Normal style header row - style in STYLE column
      currentStyle = applyCleaningToValue(styleValue, cleaningConfig, "style");
      currentStyle = currentStyle.replace(/\s+/g, " ").trim();
      stylesFound++;
      console.log(`[Jovani Parser] Found style: ${currentStyle}`);
      continue;
    }

    if (isStyleRowMisaligned) {
      // Misaligned style row - numeric style in COLOR column
      currentStyle = applyCleaningToValue(colorValue, cleaningConfig, "style");
      currentStyle = currentStyle.replace(/\s+/g, " ").trim();
      stylesFound++;
      console.log(
        `[Jovani Parser] Found style (from color column): ${currentStyle}`,
      );
      continue;
    }

    // This should be a variant row - must have currentStyle, color (non-numeric), and size
    if (!currentStyle) {
      continue; // Skip rows before first style is found
    }

    // Skip rows with no color or numeric-only color (these are style headers)
    if (!colorValue || isPurelyNumeric(colorValue)) {
      continue;
    }

    // This is a valid variant row
    const color = colorValue;
    const size = sizeValue;

    // Parse stock
    let stock = 0;
    if (typeof stockValue === "number") {
      stock = stockValue;
    } else if (typeof stockValue === "string") {
      const parsed = Math.max(
        0,
        Math.round(parseFloat(stockValue.replace(/[^0-9.-]/g, ""))),
      );
      stock = isNaN(parsed) ? 0 : parsed;
    }

    // Parse price
    let price = null;
    if (typeof priceValue === "number") {
      price = priceValue;
    } else if (typeof priceValue === "string") {
      const parsed = parseFloat(priceValue.replace(/[^0-9.-]/g, ""));
      price = isNaN(parsed) ? null : parsed;
    }

    // Build SKU - convert slashes and spaces to hyphens for Shopify compatibility
    const sku = `${currentStyle}-${color}-${size}`
      .replace(/\//g, "-")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-");

    items.push({
      sku,
      style: currentStyle,
      // FIX: Preserve "0" as valid size
      size: size === "" ? null : size,
      color: color || null,
      stock,
      price,
      rawData: Object.fromEntries(headers.map((h, i) => [h, row[i]])),
      // FIX: Add flags for consistency (no shipDate in Jovani sale files)
      hasFutureStock: false,
      preserveZeroStock: false,
    });
    variantsFound++;
  }

  console.log(
    `[Jovani Parser] Parsed ${stylesFound} styles and ${variantsFound} variants`,
  );

  return { headers, rows: dataRows, items };
}

/**
 * Parse a pivoted Excel file (like Tarik Ediz) where sizes are column headers.
 *
 * Structure:
 * - Style rows: Style number in styleColumn, product name in nameColumn
 * - Color/stock rows: D/date in statusColumn, color in colorColumn, stock values in size columns
 *
 * The statusColumn can contain:
 * - "D" = Discontinued (only keep in-stock, delete zero-stock variants)
 * - A date (e.g., "08/03/2026") = Pre-order with ship date (preserve all variants)
 */
export function parsePivotedExcelToInventory(
  buffer: Buffer,
  pivotConfig: {
    enabled?: boolean;
    styleRowPattern?: { column: number; nameColumn?: number };
    colorRowPattern?: {
      statusColumn: number;
      colorColumn: number;
      discontinuedValue?: string;
    };
    sizeColumns?: {
      startColumn: number;
      endColumn: number;
      sizeHeaders?: string[];
    };
    skipRows?: number;
  },
  cleaningConfig: any,
  _dataSourceName?: string, // Reserved for future use - prefix is applied by caller
  sheetConfig?: SheetConfig,
  fileParseConfig?: FileParseConfig,
): { headers: string[]; rows: any[][]; items: any[] } {
  console.log(`\n\n========== PIVOT PARSER INVOKED ==========`);
  console.log(`[Pivot Parser] Function called at ${new Date().toISOString()}`);
  console.log(`[Pivot Parser] pivotConfig.enabled: ${pivotConfig?.enabled}`);
  console.log(`[Pivot Parser] Buffer size: ${buffer?.length || 0} bytes`);

  // Parse workbook with encoding option if specified
  const readOptions: XLSX.ParsingOptions = { type: "buffer" };
  if (fileParseConfig?.encoding) {
    readOptions.codepage = getCodepage(fileParseConfig.encoding);
  }

  const workbook = XLSX.read(buffer, readOptions);
  const { sheet } = selectSheet(workbook, sheetConfig);
  const rawData = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false,
  }) as any[][];

  if (rawData.length === 0) {
    return { headers: [], rows: [], items: [] };
  }

  const styleCol = pivotConfig.styleRowPattern?.column ?? 0;
  const nameCol = pivotConfig.styleRowPattern?.nameColumn ?? 7;
  const statusCol = pivotConfig.colorRowPattern?.statusColumn ?? 0;
  const colorCol = pivotConfig.colorRowPattern?.colorColumn ?? 11;
  const discontinuedValue =
    pivotConfig.colorRowPattern?.discontinuedValue ?? "D";
  const startCol = pivotConfig.sizeColumns?.startColumn ?? 13;
  const endCol = pivotConfig.sizeColumns?.endColumn ?? 26;
  const skipRows = pivotConfig.skipRows ?? 5;

  // Default size headers for Tarik Ediz format: 2, 4, 6, 8, 10, 12, 14, 16, 18
  const defaultSizeHeaders = [
    "2",
    "4",
    "6",
    "8",
    "10",
    "12",
    "14",
    "16",
    "18",
    "",
    "",
    "",
    "",
    "",
  ];
  const sizeHeaders =
    pivotConfig.sizeColumns?.sizeHeaders || defaultSizeHeaders;
  // Auto-detect size headers from each style row (supports mixed numeric and letter sizes)
  const autoDetectSizeHeaders =
    pivotConfig.sizeColumns?.autoDetectSizeHeaders ?? false;
  // Only use configured headers if explicitly set AND auto-detect is disabled
  const useConfiguredSizeHeaders =
    !!pivotConfig.sizeColumns?.sizeHeaders && !autoDetectSizeHeaders;

  const items: any[] = [];
  let currentStyle = "";
  let currentName = "";
  let currentSizeHeaders: string[] = useConfiguredSizeHeaders
    ? sizeHeaders
    : []; // Size headers from the current style row or config

  console.log(
    `[Pivot Parser] Processing ${rawData.length} rows, skipping first ${skipRows} rows`,
  );
  console.log(
    `[Pivot Parser] Config: styleCol=${styleCol}, colorCol=${colorCol}, statusCol=${statusCol}`,
  );
  console.log(`[Pivot Parser] Size columns: ${startCol}-${endCol}`);
  console.log(
    `[Pivot Parser] Auto-detect size headers: ${autoDetectSizeHeaders}`,
  );
  console.log(
    `[Pivot Parser] Using configured sizeHeaders: ${useConfiguredSizeHeaders ? sizeHeaders.filter(Boolean).join(", ") : "no (extracting from rows)"}`,
  );

  for (let i = skipRows; i < rawData.length; i++) {
    const row = rawData[i];
    if (!row || row.length === 0) continue;

    const firstCell = String(row[styleCol] || "").trim();
    // Keep raw status value for date detection (Excel serial numbers)
    const statusRaw = row[statusCol];
    // Convert Excel serial dates to date strings before string conversion
    // Track if already in US format (from Excel serial conversion)
    let statusCell: string;
    let statusIsUSFormat = false;
    if (
      typeof statusRaw === "number" &&
      statusRaw >= 40000 &&
      statusRaw <= 70000
    ) {
      statusCell = excelSerialToDateString(statusRaw);
      statusIsUSFormat = true; // Already in MM/DD/YYYY format
    } else {
      statusCell = String(statusRaw || "").trim();
    }
    const colorCell = String(row[colorCol] || "").trim();

    // Skip empty rows and "Total" rows
    if (!firstCell && !statusCell && !colorCell) continue;
    if (
      firstCell.toLowerCase() === "total" ||
      String(row[4] || "").toLowerCase() === "total"
    )
      continue;

    // Check if this is a style header row (has style number, no D/date, no color data in colorCol)
    // Style rows have the style number in col0 and size headers in columns 13-26
    // Use raw value for isDateLike check to handle Excel serial dates
    // FIX: Make D comparison case-insensitive
    // FIX: Only check statusRaw for date-likeness, NOT firstCell - style numbers like "50902"
    // fall in the Excel serial date range (40000-70000) but are NOT dates
    const isStyleRow =
      firstCell &&
      firstCell.toUpperCase() !== discontinuedValue.toUpperCase() &&
      !isDateLike(statusRaw) &&
      !colorCell &&
      firstCell !== "Total";

    // Also check if a purely numeric value appears in the color column - this is likely a misaligned style number
    // Colors should contain letters, not just digits (e.g., "1012" is a style, not a color)
    const isNumericInColorCol = colorCell && isPurelyNumeric(colorCell);

    if (isStyleRow || isNumericInColorCol) {
      // Determine which value to use as the style
      // If colorCell has a numeric value, it might be the actual style number (misaligned row)
      const styleValue =
        isNumericInColorCol && !firstCell ? colorCell : firstCell;

      if (styleValue) {
        // This is a style header row - extract style and size headers from this row
        currentStyle = applyCleaningToValue(
          styleValue,
          cleaningConfig,
          "style",
        );
        currentName = String(row[nameCol] || "").trim();

        // Only extract size headers from row data if not using configured sizeHeaders
        if (!useConfiguredSizeHeaders) {
          // Extract size headers from this style row (columns 13-26)
          // Stop when we hit 3+ consecutive empty columns (indicates end of actual sizes)
          const rawSizeHeaders: string[] = [];
          let consecutiveEmpty = 0;
          let stopIndex = -1;

          for (let colIdx = startCol; colIdx <= endCol; colIdx++) {
            const sizeValue = row[colIdx];
            // FIX: Use ?? to preserve numeric 0 (valid size header)
            const size = String(sizeValue ?? "").trim();
            rawSizeHeaders.push(size);

            if (!size) {
              consecutiveEmpty++;
              // If we see 3+ consecutive empty columns after valid sizes, mark stop point
              if (consecutiveEmpty >= 3 && rawSizeHeaders.some((s) => s)) {
                stopIndex = rawSizeHeaders.length - consecutiveEmpty;
                break;
              }
            } else {
              consecutiveEmpty = 0;
            }
          }

          // If we found a stop point, truncate to exclude spurious values after the gap
          if (stopIndex > 0) {
            currentSizeHeaders = rawSizeHeaders.slice(0, stopIndex);
            console.log(
              `[Pivot Parser] Auto-detected sizes for ${currentStyle}: raw values = [${rawSizeHeaders.map((s) => s || "(empty)").join(", ")}] (truncated at ${stopIndex} due to gap)`,
            );
          } else {
            currentSizeHeaders = rawSizeHeaders;
            console.log(
              `[Pivot Parser] Auto-detected sizes for ${currentStyle}: raw values = [${currentSizeHeaders.map((s) => s || "(empty)").join(", ")}]`,
            );
          }
        } else {
          console.log(
            `[Pivot Parser] Using configured sizes for ${currentStyle}: [${currentSizeHeaders.filter(Boolean).join(", ")}]`,
          );
        }

        if (isNumericInColorCol) {
          console.log(
            `[Pivot Parser] Found style (numeric in color col): ${currentStyle} (${currentName}), sizes: ${currentSizeHeaders.filter(Boolean).join(", ")}`,
          );
        } else {
          console.log(
            `[Pivot Parser] Found style: ${currentStyle} (${currentName}), sizes: ${currentSizeHeaders.filter(Boolean).join(", ")}`,
          );
        }
      }
      continue;
    }

    // Check if this is a color/stock row (has D or date in status column AND has a valid color)
    // A valid color must contain at least one letter (not purely numeric)
    const isValidColor = colorCell && !isPurelyNumeric(colorCell);
    // FIX: Make D comparison case-insensitive
    const isDiscontinuedCell =
      statusCell.toUpperCase() === discontinuedValue.toUpperCase();
    const isColorRow =
      isValidColor &&
      (isDiscontinuedCell || isDateLike(statusCell) || !statusCell);

    // Debug: Log when we find a D row
    if (isDiscontinuedCell && isValidColor) {
      console.log(
        `[Pivot Parser] Found discontinued row: style="${currentStyle}", color="${colorCell}", status="${statusCell}"`,
      );
    }

    if (isColorRow && currentStyle && currentSizeHeaders.length > 0) {
      // Determine if discontinued or has ship date
      const isDiscontinued = isDiscontinuedCell;
      // Convert European date format (DD/MM/YYYY) to US format (MM/DD/YYYY)
      // Skip conversion if already in US format (from Excel serial number conversion)
      let shipDate: string | null = null;
      if (isDateLike(statusCell)) {
        shipDate = statusIsUSFormat
          ? statusCell
          : convertEuropeanToUSDate(statusCell);
      }

      // Parse size columns and create inventory items using the current style's size headers
      for (let colIdx = startCol; colIdx <= endCol; colIdx++) {
        const sizeIdx = colIdx - startCol;
        const size = currentSizeHeaders[sizeIdx];

        // Skip empty size headers
        if (!size) continue;

        const stockValue = row[colIdx];
        let stock = 0;

        if (typeof stockValue === "number") {
          stock = stockValue;
        } else if (typeof stockValue === "string") {
          const parsed = Math.max(
            0,
            Math.round(parseFloat(stockValue.replace(/[^0-9.-]/g, ""))),
          );
          stock = isNaN(parsed) ? 0 : parsed;
        }

        // Create SKU: style-color-size (normalized) - prefix is applied by caller
        // Convert slashes and spaces to hyphens for Shopify compatibility
        const normalizedColor = colorCell.trim();
        const sku = `${currentStyle}-${normalizedColor}-${size}`
          .replace(/\//g, "-")
          .replace(/\s+/g, "-")
          .replace(/-+/g, "-");

        // Determine the actual shipDate for this item
        const itemShipDate = stock > 0 ? null : shipDate;
        items.push({
          sku,
          style: currentStyle,
          size,
          color: normalizedColor,
          stock,
          shipDate: itemShipDate,
          discontinued: isDiscontinued,
          rawData: {
            originalStyle: currentStyle,
            productName: currentName,
            statusMarker: statusCell,
            row: i,
          },
          // FIX: Add flags based on shipDate for consistency
          hasFutureStock: itemShipDate ? true : false,
          preserveZeroStock: itemShipDate && stock === 0 ? true : false,
        });
      }
    }
  }

  console.log(`[Pivot Parser] Extracted ${items.length} inventory items`);

  // Log discontinued items count
  const discontinuedCount = items.filter((i) => i.discontinued).length;
  if (discontinuedCount > 0) {
    console.log(
      `[Pivot Parser] Found ${discontinuedCount} discontinued (D) variants`,
    );
  }

  // Generate headers for preview
  const headers = [
    "sku",
    "style",
    "size",
    "color",
    "stock",
    "shipDate",
    "discontinued",
  ];
  const rows = items.map((item) => [
    item.sku,
    item.style,
    item.size,
    item.color,
    item.stock,
    item.shipDate,
    item.discontinued,
  ]);

  return { headers, rows, items };
}

/**
 * Check if a value is purely numeric (only digits, no letters).
 * Used to detect when a style number appears in the color column.
 * Colors should contain letters, not just numbers.
 */
function isPurelyNumeric(value: string): boolean {
  if (!value || typeof value !== "string") return false;
  const trimmed = value.trim();
  // Match values that are only digits (with optional leading zeros)
  // e.g., "1012", "04195", "12345"
  return /^\d+$/.test(trimmed);
}

/**
 * Check if a value looks like a date (various formats, including Excel serial dates)
 */
function isDateLike(value: string | number): boolean {
  // Handle Excel serial date numbers (roughly 40000-60000 for years 2009-2064)
  if (typeof value === "number") {
    return value >= 40000 && value <= 70000;
  }

  if (!value || typeof value !== "string" || value.length < 6) return false;

  // Check if it's a numeric string that could be an Excel serial date
  const numericValue = parseFloat(value);
  if (!isNaN(numericValue) && numericValue >= 40000 && numericValue <= 70000) {
    return true;
  }

  // Common date patterns: MM/DD/YYYY, DD/MM/YYYY, YYYY-MM-DD, DD.MM.YYYY
  const datePatterns = [
    /^\d{1,2}\/\d{1,2}\/\d{2,4}$/, // MM/DD/YYYY or DD/MM/YYYY
    /^\d{4}-\d{1,2}-\d{1,2}$/, // YYYY-MM-DD
    /^\d{1,2}\.\d{1,2}\.\d{2,4}$/, // DD.MM.YYYY
    /^\d{1,2}-\d{1,2}-\d{2,4}$/, // DD-MM-YYYY
  ];

  return datePatterns.some((pattern) => pattern.test(value.trim()));
}

/**
 * Convert Excel serial date number to date string (MM/DD/YYYY)
 */
function excelSerialToDateString(serial: number): string {
  const excelEpoch = new Date(1899, 11, 30); // Excel epoch is Dec 30, 1899
  const date = new Date(excelEpoch.getTime() + serial * 24 * 60 * 60 * 1000);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const year = date.getFullYear();
  return `${month}/${day}/${year}`;
}

/**
 * Convert European date format (DD/MM/YYYY or DD.MM.YYYY) to US format (MM/DD/YYYY)
 */
function convertEuropeanToUSDate(dateStr: string): string {
  if (!dateStr) return dateStr;

  const trimmed = dateStr.trim();

  // Handle DD/MM/YYYY or DD/MM/YY format
  const slashMatch = trimmed.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (slashMatch) {
    const [, day, month, year] = slashMatch;
    // If first number > 12, it's definitely day-first (European)
    // Otherwise assume European format for Tarik Ediz
    return `${month.padStart(2, "0")}/${day.padStart(2, "0")}/${year}`;
  }

  // Handle DD.MM.YYYY format (European with dots)
  const dotMatch = trimmed.match(/^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$/);
  if (dotMatch) {
    const [, day, month, year] = dotMatch;
    return `${month.padStart(2, "0")}/${day.padStart(2, "0")}/${year}`;
  }

  // Handle DD-MM-YYYY format
  const dashMatch = trimmed.match(/^(\d{1,2})-(\d{1,2})-(\d{2,4})$/);
  if (dashMatch) {
    const [, day, month, year] = dashMatch;
    return `${month.padStart(2, "0")}/${day.padStart(2, "0")}/${year}`;
  }

  // Return as-is if no match (already US format or ISO format)
  return trimmed;
}

export function validateTemplate(
  headers: string[],
  columnMapping: any,
  expectedHeaders?: string[],
): TemplateValidationResult {
  const result: TemplateValidationResult = {
    valid: true,
    errors: [],
    warnings: [],
    missingColumns: [],
    extraColumns: [],
    columnChanges: false,
  };

  const requiredMappings = ["sku", "style", "size", "color", "stock"];
  const mappedColumns: string[] = [];

  for (const field of requiredMappings) {
    const mappedColumn = columnMapping?.[field];
    if (mappedColumn) {
      mappedColumns.push(mappedColumn.toLowerCase());
      const found = headers.find(
        (h) => h.toLowerCase() === mappedColumn.toLowerCase(),
      );
      if (!found) {
        result.missingColumns.push(mappedColumn);
        result.errors.push(
          `Required column "${mappedColumn}" (mapped to ${field}) not found in file`,
        );
        result.valid = false;
      }
    }
  }

  if (expectedHeaders && expectedHeaders.length > 0) {
    const normalizedExpected = expectedHeaders.map((h) =>
      h.toLowerCase().trim(),
    );
    const normalizedFound = headers.map((h) => h.toLowerCase().trim());

    const missing = normalizedExpected.filter(
      (h) => !normalizedFound.includes(h),
    );
    const extra = normalizedFound.filter(
      (h) => !normalizedExpected.includes(h),
    );

    if (missing.length > 0 || extra.length > 0) {
      result.columnChanges = true;
      result.warnings.push(
        `Template structure changed. Missing: ${missing.join(", ") || "none"}. New: ${extra.join(", ") || "none"}`,
      );
    }

    result.extraColumns = extra;
    if (missing.length > 0) {
      result.missingColumns.push(
        ...missing.filter((m) => !result.missingColumns.includes(m)),
      );
    }
  }

  return result;
}

export async function logSystemError(
  dataSourceId: string,
  errorType: string,
  errorMessage: string,
  errorDetails?: any,
  severity: string = "error",
): Promise<void> {
  try {
    await storage.createSystemError({
      dataSourceId,
      errorType,
      errorMessage,
      errorDetails,
      severity,
    });
  } catch (err) {
    console.error("Failed to log system error:", err);
  }
}

export async function processEmailAttachment(
  dataSourceId: string,
  buffer: Buffer,
  filename: string,
  forceStage?: boolean,
): Promise<{
  success: boolean;
  rowCount: number;
  staged?: boolean;
  error?: string;
}> {
  // Import the immediate alert function lazily to avoid circular dependencies
  const { sendImmediateImportAlert } = await import("./errorReporter");

  // Signal that import has started (for sync coordination)
  startImport(dataSourceId);

  try {
    const dataSource = await storage.getDataSource(dataSourceId);
    if (!dataSource) {
      failImport(dataSourceId, "Data source not found");
      return { success: false, rowCount: 0, error: "Data source not found" };
    }

    // SAFETY NET: Run centralized validation first (catches file corruption, row count drops, missing columns)
    const validationConfig = (dataSource as any).importValidationConfig || {};
    if (validationConfig.enabled !== false) {
      console.log(
        `[Email Import] Validating file "${filename}" for data source "${dataSource.name}"`,
      );
      const centralValidation = await validateImportFile(
        buffer,
        dataSourceId,
        filename,
      );

      if (!centralValidation.valid) {
        // Log the validation failure
        await logValidationFailure(
          dataSourceId,
          filename,
          centralValidation.errors,
          centralValidation.warnings,
        );

        console.error(
          `[Email Import] SAFETY BLOCK: File "${filename}" failed validation:`,
          centralValidation.errors,
        );
        failImport(
          dataSourceId,
          `Validation failed: ${centralValidation.errors.join("; ")}`,
        );
        return {
          success: false,
          rowCount: 0,
          error: `SAFETY NET: Import blocked - ${centralValidation.errors.join("; ")}`,
        };
      }

      if (centralValidation.warnings.length > 0) {
        console.log(
          `[Email Import] Validation warnings for "${filename}":`,
          centralValidation.warnings,
        );
      }
    }

    const columnMapping = (dataSource.columnMapping || {}) as any;
    const cleaningConfig = (dataSource.cleaningConfig || {}) as any;
    // FIX: Also stage files when forceStage is true (multiple link/attachment files detected)
    const isMultiFile = forceStage || (dataSource as any).ingestionMode === "multi";
    const pivotConfig = (dataSource as any).pivotConfig;

    const lastFile = await storage.getLatestFile(dataSourceId);
    const expectedHeaders = lastFile?.headers || [];
    const lastRowCount = lastFile?.rowCount || 0;

    // Use pivoted parser if pivotConfig is enabled (like Tarik Ediz)
    let headers: string[];
    let rows: any[][];
    let items: any[];

    // AUTO-DETECT OTS FORMAT: Check if file has ots1, ots2, etc. columns
    // This allows OTS files to be imported without manual configuration
    const isOTS = pivotConfig?.format === "ots_format" || isOTSFormat(buffer);

    // Sherri Hill format
    if (pivotConfig?.format === "sherri_hill") {
      console.log(
        `[Email Import] Using Sherri Hill specific parser for ${dataSource.name}`,
      );
      const result = parseSherriHillFormat(buffer, cleaningConfig);
      if (result) {
        headers = result.headers;
        rows = result.rows;
        items = result.items;
      } else {
        console.log(
          `[Email Import] Sherri Hill parser returned null, falling back to generic`,
        );
        const fallback = parseExcelToInventory(
          buffer,
          columnMapping,
          cleaningConfig,
        );
        headers = fallback.headers;
        rows = fallback.rows;
        items = fallback.items;
      }
    } else if (pivotConfig?.format === "jovani") {
      console.log(
        `[Email Import] Using Jovani specific parser for ${dataSource.name}`,
      );
      const jovaniConfig = {
        ...cleaningConfig,
        pivotedFormat: { vendor: "jovani" },
      };
      const result = parseJovaniFormat(buffer, jovaniConfig);
      if (result) {
        headers = result.headers;
        rows = result.rows;
        items = result.items;
      } else {
        console.log(
          `[Email Import] Jovani parser returned null, falling back to generic`,
        );
        const fallback = parseExcelToInventory(
          buffer,
          columnMapping,
          cleaningConfig,
        );
        headers = fallback.headers;
        rows = fallback.rows;
        items = fallback.items;
      }
    } else if (pivotConfig?.format === "tarik_ediz") {
      console.log(
        `[Email Import] Using Tarik Ediz specific parser for ${dataSource.name}`,
      );
      const result = parseTarikEdizFormat(buffer);
      if (result) {
        result.items = result.items.map((item: any) => ({
          ...item,
          style: applyCleaningToValue(
            String(item.style || ""),
            cleaningConfig,
            "style",
          ),
        }));
        headers = result.headers;
        rows = result.rows;
        items = result.items;
      } else {
        const fallback = parseExcelToInventory(
          buffer,
          columnMapping,
          cleaningConfig,
        );
        headers = fallback.headers;
        rows = fallback.rows;
        items = fallback.items;
      }
    } else if (pivotConfig?.format === "feriani_gia") {
      console.log(
        `[Email Import] Using Feriani/GIA specific parser for ${dataSource.name}`,
      );
      const result = parseFerianiGiaFormat(buffer, cleaningConfig);
      if (result) {
        headers = result.headers;
        rows = result.rows;
        items = result.items;
      } else {
        console.log(
          `[Email Import] Feriani/GIA parser returned null, falling back to generic`,
        );
        const fallback = parseExcelToInventory(
          buffer,
          columnMapping,
          cleaningConfig,
        );
        headers = fallback.headers;
        rows = fallback.rows;
        items = fallback.items;
      }
    } else if (isOTS) {
      console.log(
        `[Email Import] Using OTS FORMAT parser for ${dataSource.name}`,
      );
      const result = parseOTSFormat(
        buffer,
        cleaningConfig,
        dataSource.name,
        (dataSource as any).stockValueConfig,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
      console.log(
        `[Email Import] OTS format parse complete: ${items.length} items extracted`,
      );
    } else if (pivotConfig?.format === "generic_pivot") {
      // CRITICAL FIX: Check for generic_pivot format (INESS, Colette, Alyce, etc.)
      console.log(
        `[Email Import] Using GENERIC PIVOT parser for ${dataSource.name}`,
      );
      const result = parseGenericPivotFormat(
        buffer,
        cleaningConfig,
        dataSource.name,
        (dataSource as any).discontinuedConfig,
        (dataSource as any).stockValueConfig,
        (dataSource as any).sizeLimitConfig,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
      if (result.sizeFiltered && result.sizeFiltered > 0) {
        console.log(
          `[Email Import] Size limits filtered ${result.sizeFiltered} items during parsing`,
        );
      }
      console.log(
        `[Email Import] Generic pivot parse complete: ${items.length} items extracted`,
      );
    } else if (pivotConfig?.format === "grn_invoice") {
      console.log(
        `[Email Import] Using GRN-INVOICE parser for ${dataSource.name}`,
      );
      const result = parseGRNInvoiceFormat(
        buffer,
        cleaningConfig,
        dataSource.name,
        (dataSource as any).stockValueConfig,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
      console.log(
        `[Email Import] GRN-INVOICE parse complete: ${items.length} items extracted`,
      );
    } else if (pivotConfig?.format === "pr_date_headers") {
      console.log(
        `[Email Import] Using PR DATE HEADERS parser for ${dataSource.name}`,
      );
      const result = parsePRDateHeaderFormat(
        buffer,
        cleaningConfig,
        dataSource.name,
        (dataSource as any).stockValueConfig,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
      console.log(
        `[Email Import] PR date headers parse complete: ${items.length} items extracted`,
      );
    } else if (pivotConfig?.format === "store_multibrand") {
      console.log(
        `[Email Import] Using STORE MULTIBRAND parser for ${dataSource.name}`,
      );
      const result = parseStoreMultibrandFormat(
        buffer,
        cleaningConfig,
        dataSource.name,
        (dataSource as any).stockValueConfig,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
      console.log(
        `[Email Import] Store multibrand parse complete: ${items.length} items extracted`,
      );
    } else if (pivotConfig?.enabled) {
      console.log(
        `[Email Import] Using pivoted table parser for ${dataSource.name}`,
      );
      const result = parsePivotedExcelToInventory(
        buffer,
        pivotConfig,
        cleaningConfig,
        dataSource.name,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
      // Pivoted sources produce standardized headers (style, color, size, stock, shipDate)
      // Skip traditional columnMapping validation since it doesn't apply
      console.log(
        `[Email Import] Pivoted parse complete: ${items.length} items extracted`,
      );
    } else {
      const result = parseExcelToInventory(
        buffer,
        columnMapping,
        cleaningConfig,
      );
      headers = result.headers;
      rows = result.rows;
      items = result.items;
    }

    // Helper function to send immediate alert if enabled (wrapped to not reject import promise)
    const sendAlertIfEnabled = async (
      errorType:
        | "validation_failed"
        | "format_changed"
        | "row_count_error"
        | "column_missing",
      errorMessage: string,
      details: any,
    ) => {
      if (validationConfig.sendImmediateAlert !== false) {
        try {
          await sendImmediateImportAlert({
            dataSourceName: dataSource.name,
            filename,
            errorType,
            errorMessage,
            details,
          });
        } catch (alertError: any) {
          console.error(
            `[ImportUtils] Failed to send immediate alert: ${alertError.message}`,
          );
          // Continue - don't let SMTP failures block the import error response
        }
      }
    };

    // Skip columnMapping-based validation for pivoted sources (they use standardized output)
    if (!pivotConfig?.enabled) {
      const validation = validateTemplate(
        headers,
        columnMapping,
        expectedHeaders as string[],
      );

      // Check requireAllColumns - strict enforcement when enabled
      if (validationConfig.requireAllColumns) {
        const requiredMappings = ["sku", "style", "size", "color", "stock"];
        const missingRequired: string[] = [];

        for (const field of requiredMappings) {
          const mappedColumn = columnMapping?.[field];
          if (mappedColumn) {
            const found = headers.find(
              (h) => h.toLowerCase() === mappedColumn.toLowerCase(),
            );
            if (!found) {
              missingRequired.push(`${field} (mapped to "${mappedColumn}")`);
            }
          } else {
            // If requireAllColumns is true, ALL required fields must be mapped
            missingRequired.push(`${field} (not mapped)`);
          }
        }

        if (missingRequired.length > 0) {
          const errorMessage = `Required columns missing or not mapped: ${missingRequired.join(", ")}`;
          await logSystemError(
            dataSourceId,
            "column_missing",
            errorMessage,
            {
              missingColumns: missingRequired,
              foundColumns: headers,
              columnMapping,
            },
            "error",
          );

          await sendAlertIfEnabled("column_missing", errorMessage, {
            missingColumns: missingRequired,
            foundColumns: headers,
          });

          return { success: false, rowCount: 0, error: errorMessage };
        }
      }

      if (!validation.valid) {
        const errorMessage = `Template validation failed for ${filename}: ${validation.errors.join("; ")}`;
        await logSystemError(
          dataSourceId,
          "template_error",
          errorMessage,
          {
            expectedColumns: expectedHeaders,
            foundColumns: headers,
            parseErrors: validation.errors,
          },
          "error",
        );

        await sendAlertIfEnabled("column_missing", errorMessage, {
          missingColumns: validation.missingColumns,
          foundColumns: headers,
          expectedColumns: expectedHeaders,
        });

        return {
          success: false,
          rowCount: 0,
          error: `Template error: ${validation.errors.join("; ")}`,
        };
      }

      if (validation.columnChanges) {
        await logSystemError(
          dataSourceId,
          "template_changed",
          `Template structure changed for ${filename}: ${validation.warnings.join("; ")}`,
          {
            expectedColumns: expectedHeaders,
            foundColumns: headers,
          },
          "warning",
        );
      }
    }

    if (items.length === 0) {
      const errorMessage = `No valid items found in file ${filename}`;
      await logSystemError(
        dataSourceId,
        "parse_error",
        errorMessage,
        { foundColumns: headers },
        "error",
      );

      await sendAlertIfEnabled("validation_failed", errorMessage, {
        expectedRowCount: lastRowCount,
        actualRowCount: 0,
        foundColumns: headers,
      });

      return {
        success: false,
        rowCount: 0,
        error: "No valid items found in file",
      };
    }

    // Row count validation (if configured)
    if (validationConfig.enabled !== false) {
      const actualRowCount = items.length;

      // Check minimum row count
      if (
        validationConfig.minRowCount &&
        actualRowCount < validationConfig.minRowCount
      ) {
        const errorMessage = `File has too few items (${actualRowCount}) - minimum expected is ${validationConfig.minRowCount}`;
        await logSystemError(
          dataSourceId,
          "row_count_error",
          errorMessage,
          {
            expectedMin: validationConfig.minRowCount,
            actualRowCount,
          },
          "error",
        );

        await sendAlertIfEnabled("row_count_error", errorMessage, {
          expectedRowCount: validationConfig.minRowCount,
          actualRowCount,
        });

        return { success: false, rowCount: 0, error: errorMessage };
      }

      // Check maximum row count
      if (
        validationConfig.maxRowCount &&
        actualRowCount > validationConfig.maxRowCount
      ) {
        const errorMessage = `File has too many items (${actualRowCount}) - maximum expected is ${validationConfig.maxRowCount}`;
        await logSystemError(
          dataSourceId,
          "row_count_error",
          errorMessage,
          {
            expectedMax: validationConfig.maxRowCount,
            actualRowCount,
          },
          "error",
        );

        await sendAlertIfEnabled("row_count_error", errorMessage, {
          expectedRowCount: validationConfig.maxRowCount,
          actualRowCount,
        });

        return { success: false, rowCount: 0, error: errorMessage };
      }

      // Check row count tolerance (if we have previous file data)
      // SKIP for multi-file sources: individual files are always smaller than combined total
      if (
        validationConfig.rowCountTolerance &&
        lastRowCount > 0 &&
        !isMultiFile
      ) {
        const tolerance = validationConfig.rowCountTolerance / 100;
        const minAllowed = Math.floor(lastRowCount * (1 - tolerance));
        // Only block on DECREASE, not increase - more products is expected/good
        if (actualRowCount < minAllowed) {
          const errorMessage = `Row count dropped significantly: ${lastRowCount} -> ${actualRowCount} (tolerance: -${validationConfig.rowCountTolerance}%)`;
          await logSystemError(
            dataSourceId,
            "row_count_error",
            errorMessage,
            {
              previousRowCount: lastRowCount,
              actualRowCount,
              tolerance: validationConfig.rowCountTolerance,
              minAllowed,
            },
            "error",
          );

          await sendAlertIfEnabled("row_count_error", errorMessage, {
            expectedRowCount: lastRowCount,
            actualRowCount,
            tolerance: validationConfig.rowCountTolerance,
          });

          return { success: false, rowCount: 0, error: errorMessage };
        }
      }

      // DEFAULT SAFETY CHECK: Block if row count drops by more than 90% from last import
      // This catches completely corrupted files even without explicit tolerance config
      // SKIP for multi-file sources: individual files are always smaller than combined total
      if (
        lastRowCount > 100 &&
        actualRowCount < lastRowCount * 0.1 &&
        !isMultiFile
      ) {
        const errorMessage = `SAFETY BLOCK: File appears corrupted - row count dropped from ${lastRowCount} to ${actualRowCount} (>90% decrease). Import blocked to protect your data.`;
        await logSystemError(
          dataSourceId,
          "row_count_error",
          errorMessage,
          {
            previousRowCount: lastRowCount,
            actualRowCount,
            safetyBlock: true,
          },
          "critical",
        );

        await sendAlertIfEnabled("row_count_error", errorMessage, {
          expectedRowCount: lastRowCount,
          actualRowCount,
          safetyBlock: true,
        });

        return { success: false, rowCount: 0, error: errorMessage };
      }
    }

    // For multi-file mode, stage the file instead of importing immediately
    if (isMultiFile) {
      await storage.createUploadedFile({
        dataSourceId,
        fileName: filename,
        fileSize: buffer.length,
        rowCount: rows.length,
        previewData: rows as any,
        headers,
        fileStatus: "staged",
      } as any);

      return { success: true, rowCount: rows.length, staged: true };
    }

    // Single file mode - import immediately (matching manual upload order exactly)
    // Check update strategy
    const updateStrategy = (dataSource as any).updateStrategy || "full_sync";

    // Helper function to get prefix for a style (matching manual upload)
    // For sale files, strips "Sale" or "Sales" from the prefix to match regular file naming
    const getStylePrefix = (style: string): string => {
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

      // For sale files, strip "Sale" or "Sales" suffix from the prefix
      // This ensures "Jovani Sale 12345" becomes "Jovani 12345" (matching the regular file)
      if ((dataSource as any).sourceType === "sales") {
        const saleMatch = prefix.match(/^(.+?)\s*(Sale|Sales)$/i);
        if (saleMatch) {
          prefix = saleMatch[1].trim();
        }
      }

      return prefix;
    };

    // Helper to convert color to Title Case: "PURPLE" => "Purple", "ICE PINK" => "Ice Pink", "ICE-BLUE" => "Ice-Blue"
    const toTitleCase = (str: string): string => {
      return str
        .toLowerCase()
        .replace(/(?:^|[\s\-\/&])\S/g, (a) => a.toUpperCase());
    };

    // Step 0: Apply skip rule filtering (before processing)
    // Items with shouldSkip=true are filtered out, unless skipUnlessContinueSelling is true AND continueSelling is enabled
    const continueSelling = (dataSource as any).continueSelling ?? true; // Default to true
    let filteredItems = items.filter((item) => {
      if (item.shouldSkip) {
        // If skipUnlessContinueSelling is true, only skip if continueSelling is disabled
        if (item.skipUnlessContinueSelling && continueSelling) {
          return true; // Don't skip - continue selling is enabled
        }
        return false; // Skip this item
      }
      return true; // Keep this item
    });

    if (filteredItems.length < items.length) {
      console.log(
        `[Email Import] Filtered out ${items.length - filteredItems.length} items based on skip rule`,
      );
    }

    // Step 0.5: Filter out discontinued items that have ZERO stock
    // "D" means discontinued - but if there's still stock, we should import it to sell remaining inventory
    // Only filter out: discontinued === true AND stock === 0
    // CRITICAL FIX: Respect hasFutureStock/preserveZeroStock/shipDate flags (matching manual upload behavior)
    const discontinuedZeroStock = filteredItems.filter(
      (item) => item.discontinued === true && item.stock === 0,
    );
    if (discontinuedZeroStock.length > 0) {
      const beforeFilter = filteredItems.length;
      filteredItems = filteredItems.filter((item) => {
        // If not discontinued or has stock, keep it
        if (!(item.discontinued === true && item.stock === 0)) {
          return true;
        }
        // Item is discontinued with zero stock - check if we should preserve due to future stock
        // This matches the logic in inventoryProcessing.ts applyImportRules Step 10
        if (item.hasFutureStock || item.preserveZeroStock || item.shipDate) {
          return true; // Preserve - has future stock coming
        }
        return false; // Filter out - discontinued with zero stock and no future stock
      });
      const actuallyFiltered = beforeFilter - filteredItems.length;
      if (actuallyFiltered > 0) {
        console.log(
          `[Email Import] Filtering out ${actuallyFiltered} discontinued (D) items with zero stock (preserved ${discontinuedZeroStock.length - actuallyFiltered} with future stock)`,
        );
      }
    }

    // Step 1: Apply prefix to style AND sku BEFORE cleaning (matching manual upload order)
    const inventoryItems = filteredItems.map((item) => {
      const prefix = item.style ? getStylePrefix(item.style) : dataSource.name;
      const prefixedStyle = item.style ? `${prefix} ${item.style}` : item.style;
      // Normalize color to Title Case for SKU: "PURPLE" => "Purple"
      const normalizedColor = item.color ? toTitleCase(item.color) : item.color;
      // Construct SKU from prefixed style: "Tarik Ediz 10001" + "Purple" + "6" => "Tarik-Ediz-10001-Purple-6"
      // Convert slashes and spaces to hyphens for Shopify compatibility
      // CRITICAL FIX: Use explicit null check for size to handle size "0" correctly (matching manual upload)
      const prefixedSku =
        prefixedStyle &&
        normalizedColor &&
        item.size != null &&
        item.size !== ""
          ? `${prefixedStyle}-${normalizedColor}-${item.size}`
              .replace(/\//g, "-")
              .replace(/\s+/g, "-")
              .replace(/-+/g, "-")
          : (item.sku || "").replace(/\//g, "-").replace(/-+/g, "-"); // Also normalize fallback SKU
      return {
        dataSourceId,
        sku: prefixedSku,
        style: prefixedStyle,
        size: item.size,
        color: item.color,
        stock: item.stock,
        cost: item.cost,
        price: item.price,
        shipDate: item.shipDate,
        // CRITICAL FIX: Preserve hasFutureStock flags from parser (matching manual upload)
        hasFutureStock: item.hasFutureStock || false,
        preserveZeroStock: item.preserveZeroStock || false,
        discontinued: item.discontinued || false,
        rawData: item.rawData,
      };
    });

    // Step 2: Clean data (remove items without size, fix colors, remove duplicates)
    const cleanResult = await cleanInventoryData(
      inventoryItems,
      dataSource.name,
    );

    // Step 2.5: Apply configurable import rules (pricing, discontinued, required fields, etc.)
    const importRulesConfig = {
      // CRITICAL FIX: Add discontinuedConfig fallback (matching other import methods)
      discontinuedRules:
        (dataSource as any).discontinuedConfig ||
        (dataSource as any).discontinuedRules,
      salePriceConfig: (dataSource as any).salePriceConfig,
      priceFloorCeiling: (dataSource as any).priceFloorCeiling,
      minStockThreshold: (dataSource as any).minStockThreshold,
      stockThresholdEnabled: (dataSource as any).stockThresholdEnabled,
      requiredFieldsConfig: (dataSource as any).requiredFieldsConfig,
      dateFormatConfig: (dataSource as any).dateFormatConfig,
      valueReplacementRules: (dataSource as any).valueReplacementRules,
      regularPriceConfig: (dataSource as any).regularPriceConfig,
      cleaningConfig: dataSource.cleaningConfig,
      futureStockConfig: (dataSource as any).futureStockConfig,
      // CRITICAL FIX: Add stockValueConfig (was missing - text-to-number mappings)
      stockValueConfig: (dataSource as any).stockValueConfig,
      // CRITICAL FIX: Add complexStockConfig (was missing - pattern-based stock parsing)
      complexStockConfig: (dataSource as any).complexStockConfig,
    };
    const importRulesResult = await applyImportRules(
      cleanResult.items,
      importRulesConfig,
      rows,
    );
    if (
      importRulesResult.stats.discontinuedFiltered > 0 ||
      importRulesResult.stats.salePricingApplied > 0
    ) {
      console.log(
        `[Email Import] Import rules applied: ${importRulesResult.stats.removedCount} items removed, ${importRulesResult.stats.priceAdjustedCount} prices adjusted`,
      );
    }

    // Step 3: Apply variant rules (filter zero stock, expand sizes, etc.)
    const ruleResult = await applyVariantRules(
      importRulesResult.items,
      dataSourceId,
    );

    // Step 3.5: Apply price-based size expansion if configured (matching manual upload)
    let itemsAfterExpansion = ruleResult.items;
    let priceBasedExpansionCount = 0;
    const priceBasedExpansionConfig = (dataSource as any)
      .priceBasedExpansionConfig;
    const sizeLimitConfig = (dataSource as any).sizeLimitConfig;

    if (
      priceBasedExpansionConfig?.enabled &&
      (priceBasedExpansionConfig.tiers?.length > 0 ||
        (priceBasedExpansionConfig.defaultExpandDown ?? 0) > 0 ||
        (priceBasedExpansionConfig.defaultExpandUp ?? 0) > 0)
    ) {
      const shopifyStoreId = (dataSource as any).shopifyStoreId;
      if (shopifyStoreId) {
        console.log(
          `[Email Import] Applying price-based size expansion for "${dataSource.name}"...`,
        );
        try {
          // Get cached variant prices from Shopify
          const cacheVariants =
            await storage.getVariantCacheProductStyles(shopifyStoreId);
          const stylePriceMap = buildStylePriceMapFromCache(cacheVariants);
          console.log(
            `[Email Import] Built style price map with ${stylePriceMap.size} styles`,
          );

          // Apply price-based expansion
          const expansionResult = applyPriceBasedExpansion(
            ruleResult.items,
            priceBasedExpansionConfig,
            stylePriceMap,
            sizeLimitConfig,
          );
          itemsAfterExpansion = expansionResult.items;
          priceBasedExpansionCount = expansionResult.addedCount;

          if (priceBasedExpansionCount > 0) {
            console.log(
              `[Email Import] Price-based expansion added ${priceBasedExpansionCount} size variants`,
            );
          }
        } catch (expansionError) {
          console.error(
            `[Email Import] Price-based expansion error:`,
            expansionError,
          );
          // Continue without expansion if there's an error
        }
      } else {
        console.log(
          `[Email Import] Price-based expansion enabled but no Shopify store linked - skipping`,
        );
      }
    }

    // Step 3.75: Calculate stockInfo for each item (matching other import methods)
    const stockInfoRule = await getStockInfoRuleForEmail(dataSource, storage);
    if (stockInfoRule) {
      console.log(
        `[Email Import] Calculating stockInfo for ${itemsAfterExpansion.length} items using rule: "${stockInfoRule.name}"`,
      );
      itemsAfterExpansion = itemsAfterExpansion.map((item: any) => ({
        ...item,
        stockInfo: calculateItemStockInfo(item, stockInfoRule),
      }));
      const itemsWithStockInfo = itemsAfterExpansion.filter(
        (i: any) => i.stockInfo,
      ).length;
      console.log(
        `[Email Import] stockInfo calculated: ${itemsWithStockInfo}/${itemsAfterExpansion.length} items have messages`,
      );
    }

    // Step 4: Handle discontinued styles based on source type
    const isSaleFile = (dataSource as any).sourceType === "sales";
    const linkedSaleDataSourceId = (dataSource as any).assignedSaleDataSourceId;
    let itemsToImport = itemsAfterExpansion;

    if (isSaleFile) {
      // Sale file: Register all unique styles in discontinued_styles table
      const regResult = await registerSaleFileStyles(
        dataSourceId,
        itemsAfterExpansion,
      );
      console.log(
        `[Email Import] Sale file "${dataSource.name}" - registered ${regResult.total} styles (${regResult.added} new, ${regResult.updated} updated)`,
      );
    } else if (linkedSaleDataSourceId) {
      // Regular file with linked sale file: Filter out discontinued styles
      console.log(
        `[Email Import] Regular file "${dataSource.name}" - checking for discontinued styles from sale file`,
      );

      // First, remove any existing inventory items that have discontinued styles
      const discontinuedItemsRemoved = await removeDiscontinuedInventoryItems(
        dataSourceId,
        linkedSaleDataSourceId,
      );
      if (discontinuedItemsRemoved > 0) {
        console.log(
          `[Email Import] Removed ${discontinuedItemsRemoved} existing inventory items with discontinued styles`,
        );
      }

      // Then, filter out items from this import that have discontinued styles
      const filterResult = await filterDiscontinuedStyles(
        dataSourceId,
        itemsAfterExpansion, // BUG FIX: Use itemsAfterExpansion instead of ruleResult.items
        linkedSaleDataSourceId,
      );
      itemsToImport = filterResult.items;

      if (filterResult.removedCount > 0) {
        console.log(
          `[Email Import] Filtered out ${filterResult.removedCount} items with ${filterResult.discontinuedStyles.length} discontinued styles: ${filterResult.discontinuedStyles.slice(0, 3).join(", ")}${filterResult.discontinuedStyles.length > 3 ? "..." : ""}`,
        );
      }
    }

    // ========== SHOPIFY PRICE LOOKUP FOR COMPARE-AT (SALE FILES) ==========
    // For sale files, look up Shopify's existing price to use as compare-at (strike-through)
    const shopifyStoreId = (dataSource as any).shopifyStoreId;
    const salesConfig = (dataSource as any).salesConfig || {
      priceMultiplier: 2,
      useCompareAtPrice: true,
    };
    const priceMultiplier = salesConfig.priceMultiplier || 2;
    const useCompareAtPrice = salesConfig.useCompareAtPrice ?? true;

    let shopifyPriceMap = new Map<string, string>(); // SKU → Shopify price
    let shopifyPricesLoaded = 0;

    if (shopifyStoreId && useCompareAtPrice && isSaleFile) {
      try {
        // Collect all SKUs from items
        const skus = itemsToImport
          .map((item: any) => item.sku)
          .filter((sku: string | null) => sku && sku.trim());

        if (skus.length > 0) {
          console.log(
            `[Email Import] Looking up Shopify prices for ${skus.length} SKUs (sale file compare-at)...`,
          );
          const cachedVariants = await storage.getVariantCacheBySKUs(
            shopifyStoreId,
            skus,
          );

          for (const v of cachedVariants) {
            if (v.sku && v.price) {
              const normalizedSku = v.sku.trim().toUpperCase();
              shopifyPriceMap.set(normalizedSku, v.price);
            }
          }
          shopifyPricesLoaded = shopifyPriceMap.size;
          console.log(
            `[Email Import] Loaded ${shopifyPricesLoaded} Shopify prices for compare-at`,
          );
        }
      } catch (err) {
        console.error(
          "[Email Import] Error loading Shopify prices for compare-at:",
          err,
        );
      }
    }

    // Apply sale pricing: multiply price, set Shopify price as cost (for compare-at)
    if (isSaleFile && shopifyPricesLoaded > 0) {
      itemsToImport = itemsToImport.map((item: any) => {
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

        return {
          ...item,
          price: finalPrice,
          cost: cost,
        };
      });
      console.log(
        `[Email Import] Applied sale pricing: ${priceMultiplier}x multiplier, ${shopifyPricesLoaded} compare-at prices set`,
      );
    }

    let importedCount = 0;
    let addedCount = 0;
    let updatedCount = 0;

    if (itemsToImport.length > 0) {
      if (updateStrategy === "full_sync") {
        // Full Sync: Atomic delete + insert to guarantee no stale items remain
        console.log(
          `[Email Import Full Sync] ${dataSource.name}: Starting atomic replace with ${itemsToImport.length} items`,
        );
        const result = await storage.atomicReplaceInventoryItems(
          dataSourceId,
          itemsToImport as any,
        );
        importedCount = result.created;
        console.log(
          `[Email Import Full Sync] ${dataSource.name}: Atomic replace complete - deleted ${result.deleted}, created ${result.created} items`,
        );
      } else {
        // Replace (Create & Update): Only add/update items, keep items not in file
        console.log(
          `[Email Import Upsert] ${dataSource.name}: Upserting ${itemsToImport.length} items (existing items NOT deleted)`,
        );
        // Reset sale flags for regular inventory sources to clear stale consolidation state
        const isRegularInventory = (dataSource as any).sourceType !== "sales";
        const result = await storage.upsertInventoryItems(
          itemsToImport as any,
          dataSourceId,
          { resetSaleFlags: isRegularInventory },
        );
        addedCount = result.added;
        updatedCount = result.updated;
        importedCount = addedCount + updatedCount;
      }
    } else if (updateStrategy === "full_sync") {
      // Full sync with no items means delete everything (matching manual upload)
      console.log(
        `[Email Import Full Sync] ${dataSource.name}: No items to import, deleting all existing items`,
      );
      await storage.deleteInventoryItemsByDataSource(dataSourceId);
    }

    // Update data source last sync time
    await storage.updateDataSource(dataSourceId, {});

    await storage.createUploadedFile({
      dataSourceId,
      fileName: filename,
      fileSize: buffer.length,
      rowCount: ruleResult.items.length,
      previewData: rows.slice(0, 10) as any,
      headers,
      fileStatus: "imported",
    } as any);

    // Signal import completion (for sync coordination)
    completeImport(dataSourceId, ruleResult.items.length);
    return { success: true, rowCount: ruleResult.items.length };
  } catch (err: any) {
    console.error("Error processing email attachment:", err);
    // Signal import failure (for sync coordination)
    failImport(dataSourceId, err.message);
    await logSystemError(
      dataSourceId,
      "parse_error",
      `Failed to process file ${filename}: ${err.message}`,
      { parseErrors: [err.message] },
      "error",
    );
    return { success: false, rowCount: 0, error: err.message };
  }
}

export async function combineAndImportStagedFiles(
  dataSourceId: string,
): Promise<{ success: boolean; rowCount: number; error?: string }> {
  // Signal that import has started (for sync coordination)
  startImport(dataSourceId);

  try {
    const dataSource = await storage.getDataSource(dataSourceId);
    if (!dataSource) {
      failImport(dataSourceId, "Data source not found");
      return { success: false, rowCount: 0, error: "Data source not found" };
    }

    const stagedFiles = await storage.getStagedFiles(dataSourceId);
    if (stagedFiles.length === 0) {
      // Not a failure, just nothing to do - still signal completion
      completeImport(dataSourceId, 0);
      return {
        success: false,
        rowCount: 0,
        error: "No staged files to combine",
      };
    }

    // FIX: Process ALL staged files regardless of date
    // Previously only today's files were processed, orphaning files from previous days
    const filesToProcess = stagedFiles;

    let columnMapping = (dataSource.columnMapping || {}) as any;
    const cleaningConfig = (dataSource.cleaningConfig || {}) as any;

    const cLog = (msg: string) => {
      const line = `[${new Date().toISOString()}] ${msg}\n`;
      try { fs.appendFileSync("/tmp/email_download.log", line); } catch {}
    };

    // Auto-detect standard column names when columnMapping is empty
    // This handles pre-parsed files (e.g. from pivot parser) where headers
    // already contain standard names like sku, style, color, size, stock
    if (!columnMapping || Object.keys(columnMapping).length === 0) {
      const firstFile = filesToProcess[0];
      const firstHeaders = (firstFile?.headers as string[]) || [];
      const lowerHeaders = firstHeaders.map((h: string) => (h || "").toLowerCase().trim());

      const standardFields: Record<string, string[]> = {
        sku: ["sku"],
        style: ["style"],
        color: ["color"],
        size: ["size"],
        stock: ["stock", "quantity", "qty"],
        cost: ["cost"],
        price: ["price"],
        shipDate: ["shipdate", "ship date", "ship_date"],
        futureStock: ["futurestock", "future stock", "future_stock"],
        futureDate: ["futuredate", "future date", "future_date"],
      };

      const autoMapping: any = {};
      for (const [field, aliases] of Object.entries(standardFields)) {
        for (const alias of aliases) {
          const idx = lowerHeaders.indexOf(alias);
          if (idx >= 0) {
            autoMapping[field] = firstHeaders[idx];
            break;
          }
        }
      }

      if (Object.keys(autoMapping).length > 0) {
        columnMapping = autoMapping;
        cLog(`[Combine] columnMapping was empty - auto-detected from headers: ${JSON.stringify(columnMapping)}`);
      }
    }

    cLog(`[Combine] columnMapping: ${JSON.stringify(columnMapping)}`);
    cLog(`[Combine] Staged files to process: ${filesToProcess.length}`);

    let allItems: any[] = [];
    let allRows: any[][] = []; // Accumulate all rows for applyImportRules rawDataRows parameter

    for (const file of filesToProcess) {
      const previewData = file.previewData as any[];
      const headers = file.headers as string[];
      cLog(`[Combine] File: ${(file as any).originalFilename || 'unknown'}, headers: ${JSON.stringify(headers?.slice(0, 10))}, rows: ${previewData?.length || 0}`);

      if (!previewData || !headers) continue;
      // Use concat instead of push(...) to avoid stack overflow with large arrays
      allRows = allRows.concat(previewData);

      // Build header index map for this file
      const headerIndexMap: Record<string, number> = {};
      headers.forEach((h: string, idx: number) => {
        if (h) headerIndexMap[h.toLowerCase().trim()] = idx;
      });

      // Check if this is a pre-parsed pivoted format file
      // Pivoted files have headers like ["style", "color", "size", "stock", "cost"]
      const isPivotedPreParsed =
        cleaningConfig?.pivotedFormat?.enabled &&
        headerIndexMap["style"] !== undefined &&
        headerIndexMap["size"] !== undefined;

      const items = previewData
        .map((row: any[]) => {
          if (!Array.isArray(row)) return null;

          const getColValue = (colName: string) => {
            if (!colName) return null;
            const colIndex = headers.findIndex(
              (h) => h && h.toString().toLowerCase() === colName.toLowerCase(),
            );
            return colIndex >= 0 ? row[colIndex] : null;
          };

          let sku: string;
          let style: string;
          let size: string;
          let color: string;
          let stockValue: any;
          let costValue: any;
          let priceValue: any;
          let shipDateValue: any;
          let futureStockValue: any;
          let futureDateValue: any;

          if (isPivotedPreParsed) {
            // Use direct column names from pivoted format parsing
            sku = String(getColValue("style") || "");
            style = String(getColValue("style") || "");
            size = String(getColValue("size") ?? "");
            color = String(getColValue("color") || "");
            stockValue = getColValue("stock");
            costValue = getColValue("cost");
            priceValue = getColValue("price");
            shipDateValue = getColValue("shipDate");
          } else {
            // Use column mapping
            sku = String(getColValue(columnMapping?.sku || "") || "");
            style = String(getColValue(columnMapping?.style || "") || "");
            size = String(getColValue(columnMapping?.size || "") ?? "");
            color = String(getColValue(columnMapping?.color || "") || "");
            stockValue = getColValue(columnMapping?.stock || "");
            costValue = getColValue(columnMapping?.cost || "");
            priceValue = getColValue(columnMapping?.price || "");
            shipDateValue = getColValue(columnMapping?.shipDate || "");
            futureStockValue = getColValue(columnMapping?.futureStock || "");
            futureDateValue = getColValue(columnMapping?.futureDate || "");
          }

          style = applyCleaningToValue(
            String(style || ""),
            cleaningConfig,
            "style",
          );
          // Normalize SKU - convert slashes to hyphens for Shopify compatibility
          sku = String(sku || "")
            .trim()
            .replace(/\//g, "-")
            .replace(/-+/g, "-");
          // FIX: Use ?? to preserve numeric 0 (valid size)
          size = String(size ?? "").trim();
          color = String(color || "").trim();

          if (!sku && style) sku = style;

          // Convert stock to number (with yes/no support)
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

          // Clean cost/price values
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
                }
              }
            }
          }

          // Parse future stock value
          let futureStock: number | null = null;
          if (futureStockValue !== undefined && futureStockValue !== null && futureStockValue !== "") {
            const parsed = parseFloat(String(futureStockValue).replace(/[^0-9.-]/g, ""));
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
                }
              }
            }
          }

          return {
            sku,
            style,
            size,
            color,
            stock,
            cost,
            price,
            shipDate,
            futureStock,
            futureDate,
            hasFutureStock: shipDate || futureDate ? true : false,
            preserveZeroStock: (shipDate || futureDate) && stock === 0 ? true : false,
          };
        })
        .filter((item: any) => item && item.sku);

      cLog(`[Combine] Items extracted from file: ${items.length} (before: allItems had ${allItems.length})`);
      if (items.length === 0 && previewData?.length > 0) {
        // Log a sample row to debug why no items passed the filter
        const sampleRow = previewData[0];
        cLog(`[Combine] Sample row[0]: ${JSON.stringify(sampleRow)}`);
        cLog(`[Combine] SKU mapping: columnMapping.sku="${columnMapping?.sku}", style="${columnMapping?.style}"`);
      } else if (items.length > 0) {
        cLog(`[Combine] Sample item[0]: ${JSON.stringify(items[0])}`);
      }
      // Use concat instead of push(...) to avoid stack overflow with large arrays
      allItems = allItems.concat(items);
    }

    cLog(`[Combine] Total allItems after all files: ${allItems.length}`);
    const cleanResult = await cleanInventoryData(allItems, dataSource.name);
    cLog(`[Combine] After cleanInventoryData: ${cleanResult.items?.length || 0} items`);

    // CRITICAL FIX: Apply prefix to style BEFORE applyVariantRules
    // This ensures prefix override patterns in sizeLimitConfig work correctly
    const getStylePrefixForCombine = (style: string): string => {
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

    // Apply prefix to items before import rules
    const prefixedItems = cleanResult.items.map((item: any) => {
      const prefix = item.style
        ? getStylePrefixForCombine(item.style)
        : dataSource.name;
      const prefixedStyle = item.style ? `${prefix} ${item.style}` : item.style;
      return {
        ...item,
        style: prefixedStyle,
      };
    });

    // Apply configurable import rules (pricing, discontinued, required fields, etc.)
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
      prefixedItems, // Use prefixed items
      importRulesConfig,
      allRows, // FIX: Pass accumulated rows for sale pricing rawData lookup
    );
    const ruleResult = await applyVariantRules(
      importRulesResult.items,
      dataSourceId,
    );

    // Check update strategy
    const updateStrategy = (dataSource as any).updateStrategy || "full_sync";

    // Helper function to get prefix for a style (for final SKU construction)
    // For sale files, strips "Sale" or "Sales" from the prefix to match regular file naming
    const getStylePrefix = (style: string): string => {
      if (
        cleaningConfig?.useCustomPrefixes &&
        cleaningConfig?.stylePrefixRules?.length > 0
      ) {
        for (const rule of cleaningConfig.stylePrefixRules) {
          if (
            rule.pattern &&
            rule.prefix &&
            style.toLowerCase().startsWith(rule.pattern.toLowerCase())
          ) {
            return rule.prefix;
          }
        }
      }

      let prefix = dataSource.name;

      // For sale files, strip "Sale" or "Sales" suffix from the prefix
      // This ensures "Jovani Sale 12345" becomes "Jovani 12345" (matching the regular file)
      if ((dataSource as any).sourceType === "sales") {
        const saleMatch = prefix.match(/^(.+?)\s*(Sale|Sales)$/i);
        if (saleMatch) {
          prefix = saleMatch[1].trim();
        }
      }

      return prefix;
    };

    // Helper to normalize style/size for SKU: trim, collapse whitespace, replace spaces/slashes with dashes
    const normalizeForSku = (val: string): string => {
      return val
        .trim()
        .replace(/\s+/g, " ")
        .replace(/ /g, "-")
        .replace(/\//g, "-")
        .replace(/-+/g, "-");
    };

    // Helper for color in SKU: convert spaces and slashes to hyphens for Shopify compliance
    const normalizeColorForSku = (val: string): string => {
      return val
        .trim()
        .replace(/\s+/g, "-")
        .replace(/\//g, "-")
        .replace(/-+/g, "-");
    };

    const itemsToCreate = ruleResult.items
      .map((item) => {
        const rawStyle = String(item.style || "")
          .replace(/\s+/g, " ")
          .trim();
        const prefix = getStylePrefix(rawStyle);
        const combinedStyle = rawStyle ? `${prefix} ${rawStyle}` : rawStyle;
        const finalStyle = combinedStyle.replace(/\s+/g, " ").trim();

        // Construct canonical SKU in Style-Color-Size lowercase format
        // Color preserves spaces (e.g., "Light Blue"), style and size use dashes
        const styleWithDashes = normalizeForSku(finalStyle);
        const colorPart = normalizeColorForSku(String(item.color || ""));
        const sizePart = normalizeForSku(String(item.size || ""));

        // Build canonical SKU if all parts present, otherwise use style as fallback
        let canonicalSku = "";
        if (styleWithDashes && colorPart && sizePart) {
          canonicalSku =
            `${styleWithDashes}-${colorPart}-${sizePart}`.toLowerCase();
        } else if (styleWithDashes) {
          // Fallback: use style-based SKU for incomplete items (preserves data)
          canonicalSku = styleWithDashes.toLowerCase();
        }

        return {
          dataSourceId,
          sku: canonicalSku || null,
          style: finalStyle,
          size: item.size,
          color: item.color,
          stock: item.stock,
          cost: item.cost,
          price: item.price,
        };
      })
      .filter((item) => item.sku); // Filter out items without any SKU

    // Handle discontinued styles based on source type
    const isSaleFile = (dataSource as any).sourceType === "sales";
    const linkedSaleDataSourceId = (dataSource as any).assignedSaleDataSourceId;
    let itemsToImport = itemsToCreate;

    if (isSaleFile) {
      // Sale file: Register all unique styles in discontinued_styles table
      const regResult = await registerSaleFileStyles(
        dataSourceId,
        itemsToCreate,
      );
      console.log(
        `[Combine Staged] Sale file "${dataSource.name}" - registered ${regResult.total} styles (${regResult.added} new, ${regResult.updated} updated)`,
      );
    } else if (linkedSaleDataSourceId) {
      // Regular file with linked sale file: Filter out discontinued styles
      console.log(
        `[Combine Staged] Regular file "${dataSource.name}" - checking for discontinued styles from sale file`,
      );

      // First, remove any existing inventory items that have discontinued styles
      const discontinuedItemsRemoved = await removeDiscontinuedInventoryItems(
        dataSourceId,
        linkedSaleDataSourceId,
      );
      if (discontinuedItemsRemoved > 0) {
        console.log(
          `[Combine Staged] Removed ${discontinuedItemsRemoved} existing inventory items with discontinued styles`,
        );
      }

      // Then, filter out items from this import that have discontinued styles
      const filterResult = await filterDiscontinuedStyles(
        dataSourceId,
        itemsToCreate,
        linkedSaleDataSourceId,
      );
      itemsToImport = filterResult.items;

      if (filterResult.removedCount > 0) {
        console.log(
          `[Combine Staged] Filtered out ${filterResult.removedCount} items with ${filterResult.discontinuedStyles.length} discontinued styles: ${filterResult.discontinuedStyles.slice(0, 3).join(", ")}${filterResult.discontinuedStyles.length > 3 ? "..." : ""}`,
        );
      }
    }

    if (itemsToImport.length > 0) {
      if (updateStrategy === "full_sync") {
        // Full Sync: Atomic delete + insert to guarantee no stale items remain
        console.log(
          `[Combine Staged Full Sync] ${dataSource.name}: Starting atomic replace with ${itemsToImport.length} items`,
        );
        const result = await storage.atomicReplaceInventoryItems(
          dataSourceId,
          itemsToImport as any,
        );
        console.log(
          `[Combine Staged Full Sync] ${dataSource.name}: Atomic replace complete - deleted ${result.deleted}, created ${result.created} items`,
        );
      } else {
        // Replace (Create & Update): Only add/update items, keep items not in file
        console.log(
          `[Combine Staged Upsert] ${dataSource.name}: Upserting ${itemsToImport.length} items (existing items NOT deleted)`,
        );
        // Reset sale flags for regular inventory sources to clear stale consolidation state
        const isRegularInventory = (dataSource as any).sourceType !== "sales";
        await storage.upsertInventoryItems(itemsToImport as any, dataSourceId, {
          resetSaleFlags: isRegularInventory,
        });
      }
    } else {
      // No valid items to import - don't delete existing data, just log warning
      console.warn(
        `[Combine Staged] No valid items to import for data source ${dataSourceId} - keeping existing data`,
      );
    }

    for (const file of filesToProcess) {
      await storage.updateFileStatus(file.id, "imported");
    }

    // Signal import completion (for sync coordination)
    completeImport(dataSourceId, itemsToCreate.length);
    return { success: true, rowCount: itemsToCreate.length };
  } catch (err: any) {
    console.error("Error combining staged files:", err);

    // FIX: Clean up staged files on failure so they don't become permanent orphans
    try {
      const staleFiles = await storage.getStagedFiles(dataSourceId);
      for (const file of staleFiles) {
        await storage.updateFileStatus(file.id, "error");
      }
      if (staleFiles.length > 0) {
        console.log(
          `[Combine] Marked ${staleFiles.length} staged files as error after combine failure`,
        );
      }
    } catch (cleanupErr: any) {
      console.error(
        "[Combine] Error cleaning up staged files:",
        cleanupErr.message,
      );
    }

    // Signal import failure (for sync coordination)
    failImport(dataSourceId, err.message);
    return { success: false, rowCount: 0, error: err.message };
  }
}

/**
 * Normalize a style string for comparison in discontinued_styles table.
 * Strips whitespace and converts to consistent format.
 */
function normalizeStyle(style: string | null | undefined): string | null {
  if (!style) return null;
  return style.replace(/\s+/g, " ").trim();
}

/**
 * Extract unique styles from inventory items.
 * Returns an array of normalized style strings.
 */
export function extractUniqueStyles(
  items: { style?: string | null }[],
): string[] {
  const uniqueStyles = new Set<string>();
  for (const item of items) {
    const normalized = normalizeStyle(item.style);
    if (normalized) {
      uniqueStyles.add(normalized);
    }
  }
  return Array.from(uniqueStyles);
}

/**
 * Register styles from a sale file in the discontinued_styles table.
 * This marks these styles as "owned" by the sale file, preventing them
 * from being imported from regular files.
 *
 * @param saleDataSourceId - The ID of the sale data source
 * @param items - The inventory items from the sale file
 * @returns Object with counts of styles added and updated
 */
export async function registerSaleFileStyles(
  saleDataSourceId: string,
  items: { style?: string | null }[],
): Promise<{ added: number; updated: number; total: number }> {
  const styles = extractUniqueStyles(items);

  if (styles.length === 0) {
    console.log(
      `[registerSaleFileStyles] No styles to register for sale data source ${saleDataSourceId}`,
    );
    return { added: 0, updated: 0, total: 0 };
  }

  console.log(
    `[registerSaleFileStyles] Registering ${styles.length} styles for sale data source ${saleDataSourceId}`,
  );

  // Upsert all styles into discontinued_styles table
  const result = await storage.upsertDiscontinuedStyles(
    styles.map((style) => ({
      saleDataSourceId,
      style,
      active: true,
    })),
  );

  // Deactivate any styles that are no longer in the sale file
  const deactivated = await storage.deactivateDiscontinuedStyles(
    saleDataSourceId,
    styles,
  );
  if (deactivated > 0) {
    console.log(
      `[registerSaleFileStyles] Deactivated ${deactivated} styles no longer in sale file`,
    );
  }

  console.log(
    `[registerSaleFileStyles] Result: ${result.added} added, ${result.updated} updated`,
  );
  return { added: result.added, updated: result.updated, total: styles.length };
}

/**
 * Filter out items from a regular file that have discontinued styles (owned by sale files).
 * Also removes any existing inventory items for those discontinued styles.
 *
 * @param dataSourceId - The ID of the regular data source being imported
 * @param items - The inventory items from the regular file
 * @param linkedSaleDataSourceId - Optional: only check styles owned by this specific sale data source
 * @returns Filtered items (without discontinued styles) and count of removed items
 */
export async function filterDiscontinuedStyles<
  T extends { style?: string | null },
>(
  dataSourceId: string,
  items: T[],
  linkedSaleDataSourceId?: string | null,
): Promise<{ items: T[]; removedCount: number; discontinuedStyles: string[] }> {
  // Get all active discontinued styles
  const allDiscontinuedStyles = await storage.getAllDiscontinuedStyles();

  if (allDiscontinuedStyles.length === 0) {
    return { items, removedCount: 0, discontinuedStyles: [] };
  }

  // If we have a linked sale data source, only filter styles from that source
  const relevantDiscontinued = linkedSaleDataSourceId
    ? allDiscontinuedStyles.filter(
        (d) => d.saleDataSourceId === linkedSaleDataSourceId,
      )
    : allDiscontinuedStyles;

  if (relevantDiscontinued.length === 0) {
    return { items, removedCount: 0, discontinuedStyles: [] };
  }

  // Create a set of discontinued styles for fast lookup
  const discontinuedSet = new Set(relevantDiscontinued.map((d) => d.style));

  console.log(
    `[filterDiscontinuedStyles] Checking ${items.length} items against ${discontinuedSet.size} discontinued styles`,
  );

  // Filter out items with discontinued styles
  const filteredItems: T[] = [];
  const removedItems: T[] = [];
  const matchedStyles = new Set<string>();

  for (const item of items) {
    const normalized = normalizeStyle(item.style);
    if (normalized && discontinuedSet.has(normalized)) {
      removedItems.push(item);
      matchedStyles.add(normalized);
    } else {
      filteredItems.push(item);
    }
  }

  if (removedItems.length > 0) {
    console.log(
      `[filterDiscontinuedStyles] Filtered out ${removedItems.length} items with ${matchedStyles.size} discontinued styles: ${Array.from(matchedStyles).slice(0, 5).join(", ")}${matchedStyles.size > 5 ? "..." : ""}`,
    );
  }

  return {
    items: filteredItems,
    removedCount: removedItems.length,
    discontinuedStyles: Array.from(matchedStyles),
  };
}

/**
 * Remove existing inventory items that have discontinued styles.
 * This is called when importing a regular file to clean up any items
 * that are now owned by a sale file.
 *
 * @param dataSourceId - The ID of the regular data source
 * @param linkedSaleDataSourceId - The ID of the linked sale data source (optional)
 * @returns Count of removed inventory items
 */
export async function removeDiscontinuedInventoryItems(
  dataSourceId: string,
  linkedSaleDataSourceId?: string | null,
): Promise<number> {
  // Get all active discontinued styles
  const allDiscontinuedStyles = await storage.getAllDiscontinuedStyles();

  if (allDiscontinuedStyles.length === 0) {
    return 0;
  }

  // If we have a linked sale data source, only filter styles from that source
  const relevantDiscontinued = linkedSaleDataSourceId
    ? allDiscontinuedStyles.filter(
        (d) => d.saleDataSourceId === linkedSaleDataSourceId,
      )
    : allDiscontinuedStyles;

  if (relevantDiscontinued.length === 0) {
    return 0;
  }

  // Create a set of discontinued styles for fast lookup
  const discontinuedSet = new Set(relevantDiscontinued.map((d) => d.style));

  // Get existing inventory items for this data source
  const existingItems =
    await storage.getInventoryItemsByDataSource(dataSourceId);

  // Find items that should be removed (have discontinued styles)
  const itemsToRemove: string[] = [];
  for (const item of existingItems) {
    const normalized = normalizeStyle(item.style);
    if (normalized && discontinuedSet.has(normalized)) {
      itemsToRemove.push(item.id);
    }
  }

  if (itemsToRemove.length > 0) {
    console.log(
      `[removeDiscontinuedInventoryItems] Removing ${itemsToRemove.length} inventory items with discontinued styles from data source ${dataSourceId}`,
    );

    // Remove the items - use the existing method to mark as pending deletion then remove
    const removed =
      await storage.removeInventoryItemsPendingDeletion(itemsToRemove);
    console.log(`[removeDiscontinuedInventoryItems] Removed ${removed} items`);
    return removed;
  }

  return 0;
}

// ============================================================
// STOCK INFO CALCULATION HELPERS
// Calculates stock message for each item based on stockInfoConfig
// (Copied from routes.ts to avoid circular dependency)
// ============================================================

function calculateItemStockInfo(item: any, stockInfoRule: any): string | null {
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

  // Priority 2: In stock - ALWAYS takes priority over future date
  if (stock > threshold) {
    return stockInfoRule.inStockMessage;
  }

  // Priority 3: Has future date - ONLY for zero/low stock items
  if (shipDate && stockInfoRule.futureDateMessage) {
    try {
      const dateStr = String(shipDate).trim();
      let targetDate: Date;

      const isoMatch = dateStr.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
      const usMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
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
        return stockInfoRule.futureDateMessage.replace(
          /\{date\}/gi,
          formattedDate,
        );
      }
    } catch (e) {
      console.error(`[Email Import] Failed to parse date: ${shipDate}`, e);
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

async function getStockInfoRuleForEmail(
  dataSource: any,
  storageRef: any,
): Promise<any> {
  let stockInfoRule: any = null;

  try {
    const stockInfoConfig = (dataSource as any).stockInfoConfig;

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
      console.log(
        `[Email Import] Using stockInfoConfig: inStock="${stockInfoRule.inStockMessage}"`,
      );
    } else {
      const metafieldRules =
        await storageRef.getShopifyMetafieldRulesByDataSource(dataSource.id);

      const activeDbRule = metafieldRules.find((r: any) => r.enabled !== false);

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
        console.log(
          `[Email Import] Using Rule Engine metafield rule: inStock="${stockInfoRule.inStockMessage}"`,
        );
      } else {
        console.log(
          `[Email Import] No stockInfoConfig AND no metafield rules - stockInfo will be null`,
        );
      }
    }
  } catch (ruleError) {
    console.error(`[Email Import] Failed to get stock info rules:`, ruleError);
  }

  return stockInfoRule;
}

// ============================================================
// OTS FORMAT PARSER (for email/manual imports)
// For files with ots1, ots2, ots3... columns representing sizes
// ============================================================

/**
 * Detect if a file is in OTS format
 * OTS format has headers like: style, color, price1, ots1, ots2, ots3, etc.
 */
export function isOTSFormat(buffer: Buffer): boolean {
  try {
    const workbook = XLSX.read(buffer, { type: "buffer" });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      defval: "",
      raw: false, // CRITICAL FIX: Consistent raw: false for all parsers
    }) as any[][];

    if (data.length < 2) return false;

    const headerRow = data[0];
    const headersLower = headerRow.map((h: any) =>
      String(h ?? "")
        .trim()
        .toLowerCase(),
    );

    // Check if we have ots1, ots2, etc. columns
    const otsColumns = headersLower.filter((h: string) => /^ots\d+$/.test(h));
    return otsColumns.length >= 3; // At least 3 OTS columns indicates OTS format
  } catch (e) {
    return false;
  }
}

/**
 * Parse OTS format files
 * These files have:
 * - style, color columns
 * - ots1, ots2, ots3, etc. columns representing sizes (maps to 2, 4, 6, 8, 10, 12, 14, 16, 18)
 * - Optionally a size_whole_comp column with actual size values
 */
export function parseOTSFormat(
  buffer: Buffer,
  cleaningConfig: any,
  dataSourceName?: string,
  stockValueConfig?: {
    textMappings?:
      | Array<{ text: string; value: number }>
      | Record<string, number>;
  },
): { headers: string[]; rows: any[][]; items: any[] } {
  console.log(
    `[OTSFormat] Parsing OTS format file for ${dataSourceName || "unknown"}`,
  );

  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false, // CRITICAL FIX: Consistent raw: false for all parsers
  }) as any[][];

  const items: any[] = [];
  if (data.length < 2) {
    console.log(`[OTSFormat] File has less than 2 rows, returning empty`);
    return { headers: [], rows: [], items: [] };
  }

  const headerRow = data[0];
  const headers = headerRow.map((h: any) =>
    String(h ?? "")
      .trim()
      .toLowerCase(),
  );

  // Find column indices
  const styleIdx = headers.findIndex((h: string) => h === "style");
  const colorIdx = headers.findIndex((h: string) => h === "color");
  const priceIdx = headers.findIndex((h: string) => h.includes("price"));
  const sizeCompIdx = headers.findIndex(
    (h: string) => h.includes("size_whole") || h === "size",
  );

  // Find OTS columns (ots1, ots2, etc.)
  const otsColumns: { index: number; num: number }[] = [];
  for (let i = 0; i < headers.length; i++) {
    const match = headers[i].match(/^ots(\d+)$/);
    if (match) {
      otsColumns.push({ index: i, num: parseInt(match[1], 10) });
    }
  }
  otsColumns.sort((a, b) => a.num - b.num);

  console.log(
    `[OTSFormat] Found columns: style=${styleIdx}, color=${colorIdx}, price=${priceIdx}, otsColumns=${otsColumns.length}`,
  );

  if (styleIdx === -1 || otsColumns.length === 0) {
    console.log(`[OTSFormat] Missing required columns`);
    return { headers: [], rows: [], items: [] };
  }

  // Helper function to parse stock values with text mapping support
  const parseStockValue = (value: any): number => {
    if (value === null || value === undefined || value === "") return 0;
    if (typeof value === "number") return Math.max(0, Math.floor(value));

    const strVal = String(value).trim().toLowerCase();

    // Check user-configured text mappings first
    const textMappings = stockValueConfig?.textMappings;
    if (textMappings) {
      if (Array.isArray(textMappings)) {
        for (const mapping of textMappings) {
          if (mapping.text && mapping.text.toLowerCase().trim() === strVal) {
            return mapping.value;
          }
        }
      } else if (
        (textMappings as Record<string, number>)[strVal] !== undefined
      ) {
        return (textMappings as Record<string, number>)[strVal];
      }
    }

    const parsed = parseInt(strVal, 10);
    return isNaN(parsed) ? 0 : Math.max(0, parsed);
  };

  // Parse data rows
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

    // Parse sizes from size_whole_comp column if available
    let sizes: string[] = [];
    if (sizeCompIdx >= 0) {
      const sizeStr = String(row[sizeCompIdx] ?? "");
      sizes = sizeStr
        .trim()
        .split(/\s+/)
        .filter((s: string) => /^\d+$/.test(s));
    }
    // Default sizes if none found (maps to standard size range)
    if (sizes.length === 0) {
      sizes = ["2", "4", "6", "8", "10", "12", "14", "16", "18"];
    }

    // Map OTS columns to sizes
    for (let i = 0; i < Math.min(otsColumns.length, sizes.length); i++) {
      const stock = parseStockValue(row[otsColumns[i].index]);
      const size = sizes[i];

      // FIX: Always push ALL items regardless of stock (matching Tarik Ediz behavior)
      // This ensures size expansion can work with existing 0-stock items
      // Zero-stock filtering happens later in applyVariantRules if enabled
      const sku = `${style}-${color || "DEFAULT"}-${size}`
        .replace(/\//g, "-")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-");

      items.push({
        sku,
        style,
        color: color || "DEFAULT",
        size,
        stock,
        price,
        // FIX: Add flags for consistency (no shipDate in OTS format)
        hasFutureStock: false,
        preserveZeroStock: false,
      });
    }
  }

  console.log(
    `[OTSFormat] Parsed ${items.length} items from ${data.length - 1} data rows`,
  );

  // Build output rows
  const outputHeaders = ["sku", "style", "color", "size", "stock", "price"];
  const outputRows = items.map((item) => [
    item.sku,
    item.style,
    item.color,
    item.size,
    item.stock,
    item.price || "",
  ]);

  return {
    headers: outputHeaders,
    rows: outputRows,
    items,
  };
}

// ============================================================
// GENERIC PIVOT FORMAT PARSER
// For files like INESS/Colette, Alyce, etc. with size columns as headers
// ============================================================

/**
 * Parse generic pivot format files
 * These files have:
 * - Style column (header contains "STYLE", "CODE", or "ITEM")
 * - Color column (header contains "COLOR" but not "CODE")
 * - Size columns as headers (00, 0, 2, 4, 6, 8, 10, 12, 14, 16, etc.)
 * - Stock values in each size column cell
 */
export function parseGenericPivotFormat(
  buffer: Buffer,
  cleaningConfig: any,
  dataSourceName?: string,
  discontinuedConfig?: { keywords?: string[]; values?: string[] },
  stockValueConfig?: {
    textMappings?:
      | Array<{ text: string; value: number }>
      | Record<string, number>;
  },
  sizeLimitConfig?: SizeLimitConfig | null,
): { headers: string[]; rows: any[][]; items: any[]; sizeFiltered?: number } {
  console.log(`[GenericPivot] Parsing file for ${dataSourceName || "unknown"}`);

  // NOTE: Size limits are NOT applied during parsing because prefix override patterns
  // are designed to match PREFIXED styles (e.g., "Jovani 12345"), not RAW styles (e.g., "12345").
  // Size limits are applied later in applyVariantRules AFTER the style has been prefixed.
  if (sizeLimitConfig?.enabled) {
    console.log(
      `[GenericPivot] Size limits configured (will be applied after prefixing in applyVariantRules)`,
    );
  }

  // Helper function to parse stock values with text mapping support
  const parseStockValue = (value: any): number => {
    if (value === null || value === undefined || value === "") return 0;
    if (typeof value === "number") return Math.max(0, Math.floor(value));

    const strVal = String(value).trim().toLowerCase();

    // Default text mappings
    const defaultMappings: Record<string, number> = {
      yes: 1,
      no: 0,
      "last piece": 1,
      lastpiece: 1,
      "in stock": 1,
      "sold out": 0,
      "out of stock": 0,
      "–": 0,
      "-": 0,
      "n/a": 0,
      "": 0,
    };

    // Check user-configured text mappings first
    const textMappings = stockValueConfig?.textMappings;
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
      else if ((textMappings as Record<string, number>)[strVal] !== undefined) {
        return (textMappings as Record<string, number>)[strVal];
      }
    }

    // Check default mappings
    if (defaultMappings[strVal] !== undefined) return defaultMappings[strVal];

    // Try parsing as number
    const parsed = parseInt(strVal, 10);
    return isNaN(parsed) ? 0 : Math.max(0, parsed);
  };

  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false,
  }) as any[][];

  const items: any[] = [];
  if (data.length < 2) {
    console.log(`[GenericPivot] File has less than 2 rows, returning empty`);
    return { headers: [], rows: [], items: [] };
  }

  // Size pattern to identify size columns in headers
  const sizePattern =
    /^(000|00|OOO|OO|0|2|4|6|8|10|12|14|16|18|20|22|24|26|28|30|32|16W|18W|20W|22W|24W|26W|28W|30W|32W|XS|S|SM|M|MD|L|LG|XL|XXL|UNIT|- None -)$/i;

  // Find header row (row with 5+ size columns)
  let headerRowIdx = 0;
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

  console.log(`[GenericPivot] Header row index: ${headerRowIdx}`);
  console.log(
    `[GenericPivot] Headers: ${headersUpper.slice(0, 10).join(", ")}...`,
  );

  // Find key columns
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

  // Find discontinued/status column
  let statusIdx = headersUpper.findIndex(
    (h: string) =>
      h.includes("STATUS") ||
      h.includes("DISCONTINUED") ||
      h.includes("ACTIVE"),
  );

  // Get discontinued keywords
  const configKeywords =
    (discontinuedConfig as any)?.keywords || discontinuedConfig?.values;
  const discontinuedKeywords = configKeywords?.length
    ? configKeywords.map((v: string) => v.toLowerCase().trim())
    : ["discontinued", "disc", "inactive", "d", "no", "n", "false", "0", "cl"];

  // Find size columns
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

  console.log(
    `[GenericPivot] Style col: ${styleIdx}, Color col: ${colorIdx}, Date col: ${dateIdx}`,
  );
  console.log(
    `[GenericPivot] Found ${sizeColumns.length} size columns: ${sizeColumns.map((s) => s.size).join(", ")}`,
  );

  if (styleIdx === -1 || sizeColumns.length === 0) {
    console.log(
      `[GenericPivot] Missing required columns (style=${styleIdx}, sizes=${sizeColumns.length})`,
    );
    return { headers: [], rows: [], items: [] };
  }

  // Parse data rows
  for (let rowIdx = headerRowIdx + 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const style = String(row[styleIdx] ?? "").trim();
    const color = colorIdx >= 0 ? String(row[colorIdx] ?? "").trim() : "";
    if (!style) continue;

    // Parse ship date
    let shipDate: string | undefined;
    if (dateIdx >= 0) {
      const dateVal = row[dateIdx];
      if (dateVal && typeof dateVal === "number" && dateVal > 40000) {
        // Excel serial date
        const excelEpoch = new Date(1899, 11, 30);
        const jsDate = new Date(
          excelEpoch.getTime() + dateVal * 24 * 60 * 60 * 1000,
        );
        shipDate = jsDate.toISOString().split("T")[0];
      } else if (dateVal && typeof dateVal === "string") {
        const dateStr = String(dateVal).trim();
        if (dateStr && dateStr !== "0") {
          shipDate = dateStr;
        }
      }
    }

    // Check discontinued status
    let isDiscontinued = false;
    if (statusIdx >= 0) {
      const statusVal = String(row[statusIdx] ?? "")
        .toLowerCase()
        .trim();
      isDiscontinued = discontinuedKeywords.some(
        (k) =>
          statusVal === k || statusVal.includes(k) || statusVal.startsWith(k),
      );
    }

    // Parse stock for each size column
    for (const sc of sizeColumns) {
      const stockVal = row[sc.index];
      let stock = 0;

      if (stockVal !== null && stockVal !== undefined && stockVal !== "") {
        if (typeof stockVal === "number") {
          stock = Math.max(0, Math.floor(stockVal));
        } else {
          const strVal = String(stockVal).trim();
          const parsed = parseInt(strVal, 10);
          stock = isNaN(parsed) ? 0 : Math.max(0, parsed);
        }
      }

      // NOTE: Size limits are NOT applied here because prefix override patterns
      // are designed to match PREFIXED styles (e.g., "Jovani 12345"), not RAW styles.
      // Size filtering happens in applyVariantRules AFTER the style has been prefixed.

      // FIX: Always push ALL items regardless of stock (matching Tarik Ediz behavior)
      // This ensures size expansion can work with existing 0-stock items
      // Zero-stock filtering happens later in applyVariantRules if enabled
      const sku = `${style}-${color || "DEFAULT"}-${sc.size}`
        .replace(/\//g, "-")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-");

      items.push({
        sku,
        style,
        color: color || "DEFAULT",
        size: sc.size,
        stock,
        shipDate,
        discontinued: isDiscontinued,
        // FIX: Add flags based on shipDate for consistency
        hasFutureStock: shipDate ? true : false,
        preserveZeroStock: shipDate && stock === 0 ? true : false,
      });
    }
  }

  // NOTE: Size limits are applied in applyVariantRules with the PREFIXED style
  // (not here with the RAW style) to ensure prefix override patterns work correctly
  const sizeFiltered = 0; // Size filtering happens later in applyVariantRules

  console.log(
    `[GenericPivot] Parsed ${items.length} items from ${data.length - headerRowIdx - 1} data rows`,
  );

  // Build output rows
  const outputHeaders = [
    "sku",
    "style",
    "color",
    "size",
    "stock",
    "shipDate",
    "discontinued",
  ];
  const outputRows = items.map((item) => [
    item.sku,
    item.style,
    item.color,
    item.size,
    item.stock,
    item.shipDate || "",
    item.discontinued ? "Yes" : "No",
  ]);

  return {
    headers: outputHeaders,
    rows: outputRows,
    items,
    sizeFiltered,
  };
}

// ============================================================
// GRN-INVOICE FORMAT PARSER
// For files with "Code", "Color" columns and size columns as headers (000, 00, 0, 02, 04, etc.)
// ============================================================

export function parseGRNInvoiceFormat(
  buffer: Buffer,
  cleaningConfig: any,
  dataSourceName?: string,
  stockValueConfig?: {
    textMappings?:
      | Array<{ text: string; value: number }>
      | Record<string, number>;
  },
): { headers: string[]; rows: any[][]; items: any[] } {
  console.log(
    `[GRNInvoice] Parsing GRN-INVOICE format file for ${dataSourceName || "unknown"}`,
  );

  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rawData = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false, // CRITICAL FIX: Consistent raw: false for all parsers
  }) as any[][];

  const items: any[] = [];
  if (rawData.length < 3) {
    console.log(`[GRNInvoice] File has less than 3 rows, returning empty`);
    return { headers: [], rows: [], items: [] };
  }

  // Helper function to parse stock values with text mapping support
  const parseStock = (value: any): number => {
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
      "–": 0,
      "-": 0,
      "n/a": 0,
      "": 0,
    };

    // Check user-configured text mappings first
    const textMappings = stockValueConfig?.textMappings;
    if (textMappings) {
      if (Array.isArray(textMappings)) {
        for (const mapping of textMappings) {
          if (mapping.text && mapping.text.toLowerCase().trim() === strVal) {
            return mapping.value;
          }
        }
      } else if (
        (textMappings as Record<string, number>)[strVal] !== undefined
      ) {
        return (textMappings as Record<string, number>)[strVal];
      }
    }

    if (defaultMappings[strVal] !== undefined) return defaultMappings[strVal];

    const parsed = parseInt(strVal, 10);
    return isNaN(parsed) ? 0 : Math.max(0, parsed);
  };

  // Find header row containing "code" and "color"
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
  if (data.length < 2) {
    console.log(`[GRNInvoice] No data rows after header, returning empty`);
    return { headers: [], rows: [], items: [] };
  }

  const headerRow = data[0];
  const headersLower = headerRow.map((h: any) =>
    String(h ?? "")
      .toLowerCase()
      .trim(),
  );

  const codeIdx = headersLower.findIndex((h: string) => h === "code");
  const colorIdx = headersLower.findIndex((h: string) => h === "color");

  // Detect size columns by pattern (000, 00, 0, 02, 04, 06, 08, 10, 12, 14, 16, 18, 20, 22, 24)
  const sizePattern = /^(000|00|0|02|04|06|08|10|12|14|16|18|20|22|24)$/i;
  const sizeColumns: { index: number; size: string }[] = [];

  for (let i = 0; i < headerRow.length; i++) {
    const h = String(headerRow[i] ?? "").trim();
    if (sizePattern.test(h)) {
      let normalizedSize = h;
      // Normalize leading-zero sizes: "02" → "2", "04" → "4", etc.
      if (/^0\d$/.test(h)) normalizedSize = h.replace(/^0/, "");
      sizeColumns.push({ index: i, size: normalizedSize });
    }
  }

  console.log(
    `[GRNInvoice] Found columns: code=${codeIdx}, color=${colorIdx}, sizeColumns=${sizeColumns.length}`,
  );

  if (codeIdx === -1 || sizeColumns.length === 0) {
    console.log(`[GRNInvoice] Missing required columns (code or size columns)`);
    return { headers: [], rows: [], items: [] };
  }

  // Parse data rows
  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const code = String(row[codeIdx] ?? "").trim();
    const color = colorIdx >= 0 ? String(row[colorIdx] ?? "").trim() : "";
    if (!code) continue;

    for (const sc of sizeColumns) {
      const stock = parseStock(row[sc.index]);
      if (stock > 0) {
        const style = applyCleaningToValue(code, cleaningConfig, "style");
        const sku = `${style}-${color || "DEFAULT"}-${sc.size}`
          .replace(/\//g, "-")
          .replace(/\s+/g, "-")
          .replace(/-+/g, "-");

        items.push({
          sku,
          style,
          color: color || "DEFAULT",
          size: sc.size,
          stock,
          // FIX: Add flags for consistency (no shipDate in GRN format)
          hasFutureStock: false,
          preserveZeroStock: false,
        });
      }
    }
  }

  console.log(
    `[GRNInvoice] Parsed ${items.length} items from ${data.length - 1} data rows`,
  );

  // Build output rows
  const outputHeaders = ["sku", "style", "color", "size", "stock"];
  const outputRows = items.map((item) => [
    item.sku,
    item.style,
    item.color,
    item.size,
    item.stock,
  ]);

  return {
    headers: outputHeaders,
    rows: outputRows,
    items,
  };
}

// ============================================================
// PR DATE HEADERS FORMAT PARSER
// For files with Excel serial date numbers as column headers (4xxxx pattern)
// ============================================================

function excelSerialToDateStr(serial: number): string {
  if (!serial || serial < 40000 || serial > 55000) return "";
  const excelEpoch = new Date(1899, 11, 30);
  const jsDate = new Date(excelEpoch.getTime() + serial * 24 * 60 * 60 * 1000);
  return jsDate.toISOString().split("T")[0];
}

export function parsePRDateHeaderFormat(
  buffer: Buffer,
  cleaningConfig: any,
  dataSourceName?: string,
  stockValueConfig?: {
    textMappings?:
      | Array<{ text: string; value: number }>
      | Record<string, number>;
  },
): { headers: string[]; rows: any[][]; items: any[] } {
  console.log(
    `[PRDateHeaders] Parsing PR date headers format file for ${dataSourceName || "unknown"}`,
  );

  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false, // CRITICAL FIX: Consistent raw: false for all parsers
  }) as any[][];

  const items: any[] = [];
  if (data.length < 2) {
    console.log(`[PRDateHeaders] File has less than 2 rows, returning empty`);
    return { headers: [], rows: [], items: [] };
  }

  // Helper function to parse stock values with text mapping support
  const parseStock = (value: any): number => {
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
      "–": 0,
      "-": 0,
      "n/a": 0,
      "": 0,
    };

    const textMappings = stockValueConfig?.textMappings;
    if (textMappings) {
      if (Array.isArray(textMappings)) {
        for (const mapping of textMappings) {
          if (mapping.text && mapping.text.toLowerCase().trim() === strVal) {
            return mapping.value;
          }
        }
      } else if (
        (textMappings as Record<string, number>)[strVal] !== undefined
      ) {
        return (textMappings as Record<string, number>)[strVal];
      }
    }

    if (defaultMappings[strVal] !== undefined) return defaultMappings[strVal];

    const parsed = parseInt(strVal, 10);
    return isNaN(parsed) ? 0 : Math.max(0, parsed);
  };

  const headerRow = data[0];
  const headers = headerRow.map((h: any) => String(h ?? "").trim());

  // Find style/product column
  const styleIdx = headers.findIndex(
    (h: string) =>
      h.toLowerCase().includes("product") || h.toLowerCase().includes("code"),
  );
  // Find "available" column for current stock
  const availableIdx = headers.findIndex((h: string) =>
    h.toLowerCase().includes("available"),
  );

  // Find date columns (Excel serial numbers: 4xxxx)
  const dateColumns: { index: number; date: string }[] = [];
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    if (/^4\d{4}$/.test(h)) {
      const dateStr = excelSerialToDateStr(parseInt(h, 10));
      if (dateStr) dateColumns.push({ index: i, date: dateStr });
    }
  }

  console.log(
    `[PRDateHeaders] Found columns: style=${styleIdx}, available=${availableIdx}, dateColumns=${dateColumns.length}`,
  );

  if (styleIdx === -1) {
    console.log(`[PRDateHeaders] Missing required style/product column`);
    return { headers: [], rows: [], items: [] };
  }

  // Parse data rows
  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 2) continue;

    const rawStyle = String(row[styleIdx] ?? "").trim();
    if (!rawStyle) continue;

    const style = applyCleaningToValue(rawStyle, cleaningConfig, "style");

    // Extract size from style if it contains a dash (e.g., "PR-1234-8" → size "8")
    let extractedSize = "";
    const parts = rawStyle.split("-");
    if (parts.length >= 2) {
      const lastPart = parts[parts.length - 1];
      if (/^\d+$/.test(lastPart)) extractedSize = lastPart;
    }

    // Current stock from "available" column
    const currentStock = availableIdx >= 0 ? parseStock(row[availableIdx]) : 0;

    if (currentStock > 0) {
      const size = extractedSize || "ONE SIZE";
      const sku = `${style}-DEFAULT-${size}`
        .replace(/\//g, "-")
        .replace(/\s+/g, "-")
        .replace(/-+/g, "-");

      items.push({
        sku,
        style,
        color: "DEFAULT",
        size,
        stock: currentStock,
        // FIX: Add flags for consistency (no shipDate for current stock)
        hasFutureStock: false,
        preserveZeroStock: false,
      });
    }

    // Future/incoming stock from date columns
    for (const dc of dateColumns) {
      const futureStock = parseStock(row[dc.index]);
      if (futureStock > 0) {
        const size = extractedSize || "ONE SIZE";
        const sku = `${style}-DEFAULT-${size}`
          .replace(/\//g, "-")
          .replace(/\s+/g, "-")
          .replace(/-+/g, "-");

        items.push({
          sku,
          style,
          color: "DEFAULT",
          size,
          stock: 0,
          incomingStock: futureStock,
          shipDate: dc.date,
          // FIX: Add flags for future stock items
          hasFutureStock: true,
          preserveZeroStock: true,
        });
      }
    }
  }

  console.log(
    `[PRDateHeaders] Parsed ${items.length} items from ${data.length - 1} data rows`,
  );

  // Build output rows
  const outputHeaders = [
    "sku",
    "style",
    "color",
    "size",
    "stock",
    "incomingStock",
    "shipDate",
  ];
  const outputRows = items.map((item) => [
    item.sku,
    item.style,
    item.color,
    item.size,
    item.stock,
    item.incomingStock || 0,
    item.shipDate || "",
  ]);

  return {
    headers: outputHeaders,
    rows: outputRows,
    items,
  };
}

// ============================================================
// STORE MULTIBRAND FORMAT PARSER
// For store inventory files with product name, style, color, size, stock columns
// ============================================================

export function parseStoreMultibrandFormat(
  buffer: Buffer,
  cleaningConfig: any,
  dataSourceName?: string,
  stockValueConfig?: {
    textMappings?:
      | Array<{ text: string; value: number }>
      | Record<string, number>;
  },
): { headers: string[]; rows: any[][]; items: any[] } {
  console.log(
    `[StoreMultibrand] Parsing store multibrand format file for ${dataSourceName || "unknown"}`,
  );

  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false, // CRITICAL FIX: Consistent raw: false for all parsers
  }) as any[][];

  const items: any[] = [];
  if (data.length < 2) {
    console.log(`[StoreMultibrand] File has less than 2 rows, returning empty`);
    return { headers: [], rows: [], items: [] };
  }

  // Helper function to parse stock values with text mapping support
  const parseStock = (value: any): number => {
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
      "–": 0,
      "-": 0,
      "n/a": 0,
      "": 0,
    };

    const textMappings = stockValueConfig?.textMappings;
    if (textMappings) {
      if (Array.isArray(textMappings)) {
        for (const mapping of textMappings) {
          if (mapping.text && mapping.text.toLowerCase().trim() === strVal) {
            return mapping.value;
          }
        }
      } else if (
        (textMappings as Record<string, number>)[strVal] !== undefined
      ) {
        return (textMappings as Record<string, number>)[strVal];
      }
    }

    if (defaultMappings[strVal] !== undefined) return defaultMappings[strVal];

    const parsed = parseInt(strVal, 10);
    return isNaN(parsed) ? 0 : Math.max(0, parsed);
  };

  const headerRow = data[0];
  const headersRaw = headerRow.map((h: any) => String(h ?? "").trim());
  const headersLower = headersRaw.map((h: string) => h.toLowerCase());

  // Find column indices
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

  console.log(
    `[StoreMultibrand] Found columns: productName=${productNameIdx}, style=${styleIdx}, color=${colorIdx}, size=${sizeIdx}, stock=${stockIdx}, price=${priceIdx}`,
  );

  if (styleIdx === -1) {
    console.log(`[StoreMultibrand] Missing required style column`);
    return { headers: [], rows: [], items: [] };
  }

  // Known brands for auto-detection from product name
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

  // Parse data rows
  for (let rowIdx = 1; rowIdx < data.length; rowIdx++) {
    const row = data[rowIdx];
    if (!row || row.length < 3) continue;

    const rawStyle = String(row[styleIdx] ?? "").trim();
    if (!rawStyle) continue;

    const style = applyCleaningToValue(rawStyle, cleaningConfig, "style");
    const productName =
      productNameIdx >= 0 ? String(row[productNameIdx] ?? "").trim() : "";
    const color = colorIdx >= 0 ? String(row[colorIdx] ?? "").trim() : "";
    const size = sizeIdx >= 0 ? String(row[sizeIdx] ?? "").trim() : "ONE SIZE";
    const stock = stockIdx >= 0 ? parseStock(row[stockIdx]) : 0;
    const price =
      priceIdx >= 0
        ? parseFloat(String(row[priceIdx] || "0")) || undefined
        : undefined;

    // Detect brand from product name
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

    const sku = `${style}-${color || "DEFAULT"}-${size}`
      .replace(/\//g, "-")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-");

    items.push({
      sku,
      style,
      color: color || "DEFAULT",
      size,
      stock,
      price,
      brand,
      // FIX: Add flags for consistency (no shipDate in store format)
      hasFutureStock: false,
      preserveZeroStock: false,
    });
  }

  console.log(
    `[StoreMultibrand] Parsed ${items.length} items from ${data.length - 1} data rows`,
  );

  // Build output rows
  const outputHeaders = [
    "sku",
    "style",
    "color",
    "size",
    "stock",
    "price",
    "brand",
  ];
  const outputRows = items.map((item) => [
    item.sku,
    item.style,
    item.color,
    item.size,
    item.stock,
    item.price || "",
    item.brand || "",
  ]);

  return {
    headers: outputHeaders,
    rows: outputRows,
    items,
  };
}

// ============================================================
// TARIK EDIZ FORMAT PARSER
// Format: Pivoted table with sizes as columns, style headers, color/stock data rows
// ============================================================
function parseTarikEdizFormat(
  buffer: Buffer,
): { headers: string[]; rows: any[][]; items: any[] } | null {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    raw: false,
  }) as any[][]; // CRITICAL FIX: Consistent raw: false

  if (data.length === 0) return null;

  // Detect Tarik Ediz format: First row contains "Up-to-Date Product Inventory Report"
  // or company name contains "EDİZ" or "EDIZ"
  const firstRowText = String(data[0]?.[0] || "").toLowerCase();
  const secondRowText = String(data[1]?.[0] || "").toLowerCase();

  const isTarikEdizFormat =
    firstRowText.includes("up-to-date product inventory") ||
    firstRowText.includes("inventory report") ||
    secondRowText.includes("ediz") ||
    secondRowText.includes("edi\u0307z");

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

// ============================================================
// JOVANI FORMAT PARSER
// Format: Style rows have style/price, color rows have color/stock values
// Row 1: [null, "00", 0, 2, 4, ...] - size headers
// Style row: ["#02861", 175, ...] - style in col 0, price in col 1
// Color row: ["Taupe-Off-White", 1, 1, 1, ...] - color in col 0, stock values
// ============================================================
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
  }) as any[][]; // CRITICAL FIX: Consistent raw: false

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
    raw: false, // CRITICAL FIX: Consistent raw: false for all parsers
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
      "\u2013": 0,
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
        dateVal !== "\u2013"
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

// ============================================================
// FERIANI / GIA FORMAT PARSER
// Format: DELIVERY, STYLE, COLOR headers with sizes as columns
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
    raw: false, // CRITICAL FIX: Consistent raw: false for all parsers
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
