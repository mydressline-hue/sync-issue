import * as XLSX from "xlsx";
import { storage } from "./storage";
import Anthropic from "@anthropic-ai/sdk";
import {
  cleanInventoryData,
  applyVariantRules,
  isColorCode,
  formatColorName,
  applyImportRules,
  applyPriceBasedExpansion,
  buildStylePriceMapFromCache,
} from "./inventoryProcessing";

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

      // First, try stock text mappings (e.g., "SOLD OUT" -> 0, "Very Low" -> 1)
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

// ============================================================
// REMOVED: parsePivotedExcelToInventory() — dead code, moved to importEngine/shared parsers
// REMOVED: isPurelyNumeric(), isDateLike(), excelSerialToDateString(), convertEuropeanToUSDate()
//          — helper functions only used by removed parsePivotedExcelToInventory
// ============================================================

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

// ============================================================
// SLIMMED: processEmailAttachment()
// Heavy import logic moved to importEngine.ts
// Original was ~1065 lines (1965-3030), now ~35 lines
// ============================================================

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
  const dataSource = await storage.getDataSource(dataSourceId);
  if (!dataSource) {
    return { success: false, rowCount: 0, error: "Data source not found" };
  }

  // Check if staging is needed (multi-file data source)
  const isMultiFile =
    forceStage || (dataSource as any).ingestionMode === "multi";

  if (isMultiFile) {
    // Stage the file for later combine — importEngine handles parsing + staging
    const { stageFileForCombine } = await import("./importEngine");
    return stageFileForCombine({
      buffer,
      filename,
      dataSourceId,
      dataSource,
    });
  }

  // Single file — import directly via engine
  const { executeImport } = await import("./importEngine");
  return executeImport({
    fileBuffers: [{ buffer, originalname: filename }],
    dataSourceId,
    source: "email",
  });
}

// ============================================================
// SLIMMED: combineAndImportStagedFiles()
// Heavy combine logic moved to importEngine.ts
// Original was ~822 lines (3032-3854), now ~15 lines
// ============================================================

export async function combineAndImportStagedFiles(
  dataSourceId: string,
): Promise<{ success: boolean; rowCount: number; error?: string }> {
  // Delegate to importEngine (lazy import to avoid circular dependencies)
  const { combineAndImport } = await import("./importEngine");
  return combineAndImport(dataSourceId);
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
// REMOVED: calculateItemStockInfo() — moved to importEngine
// REMOVED: getStockInfoRuleForEmail() — moved to importEngine as getStockInfoRule()
// ============================================================

// ============================================================
// OTS FORMAT DETECTION (kept — may be imported by other files)
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

// ============================================================
// REMOVED: Dead parser copies — all moved to importEngine/shared parsers (aiImportRoutes)
//   - parseOTSFormat()
//   - parseGenericPivotFormat()
//   - parseGRNInvoiceFormat()
//   - parsePRDateHeaderFormat()
//   - parseStoreMultibrandFormat()
//   - parseTarikEdizFormat()
//   - parseJovaniFormat()
//   - parseSherriHillFormat()
//   - parseFerianiGiaFormat()
// ============================================================
