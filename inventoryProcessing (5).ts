import { storage } from "./storage";
import { suggestColorCorrections } from "./openai";
import {
  LETTER_SIZES,
  LETTER_SIZE_MAP,
  NUMERIC_SIZES,
  NUMERIC_SIZE_MAP,
  isSizeAllowed,
  SizeLimitConfig,
} from "./sizeUtils";

/**
 * Check if a ship date is a valid, parseable date (not "N/A", empty, or garbage)
 * Returns false for invalid values like "N/A", "TBD", empty strings, etc.
 */
export function isValidShipDate(
  shipDate: string | Date | null | undefined,
): boolean {
  if (!shipDate) return false;

  try {
    const dateStr = String(shipDate).trim().toLowerCase();

    // Reject common non-date values
    const invalidValues = [
      "n/a",
      "na",
      "tbd",
      "none",
      "null",
      "undefined",
      "-",
      "",
    ];
    if (invalidValues.includes(dateStr)) return false;

    // Parse ISO format: YYYY-MM-DD
    const isoMatch = dateStr.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
    // Parse US format: M/D/YYYY
    const usMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    // Parse US short format: M/D/YY
    const usShortMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2})$/);

    let targetDate: Date;

    if (isoMatch) {
      const [, year, month, day] = isoMatch;
      targetDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    } else if (usMatch) {
      const [, month, day, year] = usMatch;
      targetDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    } else if (usShortMatch) {
      const [, month, day, shortYear] = usShortMatch;
      targetDate = new Date(
        2000 + parseInt(shortYear),
        parseInt(month) - 1,
        parseInt(day),
      );
    } else {
      // Try generic parsing as last resort
      targetDate = new Date(dateStr);
    }

    // Check if valid date
    return !isNaN(targetDate.getTime());
  } catch (e) {
    return false;
  }
}

/**
 * Check if a ship date is in the past
 * Used to determine if items with expired ship dates should be filtered
 * when filterZeroStock is enabled
 */
function isShipDateInPast(shipDate: string | Date | null | undefined): boolean {
  if (!shipDate) return false;

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
      targetDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    } else if (usMatch) {
      const [, month, day, year] = usMatch;
      targetDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
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

    // Check if valid date
    if (isNaN(targetDate.getTime())) {
      return false;
    }

    const today = new Date();
    today.setHours(0, 0, 0, 0);
    targetDate.setHours(0, 0, 0, 0);

    return targetDate < today;
  } catch (e) {
    return false;
  }
}

function generateExpandedSku(
  style: string | undefined | null,
  color: string | undefined | null,
  size: string | undefined | null,
): string | null {
  if (!style || !color || size === undefined || size === null || size === "") {
    return null;
  }
  // Convert slashes and spaces to hyphens for Shopify compatibility
  const styleWithDashes = style
    .trim()
    .replace(/\//g, "-")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-");
  const colorWithDashes = color
    .trim()
    .replace(/\//g, "-")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-");
  return `${styleWithDashes}-${colorWithDashes}-${size}`;
}

const STANDARD_COLOR_NAMES = new Set([
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
  "navy",
  "beige",
  "cream",
  "ivory",
  "tan",
  "khaki",
  "maroon",
  "burgundy",
  "coral",
  "salmon",
  "peach",
  "mint",
  "teal",
  "aqua",
  "turquoise",
  "cyan",
  "magenta",
  "fuchsia",
  "lavender",
  "violet",
  "indigo",
  "gold",
  "silver",
  "bronze",
  "copper",
  "charcoal",
  "slate",
  "olive",
  "sage",
  "forest",
  "hunter",
  "emerald",
  "jade",
  "lime",
  "chartreuse",
  "mustard",
  "rust",
  "wine",
  "plum",
  "mauve",
  "lilac",
  "periwinkle",
  "rose",
  "blush",
  "nude",
  "taupe",
  "camel",
  "cognac",
  "espresso",
  "chocolate",
  "mocha",
  "sand",
  "stone",
  "ash",
  "smoke",
  "navy blue",
  "royal blue",
  "sky blue",
  "baby blue",
  "light blue",
  "dark blue",
  "hot pink",
  "light pink",
  "dark pink",
  "bright pink",
  "light green",
  "dark green",
  "light gray",
  "dark gray",
  "light grey",
  "dark grey",
  "off white",
  "off-white",
  "eggshell",
  "champagne",
  "pearl",
  "oatmeal",
  "multi",
  "multicolor",
  "multicolour",
  "print",
  "pattern",
  "floral",
  "stripe",
  "stripes",
  "striped",
  "animal",
  "leopard",
  "zebra",
  "camo",
  "camouflage",
  "tie dye",
  "tie-dye",
  "ombre",
  "heather",
  "heathered",
  "melange",
  "marl",
  "neon",
  "bright",
  "pastel",
  "muted",
  "vintage",
  "natural",
  "neutral",
  "earth",
  "denim",
  "indigo blue",
  "chambray",
  "bleach",
  "acid wash",
]);

export function isColorCode(color: string): boolean {
  const trimmed = color.trim();
  if (!trimmed) return false;

  const lower = trimmed.toLowerCase();

  if (STANDARD_COLOR_NAMES.has(lower)) return false;

  if (trimmed.includes(" ") && trimmed.length > 6) return false;

  if (trimmed.length <= 4) {
    const hasVowels = /[aeiou]/i.test(trimmed);
    const isUpperCase =
      trimmed === trimmed.toUpperCase() && /[A-Z]/.test(trimmed);
    if (isUpperCase || !hasVowels) return true;
  }

  if (
    trimmed === trimmed.toUpperCase() &&
    /^[A-Z0-9]+$/.test(trimmed) &&
    trimmed.length <= 6
  ) {
    return true;
  }

  if (
    /^[A-Z]{2,4}[0-9]+$/i.test(trimmed) ||
    /^[0-9]+[A-Z]{2,4}$/i.test(trimmed)
  ) {
    return true;
  }

  return false;
}

export function formatColorName(color: string): string {
  const trimmed = color.trim();
  if (!trimmed) return trimmed;

  return trimmed
    .toLowerCase()
    .split(/(\s+|[-/])/)
    .map((part) => {
      if (/^[\s\-\/]+$/.test(part)) return part;
      if (part.length > 0) {
        return part.charAt(0).toUpperCase() + part.slice(1);
      }
      return part;
    })
    .join("");
}

export async function cleanInventoryData(
  items: any[],
  dataSourceName?: string,
): Promise<{
  items: any[];
  noSizeRemoved: number;
  colorsFixed: number;
  aiColorsFixed: number;
  duplicatesRemoved: number;
}> {
  const itemsWithSize = items.filter((item) => {
    // Use ?? to preserve size "0" (0 is falsy but valid size)
    const size = String(item.size ?? "").trim();
    return size.length > 0;
  });
  const noSizeRemoved = items.length - itemsWithSize.length;

  const isJovaniSaleSource = dataSourceName?.toLowerCase() === "jovani sales";

  let stylesRemapped = 0;
  let itemsAfterJovaniFix = itemsWithSize;

  if (isJovaniSaleSource) {
    const jovaniStylePattern =
      /^(?:#\d{5,6}|(?:JVN|JB|AL)\d{3,6}|D\d{3,5}|\d{5,6})$/i;

    itemsAfterJovaniFix = itemsWithSize.map((item) => {
      const color = String(item.color || "").trim();
      if (jovaniStylePattern.test(color)) {
        let normalizedStyle = color.toUpperCase();
        if (normalizedStyle.startsWith("#")) {
          normalizedStyle = normalizedStyle.substring(1);
        }
        const newStyle = `Jovani ${normalizedStyle}`;
        console.log(
          `[Clean] Remapping Jovani style from color to style: ${item.style} + ${color} -> ${newStyle}`,
        );
        stylesRemapped++;
        return {
          ...item,
          style: newStyle,
          color: "Default",
        };
      }
      return item;
    });

    if (stylesRemapped > 0) {
      console.log(
        `[Clean] Remapped ${stylesRemapped} items with Jovani style numbers from color to style field`,
      );
    }
  }

  const colorMappings = await storage.getColorMappings();
  const colorMap = new Map<string, string>();
  const existingBadColors = new Set<string>();
  for (const mapping of colorMappings) {
    const normalizedBad = mapping.badColor.trim().toLowerCase();
    colorMap.set(normalizedBad, mapping.goodColor);
    existingBadColors.add(normalizedBad);
  }

  const unmappedColorCodes = new Set<string>();
  for (const item of itemsAfterJovaniFix) {
    const color = String(item.color || "").trim();
    if (color && !colorMap.has(color.toLowerCase()) && isColorCode(color)) {
      unmappedColorCodes.add(color);
    }
  }

  const aiSuggestedMappings = new Map<string, string>();

  if (unmappedColorCodes.size > 0) {
    try {
      const suggestions = await suggestColorCorrections(
        Array.from(unmappedColorCodes),
      );

      const newMappings: { badColor: string; goodColor: string }[] = [];
      for (const suggestion of suggestions) {
        const normalizedBad = suggestion.badColor.trim().toLowerCase();
        const normalizedGood = formatColorName(suggestion.goodColor);

        if (
          suggestion.confidence >= 0.7 &&
          normalizedBad !== normalizedGood.toLowerCase() &&
          !existingBadColors.has(normalizedBad)
        ) {
          colorMap.set(normalizedBad, normalizedGood);
          aiSuggestedMappings.set(normalizedBad, normalizedGood);
          newMappings.push({
            badColor: suggestion.badColor.trim(),
            goodColor: normalizedGood,
          });
        }
      }

      if (newMappings.length > 0) {
        try {
          await storage.createColorMappings(newMappings);
        } catch (dbError) {
          console.error("Failed to save AI color mappings:", dbError);
        }
      }
    } catch (error) {
      console.error("AI color suggestion failed:", error);
    }
  }

  let colorsFixed = 0;
  let aiColorsFixed = 0;
  const itemsWithFixedColors = itemsAfterJovaniFix.map((item) => {
    const color = String(item.color || "").trim();
    const normalizedColor = color.toLowerCase();
    const mappedColor = colorMap.get(normalizedColor);

    if (mappedColor && mappedColor.toLowerCase() !== normalizedColor) {
      if (aiSuggestedMappings.has(normalizedColor)) {
        aiColorsFixed++;
      } else {
        colorsFixed++;
      }
      const newColor = formatColorName(mappedColor);
      // FIX: Rebuild SKU with the corrected color to avoid SKU/color mismatch
      // Convert slashes and spaces to hyphens for Shopify compatibility
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

  let d0Removed = 0;
  let d0Converted = 0;

  const styleColorWith00 = new Set<string>();
  for (const item of itemsWithFixedColors) {
    const size = String(item.size || "")
      .trim()
      .toUpperCase();
    if (size === "00") {
      const key = `${(item.style || "").toLowerCase()}|${(item.color || "").toLowerCase()}`;
      styleColorWith00.add(key);
    }
  }

  const itemsAfterD0Normalization = itemsWithFixedColors
    .filter((item) => {
      const size = String(item.size || "")
        .trim()
        .toUpperCase();
      if (size === "D0") {
        const key = `${(item.style || "").toLowerCase()}|${(item.color || "").toLowerCase()}`;
        if (styleColorWith00.has(key)) {
          d0Removed++;
          return false;
        }
      }
      return true;
    })
    .map((item) => {
      const size = String(item.size || "")
        .trim()
        .toUpperCase();
      if (size === "D0") {
        d0Converted++;
        return { ...item, size: "00" };
      }
      return item;
    });

  if (d0Removed > 0 || d0Converted > 0) {
    console.log(
      `[Import] D0 size normalization: ${d0Removed} D0 items removed (00 exists), ${d0Converted} D0 items converted to 00`,
    );
  }

  const seen = new Map<string, any>();

  for (const item of itemsAfterD0Normalization) {
    const key = `${(item.style || "").toLowerCase()}|${(item.color || "").toLowerCase()}|${String(item.size || "").toLowerCase()}`;
    const existing = seen.get(key);

    if (!existing) {
      seen.set(key, item);
    } else {
      const existingStock =
        typeof existing.stock === "number"
          ? existing.stock
          : parseInt(existing.stock) || 0;
      const currentStock =
        typeof item.stock === "number" ? item.stock : parseInt(item.stock) || 0;
      if (currentStock > existingStock) {
        seen.set(key, item);
      }
    }
  }

  const uniqueItems = Array.from(seen.values());
  return {
    items: uniqueItems,
    noSizeRemoved,
    colorsFixed,
    aiColorsFixed,
    duplicatesRemoved: itemsAfterD0Normalization.length - uniqueItems.length,
  };
}

/**
 * Deduplicates items by style-color-size and zeroes out stock for future ship dates.
 *
 * 1. Any item with a future ship date (shipDate + offsetDays > today) gets stock=0
 * 2. Duplicate style-color-size items are collapsed to 1:
 *    - Prefer item with highest stock (current inventory)
 *    - If all stock=0: keep the one with the closest future ship date
 *
 * Called from ALL import paths (manual, email, URL, combine).
 */
export function deduplicateAndZeroFutureStock(
  items: any[],
  dateOffsetDays: number = 0,
): { items: any[]; duplicatesRemoved: number; stockZeroed: number } {
  if (!items || items.length === 0) {
    return { items: [], duplicatesRemoved: 0, stockZeroed: 0 };
  }

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  let stockZeroed = 0;

  function parseShipDate(shipDate: any): Date | null {
    if (!shipDate) return null;
    try {
      const dateStr = String(shipDate).trim().toLowerCase();
      if (!dateStr || dateStr === "n/a" || dateStr === "na" || dateStr === "tbd" || dateStr === "none") return null;

      const isoMatch = dateStr.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
      const usMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
      const usShortMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2})$/);

      let d: Date;
      if (isoMatch) {
        d = new Date(parseInt(isoMatch[1]), parseInt(isoMatch[2]) - 1, parseInt(isoMatch[3]));
      } else if (usMatch) {
        d = new Date(parseInt(usMatch[3]), parseInt(usMatch[1]) - 1, parseInt(usMatch[2]));
      } else if (usShortMatch) {
        d = new Date(2000 + parseInt(usShortMatch[3]), parseInt(usShortMatch[1]) - 1, parseInt(usShortMatch[2]));
      } else {
        d = new Date(dateStr);
      }
      if (isNaN(d.getTime())) return null;
      d.setHours(0, 0, 0, 0);
      return d;
    } catch {
      return null;
    }
  }

  function isFutureWithOffset(shipDate: any): boolean {
    const parsed = parseShipDate(shipDate);
    if (!parsed) return false;
    const adjusted = new Date(parsed);
    adjusted.setDate(adjusted.getDate() + dateOffsetDays);
    return adjusted > today;
  }

  // Step A: Zero out stock for items with future ship dates (respecting offset)
  const processedItems = items.map((item) => {
    if (!item.shipDate) return item;
    if (isFutureWithOffset(item.shipDate)) {
      const currentStock = typeof item.stock === "number" ? item.stock : parseInt(item.stock) || 0;
      if (currentStock > 0) stockZeroed++;
      return { ...item, stock: 0 };
    }
    return item;
  });

  // Step B: Dedup by style-color-size — keep 1 variant per key
  const groups = new Map<string, any[]>();
  for (const item of processedItems) {
    const key = `${(item.style || "").toLowerCase()}|${(item.color || "").toLowerCase()}|${String(item.size || "").toLowerCase()}`;
    const group = groups.get(key);
    if (group) {
      group.push(item);
    } else {
      groups.set(key, [item]);
    }
  }

  const dedupedItems: any[] = [];
  for (const [, group] of groups) {
    if (group.length === 1) {
      dedupedItems.push(group[0]);
      continue;
    }

    // Multiple items for same style-color-size
    // Prefer items with stock > 0 (current/arrived inventory)
    const withStock = group.filter((item) => {
      const stock = typeof item.stock === "number" ? item.stock : parseInt(item.stock) || 0;
      return stock > 0;
    });

    if (withStock.length > 0) {
      // Keep highest stock item
      let best = withStock[0];
      for (let i = 1; i < withStock.length; i++) {
        const bestStock = typeof best.stock === "number" ? best.stock : parseInt(best.stock) || 0;
        const curStock = typeof withStock[i].stock === "number" ? withStock[i].stock : parseInt(withStock[i].stock) || 0;
        if (curStock > bestStock) best = withStock[i];
      }
      dedupedItems.push(best);
      continue;
    }

    // All stock=0 — keep the one with closest future ship date
    const withFutureDates = group.filter((item) => isFutureWithOffset(item.shipDate));

    if (withFutureDates.length > 0) {
      withFutureDates.sort((a, b) => {
        const aDate = parseShipDate(a.shipDate)!;
        const bDate = parseShipDate(b.shipDate)!;
        return aDate.getTime() - bDate.getTime(); // Earliest first = closest
      });
      dedupedItems.push(withFutureDates[0]);
      continue;
    }

    // No future dates — keep most recent past date, or first if no dates
    const withAnyDates = group.filter((item) => parseShipDate(item.shipDate) !== null);
    if (withAnyDates.length > 0) {
      withAnyDates.sort((a, b) => {
        const aDate = parseShipDate(a.shipDate)!;
        const bDate = parseShipDate(b.shipDate)!;
        return bDate.getTime() - aDate.getTime(); // Most recent first
      });
      dedupedItems.push(withAnyDates[0]);
      continue;
    }

    // No dates at all — keep first
    dedupedItems.push(group[0]);
  }

  const duplicatesRemoved = processedItems.length - dedupedItems.length;
  if (duplicatesRemoved > 0 || stockZeroed > 0) {
    console.log(
      `[Dedup] ${processedItems.length} → ${dedupedItems.length} items (${duplicatesRemoved} duplicates removed, ${stockZeroed} future items stock zeroed, offset=${dateOffsetDays} days)`,
    );
  }

  return { items: dedupedItems, duplicatesRemoved, stockZeroed };
}

export async function applyVariantRules(
  items: any[],
  dataSourceId: string,
  configOverride?: {
    filterZeroStock?: boolean;
    filterZeroStockWithFutureDates?: boolean;
    skipVariantRules?: boolean;
  },
): Promise<{
  items: any[];
  filteredCount: number;
  addedCount: number;
  sizeFiltered?: number;
  stats?: { sizesExpanded?: number };
}> {
  const dataSource = await storage.getDataSource(dataSourceId);
  const rules = await storage.getVariantRulesByDataSource(dataSourceId);
  const enabledRules = configOverride?.skipVariantRules
    ? []
    : rules.filter((r) => r.enabled);

  let processedItems = [...items];
  let totalFiltered = 0;
  let totalAdded = 0;
  let sizeFiltered = 0;

  const sizeLimitConfig = (dataSource as any)?.sizeLimitConfig as
    | SizeLimitConfig
    | undefined;

  if (sizeLimitConfig?.enabled) {
    // Log size limit configuration
    console.log(
      `[SizeLimits] Config: min=${sizeLimitConfig.minSize || "none"}, max=${sizeLimitConfig.maxSize || "none"}, ` +
        `minLetter=${sizeLimitConfig.minLetterSize || "none"}, maxLetter=${sizeLimitConfig.maxLetterSize || "none"}, ` +
        `prefixOverrides=${sizeLimitConfig.prefixOverrides?.length || 0}`,
    );
    if (sizeLimitConfig.prefixOverrides?.length) {
      for (const override of sizeLimitConfig.prefixOverrides) {
        console.log(
          `[SizeLimits] Prefix override: pattern="${override.pattern}", ` +
            `minSize=${override.minSize || "inherit"}, maxSize=${override.maxSize || "inherit"}`,
        );
      }
    }

    const beforeCount = processedItems.length;
    let matchedOverrides = 0;
    let defaultMatches = 0;

    processedItems = processedItems.filter((item) => {
      const size = String(item.size || "").trim();
      const style = String(item.style || "");

      // Check which override (if any) matches this style
      let matchedPattern: string | null = null;
      if (sizeLimitConfig.prefixOverrides?.length) {
        for (const override of sizeLimitConfig.prefixOverrides) {
          if (override.pattern) {
            try {
              const regex = new RegExp(override.pattern, "i");
              if (regex.test(style)) {
                matchedPattern = override.pattern;
                matchedOverrides++;
                break;
              }
            } catch (e) {
              if (
                style.toLowerCase().startsWith(override.pattern.toLowerCase())
              ) {
                matchedPattern = override.pattern;
                matchedOverrides++;
                break;
              }
            }
          }
        }
      }
      if (!matchedPattern) {
        defaultMatches++;
      }

      return isSizeAllowed(size, sizeLimitConfig, style);
    });

    sizeFiltered = beforeCount - processedItems.length;
    console.log(
      `[SizeLimits] Processed ${beforeCount} items: ${matchedOverrides} matched prefix overrides, ` +
        `${defaultMatches} used default limits, ${sizeFiltered} filtered out`,
    );
  }

  // Check if zero-stock filtering is enabled
  // Use configOverride if provided, otherwise fall back to dataSource setting
  const shouldFilterZeroStock =
    configOverride?.filterZeroStock !== undefined
      ? configOverride.filterZeroStock
      : dataSource?.filterZeroStock;

  const shouldFilterZeroStockWithFutureDates =
    configOverride?.filterZeroStockWithFutureDates !== undefined
      ? configOverride.filterZeroStockWithFutureDates
      : (dataSource as any)?.filterZeroStockWithFutureDates;

  console.log(
    `[VariantRules] filterZeroStock: config=${configOverride?.filterZeroStock}, db=${dataSource?.filterZeroStock}, final=${shouldFilterZeroStock}, withFutureDates=${shouldFilterZeroStockWithFutureDates}`,
  );

  if (shouldFilterZeroStock) {
    const beforeCount = processedItems.length;
    let preOrderPreserved = 0;
    let futureStockPreserved = 0;
    let discontinuedFiltered = 0;
    let expiredFutureStockFiltered = 0;

    processedItems = processedItems.filter((item) => {
      const stock =
        typeof item.stock === "number" ? item.stock : parseInt(item.stock) || 0;

      // CRITICAL FIX: Check if ship date has passed for items with future stock flags
      // If ship date is in the past AND stock is still 0, filter out the item
      if (item.hasFutureStock || item.preserveZeroStock) {
        const shipDatePassed = isShipDateInPast(item.shipDate);
        if (shipDatePassed && stock <= 0) {
          // Ship date has passed but no stock arrived - filter out
          expiredFutureStockFiltered++;
          console.log(
            `[FilterZeroStock] Filtering expired future stock: ${item.sku || item.style} (shipDate: ${item.shipDate}, stock: ${stock})`,
          );
          return false;
        }
        // If filterZeroStockWithFutureDates is enabled, filter zero-stock even with future dates
        if (shouldFilterZeroStockWithFutureDates && stock <= 0) {
          console.log(
            `[FilterZeroStock] Filtering zero-stock with future date (filterZeroStockWithFutureDates): ${item.sku || item.style} (shipDate: ${item.shipDate})`,
          );
          return false;
        }
        // Ship date is still in the future OR has stock - preserve
        futureStockPreserved++;
        return true;
      }

      // Check shipDate directly (items without hasFutureStock flag)
      // CRITICAL FIX: Only treat as pre-order if shipDate is a valid date (not "N/A", "TBD", etc.)
      if (item.shipDate && isValidShipDate(item.shipDate)) {
        const shipDatePassed = isShipDateInPast(item.shipDate);
        if (shipDatePassed && stock <= 0) {
          // Ship date has passed but no stock arrived - filter out
          expiredFutureStockFiltered++;
          console.log(
            `[FilterZeroStock] Filtering expired pre-order: ${item.sku || item.style} (shipDate: ${item.shipDate}, stock: ${stock})`,
          );
          return false;
        }
        // If filterZeroStockWithFutureDates is enabled, filter zero-stock even with future dates
        if (shouldFilterZeroStockWithFutureDates && stock <= 0) {
          console.log(
            `[FilterZeroStock] Filtering zero-stock with future date (filterZeroStockWithFutureDates): ${item.sku || item.style} (shipDate: ${item.shipDate})`,
          );
          return false;
        }
        // Ship date is still in the future - preserve
        preOrderPreserved++;
        return true;
      }

      if (item.discontinued && stock <= 0) {
        discontinuedFiltered++;
        return false;
      }

      return stock > 0;
    });

    totalFiltered += beforeCount - processedItems.length;
    console.log(
      `[FilterZeroStock] Filtered ${totalFiltered} zero-stock items from ${beforeCount} items` +
        (preOrderPreserved > 0
          ? ` (preserved ${preOrderPreserved} pre-order items)`
          : "") +
        (futureStockPreserved > 0
          ? ` (preserved ${futureStockPreserved} future stock items)`
          : "") +
        (expiredFutureStockFiltered > 0
          ? ` (filtered ${expiredFutureStockFiltered} EXPIRED future stock items)`
          : "") +
        (discontinuedFiltered > 0
          ? ` (filtered ${discontinuedFiltered} discontinued items)`
          : ""),
    );
  }

  if (enabledRules.length === 0) {
    return {
      items: processedItems,
      filteredCount: totalFiltered,
      addedCount: 0,
      sizeFiltered,
    };
  }

  for (const rule of enabledRules) {
    if (
      rule.stockMax === 0 &&
      (rule.stockMin == null || rule.stockMin === undefined)
    ) {
      console.log(
        `[VariantRules] Found enabled rule with stockMax=0: "${rule.name}" (id: ${rule.id}) - this will filter zero stock items`,
      );
      const beforeCount = processedItems.length;
      processedItems = processedItems.filter((item) => {
        const stock =
          typeof item.stock === "number"
            ? item.stock
            : parseInt(item.stock) || 0;

        // CRITICAL FIX: Check if ship date has passed for items with future stock flags
        // Only treat shipDate as valid if it's a real date (not "N/A", "TBD", etc.)
        if (
          item.hasFutureStock ||
          item.preserveZeroStock ||
          (item.shipDate && isValidShipDate(item.shipDate))
        ) {
          const shipDatePassed = isShipDateInPast(item.shipDate);
          if (shipDatePassed && stock <= 0) {
            // Ship date has passed but no stock arrived - filter out
            return false;
          }
          // Ship date is still in the future OR has stock - preserve
          return true;
        }

        return stock > 0;
      });
      totalFiltered += beforeCount - processedItems.length;
    }
  }

  const alreadyExpanded = new Set<string>();

  // PERFORMANCE FIX: Build a Map for O(1) item lookups instead of O(n) .find() calls
  const existingSizes = new Set<string>();
  const itemsByKey = new Map<string, any>();
  for (const item of processedItems) {
    const key = `${item.style}|${item.color}|${String(item.size || "")
      .trim()
      .toUpperCase()}`;
    existingSizes.add(key);
    itemsByKey.set(key, item);
  }

  for (const rule of enabledRules) {
    if (!rule.expandSizes) continue;
    if ((rule.expandDownCount ?? 0) <= 0 && (rule.expandUpCount ?? 0) <= 0)
      continue;

    const downCount = rule.expandDownCount ?? 0;
    const upCount = rule.expandUpCount ?? 0;
    const minStock = rule.minTriggerStock ?? 1;
    const expandedStock = rule.expandedStock ?? 1;

    const newExpandedItems: any[] = [];

    for (const item of processedItems) {
      // Skip items that are already expanded - prevents double-adding during sync
      if (item.rawData?._expanded || item.isExpandedSize) continue;

      const stock =
        typeof item.stock === "number" ? item.stock : parseInt(item.stock) || 0;
      if (stock < minStock) continue;

      const currentSizeStr = String(item.size ?? "").trim();

      const key = `${item.style}|${item.color}|${currentSizeStr}`;
      if (alreadyExpanded.has(key)) continue;
      alreadyExpanded.add(key);

      const normalizedSize = currentSizeStr.toUpperCase().trim();
      let currentIndex: number | undefined;
      let sizeSequence: string[];

      if (LETTER_SIZE_MAP[normalizedSize] !== undefined) {
        currentIndex = LETTER_SIZE_MAP[normalizedSize];
        sizeSequence = LETTER_SIZES;
      } else if (NUMERIC_SIZE_MAP[currentSizeStr] !== undefined) {
        currentIndex = NUMERIC_SIZE_MAP[currentSizeStr];
        sizeSequence = NUMERIC_SIZES;
      } else {
        continue;
      }

      for (let i = 1; i <= downCount; i++) {
        const newIndex = currentIndex - i;
        if (newIndex >= 0 && newIndex < sizeSequence.length) {
          const newSize = sizeSequence[newIndex];

          if (
            sizeLimitConfig?.enabled &&
            !isSizeAllowed(newSize, sizeLimitConfig, item.style)
          ) {
            continue;
          }

          const newKey = `${item.style}|${item.color}|${newSize.toUpperCase()}`;

          if (existingSizes.has(newKey)) {
            // PERFORMANCE FIX: O(1) Map lookup instead of O(n) .find() - fixes O(n²) algorithm
            const existingItem = itemsByKey.get(newKey);
            if (existingItem) {
              const existingStock =
                typeof existingItem.stock === "number"
                  ? existingItem.stock
                  : parseInt(existingItem.stock) || 0;
              if (existingStock === 0) {
                existingItem.stock = expandedStock;
                existingItem.shipDate = null;
                existingItem.isExpandedSize = true;
                existingItem.rawData = {
                  ...existingItem.rawData,
                  _expanded: true,
                  _fromSize: currentSizeStr,
                };
              }
            }
            continue;
          }

          const newSku = generateExpandedSku(item.style, item.color, newSize);
          if (!newSku) {
            console.warn(
              `[VariantRules] Skipping expanded size ${newSize} for ${item.style}/${item.color} - could not generate SKU`,
            );
            continue;
          }
          existingSizes.add(newKey);
          newExpandedItems.push({
            ...item,
            sku: newSku,
            size: newSize,
            stock: expandedStock,
            shipDate: null,
            isExpandedSize: true,
            rawData: {
              ...item.rawData,
              _expanded: true,
              _fromSize: currentSizeStr,
            },
          });
        }
      }

      for (let i = 1; i <= upCount; i++) {
        const newIndex = currentIndex + i;
        if (newIndex >= 0 && newIndex < sizeSequence.length) {
          const newSize = sizeSequence[newIndex];

          if (
            sizeLimitConfig?.enabled &&
            !isSizeAllowed(newSize, sizeLimitConfig, item.style)
          ) {
            continue;
          }

          const newKey = `${item.style}|${item.color}|${newSize.toUpperCase()}`;

          if (existingSizes.has(newKey)) {
            // PERFORMANCE FIX: O(1) Map lookup instead of O(n) .find() - fixes O(n²) algorithm
            const existingItem = itemsByKey.get(newKey);
            if (existingItem) {
              const existingStock =
                typeof existingItem.stock === "number"
                  ? existingItem.stock
                  : parseInt(existingItem.stock) || 0;
              if (existingStock === 0) {
                existingItem.stock = expandedStock;
                existingItem.shipDate = null;
                existingItem.isExpandedSize = true;
                existingItem.rawData = {
                  ...existingItem.rawData,
                  _expanded: true,
                  _fromSize: currentSizeStr,
                };
              }
            }
            continue;
          }

          const newSku = generateExpandedSku(item.style, item.color, newSize);
          if (!newSku) {
            console.warn(
              `[VariantRules] Skipping expanded size ${newSize} for ${item.style}/${item.color} - could not generate SKU`,
            );
            continue;
          }
          existingSizes.add(newKey);
          newExpandedItems.push({
            ...item,
            sku: newSku,
            size: newSize,
            stock: expandedStock,
            shipDate: null,
            isExpandedSize: true,
            rawData: {
              ...item.rawData,
              _expanded: true,
              _fromSize: currentSizeStr,
            },
          });
        }
      }
    }

    processedItems = [...processedItems, ...newExpandedItems];
    totalAdded += newExpandedItems.length;
  }

  processedItems.sort((a, b) => {
    const styleA = (a.style || "").toLowerCase();
    const styleB = (b.style || "").toLowerCase();
    if (styleA !== styleB) {
      return styleA.localeCompare(styleB);
    }

    const colorA = (a.color || "").toLowerCase();
    const colorB = (b.color || "").toLowerCase();
    if (colorA !== colorB) {
      return colorA.localeCompare(colorB);
    }

    const sizeA = String(a.size || "").trim();
    const sizeB = String(b.size || "").trim();
    const normalizedA = sizeA.toUpperCase();
    const normalizedB = sizeB.toUpperCase();

    const letterIndexA = LETTER_SIZE_MAP[normalizedA];
    const letterIndexB = LETTER_SIZE_MAP[normalizedB];
    if (letterIndexA !== undefined && letterIndexB !== undefined) {
      return letterIndexA - letterIndexB;
    }

    const numIndexA = NUMERIC_SIZE_MAP[sizeA];
    const numIndexB = NUMERIC_SIZE_MAP[sizeB];
    if (numIndexA !== undefined && numIndexB !== undefined) {
      return numIndexA - numIndexB;
    }

    return sizeA.localeCompare(sizeB);
  });

  return {
    items: processedItems,
    filteredCount: totalFiltered,
    addedCount: totalAdded,
    sizeFiltered,
  };
}

// ============ IMPORT RULES PROCESSING FUNCTIONS ============

// Type definitions for import rules
export interface DiscontinuedRule {
  column: string;
  condition: "contains" | "equals" | "startsWith" | "endsWith";
  values: string[];
}

export interface DiscontinuedConfig {
  enabled?: boolean;
  keywords?: string[];
  skipDiscontinued?: boolean;
}

export interface SalePriceConfig {
  enabled?: boolean;
  salePriceColumn?: string;
  multiplier?: number;
  useShopifyAsCompareAt?: boolean;
  skipZeroPrice?: boolean;
}

export interface PriceFloorCeilingConfig {
  enabled?: boolean;
  minPrice?: number;
  maxPrice?: number;
  action?: "skip" | "clamp";
}

export interface RequiredFieldsConfig {
  requireStyle?: boolean;
  requireSku?: boolean;
  requirePrice?: boolean;
  requireStock?: boolean;
}

export interface DateFormatConfig {
  shipDateFormat?: string;
  autoDetect?: boolean;
}

export interface ValueReplacementRule {
  field: string;
  from: string;
  to: string;
  caseSensitive?: boolean;
}

/**
 * Filter out items based on discontinued detection
 * Uses the "discontinued" field on items (set during parsing from mapped column)
 * Checks for keywords in the discontinued value and excludes matching rows
 */
export function filterDiscontinuedItems(
  items: any[],
  config: DiscontinuedConfig | DiscontinuedRule[] | undefined,
): { items: any[]; filteredCount: number } {
  if (!config) {
    return { items, filteredCount: 0 };
  }

  // Handle the new simplified config format
  if (!Array.isArray(config)) {
    // New format: DiscontinuedConfig with enabled/keywords
    if (!config.enabled || !config.keywords || config.keywords.length === 0) {
      return { items, filteredCount: 0 };
    }

    const beforeCount = items.length;
    const keywordsLower = config.keywords.map((k) => k.toLowerCase());

    const filteredItems = items.filter((item) => {
      // Check the "discontinued" field on the item (set from mapped column during parsing)
      // Also check rawData.statusMarker for Tarik Ediz format where "D" means discontinued
      const discontinuedValue = String(
        item.discontinued ||
          item.rawData?.discontinued ||
          item.rawData?.statusMarker ||
          "",
      )
        .trim()
        .toLowerCase();
      if (!discontinuedValue) return true; // No discontinued value, keep item

      // Check if any keyword matches (case-insensitive contains match)
      for (const keyword of keywordsLower) {
        if (discontinuedValue.includes(keyword)) {
          console.log(
            `[DiscontinuedFilter] Excluding item: ${item.style || item.sku} - discontinued value "${item.discontinued}" contains "${keyword}"`,
          );
          return false; // Exclude this item
        }
      }
      return true; // Keep item
    });

    const filteredCount = beforeCount - filteredItems.length;
    if (filteredCount > 0) {
      console.log(
        `[DiscontinuedFilter] Filtered ${filteredCount} discontinued items from ${beforeCount} items`,
      );
    }
    return { items: filteredItems, filteredCount };
  }

  // Legacy format: array of DiscontinuedRule
  const rules = config as DiscontinuedRule[];
  if (rules.length === 0) {
    return { items, filteredCount: 0 };
  }

  const beforeCount = items.length;
  const filteredItems = items.filter((item) => {
    for (const rule of rules) {
      const columnValue = String(
        item[rule.column] || item.rawData?.[rule.column] || "",
      ).trim();
      if (!columnValue) continue;

      const valueLower = columnValue.toLowerCase();

      for (const checkValue of rule.values) {
        const checkLower = checkValue.toLowerCase();
        let matches = false;

        switch (rule.condition) {
          case "contains":
            matches = valueLower.includes(checkLower);
            break;
          case "equals":
            matches = valueLower === checkLower;
            break;
          case "startsWith":
            matches = valueLower.startsWith(checkLower);
            break;
          case "endsWith":
            matches = valueLower.endsWith(checkLower);
            break;
        }

        if (matches) {
          console.log(
            `[DiscontinuedFilter] Excluding item: ${item.style || item.sku} - column "${rule.column}" ${rule.condition} "${checkValue}"`,
          );
          return false; // Exclude this item
        }
      }
    }
    return true; // Keep this item
  });

  const filteredCount = beforeCount - filteredItems.length;
  if (filteredCount > 0) {
    console.log(
      `[DiscontinuedFilter] Filtered ${filteredCount} discontinued items from ${beforeCount} items`,
    );
  }

  return { items: filteredItems, filteredCount };
}

/**
 * Apply sale pricing from a single file with sale price column
 * Calculates: sale price × multiplier = final price
 * Sets setSaleCompareAt flag for sync to use Shopify price as compare-at
 */
export function applySalePricing(
  items: any[],
  config: SalePriceConfig | undefined,
  rawDataRows: any[],
): { items: any[]; processedCount: number; zeroSkipped: number } {
  if (!config?.enabled) {
    return { items, processedCount: 0, zeroSkipped: 0 };
  }

  const multiplier = config.multiplier || 1;
  let processedCount = 0;
  let zeroSkipped = 0;

  const processedItems = items.map((item, index) => {
    // Get sale price from:
    // 1. item.salePrice (set during parsing from mapped "Sale Price" column)
    // 2. rawData using configured salePriceColumn (legacy)
    // 3. item[salePriceColumn] (fallback)
    let salePriceRaw = item.salePrice;
    if (
      (salePriceRaw === undefined ||
        salePriceRaw === null ||
        salePriceRaw === "") &&
      config.salePriceColumn
    ) {
      const rawRow = rawDataRows[index] || item.rawData || {};
      salePriceRaw =
        rawRow[config.salePriceColumn] || item[config.salePriceColumn];
    }

    if (
      salePriceRaw === undefined ||
      salePriceRaw === null ||
      salePriceRaw === ""
    ) {
      return item; // No sale price, keep original
    }

    // Parse sale price (remove $ and commas)
    const salePriceStr = String(salePriceRaw).replace(/[$,]/g, "").trim();
    const salePrice = parseFloat(salePriceStr);

    if (isNaN(salePrice)) {
      return item; // Invalid sale price
    }

    // Handle zero price
    if (salePrice === 0) {
      if (config.skipZeroPrice) {
        zeroSkipped++;
        return {
          ...item,
          useShopifyPrice: true, // Keep Shopify's current price
          setSaleCompareAt: false,
        };
      }
    }

    // Calculate final price
    const finalPrice = (salePrice * multiplier).toFixed(2);
    processedCount++;

    return {
      ...item,
      price: finalPrice,
      setSaleCompareAt: config.useShopifyAsCompareAt || false,
    };
  });

  console.log(
    `[SalePricing] Processed ${processedCount} items with sale pricing (multiplier: ${multiplier})` +
      (zeroSkipped > 0
        ? `, ${zeroSkipped} zero-price items using Shopify price`
        : ""),
  );

  return { items: processedItems, processedCount, zeroSkipped };
}

/**
 * Handle zero prices - set useShopifyPrice flag when file price is $0
 * Works with both regular and sale file pricing
 */
export function handleZeroPrice(
  items: any[],
  skipZeroPrice: boolean | undefined,
): { items: any[]; zeroCount: number } {
  if (!skipZeroPrice) {
    return { items, zeroCount: 0 };
  }

  let zeroCount = 0;

  const processedItems = items.map((item) => {
    const priceStr = String(item.price || "")
      .replace(/[$,]/g, "")
      .trim();
    const price = parseFloat(priceStr);

    if (price === 0 || isNaN(price) || !priceStr) {
      zeroCount++;
      return {
        ...item,
        useShopifyPrice: true,
      };
    }
    return item;
  });

  if (zeroCount > 0) {
    console.log(
      `[ZeroPriceHandler] Marked ${zeroCount} items to use Shopify price (file price was $0 or empty)`,
    );
  }

  return { items: processedItems, zeroCount };
}

/**
 * Apply price floor/ceiling limits
 * Can skip items outside range or clamp prices to min/max
 */
export function applyPriceFloorCeiling(
  items: any[],
  config: PriceFloorCeilingConfig | undefined,
): { items: any[]; skippedCount: number; clampedCount: number } {
  if (!config?.enabled) {
    return { items, skippedCount: 0, clampedCount: 0 };
  }

  const minPrice = config.minPrice ?? 0;
  const maxPrice = config.maxPrice ?? Infinity;
  const action = config.action || "skip";

  let skippedCount = 0;
  let clampedCount = 0;

  const processedItems: any[] = [];

  for (const item of items) {
    const priceStr = String(item.price || "")
      .replace(/[$,]/g, "")
      .trim();
    const price = parseFloat(priceStr);

    if (isNaN(price)) {
      processedItems.push(item);
      continue;
    }

    if (price < minPrice || price > maxPrice) {
      if (action === "skip") {
        skippedCount++;
        console.log(
          `[PriceLimit] Skipping ${item.style || item.sku}: price $${price} outside range $${minPrice}-$${maxPrice}`,
        );
        continue; // Skip this item
      } else {
        // Clamp
        const clampedPrice = Math.max(minPrice, Math.min(maxPrice, price));
        clampedCount++;
        processedItems.push({
          ...item,
          price: clampedPrice.toFixed(2),
        });
        continue;
      }
    }

    processedItems.push(item);
  }

  if (skippedCount > 0 || clampedCount > 0) {
    console.log(
      `[PriceLimit] ${skippedCount} items skipped, ${clampedCount} items clamped (range: $${minPrice}-$${maxPrice})`,
    );
  }

  return { items: processedItems, skippedCount, clampedCount };
}

/**
 * Filter items by minimum stock threshold
 */
export function filterByStockThreshold(
  items: any[],
  minThreshold: number | undefined,
  enabled: boolean = true,
): { items: any[]; filteredCount: number } {
  if (!enabled || minThreshold === undefined || minThreshold <= 0) {
    return { items, filteredCount: 0 };
  }

  const beforeCount = items.length;
  let futureStockPreserved = 0;
  let expiredFutureStockFiltered = 0;
  const filteredItems = items.filter((item) => {
    const stock =
      typeof item.stock === "number" ? item.stock : parseInt(item.stock) || 0;

    // CRITICAL FIX: Check if ship date has passed for items with future stock flags
    // Items with shipDate should be preserved ONLY if the date is still in the future
    // Only treat shipDate as valid if it's a real date (not "N/A", "TBD", etc.)
    if (
      item.hasFutureStock ||
      item.preserveZeroStock ||
      (item.shipDate && isValidShipDate(item.shipDate))
    ) {
      const shipDatePassed = isShipDateInPast(item.shipDate);
      if (shipDatePassed && stock < minThreshold) {
        // Ship date has passed but stock is still below threshold - filter out
        expiredFutureStockFiltered++;
        return false;
      }
      // Ship date is still in the future OR has enough stock - preserve
      futureStockPreserved++;
      return true;
    }

    return stock >= minThreshold;
  });

  const filteredCount = beforeCount - filteredItems.length;
  if (filteredCount > 0 || futureStockPreserved > 0) {
    console.log(
      `[StockThreshold] Filtered ${filteredCount} items with stock below ${minThreshold}` +
        (futureStockPreserved > 0
          ? ` (preserved ${futureStockPreserved} future stock items)`
          : "") +
        (expiredFutureStockFiltered > 0
          ? ` (filtered ${expiredFutureStockFiltered} EXPIRED future stock items)`
          : ""),
    );
  }

  return { items: filteredItems, filteredCount };
}

/**
 * Validate required fields - returns validation errors for items missing required fields
 */
export function validateRequiredFields(
  items: any[],
  config: RequiredFieldsConfig | undefined,
): { valid: boolean; errors: string[]; invalidCount: number } {
  if (!config) {
    return { valid: true, errors: [], invalidCount: 0 };
  }

  const errors: string[] = [];
  let invalidCount = 0;

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const itemErrors: string[] = [];

    if (config.requireStyle && !item.style?.trim()) {
      itemErrors.push("missing style");
    }
    if (config.requireSku && !item.sku?.trim()) {
      itemErrors.push("missing SKU");
    }
    if (config.requirePrice) {
      const price = String(item.price || "")
        .replace(/[$,]/g, "")
        .trim();
      if (!price || isNaN(parseFloat(price))) {
        itemErrors.push("missing/invalid price");
      }
    }
    if (config.requireStock) {
      const stock = parseInt(item.stock);
      if (isNaN(stock)) {
        itemErrors.push("missing/invalid stock");
      }
    }

    if (itemErrors.length > 0) {
      invalidCount++;
      if (errors.length < 10) {
        errors.push(
          `Row ${i + 1} (${item.style || item.sku || "unknown"}): ${itemErrors.join(", ")}`,
        );
      }
    }
  }

  if (invalidCount > 10) {
    errors.push(
      `...and ${invalidCount - 10} more items with validation errors`,
    );
  }

  return {
    valid: invalidCount === 0,
    errors,
    invalidCount,
  };
}

/**
 * Parse ship dates with configurable format
 * Supports common date formats and auto-detection
 */
export function parseDateFormat(
  items: any[],
  config: DateFormatConfig | undefined,
): { items: any[]; parsedCount: number } {
  if (!config) {
    return { items, parsedCount: 0 };
  }

  let parsedCount = 0;

  const processedItems = items.map((item) => {
    if (!item.shipDate) return item;

    const rawDate = String(item.shipDate).trim();
    if (!rawDate) return item;

    let parsedDate: Date | null = null;

    // Try configured format first
    if (config.shipDateFormat) {
      parsedDate = parseWithFormat(rawDate, config.shipDateFormat);
    }

    // Auto-detect if enabled and no match yet
    if (!parsedDate && config.autoDetect) {
      parsedDate = autoDetectDate(rawDate);
    }

    if (parsedDate && !isNaN(parsedDate.getTime())) {
      parsedCount++;
      // Format as ISO date string for consistency
      return {
        ...item,
        shipDate: parsedDate.toISOString().split("T")[0],
      };
    }

    return item;
  });

  if (parsedCount > 0) {
    console.log(`[DateFormat] Parsed ${parsedCount} ship dates`);
  }

  return { items: processedItems, parsedCount };
}

function parseWithFormat(dateStr: string, format: string): Date | null {
  try {
    // Handle common formats
    const formatMap: { [key: string]: RegExp } = {
      "MM/DD/YYYY": /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/,
      "DD/MM/YYYY": /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/,
      "YYYY-MM-DD": /^(\d{4})-(\d{1,2})-(\d{1,2})$/,
      "DD-MMM-YY": /^(\d{1,2})-([A-Za-z]{3})-(\d{2})$/,
      "MMM DD, YYYY": /^([A-Za-z]{3})\s+(\d{1,2}),?\s+(\d{4})$/,
    };

    const regex = formatMap[format];
    if (!regex) {
      // Try native parsing
      const d = new Date(dateStr);
      return isNaN(d.getTime()) ? null : d;
    }

    const match = dateStr.match(regex);
    if (!match) return null;

    switch (format) {
      case "MM/DD/YYYY":
        return new Date(
          parseInt(match[3]),
          parseInt(match[1]) - 1,
          parseInt(match[2]),
        );
      case "DD/MM/YYYY":
        return new Date(
          parseInt(match[3]),
          parseInt(match[2]) - 1,
          parseInt(match[1]),
        );
      case "YYYY-MM-DD":
        return new Date(
          parseInt(match[1]),
          parseInt(match[2]) - 1,
          parseInt(match[3]),
        );
      default:
        return new Date(dateStr);
    }
  } catch {
    return null;
  }
}

function autoDetectDate(dateStr: string): Date | null {
  // Try common patterns
  const patterns = [
    { regex: /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/, order: "MDY" },
    { regex: /^(\d{4})-(\d{1,2})-(\d{1,2})$/, order: "YMD" },
    { regex: /^(\d{1,2})-(\d{1,2})-(\d{4})$/, order: "DMY" },
  ];

  for (const pattern of patterns) {
    const match = dateStr.match(pattern.regex);
    if (match) {
      let year: number, month: number, day: number;
      switch (pattern.order) {
        case "MDY":
          [, month, day, year] = match.map(Number);
          break;
        case "YMD":
          [, year, month, day] = match.map(Number);
          break;
        case "DMY":
          [, day, month, year] = match.map(Number);
          break;
        default:
          continue;
      }
      const d = new Date(year, month - 1, day);
      if (!isNaN(d.getTime())) return d;
    }
  }

  // Fallback to native parsing
  const d = new Date(dateStr);
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Apply value replacement rules to column values
 */
export function applyValueReplacements(
  items: any[],
  rules: ValueReplacementRule[] | undefined,
): { items: any[]; replacementCount: number } {
  if (!rules || rules.length === 0) {
    return { items, replacementCount: 0 };
  }

  let replacementCount = 0;

  const processedItems = items.map((item) => {
    const newItem = { ...item };

    for (const rule of rules) {
      // Support both old (column) and new (field) field names for backward compatibility
      const fieldName = rule.field || (rule as any).column;
      if (!fieldName) continue;

      const currentValue = String(newItem[fieldName] || "");
      if (!currentValue) continue;

      let newValue: string;
      if (rule.caseSensitive) {
        newValue = currentValue.replace(
          new RegExp(escapeRegex(rule.from), "g"),
          rule.to,
        );
      } else {
        newValue = currentValue.replace(
          new RegExp(escapeRegex(rule.from), "gi"),
          rule.to,
        );
      }

      if (newValue !== currentValue) {
        newItem[fieldName] = newValue;
        replacementCount++;
      }
    }

    return newItem;
  });

  if (replacementCount > 0) {
    console.log(
      `[ValueReplacement] Made ${replacementCount} value replacements`,
    );
  }

  return { items: processedItems, replacementCount };
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Master function to apply all import rules in order
 * This is the main entry point for processing items with data source config
 */
export async function applyImportRules(
  items: any[],
  dataSourceConfig: {
    discontinuedRules?: DiscontinuedConfig | DiscontinuedRule[];
    salePriceConfig?: SalePriceConfig;
    priceFloorCeiling?: PriceFloorCeilingConfig;
    minStockThreshold?: number;
    stockThresholdEnabled?: boolean;
    requiredFieldsConfig?: RequiredFieldsConfig;
    dateFormatConfig?: DateFormatConfig;
    valueReplacementRules?: ValueReplacementRule[];
    regularPriceConfig?: {
      useFilePrice?: boolean;
      priceMultiplier?: number;
      skipZeroPrice?: boolean;
    };
    cleaningConfig?: {
      stockTextMappings?: Array<{ text: string; value: number }>;
    };
    futureStockConfig?: {
      enabled?: boolean;
      preserveWithFutureStock?: boolean;
      useFutureDateAsShipDate?: boolean;
      minFutureStock?: number;
      dateOnlyMode?: boolean;
    };
    stockValueConfig?: {
      textMappings?: Array<{ text: string; value: number }>;
    };
    complexStockConfig?: {
      enabled?: boolean;
      patterns?: Array<{ pattern: string; value: number }>;
    };
  },
  rawDataRows: any[] = [],
): Promise<{
  items: any[];
  stats: {
    discontinuedFiltered: number;
    salePricingApplied: number;
    zeroPriceHandled: number;
    priceSkipped: number;
    priceClamped: number;
    stockFiltered: number;
    validationErrors: string[];
    datesParsed: number;
    replacementsMade: number;
    futureStockPreserved: number;
  };
}> {
  let processedItems = [...items];
  const stats = {
    discontinuedFiltered: 0,
    salePricingApplied: 0,
    zeroPriceHandled: 0,
    priceSkipped: 0,
    priceClamped: 0,
    stockFiltered: 0,
    validationErrors: [] as string[],
    datesParsed: 0,
    replacementsMade: 0,
    futureStockPreserved: 0,
  };

  // 0. Apply stock text mappings first (convert text to numbers)
  if (
    dataSourceConfig.cleaningConfig?.stockTextMappings &&
    dataSourceConfig.cleaningConfig.stockTextMappings.length > 0
  ) {
    const mappings = dataSourceConfig.cleaningConfig.stockTextMappings;
    processedItems = processedItems.map((item) => {
      const stockValue = String(item.stock || "")
        .trim()
        .toLowerCase();
      for (const mapping of mappings) {
        if (stockValue === mapping.text.toLowerCase()) {
          return { ...item, stock: mapping.value };
        }
      }
      return item;
    });
  }

  // 0a. Apply stockValueConfig text mappings (AI Importer style)
  if (
    dataSourceConfig.stockValueConfig?.textMappings &&
    dataSourceConfig.stockValueConfig.textMappings.length > 0
  ) {
    const mappings = dataSourceConfig.stockValueConfig.textMappings;
    processedItems = processedItems.map((item) => {
      // Skip if stock is already a number
      if (typeof item.stock === "number") {
        return item;
      }
      const stockValue = String(item.stock || "")
        .trim()
        .toLowerCase();
      for (const mapping of mappings) {
        if (stockValue === mapping.text.toLowerCase()) {
          return { ...item, stock: mapping.value };
        }
      }
      return item;
    });
  }

  // 0b. Apply complexStockConfig pattern matching
  if (
    dataSourceConfig.complexStockConfig?.enabled &&
    dataSourceConfig.complexStockConfig.patterns &&
    dataSourceConfig.complexStockConfig.patterns.length > 0
  ) {
    const patterns = dataSourceConfig.complexStockConfig.patterns;
    processedItems = processedItems.map((item) => {
      // Skip if stock is already a number
      if (typeof item.stock === "number") {
        return item;
      }
      const stockValue = String(item.stock || "").trim();
      for (const patternConfig of patterns) {
        try {
          const regex = new RegExp(patternConfig.pattern, "i");
          if (regex.test(stockValue)) {
            return { ...item, stock: patternConfig.value };
          }
        } catch (e) {
          // Invalid regex pattern, skip
          console.warn(
            `[InventoryProcessing] Invalid regex pattern: ${patternConfig.pattern}`,
          );
        }
      }
      return item;
    });
  }

  // 1. Apply value replacements first (clean up data)
  const replacementResult = applyValueReplacements(
    processedItems,
    dataSourceConfig.valueReplacementRules,
  );
  processedItems = replacementResult.items;
  stats.replacementsMade = replacementResult.replacementCount;

  // 2. Filter discontinued items
  const discontinuedResult = filterDiscontinuedItems(
    processedItems,
    dataSourceConfig.discontinuedRules,
  );
  processedItems = discontinuedResult.items;
  stats.discontinuedFiltered = discontinuedResult.filteredCount;

  // 3. Apply sale pricing if configured
  if (dataSourceConfig.salePriceConfig?.enabled) {
    const salePricingResult = applySalePricing(
      processedItems,
      dataSourceConfig.salePriceConfig,
      rawDataRows,
    );
    processedItems = salePricingResult.items;
    stats.salePricingApplied = salePricingResult.processedCount;
    stats.zeroPriceHandled += salePricingResult.zeroSkipped;
  }

  // 4. Apply regular price multiplier if configured (and sale pricing not enabled)
  if (
    !dataSourceConfig.salePriceConfig?.enabled &&
    dataSourceConfig.regularPriceConfig?.useFilePrice
  ) {
    const multiplier = dataSourceConfig.regularPriceConfig.priceMultiplier || 1;
    processedItems = processedItems.map((item) => {
      const priceStr = String(item.price || "")
        .replace(/[$,]/g, "")
        .trim();
      const price = parseFloat(priceStr);
      if (!isNaN(price) && price > 0) {
        return {
          ...item,
          price: (price * multiplier).toFixed(2),
        };
      }
      return item;
    });

    // Handle zero prices for regular pricing
    if (dataSourceConfig.regularPriceConfig.skipZeroPrice) {
      const zeroResult = handleZeroPrice(processedItems, true);
      processedItems = zeroResult.items;
      stats.zeroPriceHandled += zeroResult.zeroCount;
    }
  }

  // 5. Apply price floor/ceiling
  const priceResult = applyPriceFloorCeiling(
    processedItems,
    dataSourceConfig.priceFloorCeiling,
  );
  processedItems = priceResult.items;
  stats.priceSkipped = priceResult.skippedCount;
  stats.priceClamped = priceResult.clampedCount;

  // 6. Apply future stock rules (before stock threshold filtering)
  if (dataSourceConfig.futureStockConfig?.enabled) {
    const futureConfig = dataSourceConfig.futureStockConfig;
    const minFutureStock = futureConfig.minFutureStock ?? 1;
    const dateOnlyMode = futureConfig.dateOnlyMode ?? false;

    processedItems = processedItems.map((item) => {
      const currentStock = parseFloat(String(item.stock || 0)) || 0;
      const futureStock = parseFloat(String(item.futureStock || 0)) || 0;
      const hasFutureDate =
        item.futureDate && String(item.futureDate).trim() !== "";

      // Determine if this item should be preserved based on mode:
      // - Date Only Mode: preserve if current stock is 0 and has a future date (ignore futureStock quantity)
      // - Normal Mode: preserve if current stock is 0 and has sufficient future stock quantity
      const shouldPreserve =
        currentStock <= 0 &&
        ((dateOnlyMode && hasFutureDate) ||
          (!dateOnlyMode && futureStock >= minFutureStock));

      if (shouldPreserve) {
        stats.futureStockPreserved++;

        // Preserve the variant by setting a special marker
        // This tells downstream processes to NOT delete this variant
        let updatedItem = {
          ...item,
          hasFutureStock: true,
          futureStockQty: dateOnlyMode ? 1 : futureStock,
        };

        // Use future date as ship date if configured
        if (futureConfig.useFutureDateAsShipDate && hasFutureDate) {
          updatedItem.shipDate = item.futureDate;
          console.log(
            `[FutureStock] Item ${item.sku || item.style}: Using future date ${item.futureDate} as ship date`,
          );
        }

        // If preserveWithFutureStock is enabled, set stock to a minimal positive value
        // This prevents the variant from being deleted due to zero stock
        if (futureConfig.preserveWithFutureStock) {
          // Set to 0.001 to indicate "available but not in stock" - this preserves the variant
          // The sync process will see this as "has stock" and not delete the variant
          updatedItem.stock = 0;
          updatedItem.preserveZeroStock = true; // Flag for sync to know this is intentional
        }

        return updatedItem;
      }

      return item;
    });

    if (stats.futureStockPreserved > 0) {
      console.log(
        `[FutureStock] Preserved ${stats.futureStockPreserved} items with future stock${dateOnlyMode ? " (date only mode)" : ""}`,
      );
    }
  }

  // 7. Filter by stock threshold (only if enabled)
  const stockResult = filterByStockThreshold(
    processedItems,
    dataSourceConfig.minStockThreshold,
    dataSourceConfig.stockThresholdEnabled ?? false,
  );
  processedItems = stockResult.items;
  stats.stockFiltered = stockResult.filteredCount;

  // 8. Parse date formats
  const dateResult = parseDateFormat(
    processedItems,
    dataSourceConfig.dateFormatConfig,
  );
  processedItems = dateResult.items;
  stats.datesParsed = dateResult.parsedCount;

  // 9. Validate required fields (doesn't filter, just reports)
  const validationResult = validateRequiredFields(
    processedItems,
    dataSourceConfig.requiredFieldsConfig,
  );
  stats.validationErrors = validationResult.errors;

  // 10. Auto-filter discontinued items with zero stock
  // ONLY apply if filterZeroStock is enabled for this data source
  // This catches items marked as discontinued that weren't filtered by discontinuedRules
  // BUT: Respect items preserved by Future Stock Rules (hasFutureStock or preserveZeroStock)
  const shouldAutoFilterDiscontinuedZeroStock =
    (dataSourceConfig as any).filterZeroStock === true;

  if (shouldAutoFilterDiscontinuedZeroStock) {
    const beforeDiscontinuedFilter = processedItems.length;
    let expiredFutureStockFiltered = 0;
    processedItems = processedItems.filter((item) => {
      const stock =
        typeof item.stock === "number" ? item.stock : parseInt(item.stock) || 0;

      // CRITICAL FIX: Check if ship date has passed for items with future stock flags
      // Only treat shipDate as valid if it's a real date (not "N/A", "TBD", etc.)
      if (
        item.hasFutureStock ||
        item.preserveZeroStock ||
        (item.shipDate && isValidShipDate(item.shipDate))
      ) {
        const shipDatePassed = isShipDateInPast(item.shipDate);
        if (shipDatePassed && stock <= 0) {
          // Ship date has passed but no stock arrived - filter out
          expiredFutureStockFiltered++;
          console.log(
            `[AutoFilter] Filtering expired future stock: ${item.style || item.sku} (shipDate: ${item.shipDate})`,
          );
          return false;
        }
        // Ship date is still in the future OR has stock - preserve
        return true;
      }

      // Check various discontinued indicators
      const isDiscontinued =
        item.discontinued === true ||
        item.discontinued === "true" ||
        item.rawData?.discontinued === true ||
        item.rawData?.discontinued === "true" ||
        item.rawData?.statusMarker?.toLowerCase() === "d";

      if (isDiscontinued && stock <= 0) {
        console.log(
          `[AutoFilter] Excluding discontinued item with zero stock: ${item.style || item.sku}`,
        );
        return false;
      }
      return true;
    });
    const autoDiscontinuedFiltered =
      beforeDiscontinuedFilter - processedItems.length;
    if (autoDiscontinuedFiltered > 0) {
      console.log(
        `[AutoFilter] Auto-filtered ${autoDiscontinuedFiltered} items (${expiredFutureStockFiltered} expired future stock)`,
      );
      stats.discontinuedFiltered =
        (stats.discontinuedFiltered || 0) + autoDiscontinuedFiltered;
    }
  } else {
    console.log(
      `[AutoFilter] Skipping auto-filter of discontinued+zero-stock (filterZeroStock=${(dataSourceConfig as any).filterZeroStock})`,
    );
  }

  console.log(
    `[ImportRules] Processing complete: ${items.length} input → ${processedItems.length} output`,
  );

  return { items: processedItems, stats };
}

// ============ PRICE-BASED SIZE EXPANSION ============

export interface PriceBasedExpansionConfig {
  enabled?: boolean;
  tiers?: Array<{
    minPrice: number;
    expandDown: number;
    expandUp: number;
  }>;
  defaultExpandDown?: number;
  defaultExpandUp?: number;
  expandedStock?: number;
}

/**
 * Apply price-based size expansion during import
 * Looks up cached Shopify prices and expands sizes based on configured tiers
 * Higher price items get more size expansion
 */
export function applyPriceBasedExpansion(
  items: any[],
  config: PriceBasedExpansionConfig | null | undefined,
  stylePriceMap: Map<string, number>, // Map of style -> price from Shopify cache
  sizeLimitConfig?: SizeLimitConfig | null,
): { items: any[]; addedCount: number } {
  if (!config?.enabled) {
    return { items, addedCount: 0 };
  }

  // Check if we have tiers OR defaults configured
  const hasTiers = config.tiers && config.tiers.length > 0;
  const hasDefaults =
    (config.defaultExpandDown ?? 0) > 0 || (config.defaultExpandUp ?? 0) > 0;

  if (!hasTiers && !hasDefaults) {
    console.log(
      `[PriceBasedExpansion] No tiers or defaults configured, skipping`,
    );
    return { items, addedCount: 0 };
  }

  // Sort tiers by minPrice descending (highest first)
  const sortedTiers = hasTiers
    ? [...config.tiers!].sort((a, b) => b.minPrice - a.minPrice)
    : [];
  const expandedStock = config.expandedStock ?? 1;

  console.log(
    `[PriceBasedExpansion] Processing ${items.length} items with ${sortedTiers.length} tiers, defaults: down=${config.defaultExpandDown ?? 0}, up=${config.defaultExpandUp ?? 0}`,
  );
  if (hasTiers) {
    console.log(`[PriceBasedExpansion] Tiers: ${JSON.stringify(sortedTiers)}`);
  }
  console.log(
    `[PriceBasedExpansion] Style price map has ${stylePriceMap.size} entries`,
  );

  // Count items with stock > 0 (required for expansion)
  const itemsWithStock = items.filter((i) => {
    const s = typeof i.stock === "number" ? i.stock : parseInt(i.stock) || 0;
    return s > 0;
  }).length;
  console.log(
    `[PriceBasedExpansion] Items with stock > 0: ${itemsWithStock}/${items.length}`,
  );

  // Use EXACT same key generation as variant rules (line 508-513)
  const existingSizes = new Set<string>();
  const itemsByKey = new Map<string, any>();
  for (const item of items) {
    const key = `${item.style}|${item.color}|${String(item.size || "")
      .trim()
      .toUpperCase()}`;
    existingSizes.add(key);
    itemsByKey.set(key, item);
  }

  const alreadyExpanded = new Set<string>();
  const newExpandedItems: any[] = [];
  let totalAdded = 0;

  for (const item of items) {
    // Skip items that are already expanded (same as variant rules line 529)
    if (item.rawData?._expanded || item.isExpandedSize) continue;

    const style = String(item.style || "").trim();
    if (!style) continue;

    // Skip items with zero stock - only expand FROM items that have stock (same as variant rules line 531-533)
    const stock =
      typeof item.stock === "number" ? item.stock : parseInt(item.stock) || 0;
    if (stock <= 0) continue;

    // Start with defaults
    let downCount = config.defaultExpandDown ?? 0;
    let upCount = config.defaultExpandUp ?? 0;

    // Only require price lookup if we have tiers to check
    if (hasTiers) {
      // Lookup price from cache first, then fall back to item's own price
      let price = stylePriceMap.get(style);

      // If no Shopify price, use item's own price from the file
      if (price === undefined && item.price) {
        price =
          typeof item.price === "number"
            ? item.price
            : parseFloat(item.price) || undefined;
      }

      if (price === undefined) {
        // No price available - use defaults (already set above)
        console.log(
          `[PriceBasedExpansion] No price for style "${style}", using defaults: down=${downCount}, up=${upCount}`,
        );
      } else {
        // Find matching tier (first one where price >= minPrice)
        for (const tier of sortedTiers) {
          if (price >= tier.minPrice) {
            downCount = tier.expandDown;
            upCount = tier.expandUp;
            console.log(
              `[PriceBasedExpansion] Style "${style}" price $${price} matched tier minPrice=$${tier.minPrice}: down=${downCount}, up=${upCount}`,
            );
            break;
          }
        }
      }
    }
    // If no tiers, defaults are already set - no price lookup needed

    if (downCount <= 0 && upCount <= 0) continue;

    // Use EXACT same size string handling as variant rules (line 535)
    const currentSizeStr = String(item.size ?? "").trim();

    const key = `${item.style}|${item.color}|${currentSizeStr}`;
    if (alreadyExpanded.has(key)) continue;
    alreadyExpanded.add(key);

    // Use EXACT same size detection as variant rules (lines 541-554)
    const normalizedSize = currentSizeStr.toUpperCase().trim();
    let currentIndex: number | undefined;
    let sizeSequence: string[];

    if (LETTER_SIZE_MAP[normalizedSize] !== undefined) {
      currentIndex = LETTER_SIZE_MAP[normalizedSize];
      sizeSequence = LETTER_SIZES;
    } else if (NUMERIC_SIZE_MAP[currentSizeStr] !== undefined) {
      currentIndex = NUMERIC_SIZE_MAP[currentSizeStr];
      sizeSequence = NUMERIC_SIZES;
    } else {
      continue; // Unknown size format
    }

    // Expand down - EXACT same logic as variant rules (lines 556-611)
    for (let i = 1; i <= downCount; i++) {
      const newIndex = currentIndex - i;
      if (newIndex >= 0 && newIndex < sizeSequence.length) {
        const newSize = sizeSequence[newIndex];

        if (
          sizeLimitConfig?.enabled &&
          !isSizeAllowed(newSize, sizeLimitConfig, item.style)
        ) {
          continue;
        }

        const newKey = `${item.style}|${item.color}|${newSize.toUpperCase()}`;

        if (existingSizes.has(newKey)) {
          // Update existing zero-stock item (same as variant rules lines 570-590)
          const existingItem = itemsByKey.get(newKey);
          if (existingItem) {
            const existingStock =
              typeof existingItem.stock === "number"
                ? existingItem.stock
                : parseInt(existingItem.stock) || 0;
            if (existingStock === 0) {
              existingItem.stock = expandedStock;
              existingItem.shipDate = null;
              existingItem.isExpandedSize = true;
              existingItem.rawData = {
                ...existingItem.rawData,
                _expanded: true,
                _fromSize: currentSizeStr,
                _priceBasedExpansion: true,
              };
            }
          }
          continue;
        }

        // Generate SKU and validate (same as variant rules lines 592-596)
        const newSku = generateExpandedSku(item.style, item.color, newSize);
        if (!newSku) {
          console.warn(
            `[PriceBasedExpansion] Skipping expanded size ${newSize} for ${item.style}/${item.color} - could not generate SKU`,
          );
          continue;
        }

        existingSizes.add(newKey);
        newExpandedItems.push({
          ...item,
          sku: newSku,
          size: newSize,
          stock: expandedStock,
          shipDate: null,
          isExpandedSize: true,
          rawData: {
            ...item.rawData,
            _expanded: true,
            _fromSize: currentSizeStr,
            _priceBasedExpansion: true,
          },
        });
        totalAdded++;
      }
    }

    // Expand up - EXACT same logic as variant rules (lines 614-670)
    for (let i = 1; i <= upCount; i++) {
      const newIndex = currentIndex + i;
      if (newIndex >= 0 && newIndex < sizeSequence.length) {
        const newSize = sizeSequence[newIndex];

        if (
          sizeLimitConfig?.enabled &&
          !isSizeAllowed(newSize, sizeLimitConfig, item.style)
        ) {
          continue;
        }

        const newKey = `${item.style}|${item.color}|${newSize.toUpperCase()}`;

        if (existingSizes.has(newKey)) {
          // Update existing zero-stock item (same as variant rules)
          const existingItem = itemsByKey.get(newKey);
          if (existingItem) {
            const existingStock =
              typeof existingItem.stock === "number"
                ? existingItem.stock
                : parseInt(existingItem.stock) || 0;
            if (existingStock === 0) {
              existingItem.stock = expandedStock;
              existingItem.shipDate = null;
              existingItem.isExpandedSize = true;
              existingItem.rawData = {
                ...existingItem.rawData,
                _expanded: true,
                _fromSize: currentSizeStr,
                _priceBasedExpansion: true,
              };
            }
          }
          continue;
        }

        // Generate SKU and validate (same as variant rules)
        const newSku = generateExpandedSku(item.style, item.color, newSize);
        if (!newSku) {
          console.warn(
            `[PriceBasedExpansion] Skipping expanded size ${newSize} for ${item.style}/${item.color} - could not generate SKU`,
          );
          continue;
        }

        existingSizes.add(newKey);
        newExpandedItems.push({
          ...item,
          sku: newSku,
          size: newSize,
          stock: expandedStock,
          shipDate: null,
          isExpandedSize: true,
          rawData: {
            ...item.rawData,
            _expanded: true,
            _fromSize: currentSizeStr,
            _priceBasedExpansion: true,
          },
        });
        totalAdded++;
      }
    }
  }

  console.log(`[PriceBasedExpansion] Added ${totalAdded} expanded sizes`);

  return {
    items: [...items, ...newExpandedItems],
    addedCount: totalAdded,
  };
}

/**
 * Build a style -> price map from Shopify variant cache
 * Used for price-based size expansion during import
 *
 * IMPORTANT: Only store precise style matches to avoid incorrect price lookups.
 * We store the COMBINED style (e.g., "Tarik Ediz 50902") not individual parts.
 */
export function buildStylePriceMapFromCache(
  cacheVariants: Array<{
    sku: string | null;
    price: string | null;
    productTitle?: string | null;
  }>,
): Map<string, number> {
  const stylePriceMap = new Map<string, number>();

  const setPrice = (key: string, price: number) => {
    if (!key || key.length < 2) return; // Skip empty or single-char keys
    const existing = stylePriceMap.get(key);
    if (existing === undefined || price > existing) {
      stylePriceMap.set(key, price);
    }
  };

  // Helper to check if a string looks like a style number (contains at least one digit)
  const looksLikeStyleNumber = (s: string) => /\d/.test(s);

  // Helper to check if a string looks like a size (numeric dress size or letter size)
  const looksLikeSize = (s: string) => {
    const upper = s.toUpperCase().trim();
    // Letter sizes
    if (
      [
        "XXS",
        "XS",
        "S",
        "M",
        "L",
        "XL",
        "2XL",
        "3XL",
        "4XL",
        "XXL",
        "XXXL",
      ].includes(upper)
    ) {
      return true;
    }
    // Numeric sizes (0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 00, 000)
    if (/^(0{1,3}|[2-9]|1[0-9]|2[0-9]|30)$/.test(s)) {
      return true;
    }
    return false;
  };

  let skuMatches = 0;
  let titleMatches = 0;

  for (const variant of cacheVariants) {
    if (!variant.price) continue;

    const price = parseFloat(variant.price);
    if (isNaN(price) || price <= 0) continue;

    // FIRST: Try to extract style from SKU if available
    if (variant.sku) {
      // SKU formats vary:
      // - Simple: "12345-Black-4" (STYLE-COLOR-SIZE)
      // - Prefixed: "Tarik-Ediz-50902-Black-4" (VENDOR-VENDOR-STYLE-COLOR-SIZE)
      // - With vendor: "Jovani-12345-Red-6" (VENDOR-STYLE-COLOR-SIZE)
      const parts = variant.sku.split("-");

      if (parts.length >= 4) {
        // Could be "Tarik-Ediz-50902-Black-4" format
        // Check if parts[2] looks like a style number and parts[3] doesn't look like a size
        if (looksLikeStyleNumber(parts[2]) && !looksLikeSize(parts[2])) {
          // Combined: "Tarik Ediz 50902" - this matches inventory style format
          const combinedStyle = `${parts[0]} ${parts[1]} ${parts[2]}`.trim();
          setPrice(combinedStyle, price);
          skuMatches++;
        }
      }

      if (parts.length >= 3) {
        // Could be "Jovani-12345-Red-6" format
        if (looksLikeStyleNumber(parts[1]) && !looksLikeSize(parts[1])) {
          // Combined: "Jovani 12345" - this matches inventory style format
          const simpleStyle = `${parts[0]} ${parts[1]}`.trim();
          setPrice(simpleStyle, price);
          skuMatches++;
        }
      }

      // For SKUs with just style number: "12345-Black-4"
      if (
        parts.length >= 2 &&
        looksLikeStyleNumber(parts[0]) &&
        !looksLikeSize(parts[0])
      ) {
        // Just the style number itself
        setPrice(parts[0].trim(), price);
        skuMatches++;
      }
    }

    // SECOND: If no SKU or as additional matching, use productTitle directly
    // Product titles like "Tarik Ediz 98032" are exactly the style format we need
    if (variant.productTitle) {
      const title = variant.productTitle.trim();
      // Check if title looks like a style name (e.g., "Tarik Ediz 98032", "Jovani 12345")
      // Must contain at least one digit to be considered a style
      if (looksLikeStyleNumber(title)) {
        setPrice(title, price);
        titleMatches++;
      }
    }
  }

  console.log(
    `[buildStylePriceMapFromCache] Built price map with ${stylePriceMap.size} style entries (${skuMatches} from SKU, ${titleMatches} from productTitle)`,
  );

  return stylePriceMap;
}
