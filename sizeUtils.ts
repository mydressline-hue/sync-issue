/**
 * Size limit utilities for filtering inventory items by size range.
 * Used by applyVariantRules() and applyPriceBasedExpansion() to enforce
 * per-data-source size constraints, including prefix-based overrides.
 */

export interface SizeLimitConfig {
  enabled?: boolean;
  minSize?: string | null;
  maxSize?: string | null;
  minLetterSize?: string | null;
  maxLetterSize?: string | null;
  prefixOverrides?: Array<{
    pattern: string;
    minSize?: string | null;
    maxSize?: string | null;
    minLetterSize?: string | null;
    maxLetterSize?: string | null;
  }>;
}

// Letter sizes in ascending order
export const LETTER_SIZES = [
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

// Letter size → index map (includes common aliases)
export const LETTER_SIZE_MAP: Record<string, number> = {
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
  // Aliases
  XXL: 6,
  XXXL: 7,
  XXXXL: 8,
  XXXXXL: 9,
};

// Numeric sizes in ascending order
export const NUMERIC_SIZES = [
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
  "32",
  "34",
  "36",
];

// Numeric size → index map (built from NUMERIC_SIZES)
export const NUMERIC_SIZE_MAP: Record<string, number> = {};
NUMERIC_SIZES.forEach((size, index) => {
  NUMERIC_SIZE_MAP[size] = index;
});

/**
 * Get the ordinal rank of a size string for sorting purposes.
 * Returns -1 for unrecognized sizes.
 */
export function getSizeRank(size: string): number {
  const normalized = String(size || "")
    .trim()
    .toUpperCase();

  // Check letter sizes
  if (LETTER_SIZE_MAP[normalized] !== undefined) {
    // Offset letter sizes to separate them from numeric (100+)
    return 100 + LETTER_SIZE_MAP[normalized];
  }

  // Check numeric sizes
  const rawSize = String(size || "").trim();
  if (NUMERIC_SIZE_MAP[rawSize] !== undefined) {
    return NUMERIC_SIZE_MAP[rawSize];
  }

  // Check W (plus) sizes — strip the W suffix and use numeric rank + 50 offset
  const wMatch = rawSize.match(/^(\d+)W$/i);
  if (wMatch && NUMERIC_SIZE_MAP[wMatch[1]] !== undefined) {
    return 50 + NUMERIC_SIZE_MAP[wMatch[1]];
  }

  return -1;
}

/**
 * Check if a size is allowed given the size limit configuration and item style.
 *
 * Logic:
 * 1. If config is not enabled, allow everything.
 * 2. Check prefix overrides — if a style matches an override pattern, that
 *    override's limits replace the defaults (only for fields the override specifies).
 * 3. If no effective limits are set at all, allow everything.
 * 4. For recognized sizes (letter or numeric), check against the effective min/max.
 * 5. For unrecognized sizes (DOZEN, UNIT, SS, etc.), FILTER THEM OUT when any
 *    limits are active — the user explicitly constrained what sizes are valid.
 */
export function isSizeAllowed(
  size: string,
  config: SizeLimitConfig,
  style: string,
): boolean {
  if (!config?.enabled) return true;

  // Resolve effective limits (start with defaults, apply prefix overrides)
  let effectiveMinSize = config.minSize;
  let effectiveMaxSize = config.maxSize;
  let effectiveMinLetter = config.minLetterSize;
  let effectiveMaxLetter = config.maxLetterSize;

  if (config.prefixOverrides?.length && style) {
    for (const override of config.prefixOverrides) {
      if (!override.pattern) continue;
      let matches = false;
      try {
        matches = new RegExp(override.pattern, "i").test(style);
      } catch {
        matches = style
          .toLowerCase()
          .startsWith(override.pattern.toLowerCase());
      }
      if (matches) {
        // Override only the fields that the override explicitly provides
        if (override.minSize !== undefined && override.minSize !== null)
          effectiveMinSize = override.minSize;
        if (override.maxSize !== undefined && override.maxSize !== null)
          effectiveMaxSize = override.maxSize;
        if (
          override.minLetterSize !== undefined &&
          override.minLetterSize !== null
        )
          effectiveMinLetter = override.minLetterSize;
        if (
          override.maxLetterSize !== undefined &&
          override.maxLetterSize !== null
        )
          effectiveMaxLetter = override.maxLetterSize;
        break;
      }
    }
  }

  // If no limits are set at all (enabled but nothing configured), allow everything
  const hasNumericLimits = effectiveMinSize || effectiveMaxSize;
  const hasLetterLimits = effectiveMinLetter || effectiveMaxLetter;
  if (!hasNumericLimits && !hasLetterLimits) return true;

  const normalizedSize = String(size || "")
    .trim()
    .toUpperCase();
  if (!normalizedSize) return false;

  // --- Letter size check ---
  if (LETTER_SIZE_MAP[normalizedSize] !== undefined) {
    // It's a recognized letter size
    if (!hasLetterLimits) {
      // No letter limits configured — allow letter sizes through
      return true;
    }
    const idx = LETTER_SIZE_MAP[normalizedSize];
    if (effectiveMinLetter) {
      const minIdx = LETTER_SIZE_MAP[effectiveMinLetter.toUpperCase()];
      if (minIdx !== undefined && idx < minIdx) return false;
    }
    if (effectiveMaxLetter) {
      const maxIdx = LETTER_SIZE_MAP[effectiveMaxLetter.toUpperCase()];
      if (maxIdx !== undefined && idx > maxIdx) return false;
    }
    return true;
  }

  // --- Numeric size check (including W sizes) ---
  const rawSize = String(size || "").trim();
  const wMatch = rawSize.match(/^(\d+)W$/i);
  const numericPart = wMatch ? wMatch[1] : rawSize;

  if (NUMERIC_SIZE_MAP[numericPart] !== undefined) {
    // It's a recognized numeric size
    if (!hasNumericLimits) {
      // No numeric limits configured — allow numeric sizes through
      return true;
    }
    const idx = NUMERIC_SIZE_MAP[numericPart];
    if (effectiveMinSize) {
      const minStr = String(effectiveMinSize).replace(/W$/i, "");
      const minIdx = NUMERIC_SIZE_MAP[minStr];
      if (minIdx !== undefined && idx < minIdx) return false;
    }
    if (effectiveMaxSize) {
      const maxStr = String(effectiveMaxSize).replace(/W$/i, "");
      const maxIdx = NUMERIC_SIZE_MAP[maxStr];
      if (maxIdx !== undefined && idx > maxIdx) return false;
    }
    return true;
  }

  // --- Unrecognized size (DOZEN, DOZN, SS, SM, MD, LG, LLL, LL, UNIT, etc.) ---
  // When size limits are active, filter out sizes that don't match any known category
  return false;
}
