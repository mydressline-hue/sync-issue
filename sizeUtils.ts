// Size utility functions for sorting and ranking sizes
// Extracted to avoid circular dependencies between routes.ts and shopify.ts

// Letter size sequence for expansion (common clothing sizes)
export const LETTER_SIZES = ['XXS', 'XS', 'S', 'M', 'L', 'XL', '2XL', '3XL', '4XL', '5XL'];

// Map for case-insensitive lookup - store uppercase versions
export const LETTER_SIZE_MAP: Record<string, number> = {
  // Standard uppercase
  'XXS': 0, 'XS': 1, 'S': 2, 'M': 3, 'L': 4, 'XL': 5, '2XL': 6, '3XL': 7, '4XL': 8, '5XL': 9,
  // Common alternatives
  'XXL': 6, 'XXXL': 7, 'XXXXL': 8, 'XXXXXL': 9,
};

// Numeric size sequence for expansion (common dress sizes - step of 2)
// Includes 000, 00 before 0 for proper ordering
// W (plus) sizes immediately follow their base size for proper ordering
// To include W sizes, explicitly select them as min/max (e.g., max=24W includes 24W, max=24 excludes 24W)
export const NUMERIC_SIZES = [
  '000', '00', '0', '2', '4', '6', '8', '10', '12', '14',
  '16', '16W',
  '18', '18W',
  '20', '20W',
  '22', '22W',
  '24', '24W',
  '26', '26W',
  '28', '28W',
  '30', '30W',
  '32', '32W',
  '34', '34W',
  '36', '36W'
];

// Map for numeric size lookup - preserves original size format
export const NUMERIC_SIZE_MAP: Record<string, number> = {};
NUMERIC_SIZES.forEach((size, index) => {
  NUMERIC_SIZE_MAP[size] = index;
});

// Helper function to get size rank for sorting (smallest to largest)
// Returns a number where lower = smaller size, higher = larger size
export function getSizeRank(size: string): number {
  if (!size) return 99999;

  const normalized = String(size).trim();
  const upper = normalized.toUpperCase();

  // Check letter sizes first (XXS=0, XS=1, S=2, M=3, L=4, XL=5, etc.)
  const letterIndex = LETTER_SIZE_MAP[upper];
  if (letterIndex !== undefined) {
    return letterIndex;
  }

  // Check numeric sizes - these should have higher rank than letter sizes
  // so they sort separately (or we can offset them to sort after letters)
  const numericIndex = NUMERIC_SIZE_MAP[normalized];
  if (numericIndex !== undefined) {
    return 100 + numericIndex; // Offset to separate from letter sizes
  }

  // Try parsing as number for unknown numeric sizes
  const num = parseInt(normalized);
  if (!isNaN(num)) {
    // For numeric sizes, position them in order
    // 0 -> 102, 2 -> 103, 4 -> 104, etc.
    return 100 + 2 + (num / 2);
  }

  // Unknown size - put at end
  return 99998;
}

// Size limit configuration type
export interface SizeLimitConfig {
  enabled?: boolean;
  minSize?: string;        // Min numeric size (0, 2, 4...)
  maxSize?: string;        // Max numeric size (0, 2, 4...)
  minLetterSize?: string;  // Min letter size (S, M, L...)
  maxLetterSize?: string;  // Max letter size (S, M, L...)
  allowedSizes?: string[];
  prefixOverrides?: Array<{
    pattern: string;
    minSize?: string;
    maxSize?: string;
    minLetterSize?: string;
    maxLetterSize?: string;
    allowedSizes?: string[];
  }>;
}

// Helper to check if a size is a letter size
function isLetterSize(size: string): boolean {
  const upper = size.toUpperCase();
  return LETTER_SIZE_MAP[upper] !== undefined;
}

// Helper to check if a size is a W (plus) size
function isWSize(size: string): boolean {
  return size.toUpperCase().endsWith('W');
}

// Check if a size is within the allowed range
// Returns true if size is allowed, false if it should be filtered out
export function isSizeAllowed(
  size: string,
  sizeLimitConfig: SizeLimitConfig | null | undefined,
  style?: string
): boolean {
  // If no config or not enabled, allow all sizes
  if (!sizeLimitConfig?.enabled) {
    return true;
  }

  const normalizedSize = String(size || '').trim();
  if (!normalizedSize) {
    return false; // Empty sizes are never allowed
  }

  // Find applicable size limits (check prefix overrides first)
  let minSize = sizeLimitConfig.minSize;
  let maxSize = sizeLimitConfig.maxSize;
  let minLetterSize = sizeLimitConfig.minLetterSize;
  let maxLetterSize = sizeLimitConfig.maxLetterSize;
  let allowedSizes = sizeLimitConfig.allowedSizes;

  // Check for prefix-specific overrides
  if (style && sizeLimitConfig.prefixOverrides?.length) {
    for (const override of sizeLimitConfig.prefixOverrides) {
      if (override.pattern) {
        try {
          const regex = new RegExp(override.pattern, 'i');
          if (regex.test(style)) {
            // Use override settings
            minSize = override.minSize ?? minSize;
            maxSize = override.maxSize ?? maxSize;
            minLetterSize = override.minLetterSize ?? minLetterSize;
            maxLetterSize = override.maxLetterSize ?? maxLetterSize;
            allowedSizes = override.allowedSizes ?? allowedSizes;
            break;
          }
        } catch (e) {
          // Invalid regex, try literal match
          if (style.toLowerCase().startsWith(override.pattern.toLowerCase())) {
            minSize = override.minSize ?? minSize;
            maxSize = override.maxSize ?? maxSize;
            minLetterSize = override.minLetterSize ?? minLetterSize;
            maxLetterSize = override.maxLetterSize ?? maxLetterSize;
            allowedSizes = override.allowedSizes ?? allowedSizes;
            break;
          }
        }
      }
    }
  }

  // If explicit allowedSizes list is provided, check against it
  if (allowedSizes && allowedSizes.length > 0) {
    const normalizedUpper = normalizedSize.toUpperCase();
    return allowedSizes.some(s =>
      s.toUpperCase() === normalizedUpper || s === normalizedSize
    );
  }

  // Determine if this is a letter size or numeric size
  const isLetter = isLetterSize(normalizedSize);

  if (isLetter) {
    // Check against letter size limits (if configured)
    if (minLetterSize || maxLetterSize) {
      const sizeRank = getSizeRank(normalizedSize);

      if (minLetterSize) {
        const minRank = getSizeRank(minLetterSize);
        if (sizeRank < minRank) {
          return false;
        }
      }

      if (maxLetterSize) {
        const maxRank = getSizeRank(maxLetterSize);
        if (sizeRank > maxRank) {
          return false;
        }
      }
      return true;
    }

    // If numeric limits ARE set but no letter limits, BLOCK letter sizes
    // (user specified a numeric range like 00-24, so they don't want S, M, L, etc.)
    if (minSize || maxSize) {
      return false;
    }

    // If no limits at all, allow everything
    return true;
  } else {
    // Check against numeric size limits (if configured)
    if (minSize || maxSize) {
      const sizeIsW = isWSize(normalizedSize);
      const minIsW = minSize ? isWSize(minSize) : null;
      const maxIsW = maxSize ? isWSize(maxSize) : null;

      // W sizes and regular sizes are separate tracks:
      // Determine if W sizes are allowed based on the configured limits
      // W sizes are only allowed if at least one limit is a W size
      const wSizesAllowed = (minIsW === true) || (maxIsW === true);

      if (sizeIsW && !wSizesAllowed) {
        return false; // W size not allowed when both min and max are non-W
      }

      const sizeRank = getSizeRank(normalizedSize);

      if (minSize) {
        const minRank = getSizeRank(minSize);
        if (sizeRank < minRank) {
          return false;
        }
      }

      if (maxSize) {
        const maxRank = getSizeRank(maxSize);
        if (sizeRank > maxRank) {
          return false;
        }
      }
      return true;
    }

    // If letter limits ARE set but no numeric limits, BLOCK numeric sizes
    // (user specified a letter range like S-XL, so they don't want 0, 2, 4, etc.)
    if (minLetterSize || maxLetterSize) {
      return false;
    }

    // If no limits at all, allow everything
    return true;
  }
}

// Get the effective size limits for a given style (resolves prefix overrides)
export function getEffectiveSizeLimits(
  sizeLimitConfig: SizeLimitConfig | null | undefined,
  style?: string
): { minSize?: string; maxSize?: string; minLetterSize?: string; maxLetterSize?: string; allowedSizes?: string[] } | null {
  if (!sizeLimitConfig?.enabled) {
    return null;
  }

  let minSize = sizeLimitConfig.minSize;
  let maxSize = sizeLimitConfig.maxSize;
  let minLetterSize = sizeLimitConfig.minLetterSize;
  let maxLetterSize = sizeLimitConfig.maxLetterSize;
  let allowedSizes = sizeLimitConfig.allowedSizes;

  // Check for prefix-specific overrides
  if (style && sizeLimitConfig.prefixOverrides?.length) {
    for (const override of sizeLimitConfig.prefixOverrides) {
      if (override.pattern) {
        try {
          const regex = new RegExp(override.pattern, 'i');
          if (regex.test(style)) {
            minSize = override.minSize ?? minSize;
            maxSize = override.maxSize ?? maxSize;
            minLetterSize = override.minLetterSize ?? minLetterSize;
            maxLetterSize = override.maxLetterSize ?? maxLetterSize;
            allowedSizes = override.allowedSizes ?? allowedSizes;
            break;
          }
        } catch (e) {
          if (style.toLowerCase().startsWith(override.pattern.toLowerCase())) {
            minSize = override.minSize ?? minSize;
            maxSize = override.maxSize ?? maxSize;
            minLetterSize = override.minLetterSize ?? minLetterSize;
            maxLetterSize = override.maxLetterSize ?? maxLetterSize;
            allowedSizes = override.allowedSizes ?? allowedSizes;
            break;
          }
        }
      }
    }
  }

  return { minSize, maxSize, minLetterSize, maxLetterSize, allowedSizes };
}

// Generate list of allowed sizes between min and max
export function generateAllowedSizeRange(minSize?: string, maxSize?: string): string[] {
  if (!minSize && !maxSize) {
    return [];
  }

  const minRank = minSize ? getSizeRank(minSize) : 0;
  const maxRank = maxSize ? getSizeRank(maxSize) : 99999;

  // Determine if we're dealing with letter or numeric sizes
  const isLetterRange = minSize && LETTER_SIZE_MAP[minSize.toUpperCase()] !== undefined;
  const sizeList = isLetterRange ? LETTER_SIZES : NUMERIC_SIZES;
  const offset = isLetterRange ? 0 : 100;

  return sizeList.filter(size => {
    const rank = getSizeRank(size);
    return rank >= minRank && rank <= maxRank;
  });
}
