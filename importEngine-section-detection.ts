// ============ SECTION 2: FORMAT DETECTION & INTELLIGENT PARSER ============

import * as XLSX from "xlsx";

// UniversalParserConfig and PivotItem are defined in section 1 of the import engine.
// The 10 vendor parsers (parseFerianiFormat, parseJovaniSaleFormat, parseTarikEdizFormat,
// parseSherriHillFormat, parseGenericPivotFormat, parsePRDateHeaderFormat,
// parseGRNInvoiceFormat, parseStoreMultibrandFormat, parseOTSFormat, parseRowFormat)
// are also defined in section 1 and available in the same file scope.

export function autoDetectPivotFormat(
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

  // Store Multibrand: row format with a vendor/brand column + style + color + size
  const hasVendorCol = headersLower.some(
    (h: string) =>
      h.includes("vendor") || h.includes("brand") || h.includes("designer") ||
      h.includes("manufacturer"),
  );
  const hasStyleCol = headersLower.some(
    (h: string) => h.includes("style") || h === "item" || h === "code",
  );
  const hasColorCol = headersLower.some((h: string) => h.includes("color"));
  const hasSizeCol = headersLower.some((h: string) => h.includes("size"));
  if (hasVendorCol && hasStyleCol && hasColorCol && hasSizeCol) {
    return "store_multibrand";
  }

  return null;
}

// ============================================================
// INTELLIGENT PIVOT FORMAT PARSER
// ============================================================

export function parseIntelligentPivotFormat(
  buffer: Buffer,
  formatType: string,
  config: UniversalParserConfig,
  dataSourceName?: string,
  filename?: string,
): { headers: string[]; rows: any[][]; items: any[] } {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rawData = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false, // Force all values to strings for consistent date/number handling across parsers
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
