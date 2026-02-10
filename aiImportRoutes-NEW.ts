/**
 * AI Import Routes - Slim Route Handlers
 *
 * Parsers and import pipeline logic have been moved to importEngine-NEW.ts.
 * This file contains only route handlers that delegate to the engine,
 * plus non-import routes (analyze, preview, save-config, validate-db).
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
  executeImport,
  autoDetectPivotFormat,
  parseIntelligentPivotFormat,
} from "./importEngine";
import type { UniversalParserConfig } from "./importEngine";

// Re-export for backward compatibility (other files import these from aiImportRoutes)
export { autoDetectPivotFormat, parseIntelligentPivotFormat };
export type { UniversalParserConfig };

const router = Router();
const upload = multer({ storage: multer.memoryStorage() });

// ============================================================
// Type definitions, utility functions, format detector, and all
// vendor parsers moved to importEngine-NEW.ts
// ============================================================

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
      // Still return actual column headers so UI dropdowns are populated
      // and users can manually map price/cost/etc. even for pivot formats
      const pivotHeaders = rawData[0] || [];
      const pivotHeadersLower = pivotHeaders.map((h: any) =>
        String(h || "").toLowerCase(),
      );

      // Run basic column detection for pivot formats too
      const pivotMapping: any = {};
      const pivotPriceIdx = pivotHeadersLower.findIndex((h: string) =>
        h.includes("price") || h.includes("wholesale") || h.includes("cost") ||
        h.includes("msrp") || h === "line price",
      );
      const pivotDateIdx = pivotHeadersLower.findIndex((h: string) =>
        h.includes("date") || h.includes("eta") || h.includes("ship") ||
        h.includes("arrival") || h.includes("delivery"),
      );
      const pivotStatusIdx = pivotHeadersLower.findIndex((h: string) =>
        h.includes("status") || h.includes("discontinued") || h.includes("active"),
      );
      if (pivotPriceIdx >= 0) pivotMapping.price = String(pivotHeaders[pivotPriceIdx] || "");
      if (pivotDateIdx >= 0) pivotMapping.shipDate = String(pivotHeaders[pivotDateIdx] || "");
      if (pivotStatusIdx >= 0) pivotMapping.discontinued = String(pivotHeaders[pivotStatusIdx] || "");

      return res.json({
        detection: {
          success: true,
          formatType: pivotFormat.includes("pivot")
            ? pivotFormat
            : `pivot_${pivotFormat}`,
          formatConfidence: 95,
          confidence: 95,
          columnMapping: pivotMapping,
          suggestedColumnMapping: pivotMapping,
          pivotConfig: { enabled: true, format: pivotFormat },
          notes: [`Auto-detected ${pivotFormat} pivot format`],
          warnings: [],
          columns: pivotHeaders.map((h: any, i: number) => ({
            headerName: String(h || ""),
            columnIndex: i,
          })),
          detectedPatterns: {
            hasDiscontinuedIndicators: pivotStatusIdx >= 0,
            hasDateColumns: pivotDateIdx >= 0,
            hasTextStockValues: false,
            hasPriceColumn: pivotPriceIdx >= 0,
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
        h.includes("price") || h.includes("wholesale") || h.includes("cost") ||
        h.includes("msrp") || h === "line price",
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
// EXECUTE IMPORT ENDPOINT (delegated to importEngine)
// ============================================================

router.post("/execute", upload.any(), async (req: Request, res: Response) => {
  try {
    const { dataSourceId, overrideConfig } = req.body;
    const files = (req.files as Express.Multer.File[]) || [];

    const fileBuffers = files.map(f => ({ buffer: f.buffer, originalname: f.originalname }));

    const result = await executeImport({
      fileBuffers,
      dataSourceId: typeof dataSourceId === 'string' ? dataSourceId : String(dataSourceId),
      overrideConfig: overrideConfig ? JSON.parse(overrideConfig) : undefined,
      source: 'ai_import',
    });

    if (!result.success) {
      return res.status(result.safetyBlock ? 409 : 500).json(result);
    }
    res.json(result);
  } catch (error: any) {
    console.error("[AI Import] Execute error:", error);
    res.status(500).json({ success: false, error: error.message });
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
              name: "\u2713 Item Count Match",
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
              name: "\u2713 Total Stock Match",
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
              name: "\u2713 Style Count Match",
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
              name: "\u2713 Color Count Match",
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
              message: `${change >= 0 ? "+" : ""}${change.toFixed(1)}% from last import (max: \u00b1${rules.maxItemCountChange}%)`,
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
              message: `${change >= 0 ? "+" : ""}${change.toFixed(1)}% from last import (max: \u00b1${rules.maxTotalStockChange}%)`,
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
              message: `${change >= 0 ? "+" : ""}${change.toFixed(1)}% from last import (max: \u00b1${rules.maxStyleCountChange}%)`,
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

// ============================================================
// SHARED MULTI-FILE IMPORT (delegated to importEngine)
// Used by email fetcher and other callers.
// ============================================================

export async function executeAIImport(options: {
  fileBuffers: { buffer: Buffer; originalname: string }[];
  dataSourceId: string;
  source?: string;
}): Promise<{ success: boolean; itemCount: number; error?: string; fileId?: string }> {
  return executeImport({
    fileBuffers: options.fileBuffers,
    dataSourceId: options.dataSourceId,
    source: (options.source as any) || 'email',
  });
}

export default router;
