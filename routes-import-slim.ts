// ============================================================================
// routes-import-slim.ts
//
// This file contains the SLIMMED import-related functions for routes.ts.
// Each section shows what to REPLACE or DELETE, with line references from
// routes (29).ts (22,383 lines total).
//
// All heavy import logic (parsing, cleaning, rules, expansion, sale pricing,
// stockInfo, safety thresholds, DB save) is now delegated to executeImport()
// from importEngine.
// ============================================================================


// === NEW IMPORT (add near top of routes.ts, after existing imports) ===
import {
  executeImport,
  calculateItemStockInfo,
  getStockInfoRule,
  getStylePrefix,
  checkSafetyThreshold,
  parseFileForDataSource,
} from "./importEngine";


// === DELETE: checkSafetyThreshold (lines 120-137) ===
// Moved to importEngine. Remove the entire function.


// === DELETE: isCSVBuffer (lines 145-196) ===
// Only used by parseExcelToInventory (dead code). Moved to importEngine as utility.


// === DELETE: parseCSVAsText (lines 199-271) ===
// Only used by parseExcelToInventory (dead code). Moved to importEngine as utility.


// === KEEP: triggerAutoConsolidationAfterImport (lines 274-319) ===
// Already exported. No changes needed.


// === REPLACE: processUrlDataSourceImport (lines 322-802) ===
// Was ~480 lines. Now ~10 lines — thin wrapper around executeImport().
// Validation, sale import checks, parsing, cleaning, rules, expansion,
// sale pricing, stockInfo, safety, DB save all handled by executeImport.

export async function processUrlDataSourceImport(
  dataSourceId: string,
  buffer: Buffer,
  filename: string,
): Promise<{
  success: boolean;
  itemCount?: number;
  error?: string;
  headers?: string[];
}> {
  return executeImport({
    fileBuffers: [{ buffer, originalname: filename }],
    dataSourceId,
    source: "url",
  });
}


// === KEEP: multer config (line 804-805) ===
// const upload = multer({ storage: multer.memoryStorage() });


// === DELETE: applyCleaningToValue (lines 807-876) ===
// Duplicate of the version in importUtils.ts. importEngine uses importUtils version.
// Remove the entire function from routes.ts.


// === DELETE: parseTarikEdizFormat (lines 879-1019) ===
// Dead code. Shared parsers in aiImportRoutes handle Tarik Ediz format.
// Only called from parseExcelToInventory which is also being deleted.


// === DELETE: parseJovaniFormat (lines 1026-1258) ===
// Dead code. Shared parsers in aiImportRoutes handle Jovani format.
// Only called from parseExcelToInventory which is also being deleted.


// === DELETE: parseSherriHillFormat (lines 1265-1425) ===
// Dead code. Shared parsers in aiImportRoutes handle Sherri Hill format.
// Only called from parseExcelToInventory which is also being deleted.


// === DELETE: parseGenericPivotedFormat (lines 1428-1631) ===
// Dead code. Shared parsers in aiImportRoutes handle generic pivoted format.
// Only called from parseExcelToInventory which is also being deleted.


// === DELETE: calculateItemStockInfo (lines 1637-1733) ===
// Moved to importEngine. Remove the entire function from routes.ts.
// importEngine exports it for any callers that need it directly.


// === DELETE: getStockInfoRule (lines 1736-1814) ===
// Moved to importEngine. Remove the entire function from routes.ts.
// importEngine exports it for any callers that need it directly.


// === DELETE: parseExcelToInventory (lines 1817-2160) ===
// Dead code — uses old parsers (parseSherriHillFormat, parseJovaniFormat,
// parseGenericPivotedFormat, parseTarikEdizFormat). All import paths now use
// shared parsers from aiImportRoutes via importEngine.
// Remove the entire function from routes.ts.


// === DELETE: parseFerianiGiaFormat (~lines 22215-22383) ===
// Dead code. Only called from parseExcelToInventory which is also deleted.
// Shared parsers in aiImportRoutes handle Feriani/GIA format.
// Remove the entire function from routes.ts.


// === REPLACE: performCombineImport (lines 2167-3073) ===
// Was ~908 lines. Now ~30 lines — reads staged files and delegates to executeImport.
// Complex extraction logic (column mapping, Jovani sale format, combined variant,
// cleaning, future stock zeroing, dedup, rules, expansion, sale pricing, stockInfo,
// safety, DB save) all handled by executeImport with source: 'combine'.

export async function performCombineImport(dataSourceId: string): Promise<{
  success: boolean;
  rowCount: number;
  error?: string;
  details?: any;
}> {
  console.log(
    `[performCombineImport] ENTERED for dataSourceId=${dataSourceId}`,
  );

  const stagedFiles = await storage.getStagedFiles(dataSourceId);
  if (stagedFiles.length === 0) {
    console.log(
      `[performCombineImport] No staged files found for dataSourceId=${dataSourceId}`,
    );
    return { success: false, rowCount: 0, error: "No staged files to combine" };
  }
  console.log(
    `[performCombineImport] Found ${stagedFiles.length} staged files`,
  );

  const result = await executeImport({
    fileBuffers: [],
    dataSourceId,
    source: "combine",
    stagedFiles,
  });

  // Mark staged files as imported only on success
  if (result.success) {
    for (const file of stagedFiles) {
      await storage.updateFileStatus(file.id, "imported");
    }
  }

  return {
    success: result.success,
    rowCount: result.itemCount || 0,
    error: result.error,
    details: result.details,
  };
}


// === REPLACE: /upload route handler (lines 4132-4965) ===
// Was ~836 lines. Now ~65 lines.
// Keeps: startImport/failImport/completeImport state tracking, file validation,
//        multi-file staging logic (parse + create staged file record).
// Delegates: single-file import to executeImport().

  app.post(
    "/api/data-sources/:id/upload",
    upload.single("file"),
    async (req, res) => {
      const dataSourceId = req.params.id;
      startImport(dataSourceId);

      try {
        const file = req.file;
        if (!file) {
          failImport(dataSourceId, "No file uploaded");
          return res.status(400).json({ error: "No file uploaded" });
        }

        const dataSource = await storage.getDataSource(dataSourceId);
        if (!dataSource) {
          failImport(dataSourceId, "Data source not found");
          return res.status(404).json({ error: "Data source not found" });
        }

        // Pre-import file validation (kept in route handler per design)
        const validationConfig =
          (dataSource as any).importValidationConfig || {};
        if (validationConfig.enabled !== false) {
          console.log(
            `[Upload] Validating file "${file.originalname}" for "${dataSource.name}"`,
          );
          const validation = await validateImportFile(
            file.buffer,
            dataSourceId,
            file.originalname,
          );
          if (!validation.valid) {
            await logValidationFailure(
              dataSourceId,
              file.originalname,
              validation.errors,
              validation.warnings,
            );
            failImport(
              dataSourceId,
              `Validation failed: ${validation.errors.join("; ")}`,
            );
            return res.status(400).json({
              error: "Import blocked - file validation failed",
              validationErrors: validation.errors,
              validationWarnings: validation.warnings,
              message: `SAFETY NET: Import blocked. Issues: ${validation.errors.join("; ")}`,
            });
          }
          if (validation.warnings.length > 0) {
            console.log(
              `[Upload] Validation warnings:`,
              validation.warnings,
            );
          }
        }

        const isMultiFile = (dataSource as any).ingestionMode === "multi";

        // Multi-file staging: parse for preview, create staged file record, return
        if (isMultiFile) {
          const { headers, rows, detectedFormat } = await parseFileForDataSource(
            file.buffer,
            dataSource,
            file.originalname,
          );

          // Save detected format for future imports
          if (detectedFormat) {
            await storage.updateDataSource(dataSourceId, {
              formatType: detectedFormat,
              pivotConfig: { enabled: true, format: detectedFormat },
            });
          }

          const uploadedFile = await storage.createUploadedFile({
            dataSourceId,
            fileName: file.originalname,
            fileSize: file.size,
            rowCount: rows.length,
            previewData: rows as any,
            headers,
            fileStatus: "staged",
          } as any);

          const stagedFiles = await storage.getStagedFiles(dataSourceId);
          return res.status(201).json({
            success: true,
            staged: true,
            file: uploadedFile,
            stagedCount: stagedFiles.length,
            message: `File "${file.originalname}" staged. You have ${stagedFiles.length} file(s) staged. Click "Combine & Import" when ready.`,
          });
        }

        // Single file import — delegate everything to executeImport
        const result = await executeImport({
          fileBuffers: [
            { buffer: file.buffer, originalname: file.originalname, size: file.size },
          ],
          dataSourceId,
          source: "upload",
        });

        if (!result.success) {
          failImport(dataSourceId, result.error || "Import failed");
          // If safety block, return 400 with details
          if (result.safetyBlock) {
            return res.status(400).json({
              error: result.error,
              safetyBlock: true,
              existingCount: result.existingCount,
              newCount: result.newCount,
              dropPercent: result.dropPercent,
            });
          }
          return res.status(400).json({ error: result.error });
        }

        // Signal import completion for sync coordination
        completeImport(dataSourceId, result.itemCount || 0);

        // Start background comparison job for incremental sync
        try {
          const stores = await storage.getShopifyStores();
          if (stores.length > 0) {
            startComparisonJob({ storeId: stores[0].id, dataSourceId });
          }
        } catch (err) {
          console.error("[Upload] Error starting comparison job:", err);
        }

        res.status(201).json({
          success: true,
          file: result.file,
          importedItems: result.itemCount,
          addedItems: result.addedItems,
          updatedItems: result.updatedItems,
          noSizeRemoved: result.noSizeRemoved,
          colorsFixed: result.colorsFixed,
          aiColorsFixed: result.aiColorsFixed,
          duplicatesRemoved: result.duplicatesRemoved,
          filteredItems: result.filteredItems,
          updateStrategy: result.updateStrategy,
          consolidationResult: result.consolidationResult,
          message: result.message,
        });
      } catch (error: any) {
        console.error("Error uploading file:", error);
        failImport(dataSourceId, error.message || "Failed to upload file");
        res
          .status(500)
          .json({ error: error.message || "Failed to upload file" });
      }
    },
  );


// === REPLACE: /fetch-url route handler (lines 5063-5774) ===
// Was ~714 lines. Now ~60 lines.
// Keeps: URL resolution from body/config, sale import check, URL download,
//        file validation, post-import hooks (auto-consolidation, Shopify sync).
// Delegates: parsing + import to executeImport().

  app.post("/api/data-sources/:id/fetch-url", async (req, res) => {
    try {
      const dataSourceId = req.params.id;
      let { url } = req.body;

      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      // If no URL in body, use stored URL from connectionDetails
      if (
        !url &&
        dataSource.type === "url" &&
        dataSource.connectionDetails?.url
      ) {
        url = dataSource.connectionDetails.url;
      }
      if (!url) {
        return res.status(400).json({
          error:
            "URL is required. Either provide a URL in the request or configure one in the data source settings.",
        });
      }

      // Check if sale file import is required first
      const saleImportCheck =
        await checkSaleImportFirstRequirement(dataSourceId);
      if (saleImportCheck.requiresWarning) {
        console.log(
          `[URL Fetch] SKIPPED: ${dataSource.name} - ${saleImportCheck.warningMessage}`,
        );
        return res.status(400).json({
          error: "Sale file not imported",
          requiresSaleImport: true,
          saleDataSourceId: saleImportCheck.saleDataSourceId,
          saleDataSourceName: saleImportCheck.saleDataSourceName,
          message: saleImportCheck.warningMessage,
        });
      }

      // Download the file from URL
      console.log(`Fetching file from URL: ${url}`);
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(
          `Failed to fetch URL: ${response.status} ${response.statusText}`,
        );
      }
      const buffer = Buffer.from(await response.arrayBuffer());
      const urlFilename = url.split("/").pop() || "url_file.xlsx";

      // Pre-import file validation (kept in route handler per design)
      const validationConfig =
        (dataSource as any).importValidationConfig || {};
      if (validationConfig.enabled !== false) {
        console.log(
          `[URL Fetch] Validating file for "${dataSource.name}"`,
        );
        const validation = await validateImportFile(
          buffer,
          dataSourceId,
          urlFilename,
        );
        if (!validation.valid) {
          await logValidationFailure(
            dataSourceId,
            urlFilename,
            validation.errors,
            validation.warnings,
          );
          console.error(
            `[URL Fetch] SAFETY BLOCK: File failed validation:`,
            validation.errors,
          );
          return res.status(400).json({
            error: "Import blocked - file validation failed",
            validationErrors: validation.errors,
            validationWarnings: validation.warnings,
            message: `SAFETY NET: Import blocked. Issues: ${validation.errors.join("; ")}`,
          });
        }
        if (validation.warnings.length > 0) {
          console.log(`[URL Fetch] Validation warnings:`, validation.warnings);
        }
      }

      // Delegate to executeImport for parsing + full import pipeline
      const result = await executeImport({
        fileBuffers: [{ buffer, originalname: urlFilename }],
        dataSourceId,
        source: "fetch-url",
      });

      if (!result.success) {
        if (result.safetyBlock) {
          return res.status(400).json({
            error: result.error,
            safetyBlock: true,
            existingCount: result.existingCount,
            newCount: result.newCount,
            dropPercent: result.dropPercent,
          });
        }
        return res.status(400).json({ error: result.error });
      }

      // Post-import hooks
      try {
        await triggerAutoConsolidationAfterImport(dataSourceId);
      } catch (err: any) {
        console.error(
          "Error in auto-consolidation after URL import:",
          err.message,
        );
      }
      triggerShopifySyncAfterImport(dataSourceId).catch((err) => {
        console.error(
          "Error triggering Shopify sync after URL import:",
          err.message,
        );
      });

      res.json({
        success: true,
        itemCount: result.itemCount,
        noSizeRemoved: result.noSizeRemoved,
        colorsFixed: result.colorsFixed,
        duplicatesRemoved: result.duplicatesRemoved,
      });
    } catch (error: any) {
      console.error("Error fetching URL:", error);
      res
        .status(500)
        .json({ error: error.message || "Failed to fetch and process URL" });
    }
  });


// === REPLACE: /reimport route handler (lines 5958-6250) ===
// Was ~297 lines. Now ~35 lines.
// Keeps: data source + last file + existing items lookup, basic validation.
// Delegates: re-cleaning, rules, expansion, stockInfo, DB save to executeImport().
// NOTE: Fixes existing bug where undefined `rows` variable was passed to
// applyImportRules — executeImport handles this correctly.

  app.post("/api/data-sources/:id/reimport", async (req, res) => {
    try {
      const dataSourceId = req.params.id;

      const dataSource = await storage.getDataSource(dataSourceId);
      if (!dataSource) {
        return res.status(404).json({ error: "Data source not found" });
      }

      const lastFile = await storage.getLatestFile(dataSourceId);
      if (!lastFile) {
        return res.status(404).json({
          error:
            "No files found for this data source. Please upload a file first.",
        });
      }

      const existingItems = await storage.getInventoryItems(dataSourceId);
      if (existingItems.length === 0) {
        return res.status(404).json({
          error: "No inventory items found. Please upload a file first.",
        });
      }

      // Delegate to executeImport with reimport source
      // executeImport handles: delete existing, re-apply cleaning to rawData,
      // clean, rules, expansion, dedup, stockInfo, DB save
      const result = await executeImport({
        fileBuffers: [],
        dataSourceId,
        source: "reimport",
        existingItems,
        fileId: lastFile.id,
      });

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      // Post-import hooks
      try {
        await triggerAutoConsolidationAfterImport(dataSourceId);
      } catch (err: any) {
        console.error(
          "Error in auto-consolidation after reimport:",
          err.message,
        );
      }
      triggerShopifySyncAfterImport(dataSourceId).catch((err) => {
        console.error(
          "Error triggering Shopify sync after reimport:",
          err.message,
        );
      });

      res.json({
        success: true,
        importedItems: result.itemCount,
        noSizeRemoved: result.noSizeRemoved,
        colorsFixed: result.colorsFixed,
        aiColorsFixed: result.aiColorsFixed,
        duplicatesRemoved: result.duplicatesRemoved,
        filteredItems: result.filteredItems,
        addedItems: result.addedItems,
        message: result.message,
      });
    } catch (error: any) {
      console.error("Error re-importing:", error);
      res.status(500).json({ error: error.message || "Failed to re-import" });
    }
  });


// ============================================================================
// SUMMARY OF CHANGES
// ============================================================================
//
// DELETED FUNCTIONS (moved to importEngine or dead code):
//   1. checkSafetyThreshold()        (lines 120-137)   → importEngine
//   2. isCSVBuffer()                  (lines 145-196)   → importEngine (utility)
//   3. parseCSVAsText()               (lines 199-271)   → importEngine (utility)
//   4. applyCleaningToValue()         (lines 807-876)   → duplicate, importUtils has canonical version
//   5. parseTarikEdizFormat()         (lines 879-1019)  → dead code (shared parsers)
//   6. parseJovaniFormat()            (lines 1026-1258) → dead code (shared parsers)
//   7. parseSherriHillFormat()        (lines 1265-1425) → dead code (shared parsers)
//   8. parseGenericPivotedFormat()    (lines 1428-1631) → dead code (shared parsers)
//   9. calculateItemStockInfo()       (lines 1637-1733) → importEngine
//  10. getStockInfoRule()             (lines 1736-1814) → importEngine
//  11. parseExcelToInventory()        (lines 1817-2160) → dead code (uses old parsers)
//  12. parseFerianiGiaFormat()        (~lines 22215+)   → dead code (only called from #11)
//
// SLIMMED FUNCTIONS:
//   1. processUrlDataSourceImport()   (lines 322-802)   → ~10 lines  (was ~480)
//   2. performCombineImport()         (lines 2167-3073) → ~30 lines  (was ~908)
//   3. /upload route handler          (lines 4132-4965) → ~65 lines  (was ~836)
//   4. /fetch-url route handler       (lines 5063-5774) → ~60 lines  (was ~714)
//   5. /reimport route handler        (lines 5958-6250) → ~35 lines  (was ~297)
//
// UNCHANGED (kept as-is):
//   - triggerAutoConsolidationAfterImport()  (lines 274-319, already exported)
//   - multer config                          (lines 804-805)
//   - registerRoutes()                       (lines 3075+, structure preserved)
//   - ALL non-import routes (Shopify, templates, color mappings, rules, etc.)
//
// NET LINE REDUCTION:
//   Deleted: ~2,870 lines (12 functions)
//   Slimmed: ~3,235 lines → ~200 lines (5 functions, -3,035 lines)
//   Total reduction: ~5,905 lines removed from routes.ts
//
// NEW IMPORT ADDED:
//   import { executeImport, calculateItemStockInfo, getStockInfoRule,
//            getStylePrefix, checkSafetyThreshold, parseFileForDataSource
//          } from "./importEngine";
//
// IMPORTS TO REMOVE (no longer used in routes.ts after slimming):
//   - parsePivotedExcelToInventory  (from importUtils — only used in deleted fallbacks)
//   - parseGenericPivotFormat       (from importUtils — only used in deleted code)
//   Note: autoDetectPivotFormat, parseIntelligentPivotFormat, UniversalParserConfig
//   from aiImportRoutes can also be removed if parseFileForDataSource in importEngine
//   handles all parsing internally. Keep them if the /upload staging path calls
//   them directly instead of through parseFileForDataSource.
//
// BEHAVIORAL NOTES:
//   - /upload staging mode: Uses parseFileForDataSource() to get headers/rows for
//     preview data. Format detection + saving detected format is preserved.
//   - /reimport: Fixes existing bug where undefined `rows` was passed to
//     applyImportRules at line 6112. executeImport handles this correctly.
//   - /fetch-url cleaningConfig bug: Line 5269 referenced `cleaningConfig` but
//     it was defined as `fetchCleaningConfig` at line 5163. Fixed by moving
//     all logic into executeImport.
//   - Post-import hooks (auto-consolidation, Shopify sync, comparison jobs)
//     remain in route handlers, NOT in executeImport.
//   - Pre-import validation (validateImportFile) remains in route handlers
//     and processUrlDataSourceImport's caller. executeImport also performs
//     validation internally for paths that call it without pre-validation.
