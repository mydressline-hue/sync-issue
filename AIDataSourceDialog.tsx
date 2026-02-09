/**
 * AI Data Source Dialog - Complete Version
 *
 * Features:
 * 1. Connection settings (URL, Email IMAP) - same as existing
 * 2. Scheduler configuration - same as existing
 * 3. AI format detection with Claude
 * 4. Complex stock cell pattern editor
 * 5. Column-based sale detection (sale price × multiplier)
 * 6. Price-based size expansion tiers
 * 7. Shopify price lookup for compare-at
 * 8. Safety nets integration
 * 9. Multi-file email support
 */

import { useState, useCallback, useEffect } from "react";
import { useMutation, useQueryClient, useQuery } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import {
  Sparkles,
  Upload,
  AlertCircle,
  CheckCircle,
  Loader2,
  Eye,
  Save,
  RefreshCw,
  FileSpreadsheet,
  Columns,
  Filter,
  Calendar,
  Package,
  Plus,
  X,
  Trash2,
  Mail,
  Clock,
  PlayCircle,
  Settings,
  AlertTriangle,
  Info,
  ShieldCheck,
  Files,
  Database,
  DollarSign,
  Ruler,
  Zap,
  TestTube,
  Ban,
  MessageSquare,
  ClipboardCheck,
  FileCheck,
  CircleAlert,
  XCircle,
  Link,
} from "lucide-react";
import { useToast } from "@/hooks/use-toast";

// ============================================================
// TYPES
// ============================================================

interface ComplexStockPattern {
  name: string;
  pattern: string;
  extractStock?: string;
  extractDate?: string;
  markDiscontinued?: boolean;
  markSpecialOrder?: boolean;
  description: string;
}

interface PriceExpansionTier {
  minPrice: number;
  maxPrice?: number;
  expandDown: number;
  expandUp: number;
}

interface DetectedColumn {
  headerName: string;
  columnIndex: number;
  sampleValues: string[];
  detectedType: string;
  confidence: number;
}

interface FormatDetectionResult {
  success: boolean;
  formatType: string;
  formatConfidence: number;
  columns: DetectedColumn[];
  suggestedColumnMapping: Record<string, string>;
  suggestedComplexStockConfig?: {
    enabled: boolean;
    stockColumn: string;
    patterns: ComplexStockPattern[];
  };
  suggestedColumnSaleConfig?: {
    enabled: boolean;
    salePriceColumn: string;
    regularPriceColumn?: string;
    multiplier: number;
    useShopifyAsCompareAt: boolean;
    onlyWhenSalePricePresent: boolean;
  };
  suggestedPriceExpansionConfig?: {
    enabled: boolean;
    tiers: PriceExpansionTier[];
  };
  suggestedDiscontinuedConfig?: any;
  suggestedFutureStockConfig?: any;
  suggestedStockValueConfig?: any;
  detectedPatterns: {
    hasDiscontinuedIndicators: boolean;
    hasDateColumns: boolean;
    hasTextStockValues: boolean;
    hasPriceColumn: boolean;
    hasSalePriceColumn: boolean;
    hasComplexStockCells: boolean;
    complexStockExamples?: string[];
  };
  warnings: string[];
  notes: string[];
  allHeaders?: Array<{ header: string; index: number }>;
}

interface PreviewResult {
  success: boolean;
  preview: {
    sampleItems: any[];
    stats: Record<string, number>;
    uniqueStyles: number;
    formatUsed: string;
  };
  warnings: string[];
}

interface ImportResult {
  success: boolean;
  stats: Record<string, number>;
  warnings: string[];
}

interface DataSource {
  id: string;
  name: string;
  type: string;
  sourceType?: string;
}

interface AIDataSourceDialogProps {
  isOpen: boolean;
  onClose: () => void;
  existingDataSource?: any;
  onSuccess?: () => void;
}

// ============================================================
// COMPONENT
// ============================================================

export default function AIDataSourceDialog({
  isOpen,
  onClose,
  existingDataSource,
  onSuccess,
}: AIDataSourceDialogProps) {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Queries
  const { data: dataSources = [] } = useQuery<DataSource[]>({
    queryKey: ["data-sources"],
    enabled: isOpen,
  });

  const { data: shopifyStores = [] } = useQuery<any[]>({
    queryKey: ["/api/shopify/stores"],
    enabled: isOpen,
  });

  // Fetch fresh data source data when dialog opens (to avoid stale props)
  const { data: freshDataSource } = useQuery<any>({
    queryKey: ["/api/data-sources", existingDataSource?.id],
    queryFn: async () => {
      if (!existingDataSource?.id) return null;
      const res = await fetch(`/api/data-sources/${existingDataSource.id}`);
      if (!res.ok) return null;
      return res.json();
    },
    enabled: isOpen && !!existingDataSource?.id,
    staleTime: 0,
    refetchOnMount: "always",
  });

  // Use fresh data if available, otherwise fall back to prop
  const dataSourceToUse = freshDataSource || existingDataSource;

  // ============================================================
  // STATE - Connection
  // ============================================================
  const [sourceName, setSourceName] = useState("");
  const [sourceType, setSourceType] = useState<"url" | "email" | "manual">(
    "manual",
  );
  const [sourceUrl, setSourceUrl] = useState("");
  const [sourceActive, setSourceActive] = useState(true);
  const [updateStrategy, setUpdateStrategy] = useState<"replace" | "full_sync">(
    "full_sync",
  );
  const [testStatus, setTestStatus] = useState<
    "idle" | "testing" | "success" | "error"
  >("idle");

  // Email settings
  const [emailHost, setEmailHost] = useState("imap.gmail.com");
  const [emailPort, setEmailPort] = useState(993);
  const [emailSecure, setEmailSecure] = useState(true);
  const [emailUsername, setEmailUsername] = useState("");
  const [emailPassword, setEmailPassword] = useState("");
  const [emailFolder, setEmailFolder] = useState("INBOX");
  const [emailSenderWhitelist, setEmailSenderWhitelist] = useState("");
  const [emailSubjectFilter, setEmailSubjectFilter] = useState("");
  const [emailMarkAsRead, setEmailMarkAsRead] = useState(true);
  const [emailDeleteAfterDownload, setEmailDeleteAfterDownload] =
    useState(false);
  const [emailExtractLinksFromBody, setEmailExtractLinksFromBody] =
    useState(false);
  const [emailMultiFileMode, setEmailMultiFileMode] = useState(false);
  const [emailExpectedFiles, setEmailExpectedFiles] = useState(2);

  // Email fetch testing
  const [emailFetchStatus, setEmailFetchStatus] = useState<
    "idle" | "fetching" | "success" | "error"
  >("idle");
  const [emailFetchResult, setEmailFetchResult] = useState<any>(null);
  const [clearHashBeforeFetch, setClearHashBeforeFetch] = useState(false);

  // Email retry queue settings
  const [retryIfNoEmail, setRetryIfNoEmail] = useState(false);
  const [retryIntervalMinutes, setRetryIntervalMinutes] = useState(60);
  const [retryCutoffHour, setRetryCutoffHour] = useState(18);

  // Schedule settings
  const [autoUpdate, setAutoUpdate] = useState(false);
  const [updateFreq, setUpdateFreq] = useState<"hourly" | "daily" | "weekly">(
    "daily",
  );
  const [updateTime, setUpdateTime] = useState("06:00");

  // ============================================================
  // STATE - AI Detection
  // ============================================================
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [manualMultiFileMode, setManualMultiFileMode] = useState(false);
  const [stagedManualFiles, setStagedManualFiles] = useState<File[]>([]);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [detectionResult, setDetectionResult] =
    useState<FormatDetectionResult | null>(null);
  const [previewResult, setPreviewResult] = useState<PreviewResult | null>(
    null,
  );
  const [isLoadingPreview, setIsLoadingPreview] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [importResult, setImportResult] = useState<ImportResult | null>(null);

  // ============================================================
  // STATE - Column Mapping
  // ============================================================
  const [columnMapping, setColumnMapping] = useState<Record<string, string>>(
    {},
  );

  // ============================================================
  // STATE - Complex Stock Patterns
  // ============================================================
  const [complexStockEnabled, setComplexStockEnabled] = useState(false);
  const [complexStockPatterns, setComplexStockPatterns] = useState<
    ComplexStockPattern[]
  >([]);
  const [patternTestValue, setPatternTestValue] = useState("");
  const [patternTestResults, setPatternTestResults] = useState<any[]>([]);

  // ============================================================
  // STATE - Column-Based Sale
  // ============================================================
  const [columnSaleEnabled, setColumnSaleEnabled] = useState(false);
  const [salePriceColumn, setSalePriceColumn] = useState("");
  const [regularPriceColumn, setRegularPriceColumn] = useState("");
  const [saleMultiplier, setSaleMultiplier] = useState(2);
  const [useShopifyCompareAt, setUseShopifyCompareAt] = useState(true);

  // ============================================================
  // STATE - Price-Based Expansion
  // ============================================================
  const [priceExpansionEnabled, setPriceExpansionEnabled] = useState(false);
  const [priceExpansionTiers, setPriceExpansionTiers] = useState<
    PriceExpansionTier[]
  >([
    { minPrice: 500, expandDown: 4, expandUp: 1 },
    { minPrice: 0, maxPrice: 499.99, expandDown: 1, expandUp: 1 },
  ]);

  // ============================================================
  // STATE - Stock Info Messages (Metafield)
  // ============================================================
  const [stockInfoEnabled, setStockInfoEnabled] = useState(true);
  const [inStockMessage, setInStockMessage] = useState(
    "Ship Date - In Stock will ship within 1-3 business days! #22cd02",
  );
  const [outOfStockMessage, setOutOfStockMessage] = useState(
    "Ship Date - Contact for availability #ff0000",
  );
  const [sizeExpansionMessage, setSizeExpansionMessage] = useState(
    "Ship Date - In Stock will ship within 3-5 business days! #22cd02",
  );
  const [futureDateMessage, setFutureDateMessage] = useState(
    "Will Ship by - {date} #ff0000",
  );
  const [dateOffsetDays, setDateOffsetDays] = useState(0);

  // ============================================================
  // STATE - Simple Stock Mappings
  // ============================================================
  const [stockTextMappings, setStockTextMappings] = useState<
    Array<{ text: string; value: number }>
  >([]);
  const [filterZeroStock, setFilterZeroStock] = useState(false);

  // ============================================================
  // STATE - Sale & Linking
  // ============================================================
  const [isSaleFile, setIsSaleFile] = useState(false);
  const [linkedSaleDataSourceId, setLinkedSaleDataSourceId] = useState("");
  const [linkedShopifyStoreId, setLinkedShopifyStoreId] = useState("");

  // ============================================================
  // STATE - Discontinued Rules
  // ============================================================
  const [discontinuedEnabled, setDiscontinuedEnabled] = useState(true);
  const [discontinuedKeywords, setDiscontinuedKeywords] =
    useState("Discontinued");
  const [skipDiscontinued, setSkipDiscontinued] = useState(true);

  // ============================================================
  // STATE - Future Stock Config
  // ============================================================
  const [futureStockEnabled, setFutureStockEnabled] = useState(true);
  const [dateOnlyMode, setDateOnlyMode] = useState(true);
  const [useFutureDateAsShipDate, setUseFutureDateAsShipDate] = useState(true);

  // ============================================================
  // STATE - Size Limits (Enhanced)
  // ============================================================
  const [sizeLimitEnabled, setSizeLimitEnabled] = useState(false);
  const [minSize, setMinSize] = useState<string | null>(null); // Numeric min (null = no minimum)
  const [maxSize, setMaxSize] = useState<string | null>(null); // Numeric max (null = no maximum)
  const [minLetterSize, setMinLetterSize] = useState<string | null>(null); // Letter min (null = no minimum)
  const [maxLetterSize, setMaxLetterSize] = useState<string | null>(null); // Letter max (null = no maximum)
  const [sizePrefixOverrides, setSizePrefixOverrides] = useState<
    Array<{
      pattern: string;
      minSize?: string | null;
      maxSize?: string | null;
      minLetterSize?: string | null;
      maxLetterSize?: string | null;
    }>
  >([]);

  // Size options for dropdowns (includes W/plus sizes)
  const numericSizeOptions = [
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
    // W (plus) sizes
    "16W",
    "18W",
    "20W",
    "22W",
    "24W",
    "26W",
    "28W",
    "30W",
    "32W",
    "34W",
    "36W",
  ];
  const letterSizeOptions = [
    "XXS",
    "XS",
    "S",
    "M",
    "L",
    "XL",
    "XXL",
    "2XL",
    "3XL",
    "4XL",
    "5XL",
  ];

  // ============================================================
  // STATE - Style Cleaning Rules
  // ============================================================
  const [removeCharsByPosition, setRemoveCharsByPosition] = useState<
    Array<{ start: number; end: number }>
  >([]);
  const [findReplaceRules, setFindReplaceRules] = useState<
    Array<{ find: string; replace: string }>
  >([]);
  const [removePatterns, setRemovePatterns] = useState<string[]>([]);

  // ============================================================
  // STATE - Combined Variant Code Parsing
  // ============================================================
  const [combinedCodeEnabled, setCombinedCodeEnabled] = useState(false);
  const [combinedCodeColumn, setCombinedCodeColumn] = useState("");
  const [combinedCodeDelimiter, setCombinedCodeDelimiter] = useState("-");
  const [combinedCodeOrder, setCombinedCodeOrder] =
    useState("style-color-size");

  // ============================================================
  // STATE - Custom Style Prefixes
  // ============================================================
  const [customPrefixEnabled, setCustomPrefixEnabled] = useState(false);
  const [stylePrefixRules, setStylePrefixRules] = useState<
    Array<{ pattern: string; prefix: string }>
  >([]);

  // ============================================================
  // STATE - Zero Price Handling
  // ============================================================
  const [zeroPriceAction, setZeroPriceAction] = useState<
    "keep" | "skip" | "use_shopify"
  >("keep");

  // ============================================================
  // STATE - Value Replacement Rules
  // ============================================================
  const [valueReplacements, setValueReplacements] = useState<
    Array<{ field: string; from: string; to: string }>
  >([]);

  // ============================================================
  // STATE - Safety
  // ============================================================
  const [validationEnabled, setValidationEnabled] = useState(true);
  const [minRowCount, setMinRowCount] = useState<number | undefined>(undefined);
  const [rowCountTolerance, setRowCountTolerance] = useState(50);

  // ============================================================
  // STATE - Post-Import Validation Config
  // ============================================================
  const [postValidationEnabled, setPostValidationEnabled] = useState(false);

  // Checksum validation (NEW)
  const [checksumVerifyItemCount, setChecksumVerifyItemCount] = useState(true);
  const [checksumVerifyTotalStock, setChecksumVerifyTotalStock] =
    useState(true);
  const [checksumVerifyStyleCount, setChecksumVerifyStyleCount] =
    useState(true);
  const [checksumVerifyColorCount, setChecksumVerifyColorCount] =
    useState(false);
  const [checksumTolerancePercent, setChecksumTolerancePercent] = useState(0);

  // Distribution validation (NEW)
  const [distMinPercentWithStock, setDistMinPercentWithStock] = useState<
    number | undefined
  >(undefined);
  const [distMaxPercentWithStock, setDistMaxPercentWithStock] = useState<
    number | undefined
  >(undefined);
  const [distMinPercentWithPrice, setDistMinPercentWithPrice] = useState<
    number | undefined
  >(undefined);
  const [distMinPercentWithShipDate, setDistMinPercentWithShipDate] = useState<
    number | undefined
  >(undefined);

  // Historical comparison (NEW)
  const [deltaEnabled, setDeltaEnabled] = useState(false);
  const [deltaMaxItemCountDrop, setDeltaMaxItemCountDrop] = useState<
    number | undefined
  >(10);
  const [deltaMaxStockDrop, setDeltaMaxStockDrop] = useState<
    number | undefined
  >(20);
  const [deltaMaxStyleDrop, setDeltaMaxStyleDrop] = useState<
    number | undefined
  >(5);

  // Count rules
  const [countMinItems, setCountMinItems] = useState<number | undefined>(
    undefined,
  );
  const [countMaxItems, setCountMaxItems] = useState<number | undefined>(
    undefined,
  );
  const [countMinStyles, setCountMinStyles] = useState<number | undefined>(
    undefined,
  );
  const [countMaxStyles, setCountMaxStyles] = useState<number | undefined>(
    undefined,
  );
  const [countMinFutureStockItems, setCountMinFutureStockItems] = useState<
    number | undefined
  >(undefined);
  const [countMinDiscontinuedItems, setCountMinDiscontinuedItems] = useState<
    number | undefined
  >(undefined);

  // Rule checks
  const [verifyDiscontinuedDetection, setVerifyDiscontinuedDetection] =
    useState(false);
  const [verifyFutureDatesDetection, setVerifyFutureDatesDetection] =
    useState(false);
  const [verifySizeExpansion, setVerifySizeExpansion] = useState(false);
  const [verifyStockTextMappings, setVerifyStockTextMappings] = useState(false);
  const [verifyPriceExtraction, setVerifyPriceExtraction] = useState(false);

  // Spot checks
  const [spotChecks, setSpotChecks] = useState<
    Array<{
      style: string;
      color?: string;
      size?: string;
      expectedCondition:
        | "exists"
        | "stock_gt_0"
        | "has_future_date"
        | "is_discontinued"
        | "has_price";
    }>
  >([]);

  // Validation report (shown after import)
  const [validationReport, setValidationReport] = useState<any>(null);

  // DB Validation state (validate existing data without file upload)
  const [isValidatingDb, setIsValidatingDb] = useState(false);
  const [dbValidationResult, setDbValidationResult] = useState<any>(null);
  const [dbItemCount, setDbItemCount] = useState<number | null>(null);

  // ============================================================
  // STATE - UI
  // ============================================================
  const [activeTab, setActiveTab] = useState("connection");
  const [createdDataSourceId, setCreatedDataSourceId] = useState<string | null>(
    null,
  );

  // Reset form
  const resetForm = useCallback(() => {
    setSourceName("");
    setSourceType("manual");
    setSourceUrl("");
    setSourceActive(true);
    setUpdateStrategy("full_sync");
    setTestStatus("idle");
    setEmailHost("imap.gmail.com");
    setEmailPort(993);
    setEmailSecure(true);
    setEmailUsername("");
    setEmailPassword("");
    setEmailFolder("INBOX");
    setEmailSenderWhitelist("");
    setEmailSubjectFilter("");
    setEmailMarkAsRead(true);
    setEmailDeleteAfterDownload(false);
    setEmailMultiFileMode(false);
    setEmailExpectedFiles(2);
    setRetryIfNoEmail(false);
    setRetryIntervalMinutes(60);
    setRetryCutoffHour(18);
    setAutoUpdate(false);
    setUpdateFreq("daily");
    setUpdateTime("06:00");
    setSelectedFile(null);
    setManualMultiFileMode(false);
    setStagedManualFiles([]);
    setIsAnalyzing(false);
    setDetectionResult(null);
    setPreviewResult(null);
    setImportResult(null);
    setColumnMapping({});
    setComplexStockEnabled(false);
    setComplexStockPatterns([]);
    setColumnSaleEnabled(false);
    setSalePriceColumn("");
    setRegularPriceColumn("");
    setSaleMultiplier(2);
    setUseShopifyCompareAt(true);
    setPriceExpansionEnabled(false);
    setPriceExpansionTiers([
      { minPrice: 500, expandDown: 4, expandUp: 1 },
      { minPrice: 0, maxPrice: 499.99, expandDown: 1, expandUp: 1 },
    ]);
    // Reset stock info config to defaults
    setStockInfoEnabled(true);
    setInStockMessage(
      "Ship Date - In Stock will ship within 1-3 business days! #22cd02",
    );
    setOutOfStockMessage("Ship Date - Contact for availability #ff0000");
    setSizeExpansionMessage(
      "Ship Date - In Stock will ship within 3-5 business days! #22cd02",
    );
    setFutureDateMessage("Will Ship by - {date} #ff0000");
    setDateOffsetDays(0);
    setStockTextMappings([]);
    setIsSaleFile(false);
    setLinkedSaleDataSourceId("");
    setLinkedShopifyStoreId("");
    setValidationEnabled(true);
    setMinRowCount(undefined);
    setActiveTab("connection");
    setCreatedDataSourceId(null);
    // Reset post-import validation config
    setPostValidationEnabled(false);
    // Reset checksum validation
    setChecksumVerifyItemCount(true);
    setChecksumVerifyTotalStock(true);
    setChecksumVerifyStyleCount(true);
    setChecksumVerifyColorCount(false);
    setChecksumTolerancePercent(0);
    // Reset distribution validation
    setDistMinPercentWithStock(undefined);
    setDistMaxPercentWithStock(undefined);
    setDistMinPercentWithPrice(undefined);
    setDistMinPercentWithShipDate(undefined);
    // Reset historical comparison
    setDeltaEnabled(false);
    setDeltaMaxItemCountDrop(10);
    setDeltaMaxStockDrop(20);
    setDeltaMaxStyleDrop(5);
    // Reset count rules
    setCountMinItems(undefined);
    setCountMaxItems(undefined);
    setCountMinStyles(undefined);
    setCountMaxStyles(undefined);
    setCountMinFutureStockItems(undefined);
    setCountMinDiscontinuedItems(undefined);
    // Reset rule checks
    setVerifyDiscontinuedDetection(false);
    setVerifyFutureDatesDetection(false);
    setVerifySizeExpansion(false);
    setVerifyStockTextMappings(false);
    setVerifyPriceExtraction(false);
    setSpotChecks([]);
    setValidationReport(null);
    // Reset cleaning config fields
    setCustomPrefixEnabled(false);
    setStylePrefixRules([]);
    setFindReplaceRules([]);
    setRemovePatterns([]);
    setRemoveCharsByPosition([]);
    setCombinedCodeEnabled(false);
    setCombinedCodeColumn("");
    setCombinedCodeDelimiter("-");
    setCombinedCodeOrder(["style", "color", "size"]);
    // Reset other rule engine fields
    setDiscontinuedEnabled(false);
    setDiscontinuedKeywords("");
    setSkipDiscontinued(false);
    setFutureStockEnabled(false);
    setDateOnlyMode(false);
    setUseFutureDateAsShipDate(false);
    // Reset enhanced size limits
    setSizeLimitEnabled(false);
    setMinSize(null);
    setMaxSize(null);
    setMinLetterSize(null);
    setMaxLetterSize(null);
    setSizePrefixOverrides([]);
    setZeroPriceAction("keep");
    setValueReplacements([]);
    setFilterZeroStock(true);
  }, []);

  // Load existing data source values when editing (use fresh data if available)
  useEffect(() => {
    if (dataSourceToUse) {
      // Basic info
      setSourceName(dataSourceToUse.name || "");
      // Infer connection type from data (emailSettings, connectionDetails.url, or manual)
      const inferredType = dataSourceToUse.emailSettings?.host
        ? "email"
        : dataSourceToUse.connectionDetails?.url
          ? "url"
          : "manual";
      setSourceType(inferredType);
      setSourceActive(dataSourceToUse.isActive !== false);
      setUpdateStrategy(dataSourceToUse.updateStrategy || "full_sync");
      setIsSaleFile(dataSourceToUse.sourceType === "sales");
      setLinkedSaleDataSourceId(dataSourceToUse.assignedSaleDataSourceId || "");
      setLinkedShopifyStoreId(dataSourceToUse.shopifyStoreId || "");
      setAutoUpdate(dataSourceToUse.autoUpdate || false);
      setUpdateFreq(dataSourceToUse.updateFrequency || "daily");
      setUpdateTime(dataSourceToUse.updateTime || "06:00");
      setFilterZeroStock(dataSourceToUse.filterZeroStock !== false);

      // Column mapping
      if (dataSourceToUse.columnMapping) {
        setColumnMapping(dataSourceToUse.columnMapping);
      }

      // Cleaning Config - load all saved values
      const cc = dataSourceToUse.cleaningConfig || {};
      setStockTextMappings(cc.stockTextMappings || []);
      setCustomPrefixEnabled(cc.useCustomPrefixes || false);
      setStylePrefixRules(cc.stylePrefixRules || []);
      // Load find/replace rules - prefer array format, fallback to legacy single findText/replaceText
      if (
        cc.findReplaceRules &&
        Array.isArray(cc.findReplaceRules) &&
        cc.findReplaceRules.length > 0
      ) {
        setFindReplaceRules(cc.findReplaceRules);
      } else if (cc.findText) {
        setFindReplaceRules([
          { find: cc.findText, replace: cc.replaceText || "" },
        ]);
      } else {
        setFindReplaceRules([]);
      }
      setRemovePatterns(cc.removePatterns || []);
      // Load removeFirstN/LastN as position rules
      const posRules: Array<{ start?: number; end?: number }> = [];
      if (cc.removeFirstN) posRules.push({ start: 0, end: cc.removeFirstN });
      if (cc.removeLastN) posRules.push({ start: cc.removeLastN, end: -1 });
      setRemoveCharsByPosition(posRules);
      // Combined code config
      setCombinedCodeEnabled(!!cc.combinedVariantColumn);
      setCombinedCodeColumn(cc.combinedVariantColumn || "");
      setCombinedCodeDelimiter(cc.combinedVariantDelimiter || "-");
      setCombinedCodeOrder(
        cc.combinedVariantOrder || ["style", "color", "size"],
      );

      // Discontinued Rules
      const dr = dataSourceToUse.discontinuedRules || {};
      setDiscontinuedEnabled(dr.enabled || false);
      setDiscontinuedKeywords((dr.keywords || []).join(", "));
      setSkipDiscontinued(dr.skipDiscontinued || false);

      // Future Stock Config
      const fsc = dataSourceToUse.futureStockConfig || {};
      setFutureStockEnabled(fsc.enabled || false);
      setDateOnlyMode(fsc.dateOnlyMode || false);
      setUseFutureDateAsShipDate(fsc.useFutureDateAsShipDate || false);

      // Size Limit Config (Enhanced)
      const slc = dataSourceToUse.sizeLimitConfig || {};
      setSizeLimitEnabled(slc.enabled || false);
      setMinSize(slc.minSize || null);
      setMaxSize(slc.maxSize || null);
      setMinLetterSize(slc.minLetterSize || null);
      setMaxLetterSize(slc.maxLetterSize || null);
      setSizePrefixOverrides(slc.prefixOverrides || []);

      // Price-Based Expansion Config
      const pec = dataSourceToUse.priceBasedExpansionConfig || {};
      setPriceExpansionEnabled(pec.enabled || false);
      setPriceExpansionTiers(
        pec.tiers || [
          { minPrice: 500, expandDown: 4, expandUp: 1 },
          { minPrice: 0, maxPrice: 499.99, expandDown: 1, expandUp: 1 },
        ],
      );

      // Stock Info Config
      const sic = dataSourceToUse.stockInfoConfig || {};
      setStockInfoEnabled(!!sic.message1InStock);
      setInStockMessage(sic.message1InStock || "In Stock");
      setOutOfStockMessage(sic.message3Default || "Special Order");
      setSizeExpansionMessage(
        sic.message2ExtraSizes || "Available in Extra Sizes",
      );
      setFutureDateMessage(sic.message4FutureDate || "Ships {date}");
      setDateOffsetDays(sic.dateOffsetDays || 0);

      // Validation Config (pre-import)
      const vic = dataSourceToUse.importValidationConfig || {};
      setValidationEnabled(vic.enabled !== false);
      setMinRowCount(vic.minRowCount);

      // Post-Import Validation Config
      const pvc = dataSourceToUse.validationConfig || {};
      setPostValidationEnabled(pvc.enabled || false);

      // Checksum rules (NEW)
      const checksumRules = pvc.checksumRules || {};
      setChecksumVerifyItemCount(checksumRules.verifyItemCount !== false);
      setChecksumVerifyTotalStock(checksumRules.verifyTotalStock !== false);
      setChecksumVerifyStyleCount(checksumRules.verifyStyleCount !== false);
      setChecksumVerifyColorCount(checksumRules.verifyColorCount || false);
      setChecksumTolerancePercent(checksumRules.tolerancePercent || 0);

      // Distribution rules (NEW)
      const distRules = pvc.distributionRules || {};
      setDistMinPercentWithStock(distRules.minPercentWithStock);
      setDistMaxPercentWithStock(distRules.maxPercentWithStock);
      setDistMinPercentWithPrice(distRules.minPercentWithPrice);
      setDistMinPercentWithShipDate(distRules.minPercentWithShipDate);

      // Delta/Historical rules (NEW)
      const deltaRules = pvc.deltaRules || {};
      setDeltaEnabled(deltaRules.enabled || false);
      setDeltaMaxItemCountDrop(deltaRules.maxItemCountDropPercent ?? 10);
      setDeltaMaxStockDrop(deltaRules.maxStockDropPercent ?? 20);
      setDeltaMaxStyleDrop(deltaRules.maxStyleDropPercent ?? 5);

      // Count rules
      const countRules = pvc.countRules || {};
      setCountMinItems(countRules.minItems);
      setCountMaxItems(countRules.maxItems);
      setCountMinStyles(countRules.minStyles);
      setCountMaxStyles(countRules.maxStyles);
      setCountMinFutureStockItems(countRules.minFutureStockItems);
      setCountMinDiscontinuedItems(countRules.minDiscontinuedItems);
      // Rule checks
      const ruleChecks = pvc.ruleChecks || {};
      setVerifyDiscontinuedDetection(
        ruleChecks.verifyDiscontinuedDetection || false,
      );
      setVerifyFutureDatesDetection(
        ruleChecks.verifyFutureDatesDetection || false,
      );
      setVerifySizeExpansion(ruleChecks.verifySizeExpansion || false);
      setVerifyStockTextMappings(ruleChecks.verifyStockTextMappings || false);
      setVerifyPriceExtraction(ruleChecks.verifyPriceExtraction || false);
      // Spot checks
      setSpotChecks(pvc.spotChecks || []);

      // Zero Price Config (regularPriceConfig)
      const rpc = dataSourceToUse.regularPriceConfig || {};
      if (rpc.skipZeroPrice) {
        setZeroPriceAction("skip");
      } else {
        setZeroPriceAction("keep");
      }

      // Value Replacements
      setValueReplacements(dataSourceToUse.valueReplacementRules || []);

      // Sales Config
      const sc = dataSourceToUse.salesConfig || {};
      if (sc.priceMultiplier) {
        setColumnSaleEnabled(true);
        setSaleMultiplier(sc.priceMultiplier);
        setUseShopifyCompareAt(sc.useCompareAtPrice || false);
      }

      // Email settings - check if emailSettings.host exists (not sourceType which is inventory/sales)
      const es = dataSourceToUse.emailSettings || {};
      if (es.host) {
        setEmailHost(es.host || "imap.gmail.com");
        setEmailPort(es.port || 993);
        setEmailSecure(es.secure !== false);
        setEmailUsername(es.username || "");
        setEmailPassword(es.password || "");
        setEmailFolder(es.folder || "INBOX");
        setEmailSenderWhitelist((es.senderWhitelist || []).join(", "));
        setEmailSubjectFilter(es.subjectFilter || "");
        setEmailMarkAsRead(es.markAsRead !== false);
        setEmailDeleteAfterDownload(es.deleteAfterDownload || false);
        setEmailExtractLinksFromBody(es.extractLinksFromBody || false);
        setEmailMultiFileMode(
          es.multiFileMode ||
            (dataSourceToUse as any).ingestionMode === "multi" ||
            false,
        );
        setEmailExpectedFiles(es.expectedFiles || 2);
      }

      // Email retry queue settings (at data source level, not in emailSettings)
      setRetryIfNoEmail(dataSourceToUse.retryIfNoEmail || false);
      setRetryIntervalMinutes(dataSourceToUse.retryIntervalMinutes || 60);
      setRetryCutoffHour(dataSourceToUse.retryCutoffHour ?? 18);

      // URL settings
      if (dataSourceToUse.connectionDetails?.url) {
        setSourceUrl(dataSourceToUse.connectionDetails.url);
      }
    }
  }, [dataSourceToUse]);

  // Create mutation
  const createDataSourceMutation = useMutation({
    mutationFn: async (data: any) => {
      const response = await apiRequest("POST", "/api/data-sources", data);
      return response.json();
    },
    onSuccess: (data) => {
      toast({
        title: "Data source created",
        description: `"${data.name}" created.`,
      });
      setCreatedDataSourceId(data.id);
      setActiveTab("ai-detection");
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  // Test connection
  const runTestConnection = async () => {
    setTestStatus("testing");
    try {
      if (sourceType === "url") {
        const response = await apiRequest("POST", "/api/test-url-connection", {
          url: sourceUrl,
        });
        const result = await response.json();
        setTestStatus(result.success ? "success" : "error");
      } else if (sourceType === "email") {
        const response = await apiRequest(
          "POST",
          "/api/test-email-connection",
          {
            host: emailHost,
            port: emailPort,
            secure: emailSecure,
            username: emailUsername,
            password: emailPassword,
            folder: emailFolder,
          },
        );
        const result = await response.json();
        setTestStatus(result.success ? "success" : "error");
      }
    } catch (error: any) {
      setTestStatus("error");
    }
  };

  // Fetch Email Now (for testing email imports)
  const fetchEmailNow = async () => {
    const dataSourceId = createdDataSourceId || existingDataSource?.id;
    if (!dataSourceId) {
      toast({ title: "Save the data source first", variant: "destructive" });
      return;
    }
    setEmailFetchStatus("fetching");
    setEmailFetchResult(null);
    try {
      const response = await apiRequest(
        "POST",
        `/api/data-sources/${dataSourceId}/fetch-email`,
        clearHashBeforeFetch ? { clearHash: true } : {},
      );
      const result = await response.json();
      setEmailFetchResult(result);
      if (result.success) {
        setEmailFetchStatus("success");
        toast({
          title: `Email fetch complete`,
          description: `${result.filesProcessed} file(s) processed`,
        });
        queryClient.invalidateQueries({ queryKey: ["/api/data-sources"] });
      } else {
        setEmailFetchStatus("error");
        toast({
          title: "Email fetch failed",
          description: result.error || "Unknown error",
          variant: "destructive",
        });
      }
    } catch (error: any) {
      setEmailFetchStatus("error");
      setEmailFetchResult({ error: error.message });
      toast({
        title: "Email fetch failed",
        description: error.message,
        variant: "destructive",
      });
    }
  };

  // Clear email hash/logs only (without fetching)
  const clearEmailHash = async () => {
    const dataSourceId = createdDataSourceId || existingDataSource?.id;
    if (!dataSourceId) return;
    try {
      const response = await apiRequest(
        "POST",
        `/api/data-sources/${dataSourceId}/clear-email-hash`,
      );
      const result = await response.json();
      toast({
        title: "Hash cleared",
        description: result.message || `Cleared ${result.deletedCount} log(s)`,
      });
      setEmailFetchStatus("idle");
      setEmailFetchResult(null);
    } catch (error: any) {
      toast({
        title: "Failed to clear hash",
        description: error.message,
        variant: "destructive",
      });
    }
  };

  // File select
  const handleFileSelect = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) {
        if (manualMultiFileMode) {
          setStagedManualFiles((prev) => [...prev, file]);
          setSelectedFile(file);
        } else {
          setSelectedFile(file);
          setStagedManualFiles([]);
        }
        setDetectionResult(null);
        setPreviewResult(null);
        setImportResult(null);
      }
      if (e.target) {
        e.target.value = "";
      }
    },
    [manualMultiFileMode],
  );

  // Analyze with AI
  const analyzeFile = async () => {
    const filesToAnalyze =
      manualMultiFileMode && stagedManualFiles.length > 0
        ? stagedManualFiles
        : selectedFile
          ? [selectedFile]
          : [];

    if (filesToAnalyze.length === 0) return;

    const dataSourceId = createdDataSourceId || existingDataSource?.id;
    if (!dataSourceId) {
      toast({ title: "Create data source first", variant: "destructive" });
      return;
    }
    setIsAnalyzing(true);
    try {
      const formData = new FormData();
      filesToAnalyze.forEach((file, index) => {
        formData.append(index === 0 ? "file" : `file${index}`, file);
      });
      formData.append("dataSourceId", dataSourceId);
      formData.append("vendorHint", sourceName);
      if (manualMultiFileMode && filesToAnalyze.length > 1) {
        formData.append("multiFileMode", "true");
        formData.append("fileCount", String(filesToAnalyze.length));
      }
      const response = await fetch("/api/ai-import/analyze", {
        method: "POST",
        body: formData,
      });
      if (!response.ok) throw new Error((await response.json()).error);
      const result = await response.json();
      const detection = result.detection || result; // Handle both wrapped and unwrapped response
      if (!detection) {
        throw new Error("No detection result returned");
      }
      setDetectionResult(detection);

      // Apply AI suggestions
      if (detection.suggestedColumnMapping)
        setColumnMapping(detection.suggestedColumnMapping);

      if (detection.suggestedComplexStockConfig?.enabled) {
        setComplexStockEnabled(true);
        setComplexStockPatterns(
          detection.suggestedComplexStockConfig.patterns || [],
        );
      }

      if (detection.suggestedColumnSaleConfig?.enabled) {
        setColumnSaleEnabled(true);
        setSalePriceColumn(
          detection.suggestedColumnSaleConfig.salePriceColumn || "",
        );
        setRegularPriceColumn(
          detection.suggestedColumnSaleConfig.regularPriceColumn || "",
        );
        setSaleMultiplier(detection.suggestedColumnSaleConfig.multiplier || 2);
        setUseShopifyCompareAt(
          detection.suggestedColumnSaleConfig.useShopifyAsCompareAt !== false,
        );
      }

      if (detection.suggestedPriceExpansionConfig) {
        setPriceExpansionTiers(
          detection.suggestedPriceExpansionConfig.tiers || [],
        );
      }

      if (detection.suggestedStockValueConfig?.textMappings) {
        setStockTextMappings(detection.suggestedStockValueConfig.textMappings);
      }

      toast({
        title: "Analysis complete",
        description: `Format: ${detection.formatType}`,
      });
    } catch (error: any) {
      toast({
        title: "Analysis failed",
        description: error.message,
        variant: "destructive",
      });
    } finally {
      setIsAnalyzing(false);
    }
  };

  // Test patterns
  const testPatterns = async () => {
    if (!patternTestValue || complexStockPatterns.length === 0) return;
    try {
      const response = await apiRequest(
        "POST",
        "/api/ai-import/test-patterns",
        {
          patterns: complexStockPatterns,
          testValues: [patternTestValue],
        },
      );
      const result = await response.json();
      setPatternTestResults(result.results || []);
    } catch (error: any) {
      toast({
        title: "Test failed",
        description: error.message,
        variant: "destructive",
      });
    }
  };

  // Preview
  const runPreview = async () => {
    if (!selectedFile) return;
    const dataSourceId = createdDataSourceId || existingDataSource?.id;
    if (!dataSourceId) return;
    setIsLoadingPreview(true);
    try {
      const config = buildConfig();
      const formData = new FormData();
      formData.append("file", selectedFile);
      formData.append("dataSourceId", dataSourceId);
      formData.append("config", JSON.stringify(config));
      const response = await fetch("/api/ai-import/preview", {
        method: "POST",
        body: formData,
      });
      if (!response.ok) {
        let errorMessage = "Preview failed";
        try {
          const contentType = response.headers.get("content-type");
          if (contentType && contentType.includes("application/json")) {
            const error = await response.json();
            if (error.safetyBlock) {
              toast({
                title: "⚠️ SAFETY NET: Blocked",
                description: error.message,
                variant: "destructive",
              });
              return;
            }
            errorMessage = error.error || error.message || "Preview failed";
          } else {
            errorMessage = `Server error (${response.status})`;
          }
        } catch {
          errorMessage = `Server error (${response.status})`;
        }
        throw new Error(errorMessage);
      }
      const result = await response.json();
      setPreviewResult(result);
      toast({ title: "Preview ready" });
    } catch (error: any) {
      toast({
        title: "Preview failed",
        description: error.message,
        variant: "destructive",
      });
    } finally {
      setIsLoadingPreview(false);
    }
  };

  // Build config
  const buildConfig = () => {
    return {
      formatType: detectionResult?.formatType || "row",
      columnMapping,
      // CRITICAL FIX: Include pivotConfig for pivot format files (Sherri Hill, etc.)
      // Must add enabled:true because backend checks pivotConfig?.enabled
      pivotConfig: detectionResult?.pivotConfig
        ? { ...detectionResult.pivotConfig, enabled: true }
        : existingDataSource?.pivotConfig || undefined,
      // Explicitly pass filterZeroStock so it can override the DB setting
      filterZeroStock: filterZeroStock,
      complexStockConfig: complexStockEnabled
        ? {
            enabled: true,
            stockColumn: columnMapping.stock || "",
            patterns: complexStockPatterns,
          }
        : undefined,
      columnSaleConfig: columnSaleEnabled
        ? {
            enabled: true,
            salePriceColumn,
            regularPriceColumn: regularPriceColumn || undefined,
            multiplier: saleMultiplier,
            useShopifyAsCompareAt: useShopifyCompareAt,
            onlyWhenSalePricePresent: true,
          }
        : undefined,
      // Always include priceExpansionConfig with enabled flag
      priceExpansionConfig: {
        enabled: priceExpansionEnabled,
        tiers: priceExpansionTiers,
        defaultExpandDown: 0,
        defaultExpandUp: 0,
      },
      stockValueConfig:
        stockTextMappings.length > 0
          ? { textMappings: stockTextMappings }
          : undefined,
      // Include stock info config for import-time message calculation
      stockInfoConfig: stockInfoEnabled
        ? {
            message1InStock: inStockMessage,
            message2ExtraSizes: sizeExpansionMessage,
            message3Default: outOfStockMessage,
            message4FutureDate: futureDateMessage,
            dateOffsetDays: dateOffsetDays,
          }
        : undefined,
    };
  };

  // Save config
  const saveConfiguration = async () => {
    const dataSourceId = createdDataSourceId || existingDataSource?.id;
    if (!dataSourceId) return;
    try {
      const config = buildConfig();
      await fetch(`/api/ai-import/save-config/${dataSourceId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });

      const updatePayload: any = {
        columnMapping,
        filterZeroStock,
        updateStrategy, // BUG FIX: Was missing - strategy changes weren't being saved
        sourceType: isSaleFile ? "sales" : "inventory",
        assignedSaleDataSourceId: linkedSaleDataSourceId || null,
        shopifyStoreId: linkedShopifyStoreId || null,
        autoUpdate,
        updateFrequency: autoUpdate ? updateFreq : null,
        updateTime: autoUpdate && updateFreq === "daily" ? updateTime : null,

        // Connection type and details - BUG FIX: Was missing from update payload
        type: sourceType,
        connectionDetails: sourceType === "url" ? { url: sourceUrl } : null,

        // Format Type - needed for parser selection
        formatType: detectionResult?.formatType || "row",

        // CRITICAL FIX: Include pivotConfig for pivot format files (Sherri Hill, etc.)
        // Without this, daily uploads fail because they can't find pivot configuration
        // Must add enabled:true because backend checks pivotConfig?.enabled
        pivotConfig: detectionResult?.pivotConfig
          ? { ...detectionResult.pivotConfig, enabled: true }
          : existingDataSource?.pivotConfig || undefined,

        // Stock Info Config - BUG FIX: Was missing from updatePayload!
        stockInfoConfig: stockInfoEnabled
          ? {
              message1InStock: inStockMessage,
              message2ExtraSizes: sizeExpansionMessage,
              message3Default: outOfStockMessage,
              message4FutureDate: futureDateMessage,
              dateOffsetDays: dateOffsetDays,
            }
          : undefined,

        // Complex Stock Config (patterns like "In Stock", "Out of Stock")
        complexStockConfig: complexStockEnabled
          ? {
              enabled: true,
              stockColumn: columnMapping.stock || "",
              patterns: complexStockPatterns,
            }
          : undefined,

        // Column Sale Config (sale price from column)
        columnSaleConfig: columnSaleEnabled
          ? {
              enabled: true,
              salePriceColumn,
              regularPriceColumn: regularPriceColumn || undefined,
              multiplier: saleMultiplier,
              useShopifyAsCompareAt: useShopifyCompareAt,
              onlyWhenSalePricePresent: true,
            }
          : undefined,

        // Stock Value Config (text to number mappings)
        stockValueConfig:
          stockTextMappings.length > 0
            ? { textMappings: stockTextMappings }
            : undefined,

        // Sales Config
        salesConfig: columnSaleEnabled
          ? {
              priceMultiplier: saleMultiplier,
              useCompareAtPrice: useShopifyCompareAt,
            }
          : undefined,

        // Validation Config (pre-import)
        importValidationConfig: {
          enabled: validationEnabled,
          minRowCount,
          rowCountTolerance,
        },

        // Post-Import Validation Config (Enhanced)
        validationConfig: {
          enabled: postValidationEnabled,
          // Checksum validation (mathematical proof)
          checksumRules: {
            verifyItemCount: checksumVerifyItemCount,
            verifyTotalStock: checksumVerifyTotalStock,
            verifyStyleCount: checksumVerifyStyleCount,
            verifyColorCount: checksumVerifyColorCount,
            tolerancePercent: checksumTolerancePercent,
          },
          // Distribution validation (data shape)
          distributionRules: {
            minPercentWithStock: distMinPercentWithStock,
            maxPercentWithStock: distMaxPercentWithStock,
            minPercentWithPrice: distMinPercentWithPrice,
            minPercentWithShipDate: distMinPercentWithShipDate,
          },
          // Historical comparison (delta from previous)
          deltaRules: {
            enabled: deltaEnabled,
            maxItemCountDropPercent: deltaMaxItemCountDrop,
            maxStockDropPercent: deltaMaxStockDrop,
            maxStyleDropPercent: deltaMaxStyleDrop,
          },
          // Count validation (expected ranges)
          countRules: {
            minItems: countMinItems,
            maxItems: countMaxItems,
            minStyles: countMinStyles,
            maxStyles: countMaxStyles,
            minFutureStockItems: countMinFutureStockItems,
            minDiscontinuedItems: countMinDiscontinuedItems,
          },
          // Rule checks
          ruleChecks: {
            verifyDiscontinuedDetection,
            verifyFutureDatesDetection,
            verifySizeExpansion,
            verifyStockTextMappings,
            verifyPriceExtraction,
          },
          // Spot checks
          spotChecks,
        },

        // Price-Based Expansion Config
        priceBasedExpansionConfig: {
          enabled: priceExpansionEnabled,
          tiers: priceExpansionTiers,
          defaultExpandDown: 0,
          defaultExpandUp: 0,
        },

        // Discontinued Rules
        discontinuedRules: discontinuedEnabled
          ? {
              enabled: true,
              keywords: discontinuedKeywords
                .split(",")
                .map((k) => k.trim())
                .filter(Boolean),
              skipDiscontinued: skipDiscontinued,
            }
          : { enabled: false },

        // Future Stock Config
        futureStockConfig: futureStockEnabled
          ? {
              enabled: true,
              dateOnlyMode: dateOnlyMode,
              useFutureDateAsShipDate: useFutureDateAsShipDate,
            }
          : { enabled: false },

        // Size Limit Config (Enhanced)
        sizeLimitConfig: sizeLimitEnabled
          ? {
              enabled: true,
              minSize: minSize || undefined,
              maxSize: maxSize || undefined,
              minLetterSize: minLetterSize || undefined,
              maxLetterSize: maxLetterSize || undefined,
              prefixOverrides:
                sizePrefixOverrides.filter((o) => o.pattern).length > 0
                  ? sizePrefixOverrides
                      .filter((o) => o.pattern)
                      .map((o) => ({
                        pattern: o.pattern,
                        minSize: o.minSize || undefined,
                        maxSize: o.maxSize || undefined,
                        minLetterSize: o.minLetterSize || undefined,
                        maxLetterSize: o.maxLetterSize || undefined,
                      }))
                  : undefined,
            }
          : { enabled: false },

        // Cleaning Config - must match backend expected structure EXACTLY
        // Backend reads: combinedVariantColumn, combinedVariantDelimiter, useCustomPrefixes,
        //                stylePrefixRules, findText, replaceText, removePatterns,
        //                removeFirstN, removeLastN, stockTextMappings
        cleaningConfig: {
          // Stock text mappings (e.g., "Yes" -> 1)
          stockTextMappings:
            stockTextMappings.length > 0 ? stockTextMappings : undefined,

          // Style prefix rules - backend checks useCustomPrefixes && stylePrefixRules.length > 0
          useCustomPrefixes:
            customPrefixEnabled &&
            stylePrefixRules.filter((r) => r.pattern && r.prefix).length > 0,
          stylePrefixRules:
            customPrefixEnabled &&
            stylePrefixRules.filter((r) => r.pattern && r.prefix).length > 0
              ? stylePrefixRules.filter((r) => r.pattern && r.prefix)
              : undefined,

          // Find/Replace - send all rules as array
          findReplaceRules:
            findReplaceRules.filter((r) => r.find).length > 0
              ? findReplaceRules.filter((r) => r.find)
              : undefined,

          // Remove patterns (regex) - backend supports array
          removePatterns:
            removePatterns.filter(Boolean).length > 0
              ? removePatterns.filter(Boolean)
              : undefined,

          // Remove by character position - backend expects removeFirstN/removeLastN
          removeFirstN:
            removeCharsByPosition.find((r) => r.start === 0)?.end || undefined,
          removeLastN:
            removeCharsByPosition.find((r) => r.end === -1)?.start || undefined,

          // Combined code parsing - backend expects these EXACT property names
          combinedVariantColumn: combinedCodeEnabled
            ? combinedCodeColumn
            : undefined,
          combinedVariantDelimiter: combinedCodeEnabled
            ? combinedCodeDelimiter
            : undefined,
          combinedVariantOrder: combinedCodeEnabled
            ? combinedCodeOrder
            : undefined,
        },

        // Zero Price Handling (convert to backend format)
        // Backend uses regularPriceConfig.skipZeroPrice boolean
        regularPriceConfig: {
          useFilePrice: true,
          priceMultiplier: 1,
          skipZeroPrice:
            zeroPriceAction === "skip" || zeroPriceAction === "use_shopify",
        },

        // Value Replacement Rules (per-field replacements)
        valueReplacementRules:
          valueReplacements.filter((r) => r.field && r.from).length > 0
            ? valueReplacements.filter((r) => r.field && r.from)
            : undefined,
      };

      if (sourceType === "email") {
        updatePayload.emailSettings = {
          host: emailHost,
          port: emailPort,
          secure: emailSecure,
          username: emailUsername,
          password: emailPassword,
          folder: emailFolder,
          senderWhitelist: emailSenderWhitelist
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
          subjectFilter: emailSubjectFilter,
          markAsRead: emailMarkAsRead,
          deleteAfterDownload: emailDeleteAfterDownload,
          extractLinksFromBody: emailExtractLinksFromBody,
          multiFileMode: emailMultiFileMode,
          expectedFiles: emailExpectedFiles,
        };
        // Bridge email multi-file mode to data source ingestion mode
        // This ensures processEmailAttachment stages files instead of importing directly
        updatePayload.ingestionMode = emailMultiFileMode ? "multi" : "single";
        // Email retry queue settings (at data source level)
        updatePayload.retryIfNoEmail = retryIfNoEmail;
        updatePayload.retryIntervalMinutes = retryIntervalMinutes;
        updatePayload.retryCutoffHour = retryCutoffHour;
      }

      await apiRequest(
        "PATCH",
        `/api/data-sources/${dataSourceId}`,
        updatePayload,
      );
      toast({ title: "Configuration saved" });
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
    } catch (error: any) {
      toast({
        title: "Save failed",
        description: error.message,
        variant: "destructive",
      });
    }
  };

  // Execute import
  const executeImport = async () => {
    const filesToImport =
      manualMultiFileMode && stagedManualFiles.length > 0
        ? stagedManualFiles
        : selectedFile
          ? [selectedFile]
          : [];

    if (filesToImport.length === 0) return;

    const dataSourceId = createdDataSourceId || existingDataSource?.id;
    if (!dataSourceId) return;
    setIsImporting(true);
    setValidationReport(null); // Reset validation report
    try {
      // Save config first
      await saveConfiguration();

      // Build current config from dialog state
      const config = buildConfig();

      // Send file(s) AND config to ensure new settings are used (not old DB values)
      const formData = new FormData();
      filesToImport.forEach((file, index) => {
        formData.append(index === 0 ? "file" : `file${index}`, file);
      });
      formData.append("dataSourceId", dataSourceId); // BUG FIX: Backend expects this in body
      formData.append("config", JSON.stringify(config));
      if (manualMultiFileMode && filesToImport.length > 1) {
        formData.append("multiFileMode", "true");
        formData.append("fileCount", String(filesToImport.length));
      }

      const response = await fetch(`/api/ai-import/execute`, {
        method: "POST",
        body: formData,
      });
      if (!response.ok) {
        // Try to get JSON error, but fall back if response is HTML
        let errorMessage = "Import failed";
        try {
          const contentType = response.headers.get("content-type");
          if (contentType && contentType.includes("application/json")) {
            const error = await response.json();
            if (error.safetyBlock) {
              toast({
                title: "⚠️ SAFETY NET: Blocked",
                description: error.message,
                variant: "destructive",
              });
              return;
            }
            errorMessage =
              error.error || error.details || error.message || "Import failed";
          } else {
            errorMessage = `Server error (${response.status})`;
          }
        } catch {
          errorMessage = `Server error (${response.status})`;
        }
        throw new Error(errorMessage);
      }
      const result = await response.json();
      setImportResult(result);

      // Capture validation report if present
      if (result.validation) {
        setValidationReport(result.validation);
        if (!result.validation.passed) {
          toast({
            title: "Import complete with validation issues",
            description: `${result.validation.failedChecks} validation check(s) failed`,
            variant: "destructive",
          });
        } else {
          toast({
            title: "Import successful",
            description: `${result.itemCount || result.stats?.finalCount || 0} items - Validation passed (${result.validation.accuracy}%)`,
          });
        }
      } else {
        toast({
          title: "Import successful",
          description: `${result.itemCount || result.stats?.finalCount || 0} items`,
        });
      }

      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
      queryClient.invalidateQueries({ queryKey: ["/api/inventory"] });
      onSuccess?.();
    } catch (error: any) {
      toast({
        title: "Import failed",
        description: error.message,
        variant: "destructive",
      });
    } finally {
      setIsImporting(false);
    }
  };

  // Validate existing DB data (without file upload)
  const validateDbData = async () => {
    const dataSourceId = createdDataSourceId || existingDataSource?.id;
    if (!dataSourceId) {
      toast({
        title: "No data source",
        description: "Please save the data source first",
        variant: "destructive",
      });
      return;
    }

    setIsValidatingDb(true);
    setDbValidationResult(null);

    try {
      // Build validation config from current dialog state
      // Use postValidationEnabled as master switch, and check if individual rules have values
      const hasChecksumRules =
        checksumVerifyItemCount ||
        checksumVerifyTotalStock ||
        checksumVerifyStyleCount ||
        checksumVerifyColorCount;
      const hasDistributionRules =
        distMinPercentWithStock !== undefined ||
        distMinPercentWithPrice !== undefined ||
        distMaxPercentWithStock !== undefined ||
        distMinPercentWithShipDate !== undefined;
      const hasCountRules =
        countMinItems !== undefined ||
        countMaxItems !== undefined ||
        countMinStyles !== undefined ||
        countMaxStyles !== undefined;

      const validationConfig = {
        checksumRules: {
          enabled: postValidationEnabled && hasChecksumRules,
          verifyItemCount: checksumVerifyItemCount,
          verifyTotalStock: checksumVerifyTotalStock,
          verifyStyleCount: checksumVerifyStyleCount,
          verifyColorCount: checksumVerifyColorCount,
          tolerancePercent: checksumTolerancePercent,
        },
        distributionRules: {
          enabled: postValidationEnabled && hasDistributionRules,
          minPercentWithStock: distMinPercentWithStock,
          maxPercentWithStock: distMaxPercentWithStock,
          minPercentWithPrice: distMinPercentWithPrice,
          minPercentWithShipDate: distMinPercentWithShipDate,
        },
        deltaRules: {
          enabled: postValidationEnabled && deltaEnabled,
          maxItemCountChange: deltaMaxItemCountDrop,
          maxTotalStockChange: deltaMaxStockDrop,
          maxStyleCountChange: deltaMaxStyleDrop,
        },
        countRules: {
          enabled: postValidationEnabled && hasCountRules,
          minItems: countMinItems,
          maxItems: countMaxItems,
          minStyles: countMinStyles,
          maxStyles: countMaxStyles,
        },
        spotChecks: spotChecks.filter((s) => s.style),
      };

      console.log(
        "[Validation] Sending config:",
        JSON.stringify(validationConfig, null, 2),
      );

      const response = await fetch(
        `/api/ai-import/validate-db/${dataSourceId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ validationConfig }),
        },
      );

      if (!response.ok) {
        const error = await response
          .json()
          .catch(() => ({ error: "Validation failed" }));
        throw new Error(error.error || "Validation failed");
      }

      const result = await response.json();
      setDbValidationResult(result);
      setDbItemCount(result.itemCount || 0);

      if (result.passed) {
        toast({
          title: "✓ Validation Passed",
          description: `${result.itemCount} items validated successfully`,
        });
      } else {
        toast({
          title: "⚠ Validation Issues",
          description: `${result.failedChecks} checks failed`,
          variant: "destructive",
        });
      }
    } catch (error: any) {
      toast({
        title: "Validation failed",
        description: error.message,
        variant: "destructive",
      });
    } finally {
      setIsValidatingDb(false);
    }
  };

  // Create data source
  const handleCreateDataSource = () => {
    if (!sourceName) {
      toast({ title: "Name required", variant: "destructive" });
      return;
    }
    const payload: any = {
      name: sourceName,
      type: sourceType,
      status: sourceActive ? "active" : "inactive",
      updateStrategy,
      autoUpdate,
      updateFrequency: autoUpdate ? updateFreq : null,
      updateTime: autoUpdate && updateFreq === "daily" ? updateTime : null,
      sourceType: isSaleFile ? "sales" : "inventory",
      assignedSaleDataSourceId: linkedSaleDataSourceId || null,
      shopifyStoreId: linkedShopifyStoreId || null,
      connectionDetails: sourceType === "url" ? { url: sourceUrl } : null,
      importValidationConfig: {
        enabled: validationEnabled,
        minRowCount,
        rowCountTolerance,
      },
      // Post-import validation config (Enhanced)
      validationConfig: {
        enabled: postValidationEnabled,
        // Checksum validation (mathematical proof)
        checksumRules: {
          verifyItemCount: checksumVerifyItemCount,
          verifyTotalStock: checksumVerifyTotalStock,
          verifyStyleCount: checksumVerifyStyleCount,
          verifyColorCount: checksumVerifyColorCount,
          tolerancePercent: checksumTolerancePercent,
        },
        // Distribution validation (data shape)
        distributionRules: {
          minPercentWithStock: distMinPercentWithStock,
          maxPercentWithStock: distMaxPercentWithStock,
          minPercentWithPrice: distMinPercentWithPrice,
          minPercentWithShipDate: distMinPercentWithShipDate,
        },
        // Historical comparison (delta from previous)
        deltaRules: {
          enabled: deltaEnabled,
          maxItemCountDropPercent: deltaMaxItemCountDrop,
          maxStockDropPercent: deltaMaxStockDrop,
          maxStyleDropPercent: deltaMaxStyleDrop,
        },
        // Count validation (expected ranges)
        countRules: {
          minItems: countMinItems,
          maxItems: countMaxItems,
          minStyles: countMinStyles,
          maxStyles: countMaxStyles,
          minFutureStockItems: countMinFutureStockItems,
          minDiscontinuedItems: countMinDiscontinuedItems,
        },
        // Rule checks
        ruleChecks: {
          verifyDiscontinuedDetection,
          verifyFutureDatesDetection,
          verifySizeExpansion,
          verifyStockTextMappings,
          verifyPriceExtraction,
        },
        // Spot checks
        spotChecks,
      },
      // Include column mapping from AI detection
      columnMapping: columnMapping,
      // Include cleaning config with EXACT backend property names
      cleaningConfig: {
        trimWhitespace: true,
        convertYesNo: false,
        stockTextMappings: stockTextMappings.filter(
          (m) => m.text && m.value !== undefined,
        ),
        // Style prefix rules (for custom vendor names)
        useCustomPrefixes:
          customPrefixEnabled &&
          stylePrefixRules.filter((r) => r.pattern && r.prefix).length > 0,
        stylePrefixRules: stylePrefixRules.filter((r) => r.pattern && r.prefix),
        // Style cleaning - send all find/replace rules as array
        findReplaceRules: findReplaceRules.filter((r) => r.find),
        // Remove patterns (regex array)
        removePatterns: removePatterns.filter(Boolean),
        // Remove by character position - backend expects removeFirstN/removeLastN
        removeFirstN:
          removeCharsByPosition.find((r) => r.start === 0)?.end || undefined,
        removeLastN:
          removeCharsByPosition.find((r) => r.end === -1)?.start || undefined,
        // Combined code parsing - backend expects these EXACT property names
        combinedVariantColumn: combinedCodeEnabled
          ? combinedCodeColumn
          : undefined,
        combinedVariantDelimiter: combinedCodeEnabled
          ? combinedCodeDelimiter
          : undefined,
        combinedVariantOrder: combinedCodeEnabled
          ? combinedCodeOrder
          : undefined,
      },
      // Include discontinued rules
      discontinuedRules: {
        enabled: discontinuedEnabled,
        keywords: discontinuedKeywords
          .split(",")
          .map((k) => k.trim())
          .filter(Boolean),
        skipDiscontinued: skipDiscontinued,
      },
      // Include future stock config
      futureStockConfig: {
        enabled: futureStockEnabled,
        dateOnlyMode: dateOnlyMode,
        useFutureDateAsShipDate: useFutureDateAsShipDate,
        preserveWithFutureStock: true,
        minFutureStock: 1,
      },
      // Include enhanced size limits
      sizeLimitConfig: sizeLimitEnabled
        ? {
            enabled: true,
            minSize: minSize || undefined,
            maxSize: maxSize || undefined,
            minLetterSize: minLetterSize || undefined,
            maxLetterSize: maxLetterSize || undefined,
            prefixOverrides:
              sizePrefixOverrides.filter((o) => o.pattern).length > 0
                ? sizePrefixOverrides
                    .filter((o) => o.pattern)
                    .map((o) => ({
                      pattern: o.pattern,
                      minSize: o.minSize || undefined,
                      maxSize: o.maxSize || undefined,
                      minLetterSize: o.minLetterSize || undefined,
                      maxLetterSize: o.maxLetterSize || undefined,
                    }))
                : undefined,
          }
        : { enabled: false },
      // Include price-based expansion config
      priceBasedExpansionConfig: priceExpansionEnabled
        ? {
            enabled: true,
            tiers: priceExpansionTiers,
            defaultExpandDown: 0,
            defaultExpandUp: 0,
          }
        : { enabled: false },
      // Include sale price config
      salePriceConfig: columnSaleEnabled
        ? {
            enabled: true,
            multiplier: saleMultiplier,
            useShopifyAsCompareAt: useShopifyCompareAt,
          }
        : { enabled: false },
      // Style cleaning rules
      styleCleaningConfig: {
        removeCharsByPosition: removeCharsByPosition.filter(
          (r) => r.start !== undefined,
        ),
        findReplaceRules: findReplaceRules.filter((r) => r.find),
        removePatterns: removePatterns.filter(Boolean),
      },
      // Combined variant code parsing
      combinedCodeConfig: combinedCodeEnabled
        ? {
            enabled: true,
            column: combinedCodeColumn,
            delimiter: combinedCodeDelimiter,
            order: combinedCodeOrder,
          }
        : { enabled: false },
      // Custom style prefixes
      customPrefixConfig: customPrefixEnabled
        ? {
            useCustomPrefixes: true,
            stylePrefixRules: stylePrefixRules.filter(
              (r) => r.pattern && r.prefix,
            ),
          }
        : { useCustomPrefixes: false },
      // Zero price handling
      zeroPriceConfig: {
        action: zeroPriceAction,
      },
      // Value replacement rules
      valueReplacementRules: valueReplacements.filter((r) => r.field && r.from),
    };
    if (sourceType === "email") {
      payload.emailSettings = {
        host: emailHost,
        port: emailPort,
        secure: emailSecure,
        username: emailUsername,
        password: emailPassword,
        folder: emailFolder,
        senderWhitelist: emailSenderWhitelist
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        subjectFilter: emailSubjectFilter,
        markAsRead: emailMarkAsRead,
        deleteAfterDownload: emailDeleteAfterDownload,
        extractLinksFromBody: emailExtractLinksFromBody,
        multiFileMode: emailMultiFileMode,
        expectedFiles: emailExpectedFiles,
      };
      // Email retry queue settings (at data source level)
      payload.retryIfNoEmail = retryIfNoEmail;
      payload.retryIntervalMinutes = retryIntervalMinutes;
      payload.retryCutoffHour = retryCutoffHour;
    }
    createDataSourceMutation.mutate(payload);
  };

  const handleClose = () => {
    resetForm();
    onClose();
  };
  const saleDataSources = dataSources.filter(
    (ds: DataSource) => ds.sourceType === "sales",
  );

  // Add pattern
  const addPattern = () => {
    setComplexStockPatterns([
      ...complexStockPatterns,
      {
        name: `pattern_${complexStockPatterns.length + 1}`,
        pattern: "",
        extractStock: "0",
        description: "",
      },
    ]);
  };

  // Add tier
  const addTier = () => {
    setPriceExpansionTiers([
      ...priceExpansionTiers,
      { minPrice: 0, expandDown: 1, expandUp: 1 },
    ]);
  };

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-[1200px] w-[95vw] max-h-[95vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-blue-600" />
            {existingDataSource
              ? `Edit: ${existingDataSource.name}`
              : "Add Data Source with AI"}
          </DialogTitle>
          <DialogDescription>
            Configure connection, rules, and use AI to detect file format.
          </DialogDescription>
        </DialogHeader>

        <Tabs
          value={activeTab}
          onValueChange={setActiveTab}
          className="w-full mt-4"
        >
          <TabsList className="w-full grid grid-cols-6">
            <TabsTrigger value="connection">
              <Settings className="h-4 w-4 mr-1" />
              Connection
            </TabsTrigger>
            <TabsTrigger value="schedule">
              <Clock className="h-4 w-4 mr-1" />
              Schedule
            </TabsTrigger>
            <TabsTrigger
              value="ai-detection"
              disabled={!createdDataSourceId && !existingDataSource}
            >
              <Sparkles className="h-4 w-4 mr-1" />
              AI Detection
            </TabsTrigger>
            <TabsTrigger
              value="validation"
              disabled={!createdDataSourceId && !existingDataSource}
            >
              <ClipboardCheck className="h-4 w-4 mr-1" />
              Validation
            </TabsTrigger>
            <TabsTrigger
              value="preview"
              disabled={!detectionResult && !existingDataSource}
            >
              <Eye className="h-4 w-4 mr-1" />
              Preview
            </TabsTrigger>
            <TabsTrigger
              value="import"
              disabled={!previewResult && !existingDataSource}
            >
              <Database className="h-4 w-4 mr-1" />
              Import
            </TabsTrigger>
          </TabsList>

          <div className="py-4 space-y-4 max-h-[calc(95vh-200px)] overflow-y-auto">
            {/* CONNECTION TAB */}
            <TabsContent value="connection" className="space-y-4 mt-0">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label>Source Name *</Label>
                  <Input
                    placeholder="e.g. Jovani Inventory"
                    value={sourceName}
                    onChange={(e) => setSourceName(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Source Type</Label>
                  <Select
                    value={sourceType}
                    onValueChange={(v: any) => setSourceType(v)}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="manual">Manual Upload</SelectItem>
                      <SelectItem value="url">URL Feed</SelectItem>
                      <SelectItem value="email">Email Attachment</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* Update Strategy Dropdown */}
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label>Update Strategy</Label>
                  <Select
                    value={updateStrategy}
                    onValueChange={(v: "replace" | "full_sync") =>
                      setUpdateStrategy(v)
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="full_sync">
                        Full Sync (delete all, insert new)
                      </SelectItem>
                      <SelectItem value="replace">
                        Create & Update (upsert by SKU)
                      </SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    Full Sync: Removes all existing items before importing.
                    Create & Update: Adds new items and updates existing ones by
                    SKU.
                  </p>
                </div>
              </div>

              {sourceType === "url" && (
                <div className="space-y-2 border rounded-lg p-4 bg-muted/30">
                  <Label>URL</Label>
                  <div className="flex gap-2">
                    <Input
                      placeholder="https://..."
                      value={sourceUrl}
                      onChange={(e) => setSourceUrl(e.target.value)}
                      className="flex-1"
                    />
                    <Button
                      variant="secondary"
                      onClick={runTestConnection}
                      disabled={testStatus === "testing"}
                    >
                      {testStatus === "success" ? (
                        <CheckCircle className="h-4 w-4 text-green-600" />
                      ) : (
                        <PlayCircle className="h-4 w-4" />
                      )}
                      Test
                    </Button>
                  </div>
                </div>
              )}

              {sourceType === "email" && (
                <div className="space-y-4 border rounded-lg p-4 bg-muted/30">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2 font-medium">
                      <Mail className="h-4 w-4" />
                      Email (IMAP)
                    </div>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={runTestConnection}
                      disabled={testStatus === "testing"}
                    >
                      {testStatus === "testing" ? (
                        <Loader2 className="h-4 w-4 animate-spin mr-1" />
                      ) : testStatus === "success" ? (
                        <CheckCircle className="h-4 w-4 text-green-600 mr-1" />
                      ) : testStatus === "error" ? (
                        <XCircle className="h-4 w-4 text-red-600 mr-1" />
                      ) : null}
                      Test
                    </Button>
                  </div>
                  <div className="grid gap-4 md:grid-cols-3">
                    <div className="space-y-2">
                      <Label>Host</Label>
                      <Input
                        value={emailHost}
                        onChange={(e) => setEmailHost(e.target.value)}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Port</Label>
                      <Input
                        type="number"
                        value={emailPort}
                        onChange={(e) =>
                          setEmailPort(parseInt(e.target.value) || 993)
                        }
                      />
                    </div>
                    <div className="flex items-center space-x-2 pt-6">
                      <Switch
                        checked={emailSecure}
                        onCheckedChange={setEmailSecure}
                      />
                      <Label>SSL</Label>
                    </div>
                  </div>
                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="space-y-2">
                      <Label>Username</Label>
                      <Input
                        value={emailUsername}
                        onChange={(e) => setEmailUsername(e.target.value)}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Password</Label>
                      <Input
                        type="password"
                        value={emailPassword}
                        onChange={(e) => setEmailPassword(e.target.value)}
                      />
                    </div>
                  </div>
                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="space-y-2">
                      <Label>Folder</Label>
                      <Input
                        value={emailFolder}
                        onChange={(e) => setEmailFolder(e.target.value)}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Allowed Senders</Label>
                      <Input
                        value={emailSenderWhitelist}
                        onChange={(e) =>
                          setEmailSenderWhitelist(e.target.value)
                        }
                      />
                    </div>
                  </div>
                  <div className="space-y-2">
                    <Label>Subject Filter</Label>
                    <Input
                      value={emailSubjectFilter}
                      onChange={(e) => setEmailSubjectFilter(e.target.value)}
                    />
                  </div>

                  <div className="grid gap-4 md:grid-cols-2 border-t pt-4 mt-4">
                    <div className="flex items-center space-x-2">
                      <Switch
                        checked={emailMarkAsRead}
                        onCheckedChange={setEmailMarkAsRead}
                      />
                      <Label>Mark as Read after import</Label>
                    </div>
                    <div className="flex items-center space-x-2">
                      <Switch
                        checked={emailDeleteAfterDownload}
                        onCheckedChange={setEmailDeleteAfterDownload}
                      />
                      <Label>Delete from inbox after import</Label>
                    </div>
                  </div>

                  <div className="border-t pt-4 mt-4">
                    <div className="flex items-center space-x-2 mb-2">
                      <Switch
                        checked={retryIfNoEmail}
                        onCheckedChange={setRetryIfNoEmail}
                      />
                      <Label className="flex items-center gap-2">
                        <RefreshCw className="h-4 w-4" />
                        Retry if no email found at scheduled time
                      </Label>
                    </div>
                    {retryIfNoEmail && (
                      <div className="pl-6 space-y-3">
                        <p className="text-xs text-muted-foreground">
                          If no email is found during the scheduled import, the
                          system will keep checking at the specified interval
                          until the cutoff time (PST).
                        </p>
                        <div className="grid gap-4 md:grid-cols-2">
                          <div className="space-y-1">
                            <Label className="text-xs">Check every</Label>
                            <select
                              value={retryIntervalMinutes}
                              onChange={(e) =>
                                setRetryIntervalMinutes(
                                  parseInt(e.target.value),
                                )
                              }
                              className="w-full h-9 px-3 rounded-md border border-input bg-background text-sm"
                            >
                              <option value={30}>30 minutes</option>
                              <option value={60}>1 hour</option>
                              <option value={120}>2 hours</option>
                            </select>
                          </div>
                          <div className="space-y-1">
                            <Label className="text-xs">
                              Stop checking at (PST)
                            </Label>
                            <select
                              value={retryCutoffHour}
                              onChange={(e) =>
                                setRetryCutoffHour(parseInt(e.target.value))
                              }
                              className="w-full h-9 px-3 rounded-md border border-input bg-background text-sm"
                            >
                              <option value={12}>12:00 PM</option>
                              <option value={14}>2:00 PM</option>
                              <option value={16}>4:00 PM</option>
                              <option value={18}>6:00 PM</option>
                              <option value={20}>8:00 PM</option>
                            </select>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>

                  <div className="border-t pt-4 mt-4">
                    <div className="flex items-center space-x-2 mb-3">
                      <Switch
                        checked={emailExtractLinksFromBody}
                        onCheckedChange={setEmailExtractLinksFromBody}
                      />
                      <Label className="flex items-center gap-2">
                        <Link className="h-4 w-4" />
                        Extract download links from email body
                      </Label>
                    </div>
                    {emailExtractLinksFromBody && (
                      <p className="text-xs text-muted-foreground pl-6 mb-3">
                        Scans the email body for download links (CSV/Excel
                        files) and downloads them automatically. Works with
                        NetSuite and similar systems that send file links
                        instead of attachments.
                      </p>
                    )}
                    <div className="flex items-center space-x-2">
                      <Switch
                        checked={emailMultiFileMode}
                        onCheckedChange={setEmailMultiFileMode}
                      />
                      <Label className="flex items-center gap-2">
                        <Files className="h-4 w-4" />
                        Multi-File Mode
                      </Label>
                    </div>
                    {emailMultiFileMode && (
                      <div className="mt-2 pl-6">
                        <Label>Expected Files: </Label>
                        <Input
                          type="number"
                          min={2}
                          max={10}
                          value={emailExpectedFiles}
                          onChange={(e) =>
                            setEmailExpectedFiles(parseInt(e.target.value) || 2)
                          }
                          className="w-20 inline ml-2"
                        />
                      </div>
                    )}
                  </div>

                  {/* Fetch Email Now - Testing Section */}
                  <div className="border-t pt-4 mt-4">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center gap-2 font-medium text-sm">
                        <Mail className="h-4 w-4" />
                        Test Email Import
                      </div>
                    </div>

                    <div className="flex items-center space-x-2 mb-3">
                      <Switch
                        checked={clearHashBeforeFetch}
                        onCheckedChange={setClearHashBeforeFetch}
                      />
                      <Label className="text-sm">
                        Clear hash before fetch (allows re-importing same
                        emails)
                      </Label>
                    </div>

                    <div className="flex items-center gap-2">
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={fetchEmailNow}
                        disabled={
                          emailFetchStatus === "fetching" ||
                          (!createdDataSourceId && !existingDataSource?.id)
                        }
                      >
                        {emailFetchStatus === "fetching" ? (
                          <Loader2 className="h-4 w-4 animate-spin mr-1" />
                        ) : emailFetchStatus === "success" ? (
                          <CheckCircle className="h-4 w-4 text-green-600 mr-1" />
                        ) : emailFetchStatus === "error" ? (
                          <XCircle className="h-4 w-4 text-red-600 mr-1" />
                        ) : (
                          <PlayCircle className="h-4 w-4 mr-1" />
                        )}
                        Fetch Email Now
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={clearEmailHash}
                        disabled={
                          !createdDataSourceId && !existingDataSource?.id
                        }
                      >
                        <Trash2 className="h-4 w-4 mr-1" />
                        Clear Hash
                      </Button>
                    </div>

                    {emailFetchResult && (
                      <div
                        className={`mt-3 p-3 rounded-md text-sm ${
                          emailFetchResult.success
                            ? "bg-green-50 border border-green-200 text-green-800"
                            : "bg-red-50 border border-red-200 text-red-800"
                        }`}
                      >
                        {emailFetchResult.success ? (
                          <div>
                            <div className="font-medium flex items-center gap-1">
                              <CheckCircle className="h-3 w-3" />
                              {emailFetchResult.filesProcessed} file(s)
                              processed
                            </div>
                            {emailFetchResult.logs?.map(
                              (log: any, i: number) => (
                                <div key={i} className="text-xs mt-1">
                                  {log.fileName} - {log.status}
                                  {log.error && ` (${log.error})`}
                                </div>
                              ),
                            )}
                          </div>
                        ) : (
                          <div className="flex items-center gap-1">
                            <AlertCircle className="h-3 w-3" />
                            {emailFetchResult.error || "Fetch failed"}
                          </div>
                        )}
                      </div>
                    )}

                    {!createdDataSourceId && !existingDataSource?.id && (
                      <p className="text-xs text-muted-foreground mt-2">
                        Save the data source first to test email fetching
                      </p>
                    )}
                  </div>
                </div>
              )}

              <div className="grid gap-4 md:grid-cols-2">
                <div className="flex items-center space-x-2 border p-3 rounded-md">
                  <Switch
                    checked={isSaleFile}
                    onCheckedChange={setIsSaleFile}
                  />
                  <Label>This is a Sale File</Label>
                </div>
                <div className="flex items-center space-x-2 border p-3 rounded-md">
                  <Switch
                    checked={sourceActive}
                    onCheckedChange={setSourceActive}
                  />
                  <Label>Active</Label>
                </div>
              </div>

              {!isSaleFile && saleDataSources.length > 0 && (
                <div className="space-y-2">
                  <Label>Link to Sale File (filter sale styles)</Label>
                  <Select
                    value={linkedSaleDataSourceId || "__none__"}
                    onValueChange={(v) =>
                      setLinkedSaleDataSourceId(v === "__none__" ? "" : v)
                    }
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">None</SelectItem>
                      {saleDataSources.map((ds: DataSource) => (
                        <SelectItem key={ds.id} value={ds.id}>
                          {ds.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              )}

              {shopifyStores.length > 0 && (
                <div className="space-y-2">
                  <Label>Shopify Store (for price lookup)</Label>
                  <Select
                    value={linkedShopifyStoreId || "__none__"}
                    onValueChange={(v) =>
                      setLinkedShopifyStoreId(v === "__none__" ? "" : v)
                    }
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">None</SelectItem>
                      {shopifyStores.map((store: any) => (
                        <SelectItem key={store.id} value={store.id}>
                          {store.name || store.shopName}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              )}

              <Accordion
                type="single"
                collapsible
                className="border rounded-lg"
              >
                <AccordionItem value="safety" className="border-0">
                  <AccordionTrigger className="px-4">
                    <ShieldCheck className="h-4 w-4 mr-2 text-green-600" />
                    Safety Nets
                  </AccordionTrigger>
                  <AccordionContent className="px-4 pb-4">
                    <div className="flex items-center space-x-2 mb-4">
                      <Switch
                        checked={validationEnabled}
                        onCheckedChange={setValidationEnabled}
                      />
                      <Label>Enable Validation</Label>
                    </div>
                    {validationEnabled && (
                      <div className="grid gap-4 md:grid-cols-2">
                        <div className="space-y-2">
                          <Label>Min Row Count</Label>
                          <Input
                            type="number"
                            value={minRowCount || ""}
                            onChange={(e) =>
                              setMinRowCount(
                                e.target.value
                                  ? parseInt(e.target.value)
                                  : undefined,
                              )
                            }
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Row Drop Tolerance %</Label>
                          <Input
                            type="number"
                            value={rowCountTolerance}
                            onChange={(e) =>
                              setRowCountTolerance(
                                parseInt(e.target.value) || 50,
                              )
                            }
                          />
                        </div>
                      </div>
                    )}
                  </AccordionContent>
                </AccordionItem>
              </Accordion>

              <div className="flex justify-end gap-2 pt-4 border-t">
                <Button variant="outline" onClick={handleClose}>
                  Cancel
                </Button>
                {!createdDataSourceId && !existingDataSource ? (
                  <Button
                    onClick={handleCreateDataSource}
                    disabled={createDataSourceMutation.isPending || !sourceName}
                  >
                    <Plus className="mr-2 h-4 w-4" />
                    Create & Continue
                  </Button>
                ) : (
                  <Button onClick={() => setActiveTab("schedule")}>
                    Continue →
                  </Button>
                )}
              </div>
            </TabsContent>

            {/* SCHEDULE TAB */}
            <TabsContent value="schedule" className="space-y-4 mt-0">
              <div className="flex items-center justify-between border-b pb-4">
                <div>
                  <Label className="text-base">Automatic Updates</Label>
                </div>
                <Switch checked={autoUpdate} onCheckedChange={setAutoUpdate} />
              </div>
              {autoUpdate && (
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label>Frequency</Label>
                    <Select
                      value={updateFreq}
                      onValueChange={(v: any) => setUpdateFreq(v)}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="hourly">Hourly</SelectItem>
                        <SelectItem value="daily">Daily</SelectItem>
                        <SelectItem value="weekly">Weekly</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {updateFreq === "daily" && (
                    <div className="space-y-2">
                      <Label>Time (PST)</Label>
                      <Input
                        type="time"
                        value={updateTime}
                        onChange={(e) => setUpdateTime(e.target.value)}
                      />
                    </div>
                  )}
                </div>
              )}
              <div className="flex justify-between pt-4 border-t">
                <Button
                  variant="outline"
                  onClick={() => setActiveTab("connection")}
                >
                  ← Back
                </Button>
                <Button onClick={() => setActiveTab("ai-detection")}>
                  Continue to AI →
                </Button>
              </div>
            </TabsContent>

            {/* AI DETECTION TAB */}
            <TabsContent value="ai-detection" className="space-y-4 mt-0">
              {/* Multi-File Mode Toggle */}
              <div className="flex items-center justify-between border rounded-lg p-3">
                <div className="flex items-center space-x-2">
                  <Switch
                    checked={manualMultiFileMode}
                    onCheckedChange={(checked) => {
                      setManualMultiFileMode(checked);
                      if (!checked) {
                        setStagedManualFiles([]);
                      }
                    }}
                  />
                  <Label className="flex items-center gap-2">
                    <Files className="h-4 w-4" />
                    Multi-File Mode
                  </Label>
                </div>
                {manualMultiFileMode && stagedManualFiles.length > 0 && (
                  <Badge variant="secondary">
                    {stagedManualFiles.length} files staged
                  </Badge>
                )}
              </div>

              <div className="border-2 border-dashed rounded-lg p-6 text-center">
                <input
                  type="file"
                  accept=".xlsx,.xls,.csv"
                  onChange={handleFileSelect}
                  className="hidden"
                  id="ai-file-upload"
                />
                <label
                  htmlFor="ai-file-upload"
                  className="cursor-pointer flex flex-col items-center gap-2"
                >
                  <Upload className="h-10 w-10 text-muted-foreground" />
                  <span className="text-lg font-medium">
                    {manualMultiFileMode
                      ? "Click to add file"
                      : selectedFile
                        ? selectedFile.name
                        : "Click to upload"}
                  </span>
                  {manualMultiFileMode && (
                    <span className="text-sm text-muted-foreground">
                      Add multiple files, then analyze together
                    </span>
                  )}
                </label>
              </div>

              {/* Staged Files List (Multi-File Mode) */}
              {manualMultiFileMode && stagedManualFiles.length > 0 && (
                <div className="border rounded-lg p-4 space-y-2">
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium text-sm">Staged Files</span>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        setStagedManualFiles([]);
                        setSelectedFile(null);
                      }}
                      className="text-red-600 hover:text-red-700 hover:bg-red-50"
                    >
                      Clear All
                    </Button>
                  </div>
                  {stagedManualFiles.map((file, idx) => (
                    <div
                      key={idx}
                      className="flex items-center justify-between bg-muted/30 p-2 rounded"
                    >
                      <div className="flex items-center gap-2">
                        <FileSpreadsheet className="h-4 w-4 text-green-600" />
                        <span className="text-sm">{file.name}</span>
                      </div>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => {
                          const newFiles = stagedManualFiles.filter(
                            (_, i) => i !== idx,
                          );
                          setStagedManualFiles(newFiles);
                          if (newFiles.length > 0) {
                            setSelectedFile(newFiles[newFiles.length - 1]);
                          } else {
                            setSelectedFile(null);
                          }
                        }}
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                  ))}
                  <Button
                    onClick={analyzeFile}
                    disabled={isAnalyzing}
                    className="w-full mt-3 bg-blue-600 hover:bg-blue-700"
                  >
                    {isAnalyzing ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <Sparkles className="mr-2 h-4 w-4" />
                    )}
                    Analyze {stagedManualFiles.length} Files with AI
                  </Button>
                </div>
              )}

              {/* Single File Display (Non Multi-File Mode) */}
              {!manualMultiFileMode && selectedFile && (
                <div className="flex items-center justify-between bg-muted/30 p-3 rounded-lg">
                  <div className="flex items-center gap-2">
                    <FileSpreadsheet className="h-5 w-5 text-green-600" />
                    <span>{selectedFile.name}</span>
                  </div>
                  <Button
                    onClick={analyzeFile}
                    disabled={isAnalyzing}
                    className="bg-blue-600 hover:bg-blue-700"
                  >
                    {isAnalyzing ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <Sparkles className="mr-2 h-4 w-4" />
                    )}
                    Analyze with AI
                  </Button>
                </div>
              )}

              {(detectionResult || existingDataSource) && (
                <div className="space-y-4">
                  {/* Detection Summary - only show when file analyzed */}
                  {detectionResult && (
                    <div className="grid grid-cols-4 gap-4">
                      <div className="p-4 border rounded-lg text-center">
                        <div className="text-lg font-bold">
                          {detectionResult.formatType}
                        </div>
                        <div className="text-xs text-muted-foreground">
                          Format
                        </div>
                      </div>
                      <div className="p-4 border rounded-lg text-center">
                        <div className="text-lg font-bold">
                          {detectionResult.columns.length}
                        </div>
                        <div className="text-xs text-muted-foreground">
                          Columns
                        </div>
                      </div>
                      <div className="p-4 border rounded-lg text-center">
                        <div className="text-lg font-bold">
                          {Math.round(detectionResult.formatConfidence * 100)}%
                        </div>
                        <div className="text-xs text-muted-foreground">
                          Confidence
                        </div>
                      </div>
                      <div className="p-4 border rounded-lg text-center">
                        <div className="flex flex-wrap gap-1 justify-center">
                          {detectionResult.detectedPatterns
                            .hasSalePriceColumn && (
                            <Badge className="text-xs">Sale Price</Badge>
                          )}
                          {detectionResult.detectedPatterns
                            .hasComplexStockCells && (
                            <Badge className="text-xs">Complex Stock</Badge>
                          )}
                          {detectionResult.detectedPatterns.hasPriceColumn && (
                            <Badge variant="outline" className="text-xs">
                              Price
                            </Badge>
                          )}
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Editing notice when no file uploaded */}
                  {!detectionResult && existingDataSource && (
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                      <div className="flex items-center gap-2 text-blue-700">
                        <Info className="h-4 w-4" />
                        <span className="font-medium">
                          Editing saved settings
                        </span>
                      </div>
                      <p className="text-sm text-blue-600 mt-1">
                        Upload a file above to change column mappings. Other
                        settings can be edited directly.
                      </p>
                    </div>
                  )}

                  {/* Column Mapping - show dropdowns if detectionResult, otherwise show saved values */}
                  <div className="border rounded-lg p-4">
                    <div className="flex items-center gap-2 mb-4">
                      <Columns className="h-4 w-4" />
                      <span className="font-semibold">Column Mappings</span>
                    </div>
                    {detectionResult ? (
                      <div className="grid grid-cols-4 gap-3">
                        {[
                          "style",
                          "color",
                          "size",
                          "stock",
                          "price",
                          "salePrice",
                          "shipDate",
                          "cost",
                          "discontinued",
                        ].map((field) => (
                          <div
                            key={field}
                            className="p-2 border rounded bg-muted/30"
                          >
                            <div className="text-xs text-muted-foreground mb-1 capitalize">
                              {field === "discontinued"
                                ? "Status/Discontinued"
                                : field}
                            </div>
                            <Select
                              value={columnMapping[field] || "__unmapped__"}
                              onValueChange={(v) =>
                                setColumnMapping((prev) => ({
                                  ...prev,
                                  [field]: v === "__unmapped__" ? "" : v,
                                }))
                              }
                            >
                              <SelectTrigger className="h-8 text-sm">
                                <SelectValue placeholder="Select" />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="__unmapped__">
                                  Not mapped
                                </SelectItem>
                                {(
                                  detectionResult.allHeaders ||
                                  detectionResult.columns.map((c) => ({
                                    header: c.headerName,
                                    index: c.columnIndex,
                                  }))
                                )
                                  .filter(
                                    (col) => col.header && col.header.trim(),
                                  )
                                  .map((col) => (
                                    <SelectItem
                                      key={col.header}
                                      value={col.header}
                                    >
                                      {col.header}
                                    </SelectItem>
                                  ))}
                              </SelectContent>
                            </Select>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="grid grid-cols-4 gap-3">
                        {[
                          "style",
                          "color",
                          "size",
                          "stock",
                          "price",
                          "salePrice",
                          "shipDate",
                          "cost",
                          "discontinued",
                        ].map((field) => (
                          <div
                            key={field}
                            className="p-2 border rounded bg-muted/30"
                          >
                            <div className="text-xs text-muted-foreground mb-1 capitalize">
                              {field === "discontinued"
                                ? "Status/Discontinued"
                                : field}
                            </div>
                            <Input
                              value={columnMapping[field] || ""}
                              onChange={(e) =>
                                setColumnMapping((prev) => ({
                                  ...prev,
                                  [field]: e.target.value,
                                }))
                              }
                              placeholder="Column name"
                              className="h-8 text-sm"
                            />
                          </div>
                        ))}
                      </div>
                    )}
                  </div>

                  {/* COLUMN-BASED SALE DETECTION */}
                  <Accordion type="multiple" className="space-y-2">
                    <AccordionItem value="sale" className="border rounded-lg">
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <DollarSign className="h-4 w-4 text-green-600" />
                          <span>Column-Based Sale Detection</span>
                          {columnSaleEnabled && (
                            <Badge className="text-xs">Enabled</Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <div className="flex items-center space-x-2">
                            <Switch
                              checked={columnSaleEnabled}
                              onCheckedChange={setColumnSaleEnabled}
                            />
                            <Label>
                              Enable (if sale price column has value → apply
                              multiplier)
                            </Label>
                          </div>
                          {columnSaleEnabled && (
                            <>
                              <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-2">
                                  <Label>Sale Price Column</Label>
                                  {detectionResult ? (
                                    <Select
                                      value={salePriceColumn}
                                      onValueChange={setSalePriceColumn}
                                    >
                                      <SelectTrigger>
                                        <SelectValue placeholder="Select" />
                                      </SelectTrigger>
                                      <SelectContent>
                                        {(
                                          detectionResult.allHeaders ||
                                          detectionResult.columns.map((c) => ({
                                            header: c.headerName,
                                            index: c.columnIndex,
                                          }))
                                        )
                                          .filter(
                                            (col) =>
                                              col.header && col.header.trim(),
                                          )
                                          .map((col) => (
                                            <SelectItem
                                              key={col.header}
                                              value={col.header}
                                            >
                                              {col.header}
                                            </SelectItem>
                                          ))}
                                      </SelectContent>
                                    </Select>
                                  ) : (
                                    <Input
                                      value={salePriceColumn}
                                      onChange={(e) =>
                                        setSalePriceColumn(e.target.value)
                                      }
                                      placeholder="Column name"
                                    />
                                  )}
                                </div>
                                <div className="space-y-2">
                                  <Label>Regular Price Column (optional)</Label>
                                  {detectionResult ? (
                                    <Select
                                      value={regularPriceColumn || "__none__"}
                                      onValueChange={(v) =>
                                        setRegularPriceColumn(
                                          v === "__none__" ? "" : v,
                                        )
                                      }
                                    >
                                      <SelectTrigger>
                                        <SelectValue placeholder="Select" />
                                      </SelectTrigger>
                                      <SelectContent>
                                        <SelectItem value="__none__">
                                          None
                                        </SelectItem>
                                        {(
                                          detectionResult.allHeaders ||
                                          detectionResult.columns.map((c) => ({
                                            header: c.headerName,
                                            index: c.columnIndex,
                                          }))
                                        )
                                          .filter(
                                            (col) =>
                                              col.header && col.header.trim(),
                                          )
                                          .map((col) => (
                                            <SelectItem
                                              key={col.header}
                                              value={col.header}
                                            >
                                              {col.header}
                                            </SelectItem>
                                          ))}
                                      </SelectContent>
                                    </Select>
                                  ) : (
                                    <Input
                                      value={regularPriceColumn || ""}
                                      onChange={(e) =>
                                        setRegularPriceColumn(e.target.value)
                                      }
                                      placeholder="Column name (optional)"
                                    />
                                  )}
                                </div>
                              </div>
                              <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-2">
                                  <Label>Multiplier</Label>
                                  <Input
                                    type="number"
                                    step="0.1"
                                    value={saleMultiplier}
                                    onChange={(e) =>
                                      setSaleMultiplier(
                                        parseFloat(e.target.value) || 2,
                                      )
                                    }
                                  />
                                </div>
                                <div className="pt-6">
                                  <div className="flex items-center space-x-2">
                                    <Switch
                                      checked={useShopifyCompareAt}
                                      onCheckedChange={setUseShopifyCompareAt}
                                    />
                                    <Label>
                                      Use Shopify price as Compare-At
                                    </Label>
                                  </div>
                                </div>
                              </div>
                              <div className="bg-green-50 p-3 rounded text-sm">
                                <strong>How it works:</strong> If sale price
                                column has value → Sale Price × {saleMultiplier}{" "}
                                = new Shopify price
                                {useShopifyCompareAt && (
                                  <>
                                    , Shopify's current price = compare-at
                                    (strike-through)
                                  </>
                                )}
                              </div>
                            </>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* PRICE-BASED SIZE EXPANSION */}
                    <AccordionItem
                      value="expansion"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Ruler className="h-4 w-4 text-purple-600" />
                          <span>Price-Based Size Expansion</span>
                          {priceExpansionEnabled && (
                            <Badge className="text-xs">Enabled</Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <div className="flex items-center space-x-2">
                            <Switch
                              checked={priceExpansionEnabled}
                              onCheckedChange={setPriceExpansionEnabled}
                            />
                            <Label>Enable price-based size expansion</Label>
                          </div>
                          {priceExpansionEnabled && (
                            <>
                              <div className="space-y-2">
                                {priceExpansionTiers.map((tier, idx) => (
                                  <div
                                    key={idx}
                                    className="flex items-center gap-2 p-2 border rounded"
                                  >
                                    <span className="text-sm">If price</span>
                                    <Input
                                      type="number"
                                      value={tier.minPrice}
                                      onChange={(e) => {
                                        const newTiers = [
                                          ...priceExpansionTiers,
                                        ];
                                        newTiers[idx].minPrice =
                                          parseFloat(e.target.value) || 0;
                                        setPriceExpansionTiers(newTiers);
                                      }}
                                      className="w-24"
                                    />
                                    {tier.maxPrice !== undefined && (
                                      <>
                                        <span>to</span>
                                        <Input
                                          type="number"
                                          value={tier.maxPrice}
                                          onChange={(e) => {
                                            const newTiers = [
                                              ...priceExpansionTiers,
                                            ];
                                            newTiers[idx].maxPrice =
                                              parseFloat(e.target.value) || 0;
                                            setPriceExpansionTiers(newTiers);
                                          }}
                                          className="w-24"
                                        />
                                      </>
                                    )}
                                    {tier.maxPrice === undefined && (
                                      <span>+</span>
                                    )}
                                    <span>→</span>
                                    <Input
                                      type="number"
                                      value={tier.expandDown}
                                      onChange={(e) => {
                                        const newTiers = [
                                          ...priceExpansionTiers,
                                        ];
                                        newTiers[idx].expandDown =
                                          parseInt(e.target.value) || 0;
                                        setPriceExpansionTiers(newTiers);
                                      }}
                                      className="w-16"
                                    />
                                    <span>down,</span>
                                    <Input
                                      type="number"
                                      value={tier.expandUp}
                                      onChange={(e) => {
                                        const newTiers = [
                                          ...priceExpansionTiers,
                                        ];
                                        newTiers[idx].expandUp =
                                          parseInt(e.target.value) || 0;
                                        setPriceExpansionTiers(newTiers);
                                      }}
                                      className="w-16"
                                    />
                                    <span>up</span>
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() =>
                                        setPriceExpansionTiers(
                                          priceExpansionTiers.filter(
                                            (_, i) => i !== idx,
                                          ),
                                        )
                                      }
                                    >
                                      <Trash2 className="h-4 w-4 text-red-500" />
                                    </Button>
                                  </div>
                                ))}
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={addTier}
                                >
                                  <Plus className="mr-2 h-4 w-4" />
                                  Add Tier
                                </Button>
                              </div>
                              <div className="bg-purple-50 p-3 rounded text-sm">
                                <strong>Example:</strong> Product at $600 with
                                size 8 → Creates sizes 4, 6, 8, 10 (4 down, 1
                                up)
                              </div>
                            </>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* STOCK INFO MESSAGES */}
                    <AccordionItem
                      value="stockinfo"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <MessageSquare className="h-4 w-4 text-green-600" />
                          <span>Stock Info Messages</span>
                          {stockInfoEnabled && (
                            <Badge className="text-xs">Enabled</Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <div className="flex items-center space-x-2">
                            <Switch
                              checked={stockInfoEnabled}
                              onCheckedChange={setStockInfoEnabled}
                            />
                            <Label>Enable stock info metafield messages</Label>
                          </div>
                          {stockInfoEnabled && (
                            <>
                              <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-2">
                                  <Label>In Stock Message</Label>
                                  <Input
                                    value={inStockMessage}
                                    onChange={(e) =>
                                      setInStockMessage(e.target.value)
                                    }
                                    placeholder="In Stock"
                                  />
                                  <p className="text-xs text-muted-foreground">
                                    Shown when stock {">"} 0
                                  </p>
                                </div>
                                <div className="space-y-2">
                                  <Label>Out of Stock Message</Label>
                                  <Input
                                    value={outOfStockMessage}
                                    onChange={(e) =>
                                      setOutOfStockMessage(e.target.value)
                                    }
                                    placeholder="Special Order"
                                  />
                                  <p className="text-xs text-muted-foreground">
                                    Shown when stock = 0
                                  </p>
                                </div>
                              </div>
                              <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-2">
                                  <Label>Size Expansion Message</Label>
                                  <Input
                                    value={sizeExpansionMessage}
                                    onChange={(e) =>
                                      setSizeExpansionMessage(e.target.value)
                                    }
                                    placeholder="Available in Extra Sizes"
                                  />
                                  <p className="text-xs text-muted-foreground">
                                    For expanded sizes (requires size expansion)
                                  </p>
                                </div>
                                <div className="space-y-2">
                                  <Label>Future Date Message</Label>
                                  <Input
                                    value={futureDateMessage}
                                    onChange={(e) =>
                                      setFutureDateMessage(e.target.value)
                                    }
                                    placeholder="Ships {date}"
                                  />
                                  <p className="text-xs text-muted-foreground">
                                    Use {"{date}"} for formatted date
                                  </p>
                                </div>
                              </div>
                              <div className="w-1/2">
                                <Label>Date Offset (days)</Label>
                                <Input
                                  type="number"
                                  value={dateOffsetDays}
                                  onChange={(e) =>
                                    setDateOffsetDays(
                                      parseInt(e.target.value) || 0,
                                    )
                                  }
                                  className="w-24"
                                />
                                <p className="text-xs text-muted-foreground mt-1">
                                  Add days to ship date display
                                </p>
                              </div>
                              <div className="bg-green-50 p-3 rounded text-sm space-y-1">
                                <strong>Message Priority:</strong>
                                <ol className="list-decimal ml-4">
                                  <li>
                                    Expanded sizes → "{sizeExpansionMessage}"
                                  </li>
                                  <li>
                                    Has future date → "{futureDateMessage}"
                                  </li>
                                  <li>In stock → "{inStockMessage}"</li>
                                  <li>Out of stock → "{outOfStockMessage}"</li>
                                </ol>
                              </div>
                            </>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* COMPLEX STOCK PATTERNS */}
                    <AccordionItem
                      value="complex"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Zap className="h-4 w-4 text-orange-600" />
                          <span>Complex Stock Cell Parsing</span>
                          {complexStockEnabled && (
                            <Badge className="text-xs">
                              {complexStockPatterns.length} patterns
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <div className="flex items-center space-x-2">
                            <Switch
                              checked={complexStockEnabled}
                              onCheckedChange={setComplexStockEnabled}
                            />
                            <Label>Enable complex stock parsing</Label>
                          </div>

                          {detectionResult?.detectedPatterns
                            ?.complexStockExamples &&
                            detectionResult.detectedPatterns
                              .complexStockExamples.length > 0 && (
                              <div className="bg-orange-50 p-3 rounded">
                                <div className="font-medium text-sm mb-2">
                                  Detected Examples:
                                </div>
                                {detectionResult.detectedPatterns.complexStockExamples
                                  .slice(0, 3)
                                  .map((ex, i) => (
                                    <div
                                      key={i}
                                      className="text-xs font-mono bg-white p-1 rounded mb-1"
                                    >
                                      "{ex}"
                                    </div>
                                  ))}
                              </div>
                            )}

                          {complexStockEnabled && (
                            <>
                              <div className="space-y-2">
                                {complexStockPatterns.map((pattern, idx) => (
                                  <div
                                    key={idx}
                                    className="p-3 border rounded space-y-2"
                                  >
                                    <div className="flex items-center justify-between">
                                      <Input
                                        value={pattern.name}
                                        onChange={(e) => {
                                          const newPatterns = [
                                            ...complexStockPatterns,
                                          ];
                                          newPatterns[idx].name =
                                            e.target.value;
                                          setComplexStockPatterns(newPatterns);
                                        }}
                                        placeholder="Pattern name"
                                        className="w-40"
                                      />
                                      <Button
                                        variant="ghost"
                                        size="sm"
                                        onClick={() =>
                                          setComplexStockPatterns(
                                            complexStockPatterns.filter(
                                              (_, i) => i !== idx,
                                            ),
                                          )
                                        }
                                      >
                                        <Trash2 className="h-4 w-4 text-red-500" />
                                      </Button>
                                    </div>
                                    <div className="space-y-1">
                                      <Label className="text-xs">
                                        Regex Pattern
                                      </Label>
                                      <Input
                                        value={pattern.pattern}
                                        onChange={(e) => {
                                          const newPatterns = [
                                            ...complexStockPatterns,
                                          ];
                                          newPatterns[idx].pattern =
                                            e.target.value;
                                          setComplexStockPatterns(newPatterns);
                                        }}
                                        placeholder="e.g. sold out.*more coming.*no"
                                        className="font-mono text-sm"
                                      />
                                    </div>
                                    <div className="grid grid-cols-2 gap-2">
                                      <div className="space-y-1">
                                        <Label className="text-xs">
                                          Extract Stock
                                        </Label>
                                        <Input
                                          value={pattern.extractStock || ""}
                                          onChange={(e) => {
                                            const newPatterns = [
                                              ...complexStockPatterns,
                                            ];
                                            newPatterns[idx].extractStock =
                                              e.target.value;
                                            setComplexStockPatterns(
                                              newPatterns,
                                            );
                                          }}
                                          placeholder="0 or $1"
                                          className="text-sm"
                                        />
                                      </div>
                                      <div className="space-y-1">
                                        <Label className="text-xs">
                                          Extract Date
                                        </Label>
                                        <Input
                                          value={pattern.extractDate || ""}
                                          onChange={(e) => {
                                            const newPatterns = [
                                              ...complexStockPatterns,
                                            ];
                                            newPatterns[idx].extractDate =
                                              e.target.value;
                                            setComplexStockPatterns(
                                              newPatterns,
                                            );
                                          }}
                                          placeholder="$1"
                                          className="text-sm"
                                        />
                                      </div>
                                    </div>
                                    <div className="flex gap-4">
                                      <div className="flex items-center space-x-2">
                                        <Switch
                                          checked={
                                            pattern.markDiscontinued || false
                                          }
                                          onCheckedChange={(v) => {
                                            const newPatterns = [
                                              ...complexStockPatterns,
                                            ];
                                            newPatterns[idx].markDiscontinued =
                                              v;
                                            setComplexStockPatterns(
                                              newPatterns,
                                            );
                                          }}
                                        />
                                        <Label className="text-xs">
                                          Mark Discontinued
                                        </Label>
                                      </div>
                                      <div className="flex items-center space-x-2">
                                        <Switch
                                          checked={
                                            pattern.markSpecialOrder || false
                                          }
                                          onCheckedChange={(v) => {
                                            const newPatterns = [
                                              ...complexStockPatterns,
                                            ];
                                            newPatterns[idx].markSpecialOrder =
                                              v;
                                            setComplexStockPatterns(
                                              newPatterns,
                                            );
                                          }}
                                        />
                                        <Label className="text-xs">
                                          Special Order
                                        </Label>
                                      </div>
                                    </div>
                                    <Input
                                      value={pattern.description}
                                      onChange={(e) => {
                                        const newPatterns = [
                                          ...complexStockPatterns,
                                        ];
                                        newPatterns[idx].description =
                                          e.target.value;
                                        setComplexStockPatterns(newPatterns);
                                      }}
                                      placeholder="Description"
                                      className="text-sm"
                                    />
                                  </div>
                                ))}
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={addPattern}
                                >
                                  <Plus className="mr-2 h-4 w-4" />
                                  Add Pattern
                                </Button>
                              </div>

                              {/* Pattern Tester */}
                              <div className="border-t pt-4">
                                <div className="flex items-center gap-2 mb-2">
                                  <TestTube className="h-4 w-4" />
                                  <Label>Test Patterns</Label>
                                </div>
                                <div className="flex gap-2">
                                  <Input
                                    value={patternTestValue}
                                    onChange={(e) =>
                                      setPatternTestValue(e.target.value)
                                    }
                                    placeholder="Enter test value..."
                                    className="flex-1"
                                  />
                                  <Button
                                    variant="secondary"
                                    onClick={testPatterns}
                                  >
                                    Test
                                  </Button>
                                </div>
                                {patternTestResults.length > 0 && (
                                  <div className="mt-2 p-2 bg-muted rounded text-sm">
                                    {patternTestResults.map((r, i) => (
                                      <div key={i}>
                                        {r.matched ? (
                                          <span className="text-green-600">
                                            ✓ Matched: {r.patternName} → Stock:{" "}
                                            {r.extractedStock}, Date:{" "}
                                            {r.extractedDate || "N/A"}
                                          </span>
                                        ) : (
                                          <span className="text-red-600">
                                            ✗ No match
                                          </span>
                                        )}
                                      </div>
                                    ))}
                                  </div>
                                )}
                              </div>
                            </>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* SIMPLE STOCK MAPPINGS */}
                    <AccordionItem
                      value="stockmap"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Package className="h-4 w-4" />
                          <span>Simple Stock Text Mappings</span>
                          {stockTextMappings.length > 0 && (
                            <Badge variant="outline" className="text-xs">
                              {stockTextMappings.length}
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-2">
                          <p className="text-sm text-muted-foreground">
                            Map text values like "YES" → 1, "NO" → 0
                          </p>
                          {stockTextMappings.map((m, idx) => (
                            <div key={idx} className="flex items-center gap-2">
                              <Input
                                value={m.text}
                                onChange={(e) => {
                                  const newMappings = [...stockTextMappings];
                                  newMappings[idx].text = e.target.value;
                                  setStockTextMappings(newMappings);
                                }}
                                className="w-32"
                                placeholder="Text"
                              />
                              <span>→</span>
                              <Input
                                type="number"
                                value={m.value}
                                onChange={(e) => {
                                  const newMappings = [...stockTextMappings];
                                  newMappings[idx].value =
                                    parseInt(e.target.value) || 0;
                                  setStockTextMappings(newMappings);
                                }}
                                className="w-20"
                              />
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() =>
                                  setStockTextMappings(
                                    stockTextMappings.filter(
                                      (_, i) => i !== idx,
                                    ),
                                  )
                                }
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </div>
                          ))}
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() =>
                              setStockTextMappings([
                                ...stockTextMappings,
                                { text: "", value: 0 },
                              ])
                            }
                          >
                            <Plus className="mr-2 h-4 w-4" />
                            Add Mapping
                          </Button>

                          {/* Filter Zero Stock Toggle */}
                          <div className="pt-4 mt-4 border-t">
                            <div className="flex items-center justify-between">
                              <div>
                                <Label className="font-medium">
                                  Filter Zero Stock Items
                                </Label>
                                <p className="text-xs text-muted-foreground">
                                  Remove items with 0 stock during import
                                </p>
                              </div>
                              <Switch
                                checked={filterZeroStock}
                                onCheckedChange={setFilterZeroStock}
                              />
                            </div>
                          </div>
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Discontinued Rules */}
                    <AccordionItem
                      value="discontinued"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Ban className="h-4 w-4 text-red-500" />
                          <span>Discontinued Detection</span>
                          {discontinuedEnabled && (
                            <Badge className="bg-red-100 text-red-700">
                              Enabled
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <div className="flex items-center justify-between">
                            <Label>Enable Discontinued Detection</Label>
                            <Switch
                              checked={discontinuedEnabled}
                              onCheckedChange={setDiscontinuedEnabled}
                            />
                          </div>
                          {discontinuedEnabled && (
                            <>
                              <div className="space-y-2">
                                <Label>Keywords (comma separated)</Label>
                                <Input
                                  value={discontinuedKeywords}
                                  onChange={(e) =>
                                    setDiscontinuedKeywords(e.target.value)
                                  }
                                  placeholder="Discontinued, Out of Stock, EOL"
                                />
                                <p className="text-xs text-muted-foreground">
                                  Items with these keywords will be marked
                                  discontinued
                                </p>
                              </div>
                              <div className="flex items-center justify-between">
                                <Label>Skip Discontinued Items</Label>
                                <Switch
                                  checked={skipDiscontinued}
                                  onCheckedChange={setSkipDiscontinued}
                                />
                              </div>
                            </>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Future Stock Config */}
                    <AccordionItem
                      value="futurestock"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Calendar className="h-4 w-4 text-blue-500" />
                          <span>Future Stock / Ship Dates</span>
                          {futureStockEnabled && (
                            <Badge className="bg-blue-100 text-blue-700">
                              Enabled
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <div className="flex items-center justify-between">
                            <Label>Enable Future Stock Handling</Label>
                            <Switch
                              checked={futureStockEnabled}
                              onCheckedChange={setFutureStockEnabled}
                            />
                          </div>
                          {futureStockEnabled && (
                            <>
                              <div className="flex items-center justify-between">
                                <div>
                                  <Label>Date Only Mode</Label>
                                  <p className="text-xs text-muted-foreground">
                                    Process dates even without future stock qty
                                  </p>
                                </div>
                                <Switch
                                  checked={dateOnlyMode}
                                  onCheckedChange={setDateOnlyMode}
                                />
                              </div>
                              <div className="flex items-center justify-between">
                                <div>
                                  <Label>Use Future Date as Ship Date</Label>
                                  <p className="text-xs text-muted-foreground">
                                    Sets ship date for items with 0 stock
                                  </p>
                                </div>
                                <Switch
                                  checked={useFutureDateAsShipDate}
                                  onCheckedChange={setUseFutureDateAsShipDate}
                                />
                              </div>
                            </>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Size Limits (Enhanced) */}
                    <AccordionItem
                      value="sizelimits"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Ruler className="h-4 w-4 text-purple-500" />
                          <span>Size Limits</span>
                          {sizeLimitEnabled && (
                            <Badge className="bg-purple-100 text-purple-700">
                              {minSize || minLetterSize
                                ? `${minSize || minLetterSize}`
                                : "No min"}{" "}
                              -{" "}
                              {maxSize || maxLetterSize
                                ? `${maxSize || maxLetterSize}`
                                : "No max"}
                              {sizePrefixOverrides.length > 0 &&
                                ` +${sizePrefixOverrides.length} override${sizePrefixOverrides.length > 1 ? "s" : ""}`}
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <p className="text-sm text-muted-foreground mb-4">
                          Restrict which sizes are imported and expanded for
                          this data source.
                        </p>

                        <div className="space-y-6">
                          {/* Enable Toggle */}
                          <div className="flex items-center justify-between p-3 bg-muted/30 rounded-lg">
                            <div>
                              <Label className="font-medium">
                                Enable size limits
                              </Label>
                              <p className="text-xs text-muted-foreground">
                                Filter out sizes outside the allowed range
                                during import and expansion.
                              </p>
                            </div>
                            <Switch
                              checked={sizeLimitEnabled}
                              onCheckedChange={setSizeLimitEnabled}
                            />
                          </div>

                          {sizeLimitEnabled && (
                            <>
                              {/* Numeric Size Range */}
                              <div className="border-l-4 border-blue-400 pl-4 space-y-3">
                                <Label className="font-medium">
                                  Numeric Size Range (0, 2, 4, 6...)
                                </Label>
                                <div className="grid grid-cols-2 gap-4">
                                  <div className="space-y-1">
                                    <Label className="text-xs text-muted-foreground">
                                      Minimum Size
                                    </Label>
                                    <Select
                                      value={minSize || "none"}
                                      onValueChange={(v) =>
                                        setMinSize(v === "none" ? null : v)
                                      }
                                    >
                                      <SelectTrigger>
                                        <SelectValue placeholder="No minimum" />
                                      </SelectTrigger>
                                      <SelectContent className="max-h-48 overflow-y-auto">
                                        <SelectItem value="none">
                                          No minimum
                                        </SelectItem>
                                        {numericSizeOptions.map((s) => (
                                          <SelectItem key={s} value={s}>
                                            {s}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  </div>
                                  <div className="space-y-1">
                                    <Label className="text-xs text-muted-foreground">
                                      Maximum Size
                                    </Label>
                                    <Select
                                      value={maxSize || "none"}
                                      onValueChange={(v) =>
                                        setMaxSize(v === "none" ? null : v)
                                      }
                                    >
                                      <SelectTrigger>
                                        <SelectValue placeholder="No maximum" />
                                      </SelectTrigger>
                                      <SelectContent className="max-h-48 overflow-y-auto">
                                        <SelectItem value="none">
                                          No maximum
                                        </SelectItem>
                                        {numericSizeOptions.map((s) => (
                                          <SelectItem key={s} value={s}>
                                            {s}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  </div>
                                </div>
                              </div>

                              {/* Letter Size Range */}
                              <div className="border-l-4 border-green-400 pl-4 space-y-3">
                                <Label className="font-medium">
                                  Letter Size Range (S, M, L, XL...)
                                </Label>
                                <div className="grid grid-cols-2 gap-4">
                                  <div className="space-y-1">
                                    <Label className="text-xs text-muted-foreground">
                                      Minimum Size
                                    </Label>
                                    <Select
                                      value={minLetterSize || "none"}
                                      onValueChange={(v) =>
                                        setMinLetterSize(
                                          v === "none" ? null : v,
                                        )
                                      }
                                    >
                                      <SelectTrigger>
                                        <SelectValue placeholder="No minimum" />
                                      </SelectTrigger>
                                      <SelectContent className="max-h-48 overflow-y-auto">
                                        <SelectItem value="none">
                                          No minimum
                                        </SelectItem>
                                        {letterSizeOptions.map((s) => (
                                          <SelectItem key={s} value={s}>
                                            {s}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  </div>
                                  <div className="space-y-1">
                                    <Label className="text-xs text-muted-foreground">
                                      Maximum Size
                                    </Label>
                                    <Select
                                      value={maxLetterSize || "none"}
                                      onValueChange={(v) =>
                                        setMaxLetterSize(
                                          v === "none" ? null : v,
                                        )
                                      }
                                    >
                                      <SelectTrigger>
                                        <SelectValue placeholder="No maximum" />
                                      </SelectTrigger>
                                      <SelectContent className="max-h-48 overflow-y-auto">
                                        <SelectItem value="none">
                                          No maximum
                                        </SelectItem>
                                        {letterSizeOptions.map((s) => (
                                          <SelectItem key={s} value={s}>
                                            {s}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                  </div>
                                </div>
                              </div>

                              {/* Style Prefix Overrides */}
                              <div className="border-l-4 border-orange-400 pl-4 space-y-3">
                                <div className="flex items-center justify-between">
                                  <div>
                                    <Label className="font-medium">
                                      Style Prefix Overrides
                                    </Label>
                                    <p className="text-xs text-muted-foreground">
                                      Override size limits for styles matching
                                      specific patterns (e.g., "JVN" for Jovani
                                      JVN styles, "^\d" for styles starting with
                                      numbers).
                                    </p>
                                  </div>
                                  <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() =>
                                      setSizePrefixOverrides([
                                        ...sizePrefixOverrides,
                                        {
                                          pattern: "",
                                          minSize: null,
                                          maxSize: null,
                                          minLetterSize: null,
                                          maxLetterSize: null,
                                        },
                                      ])
                                    }
                                  >
                                    <Plus className="mr-2 h-4 w-4" />
                                    Add Override
                                  </Button>
                                </div>

                                {sizePrefixOverrides.map((override, idx) => (
                                  <div
                                    key={idx}
                                    className="flex items-center gap-2 p-3 bg-muted/30 rounded-lg"
                                  >
                                    <Input
                                      value={override.pattern}
                                      onChange={(e) => {
                                        const updated = [
                                          ...sizePrefixOverrides,
                                        ];
                                        updated[idx].pattern = e.target.value;
                                        setSizePrefixOverrides(updated);
                                      }}
                                      placeholder="Pattern (e.g., JVN)"
                                      className="w-32"
                                    />
                                    <Select
                                      value={override.minSize || "none"}
                                      onValueChange={(v) => {
                                        const updated = [
                                          ...sizePrefixOverrides,
                                        ];
                                        updated[idx].minSize =
                                          v === "none" ? null : v;
                                        setSizePrefixOverrides(updated);
                                      }}
                                    >
                                      <SelectTrigger className="w-24">
                                        <SelectValue placeholder="Min" />
                                      </SelectTrigger>
                                      <SelectContent className="max-h-48 overflow-y-auto">
                                        <SelectItem value="none">
                                          None
                                        </SelectItem>
                                        {numericSizeOptions.map((s) => (
                                          <SelectItem key={s} value={s}>
                                            {s}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                    <span className="text-muted-foreground">
                                      to
                                    </span>
                                    <Select
                                      value={override.maxSize || "none"}
                                      onValueChange={(v) => {
                                        const updated = [
                                          ...sizePrefixOverrides,
                                        ];
                                        updated[idx].maxSize =
                                          v === "none" ? null : v;
                                        setSizePrefixOverrides(updated);
                                      }}
                                    >
                                      <SelectTrigger className="w-24">
                                        <SelectValue placeholder="Max" />
                                      </SelectTrigger>
                                      <SelectContent className="max-h-48 overflow-y-auto">
                                        <SelectItem value="none">
                                          None
                                        </SelectItem>
                                        {numericSizeOptions.map((s) => (
                                          <SelectItem key={s} value={s}>
                                            {s}
                                          </SelectItem>
                                        ))}
                                      </SelectContent>
                                    </Select>
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() =>
                                        setSizePrefixOverrides(
                                          sizePrefixOverrides.filter(
                                            (_, i) => i !== idx,
                                          ),
                                        )
                                      }
                                    >
                                      <Trash2 className="h-4 w-4 text-red-500" />
                                    </Button>
                                  </div>
                                ))}
                              </div>

                              {/* Info Box */}
                              <div className="bg-teal-50 border border-teal-200 rounded-lg p-4 text-sm text-teal-800">
                                <strong>How it works:</strong> Sizes outside the
                                allowed range are filtered out during import.
                                Size expansion also respects these limits - new
                                sizes won't be added beyond the configured
                                range.
                              </div>
                            </>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Style Cleaning - Find & Replace */}
                    <AccordionItem
                      value="stylecleaning"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <RefreshCw className="h-4 w-4 text-orange-500" />
                          <span>Style Find & Replace</span>
                          {findReplaceRules.length > 0 && (
                            <Badge variant="outline">
                              {findReplaceRules.length} rules
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-3">
                          <p className="text-sm text-muted-foreground">
                            Find and replace text in style numbers
                          </p>
                          {findReplaceRules.map((rule, idx) => (
                            <div key={idx} className="flex items-center gap-2">
                              <Input
                                value={rule.find}
                                onChange={(e) => {
                                  const newRules = [...findReplaceRules];
                                  newRules[idx].find = e.target.value;
                                  setFindReplaceRules(newRules);
                                }}
                                className="flex-1"
                                placeholder="Find (e.g., 22FWRFSH-)"
                              />
                              <span>→</span>
                              <Input
                                value={rule.replace}
                                onChange={(e) => {
                                  const newRules = [...findReplaceRules];
                                  newRules[idx].replace = e.target.value;
                                  setFindReplaceRules(newRules);
                                }}
                                className="flex-1"
                                placeholder="Replace (leave empty to remove)"
                              />
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() =>
                                  setFindReplaceRules(
                                    findReplaceRules.filter(
                                      (_, i) => i !== idx,
                                    ),
                                  )
                                }
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </div>
                          ))}
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() =>
                              setFindReplaceRules([
                                ...findReplaceRules,
                                { find: "", replace: "" },
                              ])
                            }
                          >
                            <Plus className="mr-2 h-4 w-4" />
                            Add Rule
                          </Button>
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Remove Patterns from Style */}
                    <AccordionItem
                      value="removepatterns"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Trash2 className="h-4 w-4 text-red-500" />
                          <span>Remove Patterns from Style</span>
                          {removePatterns.length > 0 && (
                            <Badge variant="outline">
                              {removePatterns.length}
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-3">
                          <p className="text-sm text-muted-foreground">
                            Remove specific text patterns or prefixes from style
                            numbers
                          </p>
                          {removePatterns.map((pattern, idx) => (
                            <div key={idx} className="flex items-center gap-2">
                              <Input
                                value={pattern}
                                onChange={(e) => {
                                  const newPatterns = [...removePatterns];
                                  newPatterns[idx] = e.target.value;
                                  setRemovePatterns(newPatterns);
                                }}
                                className="flex-1"
                                placeholder="Pattern to remove (e.g., 22FWRFSH-)"
                              />
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() =>
                                  setRemovePatterns(
                                    removePatterns.filter((_, i) => i !== idx),
                                  )
                                }
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </div>
                          ))}
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() =>
                              setRemovePatterns([...removePatterns, ""])
                            }
                          >
                            <Plus className="mr-2 h-4 w-4" />
                            Add Pattern
                          </Button>
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Combined Variant Code Parsing */}
                    <AccordionItem
                      value="combinedcode"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Columns className="h-4 w-4 text-indigo-500" />
                          <span>Combined Variant Code Parsing</span>
                          {combinedCodeEnabled && (
                            <Badge className="bg-indigo-100 text-indigo-700">
                              Enabled
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <div className="flex items-center justify-between">
                            <div>
                              <Label>Enable Combined Code Parsing</Label>
                              <p className="text-xs text-muted-foreground">
                                Parse STYLE-COLOR-SIZE from a single column
                              </p>
                            </div>
                            <Switch
                              checked={combinedCodeEnabled}
                              onCheckedChange={setCombinedCodeEnabled}
                            />
                          </div>
                          {combinedCodeEnabled && (
                            <>
                              <div className="space-y-2">
                                <Label>Source Column</Label>
                                {detectionResult ? (
                                  <Select
                                    value={combinedCodeColumn}
                                    onValueChange={setCombinedCodeColumn}
                                  >
                                    <SelectTrigger>
                                      <SelectValue placeholder="Select column" />
                                    </SelectTrigger>
                                    <SelectContent>
                                      {(
                                        detectionResult.allHeaders ||
                                        detectionResult.columns.map((c) => ({
                                          header: c.headerName,
                                          index: c.columnIndex,
                                        }))
                                      )
                                        .filter((col) => col.header?.trim())
                                        .map((col) => (
                                          <SelectItem
                                            key={col.header}
                                            value={col.header}
                                          >
                                            {col.header}
                                          </SelectItem>
                                        ))}
                                    </SelectContent>
                                  </Select>
                                ) : (
                                  <Input
                                    value={combinedCodeColumn}
                                    onChange={(e) =>
                                      setCombinedCodeColumn(e.target.value)
                                    }
                                    placeholder="Column name"
                                  />
                                )}
                              </div>
                              <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-2">
                                  <Label>Delimiter</Label>
                                  <Input
                                    value={combinedCodeDelimiter}
                                    onChange={(e) =>
                                      setCombinedCodeDelimiter(e.target.value)
                                    }
                                    placeholder="-"
                                  />
                                </div>
                                <div className="space-y-2">
                                  <Label>Order</Label>
                                  <Select
                                    value={combinedCodeOrder}
                                    onValueChange={setCombinedCodeOrder}
                                  >
                                    <SelectTrigger>
                                      <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                      <SelectItem value="style-color-size">
                                        Style-Color-Size
                                      </SelectItem>
                                      <SelectItem value="style-size-color">
                                        Style-Size-Color
                                      </SelectItem>
                                      <SelectItem value="color-style-size">
                                        Color-Style-Size
                                      </SelectItem>
                                    </SelectContent>
                                  </Select>
                                </div>
                              </div>
                              <p className="text-xs text-muted-foreground">
                                Example: "ARMANI-BLK-0" → Style: ARMANI, Color:
                                BLK, Size: 0
                              </p>
                            </>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Custom Style Prefixes */}
                    <AccordionItem
                      value="customprefixes"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Settings className="h-4 w-4 text-gray-500" />
                          <span>Custom Style Prefixes</span>
                          {customPrefixEnabled && (
                            <Badge variant="outline">
                              {stylePrefixRules.length} rules
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <div className="flex items-center justify-between">
                            <div>
                              <Label>Enable Custom Prefixes</Label>
                              <p className="text-xs text-muted-foreground">
                                Add custom name prefix based on style pattern
                              </p>
                            </div>
                            <Switch
                              checked={customPrefixEnabled}
                              onCheckedChange={setCustomPrefixEnabled}
                            />
                          </div>
                          {customPrefixEnabled && (
                            <div className="space-y-3">
                              {stylePrefixRules.map((rule, idx) => (
                                <div
                                  key={idx}
                                  className="flex items-center gap-2"
                                >
                                  <Input
                                    value={rule.pattern}
                                    onChange={(e) => {
                                      const newRules = [...stylePrefixRules];
                                      newRules[idx].pattern = e.target.value;
                                      setStylePrefixRules(newRules);
                                    }}
                                    className="flex-1"
                                    placeholder="Pattern (e.g., ^JK)"
                                  />
                                  <span>→</span>
                                  <Input
                                    value={rule.prefix}
                                    onChange={(e) => {
                                      const newRules = [...stylePrefixRules];
                                      newRules[idx].prefix = e.target.value;
                                      setStylePrefixRules(newRules);
                                    }}
                                    className="flex-1"
                                    placeholder="Prefix (e.g., Johnathan Kayne)"
                                  />
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() =>
                                      setStylePrefixRules(
                                        stylePrefixRules.filter(
                                          (_, i) => i !== idx,
                                        ),
                                      )
                                    }
                                  >
                                    <X className="h-4 w-4" />
                                  </Button>
                                </div>
                              ))}
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() =>
                                  setStylePrefixRules([
                                    ...stylePrefixRules,
                                    { pattern: "", prefix: "" },
                                  ])
                                }
                              >
                                <Plus className="mr-2 h-4 w-4" />
                                Add Prefix Rule
                              </Button>
                              <p className="text-xs text-muted-foreground">
                                If no rule matches, data source name is used as
                                prefix
                              </p>
                            </div>
                          )}
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Zero Price Handling */}
                    <AccordionItem
                      value="zeroprice"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <DollarSign className="h-4 w-4 text-yellow-500" />
                          <span>Zero Price Handling</span>
                          <Badge variant="outline">
                            {zeroPriceAction === "keep"
                              ? "Keep"
                              : zeroPriceAction === "skip"
                                ? "Skip"
                                : "Use Shopify"}
                          </Badge>
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-4">
                          <p className="text-sm text-muted-foreground">
                            What to do when file price is $0
                          </p>
                          <Select
                            value={zeroPriceAction}
                            onValueChange={(v: any) => setZeroPriceAction(v)}
                          >
                            <SelectTrigger>
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="keep">
                                Keep $0 price
                              </SelectItem>
                              <SelectItem value="skip">
                                Skip items with $0 price
                              </SelectItem>
                              <SelectItem value="use_shopify">
                                Use Shopify price instead
                              </SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Value Replacement Rules */}
                    <AccordionItem
                      value="valuereplace"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <RefreshCw className="h-4 w-4 text-teal-500" />
                          <span>Value Replacement Rules</span>
                          {valueReplacements.length > 0 && (
                            <Badge variant="outline">
                              {valueReplacements.length}
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-3">
                          <p className="text-sm text-muted-foreground">
                            Replace specific values in any field
                          </p>
                          {valueReplacements.map((rule, idx) => (
                            <div key={idx} className="flex items-center gap-2">
                              <Select
                                value={rule.field}
                                onValueChange={(v) => {
                                  const newRules = [...valueReplacements];
                                  newRules[idx].field = v;
                                  setValueReplacements(newRules);
                                }}
                              >
                                <SelectTrigger className="w-28">
                                  <SelectValue placeholder="Field" />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="color">Color</SelectItem>
                                  <SelectItem value="size">Size</SelectItem>
                                  <SelectItem value="style">Style</SelectItem>
                                  <SelectItem value="stock">Stock</SelectItem>
                                </SelectContent>
                              </Select>
                              <Input
                                value={rule.from}
                                onChange={(e) => {
                                  const newRules = [...valueReplacements];
                                  newRules[idx].from = e.target.value;
                                  setValueReplacements(newRules);
                                }}
                                className="flex-1"
                                placeholder="From value"
                              />
                              <span>→</span>
                              <Input
                                value={rule.to}
                                onChange={(e) => {
                                  const newRules = [...valueReplacements];
                                  newRules[idx].to = e.target.value;
                                  setValueReplacements(newRules);
                                }}
                                className="flex-1"
                                placeholder="To value"
                              />
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() =>
                                  setValueReplacements(
                                    valueReplacements.filter(
                                      (_, i) => i !== idx,
                                    ),
                                  )
                                }
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </div>
                          ))}
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() =>
                              setValueReplacements([
                                ...valueReplacements,
                                { field: "color", from: "", to: "" },
                              ])
                            }
                          >
                            <Plus className="mr-2 h-4 w-4" />
                            Add Replacement
                          </Button>
                        </div>
                      </AccordionContent>
                    </AccordionItem>

                    {/* Remove Characters by Position */}
                    <AccordionItem
                      value="removeposition"
                      className="border rounded-lg"
                    >
                      <AccordionTrigger className="px-4">
                        <div className="flex items-center gap-2">
                          <Trash2 className="h-4 w-4 text-gray-500" />
                          <span>Remove Characters by Position</span>
                          {removeCharsByPosition.length > 0 && (
                            <Badge variant="outline">
                              {removeCharsByPosition.length}
                            </Badge>
                          )}
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-3">
                          <p className="text-sm text-muted-foreground">
                            Remove characters from style at specific positions
                          </p>
                          {removeCharsByPosition.map((rule, idx) => (
                            <div key={idx} className="flex items-center gap-2">
                              <div className="flex items-center gap-1">
                                <Label className="text-xs">Start:</Label>
                                <Input
                                  type="number"
                                  value={rule.start}
                                  onChange={(e) => {
                                    const newRules = [...removeCharsByPosition];
                                    newRules[idx].start =
                                      parseInt(e.target.value) || 0;
                                    setRemoveCharsByPosition(newRules);
                                  }}
                                  className="w-16"
                                />
                              </div>
                              <div className="flex items-center gap-1">
                                <Label className="text-xs">End:</Label>
                                <Input
                                  type="number"
                                  value={rule.end}
                                  onChange={(e) => {
                                    const newRules = [...removeCharsByPosition];
                                    newRules[idx].end =
                                      parseInt(e.target.value) || 0;
                                    setRemoveCharsByPosition(newRules);
                                  }}
                                  className="w-16"
                                />
                              </div>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() =>
                                  setRemoveCharsByPosition(
                                    removeCharsByPosition.filter(
                                      (_, i) => i !== idx,
                                    ),
                                  )
                                }
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </div>
                          ))}
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() =>
                              setRemoveCharsByPosition([
                                ...removeCharsByPosition,
                                { start: 0, end: 0 },
                              ])
                            }
                          >
                            <Plus className="mr-2 h-4 w-4" />
                            Add Position Rule
                          </Button>
                          <p className="text-xs text-muted-foreground">
                            Example: Start: 0, End: 5 removes first 5 characters
                          </p>
                        </div>
                      </AccordionContent>
                    </AccordionItem>
                  </Accordion>
                </div>
              )}

              <div className="flex justify-between pt-4 border-t">
                <Button
                  variant="outline"
                  onClick={() => setActiveTab("schedule")}
                >
                  ← Back
                </Button>
                <div className="flex gap-2">
                  {(detectionResult || existingDataSource) && (
                    <>
                      <Button variant="outline" onClick={saveConfiguration}>
                        <Save className="mr-2 h-4 w-4" />
                        Save Config
                      </Button>
                      {detectionResult && (
                        <Button
                          onClick={runPreview}
                          disabled={isLoadingPreview}
                        >
                          {isLoadingPreview ? (
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                          ) : (
                            <Eye className="mr-2 h-4 w-4" />
                          )}
                          Preview
                        </Button>
                      )}
                    </>
                  )}
                </div>
              </div>
            </TabsContent>

            {/* VALIDATION RULES TAB */}
            <TabsContent value="validation" className="space-y-4 mt-0">
              <div className="flex items-center justify-between pb-2 border-b">
                <div>
                  <h3 className="text-lg font-semibold flex items-center gap-2">
                    <ClipboardCheck className="h-5 w-5 text-blue-600" />
                    Post-Import Validation Rules
                  </h3>
                  <p className="text-sm text-muted-foreground">
                    Configure checks that run after each import to verify data
                    accuracy
                  </p>
                </div>
                <div className="flex items-center gap-2">
                  <Label>Enabled</Label>
                  <Switch
                    checked={postValidationEnabled}
                    onCheckedChange={setPostValidationEnabled}
                  />
                </div>
              </div>

              {postValidationEnabled && (
                <div className="space-y-6">
                  {/* CHECKSUM VALIDATION (NEW - Most Important) */}
                  <div className="border-2 border-blue-200 rounded-lg p-4 bg-blue-50/30">
                    <h4 className="text-sm font-semibold mb-3 flex items-center gap-2">
                      <ShieldCheck className="h-4 w-4 text-blue-600" />
                      Checksum Validation (Mathematical Proof)
                    </h4>
                    <p className="text-xs text-muted-foreground mb-3">
                      Verify imported data exactly matches source file. This is
                      the most accurate validation.
                    </p>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-3">
                        <div className="flex items-center gap-2">
                          <Switch
                            id="checksumItemCount"
                            checked={checksumVerifyItemCount}
                            onCheckedChange={setChecksumVerifyItemCount}
                          />
                          <Label
                            htmlFor="checksumItemCount"
                            className="text-sm"
                          >
                            Verify item count matches source
                          </Label>
                        </div>
                        <div className="flex items-center gap-2">
                          <Switch
                            id="checksumTotalStock"
                            checked={checksumVerifyTotalStock}
                            onCheckedChange={setChecksumVerifyTotalStock}
                          />
                          <Label
                            htmlFor="checksumTotalStock"
                            className="text-sm"
                          >
                            Verify total stock matches source
                          </Label>
                        </div>
                      </div>
                      <div className="space-y-3">
                        <div className="flex items-center gap-2">
                          <Switch
                            id="checksumStyleCount"
                            checked={checksumVerifyStyleCount}
                            onCheckedChange={setChecksumVerifyStyleCount}
                          />
                          <Label
                            htmlFor="checksumStyleCount"
                            className="text-sm"
                          >
                            Verify unique style count
                          </Label>
                        </div>
                        <div className="flex items-center gap-2">
                          <Switch
                            id="checksumColorCount"
                            checked={checksumVerifyColorCount}
                            onCheckedChange={setChecksumVerifyColorCount}
                          />
                          <Label
                            htmlFor="checksumColorCount"
                            className="text-sm"
                          >
                            Verify unique color count
                          </Label>
                        </div>
                      </div>
                    </div>
                    <div className="mt-3 flex items-center gap-2">
                      <Label className="text-xs">Tolerance %:</Label>
                      <Input
                        type="number"
                        min="0"
                        max="100"
                        className="w-20 h-8"
                        value={checksumTolerancePercent}
                        onChange={(e) =>
                          setChecksumTolerancePercent(
                            parseInt(e.target.value) || 0,
                          )
                        }
                      />
                      <span className="text-xs text-muted-foreground">
                        (0 = exact match required)
                      </span>
                    </div>
                  </div>

                  {/* DISTRIBUTION VALIDATION (NEW) */}
                  <div className="border rounded-lg p-4">
                    <h4 className="text-sm font-semibold mb-3 flex items-center gap-2">
                      <FileCheck className="h-4 w-4 text-purple-600" />
                      Distribution Validation (Data Shape)
                    </h4>
                    <p className="text-xs text-muted-foreground mb-3">
                      Verify the distribution of values is within expected
                      ranges
                    </p>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-1">
                        <Label className="text-xs">
                          Min % with Stock &gt; 0
                        </Label>
                        <Input
                          type="number"
                          placeholder="e.g. 85"
                          min="0"
                          max="100"
                          value={distMinPercentWithStock ?? ""}
                          onChange={(e) =>
                            setDistMinPercentWithStock(
                              e.target.value
                                ? parseInt(e.target.value)
                                : undefined,
                            )
                          }
                        />
                      </div>
                      <div className="space-y-1">
                        <Label className="text-xs">
                          Max % with Stock &gt; 0
                        </Label>
                        <Input
                          type="number"
                          placeholder="e.g. 99"
                          min="0"
                          max="100"
                          value={distMaxPercentWithStock ?? ""}
                          onChange={(e) =>
                            setDistMaxPercentWithStock(
                              e.target.value
                                ? parseInt(e.target.value)
                                : undefined,
                            )
                          }
                        />
                      </div>
                      <div className="space-y-1">
                        <Label className="text-xs">Min % with Price</Label>
                        <Input
                          type="number"
                          placeholder="e.g. 90"
                          min="0"
                          max="100"
                          value={distMinPercentWithPrice ?? ""}
                          onChange={(e) =>
                            setDistMinPercentWithPrice(
                              e.target.value
                                ? parseInt(e.target.value)
                                : undefined,
                            )
                          }
                        />
                      </div>
                      <div className="space-y-1">
                        <Label className="text-xs">Min % with Ship Date</Label>
                        <Input
                          type="number"
                          placeholder="e.g. 30"
                          min="0"
                          max="100"
                          value={distMinPercentWithShipDate ?? ""}
                          onChange={(e) =>
                            setDistMinPercentWithShipDate(
                              e.target.value
                                ? parseInt(e.target.value)
                                : undefined,
                            )
                          }
                        />
                      </div>
                    </div>
                  </div>

                  {/* HISTORICAL COMPARISON (NEW) */}
                  <div className="border rounded-lg p-4">
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="text-sm font-semibold flex items-center gap-2">
                        <RefreshCw className="h-4 w-4 text-orange-600" />
                        Historical Comparison
                      </h4>
                      <div className="flex items-center gap-2">
                        <Label className="text-xs">Enabled</Label>
                        <Switch
                          checked={deltaEnabled}
                          onCheckedChange={setDeltaEnabled}
                        />
                      </div>
                    </div>
                    <p className="text-xs text-muted-foreground mb-3">
                      Compare against previous import to detect anomalies
                    </p>
                    {deltaEnabled && (
                      <div className="grid grid-cols-3 gap-3">
                        <div className="space-y-1">
                          <Label className="text-xs">
                            Max Item Count Drop %
                          </Label>
                          <Input
                            type="number"
                            placeholder="e.g. 10"
                            min="0"
                            max="100"
                            value={deltaMaxItemCountDrop ?? ""}
                            onChange={(e) =>
                              setDeltaMaxItemCountDrop(
                                e.target.value
                                  ? parseInt(e.target.value)
                                  : undefined,
                              )
                            }
                          />
                        </div>
                        <div className="space-y-1">
                          <Label className="text-xs">Max Stock Drop %</Label>
                          <Input
                            type="number"
                            placeholder="e.g. 20"
                            min="0"
                            max="100"
                            value={deltaMaxStockDrop ?? ""}
                            onChange={(e) =>
                              setDeltaMaxStockDrop(
                                e.target.value
                                  ? parseInt(e.target.value)
                                  : undefined,
                              )
                            }
                          />
                        </div>
                        <div className="space-y-1">
                          <Label className="text-xs">Max Style Drop %</Label>
                          <Input
                            type="number"
                            placeholder="e.g. 5"
                            min="0"
                            max="100"
                            value={deltaMaxStyleDrop ?? ""}
                            onChange={(e) =>
                              setDeltaMaxStyleDrop(
                                e.target.value
                                  ? parseInt(e.target.value)
                                  : undefined,
                              )
                            }
                          />
                        </div>
                      </div>
                    )}
                  </div>

                  {/* COUNT VALIDATION */}
                  <div className="border rounded-lg p-4">
                    <h4 className="text-sm font-semibold mb-3 flex items-center gap-2">
                      <FileCheck className="h-4 w-4 text-green-600" />
                      Count Validation (Expected Ranges)
                    </h4>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="grid grid-cols-2 gap-3">
                        <div className="space-y-1">
                          <Label className="text-xs">Min Items</Label>
                          <Input
                            type="number"
                            placeholder="e.g. 15000"
                            value={countMinItems ?? ""}
                            onChange={(e) =>
                              setCountMinItems(
                                e.target.value
                                  ? parseInt(e.target.value)
                                  : undefined,
                              )
                            }
                          />
                        </div>
                        <div className="space-y-1">
                          <Label className="text-xs">Max Items</Label>
                          <Input
                            type="number"
                            placeholder="e.g. 25000"
                            value={countMaxItems ?? ""}
                            onChange={(e) =>
                              setCountMaxItems(
                                e.target.value
                                  ? parseInt(e.target.value)
                                  : undefined,
                              )
                            }
                          />
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                        <div className="space-y-1">
                          <Label className="text-xs">Min Styles</Label>
                          <Input
                            type="number"
                            placeholder="e.g. 500"
                            value={countMinStyles ?? ""}
                            onChange={(e) =>
                              setCountMinStyles(
                                e.target.value
                                  ? parseInt(e.target.value)
                                  : undefined,
                              )
                            }
                          />
                        </div>
                        <div className="space-y-1">
                          <Label className="text-xs">Max Styles</Label>
                          <Input
                            type="number"
                            placeholder="e.g. 2000"
                            value={countMaxStyles ?? ""}
                            onChange={(e) =>
                              setCountMaxStyles(
                                e.target.value
                                  ? parseInt(e.target.value)
                                  : undefined,
                              )
                            }
                          />
                        </div>
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-3 mt-3">
                      <div className="space-y-1">
                        <Label className="text-xs">
                          Min Future Stock Items
                        </Label>
                        <Input
                          type="number"
                          placeholder="e.g. 5000"
                          value={countMinFutureStockItems ?? ""}
                          onChange={(e) =>
                            setCountMinFutureStockItems(
                              e.target.value
                                ? parseInt(e.target.value)
                                : undefined,
                            )
                          }
                        />
                      </div>
                      <div className="space-y-1">
                        <Label className="text-xs">
                          Min Discontinued Items
                        </Label>
                        <Input
                          type="number"
                          placeholder="e.g. 0"
                          value={countMinDiscontinuedItems ?? ""}
                          onChange={(e) =>
                            setCountMinDiscontinuedItems(
                              e.target.value
                                ? parseInt(e.target.value)
                                : undefined,
                            )
                          }
                        />
                      </div>
                    </div>
                  </div>

                  {/* RULE VALIDATION */}
                  <div className="border rounded-lg p-4">
                    <h4 className="text-sm font-semibold mb-3 flex items-center gap-2">
                      <ShieldCheck className="h-4 w-4 text-blue-600" />
                      Rule Validation
                    </h4>
                    <p className="text-xs text-muted-foreground mb-3">
                      Verify that import rules were correctly applied
                    </p>
                    <div className="grid grid-cols-2 gap-3">
                      <div className="flex items-center gap-2">
                        <Switch
                          id="verifyDiscontinued"
                          checked={verifyDiscontinuedDetection}
                          onCheckedChange={setVerifyDiscontinuedDetection}
                        />
                        <Label htmlFor="verifyDiscontinued" className="text-sm">
                          Verify discontinued detection
                        </Label>
                      </div>
                      <div className="flex items-center gap-2">
                        <Switch
                          id="verifyFutureDates"
                          checked={verifyFutureDatesDetection}
                          onCheckedChange={setVerifyFutureDatesDetection}
                        />
                        <Label htmlFor="verifyFutureDates" className="text-sm">
                          Verify future dates parsed
                        </Label>
                      </div>
                      <div className="flex items-center gap-2">
                        <Switch
                          id="verifySizeExpansion"
                          checked={verifySizeExpansion}
                          onCheckedChange={setVerifySizeExpansion}
                        />
                        <Label
                          htmlFor="verifySizeExpansion"
                          className="text-sm"
                        >
                          Verify size expansion applied
                        </Label>
                      </div>
                      <div className="flex items-center gap-2">
                        <Switch
                          id="verifyStockMappings"
                          checked={verifyStockTextMappings}
                          onCheckedChange={setVerifyStockTextMappings}
                        />
                        <Label
                          htmlFor="verifyStockMappings"
                          className="text-sm"
                        >
                          Verify stock text mappings
                        </Label>
                      </div>
                      <div className="flex items-center gap-2">
                        <Switch
                          id="verifyPriceExtraction"
                          checked={verifyPriceExtraction}
                          onCheckedChange={setVerifyPriceExtraction}
                        />
                        <Label
                          htmlFor="verifyPriceExtraction"
                          className="text-sm"
                        >
                          Verify price extraction
                        </Label>
                      </div>
                    </div>
                  </div>

                  {/* SPOT CHECKS */}
                  <div className="border rounded-lg p-4">
                    <h4 className="text-sm font-semibold mb-3 flex items-center gap-2">
                      <TestTube className="h-4 w-4 text-purple-600" />
                      Spot Checks
                    </h4>
                    <p className="text-xs text-muted-foreground mb-3">
                      Verify specific records exist with expected values
                    </p>

                    {spotChecks.length > 0 && (
                      <div className="border rounded-lg overflow-hidden mb-3">
                        <Table>
                          <TableHeader>
                            <TableRow className="bg-gray-50">
                              <TableHead className="text-xs">Style</TableHead>
                              <TableHead className="text-xs">Color</TableHead>
                              <TableHead className="text-xs">Size</TableHead>
                              <TableHead className="text-xs">
                                Expected Condition
                              </TableHead>
                              <TableHead className="text-xs w-12"></TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {spotChecks.map((check, idx) => (
                              <TableRow key={idx}>
                                <TableCell>
                                  <Input
                                    value={check.style}
                                    onChange={(e) => {
                                      const newChecks = [...spotChecks];
                                      newChecks[idx].style = e.target.value;
                                      setSpotChecks(newChecks);
                                    }}
                                    placeholder="e.g. 55618"
                                    className="h-8"
                                  />
                                </TableCell>
                                <TableCell>
                                  <Input
                                    value={check.color || ""}
                                    onChange={(e) => {
                                      const newChecks = [...spotChecks];
                                      newChecks[idx].color =
                                        e.target.value || undefined;
                                      setSpotChecks(newChecks);
                                    }}
                                    placeholder="e.g. BLACK"
                                    className="h-8"
                                  />
                                </TableCell>
                                <TableCell>
                                  <Input
                                    value={check.size || ""}
                                    onChange={(e) => {
                                      const newChecks = [...spotChecks];
                                      newChecks[idx].size =
                                        e.target.value || undefined;
                                      setSpotChecks(newChecks);
                                    }}
                                    placeholder="e.g. 4"
                                    className="h-8"
                                  />
                                </TableCell>
                                <TableCell>
                                  <Select
                                    value={check.expectedCondition}
                                    onValueChange={(v: any) => {
                                      const newChecks = [...spotChecks];
                                      newChecks[idx].expectedCondition = v;
                                      setSpotChecks(newChecks);
                                    }}
                                  >
                                    <SelectTrigger className="h-8">
                                      <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                      <SelectItem value="exists">
                                        Record Exists
                                      </SelectItem>
                                      <SelectItem value="stock_gt_0">
                                        Stock &gt; 0
                                      </SelectItem>
                                      <SelectItem value="has_future_date">
                                        Has Future Date
                                      </SelectItem>
                                      <SelectItem value="is_discontinued">
                                        Is Discontinued
                                      </SelectItem>
                                      <SelectItem value="has_price">
                                        Has Price
                                      </SelectItem>
                                    </SelectContent>
                                  </Select>
                                </TableCell>
                                <TableCell>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() =>
                                      setSpotChecks(
                                        spotChecks.filter((_, i) => i !== idx),
                                      )
                                    }
                                  >
                                    <Trash2 className="h-4 w-4 text-red-500" />
                                  </Button>
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    )}

                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        setSpotChecks([
                          ...spotChecks,
                          {
                            style: "",
                            color: undefined,
                            size: undefined,
                            expectedCondition: "exists",
                          },
                        ])
                      }
                    >
                      <Plus className="mr-2 h-4 w-4" />
                      Add Spot Check
                    </Button>
                  </div>

                  {/* Save Button */}
                  <div className="flex justify-end pt-2">
                    <Button onClick={saveConfiguration}>
                      <Save className="mr-2 h-4 w-4" />
                      Save Validation Rules
                    </Button>
                  </div>
                </div>
              )}

              {!postValidationEnabled && (
                <div className="text-center py-12 text-muted-foreground">
                  <ClipboardCheck className="h-12 w-12 mx-auto mb-4 opacity-20" />
                  <p>Enable validation to configure post-import checks</p>
                </div>
              )}

              <div className="flex justify-between pt-4 border-t">
                <Button
                  variant="outline"
                  onClick={() => setActiveTab("ai-detection")}
                >
                  ← Back to AI Detection
                </Button>
                <Button
                  onClick={() => setActiveTab("preview")}
                  disabled={!detectionResult}
                >
                  Continue to Preview →
                </Button>
              </div>
            </TabsContent>

            {/* PREVIEW TAB */}
            <TabsContent value="preview" className="space-y-4 mt-0">
              {previewResult && previewResult.preview && (
                <>
                  <div className="grid grid-cols-6 gap-3">
                    <div className="p-3 bg-gray-50 rounded-lg text-center">
                      <div className="text-xl font-bold">
                        {previewResult.preview.stats?.totalRows || 0}
                      </div>
                      <div className="text-xs text-muted-foreground">Rows</div>
                    </div>
                    <div className="p-3 bg-blue-50 rounded-lg text-center">
                      <div className="text-xl font-bold text-blue-600">
                        {previewResult.preview.stats?.totalItems || 0}
                      </div>
                      <div className="text-xs text-muted-foreground">Items</div>
                    </div>
                    <div className="p-3 bg-green-50 rounded-lg text-center">
                      <div className="text-xl font-bold text-green-600">
                        {previewResult.preview.stats?.saleItems || 0}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Sale Items
                      </div>
                    </div>
                    <div className="p-3 bg-yellow-50 rounded-lg text-center">
                      <div className="text-xl font-bold text-yellow-600">
                        {previewResult.preview.stats?.expandedSizes || 0}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Expanded
                      </div>
                    </div>
                    <div className="p-3 bg-orange-50 rounded-lg text-center">
                      <div className="text-xl font-bold text-orange-600">
                        {previewResult.preview.stats?.complexStockParsed || 0}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Complex Parsed
                      </div>
                    </div>
                    <div className="p-3 bg-purple-50 rounded-lg text-center">
                      <div className="text-xl font-bold text-purple-600">
                        {previewResult.preview.uniqueStyles || 0}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Styles
                      </div>
                    </div>
                  </div>

                  {previewResult.preview.sampleItems &&
                    previewResult.preview.sampleItems.length > 0 && (
                      <div className="border rounded-lg overflow-auto max-h-[300px]">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>Style</TableHead>
                              <TableHead>Color</TableHead>
                              <TableHead>Size</TableHead>
                              <TableHead className="text-right">
                                Stock
                              </TableHead>
                              <TableHead className="text-right">
                                Price
                              </TableHead>
                              <TableHead>Ship Date</TableHead>
                              <TableHead>Flags</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {previewResult.preview.sampleItems
                              .slice(0, 15)
                              .map((item, idx) => (
                                <TableRow
                                  key={idx}
                                  className={
                                    item.isExpandedSize ? "bg-yellow-50" : ""
                                  }
                                >
                                  <TableCell className="font-mono">
                                    {item.style}
                                  </TableCell>
                                  <TableCell>{item.color}</TableCell>
                                  <TableCell>
                                    {item.size}
                                    {item.isExpandedSize && (
                                      <span
                                        className="ml-1 text-xs text-yellow-600"
                                        title={`Expanded from size ${item.expandedFrom}`}
                                      >
                                        ✨
                                      </span>
                                    )}
                                  </TableCell>
                                  <TableCell className="text-right">
                                    {item.stock}
                                  </TableCell>
                                  <TableCell className="text-right">
                                    {item.price ? `$${item.price}` : "-"}
                                  </TableCell>
                                  <TableCell>{item.shipDate || "-"}</TableCell>
                                  <TableCell>
                                    {item.isExpandedSize && (
                                      <Badge className="bg-yellow-100 text-yellow-700 text-xs mr-1">
                                        Expanded
                                      </Badge>
                                    )}
                                    {item.isSaleItem && (
                                      <Badge className="text-xs mr-1">
                                        Sale
                                      </Badge>
                                    )}
                                    {item.discontinued && (
                                      <Badge
                                        variant="destructive"
                                        className="text-xs mr-1"
                                      >
                                        Disc
                                      </Badge>
                                    )}
                                    {item.specialOrder && (
                                      <Badge
                                        variant="outline"
                                        className="text-xs"
                                      >
                                        SO
                                      </Badge>
                                    )}
                                  </TableCell>
                                </TableRow>
                              ))}
                          </TableBody>
                        </Table>
                      </div>
                    )}
                </>
              )}
              <div className="flex justify-between pt-4 border-t">
                <Button
                  variant="outline"
                  onClick={() => setActiveTab("ai-detection")}
                >
                  ← Back
                </Button>
                <Button
                  onClick={() => setActiveTab("import")}
                  className="bg-green-600 hover:bg-green-700"
                >
                  Continue to Import →
                </Button>
              </div>
            </TabsContent>

            {/* IMPORT TAB */}
            <TabsContent value="import" className="space-y-4 mt-0">
              {!importResult ? (
                <div className="py-6">
                  {/* Two columns: Import New File | Validate Existing Data */}
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {/* LEFT: Import New File */}
                    <div className="border rounded-lg p-6 text-center">
                      <Database className="h-12 w-12 mx-auto text-green-500 mb-3" />
                      <h3 className="text-lg font-semibold mb-2">
                        Import New File
                      </h3>
                      <p className="text-sm text-muted-foreground mb-4">
                        Upload and process a new inventory file
                      </p>

                      {manualMultiFileMode && stagedManualFiles.length > 0 ? (
                        <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 mb-4 text-sm">
                          <span className="text-gray-600">Files: </span>
                          <span className="font-medium">
                            {stagedManualFiles.length} files staged
                          </span>
                          <p className="text-xs text-gray-500 mt-1">
                            {stagedManualFiles.map((f) => f.name).join(", ")}
                          </p>
                        </div>
                      ) : !selectedFile ? (
                        <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 mb-4 text-sm">
                          <AlertCircle className="h-4 w-4 inline mr-2 text-amber-600" />
                          <span className="text-amber-800">
                            No file selected
                          </span>
                          <p className="text-xs text-amber-600 mt-1">
                            Go to AI Detection tab to upload
                          </p>
                        </div>
                      ) : (
                        <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 mb-4 text-sm">
                          <span className="text-gray-600">File: </span>
                          <span className="font-medium">
                            {selectedFile.name}
                          </span>
                        </div>
                      )}

                      {validationEnabled && selectedFile && (
                        <div className="flex items-center justify-center gap-2 text-sm text-green-700 mb-3">
                          <ShieldCheck className="h-4 w-4" />
                          <span>Pre-Import Validation Active</span>
                        </div>
                      )}

                      <Button
                        onClick={executeImport}
                        disabled={
                          isImporting ||
                          (!selectedFile && stagedManualFiles.length === 0)
                        }
                        size="lg"
                        className="w-full bg-green-600 hover:bg-green-700"
                      >
                        {isImporting ? (
                          <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                        ) : (
                          <CheckCircle className="mr-2 h-5 w-5" />
                        )}
                        {manualMultiFileMode && stagedManualFiles.length > 1
                          ? `Execute Import (${stagedManualFiles.length} files)`
                          : "Execute Import"}
                      </Button>
                    </div>

                    {/* RIGHT: Validate Existing Database */}
                    <div className="border rounded-lg p-6 text-center">
                      <ClipboardCheck className="h-12 w-12 mx-auto text-blue-500 mb-3" />
                      <h3 className="text-lg font-semibold mb-2">
                        Validate Current Data
                      </h3>
                      <p className="text-sm text-muted-foreground mb-4">
                        Run validation checks on existing database records
                      </p>

                      {existingDataSource ? (
                        <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 mb-4 text-sm">
                          <span className="text-blue-700">Data source: </span>
                          <span className="font-medium text-blue-800">
                            {existingDataSource.name}
                          </span>
                          {dbItemCount !== null && (
                            <p className="text-xs text-blue-600 mt-1">
                              {dbItemCount} items in database
                            </p>
                          )}
                        </div>
                      ) : (
                        <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 mb-4 text-sm text-gray-500">
                          Save data source first to validate
                        </div>
                      )}

                      {postValidationEnabled && (
                        <div className="flex items-center justify-center gap-2 text-sm text-blue-700 mb-3">
                          <ShieldCheck className="h-4 w-4" />
                          <span>Post-Import Validation Active</span>
                        </div>
                      )}

                      <Button
                        onClick={validateDbData}
                        disabled={
                          isValidatingDb ||
                          (!existingDataSource && !createdDataSourceId)
                        }
                        size="lg"
                        variant="outline"
                        className="w-full border-blue-300 text-blue-700 hover:bg-blue-50"
                      >
                        {isValidatingDb ? (
                          <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                        ) : (
                          <ClipboardCheck className="mr-2 h-5 w-5" />
                        )}
                        Validate Database
                      </Button>
                    </div>
                  </div>

                  {/* DB Validation Results */}
                  {dbValidationResult && (
                    <div
                      className={`mt-6 border rounded-lg p-4 ${dbValidationResult.passed ? "bg-green-50 border-green-200" : "bg-red-50 border-red-200"}`}
                    >
                      <div className="flex items-center justify-between mb-4">
                        <h4
                          className={`text-lg font-semibold flex items-center gap-2 ${dbValidationResult.passed ? "text-green-800" : "text-red-800"}`}
                        >
                          {dbValidationResult.passed ? (
                            <>
                              <CheckCircle className="h-5 w-5 text-green-600" />{" "}
                              Database Validation Passed
                            </>
                          ) : (
                            <>
                              <AlertCircle className="h-5 w-5 text-red-600" />{" "}
                              Database Validation Issues
                            </>
                          )}
                        </h4>
                        <span className="text-sm">
                          {dbValidationResult.itemCount} items checked
                        </span>
                      </div>

                      {/* DB Stats vs Expected Stats Comparison */}
                      <div className="mb-4">
                        <div className="text-xs font-semibold text-muted-foreground mb-2">
                          Database Stats vs Expected (from last import)
                        </div>
                        <div className="grid grid-cols-4 gap-3 text-center text-sm">
                          <div className="p-2 border rounded bg-white">
                            <div className="font-bold">
                              {dbValidationResult.itemCount || 0}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              DB Items
                            </div>
                            {dbValidationResult.expectedStats && (
                              <div
                                className={`text-xs ${dbValidationResult.itemCount === dbValidationResult.expectedStats.itemCount ? "text-green-600" : "text-red-600"}`}
                              >
                                Expected:{" "}
                                {dbValidationResult.expectedStats.itemCount}
                              </div>
                            )}
                          </div>
                          <div className="p-2 border rounded bg-white">
                            <div className="font-bold">
                              {dbValidationResult.styleCount || 0}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              DB Styles
                            </div>
                            {dbValidationResult.expectedStats && (
                              <div
                                className={`text-xs ${dbValidationResult.styleCount === dbValidationResult.expectedStats.uniqueStyles ? "text-green-600" : "text-red-600"}`}
                              >
                                Expected:{" "}
                                {dbValidationResult.expectedStats.uniqueStyles}
                              </div>
                            )}
                          </div>
                          <div className="p-2 border rounded bg-white">
                            <div className="font-bold">
                              {dbValidationResult.totalStock || 0}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              DB Stock
                            </div>
                            {dbValidationResult.expectedStats && (
                              <div
                                className={`text-xs ${dbValidationResult.totalStock === dbValidationResult.expectedStats.totalStock ? "text-green-600" : "text-red-600"}`}
                              >
                                Expected:{" "}
                                {dbValidationResult.expectedStats.totalStock}
                              </div>
                            )}
                          </div>
                          <div
                            className={`p-2 border rounded ${dbValidationResult.passed ? "bg-green-100" : "bg-red-100"}`}
                          >
                            <div
                              className={`font-bold ${dbValidationResult.passed ? "text-green-600" : "text-red-600"}`}
                            >
                              {dbValidationResult.passedChecks}/
                              {dbValidationResult.totalChecks}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              Checks Passed
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Detailed Results grouped by category */}
                      {dbValidationResult.results && (
                        <div className="space-y-2 text-sm">
                          {dbValidationResult.results.map(
                            (check: any, idx: number) => (
                              <div
                                key={idx}
                                className={`flex items-center gap-2 p-2 rounded ${check.passed ? "bg-green-100" : "bg-red-100"}`}
                              >
                                {check.passed ? (
                                  <CheckCircle className="h-4 w-4 text-green-600 flex-shrink-0" />
                                ) : (
                                  <AlertCircle className="h-4 w-4 text-red-600 flex-shrink-0" />
                                )}
                                <span className="font-medium">
                                  {check.name}
                                </span>
                                <span className="text-muted-foreground flex-1">
                                  {check.message}
                                </span>
                                {check.category && (
                                  <Badge variant="outline" className="text-xs">
                                    {check.category}
                                  </Badge>
                                )}
                              </div>
                            ),
                          )}
                        </div>
                      )}

                      {/* DETAILED DIFF - Product Level Validation */}
                      {dbValidationResult.detailedDiff && (
                        <div
                          className={`mt-4 border rounded-lg p-4 ${
                            !dbValidationResult.detailedDiff.hasProductData
                              ? "bg-gray-50 border-gray-200"
                              : dbValidationResult.detailedDiff
                                    .missingStyleCount > 0 ||
                                  dbValidationResult.detailedDiff
                                    .extraStyleCount > 0 ||
                                  dbValidationResult.detailedDiff
                                    .missingColorCount > 0 ||
                                  dbValidationResult.detailedDiff
                                    .extraColorCount > 0 ||
                                  dbValidationResult.detailedDiff
                                    .productsWithIssues > 0
                                ? "bg-amber-50 border-amber-200"
                                : "bg-green-50 border-green-200"
                          }`}
                        >
                          <h5
                            className={`font-semibold mb-3 flex items-center gap-2 ${
                              !dbValidationResult.detailedDiff.hasProductData
                                ? "text-gray-700"
                                : dbValidationResult.detailedDiff
                                      .missingStyleCount > 0 ||
                                    dbValidationResult.detailedDiff
                                      .extraStyleCount > 0 ||
                                    dbValidationResult.detailedDiff
                                      .missingColorCount > 0 ||
                                    dbValidationResult.detailedDiff
                                      .extraColorCount > 0 ||
                                    dbValidationResult.detailedDiff
                                      .productsWithIssues > 0
                                  ? "text-amber-800"
                                  : "text-green-800"
                            }`}
                          >
                            {!dbValidationResult.detailedDiff.hasProductData ? (
                              <>
                                <AlertCircle className="h-4 w-4" /> Product Data
                                Not Available - Re-import Required
                              </>
                            ) : dbValidationResult.detailedDiff
                                .missingStyleCount > 0 ||
                              dbValidationResult.detailedDiff.extraStyleCount >
                                0 ||
                              dbValidationResult.detailedDiff
                                .missingColorCount > 0 ||
                              dbValidationResult.detailedDiff.extraColorCount >
                                0 ||
                              dbValidationResult.detailedDiff
                                .productsWithIssues > 0 ? (
                              <>
                                <AlertCircle className="h-4 w-4" />{" "}
                                {
                                  dbValidationResult.detailedDiff
                                    .productsWithIssues
                                }{" "}
                                Products Have Issues
                              </>
                            ) : (
                              <>
                                <CheckCircle className="h-4 w-4" /> All{" "}
                                {
                                  dbValidationResult.detailedDiff
                                    .totalProductsChecked
                                }{" "}
                                Products Match!
                              </>
                            )}
                          </h5>

                          {/* No productData - needs re-import */}
                          {!dbValidationResult.detailedDiff.hasProductData && (
                            <div className="text-sm text-gray-600">
                              <p className="mb-2">
                                Product-level validation is not available.
                              </p>
                              <p className="text-xs text-gray-500">
                                <strong>To enable:</strong> Re-import the file
                                with the latest code to save product-level data.
                              </p>
                            </div>
                          )}

                          {/* All matched */}
                          {dbValidationResult.detailedDiff.hasProductData &&
                            dbValidationResult.detailedDiff
                              .missingStyleCount === 0 &&
                            dbValidationResult.detailedDiff.extraStyleCount ===
                              0 &&
                            dbValidationResult.detailedDiff
                              .missingColorCount === 0 &&
                            dbValidationResult.detailedDiff.extraColorCount ===
                              0 &&
                            dbValidationResult.detailedDiff
                              .productsWithIssues === 0 && (
                              <div className="text-sm text-green-700">
                                <p>
                                  ✅ All{" "}
                                  {
                                    dbValidationResult.detailedDiff
                                      .totalProductsChecked
                                  }{" "}
                                  products validated successfully.
                                </p>
                                <p>
                                  ✅ Variant counts, colors, sizes, stock all
                                  match.
                                </p>
                              </div>
                            )}

                          {/* === STYLE DIFF === */}
                          {(dbValidationResult.detailedDiff.missingStyleCount >
                            0 ||
                            dbValidationResult.detailedDiff.extraStyleCount >
                              0) && (
                            <div className="mb-4 p-3 border rounded bg-white">
                              <div className="text-sm font-semibold mb-2">
                                📦 Style Differences
                              </div>
                              {dbValidationResult.detailedDiff
                                .missingStyleCount > 0 && (
                                <div className="mb-2">
                                  <div className="text-xs font-medium text-red-700">
                                    ❌ Missing from DB (
                                    {
                                      dbValidationResult.detailedDiff
                                        .missingStyleCount
                                    }{" "}
                                    styles):
                                  </div>
                                  <div className="text-xs text-red-600 bg-red-50 p-2 rounded max-h-20 overflow-y-auto mt-1">
                                    {dbValidationResult.detailedDiff.missingStyles?.join(
                                      ", ",
                                    )}
                                    {dbValidationResult.detailedDiff
                                      .missingStyleCount > 50 && (
                                      <span className="text-muted-foreground">
                                        {" "}
                                        ...and{" "}
                                        {dbValidationResult.detailedDiff
                                          .missingStyleCount - 50}{" "}
                                        more
                                      </span>
                                    )}
                                  </div>
                                </div>
                              )}
                              {dbValidationResult.detailedDiff.extraStyleCount >
                                0 && (
                                <div>
                                  <div className="text-xs font-medium text-orange-700">
                                    ⚠️ Extra in DB (
                                    {
                                      dbValidationResult.detailedDiff
                                        .extraStyleCount
                                    }{" "}
                                    styles):
                                  </div>
                                  <div className="text-xs text-orange-600 bg-orange-50 p-2 rounded max-h-20 overflow-y-auto mt-1">
                                    {dbValidationResult.detailedDiff.extraStyles?.join(
                                      ", ",
                                    )}
                                    {dbValidationResult.detailedDiff
                                      .extraStyleCount > 50 && (
                                      <span className="text-muted-foreground">
                                        {" "}
                                        ...and{" "}
                                        {dbValidationResult.detailedDiff
                                          .extraStyleCount - 50}{" "}
                                        more
                                      </span>
                                    )}
                                  </div>
                                </div>
                              )}
                            </div>
                          )}

                          {/* === COLOR DIFF === */}
                          {(dbValidationResult.detailedDiff.missingColorCount >
                            0 ||
                            dbValidationResult.detailedDiff.extraColorCount >
                              0) && (
                            <div className="mb-4 p-3 border rounded bg-white">
                              <div className="text-sm font-semibold mb-2">
                                🎨 Color Differences
                              </div>
                              {dbValidationResult.detailedDiff
                                .missingColorCount > 0 && (
                                <div className="mb-2">
                                  <div className="text-xs font-medium text-red-700">
                                    ❌ Missing from DB (
                                    {
                                      dbValidationResult.detailedDiff
                                        .missingColorCount
                                    }{" "}
                                    colors):
                                  </div>
                                  <div className="text-xs text-red-600 bg-red-50 p-2 rounded max-h-20 overflow-y-auto mt-1">
                                    {dbValidationResult.detailedDiff.missingColors?.join(
                                      ", ",
                                    )}
                                    {dbValidationResult.detailedDiff
                                      .missingColorCount > 30 && (
                                      <span className="text-muted-foreground">
                                        {" "}
                                        ...and{" "}
                                        {dbValidationResult.detailedDiff
                                          .missingColorCount - 30}{" "}
                                        more
                                      </span>
                                    )}
                                  </div>
                                </div>
                              )}
                              {dbValidationResult.detailedDiff.extraColorCount >
                                0 && (
                                <div>
                                  <div className="text-xs font-medium text-orange-700">
                                    ⚠️ Extra in DB (
                                    {
                                      dbValidationResult.detailedDiff
                                        .extraColorCount
                                    }{" "}
                                    colors):
                                  </div>
                                  <div className="text-xs text-orange-600 bg-orange-50 p-2 rounded max-h-20 overflow-y-auto mt-1">
                                    {dbValidationResult.detailedDiff.extraColors?.join(
                                      ", ",
                                    )}
                                    {dbValidationResult.detailedDiff
                                      .extraColorCount > 30 && (
                                      <span className="text-muted-foreground">
                                        {" "}
                                        ...and{" "}
                                        {dbValidationResult.detailedDiff
                                          .extraColorCount - 30}{" "}
                                        more
                                      </span>
                                    )}
                                  </div>
                                </div>
                              )}
                            </div>
                          )}

                          {/* === PRODUCT-LEVEL ISSUES === */}
                          {dbValidationResult.detailedDiff.productIssues
                            ?.length > 0 && (
                            <div className="mb-4 p-3 border rounded bg-white">
                              <div className="text-sm font-semibold mb-2">
                                📋 Product-Level Issues (
                                {
                                  dbValidationResult.detailedDiff
                                    .productsWithIssues
                                }{" "}
                                products)
                              </div>
                              <div className="space-y-2 max-h-60 overflow-y-auto">
                                {dbValidationResult.detailedDiff.productIssues.map(
                                  (issue: any, idx: number) => (
                                    <div
                                      key={idx}
                                      className={`text-xs p-2 rounded border-l-4 ${
                                        issue.severity === "error"
                                          ? "bg-red-50 border-red-500"
                                          : issue.severity === "warning"
                                            ? "bg-orange-50 border-orange-500"
                                            : "bg-blue-50 border-blue-500"
                                      }`}
                                    >
                                      <div className="flex items-start gap-2">
                                        <span className="font-semibold text-gray-800 min-w-[140px]">
                                          {issue.style}
                                        </span>
                                        <span
                                          className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                                            issue.issue === "EXTRA_VARIANTS" ||
                                            issue.issue ===
                                              "EXTRA_EXPANDED_SIZES"
                                              ? "bg-orange-200 text-orange-800"
                                              : issue.issue ===
                                                    "MISSING_VARIANTS" ||
                                                  issue.issue ===
                                                    "MISSING_PRODUCT"
                                                ? "bg-red-200 text-red-800"
                                                : issue.issue ===
                                                    "STOCK_MISMATCH"
                                                  ? "bg-yellow-200 text-yellow-800"
                                                  : "bg-gray-200 text-gray-800"
                                          }`}
                                        >
                                          {issue.issue.replace(/_/g, " ")}
                                        </span>
                                      </div>
                                      <div className="mt-1 grid grid-cols-2 gap-2 text-[11px]">
                                        <div>
                                          <span className="text-gray-500">
                                            Expected:
                                          </span>{" "}
                                          {issue.expected}
                                        </div>
                                        <div>
                                          <span className="text-gray-500">
                                            Actual:
                                          </span>{" "}
                                          {issue.actual}
                                        </div>
                                      </div>
                                    </div>
                                  ),
                                )}
                                {dbValidationResult.detailedDiff
                                  .productsWithIssues > 50 && (
                                  <div className="text-xs text-muted-foreground text-center py-2">
                                    ...and{" "}
                                    {dbValidationResult.detailedDiff
                                      .productsWithIssues - 50}{" "}
                                    more products with issues
                                  </div>
                                )}
                              </div>
                            </div>
                          )}

                          {/* Summary */}
                          {dbValidationResult.detailedDiff.hasProductData && (
                            <div className="mt-2 text-xs text-muted-foreground border-t pt-2">
                              <span className="font-medium">Summary:</span>{" "}
                              {
                                dbValidationResult.detailedDiff
                                  .totalProductsChecked
                              }{" "}
                              products checked |
                              {
                                dbValidationResult.detailedDiff
                                  .productsWithIssues
                              }{" "}
                              with issues | Expected{" "}
                              {dbValidationResult.detailedDiff.styleListCount}{" "}
                              styles, DB has{" "}
                              {dbValidationResult.detailedDiff.dbStyleCount}
                            </div>
                          )}
                        </div>
                      )}

                      {/* Show when last import was */}
                      {dbValidationResult.expectedStats?.importedAt && (
                        <div className="mt-3 text-xs text-muted-foreground">
                          Last import:{" "}
                          {new Date(
                            dbValidationResult.expectedStats.importedAt,
                          ).toLocaleString()}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="bg-green-50 border border-green-200 rounded-lg p-6">
                    <CheckCircle className="h-10 w-10 text-green-600 inline mr-3" />
                    <span className="text-xl font-semibold text-green-800">
                      Import Successful!
                    </span>
                    <p className="text-green-700 mt-2">
                      {importResult.stats?.finalCount ||
                        importResult.itemCount ||
                        0}{" "}
                      items imported
                    </p>
                  </div>
                  <div className="grid grid-cols-4 gap-3 text-center text-sm">
                    <div className="p-2 border rounded">
                      <div className="font-bold">
                        {importResult.stats?.totalParsed ||
                          importResult.itemCount ||
                          0}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Parsed
                      </div>
                    </div>
                    <div className="p-2 border rounded">
                      <div className="font-bold">
                        {importResult.stats?.saleItemsDetected || 0}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Sale Items
                      </div>
                    </div>
                    <div className="p-2 border rounded">
                      <div className="font-bold">
                        {importResult.stats?.shopifyPricesLoaded || 0}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Shopify Prices
                      </div>
                    </div>
                    <div className="p-2 border rounded bg-green-50">
                      <div className="font-bold text-green-600">
                        {importResult.stats?.finalCount ||
                          importResult.itemCount ||
                          0}
                      </div>
                      <div className="text-xs text-muted-foreground">Final</div>
                    </div>
                  </div>

                  {/* VALIDATION REPORT */}
                  {validationReport && validationReport.totalChecks > 0 && (
                    <div
                      className={`border rounded-lg p-4 ${validationReport.passed ? "bg-green-50 border-green-200" : "bg-red-50 border-red-200"}`}
                    >
                      <div className="flex items-center justify-between mb-4">
                        <h4
                          className={`text-lg font-semibold flex items-center gap-2 ${validationReport.passed ? "text-green-800" : "text-red-800"}`}
                        >
                          {validationReport.passed ? (
                            <CheckCircle className="h-5 w-5 text-green-600" />
                          ) : (
                            <CircleAlert className="h-5 w-5 text-red-600" />
                          )}
                          Validation Report
                        </h4>
                        <div
                          className={`text-2xl font-bold ${validationReport.passed ? "text-green-600" : "text-red-600"}`}
                        >
                          {validationReport.accuracy}%
                        </div>
                      </div>

                      <div className="text-sm mb-4 text-center">
                        <span
                          className={
                            validationReport.passed
                              ? "text-green-700"
                              : "text-red-700"
                          }
                        >
                          {validationReport.passedChecks}/
                          {validationReport.totalChecks} checks passed
                        </span>
                      </div>

                      {/* Checksum Results (NEW - Most Important) */}
                      {validationReport.checksumResults &&
                        validationReport.checksumResults.length > 0 && (
                          <div className="mb-3 p-3 bg-blue-50/50 rounded-lg border border-blue-200">
                            <h5 className="text-sm font-medium mb-2 flex items-center gap-1 text-blue-800">
                              <ShieldCheck className="h-4 w-4" />
                              Checksum Validation (Source vs Imported)
                            </h5>
                            <div className="space-y-1">
                              {validationReport.checksumResults.map(
                                (result: any, idx: number) => (
                                  <div
                                    key={idx}
                                    className="flex items-center gap-2 text-sm"
                                  >
                                    {result.passed ? (
                                      <CheckCircle className="h-4 w-4 text-green-600" />
                                    ) : (
                                      <CircleAlert className="h-4 w-4 text-red-600" />
                                    )}
                                    <span
                                      className={
                                        result.passed
                                          ? "text-green-700"
                                          : "text-red-700"
                                      }
                                    >
                                      {result.name}:{" "}
                                      {result.sourceValue.toLocaleString()} →{" "}
                                      {result.importedValue.toLocaleString()}
                                      {result.difference !== 0 && (
                                        <span className="text-gray-500 ml-1">
                                          ({result.difference > 0 ? "+" : ""}
                                          {result.difference.toLocaleString()},{" "}
                                          {result.differencePercent}%)
                                        </span>
                                      )}
                                      {result.difference === 0 && (
                                        <span className="text-green-600 ml-1 font-medium">
                                          EXACT MATCH
                                        </span>
                                      )}
                                    </span>
                                  </div>
                                ),
                              )}
                            </div>
                          </div>
                        )}

                      {/* Distribution Results (NEW) */}
                      {validationReport.distributionResults &&
                        validationReport.distributionResults.length > 0 && (
                          <div className="mb-3 p-3 bg-purple-50/50 rounded-lg border border-purple-200">
                            <h5 className="text-sm font-medium mb-2 flex items-center gap-1 text-purple-800">
                              <FileCheck className="h-4 w-4" />
                              Distribution Validation (Data Shape)
                            </h5>
                            <div className="space-y-1">
                              {validationReport.distributionResults.map(
                                (result: any, idx: number) => (
                                  <div
                                    key={idx}
                                    className="flex items-center gap-2 text-sm"
                                  >
                                    {result.passed ? (
                                      <CheckCircle className="h-4 w-4 text-green-600" />
                                    ) : (
                                      <CircleAlert className="h-4 w-4 text-red-600" />
                                    )}
                                    <span
                                      className={
                                        result.passed
                                          ? "text-green-700"
                                          : "text-red-700"
                                      }
                                    >
                                      {result.name}: {result.actualPercent}%
                                      (expected: {result.expectedRange})
                                    </span>
                                  </div>
                                ),
                              )}
                            </div>
                          </div>
                        )}

                      {/* Delta/Historical Results (NEW) */}
                      {validationReport.deltaResults &&
                        validationReport.deltaResults.length > 0 && (
                          <div className="mb-3 p-3 bg-orange-50/50 rounded-lg border border-orange-200">
                            <h5 className="text-sm font-medium mb-2 flex items-center gap-1 text-orange-800">
                              <RefreshCw className="h-4 w-4" />
                              Historical Comparison (vs Previous Import)
                            </h5>
                            <div className="space-y-1">
                              {validationReport.deltaResults.map(
                                (result: any, idx: number) => (
                                  <div
                                    key={idx}
                                    className="flex items-center gap-2 text-sm"
                                  >
                                    {result.passed ? (
                                      <CheckCircle className="h-4 w-4 text-green-600" />
                                    ) : (
                                      <CircleAlert className="h-4 w-4 text-red-600" />
                                    )}
                                    <span
                                      className={
                                        result.passed
                                          ? "text-green-700"
                                          : "text-red-700"
                                      }
                                    >
                                      {result.name}:{" "}
                                      {result.previousValue.toLocaleString()} →{" "}
                                      {result.currentValue.toLocaleString()}
                                      <span
                                        className={`ml-1 ${result.changePercent >= 0 ? "text-green-600" : "text-red-600"}`}
                                      >
                                        ({result.changePercent > 0 ? "+" : ""}
                                        {result.changePercent}%)
                                      </span>
                                      <span className="text-gray-500 ml-1">
                                        (limit: ±{result.maxAllowedPercent}%)
                                      </span>
                                    </span>
                                  </div>
                                ),
                              )}
                            </div>
                          </div>
                        )}

                      {/* Count Results */}
                      {validationReport.countResults &&
                        validationReport.countResults.length > 0 && (
                          <div className="mb-3">
                            <h5 className="text-sm font-medium mb-2 flex items-center gap-1">
                              <FileCheck className="h-4 w-4" />
                              Count Checks
                            </h5>
                            <div className="space-y-1">
                              {validationReport.countResults.map(
                                (result: any, idx: number) => (
                                  <div
                                    key={idx}
                                    className="flex items-center gap-2 text-sm"
                                  >
                                    {result.passed ? (
                                      <CheckCircle className="h-4 w-4 text-green-600" />
                                    ) : (
                                      <CircleAlert className="h-4 w-4 text-red-600" />
                                    )}
                                    <span
                                      className={
                                        result.passed
                                          ? "text-green-700"
                                          : "text-red-700"
                                      }
                                    >
                                      {result.name}:{" "}
                                      {result.actual.toLocaleString()}{" "}
                                      (expected: {result.expected})
                                    </span>
                                  </div>
                                ),
                              )}
                            </div>
                          </div>
                        )}

                      {/* Rule Results */}
                      {validationReport.ruleResults &&
                        validationReport.ruleResults.length > 0 && (
                          <div className="mb-3">
                            <h5 className="text-sm font-medium mb-2 flex items-center gap-1">
                              <ShieldCheck className="h-4 w-4" />
                              Rule Checks
                            </h5>
                            <div className="space-y-1">
                              {validationReport.ruleResults.map(
                                (result: any, idx: number) => (
                                  <div
                                    key={idx}
                                    className="flex items-center gap-2 text-sm"
                                  >
                                    {result.passed ? (
                                      <CheckCircle className="h-4 w-4 text-green-600" />
                                    ) : (
                                      <CircleAlert className="h-4 w-4 text-red-600" />
                                    )}
                                    <span
                                      className={
                                        result.passed
                                          ? "text-green-700"
                                          : "text-red-700"
                                      }
                                    >
                                      {result.name}: {result.actual}
                                    </span>
                                  </div>
                                ),
                              )}
                            </div>
                          </div>
                        )}

                      {/* Spot Check Results */}
                      {validationReport.spotCheckResults &&
                        validationReport.spotCheckResults.length > 0 && (
                          <div>
                            <h5 className="text-sm font-medium mb-2 flex items-center gap-1">
                              <TestTube className="h-4 w-4" />
                              Spot Checks
                            </h5>
                            <div className="space-y-1">
                              {validationReport.spotCheckResults.map(
                                (result: any, idx: number) => (
                                  <div
                                    key={idx}
                                    className="flex items-center gap-2 text-sm"
                                  >
                                    {result.passed ? (
                                      <CheckCircle className="h-4 w-4 text-green-600" />
                                    ) : (
                                      <CircleAlert className="h-4 w-4 text-red-600" />
                                    )}
                                    <span
                                      className={
                                        result.passed
                                          ? "text-green-700"
                                          : "text-red-700"
                                      }
                                    >
                                      {result.style}
                                      {result.color && `-${result.color}`}
                                      {result.size && `-${result.size}`}:{" "}
                                      {result.actual}
                                    </span>
                                  </div>
                                ),
                              )}
                            </div>
                          </div>
                        )}
                    </div>
                  )}

                  <div className="flex justify-end pt-4 border-t">
                    <Button variant="outline" onClick={handleClose}>
                      Close
                    </Button>
                  </div>
                </div>
              )}
            </TabsContent>
          </div>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}
