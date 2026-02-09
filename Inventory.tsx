import React, { useCallback, useState, useEffect, useMemo } from "react";
import { useDropzone } from "react-dropzone";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import * as XLSX from "xlsx";
import CleanupContent from "./Cleanup";
import ImportRulesTab from "@/components/ImportRulesTab";
import AIDataSourceDialog from "@/components/AIDataSourceDialog";
import GlobalValidatorDashboard from "@/components/GlobalValidatorDashboard";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
  CardFooter,
} from "@/components/ui/card";
import { formatDatePST } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  ResponsiveTabs,
  TabsContent as ResponsiveTabsContent,
} from "@/components/ui/responsive-tabs";
import { Badge } from "@/components/ui/badge";
import {
  UploadCloud,
  FileSpreadsheet,
  CheckCircle,
  AlertTriangle,
  FileText,
  ArrowRight,
  Link as LinkIcon,
  Mail,
  Plus,
  Clock,
  Settings2,
  Save,
  Wand2,
  Sparkles,
  Eraser,
  RefreshCw,
  PlayCircle,
  FolderOpen,
  Trash2,
  Download,
  Database,
  Copy,
  Search,
  X,
  Filter,
  Edit2,
  Tag,
  ShoppingBag,
  Palette,
  ChevronDown,
  ChevronRight,
  Upload,
  GitBranch,
  Ruler,
  Shield,
  DollarSign,
  Files,
} from "lucide-react";
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Switch } from "@/components/ui/switch";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Separator } from "@/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { useToast } from "@/hooks/use-toast";

interface DataSource {
  id: string;
  name: string;
  type: string;
  columnMapping?: any;
  cleaningConfig?: any;
  autoUpdate?: boolean;
  updateFrequency?: string;
  updateTime?: string;
  connectionDetails?: any;
  lastSync?: string;
  status: string;
  continueSelling?: boolean | null;
  sourceType?: string; // 'inventory' | 'sales'
  salesConfig?: {
    priceMultiplier?: number;
    useCompareAtPrice?: boolean;
  };
  regularPriceConfig?: {
    useFilePrice?: boolean;
    priceMultiplier?: number;
  };
  linkedDataSourceId?: string | null; // DEPRECATED: Link to parent regular inventory data source
  assignedSaleDataSourceId?: string | null; // Assigned sale file for regular data sources
  requireSaleImportFirst?: boolean | null; // Warn if importing before sale file
  priceBasedExpansionConfig?: {
    enabled?: boolean;
    tiers?: Array<{
      minPrice: number;
      expandDown: number;
      expandUp: number;
    }>;
    defaultExpandDown?: number;
    defaultExpandUp?: number;
    expandedStock?: number;
  };
  variantSyncConfig?: {
    enableVariantCreation?: boolean;
    allowZeroStockCreation?: boolean;
    enableVariantDeletion?: boolean;
    maxDeletionLimit?: number;
    deleteAction?: "delete" | "zero_out";
    useExactSkuMatching?: boolean;
  };
  filterZeroStock?: boolean;
  sizeLimitConfig?: {
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
  };
  autoSyncToShopify?: boolean;
  shopifyStoreId?: string | null;
  salePriceConfig?: any;
  discontinuedRules?: any;
  priceFloorCeiling?: any;
  minStockThreshold?: number;
  requiredFieldsConfig?: any;
  dateFormatConfig?: any;
  valueReplacementRules?: any;
  sheetConfig?: any;
  fileParseConfig?: any;
}

interface VariantRule {
  id: string;
  name: string;
  dataSourceId?: string | null;
  stockMin?: number | null;
  stockMax?: number | null;
  sizes?: string[] | null;
  colors?: string[] | null;
  expandSizes?: boolean;
  expandDownCount?: number | null;
  expandUpCount?: number | null;
  minTriggerStock?: number | null;
  expandedStock?: number | null;
  enabled?: boolean;
  createdAt?: string;
}

interface ShopifyMetafieldRule {
  id: string;
  name: string;
  dataSourceId?: string | null;
  metafieldNamespace: string;
  metafieldKey: string;
  stockThreshold?: number | null;
  inStockMessage: string;
  sizeExpansionMessage?: string | null;
  outOfStockMessage: string;
  futureDateMessage?: string | null;
  dateOffsetDays?: number | null;
  enabled?: boolean;
  createdAt?: string;
}

interface ColorMapping {
  id: string;
  badColor: string;
  goodColor: string;
}

interface MasterInventoryItem {
  id: string;
  sku: string;
  style: string | null;
  size: string | null;
  color: string | null;
  stock: number | null;
  cost: string | null;
  price: string | null;
  shipDate: string | null;
  sourceName: string;
  dataSourceId: string | null;
  importedAt: string | null;
  isExpandedSize?: boolean | null;
  saleOwnsStyle?: boolean | null;
  isSaleFile?: boolean;
  cachedShopifyPrice?: number | null;
}

function calculateStockMessage(
  rule: ShopifyMetafieldRule | null,
  item: {
    stock: number | null;
    shipDate: string | null;
    isExpandedSize?: boolean | null;
  },
): string {
  if (!rule || !rule.enabled) return "";

  const stock = item.stock ?? 0;
  const threshold = rule.stockThreshold ?? 0;
  const shipDateStr = item.shipDate;
  const isExpandedSize = item.isExpandedSize ?? false;

  // Check for expanded sizes first (special call-for-availability message)
  if (isExpandedSize && rule.sizeExpansionMessage) {
    return rule.sizeExpansionMessage;
  }

  // Check for future ship date (pre-order scenario)
  if (shipDateStr && rule.futureDateMessage) {
    try {
      const shipDate = new Date(shipDateStr);
      const today = new Date();
      today.setHours(0, 0, 0, 0);

      const offsetDays = rule.dateOffsetDays ?? 0;
      const adjustedDate = new Date(shipDate);
      adjustedDate.setDate(adjustedDate.getDate() + offsetDays);

      // Only show future date message if date is in the future AND stock is 0
      if (adjustedDate > today && stock <= 0) {
        const monthNames = [
          "January",
          "February",
          "March",
          "April",
          "May",
          "June",
          "July",
          "August",
          "September",
          "October",
          "November",
          "December",
        ];
        const formattedDate = `${monthNames[adjustedDate.getMonth()]} ${adjustedDate.getDate()}, ${adjustedDate.getFullYear()}`;
        return rule.futureDateMessage.replace("{date}", formattedDate);
      }
    } catch {
      // Invalid date, continue to stock-based logic
    }
  }

  // Stock-based message
  if (stock > threshold) {
    return rule.inStockMessage || "";
  } else {
    return rule.outOfStockMessage || "";
  }
}

interface PaginatedResponse {
  items: MasterInventoryItem[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}

function MasterInventoryTab() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Pagination state
  const [page, setPage] = useState(1);
  const [limit] = useState(100);

  // Filter state
  const [searchStyle, setSearchStyle] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [filterSource, setFilterSource] = useState("");
  const [stockFilter, setStockFilter] = useState<
    "all" | "inStock" | "outOfStock"
  >("all");
  const [showOnlyInCache, setShowOnlyInCache] = useState(false);

  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(searchStyle);
      setPage(1);
    }, 300);
    return () => clearTimeout(timer);
  }, [searchStyle]);

  const {
    data: paginatedData,
    isLoading,
    error,
  } = useQuery<PaginatedResponse>({
    queryKey: [
      "master-inventory-paginated",
      page,
      limit,
      debouncedSearch,
      filterSource,
      showOnlyInCache,
    ],
    queryFn: async () => {
      const params = new URLSearchParams({
        page: page.toString(),
        limit: limit.toString(),
      });
      if (debouncedSearch) params.append("search", debouncedSearch);
      if (filterSource) params.append("dataSourceId", filterSource);
      if (showOnlyInCache) params.append("onlyInCache", "true");

      const res = await fetch(`/api/inventory/master/paginated?${params}`);
      if (!res.ok) throw new Error("Failed to fetch master inventory");
      return res.json();
    },
  });

  const masterInventory = paginatedData?.items || [];
  const totalItems = paginatedData?.total || 0;
  const totalPages = paginatedData?.totalPages || 0;

  // Fetch data sources for filter dropdown
  const { data: dataSources = [] } = useQuery<DataSource[]>({
    queryKey: ["data-sources"],
    queryFn: async () => {
      const res = await fetch("/api/data-sources");
      if (!res.ok) throw new Error("Failed to fetch data sources");
      return res.json();
    },
  });

  // Fetch all metafield rules for stock message calculation
  const { data: metafieldRules = [] } = useQuery<ShopifyMetafieldRule[]>({
    queryKey: ["all-metafield-rules"],
    queryFn: async () => {
      const res = await fetch("/api/shopify-metafield-rules");
      if (!res.ok) throw new Error("Failed to fetch metafield rules");
      return res.json();
    },
  });

  // Fetch summary stats for all inventory (not just current page)
  const { data: summaryData } = useQuery<{
    totalItems: number;
    totalStock: number;
    uniqueStyles: number;
    byDataSource: Array<{
      dataSourceId: string;
      sourceName: string;
      count: number;
      totalStock: number;
    }>;
  }>({
    queryKey: ["master-inventory-summary", filterSource],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (filterSource) params.append("dataSourceId", filterSource);
      const res = await fetch(`/api/inventory/master/summary?${params}`);
      if (!res.ok) throw new Error("Failed to fetch inventory summary");
      return res.json();
    },
  });

  // Build a map of dataSourceId -> enabled metafield rule for quick lookup
  const rulesByDataSourceId = useMemo(() => {
    const map = new Map<string, ShopifyMetafieldRule>();
    for (const rule of metafieldRules) {
      if (rule.enabled && rule.dataSourceId) {
        map.set(rule.dataSourceId, rule);
      }
    }
    return map;
  }, [metafieldRules]);

  // Apply client-side stock filter only (search/source filter handled by server)
  const filteredInventory = masterInventory.filter((item) => {
    if (stockFilter === "inStock" && (!item.stock || item.stock <= 0)) {
      return false;
    }
    if (stockFilter === "outOfStock" && item.stock && item.stock > 0) {
      return false;
    }
    return true;
  });

  const clearFilters = () => {
    setSearchStyle("");
    setFilterSource("");
    setStockFilter("all");
    setShowOnlyInCache(false);
    setPage(1);
  };

  const hasActiveFilters = searchStyle || filterSource || stockFilter !== "all" || showOnlyInCache;

  const handleDownload = async () => {
    try {
      const url = filterSource 
        ? `/api/inventory/master/download?dataSourceId=${filterSource}`
        : "/api/inventory/master/download";
      const response = await fetch(url);
      if (!response.ok) throw new Error("Download failed");

      const blob = await response.blob();
      const blobUrl = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = blobUrl;
      const sourceName = filterSource 
        ? dataSources.find(ds => ds.id === filterSource)?.name?.replace(/\s+/g, '_') || 'filtered'
        : 'all';
      a.download = `master_inventory_${sourceName}_${new Date().toISOString().split("T")[0]}.csv`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(blobUrl);
      a.remove();

      toast({
        title: "Download Started",
        description: filterSource 
          ? `Downloading inventory for ${dataSources.find(ds => ds.id === filterSource)?.name || 'selected source'}.`
          : "Your master inventory file is downloading.",
      });
    } catch (error) {
      toast({
        title: "Download Failed",
        description: "Could not download the inventory file.",
        variant: "destructive",
      });
    }
  };

  const handleClearInventory = async () => {
    const selectedSourceName = filterSource 
      ? dataSources.find(ds => ds.id === filterSource)?.name 
      : null;
    
    const confirmMessage = selectedSourceName
      ? `Are you sure you want to clear all items from "${selectedSourceName}"? This action cannot be undone.`
      : "Are you sure you want to clear all items from the master inventory? This action cannot be undone.";
    
    if (!confirm(confirmMessage)) {
      return;
    }

    try {
      const url = filterSource 
        ? `/api/inventory/clear?dataSourceId=${filterSource}`
        : "/api/inventory/clear";
      
      const response = await fetch(url, {
        method: "DELETE",
      });
      if (!response.ok) throw new Error("Clear failed");

      queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
      queryClient.invalidateQueries({ queryKey: ["master-inventory-paginated"] });
      queryClient.invalidateQueries({ queryKey: ["master-inventory-summary"] });
      
      toast({
        title: "Inventory Cleared",
        description: selectedSourceName 
          ? `All items from "${selectedSourceName}" have been removed.`
          : "All items have been removed from the master inventory.",
      });
    } catch (error) {
      toast({
        title: "Clear Failed",
        description: "Could not clear the inventory.",
        variant: "destructive",
      });
    }
  };

  const summaryTotalItems = summaryData?.totalItems ?? totalItems;
  const summaryTotalStock = summaryData?.totalStock ?? 0;
  const uniqueSources = summaryData?.byDataSource?.length ?? 0;

  return (
    <div className="space-y-4">
      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className={`h-10 w-10 rounded-lg flex items-center justify-center ${showOnlyInCache ? 'bg-teal-100 dark:bg-teal-900/30' : 'bg-primary/10'}`}>
                <Database className={`h-5 w-5 ${showOnlyInCache ? 'text-teal-600 dark:text-teal-400' : 'text-primary'}`} />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">
                  {showOnlyInCache ? 'In Shopify' : 'Total Items'}
                </p>
                <p
                  className="text-2xl font-bold"
                  data-testid="text-total-items"
                >
                  {showOnlyInCache ? totalItems.toLocaleString() : summaryTotalItems.toLocaleString()}
                  {showOnlyInCache && (
                    <span className="text-sm font-normal text-muted-foreground ml-2">
                      of {summaryTotalItems.toLocaleString()}
                    </span>
                  )}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="h-10 w-10 bg-green-100 rounded-lg flex items-center justify-center">
                <CheckCircle className="h-5 w-5 text-green-600" />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Total Stock</p>
                <p
                  className="text-2xl font-bold"
                  data-testid="text-total-stock"
                >
                  {summaryTotalStock.toLocaleString()}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="h-10 w-10 bg-blue-100 rounded-lg flex items-center justify-center">
                <FolderOpen className="h-5 w-5 text-blue-600" />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Data Sources</p>
                <p
                  className="text-2xl font-bold"
                  data-testid="text-data-sources"
                >
                  {uniqueSources}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6 flex gap-2">
            <Button
              onClick={handleDownload}
              className="flex-1 h-full min-h-[60px]"
              disabled={totalItems === 0}
              data-testid="button-download-master"
            >
              <Download className="mr-2 h-5 w-5" />
              {filterSource 
                ? `Download ${dataSources.find(ds => ds.id === filterSource)?.name || 'Source'}`
                : "Download All"}
            </Button>
            <Button
              onClick={handleClearInventory}
              variant="destructive"
              className="h-full min-h-[60px]"
              disabled={totalItems === 0}
              data-testid="button-clear-master"
            >
              <Trash2 className="h-5 w-5" />
            </Button>
          </CardContent>
        </Card>
      </div>

      {/* Filter Section */}
      <Card>
        <CardContent className="pt-4">
          <div className="flex flex-wrap gap-3 items-end">
            <div className="flex-1 min-w-[200px]">
              <Label className="text-xs text-muted-foreground mb-1 block">
                Search Style
              </Label>
              <div className="relative">
                <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search by style..."
                  value={searchStyle}
                  onChange={(e) => setSearchStyle(e.target.value)}
                  className="pl-8"
                  data-testid="input-search-style"
                />
              </div>
            </div>
            <div className="w-[180px]">
              <Label className="text-xs text-muted-foreground mb-1 block">
                Data Source
              </Label>
              <Select
                value={filterSource || "__all__"}
                onValueChange={(v) => {
                  setFilterSource(v === "__all__" ? "" : v);
                  setPage(1);
                }}
              >
                <SelectTrigger data-testid="select-filter-source">
                  <SelectValue placeholder="All Sources" />
                </SelectTrigger>
                <SelectContent className="max-h-[300px] overflow-y-auto">
                  <SelectItem value="__all__">All Sources</SelectItem>
                  {dataSources.map((ds) => (
                    <SelectItem key={ds.id} value={ds.id}>
                      {ds.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="w-[140px]">
              <Label className="text-xs text-muted-foreground mb-1 block">
                Stock Status
              </Label>
              <Select
                value={stockFilter}
                onValueChange={(v) => setStockFilter(v as any)}
              >
                <SelectTrigger data-testid="select-filter-stock">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Items</SelectItem>
                  <SelectItem value="inStock">In Stock</SelectItem>
                  <SelectItem value="outOfStock">Out of Stock</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-2 h-9 self-end">
              <Switch
                id="cache-filter"
                checked={showOnlyInCache}
                onCheckedChange={(checked) => {
                  setShowOnlyInCache(checked);
                  setPage(1);
                }}
                data-testid="switch-only-in-cache"
              />
              <Label htmlFor="cache-filter" className="text-xs text-muted-foreground cursor-pointer">
                Only in Shopify
              </Label>
            </div>
            {hasActiveFilters && (
              <Button
                variant="ghost"
                size="sm"
                onClick={clearFilters}
                className="h-9"
                data-testid="button-clear-filters"
              >
                <X className="h-4 w-4 mr-1" />
                Clear
              </Button>
            )}
          </div>
          <div className="mt-3 flex items-center gap-2 text-sm text-muted-foreground">
            <Filter className="h-4 w-4" />
            Showing page {page} of {totalPages} ({totalItems.toLocaleString()}{" "}
            total items)
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Master Inventory Preview
          </CardTitle>
          <CardDescription>
            Combined inventory from all uploaded files. This file can be used to
            update your Shopify inventory.
          </CardDescription>
          {summaryData?.byDataSource && dataSources.length > 0 && (
            <div className="mt-3 p-3 bg-muted/50 rounded-lg" data-testid="datasource-import-status">
              <div className="flex items-center gap-2 mb-2">
                <span className="text-sm font-medium">
                  {summaryData.byDataSource.length} of {dataSources.length} data sources imported
                </span>
                {summaryData.byDataSource.length === dataSources.length ? (
                  <Badge variant="outline" className="bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300 border-green-200 dark:border-green-800">All imported</Badge>
                ) : summaryData.byDataSource.length === 0 ? (
                  <Badge variant="outline" className="bg-yellow-50 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300 border-yellow-200">None imported</Badge>
                ) : (
                  <Badge variant="outline" className="bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border-blue-200">Partial</Badge>
                )}
              </div>
              {summaryData.byDataSource.length > 0 && (
                <div className="flex flex-wrap gap-2">
                  {summaryData.byDataSource.map((source) => (
                    <Badge key={source.dataSourceId} variant="secondary" className="text-xs">
                      {source.sourceName}: {source.count.toLocaleString()}
                    </Badge>
                  ))}
                </div>
              )}
            </div>
          )}
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
              <span className="ml-2 text-muted-foreground">
                Loading inventory...
              </span>
            </div>
          ) : error ? (
            <div className="flex items-center justify-center py-12 text-red-500">
              <AlertTriangle className="h-6 w-6 mr-2" />
              Failed to load inventory
            </div>
          ) : totalItems === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
              <FileSpreadsheet className="h-12 w-12 mb-4 opacity-50" />
              <p className="text-lg font-medium">No inventory data yet</p>
              <p className="text-sm">
                Upload files and import them to see combined inventory here.
              </p>
            </div>
          ) : (
            <div className="border rounded-lg overflow-auto max-h-[500px]">
              <table
                className="w-full text-sm"
                data-testid="table-master-inventory"
              >
                <thead className="bg-muted/50 sticky top-0">
                  <tr>
                    <th className="px-4 py-3 text-left font-medium">Style</th>
                    <th className="px-4 py-3 text-left font-medium">Color</th>
                    <th className="px-4 py-3 text-left font-medium">Size</th>
                    <th className="px-4 py-3 text-right font-medium">Stock</th>
                    <th className="px-4 py-3 text-right font-medium">Price</th>
                    <th className="px-4 py-3 text-right font-medium">
                      Shopify Price
                    </th>
                    <th className="px-4 py-3 text-left font-medium">
                      Ship Date
                    </th>
                    <th className="px-4 py-3 text-left font-medium">
                      Stock Message
                    </th>
                    <th className="px-4 py-3 text-left font-medium">SKU</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredInventory.map((item, index) => (
                    <tr
                      key={item.id}
                      className={`border-t hover:bg-muted/30 ${item.isExpandedSize ? "bg-purple-50 dark:bg-purple-500/15" : ""}`}
                      data-testid={`row-inventory-${index}`}
                    >
                      <td className="px-4 py-2 font-medium">
                        <span className="flex items-center gap-1.5">
                          {item.style || item.sku || "-"}
                          {item.isSaleFile && (
                            <Tag
                              className="h-3.5 w-3.5 text-orange-500"
                              title="From Sale File"
                            />
                          )}
                        </span>
                      </td>
                      <td className="px-4 py-2">{item.color || "-"}</td>
                      <td className="px-4 py-2">
                        {item.size ? (
                          <Badge
                            variant="outline"
                            className={`text-xs ${item.isExpandedSize ? "border-purple-400 bg-purple-100 text-purple-700 dark:bg-purple-500/30 dark:text-purple-200 dark:border-purple-400/50" : ""}`}
                          >
                            {item.size}
                            {item.isExpandedSize && (
                              <span className="ml-1">+</span>
                            )}
                          </Badge>
                        ) : (
                          "-"
                        )}
                      </td>
                      <td className="px-4 py-2 text-right font-medium">
                        <Badge
                          variant={
                            item.stock && item.stock > 0
                              ? "default"
                              : "secondary"
                          }
                          className="text-xs"
                        >
                          {item.stock ?? 0}
                        </Badge>
                      </td>
                      <td className="px-4 py-2 text-right text-muted-foreground">
                        {item.price ? `$${item.price}` : "-"}
                      </td>
                      <td
                        className="px-4 py-2 text-right"
                        data-testid={`text-shopify-price-${index}`}
                      >
                        {item.cachedShopifyPrice ? (
                          <span className="text-green-600 font-medium">
                            ${item.cachedShopifyPrice.toLocaleString()}
                          </span>
                        ) : (
                          <span className="text-muted-foreground">-</span>
                        )}
                      </td>
                      <td className="px-4 py-2 text-muted-foreground">
                        {item.shipDate || "-"}
                      </td>
                      <td className="px-4 py-2">
                        {(() => {
                          const stockInfo = (item as any).stockInfo;
                          if (stockInfo) {
                            return (
                              <span className="text-xs text-blue-600 dark:text-teal-400">
                                {stockInfo}
                              </span>
                            );
                          }
                          const rule = item.dataSourceId
                            ? rulesByDataSourceId.get(item.dataSourceId)
                            : null;
                          const message = calculateStockMessage(
                            rule || null,
                            item,
                          );
                          return message ? (
                            <span className="text-xs text-blue-600 dark:text-teal-400">
                              {message}
                            </span>
                          ) : (
                            <span className="text-xs text-muted-foreground">
                              -
                            </span>
                          );
                        })()}
                      </td>
                      <td className="px-4 py-2 font-mono text-xs text-muted-foreground">
                        {item.sku || "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {totalPages > 1 && (
                <div className="p-3 flex items-center justify-between bg-muted/30 border-t">
                  <span className="text-sm text-muted-foreground">
                    Page {page} of {totalPages} ({totalItems.toLocaleString()}{" "}
                    total items)
                  </span>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setPage(1)}
                      disabled={page === 1}
                    >
                      First
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setPage((p) => Math.max(1, p - 1))}
                      disabled={page === 1}
                    >
                      Previous
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        setPage((p) => Math.min(totalPages, p + 1))
                      }
                      disabled={page === totalPages}
                    >
                      Next
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setPage(totalPages)}
                      disabled={page === totalPages}
                    >
                      Last
                    </Button>
                  </div>
                </div>
              )}
              {filteredInventory.length === 0 && hasActiveFilters && (
                <div className="p-6 text-center text-muted-foreground">
                  <Search className="h-8 w-8 mx-auto mb-2 opacity-50" />
                  <p>No items match your search</p>
                  <Button variant="link" size="sm" onClick={clearFilters}>
                    Clear filters
                  </Button>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function RuleEngineSection({
  selectedDataSourceId,
  dataSources,
}: {
  selectedDataSourceId: string;
  dataSources: DataSource[];
}) {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Fetch Shopify stores for auto-sync dropdown
  const { data: shopifyStores = [] } = useQuery<
    { id: string; name: string; domain: string }[]
  >({
    queryKey: ["shopify-stores"],
    queryFn: async () => {
      const res = await fetch("/api/shopify/stores");
      if (!res.ok) throw new Error("Failed to fetch stores");
      return res.json();
    },
  });

  // Fetch sync status for the selected data source
  const { data: syncStatus } = useQuery<{
    status: "idle" | "queued" | "running" | "completed" | "failed";
    progress?: { current: number; total: number; phase: string };
    lastSyncAt?: string;
    lastError?: string;
  }>({
    queryKey: ["data-source-sync-status", selectedDataSourceId],
    queryFn: async () => {
      const res = await fetch(
        `/api/data-sources/${selectedDataSourceId}/sync-status`,
      );
      if (!res.ok) throw new Error("Failed to fetch sync status");
      return res.json();
    },
    enabled: !!selectedDataSourceId,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data?.status === "running" || data?.status === "queued") return 2000;
      return false;
    },
  });

  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<VariantRule | null>(null);

  const [ruleName, setRuleName] = useState("");
  const [sizesList, setSizesList] = useState("");
  const [colorsList, setColorsList] = useState("");
  const [expandSizes, setExpandSizes] = useState(false);
  const [expandDownCount, setExpandDownCount] = useState("2");
  const [expandUpCount, setExpandUpCount] = useState("1");
  const [minTriggerStock, setMinTriggerStock] = useState("1");
  const [expandedStock, setExpandedStock] = useState("1");

  const [isMetafieldDialogOpen, setIsMetafieldDialogOpen] = useState(false);
  const [editingMetafieldRule, setEditingMetafieldRule] =
    useState<ShopifyMetafieldRule | null>(null);
  const [metafieldRuleName, setMetafieldRuleName] = useState("");
  const [metafieldNamespace, setMetafieldNamespace] = useState("my_fields");
  const [metafieldKey, setMetafieldKey] = useState("stock_info");
  const [stockThreshold, setStockThreshold] = useState("0");
  const [inStockMessage, setInStockMessage] = useState("");
  const [sizeExpansionMessage, setSizeExpansionMessage] = useState("");
  const [outOfStockMessage, setOutOfStockMessage] = useState("");
  const [futureDateMessage, setFutureDateMessage] = useState("");
  const [dateOffsetDays, setDateOffsetDays] = useState("0");

  const [newBadColor, setNewBadColor] = useState("");
  const [newGoodColor, setNewGoodColor] = useState("");
  const [colorMappingsOpen, setColorMappingsOpen] = useState(false);

  const { data: rules = [], isLoading } = useQuery<VariantRule[]>({
    queryKey: ["variant-rules", selectedDataSourceId],
    queryFn: async () => {
      const url = selectedDataSourceId
        ? `/api/rules?dataSourceId=${selectedDataSourceId}`
        : "/api/rules";
      const res = await fetch(url);
      if (!res.ok) throw new Error("Failed to fetch rules");
      return res.json();
    },
    enabled: !!selectedDataSourceId,
  });

  const { data: metafieldRules = [], isLoading: isLoadingMetafieldRules } =
    useQuery<ShopifyMetafieldRule[]>({
      queryKey: ["shopify-metafield-rules", selectedDataSourceId],
      queryFn: async () => {
        const url = selectedDataSourceId
          ? `/api/shopify-metafield-rules?dataSourceId=${selectedDataSourceId}`
          : "/api/shopify-metafield-rules";
        const res = await fetch(url);
        if (!res.ok) throw new Error("Failed to fetch Shopify metafield rules");
        return res.json();
      },
      enabled: !!selectedDataSourceId,
    });

  const { data: colorMappings = [], isLoading: isLoadingColorMappings } =
    useQuery<ColorMapping[]>({
      queryKey: ["/api/color-mappings"],
    });

  const uploadColorMappingsMutation = useMutation({
    mutationFn: async (file: File) => {
      const formData = new FormData();
      formData.append("file", file);
      const res = await fetch("/api/color-mappings/upload", {
        method: "POST",
        body: formData,
      });
      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || "Upload failed");
      }
      return res.json();
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ["/api/color-mappings"] });
      toast({
        title: "Success",
        description: data.message || "Color mappings uploaded successfully",
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const addColorMappingMutation = useMutation({
    mutationFn: async ({
      badColor,
      goodColor,
    }: {
      badColor: string;
      goodColor: string;
    }) => {
      const res = await fetch("/api/color-mappings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ badColor, goodColor }),
      });
      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || "Failed to add mapping");
      }
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/color-mappings"] });
      setNewBadColor("");
      setNewGoodColor("");
      toast({ title: "Success", description: "Color mapping added" });
    },
    onError: (error: Error) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const deleteColorMappingMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/color-mappings/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) throw new Error("Failed to delete");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/color-mappings"] });
      toast({ title: "Success", description: "Mapping deleted" });
    },
  });

  const clearAllColorMappingsMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch("/api/color-mappings", { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to clear");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/color-mappings"] });
      toast({ title: "Success", description: "All color mappings cleared" });
    },
  });

  const onColorMappingDrop = useCallback(
    (acceptedFiles: File[]) => {
      if (acceptedFiles.length > 0) {
        uploadColorMappingsMutation.mutate(acceptedFiles[0]);
      }
    },
    [uploadColorMappingsMutation],
  );

  const {
    getRootProps: getColorMappingRootProps,
    getInputProps: getColorMappingInputProps,
    isDragActive: isColorMappingDragActive,
  } = useDropzone({
    onDrop: onColorMappingDrop,
    accept: {
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [
        ".xlsx",
      ],
      "application/vnd.ms-excel": [".xls"],
      "text/csv": [".csv"],
    },
    multiple: false,
  });

  const handleAddColorMapping = () => {
    if (newBadColor.trim() && newGoodColor.trim()) {
      addColorMappingMutation.mutate({
        badColor: newBadColor.trim(),
        goodColor: newGoodColor.trim(),
      });
    }
  };

  const createRuleMutation = useMutation({
    mutationFn: async (data: any) => {
      const res = await fetch("/api/rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("Failed to create rule");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["variant-rules"] });
      toast({ title: "Success", description: "Rule created successfully" });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const updateRuleMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) => {
      const res = await fetch(`/api/rules/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("Failed to update rule");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["variant-rules"] });
      toast({ title: "Success", description: "Rule updated successfully" });
    },
  });

  const deleteRuleMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/rules/${id}`, { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to delete rule");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["variant-rules"] });
      toast({ title: "Success", description: "Rule deleted" });
    },
  });

  const createMetafieldRuleMutation = useMutation({
    mutationFn: async (data: any) => {
      const res = await fetch("/api/shopify-metafield-rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("Failed to create Shopify metafield rule");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["shopify-metafield-rules"] });
      toast({
        title: "Success",
        description: "Shopify metafield rule created",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const updateMetafieldRuleMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) => {
      const res = await fetch(`/api/shopify-metafield-rules/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("Failed to update Shopify metafield rule");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["shopify-metafield-rules"] });
      toast({
        title: "Success",
        description: "Shopify metafield rule updated",
      });
    },
  });

  const deleteMetafieldRuleMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/shopify-metafield-rules/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) throw new Error("Failed to delete Shopify metafield rule");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["shopify-metafield-rules"] });
      toast({
        title: "Success",
        description: "Shopify metafield rule deleted",
      });
    },
  });

  const updateDataSourceMutation = useMutation({
    mutationFn: async ({
      id,
      data,
    }: {
      id: string;
      data: {
        continueSelling?: boolean | null;
        sourceType?: string;
        salesConfig?: { priceMultiplier?: number; useCompareAtPrice?: boolean };
        regularPriceConfig?: {
          useFilePrice?: boolean;
          priceMultiplier?: number;
        };
        linkedDataSourceId?: string | null;
        assignedSaleDataSourceId?: string | null;
        variantSyncConfig?: {
          enableVariantCreation?: boolean;
          allowZeroStockCreation?: boolean;
          enableVariantDeletion?: boolean;
          maxDeletionLimit?: number;
          deleteAction?: "delete" | "zero_out";
          useExactSkuMatching?: boolean;
        };
        columnMapping?: {
          sku?: string;
          style?: string;
          size?: string;
          color?: string;
          stock?: string;
          cost?: string;
          price?: string;
          shipDate?: string;
        };
        filterZeroStock?: boolean;
        sizeLimitConfig?: {
          enabled?: boolean;
          minSize?: string | null;
          maxSize?: string | null;
          prefixOverrides?: Array<{
            pattern: string;
            minSize?: string | null;
            maxSize?: string | null;
          }>;
        };
        importValidationConfig?: {
          enabled?: boolean;
          minRowCount?: number | null;
          maxRowCount?: number | null;
          rowCountTolerance?: number;
          requireAllColumns?: boolean;
          sendImmediateAlert?: boolean;
        };
        autoSyncToShopify?: boolean;
        shopifyStoreId?: string | null;
        requireSaleImportFirst?: boolean | null;
        priceBasedExpansionConfig?: {
          enabled?: boolean;
          tiers?: Array<{
            minPrice: number;
            expandDown: number;
            expandUp: number;
          }>;
          defaultExpandDown?: number;
          defaultExpandUp?: number;
          expandedStock?: number;
        };
      };
    }) => {
      const res = await fetch(`/api/data-sources/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("Failed to update data source");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
      toast({ title: "Success", description: "Data source setting updated" });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const resetForm = () => {
    setEditingRule(null);
    setRuleName("");
    setSizesList("");
    setColorsList("");
    setExpandSizes(false);
    setExpandDownCount("2");
    setExpandUpCount("1");
    setMinTriggerStock("1");
    setExpandedStock("1");
  };

  const resetMetafieldForm = () => {
    setEditingMetafieldRule(null);
    setMetafieldRuleName("");
    setMetafieldNamespace("my_fields");
    setMetafieldKey("stock_info");
    setStockThreshold("0");
    setInStockMessage("");
    setSizeExpansionMessage("");
    setOutOfStockMessage("");
    setFutureDateMessage("");
    setDateOffsetDays("0");
  };

  const handleOpenMetafieldCreate = () => {
    resetMetafieldForm();
    setIsMetafieldDialogOpen(true);
  };

  const handleEditMetafieldRule = (rule: ShopifyMetafieldRule) => {
    setEditingMetafieldRule(rule);
    setMetafieldRuleName(rule.name);
    setMetafieldNamespace(rule.metafieldNamespace);
    setMetafieldKey(rule.metafieldKey);
    setStockThreshold(rule.stockThreshold?.toString() || "0");
    setInStockMessage(rule.inStockMessage);
    setSizeExpansionMessage(rule.sizeExpansionMessage || "");
    setOutOfStockMessage(rule.outOfStockMessage);
    setFutureDateMessage(rule.futureDateMessage || "");
    setDateOffsetDays(rule.dateOffsetDays?.toString() || "0");
    setIsMetafieldDialogOpen(true);
  };

  const handleDeleteMetafieldRule = (id: string) => {
    if (
      window.confirm(
        "Are you sure you want to delete this Shopify metafield rule?",
      )
    ) {
      deleteMetafieldRuleMutation.mutate(id);
    }
  };

  const handleToggleMetafieldActive = (rule: ShopifyMetafieldRule) => {
    updateMetafieldRuleMutation.mutate({
      id: rule.id,
      data: { enabled: !rule.enabled },
    });
  };

  const handleSaveMetafieldRule = (e: React.FormEvent) => {
    e.preventDefault();

    if (!selectedDataSourceId) {
      toast({
        title: "Error",
        description: "Please select a data source first",
        variant: "destructive",
      });
      return;
    }

    if (!inStockMessage || !outOfStockMessage) {
      toast({
        title: "Error",
        description: "Please enter both in-stock and out-of-stock messages",
        variant: "destructive",
      });
      return;
    }

    const ruleData = {
      name: metafieldRuleName || `${metafieldNamespace}.${metafieldKey} Rule`,
      dataSourceId: selectedDataSourceId,
      metafieldNamespace,
      metafieldKey,
      stockThreshold: parseInt(stockThreshold) || 0,
      inStockMessage,
      sizeExpansionMessage: sizeExpansionMessage || null,
      outOfStockMessage,
      futureDateMessage: futureDateMessage || null,
      dateOffsetDays: parseInt(dateOffsetDays) || 0,
      enabled: true,
    };

    if (editingMetafieldRule) {
      updateMetafieldRuleMutation.mutate({
        id: editingMetafieldRule.id,
        data: ruleData,
      });
    } else {
      createMetafieldRuleMutation.mutate(ruleData);
    }

    setIsMetafieldDialogOpen(false);
    resetMetafieldForm();
  };

  const handleOpenCreate = () => {
    resetForm();
    setIsDialogOpen(true);
  };

  const handleEditRule = (rule: VariantRule) => {
    setEditingRule(rule);
    setRuleName(rule.name);
    setSizesList(rule.sizes?.join(", ") || "");
    setColorsList(rule.colors?.join(", ") || "");
    setExpandSizes(rule.expandSizes || false);
    setExpandDownCount(rule.expandDownCount?.toString() || "2");
    setExpandUpCount(rule.expandUpCount?.toString() || "1");
    setMinTriggerStock(rule.minTriggerStock?.toString() || "1");
    setExpandedStock(rule.expandedStock?.toString() || "1");
    setIsDialogOpen(true);
  };

  const handleDeleteRule = (id: string) => {
    if (window.confirm("Are you sure you want to delete this rule?")) {
      deleteRuleMutation.mutate(id);
    }
  };

  const handleToggleActive = (rule: VariantRule) => {
    updateRuleMutation.mutate({
      id: rule.id,
      data: { enabled: !rule.enabled },
    });
  };

  const handleSaveRule = (e: React.FormEvent) => {
    e.preventDefault();

    if (!selectedDataSourceId) {
      toast({
        title: "Error",
        description: "Please select a data source first",
        variant: "destructive",
      });
      return;
    }

    const ruleData = {
      name: ruleName,
      dataSourceId: selectedDataSourceId,
      stockMin: null,
      stockMax: null,
      sizes: sizesList ? sizesList.split(",").map((s) => s.trim()) : [],
      colors: colorsList ? colorsList.split(",").map((c) => c.trim()) : [],
      expandSizes,
      expandDownCount: expandDownCount ? parseInt(expandDownCount) : 0,
      expandUpCount: expandUpCount ? parseInt(expandUpCount) : 0,
      minTriggerStock: minTriggerStock ? parseInt(minTriggerStock) : 1,
      expandedStock: expandedStock ? parseInt(expandedStock) : 1,
      enabled: true,
    };

    if (editingRule) {
      updateRuleMutation.mutate({ id: editingRule.id, data: ruleData });
    } else {
      createRuleMutation.mutate(ruleData);
    }

    setIsDialogOpen(false);
    resetForm();
  };

  const selectedDataSource = dataSources.find(
    (ds) => ds.id === selectedDataSourceId,
  );

  if (!selectedDataSourceId) {
    return (
      <Card className="mt-8">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5" />
            Rule Engine
          </CardTitle>
          <CardDescription>
            Select a data source above to view and manage its automation rules.
          </CardDescription>
        </CardHeader>
        <CardContent className="text-center py-8 text-muted-foreground">
          <FileSpreadsheet className="h-12 w-12 mx-auto mb-4 opacity-50" />
          <p>Choose a company/data source to configure processing rules.</p>
        </CardContent>
      </Card>
    );
  }

  const [copyFromDialogOpen, setCopyFromDialogOpen] = useState(false);
  const [copyFromSourceId, setCopyFromSourceId] = useState<string>("");

  const duplicateRulesMutation = useMutation({
    mutationFn: async ({
      sourceId,
      targetId,
    }: {
      sourceId: string;
      targetId: string;
    }) => {
      const res = await fetch(
        `/api/data-sources/${sourceId}/duplicate-rules/${targetId}`,
        {
          method: "POST",
        },
      );
      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || "Failed to copy rules");
      }
      return res.json();
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({
        queryKey: ["variant-rules", selectedDataSourceId],
      });
      queryClient.invalidateQueries({
        queryKey: ["shopify-metafield-rules", selectedDataSourceId],
      });
      toast({ title: "Success", description: data.message });
      setCopyFromDialogOpen(false);
      setCopyFromSourceId("");
    },
    onError: (error: Error) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const otherDataSources = dataSources.filter(
    (ds) => ds.id !== selectedDataSourceId,
  );

  return (
    <div className="mt-8 space-y-6">
      <Separator />
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-display font-bold text-foreground">
            Rule Engine
          </h2>
          <p className="text-muted-foreground">
            Manage automation rules for inventory file processing. Rules are
            specific to each data source.
          </p>
        </div>
      </div>

      {/* Shopify Inventory Settings */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-lg">
            <ShoppingBag className="h-5 w-5" />
            Shopify Inventory Settings
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Source Type Selection */}
          <div className="space-y-3">
            <Label className="font-medium">Data Source Type</Label>
            <p className="text-sm text-muted-foreground">
              Choose whether this is a regular inventory file or a sales file
              with special pricing.
            </p>
            <RadioGroup
              value={selectedDataSource?.sourceType || "inventory"}
              onValueChange={(value) => {
                if (selectedDataSourceId) {
                  updateDataSourceMutation.mutate({
                    id: selectedDataSourceId,
                    data: {
                      sourceType: value,
                      salesConfig:
                        value === "sales"
                          ? { priceMultiplier: 1, useCompareAtPrice: true }
                          : undefined,
                    },
                  });
                }
              }}
              className="flex gap-4"
              data-testid="radio-source-type"
            >
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="inventory" id="source-inventory" />
                <Label htmlFor="source-inventory" className="cursor-pointer">
                  Regular Inventory
                </Label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="sales" id="source-sales" />
                <Label htmlFor="source-sales" className="cursor-pointer">
                  Sales File
                </Label>
              </div>
            </RadioGroup>
          </div>

          {/* Sales Configuration (only visible when sourceType is 'sales') */}
          {selectedDataSource?.sourceType === "sales" && (
            <div className="space-y-4 p-4 border rounded-lg bg-amber-50/50 dark:bg-amber-950/20">
              <div className="flex items-center gap-2 text-amber-700 dark:text-amber-400">
                <Tag className="h-4 w-4" />
                <span className="font-medium">Sales File Configuration</span>
              </div>

              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div>
                    <Label className="font-medium">Price Multiplier</Label>
                    <p className="text-sm text-muted-foreground">
                      Multiply the sale price from the file by this value for
                      Shopify selling price.
                    </p>
                  </div>
                  <Select
                    value={String(
                      selectedDataSource?.salesConfig?.priceMultiplier || 1,
                    )}
                    onValueChange={(value) => {
                      if (selectedDataSourceId) {
                        updateDataSourceMutation.mutate({
                          id: selectedDataSourceId,
                          data: {
                            salesConfig: {
                              ...selectedDataSource?.salesConfig,
                              priceMultiplier: Number(value),
                            },
                          },
                        });
                      }
                    }}
                  >
                    <SelectTrigger
                      className="w-24"
                      data-testid="select-price-multiplier"
                    >
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="1">1x</SelectItem>
                      <SelectItem value="1.5">1.5x</SelectItem>
                      <SelectItem value="2">2x</SelectItem>
                      <SelectItem value="2.5">2.5x</SelectItem>
                      <SelectItem value="3">3x</SelectItem>
                      <SelectItem value="3.5">3.5x</SelectItem>
                      <SelectItem value="4">4x</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex items-center justify-between">
                  <div>
                    <Label className="font-medium">Set Compare-at Price</Label>
                    <p className="text-sm text-muted-foreground">
                      Use the retail price as the compare-at (strikethrough)
                      price on Shopify.
                    </p>
                  </div>
                  <Switch
                    checked={
                      selectedDataSource?.salesConfig?.useCompareAtPrice ?? true
                    }
                    onCheckedChange={(checked) => {
                      if (selectedDataSourceId) {
                        updateDataSourceMutation.mutate({
                          id: selectedDataSourceId,
                          data: {
                            salesConfig: {
                              ...selectedDataSource?.salesConfig,
                              useCompareAtPrice: checked,
                            },
                          },
                        });
                      }
                    }}
                    data-testid="switch-compare-at-price"
                  />
                </div>

                <div className="p-3 bg-amber-100/50 dark:bg-amber-900/30 rounded border border-amber-200 dark:border-amber-800">
                  <p className="text-sm text-amber-800 dark:text-amber-300">
                    <strong>How to Use Sale Files:</strong>
                  </p>
                  <ul className="text-sm text-amber-800 dark:text-amber-300 mt-2 space-y-1 list-disc list-inside">
                    <li>
                      Assign this sale file to a regular data source in the
                      regular file's settings
                    </li>
                    <li>
                      When syncing the regular file, products in BOTH files use
                      sale pricing
                    </li>
                    <li>
                      Products only in the sale file also get sale pricing
                    </li>
                    <li>
                      Products only in the regular file keep their regular
                      pricing
                    </li>
                  </ul>
                </div>
              </div>
            </div>
          )}

          {/* Regular Inventory Pricing Configuration (only visible when sourceType is 'inventory' or undefined) */}
          {(selectedDataSource?.sourceType === "inventory" ||
            !selectedDataSource?.sourceType) && (
            <div className="space-y-4 p-4 border rounded-lg bg-blue-50/50 dark:bg-blue-950/20">
              <div className="flex items-center gap-2 text-blue-700 dark:text-blue-400">
                <DollarSign className="h-4 w-4" />
                <span className="font-medium">
                  Regular File Pricing (Optional)
                </span>
              </div>

              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div>
                    <Label className="font-medium">Use Price from File</Label>
                    <p className="text-sm text-muted-foreground">
                      Use the price column from the import file instead of
                      keeping Shopify's existing price.
                    </p>
                  </div>
                  <Switch
                    checked={
                      selectedDataSource?.regularPriceConfig?.useFilePrice ??
                      false
                    }
                    onCheckedChange={(checked) => {
                      if (selectedDataSourceId) {
                        updateDataSourceMutation.mutate({
                          id: selectedDataSourceId,
                          data: {
                            regularPriceConfig: {
                              ...selectedDataSource?.regularPriceConfig,
                              useFilePrice: checked,
                              priceMultiplier: checked
                                ? selectedDataSource?.regularPriceConfig
                                    ?.priceMultiplier || 1
                                : undefined,
                            },
                          },
                        });
                      }
                    }}
                    data-testid="switch-use-file-price"
                  />
                </div>

                {selectedDataSource?.regularPriceConfig?.useFilePrice && (
                  <div className="flex items-center justify-between pl-4 border-l-2 border-blue-300 dark:border-blue-700">
                    <div>
                      <Label className="font-medium">Price Multiplier</Label>
                      <p className="text-sm text-muted-foreground">
                        Multiply the file price by this value (e.g., 1.5 = 50%
                        markup).
                      </p>
                    </div>
                    <Select
                      value={String(
                        selectedDataSource?.regularPriceConfig
                          ?.priceMultiplier || 1,
                      )}
                      onValueChange={(value) => {
                        if (selectedDataSourceId) {
                          updateDataSourceMutation.mutate({
                            id: selectedDataSourceId,
                            data: {
                              regularPriceConfig: {
                                ...selectedDataSource?.regularPriceConfig,
                                priceMultiplier: Number(value),
                              },
                            },
                          });
                        }
                      }}
                    >
                      <SelectTrigger
                        className="w-24"
                        data-testid="select-regular-price-multiplier"
                      >
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="1">1x</SelectItem>
                        <SelectItem value="1.25">1.25x</SelectItem>
                        <SelectItem value="1.5">1.5x</SelectItem>
                        <SelectItem value="1.75">1.75x</SelectItem>
                        <SelectItem value="2">2x</SelectItem>
                        <SelectItem value="2.25">2.25x</SelectItem>
                        <SelectItem value="2.5">2.5x</SelectItem>
                        <SelectItem value="2.75">2.75x</SelectItem>
                        <SelectItem value="3">3x</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                )}

                <div className="p-3 bg-blue-100/50 dark:bg-blue-900/30 rounded border border-blue-200 dark:border-blue-800">
                  <p className="text-sm text-blue-800 dark:text-blue-300">
                    <strong>Default Behavior:</strong> When disabled, regular
                    inventory files keep the existing Shopify price. Enable this
                    if your import file contains accurate pricing that should
                    override Shopify.
                  </p>
                </div>
              </div>
            </div>
          )}

          <Separator />

          {/* Continue Selling Toggle */}
          <div className="flex items-center justify-between">
            <div>
              <Label className="font-medium">
                Continue selling when out of stock
              </Label>
              <p className="text-sm text-muted-foreground">
                Allow customers to purchase products from this data source even
                when inventory is zero.
              </p>
            </div>
            <Switch
              checked={selectedDataSource?.continueSelling ?? true}
              onCheckedChange={(checked) => {
                if (selectedDataSourceId) {
                  updateDataSourceMutation.mutate({
                    id: selectedDataSourceId,
                    data: { continueSelling: checked },
                  });
                }
              }}
              data-testid="switch-continue-selling"
            />
          </div>

          {/* Filter Zero Stock Toggle */}
          <div className="flex items-center justify-between">
            <div>
              <Label className="font-medium">
                Filter out zero-stock items during import
              </Label>
              <p className="text-sm text-muted-foreground">
                Remove items with zero stock before applying rules. Only items
                with actual inventory will be imported.
              </p>
            </div>
            <Switch
              checked={selectedDataSource?.filterZeroStock ?? false}
              onCheckedChange={(checked) => {
                if (selectedDataSourceId) {
                  updateDataSourceMutation.mutate({
                    id: selectedDataSourceId,
                    data: { filterZeroStock: checked },
                  });
                }
              }}
              data-testid="switch-filter-zero-stock"
            />
          </div>

          {selectedDataSource?.filterZeroStock && (
            <div className="flex items-center justify-between pl-4 border-l-2 border-amber-400">
              <div>
                <Label className="font-medium">
                  Also filter zero stock with future dates
                </Label>
                <p className="text-sm text-muted-foreground">
                  Remove zero-stock items even if they have a future ship date.
                  Use this for vendors that put dates on all items (not just pre-orders).
                </p>
              </div>
              <Switch
                checked={(selectedDataSource as any)?.filterZeroStockWithFutureDates ?? false}
                onCheckedChange={(checked) => {
                  if (selectedDataSourceId) {
                    updateDataSourceMutation.mutate({
                      id: selectedDataSourceId,
                      data: { filterZeroStockWithFutureDates: checked },
                    });
                  }
                }}
                data-testid="switch-filter-zero-stock-future-dates"
              />
            </div>
          )}

          <Separator />

          {/* Auto Sync to Shopify */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <div>
                <Label className="font-medium flex items-center gap-2">
                  <RefreshCw className="h-4 w-4 text-green-600" />
                  Auto Sync to Shopify
                </Label>
                <p className="text-sm text-muted-foreground">
                  Automatically sync this data source to Shopify after each
                  import completes.
                </p>
              </div>
              <Switch
                checked={selectedDataSource?.autoSyncToShopify ?? false}
                onCheckedChange={(checked) => {
                  if (selectedDataSourceId) {
                    updateDataSourceMutation.mutate({
                      id: selectedDataSourceId,
                      data: { autoSyncToShopify: checked },
                    });
                  }
                }}
                data-testid="switch-auto-sync-shopify"
              />
            </div>

            {selectedDataSource?.autoSyncToShopify && (
              <div className="pl-6 space-y-2">
                <Label className="text-sm">Sync to Shopify Store</Label>
                <Select
                  value={selectedDataSource?.shopifyStoreId || "none"}
                  onValueChange={(value) => {
                    if (selectedDataSourceId) {
                      updateDataSourceMutation.mutate({
                        id: selectedDataSourceId,
                        data: {
                          shopifyStoreId: value === "none" ? null : value,
                        },
                      });
                    }
                  }}
                >
                  <SelectTrigger
                    className="w-full"
                    data-testid="select-shopify-store-sync"
                  >
                    <SelectValue placeholder="Select a Shopify store..." />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">Select a store...</SelectItem>
                    {shopifyStores.map((store) => (
                      <SelectItem key={store.id} value={store.id}>
                        {store.name || store.domain}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {selectedDataSource?.autoSyncToShopify &&
                  !selectedDataSource?.shopifyStoreId && (
                    <p className="text-xs text-amber-600">
                      Please select a Shopify store for auto-sync to work.
                    </p>
                  )}

                {/* Sync Status Indicator */}
                {syncStatus &&
                  (syncStatus.status === "running" ||
                    syncStatus.status === "queued") && (
                    <div className="mt-3 p-3 bg-blue-50 dark:bg-blue-950/30 rounded border border-blue-200 dark:border-blue-800">
                      <div className="flex items-center gap-2 mb-2">
                        <RefreshCw className="h-4 w-4 animate-spin text-blue-600" />
                        <span className="text-sm font-medium text-blue-800 dark:text-blue-200">
                          {syncStatus.status === "queued"
                            ? "Sync Queued"
                            : "Syncing to Shopify..."}
                        </span>
                      </div>
                      {syncStatus.progress &&
                        syncStatus.status === "running" && (
                          <>
                            <Progress
                              value={
                                syncStatus.progress.total > 0
                                  ? (syncStatus.progress.current /
                                      syncStatus.progress.total) *
                                    100
                                  : 0
                              }
                              className="h-2 mb-1"
                            />
                            <p className="text-xs text-blue-600 dark:text-blue-400">
                              {syncStatus.progress.phase}:{" "}
                              {syncStatus.progress.current.toLocaleString()} /{" "}
                              {syncStatus.progress.total.toLocaleString()}
                            </p>
                          </>
                        )}
                    </div>
                  )}

                {syncStatus?.status === "completed" &&
                  syncStatus.lastSyncAt && (
                    <p className="text-xs text-green-600 mt-2">
                      Last synced: {formatDatePST(syncStatus.lastSyncAt)}
                    </p>
                  )}

                {syncStatus?.status === "failed" && syncStatus.lastError && (
                  <div className="mt-2 p-2 bg-red-50 dark:bg-red-950/30 rounded border border-red-200 dark:border-red-800">
                    <p className="text-xs text-red-600 dark:text-red-400">
                      Last sync failed: {syncStatus.lastError}
                    </p>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Assigned Sale File - only show for regular (non-sales) data sources */}
          {selectedDataSource?.sourceType !== "sales" &&
            (() => {
              // Calculate auto-link status based on name pattern
              const currentName = selectedDataSource?.name || "";
              const expectedSaleName1 = `${currentName} Sale`;
              const expectedSaleName2 = `${currentName} Sales`;
              const autoLinkedSaleFile = dataSources?.find(
                (ds: DataSource) =>
                  ds.id !== selectedDataSourceId &&
                  (ds.name === expectedSaleName1 ||
                    ds.name === expectedSaleName2),
              );

              // Get the currently assigned sale file
              const assignedSaleFile =
                selectedDataSource?.assignedSaleDataSourceId
                  ? dataSources?.find(
                      (ds: DataSource) =>
                        ds.id === selectedDataSource.assignedSaleDataSourceId,
                    )
                  : null;

              // Determine link status
              const isAutoLinked =
                autoLinkedSaleFile &&
                selectedDataSource?.assignedSaleDataSourceId ===
                  autoLinkedSaleFile.id;
              const isManuallyLinked = assignedSaleFile && !isAutoLinked;
              const hasLinkError =
                selectedDataSource?.assignedSaleDataSourceId &&
                !assignedSaleFile;
              const linkedToWrongType =
                assignedSaleFile && assignedSaleFile.sourceType !== "sales";

              return (
                <>
                  <Separator />
                  <div className="space-y-3">
                    <div className="flex items-center gap-2">
                      <Tag className="h-4 w-4 text-orange-600" />
                      <Label className="font-medium">Assigned Sale File</Label>
                      {isAutoLinked && (
                        <Badge
                          variant="outline"
                          className="text-green-600 border-green-300 bg-green-50 dark:bg-green-950/30"
                        >
                          <CheckCircle className="h-3 w-3 mr-1" />
                          Auto-linked
                        </Badge>
                      )}
                      {isManuallyLinked && (
                        <Badge
                          variant="outline"
                          className="text-blue-600 border-blue-300 bg-blue-50 dark:bg-blue-950/30"
                        >
                          Manual
                        </Badge>
                      )}
                    </div>

                    {/* Error states */}
                    {hasLinkError && (
                      <div className="p-3 bg-red-100/50 dark:bg-red-900/30 rounded border border-red-200 dark:border-red-800 flex items-start gap-2">
                        <AlertTriangle className="h-4 w-4 text-red-600 mt-0.5 flex-shrink-0" />
                        <div>
                          <p className="text-sm font-medium text-red-800 dark:text-red-300">
                            Linked sale file not found
                          </p>
                          <p className="text-xs text-red-600 dark:text-red-400">
                            The assigned sale file no longer exists. Please
                            select a different one.
                          </p>
                        </div>
                      </div>
                    )}

                    {linkedToWrongType && (
                      <div className="p-3 bg-amber-100/50 dark:bg-amber-900/30 rounded border border-amber-200 dark:border-amber-800 flex items-start gap-2">
                        <AlertTriangle className="h-4 w-4 text-amber-600 mt-0.5 flex-shrink-0" />
                        <div>
                          <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
                            Linked to non-sale data source
                          </p>
                          <p className="text-xs text-amber-600 dark:text-amber-400">
                            "{assignedSaleFile?.name}" is not configured as a
                            sale file. Change its Data Type to "Sale File" for
                            proper pricing.
                          </p>
                        </div>
                      </div>
                    )}

                    {/* Auto-link suggestion */}
                    {!selectedDataSource?.assignedSaleDataSourceId &&
                      autoLinkedSaleFile && (
                        <div className="p-3 bg-green-100/50 dark:bg-green-900/30 rounded border border-green-200 dark:border-green-800 flex items-start gap-2">
                          <Sparkles className="h-4 w-4 text-green-600 mt-0.5 flex-shrink-0" />
                          <div className="flex-1">
                            <p className="text-sm font-medium text-green-800 dark:text-green-300">
                              Auto-link available
                            </p>
                            <p className="text-xs text-green-600 dark:text-green-400">
                              Found matching sale file: "
                              {autoLinkedSaleFile.name}"
                            </p>
                            <Button
                              variant="outline"
                              size="sm"
                              className="mt-2 border-green-300 text-green-700 dark:text-green-300 hover:bg-green-50"
                              onClick={() => {
                                if (selectedDataSourceId) {
                                  updateDataSourceMutation.mutate({
                                    id: selectedDataSourceId,
                                    data: {
                                      assignedSaleDataSourceId:
                                        autoLinkedSaleFile.id,
                                    },
                                  });
                                }
                              }}
                            >
                              <LinkIcon className="h-3 w-3 mr-1" />
                              Link to {autoLinkedSaleFile.name}
                            </Button>
                          </div>
                        </div>
                      )}

                    <p className="text-sm text-muted-foreground">
                      {isAutoLinked
                        ? `Automatically linked based on name pattern (${currentName} → ${assignedSaleFile?.name}).`
                        : "Assign a sale file to this data source. Discontinued styles from sale files won't appear in regular inventory."}
                    </p>
                    <Select
                      value={
                        selectedDataSource?.assignedSaleDataSourceId || "none"
                      }
                      onValueChange={(value) => {
                        if (selectedDataSourceId) {
                          updateDataSourceMutation.mutate({
                            id: selectedDataSourceId,
                            data: {
                              assignedSaleDataSourceId:
                                value === "none" ? null : value,
                            },
                          });
                        }
                      }}
                    >
                      <SelectTrigger
                        className="w-full"
                        data-testid="select-assigned-sale-file"
                      >
                        <SelectValue placeholder="Select a sale file..." />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="none">
                          No sale file assigned
                        </SelectItem>
                        {dataSources
                          ?.filter(
                            (ds: DataSource) => ds.id !== selectedDataSourceId,
                          )
                          .map((ds: DataSource) => (
                            <SelectItem key={ds.id} value={ds.id}>
                              {ds.name}{" "}
                              {ds.sourceType === "sales" ? "(Sale)" : ""}
                              {ds.id === autoLinkedSaleFile?.id &&
                                " ✓ Auto-match"}
                            </SelectItem>
                          ))}
                      </SelectContent>
                    </Select>

                    {selectedDataSource?.assignedSaleDataSourceId && (
                      <>
                        {/* Require Sale Import First toggle */}
                        <div className="flex items-center justify-between p-3 border rounded-lg">
                          <div>
                            <Label className="font-medium">
                              Require sale file import first
                            </Label>
                            <p className="text-sm text-muted-foreground">
                              Show a warning if importing this file before the
                              linked sale file has been imported.
                            </p>
                          </div>
                          <Switch
                            checked={
                              selectedDataSource?.requireSaleImportFirst !==
                              false
                            }
                            onCheckedChange={(checked) => {
                              if (selectedDataSourceId) {
                                updateDataSourceMutation.mutate({
                                  id: selectedDataSourceId,
                                  data: { requireSaleImportFirst: checked },
                                });
                              }
                            }}
                            data-testid="switch-require-sale-import-first"
                          />
                        </div>

                        <div className="p-3 bg-orange-100/50 dark:bg-orange-900/30 rounded border border-orange-200 dark:border-orange-800">
                          <p className="text-sm text-orange-800 dark:text-orange-300">
                            <strong>How it works:</strong>
                          </p>
                          <ul className="text-sm text-orange-800 dark:text-orange-300 mt-2 space-y-1 list-disc list-inside">
                            <li>
                              <strong>Sale file import:</strong> Registers
                              discontinued styles automatically
                            </li>
                            <li>
                              <strong>Regular file import:</strong> Filters out
                              discontinued styles - they won't appear in
                              inventory
                            </li>
                            <li>
                              <strong>Sync:</strong> Sale items use sale pricing
                              with multiplier, regular items use standard
                              pricing
                            </li>
                          </ul>
                          <p className="text-xs text-orange-600 dark:text-orange-400 mt-2">
                            To activate: Import your sale file first, then
                            re-import the regular file. Discontinued styles will
                            be automatically filtered.
                          </p>
                        </div>
                      </>
                    )}
                  </div>
                </>
              );
            })()}

          <Separator />

          {/* Variant Sync Settings */}
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <GitBranch className="h-4 w-4 text-purple-600" />
              <Label className="font-medium">Variant Sync Settings</Label>
            </div>
            <p className="text-sm text-muted-foreground">
              Control how variants are automatically created or deleted during
              Shopify sync for this data source.
            </p>

            {/* Enable Variant Creation */}
            <div className="flex items-center justify-between p-3 border rounded-lg">
              <div>
                <Label className="font-medium">Auto-create new variants</Label>
                <p className="text-sm text-muted-foreground">
                  Automatically create variants in Shopify when new sizes/colors
                  appear in inventory.
                </p>
              </div>
              <Switch
                checked={
                  selectedDataSource?.variantSyncConfig
                    ?.enableVariantCreation ?? false
                }
                onCheckedChange={(checked) => {
                  if (selectedDataSourceId) {
                    updateDataSourceMutation.mutate({
                      id: selectedDataSourceId,
                      data: {
                        variantSyncConfig: {
                          ...selectedDataSource?.variantSyncConfig,
                          enableVariantCreation: checked,
                        },
                      },
                    });
                  }
                }}
                data-testid="switch-variant-creation"
              />
            </div>

            {/* Allow Zero Stock Creation - Only show when variant creation is enabled */}
            {selectedDataSource?.variantSyncConfig?.enableVariantCreation && (
              <div className="flex items-center justify-between p-3 border rounded-lg bg-purple-50/50 dark:bg-purple-950/20 ml-4">
                <div>
                  <Label className="font-medium">
                    Allow zero stock variants (Special Orders)
                  </Label>
                  <p className="text-sm text-muted-foreground">
                    Create variants even when stock is 0 and no future ship
                    date. Use for vendors that accept special orders.
                  </p>
                </div>
                <Switch
                  checked={
                    selectedDataSource?.variantSyncConfig
                      ?.allowZeroStockCreation ?? false
                  }
                  onCheckedChange={(checked) => {
                    if (selectedDataSourceId) {
                      updateDataSourceMutation.mutate({
                        id: selectedDataSourceId,
                        data: {
                          variantSyncConfig: {
                            ...selectedDataSource?.variantSyncConfig,
                            allowZeroStockCreation: checked,
                          },
                        },
                      });
                    }
                  }}
                  data-testid="switch-allow-zero-stock"
                />
              </div>
            )}

            {/* Enable Variant Deletion */}
            <div className="flex items-center justify-between p-3 border rounded-lg">
              <div>
                <Label className="font-medium">
                  Auto-delete removed variants
                </Label>
                <p className="text-sm text-muted-foreground">
                  Remove variants from Shopify when they no longer appear in
                  inventory.
                </p>
              </div>
              <Switch
                checked={
                  selectedDataSource?.variantSyncConfig
                    ?.enableVariantDeletion ?? false
                }
                onCheckedChange={(checked) => {
                  if (selectedDataSourceId) {
                    updateDataSourceMutation.mutate({
                      id: selectedDataSourceId,
                      data: {
                        variantSyncConfig: {
                          ...selectedDataSource?.variantSyncConfig,
                          enableVariantDeletion: checked,
                        },
                      },
                    });
                  }
                }}
                data-testid="switch-variant-deletion"
              />
            </div>

            {/* Deletion Action & Max Limit - Only show when deletion is enabled */}
            {selectedDataSource?.variantSyncConfig?.enableVariantDeletion && (
              <div className="space-y-4 p-4 border rounded-lg bg-purple-50/50 dark:bg-purple-950/20">
                <div className="flex items-center justify-between">
                  <div>
                    <Label className="font-medium">Deletion Action</Label>
                    <p className="text-sm text-muted-foreground">
                      What to do with variants not in inventory.
                    </p>
                  </div>
                  <Select
                    value={
                      selectedDataSource?.variantSyncConfig?.deleteAction ||
                      "delete"
                    }
                    onValueChange={(value: "delete" | "zero_out") => {
                      if (selectedDataSourceId) {
                        updateDataSourceMutation.mutate({
                          id: selectedDataSourceId,
                          data: {
                            variantSyncConfig: {
                              ...selectedDataSource?.variantSyncConfig,
                              deleteAction: value,
                            },
                          },
                        });
                      }
                    }}
                  >
                    <SelectTrigger
                      className="w-40"
                      data-testid="select-delete-action"
                    >
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="delete">Delete variant</SelectItem>
                      <SelectItem value="zero_out">Zero out stock</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex items-center justify-between">
                  <div>
                    <Label className="font-medium">
                      Max Deletions per Sync
                    </Label>
                    <p className="text-sm text-muted-foreground">
                      Safety limit to prevent accidental mass deletions.
                    </p>
                  </div>
                  <Select
                    value={String(
                      selectedDataSource?.variantSyncConfig?.maxDeletionLimit ||
                        100,
                    )}
                    onValueChange={(value) => {
                      if (selectedDataSourceId) {
                        updateDataSourceMutation.mutate({
                          id: selectedDataSourceId,
                          data: {
                            variantSyncConfig: {
                              ...selectedDataSource?.variantSyncConfig,
                              maxDeletionLimit: Number(value),
                            },
                          },
                        });
                      }
                    }}
                  >
                    <SelectTrigger
                      className="w-28"
                      data-testid="select-max-deletions"
                    >
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="100">100</SelectItem>
                      <SelectItem value="200">200</SelectItem>
                      <SelectItem value="300">300</SelectItem>
                      <SelectItem value="400">400</SelectItem>
                      <SelectItem value="500">500</SelectItem>
                      <SelectItem value="1000">1,000</SelectItem>
                      <SelectItem value="1500">1,500</SelectItem>
                      <SelectItem value="2000">2,000</SelectItem>
                      <SelectItem value="2500">2,500</SelectItem>
                      <SelectItem value="3000">3,000</SelectItem>
                      <SelectItem value="4000">4,000</SelectItem>
                      <SelectItem value="5000">5,000</SelectItem>
                      <SelectItem value="10000">10,000</SelectItem>
                      <SelectItem value="25000">25,000</SelectItem>
                      <SelectItem value="50000">50,000</SelectItem>
                      <SelectItem value="100000">100,000</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                {/* Exact SKU Matching Toggle */}
                <div className="flex items-center justify-between">
                  <div>
                    <Label className="font-medium">Exact SKU Matching</Label>
                    <p className="text-sm text-muted-foreground">
                      Delete variants if SKU doesn't exactly match inventory
                      (stricter matching).
                    </p>
                  </div>
                  <Switch
                    checked={
                      selectedDataSource?.variantSyncConfig
                        ?.useExactSkuMatching ?? false
                    }
                    onCheckedChange={(checked) => {
                      if (selectedDataSourceId) {
                        updateDataSourceMutation.mutate({
                          id: selectedDataSourceId,
                          data: {
                            variantSyncConfig: {
                              ...selectedDataSource?.variantSyncConfig,
                              useExactSkuMatching: checked,
                            },
                          },
                        });
                      }
                    }}
                    data-testid="switch-exact-sku-matching"
                  />
                </div>

                <div className="p-3 bg-purple-100/50 dark:bg-purple-900/30 rounded border border-purple-200 dark:border-purple-800">
                  <p className="text-sm text-purple-800 dark:text-purple-300">
                    <strong>How it works:</strong> When sync runs, variants in
                    Shopify that don't exist in your inventory will be{" "}
                    {selectedDataSource?.variantSyncConfig?.deleteAction ===
                    "zero_out"
                      ? "set to 0 stock"
                      : "permanently deleted"}{" "}
                    (up to{" "}
                    {(
                      selectedDataSource?.variantSyncConfig?.maxDeletionLimit ||
                      100
                    ).toLocaleString()}{" "}
                    per sync).
                    {selectedDataSource?.variantSyncConfig
                      ?.useExactSkuMatching && (
                      <span className="block mt-2 font-medium text-orange-600 dark:text-orange-400">
                        ⚠️ Exact SKU Matching enabled: Variants will be removed
                        if their SKU doesn't exactly match an inventory SKU
                        (case-sensitive, no normalization).
                      </span>
                    )}
                  </p>
                </div>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Import Safety Settings */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-lg">
            <Shield className="h-5 w-5 text-red-600" />
            Import Safety Settings
          </CardTitle>
          <CardDescription>
            Protect your inventory from corrupted or malformed files. Files that
            fail validation will be blocked before import.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Enable Import Validation */}
          <div className="flex items-center justify-between">
            <div>
              <Label className="font-medium">Enable Import Validation</Label>
              <p className="text-sm text-muted-foreground">
                Validate files before import to prevent data corruption.
              </p>
            </div>
            <Switch
              checked={
                (selectedDataSource as any)?.importValidationConfig?.enabled !==
                false
              }
              onCheckedChange={(checked) => {
                if (selectedDataSourceId) {
                  updateDataSourceMutation.mutate({
                    id: selectedDataSourceId,
                    data: {
                      importValidationConfig: {
                        ...(selectedDataSource as any)?.importValidationConfig,
                        enabled: checked,
                      },
                    },
                  });
                }
              }}
              data-testid="switch-import-validation"
            />
          </div>

          {(selectedDataSource as any)?.importValidationConfig?.enabled !==
            false && (
            <div className="space-y-4 p-4 border rounded-lg bg-red-50/50 dark:bg-red-950/20">
              {/* Row Count Drop Protection */}
              <div className="flex items-center justify-between">
                <div>
                  <Label className="font-medium">Row Count Tolerance (%)</Label>
                  <p className="text-sm text-muted-foreground">
                    Block imports if row count changes more than this percentage
                    from the last successful import.
                  </p>
                </div>
                <Select
                  value={String(
                    (selectedDataSource as any)?.importValidationConfig
                      ?.rowCountTolerance || 0,
                  )}
                  onValueChange={(value) => {
                    if (selectedDataSourceId) {
                      updateDataSourceMutation.mutate({
                        id: selectedDataSourceId,
                        data: {
                          importValidationConfig: {
                            ...(selectedDataSource as any)
                              ?.importValidationConfig,
                            rowCountTolerance: Number(value),
                          },
                        },
                      });
                    }
                  }}
                >
                  <SelectTrigger
                    className="w-32"
                    data-testid="select-row-tolerance"
                  >
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="0">Disabled</SelectItem>
                    <SelectItem value="5">5%</SelectItem>
                    <SelectItem value="10">10%</SelectItem>
                    <SelectItem value="15">15%</SelectItem>
                    <SelectItem value="20">20%</SelectItem>
                    <SelectItem value="25">25%</SelectItem>
                    <SelectItem value="30">30%</SelectItem>
                    <SelectItem value="50">50%</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Minimum Row Count */}
              <div className="flex items-center justify-between">
                <div>
                  <Label className="font-medium">Minimum Row Count</Label>
                  <p className="text-sm text-muted-foreground">
                    Block imports with fewer rows than this threshold.
                  </p>
                </div>
                <Input
                  type="number"
                  className="w-32"
                  value={
                    (selectedDataSource as any)?.importValidationConfig
                      ?.minRowCount || ""
                  }
                  placeholder="None"
                  onChange={(e) => {
                    if (selectedDataSourceId) {
                      const value = e.target.value
                        ? Number(e.target.value)
                        : null;
                      updateDataSourceMutation.mutate({
                        id: selectedDataSourceId,
                        data: {
                          importValidationConfig: {
                            ...(selectedDataSource as any)
                              ?.importValidationConfig,
                            minRowCount: value,
                          },
                        },
                      });
                    }
                  }}
                  data-testid="input-min-rows"
                />
              </div>

              {/* Maximum Row Count */}
              <div className="flex items-center justify-between">
                <div>
                  <Label className="font-medium">Maximum Row Count</Label>
                  <p className="text-sm text-muted-foreground">
                    Block imports with more rows than this threshold.
                  </p>
                </div>
                <Input
                  type="number"
                  className="w-32"
                  value={
                    (selectedDataSource as any)?.importValidationConfig
                      ?.maxRowCount || ""
                  }
                  placeholder="None"
                  onChange={(e) => {
                    if (selectedDataSourceId) {
                      const value = e.target.value
                        ? Number(e.target.value)
                        : null;
                      updateDataSourceMutation.mutate({
                        id: selectedDataSourceId,
                        data: {
                          importValidationConfig: {
                            ...(selectedDataSource as any)
                              ?.importValidationConfig,
                            maxRowCount: value,
                          },
                        },
                      });
                    }
                  }}
                  data-testid="input-max-rows"
                />
              </div>

              {/* Require All Columns */}
              <div className="flex items-center justify-between">
                <div>
                  <Label className="font-medium">
                    Require All Mapped Columns
                  </Label>
                  <p className="text-sm text-muted-foreground">
                    Block imports if any mapped columns are missing from the
                    file.
                  </p>
                </div>
                <Switch
                  checked={
                    (selectedDataSource as any)?.importValidationConfig
                      ?.requireAllColumns ?? false
                  }
                  onCheckedChange={(checked) => {
                    if (selectedDataSourceId) {
                      updateDataSourceMutation.mutate({
                        id: selectedDataSourceId,
                        data: {
                          importValidationConfig: {
                            ...(selectedDataSource as any)
                              ?.importValidationConfig,
                            requireAllColumns: checked,
                          },
                        },
                      });
                    }
                  }}
                  data-testid="switch-require-columns"
                />
              </div>

              {/* Send Immediate Alert */}
              <div className="flex items-center justify-between">
                <div>
                  <Label className="font-medium">
                    Send Email Alert on Failure
                  </Label>
                  <p className="text-sm text-muted-foreground">
                    Send an immediate email alert when an import is blocked.
                  </p>
                </div>
                <Switch
                  checked={
                    (selectedDataSource as any)?.importValidationConfig
                      ?.sendImmediateAlert !== false
                  }
                  onCheckedChange={(checked) => {
                    if (selectedDataSourceId) {
                      updateDataSourceMutation.mutate({
                        id: selectedDataSourceId,
                        data: {
                          importValidationConfig: {
                            ...(selectedDataSource as any)
                              ?.importValidationConfig,
                            sendImmediateAlert: checked,
                          },
                        },
                      });
                    }
                  }}
                  data-testid="switch-import-alert"
                />
              </div>

              <div className="p-3 bg-red-100/50 dark:bg-red-900/30 rounded border border-red-200 dark:border-red-800">
                <p className="text-sm text-red-800 dark:text-red-300">
                  <strong>Default Protection:</strong> Even without custom
                  settings, files with more than 90% row count drop are
                  automatically blocked to prevent accidental data loss from
                  corrupted files.
                </p>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Color Mappings Section */}
      <Collapsible open={colorMappingsOpen} onOpenChange={setColorMappingsOpen}>
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="flex items-center gap-2 text-lg">
                <Palette className="h-5 w-5" />
                Global Color Mappings
              </CardTitle>
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm">
                  {colorMappingsOpen ? (
                    <ChevronDown className="h-4 w-4" />
                  ) : (
                    <ChevronRight className="h-4 w-4" />
                  )}
                </Button>
              </CollapsibleTrigger>
            </div>
            <CardDescription>
              Translate abbreviated or incorrect color names to standard names.
              Applied to all data sources.
              {colorMappings.length > 0 &&
                ` (${colorMappings.length} mappings)`}
            </CardDescription>
          </CardHeader>
          <CollapsibleContent>
            <CardContent className="space-y-4">
              <div
                {...getColorMappingRootProps()}
                className={`border-2 border-dashed rounded-lg p-4 text-center cursor-pointer transition-colors ${isColorMappingDragActive ? "border-primary bg-primary/5" : "border-muted-foreground/25 hover:border-primary/50"}`}
              >
                <input {...getColorMappingInputProps()} />
                <Upload className="h-6 w-6 mx-auto mb-2 text-muted-foreground" />
                <p className="text-sm text-muted-foreground">
                  Drop Excel file here or click to upload color mappings
                </p>
                <p className="text-xs text-muted-foreground mt-1">
                  File should have "bad_color" and "good_color" columns
                </p>
              </div>

              <div className="flex gap-2">
                <Input
                  placeholder="Bad color (e.g., BLK)"
                  value={newBadColor}
                  onChange={(e) => setNewBadColor(e.target.value)}
                  className="flex-1"
                />
                <ArrowRight className="h-4 w-4 self-center text-muted-foreground" />
                <Input
                  placeholder="Good color (e.g., Black)"
                  value={newGoodColor}
                  onChange={(e) => setNewGoodColor(e.target.value)}
                  className="flex-1"
                />
                <Button
                  onClick={handleAddColorMapping}
                  disabled={!newBadColor.trim() || !newGoodColor.trim()}
                >
                  <Plus className="h-4 w-4" />
                </Button>
              </div>

              {colorMappings.length > 0 && (
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">
                      {colorMappings.length} color mappings
                    </span>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-destructive hover:text-destructive"
                      onClick={() => {
                        if (window.confirm("Delete all color mappings?")) {
                          clearAllColorMappingsMutation.mutate();
                        }
                      }}
                    >
                      <Trash2 className="h-4 w-4 mr-1" />
                      Clear All
                    </Button>
                  </div>
                  <div className="max-h-40 overflow-y-auto border rounded-lg">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Bad Color</TableHead>
                          <TableHead>Good Color</TableHead>
                          <TableHead className="w-12"></TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {colorMappings.slice(0, 20).map((mapping) => (
                          <TableRow key={mapping.id}>
                            <TableCell className="font-mono text-sm">
                              {mapping.badColor}
                            </TableCell>
                            <TableCell>{mapping.goodColor}</TableCell>
                            <TableCell>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-6 w-6 text-destructive"
                                onClick={() =>
                                  deleteColorMappingMutation.mutate(mapping.id)
                                }
                              >
                                <Trash2 className="h-3 w-3" />
                              </Button>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                    {colorMappings.length > 20 && (
                      <div className="p-2 text-center text-xs text-muted-foreground bg-muted/30">
                        Showing 20 of {colorMappings.length} mappings
                      </div>
                    )}
                  </div>
                </div>
              )}
            </CardContent>
          </CollapsibleContent>
        </Card>
      </Collapsible>

      {/* Variant Rule Dialog */}
      <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
        <DialogContent className="sm:max-w-[600px] max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {editingRule ? "Edit Rule" : "Create New Automation Rule"}
            </DialogTitle>
            <DialogDescription>
              Rules apply only to products processed from inventory files.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleSaveRule} className="space-y-6 py-4">
            <div className="p-3 bg-blue-50 dark:bg-blue-900/30 text-blue-800 text-xs rounded-md flex items-center gap-2 border border-blue-100">
              <Sparkles className="h-4 w-4" />
              Variant expansion rules automatically create product variants
              based on stock levels.
            </div>

            <div className="space-y-2">
              <Label htmlFor="name">Rule Name</Label>
              <Input
                id="name"
                placeholder="e.g., Medium Stock Variant Expansion"
                value={ruleName}
                onChange={(e) => setRuleName(e.target.value)}
                required
              />
            </div>

            <div className="space-y-2">
              <Label>Generate Static Variants (Action)</Label>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label className="text-xs text-muted-foreground">
                    Sizes (comma-separated)
                  </Label>
                  <Input
                    placeholder="e.g., S, M, L, XL"
                    value={sizesList}
                    onChange={(e) => setSizesList(e.target.value)}
                  />
                </div>
                <div>
                  <Label className="text-xs text-muted-foreground">
                    Colors (comma-separated)
                  </Label>
                  <Input
                    placeholder="e.g., Red, Blue, Black"
                    value={colorsList}
                    onChange={(e) => setColorsList(e.target.value)}
                  />
                </div>
              </div>
            </div>

            <div className="border-t pt-4 mt-4">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <Label className="text-base font-semibold">
                    Automatic Size Expansion
                  </Label>
                  <p className="text-xs text-muted-foreground">
                    Auto-add adjacent sizes when item has stock
                  </p>
                </div>
                <Switch
                  checked={expandSizes}
                  onCheckedChange={setExpandSizes}
                />
              </div>

              {expandSizes && (
                <div className="space-y-4 p-4 bg-blue-50/50 rounded-lg border border-blue-100">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <Label className="text-xs">
                        Expand Down (smaller sizes)
                      </Label>
                      <Input
                        type="number"
                        min="0"
                        value={expandDownCount}
                        onChange={(e) => setExpandDownCount(e.target.value)}
                      />
                    </div>
                    <div>
                      <Label className="text-xs">
                        Expand Up (larger sizes)
                      </Label>
                      <Input
                        type="number"
                        min="0"
                        value={expandUpCount}
                        onChange={(e) => setExpandUpCount(e.target.value)}
                      />
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <Label className="text-xs">Min Trigger Stock</Label>
                      <Input
                        type="number"
                        min="1"
                        value={minTriggerStock}
                        onChange={(e) => setMinTriggerStock(e.target.value)}
                      />
                    </div>
                    <div>
                      <Label className="text-xs">Expanded Stock Value</Label>
                      <Input
                        type="number"
                        min="1"
                        value={expandedStock}
                        onChange={(e) => setExpandedStock(e.target.value)}
                      />
                    </div>
                  </div>
                </div>
              )}
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => setIsDialogOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit">
                {editingRule ? "Update Rule" : "Save Rule"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Metafield Rule Dialog */}
      <Dialog
        open={isMetafieldDialogOpen}
        onOpenChange={setIsMetafieldDialogOpen}
      >
        <DialogContent className="sm:max-w-[500px] max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {editingMetafieldRule
                ? "Edit Metafield Rule"
                : "Create Shopify Metafield Rule"}
            </DialogTitle>
            <DialogDescription>
              Set different messages for the variant metafield based on stock
              level.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleSaveMetafieldRule} className="space-y-4 py-4">
            <div className="p-3 bg-green-50 dark:bg-green-900/30 text-green-800 text-xs rounded-md flex items-center gap-2 border border-green-100">
              <ShoppingBag className="h-4 w-4" />
              This rule will be applied when syncing inventory to Shopify.
            </div>

            <div className="space-y-2">
              <Label htmlFor="metafield-name">Rule Name (optional)</Label>
              <Input
                id="metafield-name"
                placeholder="e.g., Stock Info Message"
                value={metafieldRuleName}
                onChange={(e) => setMetafieldRuleName(e.target.value)}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>Metafield Namespace</Label>
                <Input
                  value={metafieldNamespace}
                  onChange={(e) => setMetafieldNamespace(e.target.value)}
                  placeholder="my_fields"
                />
              </div>
              <div className="space-y-2">
                <Label>Metafield Key</Label>
                <Input
                  value={metafieldKey}
                  onChange={(e) => setMetafieldKey(e.target.value)}
                  placeholder="stock_info"
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label>Stock Threshold</Label>
              <Input
                type="number"
                value={stockThreshold}
                onChange={(e) => setStockThreshold(e.target.value)}
                placeholder="0"
              />
              <p className="text-xs text-muted-foreground">
                If stock is greater than this value, the "In Stock" message is
                used.
              </p>
            </div>

            <div className="space-y-2">
              <Label className="text-green-600">1. In Stock Message</Label>
              <Textarea
                value={inStockMessage}
                onChange={(e) => setInStockMessage(e.target.value)}
                placeholder="e.g., In Stock - Ships within 1-2 days"
                required
              />
            </div>

            <div className="space-y-2">
              <Label className="text-blue-600">
                2. Size Expansion Message (Optional)
              </Label>
              <Textarea
                value={sizeExpansionMessage}
                onChange={(e) => setSizeExpansionMessage(e.target.value)}
                placeholder="e.g., Available to Order - Ships in 1-2 weeks"
              />
            </div>

            <div className="space-y-2">
              <Label className="text-red-600">3. Out of Stock Message</Label>
              <Textarea
                value={outOfStockMessage}
                onChange={(e) => setOutOfStockMessage(e.target.value)}
                placeholder="e.g., Made to Order - Ships in 2-3 weeks"
                required
              />
            </div>

            <div className="space-y-2">
              <Label className="text-orange-600">
                4. Future Ship Date Message (Optional)
              </Label>
              <Textarea
                value={futureDateMessage}
                onChange={(e) => setFutureDateMessage(e.target.value)}
                placeholder="e.g., Ship Date - {date} #ff0000 (use {date} placeholder)"
              />
              <p className="text-xs text-muted-foreground">
                Use {"{"}
                <strong>date</strong>
                {"}"} as placeholder. Date comes from the Ship Date column in
                your Excel file.
              </p>
            </div>

            <div className="space-y-2">
              <Label>Date Offset (Days)</Label>
              <Input
                type="number"
                value={dateOffsetDays}
                onChange={(e) => setDateOffsetDays(e.target.value)}
                placeholder="0"
              />
              <p className="text-xs text-muted-foreground">
                Days to add to the ship date before displaying (e.g., 14 = 2
                weeks).
              </p>
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => setIsMetafieldDialogOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit">
                {editingMetafieldRule ? "Update Rule" : "Save Rule"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}

export default function Inventory() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const [activeTab, setActiveTab] = useState("upload");
  const [uploadMethod, setUploadMethod] = useState("manual");
  const [selectedDataSourceId, setSelectedDataSourceId] = useState<string>("");
  const [isSourceDialogOpen, setIsSourceDialogOpen] = useState(false);
  const [isAISourceDialogOpen, setIsAISourceDialogOpen] = useState(false);
  const [aiDialogDataSource, setAiDialogDataSource] =
    useState<DataSource | null>(null);
  const [editingSource, setEditingSource] = useState<DataSource | null>(null);
  const [aiMappingStatus, setAiMappingStatus] = useState("idle");
  const [testStatus, setTestStatus] = useState("idle");
  const [isPreviewOpen, setIsPreviewOpen] = useState(false);
  const [previewSource, setPreviewSource] = useState<DataSource | null>(null);

  // Real File Preview State
  const [previewHeaders, setPreviewHeaders] = useState<string[]>([]);
  const [previewRows, setPreviewRows] = useState<any[]>([]);
  const [allFileRows, setAllFileRows] = useState<any[]>([]); // Store ALL rows for import
  const [previewFileName, setPreviewFileName] = useState<string>("");
  const [currentFile, setCurrentFile] = useState<File | null>(null);
  const [isPivotedFormat, setIsPivotedFormat] = useState(false);
  const [sizeColumns, setSizeColumns] = useState<string[]>([]);
  const [autoDetectSizeHeaders, setAutoDetectSizeHeaders] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);

  // Sale import warning state
  const [saleImportWarning, setSaleImportWarning] = useState<{
    show: boolean;
    message: string;
    saleFileName: string;
    pendingFile: File | null;
    pendingDataSourceId: string | null;
  }>({
    show: false,
    message: "",
    saleFileName: "",
    pendingFile: null,
    pendingDataSourceId: null,
  });

  // Mapping State
  const [columnMapping, setColumnMapping] = useState({
    sku: "",
    style: "",
    size: "",
    color: "",
    stock: "",
    cost: "",
    price: "",
    shipDate: "",
    discontinued: "",
    salePrice: "",
    futureStock: "",
    futureDate: "",
  });

  // Data Cleaning State
  const [cleaningConfig, setCleaningConfig] = useState({
    trimWhitespace: true,
    removeLetters: false,
    removeNumbers: false,
    removeSpecialChars: false,
    removeFirstN: 0,
    removeLastN: 0,
    findText: "",
    replaceText: "",
    convertYesNo: false,
    yesValue: 1,
    noValue: 0,
    useCustomPrefixes: false,
    stylePrefixRules: [] as Array<{ pattern: string; prefix: string }>,
    removePatterns: [] as string[],
    // Combined variant column parsing (e.g., "AMARNI-BLK-0" = style-color-size)
    combinedVariantColumn: "",
    combinedVariantDelimiter: "-",
  });

  // Import Rules Configuration State
  const [importRulesConfig, setImportRulesConfig] = useState<{
    columnMapping?: any;
    salePriceConfig?: any;
    discontinuedRules?: any;
    priceFloorCeiling?: any;
    minStockThreshold?: number;
    stockThresholdEnabled?: boolean;
    requiredFieldsConfig?: any;
    dateFormatConfig?: any;
    valueReplacementRules?: any;
    sheetConfig?: any;
    fileParseConfig?: any;
    cleaningConfig?: any;
    regularPriceConfig?: any;
    priceBasedExpansionConfig?: any;
    futureStockConfig?: any;
    // BUG FIX: Add sizeLimitConfig to state type (was missing)
    sizeLimitConfig?: {
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
    };
  }>({});

  // Form State
  const [sourceName, setSourceName] = useState("");
  const [sourceType, setSourceType] = useState("manual");
  const [sourceUrl, setSourceUrl] = useState("");
  const [sourceActive, setSourceActive] = useState(true);

  // Email settings state
  const [emailHost, setEmailHost] = useState("");
  const [emailPort, setEmailPort] = useState(993);
  const [emailSecure, setEmailSecure] = useState(true);
  const [emailUsername, setEmailUsername] = useState("");
  const [emailPassword, setEmailPassword] = useState("");
  const [emailFolder, setEmailFolder] = useState("INBOX");
  const [emailSenderWhitelist, setEmailSenderWhitelist] = useState("");
  const [emailSubjectFilter, setEmailSubjectFilter] = useState("");
  const [emailMarkAsRead, setEmailMarkAsRead] = useState(true);

  const [autoUpdate, setAutoUpdate] = useState(false);
  const [updateFreq, setUpdateFreq] = useState("daily");
  const [updateTime, setUpdateTime] = useState("09:00");
  const [ingestionMode, setIngestionMode] = useState("single");
  const [updateStrategy, setUpdateStrategy] = useState("replace"); // 'replace' or 'full_sync'

  // Multi-file mode state (server-side staging)
  const [stagedFiles, setStagedFiles] = useState<any[]>([]);
  const [isCombining, setIsCombining] = useState(false);

  // Client-side multi-file staging (local files before upload)
  const [clientMultiFileMode, setClientMultiFileMode] = useState(false);
  const [clientStagedFiles, setClientStagedFiles] = useState<File[]>([]);

  // URL upload state
  const [urlInput, setUrlInput] = useState("");
  const [isFetchingUrl, setIsFetchingUrl] = useState(false);
  const [isFetchingEmail, setIsFetchingEmail] = useState(false);
  const [fetchingEmailDataSourceId, setFetchingEmailDataSourceId] = useState<
    string | null
  >(null);
  const [fetchingUrlDataSourceId, setFetchingUrlDataSourceId] = useState<
    string | null
  >(null);

  // Fetch data sources from backend
  const { data: dataSources = [], isLoading } = useQuery({
    queryKey: ["data-sources"],
    queryFn: async () => {
      const res = await fetch("/api/data-sources");
      if (!res.ok) throw new Error("Failed to fetch data sources");
      return res.json();
    },
  });

  // Fetch recent uploads for upload history
  const { data: recentUploads = [] } = useQuery({
    queryKey: ["recent-uploads"],
    queryFn: async () => {
      const res = await fetch("/api/uploads/recent?limit=50");
      if (!res.ok) throw new Error("Failed to fetch recent uploads");
      return res.json();
    },
  });

  // Create data source mutation
  const createDataSourceMutation = useMutation({
    mutationFn: async (data: any & { file?: File }) => {
      const { file, ...dataSourceData } = data;
      const res = await fetch("/api/data-sources", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(dataSourceData),
      });
      if (!res.ok) throw new Error("Failed to create data source");
      const created = await res.json();

      // Auto-upload file if provided
      if (file) {
        const formData = new FormData();
        formData.append("file", file);
        const uploadRes = await fetch(
          `/api/data-sources/${created.id}/upload`,
          {
            method: "POST",
            body: formData,
          },
        );
        if (!uploadRes.ok) {
          const err = await uploadRes.json();
          throw new Error(err.error || "Failed to upload file");
        }
        return uploadRes.json();
      }

      return created;
    },
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
      queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
      queryClient.invalidateQueries({ queryKey: ["recent-uploads"] });
      if (result.importedItems !== undefined) {
        toast({
          title: "Success",
          description: `Data source saved and ${result.importedItems.toLocaleString()} items imported to master inventory`,
        });
      } else {
        toast({
          title: "Success",
          description:
            "Data source created successfully. You can now configure schedule, mapping, and rules.",
        });
      }
      // Switch to edit mode so user can configure other tabs
      if (result && result.id) {
        setEditingSource(result);
      }
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  // Update data source mutation
  const updateDataSourceMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) => {
      const res = await fetch(`/api/data-sources/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("Failed to update data source");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
      toast({
        title: "Success",
        description: "Data source updated successfully",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  // Re-import mutation (apply cleaning settings to existing data)
  const reimportMutation = useMutation({
    mutationFn: async (dataSourceId: string) => {
      const res = await fetch(`/api/data-sources/${dataSourceId}/reimport`, {
        method: "POST",
      });
      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || "Failed to re-import");
      }
      return res.json();
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ["inventory"] });
      queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
      toast({ title: "Success", description: data.message });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  // Delete data source mutation
  const deleteDataSourceMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/data-sources/${id}`, { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to delete data source");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
      toast({ title: "Success", description: "Data source deleted" });
    },
  });

  const clearHistoryMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch("/api/uploads/history", { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to clear history");
      return res.json();
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ["recent-uploads"] });
      toast({
        title: "Success",
        description: `Cleared ${data.deletedCount} upload records`,
      });
    },
  });

  // Fetch staged files when a multi-file data source is selected
  const fetchStagedFiles = async (dataSourceId: string) => {
    try {
      const res = await fetch(`/api/data-sources/${dataSourceId}/staged-files`);
      if (res.ok) {
        const files = await res.json();
        setStagedFiles(files);
      }
    } catch (error) {
      console.error("Error fetching staged files:", error);
    }
  };

  // Combine and import staged files
  // Import all client-staged files together using AI import execute endpoint
  // This uses the existing multi-file support in aiImportRoutes without changing import/parser/sync code
  const handleClientMultiFileImport = async () => {
    if (!selectedDataSourceId || clientStagedFiles.length === 0) return;

    setIsImporting(true);
    setUploadProgress(0);

    try {
      const formData = new FormData();
      // Use "files" field name for multi-file support (matches AI import execute endpoint)
      clientStagedFiles.forEach((file) => {
        formData.append("files", file);
      });
      formData.append("dataSourceId", selectedDataSourceId);
      formData.append("multiFileMode", "true");

      const result = await new Promise<any>((resolve, reject) => {
        const xhr = new XMLHttpRequest();

        xhr.upload.addEventListener("progress", (event) => {
          if (event.lengthComputable) {
            const percentComplete = Math.round(
              (event.loaded / event.total) * 100,
            );
            setUploadProgress(percentComplete);
          }
        });

        xhr.addEventListener("load", () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            try {
              resolve(JSON.parse(xhr.responseText));
            } catch {
              reject(new Error("Invalid response"));
            }
          } else {
            try {
              const errData = JSON.parse(xhr.responseText);
              reject(
                new Error(errData.error || errData.message || "Upload failed"),
              );
            } catch {
              reject(new Error("Upload failed"));
            }
          }
        });

        xhr.addEventListener("error", () => reject(new Error("Upload failed")));
        xhr.addEventListener("abort", () =>
          reject(new Error("Upload cancelled")),
        );

        // Use AI import execute endpoint which already supports multi-file consolidation
        xhr.open("POST", `/api/ai-import/execute`);
        xhr.send(formData);
      });

      toast({
        title: "Import successful",
        description: `Imported ${clientStagedFiles.length} files with ${result.itemCount?.toLocaleString() || result.stats?.importedItems?.toLocaleString() || 0} items`,
      });

      // Clear staged files and reset multi-file mode
      setClientStagedFiles([]);
      setClientMultiFileMode(false);
      setPreviewFileName("");
      setPreviewHeaders([]);
      setPreviewRows([]);

      // Refresh data
      queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
    } catch (error: any) {
      toast({
        title: "Import failed",
        description: error.message,
        variant: "destructive",
      });
    } finally {
      setIsImporting(false);
      setUploadProgress(0);
    }
  };

  const handleCombineImport = async () => {
    if (!selectedDataSourceId) return;

    setIsCombining(true);
    try {
      const res = await fetch(
        `/api/data-sources/${selectedDataSourceId}/combine-import`,
        {
          method: "POST",
        },
      );

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || "Failed to combine files");
      }

      const result = await res.json();
      toast({ title: "Success", description: result.message });

      // Refresh all relevant queries
      queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });

      // Clear staged files - the useEffect will not re-fetch since files are now "imported"
      setStagedFiles([]);
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    } finally {
      setIsCombining(false);
    }
  };

  // Delete a staged file
  const handleDeleteStagedFile = async (fileId: string) => {
    if (!selectedDataSourceId) return;

    try {
      const res = await fetch(
        `/api/data-sources/${selectedDataSourceId}/staged-files/${fileId}`,
        {
          method: "DELETE",
        },
      );

      if (res.ok) {
        // Remove the file from local state
        setStagedFiles((prev) => prev.filter((f) => f.id !== fileId));
        toast({
          title: "File removed",
          description: "Staged file has been removed",
        });
      }
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message,
        variant: "destructive",
      });
    }
  };

  // Fetch and process URL
  const handleFetchUrl = async () => {
    if (!urlInput.trim()) {
      toast({
        title: "Error",
        description: "Please enter a URL",
        variant: "destructive",
      });
      return;
    }
    if (!selectedDataSourceId) {
      toast({
        title: "Error",
        description: "Please select a data source first",
        variant: "destructive",
      });
      return;
    }

    setIsFetchingUrl(true);
    toast({
      title: "Fetching URL",
      description: "Downloading and processing file...",
    });

    try {
      const res = await fetch(
        `/api/data-sources/${selectedDataSourceId}/fetch-url`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: urlInput.trim() }),
        },
      );

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || "Failed to fetch URL");
      }

      const result = await res.json();
      const details = [];
      if (result.noSizeRemoved > 0)
        details.push(`${result.noSizeRemoved} skipped (no size)`);
      if (result.duplicatesRemoved > 0)
        details.push(`${result.duplicatesRemoved} duplicates removed`);
      if (result.colorsFixed > 0)
        details.push(`${result.colorsFixed} colors fixed`);

      toast({
        title: "URL Import Successful",
        description: `Imported ${result.itemCount} items.${details.length > 0 ? " " + details.join(", ") + "." : ""}`,
      });

      // Refresh queries
      queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });

      // Clear URL input
      setUrlInput("");
    } catch (error: any) {
      toast({
        title: "URL Fetch Failed",
        description: error.message,
        variant: "destructive",
      });
    } finally {
      setIsFetchingUrl(false);
    }
  };

  // Fetch staged files when selected data source changes (for multi-file mode)
  // Use a derived variable to avoid dependency on dataSources array reference
  const selectedSource = dataSources.find(
    (s: DataSource) => s.id === selectedDataSourceId,
  );
  const isMultiFileMode =
    selectedSource && (selectedSource as any).ingestionMode === "multi";

  useEffect(() => {
    if (selectedDataSourceId && isMultiFileMode) {
      fetchStagedFiles(selectedDataSourceId);
    } else {
      setStagedFiles([]);
    }
  }, [selectedDataSourceId, isMultiFileMode]);

  // Import to master inventory function
  const handleImportToMaster = async (dataSourceId: string) => {
    if (allFileRows.length === 0 || previewHeaders.length === 0) {
      toast({
        title: "No Data",
        description: "Please upload a file first",
        variant: "destructive",
      });
      return;
    }

    setIsImporting(true);

    try {
      // Parse the rows using column mapping
      const items = allFileRows
        .map((row: any[]) => {
          const getColValue = (colName: string) => {
            if (!colName) return null;
            const colIndex = previewHeaders.findIndex(
              (h) => h && h.toString().toLowerCase() === colName.toLowerCase(),
            );
            return colIndex >= 0 ? row[colIndex] : null;
          };

          // Get mapped values
          let sku = getColValue(columnMapping.sku) || "";
          let style = getColValue(columnMapping.style) || "";
          let size = getColValue(columnMapping.size) || "";
          let color = getColValue(columnMapping.color) || "";
          let stockValue = getColValue(columnMapping.stock);

          // Handle combined color>size format (e.g., "Black/Multi > 00 >")
          const colorSizeCol = previewHeaders.find(
            (h) => h && h.toString().toLowerCase().includes("colorsize"),
          );
          if (colorSizeCol && !color && !size) {
            const colIndex = previewHeaders.indexOf(colorSizeCol);
            const combined = row[colIndex]?.toString() || "";
            // Parse "Color > Size >" format
            const parts = combined.split(">").map((s: string) => s.trim());
            if (parts.length >= 2) {
              color = parts[0] || "";
              size = parts[1] || "";
            }
          }

          // If no SKU mapped, use style as SKU
          if (!sku && style) {
            sku = style;
          }

          // Convert stock to number
          let stock = 0;
          if (typeof stockValue === "number") {
            stock = stockValue;
          } else if (typeof stockValue === "string") {
            const parsed = parseInt(stockValue.replace(/[^0-9-]/g, ""));
            stock = isNaN(parsed) ? 0 : parsed;
          }

          return {
            sku: String(sku || ""),
            style: style ? String(style) : null,
            size: size ? String(size) : null,
            color: color ? String(color) : null,
            stock,
            rawData: Object.fromEntries(
              previewHeaders.map((h, i) => [h, row[i]]),
            ),
          };
        })
        .filter((item) => item.sku); // Only keep items with SKU

      const response = await fetch("/api/inventory/import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dataSourceId, items }),
      });

      if (!response.ok) {
        throw new Error("Import failed");
      }

      const result = await response.json();

      toast({
        title: "Import Successful",
        description: `Imported ${result.count} items to master inventory`,
      });

      // Refresh master inventory
      queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
    } catch (error: any) {
      toast({
        title: "Import Failed",
        description: error.message || "Could not import inventory",
        variant: "destructive",
      });
    } finally {
      setIsImporting(false);
    }
  };

  const processFile = (file: File) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const arrayBuffer = e.target?.result as ArrayBuffer;
        if (!arrayBuffer) {
          toast({
            title: "Error",
            description: "Could not read the file. Please try again.",
            variant: "destructive",
          });
          return;
        }

        // Detect UTF-16 encoding via BOM (Byte Order Mark)
        const uint8 = new Uint8Array(arrayBuffer);
        let data: ArrayBuffer | string = arrayBuffer;
        let readType: "array" | "string" = "array";

        // Check for UTF-16 LE BOM (0xFF 0xFE) or UTF-16 BE BOM (0xFE 0xFF)
        if (uint8.length >= 2) {
          const isUtf16LE = uint8[0] === 0xff && uint8[1] === 0xfe;
          const isUtf16BE = uint8[0] === 0xfe && uint8[1] === 0xff;

          if (isUtf16LE || isUtf16BE) {
            console.log(
              `Detected UTF-16 ${isUtf16LE ? "LE" : "BE"} encoding, converting to UTF-8...`,
            );
            try {
              const decoder = new TextDecoder(
                isUtf16LE ? "utf-16le" : "utf-16be",
              );
              data = decoder.decode(arrayBuffer);
              readType = "string";
            } catch (decodeError) {
              console.error("UTF-16 decode error:", decodeError);
              toast({
                title: "Encoding Error",
                description:
                  "Could not decode UTF-16 file. Try opening in Excel and saving as CSV (UTF-8).",
                variant: "destructive",
              });
              return;
            }
          }
        }

        let workbook;
        try {
          workbook = XLSX.read(data, { type: readType });
        } catch (parseError) {
          console.error("XLSX parse error:", parseError);
          toast({
            title: "File Format Error",
            description:
              "Could not parse this file. Try opening in Excel and saving as CSV (UTF-8) or XLSX format.",
            variant: "destructive",
          });
          return;
        }

        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];

        // Get raw data first to inspect structure
        const rawData = XLSX.utils.sheet_to_json(sheet, {
          header: 1,
          raw: false,
        }) as any[][];

        if (rawData.length > 0) {
          // Intelligent Header Detection
          // Scan first 10 rows to find the most likely header row
          // A header row usually contains string keywords matching our fields
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

          // Convert all headers to strings and filter out empty ones
          const rawHeaders = rawData[headerRowIndex] as any[];
          const headers = rawHeaders.map((h, i) => {
            if (h === null || h === undefined || String(h).trim() === "") {
              return `Column ${i + 1}`; // Give unnamed columns a default name
            }
            return String(h).trim();
          });
          console.log(
            `[FileProcess] Detected ${headers.length} columns:`,
            headers,
          );
          // Data rows start after the header
          const allRows = rawData
            .slice(headerRowIndex + 1)
            .filter(
              (row: any[]) =>
                row &&
                row.some(
                  (cell) => cell !== null && cell !== undefined && cell !== "",
                ),
            );
          const previewRowsSlice = allRows.slice(0, 10); // Preview first 10 rows

          setPreviewHeaders(headers);
          setPreviewRows(previewRowsSlice);
          setAllFileRows(allRows); // Store ALL rows for import
          setPreviewFileName(file.name);
          setCurrentFile(file); // Store file for import
        } else {
          toast({
            title: "Empty File",
            description:
              "The file appears to be empty or has no readable data.",
            variant: "destructive",
          });
        }
      } catch (error) {
        console.error("File processing error:", error);
        toast({
          title: "Error Processing File",
          description:
            "An unexpected error occurred while reading the file. Please try a different file format.",
          variant: "destructive",
        });
      }
    };
    reader.onerror = () => {
      toast({
        title: "File Read Error",
        description:
          "Could not read the file. Please check the file and try again.",
        variant: "destructive",
      });
    };
    reader.readAsArrayBuffer(file);
  };

  const onDrop = useCallback(
    async (acceptedFiles: File[]) => {
      if (acceptedFiles.length > 0) {
        // Client-side multi-file mode: stage all files locally instead of uploading
        if (clientMultiFileMode) {
          setClientStagedFiles((prev) => [...prev, ...acceptedFiles]);
          processFile(acceptedFiles[acceptedFiles.length - 1]); // Show preview of last file
          return;
        }

        // Single file mode - use first file
        const file = acceptedFiles[0];
        processFile(file);

        // If a data source is selected, upload to that data source
        if (selectedDataSourceId) {
          await uploadFileToBackend(file, selectedDataSourceId);
        }
      }
    },
    [selectedDataSourceId, clientMultiFileMode],
  );

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    multiple: clientMultiFileMode, // Allow multiple files in multi-file mode
  });

  const handlePreviewSource = async (source: DataSource) => {
    setPreviewSource(source);

    // Always fetch real inventory data from the data source when clicking Preview
    try {
      const response = await fetch(
        `/api/data-sources/${source.id}/inventory-preview`,
      );
      if (response.ok) {
        const data = await response.json();
        if (data.headers && data.rows && data.rows.length > 0) {
          setPreviewHeaders(data.headers);
          setPreviewRows(data.rows);
          setPreviewFileName(data.fileName || `${source.name} - Data Preview`);
        } else {
          // No data in this source yet
          setPreviewHeaders(["Style", "Color", "Size", "Stock", "Status"]);
          setPreviewRows([]);
          setPreviewFileName(`${source.name} - No data yet`);
        }
      } else {
        // API error, show empty state
        setPreviewHeaders(["Style", "Color", "Size", "Stock", "Status"]);
        setPreviewRows([]);
        setPreviewFileName(`${source.name} - No data available`);
      }
    } catch (error) {
      console.error("Error fetching preview data:", error);
      setPreviewHeaders(["Style", "Color", "Size", "Stock", "Status"]);
      setPreviewRows([]);
      setPreviewFileName(`${source.name} - Error loading data`);
    }

    setIsPreviewOpen(true);
  };

  const handleFetchEmail = async (dataSourceId: string) => {
    setIsFetchingEmail(true);
    setFetchingEmailDataSourceId(dataSourceId);

    try {
      const response = await fetch(
        `/api/data-sources/${dataSourceId}/fetch-email`,
        {
          method: "POST",
        },
      );

      const result = await response.json();

      if (!response.ok) {
        toast({
          title: "Email Fetch Failed",
          description: result.error || "Could not fetch emails",
          variant: "destructive",
        });
        return;
      }

      if (result.filesProcessed > 0) {
        const logSummary = result.logs?.slice(-3).join(" → ") || "";
        toast({
          title: "Emails Fetched Successfully",
          description:
            `Processed ${result.filesProcessed} file(s). ${result.itemCount ? `Imported ${result.itemCount} items.` : ""} ${logSummary}`.trim(),
        });
        queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
        queryClient.invalidateQueries({ queryKey: ["data-sources"] });
      } else {
        const logInfo =
          result.logs?.length > 0
            ? result.logs.slice(-2).join(" ")
            : "Check email settings or sender address filter.";
        toast({
          title: "No New Emails",
          description: `No new attachments found. ${logInfo}`,
        });
      }
    } catch (error: any) {
      toast({
        title: "Email Fetch Failed",
        description: error.message || "Could not connect to email server",
        variant: "destructive",
      });
    } finally {
      setIsFetchingEmail(false);
      setFetchingEmailDataSourceId(null);
    }
  };

  const handleFetchUrlForDataSource = async (dataSourceId: string) => {
    setIsFetchingUrl(true);
    setFetchingUrlDataSourceId(dataSourceId);

    try {
      const response = await fetch(
        `/api/data-sources/${dataSourceId}/fetch-url`,
        {
          method: "POST",
        },
      );

      const result = await response.json();

      if (!response.ok) {
        toast({
          title: "URL Fetch Failed",
          description: result.error || "Could not fetch from URL",
          variant: "destructive",
        });
        return;
      }

      if (result.itemCount > 0) {
        const logSummary = result.logs?.slice(-3).join(" → ") || "";
        toast({
          title: "URL Fetched Successfully",
          description:
            `Imported ${result.itemCount} items. ${logSummary}`.trim(),
        });
        queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
        queryClient.invalidateQueries({ queryKey: ["data-sources"] });
        queryClient.invalidateQueries({ queryKey: ["recent-uploads"] });
      } else {
        const logInfo =
          result.logs?.length > 0
            ? result.logs.slice(-2).join(" ")
            : "Check URL configuration.";
        toast({
          title: "No Items Imported",
          description: `No items found in URL response. ${logInfo}`,
        });
      }
    } catch (error: any) {
      toast({
        title: "URL Fetch Failed",
        description: error.message || "Could not fetch from URL",
        variant: "destructive",
      });
    } finally {
      setIsFetchingUrl(false);
      setFetchingUrlDataSourceId(null);
    }
  };

  const handleDialogFileUpload = async (
    e: React.ChangeEvent<HTMLInputElement>,
  ) => {
    if (e.target.files && e.target.files.length > 0) {
      const file = e.target.files[0];
      processFile(file);

      // Auto-upload to backend if we have a data source selected
      if (previewSource) {
        await uploadFileToBackend(file, previewSource.id);
      }
    }
  };

  // Upload file to backend and auto-import to master inventory (or stage for multi-file mode)
  const uploadFileToBackend = async (
    file: File,
    dataSourceId: string,
    skipWarningCheck = false,
  ) => {
    // Check if sale file import is required first (only for manual uploads)
    if (!skipWarningCheck) {
      try {
        const checkRes = await fetch(
          `/api/data-sources/${dataSourceId}/check-sale-import`,
        );
        if (checkRes.ok) {
          const checkResult = await checkRes.json();
          if (checkResult.requiresWarning) {
            // Show warning dialog instead of uploading
            setSaleImportWarning({
              show: true,
              message:
                checkResult.warningMessage ||
                "Sale file has not been imported yet.",
              saleFileName: checkResult.saleDataSourceName || "Unknown",
              pendingFile: file,
              pendingDataSourceId: dataSourceId,
            });
            return; // Don't proceed with upload - user must confirm
          }
        }
      } catch (error) {
        console.warn(
          "[uploadFileToBackend] Could not check sale import requirement:",
          error,
        );
        // Continue with upload if check fails
      }
    }

    setIsImporting(true);
    setUploadProgress(0);

    try {
      const formData = new FormData();
      formData.append("file", file);

      // Use XMLHttpRequest for progress tracking
      const result = await new Promise<any>((resolve, reject) => {
        const xhr = new XMLHttpRequest();

        xhr.upload.addEventListener("progress", (event) => {
          if (event.lengthComputable) {
            const percentComplete = Math.round(
              (event.loaded / event.total) * 100,
            );
            setUploadProgress(percentComplete);
          }
        });

        xhr.addEventListener("load", () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            try {
              resolve(JSON.parse(xhr.responseText));
            } catch {
              reject(new Error("Invalid response"));
            }
          } else {
            reject(new Error("Upload failed"));
          }
        });

        xhr.addEventListener("error", () => reject(new Error("Upload failed")));
        xhr.addEventListener("abort", () =>
          reject(new Error("Upload cancelled")),
        );

        xhr.open("POST", `/api/data-sources/${dataSourceId}/upload`);
        xhr.send(formData);
      });

      // Check if file was staged (multi-file mode) or imported directly
      if (result.staged) {
        // Multi-file mode - file was staged
        toast({
          title: "File Staged",
          description:
            result.message ||
            `File staged. ${result.stagedCount} file(s) ready to combine.`,
        });

        // Refresh staged files list
        fetchStagedFiles(dataSourceId);

        // Clear the preview since file is staged, not imported
        setPreviewFileName("");
        setPreviewHeaders([]);
        setPreviewRows([]);
        setAllFileRows([]);
      } else {
        // Single file mode - file was imported directly
        // Update preview with the server's converted/cleaned data format
        if (result.file) {
          setPreviewHeaders(
            result.file.headers || ["style", "color", "size", "stock"],
          );
          setPreviewRows(result.file.previewData || []);
          setPreviewFileName(result.file.fileName || file.name);
        }

        toast({
          title: "File Imported Successfully",
          description:
            result.message ||
            `Imported ${result.importedItems} items to master inventory`,
        });

        // Refresh master inventory
        queryClient.invalidateQueries({ queryKey: ["master-inventory"] });
      }

      // Refresh data sources and upload history
      queryClient.invalidateQueries({ queryKey: ["data-sources"] });
      queryClient.invalidateQueries({ queryKey: ["recent-uploads"] });
    } catch (error: any) {
      toast({
        title: "Import Failed",
        description: error.message || "Could not import file",
        variant: "destructive",
      });
    } finally {
      setIsImporting(false);
      setUploadProgress(0);
    }
  };

  const resetForm = () => {
    setSourceName("");
    setSourceType("manual");
    setSourceUrl("");
    setSourceActive(true);
    setAutoUpdate(false);
    setUpdateFreq("daily");
    setUpdateTime("09:00");
    setIngestionMode("single");
    setUpdateStrategy("replace");
    setTestStatus("idle");
    setPreviewHeaders([]);
    setPreviewRows([]);
    setAllFileRows([]);
    setPreviewFileName("");
    setCurrentFile(null);
    setColumnMapping({
      sku: "",
      style: "",
      size: "",
      color: "",
      stock: "",
      cost: "",
      price: "",
      shipDate: "",
      discontinued: "",
      salePrice: "",
      futureStock: "",
      futureDate: "",
    });
    // Reset email settings
    setEmailHost("");
    setEmailPort(993);
    setEmailSecure(true);
    setEmailUsername("");
    setEmailPassword("");
    setEmailFolder("INBOX");
    setEmailSenderWhitelist("");
    setEmailSubjectFilter("");
    setEmailMarkAsRead(true);
    // Reset import rules config
    setImportRulesConfig({});
  };

  const handleEditSource = (source: DataSource) => {
    setEditingSource(source);
    setSourceName(source.name);
    setSourceType(source.type);
    setSourceUrl(source.connectionDetails?.url || "");
    setSourceActive(source.status === "active");
    setAutoUpdate(source.autoUpdate || false);
    setUpdateFreq(source.updateFrequency || "daily");
    setUpdateTime(source.updateTime || "09:00");
    setIngestionMode((source as any).ingestionMode || "single");
    setUpdateStrategy((source as any).updateStrategy || "replace");
    setColumnMapping(
      source.columnMapping || {
        sku: "",
        style: "",
        size: "",
        color: "",
        stock: "",
        cost: "",
        price: "",
        shipDate: "",
        discontinued: "",
        salePrice: "",
      },
    );
    // Load email settings
    const emailSettings = (source as any).emailSettings || {};
    setEmailHost(emailSettings.host || "");
    setEmailPort(emailSettings.port || 993);
    setEmailSecure(emailSettings.secure ?? true);
    setEmailUsername(emailSettings.username || "");
    setEmailPassword(emailSettings.password || "");
    setEmailFolder(emailSettings.folder || "INBOX");
    setEmailSenderWhitelist((emailSettings.senderWhitelist || []).join(", "));
    setEmailSubjectFilter(emailSettings.subjectFilter || "");
    setEmailMarkAsRead(emailSettings.markAsRead ?? true);
    setCleaningConfig({
      trimWhitespace: source.cleaningConfig?.trimWhitespace ?? true,
      removeLetters: source.cleaningConfig?.removeLetters ?? false,
      removeNumbers: source.cleaningConfig?.removeNumbers ?? false,
      removeSpecialChars: source.cleaningConfig?.removeSpecialChars ?? false,
      removeFirstN: source.cleaningConfig?.removeFirstN ?? 0,
      removeLastN: source.cleaningConfig?.removeLastN ?? 0,
      findText: source.cleaningConfig?.findText ?? "",
      replaceText: source.cleaningConfig?.replaceText ?? "",
      convertYesNo: source.cleaningConfig?.convertYesNo ?? false,
      yesValue: source.cleaningConfig?.yesValue ?? 1,
      noValue: source.cleaningConfig?.noValue ?? 0,
      useCustomPrefixes: source.cleaningConfig?.useCustomPrefixes ?? false,
      stylePrefixRules: source.cleaningConfig?.stylePrefixRules ?? [],
      removePatterns: source.cleaningConfig?.removePatterns ?? [],
      combinedVariantColumn: source.cleaningConfig?.combinedVariantColumn ?? "",
      combinedVariantDelimiter:
        source.cleaningConfig?.combinedVariantDelimiter ?? "-",
    });
    // Set import rules config from data source
    setImportRulesConfig({
      columnMapping: source.columnMapping,
      salePriceConfig: source.salePriceConfig,
      discontinuedRules: source.discontinuedRules,
      priceFloorCeiling: source.priceFloorCeiling,
      minStockThreshold: source.minStockThreshold,
      stockThresholdEnabled: (source as any).stockThresholdEnabled,
      requiredFieldsConfig: source.requiredFieldsConfig,
      dateFormatConfig: source.dateFormatConfig,
      valueReplacementRules: source.valueReplacementRules,
      sheetConfig: source.sheetConfig,
      fileParseConfig: source.fileParseConfig,
      cleaningConfig: source.cleaningConfig,
      regularPriceConfig: source.regularPriceConfig,
      priceBasedExpansionConfig: (source as any).priceBasedExpansionConfig,
      futureStockConfig: (source as any).futureStockConfig,
      // BUG FIX: Load sizeLimitConfig when editing (was missing)
      sizeLimitConfig: (source as any).sizeLimitConfig,
    });
    // Initialize pivot config from data source
    const pivotConfig = (source as any).pivotConfig;
    if (pivotConfig?.enabled) {
      setIsPivotedFormat(true);
      setSizeColumns(pivotConfig.sizeColumns?.sizeHeaders || []);
      setAutoDetectSizeHeaders(
        pivotConfig.sizeColumns?.autoDetectSizeHeaders || false,
      );
    } else {
      setIsPivotedFormat(false);
      setSizeColumns([]);
      setAutoDetectSizeHeaders(false);
    }
    setIsSourceDialogOpen(true);
    setAiMappingStatus("idle");
    setTestStatus("idle");
  };

  const handleNewSource = () => {
    setEditingSource(null);
    resetForm();
    setIsSourceDialogOpen(true);
    setAiMappingStatus("idle");
  };

  const handleSaveSource = () => {
    const dataSourcePayload: any = {
      name: sourceName || "New Source",
      type: sourceType,
      columnMapping,
      cleaningConfig: importRulesConfig.cleaningConfig || cleaningConfig,
      autoUpdate,
      updateFrequency: autoUpdate ? updateFreq : null,
      updateTime: autoUpdate && updateFreq === "daily" ? updateTime : null,
      connectionDetails: sourceType === "url" ? { url: sourceUrl } : null,
      status: sourceActive ? "active" : "inactive",
      ingestionMode,
      updateStrategy,
      // Import rules configuration
      salePriceConfig: importRulesConfig.salePriceConfig,
      discontinuedRules: importRulesConfig.discontinuedRules,
      priceFloorCeiling: importRulesConfig.priceFloorCeiling,
      minStockThreshold: importRulesConfig.minStockThreshold,
      requiredFieldsConfig: importRulesConfig.requiredFieldsConfig,
      dateFormatConfig: importRulesConfig.dateFormatConfig,
      valueReplacementRules: importRulesConfig.valueReplacementRules,
      sheetConfig: importRulesConfig.sheetConfig,
      fileParseConfig: importRulesConfig.fileParseConfig,
    };

    // Add email settings when source type is email
    if (sourceType === "email") {
      dataSourcePayload.emailSettings = {
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
      };
    }

    if (editingSource) {
      updateDataSourceMutation.mutate({
        id: editingSource.id,
        data: dataSourcePayload,
      });
    } else {
      // Include the file for automatic upload when creating a new data source
      createDataSourceMutation.mutate({
        ...dataSourcePayload,
        file: currentFile || undefined,
      });
    }

    setIsSourceDialogOpen(false);
    resetForm();
  };

  // Save CONNECTION only: name, type, URL/email settings, status, ingestion mode, update strategy
  const handleSaveConnection = () => {
    if (!editingSource) {
      toast({
        title: "Error",
        description: "Cannot save without an existing data source",
        variant: "destructive",
      });
      return;
    }

    const connectionPayload: any = {
      name: sourceName || "New Source",
      type: sourceType,
      connectionDetails: sourceType === "url" ? { url: sourceUrl } : null,
      status: sourceActive ? "active" : "inactive",
      ingestionMode,
      updateStrategy,
    };

    // Add email settings when source type is email
    if (sourceType === "email") {
      connectionPayload.emailSettings = {
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
      };
    }

    updateDataSourceMutation.mutate({
      id: editingSource.id,
      data: connectionPayload,
    });
    toast({
      title: "Success",
      description: "Connection details saved successfully",
    });
  };

  // Create new data source from dialog (when editingSource is null)
  const handleCreateDataSource = () => {
    if (!sourceName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a source name",
        variant: "destructive",
      });
      return;
    }

    const createPayload: any = {
      name: sourceName,
      type: sourceType,
      connectionDetails: sourceType === "url" ? { url: sourceUrl } : null,
      status: sourceActive ? "active" : "inactive",
      ingestionMode,
      updateStrategy,
    };

    // Add email settings when source type is email
    if (sourceType === "email") {
      createPayload.emailSettings = {
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
      };
    }

    createDataSourceMutation.mutate(createPayload);
  };

  // Save SCHEDULE only: auto-update settings
  const handleSaveSchedule = () => {
    if (!editingSource) {
      toast({
        title: "Error",
        description: "Cannot save without an existing data source",
        variant: "destructive",
      });
      return;
    }

    const schedulePayload: any = {
      autoUpdate,
      updateFrequency: autoUpdate ? updateFreq : null,
      updateTime: autoUpdate && updateFreq === "daily" ? updateTime : null,
    };

    updateDataSourceMutation.mutate({
      id: editingSource.id,
      data: schedulePayload,
    });
    toast({ title: "Success", description: "Schedule saved successfully" });
  };

  // Save COLUMN MAPPING only: column mapping + pivot config
  const handleSaveColumnMapping = () => {
    if (!editingSource) {
      toast({
        title: "Error",
        description: "Cannot save without an existing data source",
        variant: "destructive",
      });
      return;
    }

    const mappingPayload: any = {
      columnMapping,
      pivotConfig: isPivotedFormat
        ? {
            enabled: true,
            sizeColumns: {
              sizeHeaders: sizeColumns,
              autoDetectSizeHeaders: autoDetectSizeHeaders,
            },
          }
        : { enabled: false },
    };

    updateDataSourceMutation.mutate({
      id: editingSource.id,
      data: mappingPayload,
    });
    toast({
      title: "Success",
      description: "Column mapping saved successfully",
    });
  };

  // Save IMPORT RULES only: cleaning config, discontinued rules, price config, etc.
  const handleSaveImportRules = () => {
    if (!editingSource) {
      toast({
        title: "Error",
        description: "Cannot save without an existing data source",
        variant: "destructive",
      });
      return;
    }

    // Use the separate cleaningConfig state (not importRulesConfig.cleaningConfig)
    // because cleaningConfig is managed as its own useState
    const importRulesPayload: any = {
      cleaningConfig: cleaningConfig,
      salePriceConfig: importRulesConfig.salePriceConfig,
      discontinuedRules: importRulesConfig.discontinuedRules,
      priceFloorCeiling: importRulesConfig.priceFloorCeiling,
      minStockThreshold: importRulesConfig.minStockThreshold,
      stockThresholdEnabled: importRulesConfig.stockThresholdEnabled,
      requiredFieldsConfig: importRulesConfig.requiredFieldsConfig,
      dateFormatConfig: importRulesConfig.dateFormatConfig,
      valueReplacementRules: importRulesConfig.valueReplacementRules,
      sheetConfig: importRulesConfig.sheetConfig,
      fileParseConfig: importRulesConfig.fileParseConfig,
      priceBasedExpansionConfig: importRulesConfig.priceBasedExpansionConfig,
      futureStockConfig: importRulesConfig.futureStockConfig,
      // BUG FIX: Include sizeLimitConfig in save payload (was missing)
      sizeLimitConfig: importRulesConfig.sizeLimitConfig,
    };

    updateDataSourceMutation.mutate(
      {
        id: editingSource.id,
        data: importRulesPayload,
      },
      {
        onSuccess: () => {
          toast({
            title: "Success",
            description: "Import rules saved successfully",
          });
        },
        onError: (error: any) => {
          toast({
            title: "Error",
            description: error.message || "Failed to save import rules",
            variant: "destructive",
          });
        },
      },
    );
  };

  // Save as Template function
  const handleSaveAsTemplate = async () => {
    if (!editingSource) return;
    const templateName = prompt(
      "Enter template name:",
      `${editingSource.name} Template`,
    );
    if (!templateName) return;
    try {
      const res = await fetch("/api/templates", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: templateName,
          dataSourceId: editingSource.id,
        }),
      });
      if (res.ok) {
        toast({
          title: "Success",
          description:
            "Template saved successfully! You can find it in Settings > Templates.",
        });
      } else {
        toast({
          title: "Error",
          description: "Failed to save template",
          variant: "destructive",
        });
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to save template",
        variant: "destructive",
      });
    }
  };

  const runAiMapping = async () => {
    if (previewHeaders.length === 0) {
      toast({
        title: "No file loaded",
        description: "Please upload an Excel file first",
        variant: "destructive",
      });
      return;
    }

    setAiMappingStatus("scanning");

    try {
      const response = await fetch("/api/ai/analyze-columns", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          headers: previewHeaders,
          sampleRows: previewRows.slice(0, 10),
        }),
      });

      if (!response.ok) {
        throw new Error("AI analysis failed");
      }

      const result = await response.json();

      // Handle pivoted format detection
      setIsPivotedFormat(result.isPivoted || false);
      if (result.sizeColumns && Array.isArray(result.sizeColumns)) {
        setSizeColumns(result.sizeColumns);
      }

      const newMapping = {
        sku: result.sku || "",
        style: result.style || "",
        size: result.isPivoted ? "" : result.size || "",
        color: result.color || "",
        stock: result.isPivoted ? "" : result.stock || "",
        cost: result.cost || "",
        price: result.price || "",
        shipDate: result.shipDate || "",
        discontinued: result.discontinued || "",
        salePrice: result.salePrice || "",
        futureStock: result.futureStock || "",
        futureDate: result.futureDate || "",
      };

      setColumnMapping(newMapping);
      setAiMappingStatus("completed");

      const formatMsg = result.isPivoted
        ? `PIVOTED FORMAT detected! Sizes are column headers. Found ${result.sizeColumns?.length || 0} size columns.`
        : "Standard format detected.";

      toast({
        title: "AI Mapping Complete",
        description: `${formatMsg} Confidence: ${Math.round((result.confidence || 0) * 100)}%.`,
      });
    } catch (error) {
      console.error("AI mapping error:", error);
      setAiMappingStatus("idle");
      toast({
        title: "AI Analysis Failed",
        description: "Falling back to manual mapping",
        variant: "destructive",
      });
    }
  };

  const runTestConnection = async () => {
    setTestStatus("testing");
    try {
      const response = await fetch("/api/test-url-connection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: sourceUrl }),
      });
      const result = await response.json();
      setTestStatus(result.success ? "success" : "error");
    } catch (error) {
      setTestStatus("error");
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-display font-bold text-foreground">
            Inventory Upload
          </h1>
          <p className="text-muted-foreground">
            Upload Excel files to update stock levels and pricing.
          </p>
        </div>
      </div>

      <ResponsiveTabs
        value={activeTab}
        onValueChange={setActiveTab}
        className="w-full"
        tabs={[
          { value: "upload", label: "New Upload" },
          { value: "sources", label: "Data Sources" },
          { value: "history", label: "Upload History" },
          {
            value: "master",
            label: "Master Inventory",
            testId: "tab-master-inventory",
          },
          {
            value: "validator",
            label: "Global Validator",
            testId: "tab-global-validator",
          },
        ]}
      >
        <TabsContent value="upload" className="space-y-4">
          <div className="grid gap-6 md:grid-cols-2">
            <Card className="h-full">
              <CardHeader>
                <CardTitle>Source Configuration</CardTitle>
                <CardDescription>
                  Select how you want to import this file.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-3">
                  <Label>Import Method</Label>
                  <RadioGroup
                    defaultValue="manual"
                    onValueChange={setUploadMethod}
                    className="grid grid-cols-1 sm:grid-cols-3 gap-4"
                  >
                    <div>
                      <RadioGroupItem
                        value="manual"
                        id="manual"
                        className="peer sr-only"
                      />
                      <Label
                        htmlFor="manual"
                        className="flex flex-col items-center justify-between rounded-md border-2 border-muted bg-popover p-4 hover:bg-accent hover:text-accent-foreground peer-data-[state=checked]:border-primary [&:has([data-state=checked])]:border-primary"
                      >
                        <UploadCloud className="mb-3 h-6 w-6" />
                        Manual
                      </Label>
                    </div>
                    <div>
                      <RadioGroupItem
                        value="link"
                        id="link"
                        className="peer sr-only"
                      />
                      <Label
                        htmlFor="link"
                        className="flex flex-col items-center justify-between rounded-md border-2 border-muted bg-popover p-4 hover:bg-accent hover:text-accent-foreground peer-data-[state=checked]:border-primary [&:has([data-state=checked])]:border-primary"
                      >
                        <LinkIcon className="mb-3 h-6 w-6" />
                        URL Link
                      </Label>
                    </div>
                    <div>
                      <RadioGroupItem
                        value="email"
                        id="email"
                        className="peer sr-only"
                      />
                      <Label
                        htmlFor="email"
                        className="flex flex-col items-center justify-between rounded-md border-2 border-muted bg-popover p-4 hover:bg-accent hover:text-accent-foreground peer-data-[state=checked]:border-primary [&:has([data-state=checked])]:border-primary"
                      >
                        <Mail className="mb-3 h-6 w-6" />
                        Email
                      </Label>
                    </div>
                  </RadioGroup>
                </div>

                <div className="space-y-2">
                  <Label>Data Source</Label>
                  <Select
                    value={selectedDataSourceId}
                    onValueChange={setSelectedDataSourceId}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select data source..." />
                    </SelectTrigger>
                    <SelectContent className="max-h-[300px] overflow-y-auto">
                      {dataSources.map((source: DataSource) => (
                        <SelectItem key={source.id} value={source.id}>
                          {source.name}
                        </SelectItem>
                      ))}
                      {dataSources.length === 0 && (
                        <SelectItem value="none" disabled>
                          No data sources - create one first
                        </SelectItem>
                      )}
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    Select a data source to use its column mapping and cleaning
                    settings
                  </p>
                </div>

                <div className="space-y-2">
                  <Label>Update Strategy</Label>
                  {(() => {
                    const selectedSource = dataSources.find(
                      (s: DataSource) => s.id === selectedDataSourceId,
                    );
                    const strategy =
                      (selectedSource as any)?.updateStrategy || "replace";
                    const strategyLabel =
                      strategy === "full_sync"
                        ? "Full Sync (delete missing items)"
                        : "Create & Update (keep missing items)";
                    return (
                      <>
                        <div className="flex h-10 w-full items-center justify-between rounded-md border border-input bg-muted px-3 py-2 text-sm">
                          <span>{strategyLabel}</span>
                        </div>
                        <p className="text-xs text-muted-foreground">
                          {strategy === "full_sync"
                            ? "Items in master not in file will be deleted."
                            : "New items added, existing updated, missing items kept."}{" "}
                          Edit the data source to change this setting.
                        </p>
                      </>
                    );
                  })()}
                </div>
              </CardContent>
            </Card>

            <div className="h-full">
              {uploadMethod === "manual" && (
                <div className="space-y-4 h-full">
                  {/* Client-side Multi-File Mode Toggle */}
                  <div className="flex items-center justify-between border rounded-lg p-3 bg-muted/30">
                    <div className="flex items-center space-x-2">
                      <Switch
                        id="client-multi-file-mode"
                        data-testid="switch-client-multi-file-mode"
                        checked={clientMultiFileMode}
                        onCheckedChange={(checked) => {
                          setClientMultiFileMode(checked);
                          if (!checked) {
                            setClientStagedFiles([]);
                          }
                        }}
                      />
                      <Label
                        htmlFor="client-multi-file-mode"
                        className="flex items-center gap-2 cursor-pointer"
                      >
                        <Files className="h-4 w-4" />
                        Multi-File Mode
                      </Label>
                    </div>
                    {clientMultiFileMode && clientStagedFiles.length > 0 && (
                      <Badge
                        variant="secondary"
                        data-testid="badge-staged-files-count"
                      >
                        {clientStagedFiles.length} files staged
                      </Badge>
                    )}
                  </div>

                  {/* Client Staged Files List */}
                  {clientMultiFileMode && clientStagedFiles.length > 0 && (
                    <Card>
                      <CardHeader className="py-3">
                        <div className="flex items-center justify-between">
                          <CardTitle className="text-sm font-medium">
                            Staged Files ({clientStagedFiles.length})
                          </CardTitle>
                          <div className="flex gap-2">
                            <Button
                              variant="ghost"
                              size="sm"
                              data-testid="button-clear-staged-files"
                              onClick={() => {
                                setClientStagedFiles([]);
                                setPreviewFileName("");
                              }}
                              className="text-red-600 hover:text-red-700 hover:bg-red-50"
                            >
                              Clear All
                            </Button>
                            <Button
                              size="sm"
                              data-testid="button-import-staged-files"
                              onClick={handleClientMultiFileImport}
                              disabled={isImporting || !selectedDataSourceId}
                            >
                              {isImporting ? (
                                <>
                                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                                  Importing...
                                </>
                              ) : (
                                <>
                                  <Database className="mr-2 h-4 w-4" />
                                  Import {clientStagedFiles.length} Files
                                </>
                              )}
                            </Button>
                          </div>
                        </div>
                      </CardHeader>
                      <CardContent className="py-2">
                        <div className="space-y-2 max-h-40 overflow-y-auto">
                          {clientStagedFiles.map((file, idx) => (
                            <div
                              key={idx}
                              data-testid={`row-staged-file-${idx}`}
                              className="flex items-center justify-between p-2 bg-muted/50 rounded-md"
                            >
                              <div className="flex items-center gap-2">
                                <FileSpreadsheet className="h-4 w-4 text-green-600" />
                                <span
                                  className="text-sm truncate max-w-[200px]"
                                  data-testid={`text-staged-file-name-${idx}`}
                                >
                                  {file.name}
                                </span>
                                <Badge variant="secondary" className="text-xs">
                                  {(file.size / 1024).toFixed(0)} KB
                                </Badge>
                              </div>
                              <Button
                                variant="ghost"
                                size="sm"
                                data-testid={`button-remove-staged-file-${idx}`}
                                onClick={() => {
                                  const newFiles = clientStagedFiles.filter(
                                    (_, i) => i !== idx,
                                  );
                                  setClientStagedFiles(newFiles);
                                  if (newFiles.length === 0) {
                                    setPreviewFileName("");
                                  }
                                }}
                              >
                                <Trash2 className="h-4 w-4 text-muted-foreground hover:text-destructive" />
                              </Button>
                            </div>
                          ))}
                        </div>
                      </CardContent>
                    </Card>
                  )}

                  <div
                    {...getRootProps()}
                    className={`
                      border-2 border-dashed rounded-xl flex flex-col items-center justify-center p-10 text-center transition-colors cursor-pointer relative
                      ${isDragActive ? "border-primary bg-primary/5" : "border-border hover:border-primary/50 hover:bg-muted/50"}
                      ${previewFileName && !isDragActive && !clientMultiFileMode ? "bg-green-50/50 border-green-200 dark:border-green-800" : ""}
                      ${clientMultiFileMode ? "bg-blue-50/30 border-blue-200" : ""}
                    `}
                  >
                    <input {...getInputProps()} />
                    <div
                      className={`h-16 w-16 rounded-full flex items-center justify-center mb-4 ${
                        clientMultiFileMode
                          ? "bg-blue-100 text-blue-600"
                          : previewFileName
                            ? "bg-green-100 text-green-600"
                            : "bg-primary/10 text-primary"
                      }`}
                    >
                      {clientMultiFileMode ? (
                        <Files className="h-8 w-8" />
                      ) : previewFileName ? (
                        <FileSpreadsheet className="h-8 w-8" />
                      ) : (
                        <UploadCloud className="h-8 w-8" />
                      )}
                    </div>
                    <h3 className="text-lg font-semibold">
                      {isDragActive
                        ? "Drop the file here"
                        : clientMultiFileMode
                          ? "Add Files to Stage"
                          : previewFileName
                            ? "File Ready"
                            : "Drag & Drop Excel File"}
                    </h3>
                    <p className="text-sm text-muted-foreground mt-2 max-w-xs">
                      {clientMultiFileMode
                        ? `Click to add files. ${clientStagedFiles.length} file${clientStagedFiles.length !== 1 ? "s" : ""} staged.`
                        : previewFileName
                          ? previewFileName
                          : "Click anywhere or drag & drop. Supports .xlsx, .csv."}
                    </p>
                    {!selectedDataSourceId && !previewFileName && (
                      <p className="text-xs text-amber-600 mt-2 flex items-center gap-1">
                        <AlertTriangle className="h-3 w-3" />
                        Select a Data Source above to import files
                      </p>
                    )}
                    {/* Show target data source in both single and multi-file modes */}
                    {selectedDataSourceId && !isImporting && (
                      <p
                        className={`text-xs mt-2 ${clientMultiFileMode ? "text-blue-600" : "text-green-600"}`}
                      >
                        Will import to:{" "}
                        {
                          dataSources.find(
                            (s: DataSource) => s.id === selectedDataSourceId,
                          )?.name
                        }
                        {clientMultiFileMode &&
                          ` (${clientStagedFiles.length} files staged)`}
                        {!clientMultiFileMode &&
                          (
                            dataSources.find(
                              (s: DataSource) => s.id === selectedDataSourceId,
                            ) as any
                          )?.ingestionMode === "multi" &&
                          " (Multi-file mode)"}
                      </p>
                    )}
                    {previewFileName &&
                      !isImporting &&
                      !clientMultiFileMode && (
                        <div className="flex gap-2 mt-6">
                          <Button
                            variant="outline"
                            onClick={(e) => {
                              e.stopPropagation();
                              handlePreviewSource({
                                name: "Manual Upload",
                                type: "Manual",
                              } as any);
                            }}
                          >
                            Preview Content
                          </Button>
                        </div>
                      )}
                  </div>

                  {/* Upload Progress Bar - shown below the dropzone */}
                  {isImporting && (
                    <Card className="border-blue-200 bg-blue-50/50">
                      <CardContent className="py-4">
                        <div className="space-y-3">
                          <div className="flex items-center justify-between text-sm">
                            <span className="font-medium text-blue-800">
                              {uploadProgress < 100
                                ? "Uploading file..."
                                : "Processing file..."}
                            </span>
                            <span className="text-blue-600 font-semibold">
                              {uploadProgress}%
                            </span>
                          </div>
                          <Progress value={uploadProgress} className="h-3" />
                          <p className="text-xs text-blue-600 text-center">
                            {uploadProgress < 100
                              ? "Please wait while your file is being uploaded"
                              : "Parsing and importing data to inventory..."}
                          </p>
                        </div>
                      </CardContent>
                    </Card>
                  )}

                  {/* Staged Files for Multi-File Mode */}
                  {selectedDataSourceId &&
                    (
                      dataSources.find(
                        (s: DataSource) => s.id === selectedDataSourceId,
                      ) as any
                    )?.ingestionMode === "multi" && (
                      <Card>
                        <CardHeader className="py-3">
                          <div className="flex items-center justify-between">
                            <CardTitle className="text-sm font-medium">
                              Staged Files ({stagedFiles.length})
                            </CardTitle>
                            {stagedFiles.length > 0 && (
                              <Button
                                size="sm"
                                onClick={handleCombineImport}
                                disabled={isCombining}
                              >
                                {isCombining ? (
                                  <>
                                    <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                                    Combining...
                                  </>
                                ) : (
                                  <>
                                    <Database className="mr-2 h-4 w-4" />
                                    Combine & Import
                                  </>
                                )}
                              </Button>
                            )}
                          </div>
                        </CardHeader>
                        <CardContent className="py-2">
                          {stagedFiles.length === 0 ? (
                            <p className="text-sm text-muted-foreground text-center py-4">
                              No files staged yet. Upload files above to stage
                              them for combining.
                            </p>
                          ) : (
                            <div className="space-y-2">
                              {stagedFiles.map((file: any) => (
                                <div
                                  key={file.id}
                                  className="flex items-center justify-between p-2 bg-muted/50 rounded-md"
                                >
                                  <div className="flex items-center gap-2">
                                    <FileSpreadsheet className="h-4 w-4 text-muted-foreground" />
                                    <span className="text-sm">
                                      {file.fileName}
                                    </span>
                                    <Badge
                                      variant="secondary"
                                      className="text-xs"
                                    >
                                      {file.rowCount?.toLocaleString() || 0}{" "}
                                      rows
                                    </Badge>
                                  </div>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() =>
                                      handleDeleteStagedFile(file.id)
                                    }
                                  >
                                    <Trash2 className="h-4 w-4 text-muted-foreground hover:text-destructive" />
                                  </Button>
                                </div>
                              ))}
                            </div>
                          )}
                        </CardContent>
                      </Card>
                    )}
                </div>
              )}

              {uploadMethod === "link" && (
                <Card className="h-full flex flex-col justify-center">
                  <CardHeader>
                    <CardTitle>Import from URL</CardTitle>
                    <CardDescription>
                      Enter a direct download link to an Excel or CSV file.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="space-y-2">
                      <Label>File URL</Label>
                      <Input
                        placeholder="https://supplier.com/feeds/inventory.csv"
                        value={urlInput}
                        onChange={(e) => setUrlInput(e.target.value)}
                        data-testid="input-url"
                      />
                    </div>
                    {!selectedDataSourceId && (
                      <div className="p-3 bg-amber-50 border border-amber-200 rounded text-xs text-amber-700 flex gap-2">
                        <AlertTriangle className="h-4 w-4 shrink-0" />
                        Please select a data source above before fetching.
                      </div>
                    )}
                    <div className="p-3 bg-muted rounded text-xs text-muted-foreground flex gap-2">
                      <AlertTriangle className="h-4 w-4 shrink-0" />
                      Ensure the link is publicly accessible or includes an
                      authentication token.
                    </div>
                    <Button
                      className="w-full"
                      onClick={handleFetchUrl}
                      disabled={
                        isFetchingUrl ||
                        !selectedDataSourceId ||
                        !urlInput.trim()
                      }
                      data-testid="button-fetch-url"
                    >
                      {isFetchingUrl ? (
                        <>
                          <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                          Fetching...
                        </>
                      ) : (
                        <>
                          <LinkIcon className="mr-2 h-4 w-4" />
                          Fetch & Process
                        </>
                      )}
                    </Button>
                  </CardContent>
                </Card>
              )}

              {uploadMethod === "email" && (
                <Card className="h-full flex flex-col justify-center">
                  <CardHeader>
                    <CardTitle>Import via Email</CardTitle>
                    <CardDescription>
                      Send inventory files to this address to auto-process.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-6">
                    <div className="space-y-2 text-center">
                      <div className="p-4 bg-muted/50 rounded-lg border border-dashed border-primary/20">
                        <code className="text-lg font-mono text-primary select-all">
                          upload+nike@inventory-ai.mail.com
                        </code>
                      </div>
                      <p className="text-xs text-muted-foreground mt-2">
                        Files sent to this address will be automatically mapped
                        using the selected Supplier Template.
                      </p>
                    </div>
                    <div className="space-y-2">
                      <Label>Allowed Senders (Whitelist)</Label>
                      <Input placeholder="inventory@nike.com, supply@adidas.com" />
                    </div>
                    <Button variant="outline" className="w-full">
                      <CheckCircle className="mr-2 h-4 w-4" />
                      Save Email Configuration
                    </Button>
                  </CardContent>
                </Card>
              )}
            </div>
          </div>

          {/* File Preview Section - Shows when file is uploaded */}
          {previewHeaders.length > 0 && previewFileName && (
            <Card className="mt-6">
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="h-10 w-10 rounded-lg bg-green-100 flex items-center justify-center">
                      <FileSpreadsheet className="h-5 w-5 text-green-600" />
                    </div>
                    <div>
                      <CardTitle className="text-lg">
                        {previewFileName}
                      </CardTitle>
                      <CardDescription>
                        {previewRows.length} rows, {previewHeaders.length}{" "}
                        columns
                        {isPivotedFormat &&
                          " - Pivoted Format (sizes as columns)"}
                      </CardDescription>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {isPivotedFormat && (
                      <Badge className="bg-purple-600">Pivoted Format</Badge>
                    )}
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={runAiMapping}
                      disabled={aiMappingStatus === "scanning"}
                    >
                      {aiMappingStatus === "scanning" ? (
                        <>
                          <RefreshCw className="mr-2 h-3 w-3 animate-spin" />
                          Analyzing...
                        </>
                      ) : aiMappingStatus === "completed" ? (
                        <>
                          <CheckCircle className="mr-2 h-3 w-3 text-green-600" />
                          AI Mapped
                        </>
                      ) : (
                        <>
                          <Wand2 className="mr-2 h-3 w-3" />
                          AI Auto-Map
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {/* Detected Size Columns for Pivoted Format */}
                {isPivotedFormat && sizeColumns.length > 0 && (
                  <div className="bg-purple-50 border border-purple-200 rounded-lg p-3 mb-4">
                    <div className="flex items-center gap-2 mb-2">
                      <span className="text-xs text-purple-700 font-medium">
                        Detected size columns:
                      </span>
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {sizeColumns.slice(0, 20).map((size, i) => (
                        <Badge
                          key={i}
                          variant="outline"
                          className="text-[10px] bg-white dark:bg-gray-800 border-purple-300 text-purple-700"
                        >
                          {size}
                        </Badge>
                      ))}
                      {sizeColumns.length > 20 && (
                        <Badge
                          variant="outline"
                          className="text-[10px] bg-white dark:bg-gray-800 border-purple-300 text-purple-700"
                        >
                          +{sizeColumns.length - 20} more
                        </Badge>
                      )}
                    </div>
                    <div className="flex items-center gap-2 mt-3 pt-3 border-t border-purple-200">
                      <input
                        type="checkbox"
                        id="autoDetectSizeHeaders"
                        checked={autoDetectSizeHeaders}
                        onChange={(e) =>
                          setAutoDetectSizeHeaders(e.target.checked)
                        }
                        className="h-4 w-4 rounded border-purple-300 text-purple-600 focus:ring-purple-500"
                      />
                      <label
                        htmlFor="autoDetectSizeHeaders"
                        className="text-xs text-purple-700"
                      >
                        Auto-detect sizes from each style row (supports mixed
                        numeric & letter sizes)
                      </label>
                    </div>
                  </div>
                )}

                {/* Raw Data Table */}
                <div className="rounded-md border overflow-hidden">
                  <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50 sticky top-0 z-10">
                        <tr className="border-b">
                          <th className="h-10 px-3 text-left font-medium text-muted-foreground w-12 bg-muted/50">
                            #
                          </th>
                          {previewHeaders.map((header, idx) => (
                            <th
                              key={idx}
                              className="h-10 px-3 text-left font-medium text-muted-foreground whitespace-nowrap bg-muted/50"
                            >
                              <div className="flex items-center gap-1">
                                {header}
                                {sizeColumns.includes(header) && (
                                  <span className="text-[10px] text-purple-500 font-normal">
                                    (size)
                                  </span>
                                )}
                                {columnMapping.sku === header && (
                                  <Badge
                                    variant="outline"
                                    className="text-[8px] h-4 px-1 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border-blue-200 ml-1"
                                  >
                                    SKU
                                  </Badge>
                                )}
                                {columnMapping.style === header && (
                                  <Badge
                                    variant="outline"
                                    className="text-[8px] h-4 px-1 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border-blue-200 ml-1"
                                  >
                                    Style
                                  </Badge>
                                )}
                                {columnMapping.color === header && (
                                  <Badge
                                    variant="outline"
                                    className="text-[8px] h-4 px-1 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border-blue-200 ml-1"
                                  >
                                    Color
                                  </Badge>
                                )}
                              </div>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {previewRows.map((row, i) => (
                          <tr
                            key={i}
                            className="border-b last:border-0 hover:bg-muted/20"
                          >
                            <td className="p-3 font-mono text-xs text-muted-foreground">
                              {i + 1}
                            </td>
                            {row.map((cell: any, j: number) => (
                              <td key={j} className="p-3 whitespace-nowrap">
                                {cell === "Yes" ? (
                                  <Badge
                                    variant="outline"
                                    className="text-[10px] h-5 bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300 border-green-200 dark:border-green-800"
                                  >
                                    Yes
                                  </Badge>
                                ) : cell === "No" ? (
                                  <Badge
                                    variant="outline"
                                    className="text-[10px] h-5 bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-300 border-red-200"
                                  >
                                    No
                                  </Badge>
                                ) : cell === "Last Piece" ? (
                                  <Badge
                                    variant="outline"
                                    className="text-[10px] h-5 bg-orange-50 text-orange-700 border-orange-200"
                                  >
                                    Last Piece
                                  </Badge>
                                ) : (
                                  String(cell ?? "-")
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <p className="text-xs text-muted-foreground mt-3">
                  Showing {previewRows.length} preview rows. Full file will be
                  processed on import.
                </p>
              </CardContent>
            </Card>
          )}

          {/* Rule Engine Section - Only show on New Upload tab when data source is selected */}
          {selectedDataSourceId && (
            <RuleEngineSection
              selectedDataSourceId={selectedDataSourceId}
              dataSources={dataSources}
            />
          )}
        </TabsContent>

        <TabsContent value="sources">
          <Card>
            <CardHeader className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
              <div>
                <CardTitle>Company Data Sources</CardTitle>
                <CardDescription>
                  Manage automated feeds from your suppliers.
                </CardDescription>
              </div>
              <div className="flex gap-2 flex-wrap">
                <Button
                  onClick={() => {
                    setAiDialogDataSource(null);
                    setIsAISourceDialogOpen(true);
                  }}
                  variant="secondary"
                  className="bg-blue-50 dark:bg-blue-900/30 hover:bg-blue-100 text-blue-700 dark:text-blue-300 border-blue-200"
                >
                  <Sparkles className="mr-2 h-4 w-4" />
                  Add using AI
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {isLoading ? (
                  <div className="text-center py-8 text-muted-foreground">
                    Loading data sources...
                  </div>
                ) : dataSources.length === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    No data sources yet. Click "Add using AI" to create one.
                  </div>
                ) : (
                  dataSources.map((source: DataSource) => {
                    const typeLabel =
                      source.type === "url"
                        ? "URL Feed"
                        : source.type === "email"
                          ? "Email"
                          : "Manual";
                    const displayStatus =
                      source.status === "active" ? "Active" : "Inactive";
                    const scheduleLabel = source.autoUpdate
                      ? `${source.updateFrequency?.charAt(0).toUpperCase()}${source.updateFrequency?.slice(1)} ${source.updateFrequency === "daily" && source.updateTime ? `@ ${source.updateTime}` : ""}`
                      : "Manual";

                    return (
                      <div
                        key={source.id}
                        className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4 p-4 border rounded-lg hover:bg-muted/50 transition-colors"
                      >
                        <div className="flex items-center gap-4">
                          <div className="h-10 w-10 rounded-full bg-primary/10 flex items-center justify-center text-primary font-bold">
                            {source.name.charAt(0).toUpperCase()}
                          </div>
                          <div>
                            <h4 className="font-semibold">{source.name}</h4>
                            <div className="flex items-center gap-2 text-sm text-muted-foreground">
                              {source.type === "email" && (
                                <Mail className="h-3 w-3" />
                              )}
                              {source.type === "url" && (
                                <LinkIcon className="h-3 w-3" />
                              )}
                              {source.type === "manual" && (
                                <UploadCloud className="h-3 w-3" />
                              )}
                              <span>{typeLabel}</span>
                            </div>
                          </div>
                        </div>
                        <div className="flex flex-wrap items-center gap-4 sm:gap-6">
                          <div className="text-left sm:text-right flex flex-col sm:items-end">
                            <Badge
                              variant={
                                source.status === "active"
                                  ? "default"
                                  : "secondary"
                              }
                            >
                              {displayStatus}
                            </Badge>
                            <div className="flex items-center gap-1 text-xs text-muted-foreground mt-1">
                              <Clock className="h-3 w-3" />
                              {scheduleLabel}
                            </div>
                          </div>
                          <div className="flex gap-2 flex-wrap">
                            {source.type === "email" && (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleFetchEmail(source.id)}
                                disabled={
                                  isFetchingEmail &&
                                  fetchingEmailDataSourceId === source.id
                                }
                                data-testid={`button-fetch-email-${source.id}`}
                              >
                                {isFetchingEmail &&
                                fetchingEmailDataSourceId === source.id ? (
                                  <>
                                    <RefreshCw className="mr-2 h-3 w-3 animate-spin" />
                                    Fetching...
                                  </>
                                ) : (
                                  <>
                                    <Mail className="mr-2 h-3 w-3" />
                                    Fetch Emails
                                  </>
                                )}
                              </Button>
                            )}
                            {source.type === "url" && (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() =>
                                  handleFetchUrlForDataSource(source.id)
                                }
                                disabled={
                                  isFetchingUrl &&
                                  fetchingUrlDataSourceId === source.id
                                }
                                data-testid={`button-fetch-url-${source.id}`}
                              >
                                {isFetchingUrl &&
                                fetchingUrlDataSourceId === source.id ? (
                                  <>
                                    <RefreshCw className="mr-2 h-3 w-3 animate-spin" />
                                    Fetching...
                                  </>
                                ) : (
                                  <>
                                    <LinkIcon className="mr-2 h-3 w-3" />
                                    Fetch URL
                                  </>
                                )}
                              </Button>
                            )}
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => handlePreviewSource(source)}
                            >
                              <FileSpreadsheet className="mr-2 h-3 w-3" />
                              Preview
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => {
                                setAiDialogDataSource(source);
                                setIsAISourceDialogOpen(true);
                              }}
                            >
                              Manage
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="text-red-600 hover:text-red-700 hover:bg-red-50"
                              onClick={() => {
                                if (
                                  window.confirm(
                                    `Are you sure you want to delete "${source.name}"?`,
                                  )
                                ) {
                                  deleteDataSourceMutation.mutate(source.id);
                                }
                              }}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="history">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <div>
                <CardTitle>Recent Uploads</CardTitle>
                <CardDescription>
                  Upload history across all data sources
                </CardDescription>
              </div>
              {recentUploads.length > 0 && (
                <Button
                  variant="outline"
                  size="sm"
                  className="text-red-600 hover:text-red-700 hover:bg-red-50"
                  onClick={() => {
                    if (
                      window.confirm(
                        "Are you sure you want to clear all upload history?",
                      )
                    ) {
                      clearHistoryMutation.mutate();
                    }
                  }}
                  disabled={clearHistoryMutation.isPending}
                  data-testid="button-clear-history"
                >
                  <Trash2 className="mr-2 h-4 w-4" />
                  {clearHistoryMutation.isPending
                    ? "Clearing..."
                    : "Clear History"}
                </Button>
              )}
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {recentUploads.length === 0 ? (
                  <div className="text-center py-8 text-muted-foreground">
                    No uploads yet. Upload a file to a data source to see it
                    here.
                  </div>
                ) : (
                  recentUploads.map((file: any) => {
                    const uploadDate = file.uploadedAt
                      ? new Date(file.uploadedAt)
                      : null;
                    const now = new Date();
                    let dateLabel = "Unknown";
                    if (uploadDate) {
                      const diffMs = now.getTime() - uploadDate.getTime();
                      const diffDays = Math.floor(
                        diffMs / (1000 * 60 * 60 * 24),
                      );
                      if (diffDays === 0) {
                        dateLabel = `Today, ${uploadDate.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
                      } else if (diffDays === 1) {
                        dateLabel = `Yesterday, ${uploadDate.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
                      } else {
                        dateLabel = uploadDate.toLocaleDateString([], {
                          month: "short",
                          day: "numeric",
                          year: "numeric",
                        });
                      }
                    }

                    const sourceType = file.sourceType || "manual";
                    const importStatus = file.importStatus || "success";

                    const getSourceIcon = () => {
                      switch (sourceType) {
                        case "email":
                          return <Mail className="h-5 w-5" />;
                        case "url":
                          return <LinkIcon className="h-5 w-5" />;
                        default:
                          return <Upload className="h-5 w-5" />;
                      }
                    };

                    const getSourceLabel = () => {
                      switch (sourceType) {
                        case "email":
                          return "Email";
                        case "url":
                          return "URL";
                        default:
                          return "Manual";
                      }
                    };

                    const getStatusBadge = () => {
                      switch (importStatus) {
                        case "error":
                          return (
                            <Badge variant="destructive" className="bg-red-500">
                              Error
                            </Badge>
                          );
                        case "rejected":
                          return (
                            <Badge
                              variant="outline"
                              className="border-orange-500 text-orange-600 bg-orange-50"
                            >
                              Rejected
                            </Badge>
                          );
                        default:
                          return (
                            <Badge variant="default" className="bg-green-500">
                              Success
                            </Badge>
                          );
                      }
                    };

                    return (
                      <div
                        key={file.id}
                        className="flex items-center justify-between border-b border-border pb-4 last:border-0 last:pb-0"
                      >
                        <div className="flex items-center gap-4">
                          <div
                            className={`h-10 w-10 rounded-lg flex items-center justify-center ${
                              sourceType === "email"
                                ? "bg-purple-100 text-purple-600 dark:bg-purple-900/30 dark:text-purple-400"
                                : sourceType === "url"
                                  ? "bg-blue-100 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400"
                                  : "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400"
                            }`}
                          >
                            {getSourceIcon()}
                          </div>
                          <div>
                            <p className="font-medium">{file.fileName}</p>
                            <div className="flex items-center gap-2 text-xs text-muted-foreground">
                              <span>{dateLabel}</span>
                              <span className="text-muted-foreground/50">
                                •
                              </span>
                              <span
                                className={`${
                                  sourceType === "email"
                                    ? "text-purple-600 dark:text-purple-400"
                                    : sourceType === "url"
                                      ? "text-blue-600 dark:text-blue-400"
                                      : "text-gray-600 dark:text-gray-400"
                                }`}
                              >
                                {getSourceLabel()}
                              </span>
                            </div>
                            <p className="text-xs text-primary">
                              {file.dataSourceName}
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center gap-4">
                          <div className="text-right">
                            <p className="text-sm font-medium">
                              {(file.rowCount || 0).toLocaleString()} Records
                            </p>
                            {file.fileSize && (
                              <p className="text-xs text-muted-foreground">
                                {(file.fileSize / 1024).toFixed(1)} KB
                              </p>
                            )}
                          </div>
                          {getStatusBadge()}
                        </div>
                        {file.importError && (
                          <div
                            className="ml-4 text-xs text-red-500 max-w-[200px] truncate"
                            title={file.importError}
                          >
                            {file.importError}
                          </div>
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="master">
          <MasterInventoryTab />
        </TabsContent>

        <TabsContent value="validator">
          <GlobalValidatorDashboard />
        </TabsContent>
      </ResponsiveTabs>

      {/* Source Editor Dialog */}
      <Dialog open={isSourceDialogOpen} onOpenChange={setIsSourceDialogOpen}>
        <DialogContent className="sm:max-w-[900px] w-[95vw] max-h-[95vh] overflow-y-auto overflow-x-auto">
          <DialogHeader>
            <DialogTitle>
              {editingSource
                ? `Edit Source: ${editingSource.name}`
                : "Add New Data Source"}
            </DialogTitle>
            <DialogDescription>
              Configure connection details, schedule, and column mapping for
              this supplier.
            </DialogDescription>
          </DialogHeader>

          <Tabs defaultValue="details" className="w-full mt-4">
            <TabsList className="w-full grid grid-cols-2 md:grid-cols-4 gap-1">
              <TabsTrigger value="details">Connection</TabsTrigger>
              <TabsTrigger value="schedule">Schedule</TabsTrigger>
              <TabsTrigger value="mapping">Mapping</TabsTrigger>
              <TabsTrigger value="importRules">Import Rules</TabsTrigger>
            </TabsList>

            <div className="py-4 space-y-4 overflow-y-auto max-h-[calc(95vh-200px)]">
              <TabsContent value="details" className="space-y-4 mt-0">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label>Source Name</Label>
                    <Input
                      placeholder="e.g. Nike Inc."
                      value={sourceName}
                      onChange={(e) => setSourceName(e.target.value)}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Source Type</Label>
                    <Select onValueChange={setSourceType} value={sourceType}>
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="url">URL Feed (Link)</SelectItem>
                        <SelectItem value="email">Email Attachment</SelectItem>
                        <SelectItem value="manual">
                          Manual Upload Only
                        </SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {sourceType === "url" && (
                  <div className="space-y-2">
                    <Label>Connection URL</Label>
                    <div className="flex gap-2">
                      <Input
                        placeholder="https://..."
                        value={sourceUrl}
                        onChange={(e) => setSourceUrl(e.target.value)}
                        className="flex-1"
                        data-testid="input-source-url"
                      />
                      <Button
                        variant="secondary"
                        onClick={runTestConnection}
                        disabled={
                          testStatus === "testing" || testStatus === "success"
                        }
                        data-testid="button-test-connection"
                      >
                        {testStatus === "testing" ? (
                          <>
                            <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                            Testing...
                          </>
                        ) : testStatus === "success" ? (
                          <>
                            <CheckCircle className="mr-2 h-4 w-4 text-green-600" />
                            Working
                          </>
                        ) : (
                          <>
                            <PlayCircle className="mr-2 h-4 w-4" />
                            Test
                          </>
                        )}
                      </Button>
                    </div>
                    {testStatus === "success" && (
                      <p className="text-xs text-green-600 flex items-center animate-in fade-in">
                        <CheckCircle className="h-3 w-3 mr-1" />
                        Successfully connected to source.
                      </p>
                    )}
                  </div>
                )}

                {sourceType === "email" && (
                  <div className="space-y-4 border rounded-lg p-4 bg-muted/30">
                    <div className="flex items-center gap-2 text-sm font-medium">
                      <Mail className="h-4 w-4" />
                      Email Server Configuration (IMAP)
                    </div>

                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-2">
                        <Label>IMAP Server Host</Label>
                        <Input
                          placeholder="imap.gmail.com"
                          value={emailHost}
                          onChange={(e) => setEmailHost(e.target.value)}
                          data-testid="input-email-host"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>Port</Label>
                        <Input
                          type="number"
                          placeholder="993"
                          value={emailPort}
                          onChange={(e) =>
                            setEmailPort(parseInt(e.target.value) || 993)
                          }
                          data-testid="input-email-port"
                        />
                      </div>
                    </div>

                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-2">
                        <Label>Email / Username</Label>
                        <Input
                          placeholder="your-email@company.com"
                          value={emailUsername}
                          onChange={(e) => setEmailUsername(e.target.value)}
                          data-testid="input-email-username"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>Password / App Password</Label>
                        <Input
                          type="password"
                          placeholder="App-specific password"
                          value={emailPassword}
                          onChange={(e) => setEmailPassword(e.target.value)}
                          data-testid="input-email-password"
                        />
                      </div>
                    </div>

                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-2">
                        <Label>Folder</Label>
                        <Input
                          placeholder="INBOX"
                          value={emailFolder}
                          onChange={(e) => setEmailFolder(e.target.value)}
                          data-testid="input-email-folder"
                        />
                      </div>
                      <div className="flex items-center space-x-2 pt-6">
                        <Switch
                          id="email-secure"
                          checked={emailSecure}
                          onCheckedChange={setEmailSecure}
                          data-testid="switch-email-secure"
                        />
                        <Label htmlFor="email-secure">Use SSL/TLS</Label>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <Label>Allowed Senders (comma-separated)</Label>
                      <Input
                        placeholder="supplier@company.com, vendor@example.com"
                        value={emailSenderWhitelist}
                        onChange={(e) =>
                          setEmailSenderWhitelist(e.target.value)
                        }
                        data-testid="input-email-whitelist"
                      />
                      <p className="text-xs text-muted-foreground">
                        Only process emails from these addresses. Leave empty to
                        accept from anyone.
                      </p>
                    </div>

                    <div className="space-y-2">
                      <Label>Subject Filter (optional)</Label>
                      <Input
                        placeholder="Inventory Update"
                        value={emailSubjectFilter}
                        onChange={(e) => setEmailSubjectFilter(e.target.value)}
                        data-testid="input-email-subject"
                      />
                      <p className="text-xs text-muted-foreground">
                        Only process emails with subjects containing this text.
                      </p>
                    </div>

                    <div className="flex items-center space-x-2">
                      <Switch
                        id="email-mark-read"
                        checked={emailMarkAsRead}
                        onCheckedChange={setEmailMarkAsRead}
                        data-testid="switch-email-mark-read"
                      />
                      <Label htmlFor="email-mark-read">
                        Mark emails as read after processing
                      </Label>
                    </div>
                  </div>
                )}

                <div className="flex items-center space-x-2 border p-3 rounded-md">
                  <Switch
                    id="active-mode"
                    checked={sourceActive}
                    onCheckedChange={setSourceActive}
                  />
                  <Label htmlFor="active-mode">Source Active</Label>
                </div>

                <div className="space-y-2">
                  <Label>File Upload Mode</Label>
                  <Select
                    onValueChange={setIngestionMode}
                    value={ingestionMode}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="single">
                        Single File (replace existing data)
                      </SelectItem>
                      <SelectItem value="multi">
                        Multi-File (combine multiple files)
                      </SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    {ingestionMode === "multi"
                      ? "Upload multiple files and combine them before importing. Files will be staged until you click 'Combine & Import'."
                      : "Each uploaded file will replace the previous data for this source."}
                  </p>
                </div>

                <div className="space-y-2">
                  <Label>Update Strategy</Label>
                  <Select
                    onValueChange={setUpdateStrategy}
                    value={updateStrategy}
                  >
                    <SelectTrigger data-testid="select-update-strategy">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="replace">
                        Create & Update (keep missing items)
                      </SelectItem>
                      <SelectItem value="full_sync">
                        Full Sync (delete missing items)
                      </SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    {updateStrategy === "full_sync"
                      ? "Items in master inventory that are NOT in the file will be deleted. The file becomes the source of truth."
                      : "Add new items, update existing items, but keep items in master that are not in the file."}
                  </p>
                </div>

                <div className="flex justify-between items-center pt-4 border-t">
                  {editingSource ? (
                    <Button variant="outline" onClick={handleSaveAsTemplate}>
                      <Copy className="mr-2 h-4 w-4" />
                      Save as Template
                    </Button>
                  ) : (
                    <div />
                  )}
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      onClick={() => setIsSourceDialogOpen(false)}
                    >
                      Cancel
                    </Button>
                    {editingSource ? (
                      <Button onClick={handleSaveConnection}>
                        <Save className="mr-2 h-4 w-4" />
                        Save Connection
                      </Button>
                    ) : (
                      <Button
                        onClick={handleCreateDataSource}
                        disabled={createDataSourceMutation.isPending}
                        data-testid="button-create-data-source"
                      >
                        {createDataSourceMutation.isPending ? (
                          <>
                            <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                            Creating...
                          </>
                        ) : (
                          <>
                            <Plus className="mr-2 h-4 w-4" />
                            Create Data Source
                          </>
                        )}
                      </Button>
                    )}
                  </div>
                </div>
              </TabsContent>

              <TabsContent value="schedule" className="space-y-4 mt-0">
                <div className="space-y-4">
                  <div className="flex items-center justify-between border-b pb-4">
                    <div className="space-y-0.5">
                      <Label className="text-base">Automatic Updates</Label>
                      <p className="text-sm text-muted-foreground">
                        Fetch inventory levels automatically
                      </p>
                    </div>
                    <Switch
                      checked={autoUpdate}
                      onCheckedChange={setAutoUpdate}
                    />
                  </div>

                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="space-y-2">
                      <Label>Update Frequency</Label>
                      <Select
                        onValueChange={setUpdateFreq}
                        value={updateFreq}
                        disabled={!autoUpdate}
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
                    <div className="space-y-2">
                      <Label>Time (PST)</Label>
                      <Input
                        type="time"
                        value={updateTime}
                        onChange={(e) => setUpdateTime(e.target.value)}
                        disabled={!autoUpdate}
                      />
                    </div>
                  </div>
                </div>

                {editingSource ? (
                  <div className="flex justify-end gap-2 pt-4 border-t">
                    <Button
                      variant="outline"
                      onClick={() => setIsSourceDialogOpen(false)}
                    >
                      Cancel
                    </Button>
                    <Button onClick={handleSaveSchedule}>
                      <Save className="mr-2 h-4 w-4" />
                      Save Schedule
                    </Button>
                  </div>
                ) : (
                  <div className="bg-muted/50 rounded-lg p-4 text-center border-t mt-4">
                    <p className="text-sm text-muted-foreground">
                      Create the data source first in the Connection Details tab
                      to configure schedule settings.
                    </p>
                  </div>
                )}
              </TabsContent>

              <TabsContent value="mapping" className="space-y-4 mt-0">
                {/* AI Mapping Header */}
                <div className="flex items-center justify-between bg-blue-50/50 p-3 rounded-lg border border-blue-100">
                  <div className="flex items-center gap-3">
                    <div className="h-8 w-8 rounded-full bg-blue-100 flex items-center justify-center text-blue-600">
                      <Wand2 className="h-4 w-4" />
                    </div>
                    <div>
                      <p className="text-sm font-medium text-blue-900">
                        AI Column Recognition
                      </p>
                      <p className="text-xs text-blue-700 dark:text-blue-300">
                        Auto-detect SKU, Stock, and Attributes
                      </p>
                    </div>
                  </div>
                  <Button
                    size="sm"
                    onClick={runAiMapping}
                    disabled={aiMappingStatus === "scanning"}
                    className={
                      aiMappingStatus === "completed"
                        ? "bg-green-600 hover:bg-green-700"
                        : "bg-blue-600 hover:bg-blue-700"
                    }
                  >
                    {aiMappingStatus === "scanning" ? (
                      <>
                        <RefreshCw className="mr-2 h-3 w-3 animate-spin" />
                        Scanning...
                      </>
                    ) : aiMappingStatus === "completed" ? (
                      <>
                        <Sparkles className="mr-2 h-3 w-3" />
                        Mapped!
                      </>
                    ) : (
                      <>
                        <Wand2 className="mr-2 h-3 w-3" />
                        Auto-Map Columns
                      </>
                    )}
                  </Button>
                </div>

                {/* Raw File Preview */}
                {previewHeaders.length > 0 && (
                  <div className="bg-muted/30 p-4 rounded-lg border space-y-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <FileSpreadsheet className="h-4 w-4 text-green-600" />
                        <h4 className="font-semibold text-sm">
                          File Preview: {previewFileName}
                        </h4>
                      </div>
                      {isPivotedFormat && (
                        <Badge className="bg-purple-100 text-purple-700 border-purple-200">
                          Pivoted Format (Sizes as Columns)
                        </Badge>
                      )}
                    </div>
                    <div className="rounded-md border overflow-x-auto max-h-[200px] overflow-y-auto">
                      <table className="w-full text-xs">
                        <thead className="bg-muted/50 sticky top-0">
                          <tr className="border-b">
                            {previewHeaders.slice(0, 12).map((header, idx) => (
                              <th
                                key={idx}
                                className="h-8 px-2 text-left font-medium text-muted-foreground whitespace-nowrap"
                              >
                                {header}
                                {sizeColumns.includes(header) && (
                                  <span className="ml-1 text-purple-500">
                                    (size)
                                  </span>
                                )}
                              </th>
                            ))}
                            {previewHeaders.length > 12 && (
                              <th className="h-8 px-2 text-left font-medium text-muted-foreground">
                                ...+{previewHeaders.length - 12} more
                              </th>
                            )}
                          </tr>
                        </thead>
                        <tbody>
                          {previewRows.slice(0, 5).map((row, i) => (
                            <tr
                              key={i}
                              className="border-b last:border-0 hover:bg-muted/20"
                            >
                              {row.slice(0, 12).map((cell: any, j: number) => (
                                <td key={j} className="p-2 whitespace-nowrap">
                                  {cell === "Yes" ? (
                                    <Badge
                                      variant="outline"
                                      className="text-[10px] h-5 bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300 border-green-200 dark:border-green-800"
                                    >
                                      Yes
                                    </Badge>
                                  ) : cell === "No" ? (
                                    <Badge
                                      variant="outline"
                                      className="text-[10px] h-5 bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-300 border-red-200"
                                    >
                                      No
                                    </Badge>
                                  ) : cell === "Last Piece" ? (
                                    <Badge
                                      variant="outline"
                                      className="text-[10px] h-5 bg-orange-50 text-orange-700 border-orange-200"
                                    >
                                      Last
                                    </Badge>
                                  ) : (
                                    String(cell || "-")
                                  )}
                                </td>
                              ))}
                              {row.length > 12 && (
                                <td className="p-2 text-muted-foreground">
                                  ...
                                </td>
                              )}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <p className="text-xs text-muted-foreground">
                      Showing {Math.min(5, previewRows.length)} of{" "}
                      {previewRows.length} rows, {previewHeaders.length} columns
                    </p>
                  </div>
                )}

                <div className="bg-muted/30 p-4 rounded-lg border space-y-4">
                  <div className="flex items-center justify-between">
                    <h4 className="font-semibold text-sm">
                      Excel Column Mapping
                    </h4>
                    <div>
                      <Label
                        htmlFor="mapping-upload"
                        className="cursor-pointer inline-flex items-center justify-center whitespace-nowrap rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-9 rounded-md px-3"
                      >
                        <FileSpreadsheet className="mr-2 h-3 w-3" />
                        Load from File
                      </Label>
                      <Input
                        id="mapping-upload"
                        type="file"
                        className="hidden"
                        onChange={(e) => {
                          if (e.target.files && e.target.files.length > 0) {
                            processFile(e.target.files[0]);
                          }
                        }}
                      />
                    </div>
                  </div>
                  <Separator />

                  {/* Pivoted Format Notice */}
                  {isPivotedFormat && (
                    <div className="bg-purple-50 border border-purple-200 rounded-lg p-3 space-y-2">
                      <div className="flex items-center gap-2">
                        <Badge className="bg-purple-600">Pivoted Format</Badge>
                        <span className="text-xs text-purple-700 font-medium">
                          Size columns detected as headers
                        </span>
                      </div>
                      <p className="text-xs text-purple-600">
                        This file has sizes as column headers. Each row
                        represents a Style + Color, and availability
                        (Yes/No/Last Piece) is in each size column.
                      </p>
                      {sizeColumns.length > 0 && (
                        <div className="flex flex-wrap gap-1 mt-2">
                          <span className="text-xs text-purple-700 font-medium">
                            Detected sizes:
                          </span>
                          {sizeColumns.slice(0, 15).map((size, i) => (
                            <Badge
                              key={i}
                              variant="outline"
                              className="text-[10px] bg-white dark:bg-gray-800 border-purple-300 text-purple-700"
                            >
                              {size}
                            </Badge>
                          ))}
                          {sizeColumns.length > 15 && (
                            <Badge
                              variant="outline"
                              className="text-[10px] bg-white dark:bg-gray-800 border-purple-300 text-purple-700"
                            >
                              +{sizeColumns.length - 15} more
                            </Badge>
                          )}
                        </div>
                      )}
                      <div className="flex items-center gap-2 mt-3 pt-3 border-t border-purple-200">
                        <input
                          type="checkbox"
                          id="autoDetectSizeHeadersMapping"
                          checked={autoDetectSizeHeaders}
                          onChange={(e) =>
                            setAutoDetectSizeHeaders(e.target.checked)
                          }
                          className="h-4 w-4 rounded border-purple-300 text-purple-600 focus:ring-purple-500"
                        />
                        <label
                          htmlFor="autoDetectSizeHeadersMapping"
                          className="text-xs text-purple-700"
                        >
                          Auto-detect sizes from each style row (supports mixed
                          numeric & letter sizes)
                        </label>
                      </div>
                    </div>
                  )}

                  <div className="grid gap-3">
                    {[
                      {
                        key: "sku",
                        label: "Product Code (SKU)",
                        required: true,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "style",
                        label: "Style (Product Name)",
                        required: true,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "size",
                        label: "Size",
                        required: true,
                        hiddenOnPivot: true,
                      },
                      {
                        key: "color",
                        label: "Color",
                        required: true,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "stock",
                        label: "Availability (Stock)",
                        required: true,
                        hiddenOnPivot: true,
                      },
                      {
                        key: "cost",
                        label: "Cost Price",
                        required: false,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "price",
                        label: "Selling Price",
                        required: false,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "shipDate",
                        label: "Ship Date",
                        required: false,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "discontinued",
                        label: "Discontinued Status",
                        required: false,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "salePrice",
                        label: "Sale Price",
                        required: false,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "futureStock",
                        label: "Future Stock (Incoming Qty)",
                        required: false,
                        hiddenOnPivot: false,
                      },
                      {
                        key: "futureDate",
                        label: "Future Stock Date",
                        required: false,
                        hiddenOnPivot: false,
                      },
                    ]
                      .filter(
                        (field) => !(isPivotedFormat && field.hiddenOnPivot),
                      )
                      .map((field) => (
                        <div
                          key={field.key}
                          className="grid grid-cols-12 gap-2 items-center"
                        >
                          <div className="col-span-4 flex items-center gap-2">
                            <Label className="text-xs font-medium">
                              {field.label}
                            </Label>
                            {field.required && (
                              <span className="text-red-500 text-[10px]">
                                *
                              </span>
                            )}
                            {aiMappingStatus === "completed" &&
                              columnMapping[
                                field.key as keyof typeof columnMapping
                              ] && (
                                <Badge
                                  variant="outline"
                                  className="text-[10px] h-4 px-1 bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300 border-green-200 dark:border-green-800"
                                >
                                  Detected
                                </Badge>
                              )}
                          </div>
                          <Select
                            value={
                              columnMapping[
                                field.key as keyof typeof columnMapping
                              ]
                            }
                            onValueChange={(val) =>
                              setColumnMapping({
                                ...columnMapping,
                                [field.key]: val,
                              })
                            }
                          >
                            <SelectTrigger className="col-span-8 h-8">
                              <SelectValue placeholder="Select Column..." />
                            </SelectTrigger>
                            <SelectContent className="max-h-60 overflow-y-auto">
                              {(previewHeaders.length > 0
                                ? previewHeaders.filter(
                                    (h) => h && String(h).trim() !== "",
                                  )
                                : [
                                    "Item_ID (SKU)",
                                    "Description",
                                    "Variant_Color",
                                    "Variant_Size",
                                    "MSRP",
                                    "Cost_Price",
                                    "Qty_Available",
                                    "Status",
                                  ]
                              ).map((header, idx) => (
                                <SelectItem key={idx} value={String(header)}>
                                  {String(header)}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                      ))}
                  </div>
                </div>

                {editingSource ? (
                  <div className="flex justify-end gap-2 pt-4 border-t">
                    <Button
                      variant="outline"
                      onClick={() => setIsSourceDialogOpen(false)}
                    >
                      Cancel
                    </Button>
                    <Button onClick={handleSaveColumnMapping}>
                      <Save className="mr-2 h-4 w-4" />
                      Save Column Mapping
                    </Button>
                  </div>
                ) : (
                  <div className="bg-muted/50 rounded-lg p-4 text-center border-t mt-4">
                    <p className="text-sm text-muted-foreground">
                      Create the data source first in the Connection Details tab
                      to configure column mapping.
                    </p>
                  </div>
                )}
              </TabsContent>

              <TabsContent value="importRules" className="space-y-4 mt-0">
                <ImportRulesTab
                  config={importRulesConfig}
                  onChange={(newConfig) =>
                    setImportRulesConfig((prev) => ({
                      ...prev,
                      ...newConfig,
                    }))
                  }
                />
                <div className="flex justify-end pt-4 border-t">
                  <Button onClick={handleSaveImportRules}>
                    <Save className="mr-2 h-4 w-4" />
                    Save Import Rules
                  </Button>
                </div>
              </TabsContent>
            </div>
          </Tabs>
        </DialogContent>
      </Dialog>

      {/* File Preview Dialog - Shows actual raw file data */}
      <Dialog open={isPreviewOpen} onOpenChange={setIsPreviewOpen}>
        <DialogContent className="sm:max-w-[95vw] max-h-[90vh] overflow-hidden">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FileSpreadsheet className="h-5 w-5 text-green-600" />
              File Preview: {previewFileName || previewSource?.name}
            </DialogTitle>
            <DialogDescription className="flex items-center gap-2">
              {previewRows.length} rows, {previewHeaders.length} columns
              {isPivotedFormat && (
                <Badge className="bg-purple-600 ml-2">Pivoted Format</Badge>
              )}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-4">
            {/* Pivoted Format Notice */}
            {isPivotedFormat && sizeColumns.length > 0 && (
              <div className="bg-purple-50 border border-purple-200 rounded-lg p-3">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-xs text-purple-700 font-medium">
                    Detected size columns:
                  </span>
                </div>
                <div className="flex flex-wrap gap-1">
                  {sizeColumns.slice(0, 15).map((size, i) => (
                    <Badge
                      key={i}
                      variant="outline"
                      className="text-[10px] bg-white dark:bg-gray-800 border-purple-300 text-purple-700"
                    >
                      {size}
                    </Badge>
                  ))}
                  {sizeColumns.length > 15 && (
                    <Badge
                      variant="outline"
                      className="text-[10px] bg-white dark:bg-gray-800 border-purple-300 text-purple-700"
                    >
                      +{sizeColumns.length - 15} more
                    </Badge>
                  )}
                </div>
              </div>
            )}

            {/* Raw Data Table - Shows actual Excel columns */}
            <div className="rounded-md border overflow-hidden">
              <div className="overflow-x-auto max-h-[50vh] overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50 sticky top-0 z-10">
                    <tr className="border-b">
                      <th className="h-10 px-3 text-left font-medium text-muted-foreground w-12 bg-muted/50">
                        #
                      </th>
                      {previewHeaders.map((header, idx) => (
                        <th
                          key={idx}
                          className="h-10 px-3 text-left font-medium text-muted-foreground whitespace-nowrap bg-muted/50"
                        >
                          <div className="flex items-center gap-1">
                            {header}
                            {sizeColumns.includes(header) && (
                              <span className="text-[10px] text-purple-500 font-normal">
                                (size)
                              </span>
                            )}
                            {columnMapping.sku === header && (
                              <Badge
                                variant="outline"
                                className="text-[8px] h-4 px-1 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border-blue-200 ml-1"
                              >
                                SKU
                              </Badge>
                            )}
                            {columnMapping.style === header && (
                              <Badge
                                variant="outline"
                                className="text-[8px] h-4 px-1 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border-blue-200 ml-1"
                              >
                                Style
                              </Badge>
                            )}
                            {columnMapping.color === header && (
                              <Badge
                                variant="outline"
                                className="text-[8px] h-4 px-1 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border-blue-200 ml-1"
                              >
                                Color
                              </Badge>
                            )}
                          </div>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {previewRows.length > 0 ? (
                      previewRows.map((row, i) => (
                        <tr
                          key={i}
                          className="border-b last:border-0 hover:bg-muted/20"
                        >
                          <td className="p-3 font-mono text-xs text-muted-foreground">
                            {i + 1}
                          </td>
                          {row.map((cell: any, j: number) => (
                            <td key={j} className="p-3 whitespace-nowrap">
                              {cell === "Yes" ? (
                                <Badge
                                  variant="outline"
                                  className="text-[10px] h-5 bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300 border-green-200 dark:border-green-800"
                                >
                                  Yes
                                </Badge>
                              ) : cell === "No" ? (
                                <Badge
                                  variant="outline"
                                  className="text-[10px] h-5 bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-300 border-red-200"
                                >
                                  No
                                </Badge>
                              ) : cell === "Last Piece" ? (
                                <Badge
                                  variant="outline"
                                  className="text-[10px] h-5 bg-orange-50 text-orange-700 border-orange-200"
                                >
                                  Last Piece
                                </Badge>
                              ) : (
                                String(cell ?? "-")
                              )}
                            </td>
                          ))}
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td
                          colSpan={previewHeaders.length + 1}
                          className="p-8 text-center text-muted-foreground"
                        >
                          No data to display. Upload a file first.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="flex items-center gap-4 pt-4 border-t">
              <div className="flex-1">
                <Label htmlFor="preview-upload" className="text-xs mb-1 block">
                  Load Different File
                </Label>
                <Input
                  id="preview-upload"
                  type="file"
                  onChange={handleDialogFileUpload}
                  className="h-8 text-xs"
                />
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={runAiMapping}
                disabled={
                  aiMappingStatus === "scanning" || previewHeaders.length === 0
                }
              >
                {aiMappingStatus === "scanning" ? (
                  <>
                    <RefreshCw className="mr-2 h-3 w-3 animate-spin" />
                    Analyzing...
                  </>
                ) : (
                  <>
                    <Wand2 className="mr-2 h-3 w-3" />
                    AI Auto-Map
                  </>
                )}
              </Button>
            </div>
          </div>

          <DialogFooter className="flex gap-2 items-center justify-between w-full">
            <div className="flex items-center gap-2">
              {isImporting && (
                <div className="flex items-center text-sm text-green-600">
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Importing {allFileRows.length.toLocaleString()} rows to master
                  inventory...
                </div>
              )}
            </div>
            <Button variant="outline" onClick={() => setIsPreviewOpen(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Sale Import Warning Dialog */}
      <Dialog
        open={saleImportWarning.show}
        onOpenChange={(open) => {
          if (!open) {
            setSaleImportWarning({
              show: false,
              message: "",
              saleFileName: "",
              pendingFile: null,
              pendingDataSourceId: null,
            });
          }
        }}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-amber-600">
              <AlertTriangle className="h-5 w-5" />
              Sale File Not Imported
            </DialogTitle>
            <DialogDescription className="text-left">
              {saleImportWarning.message}
            </DialogDescription>
          </DialogHeader>
          <div className="py-4">
            <p className="text-sm text-muted-foreground">
              Importing now may include styles that should be filtered as
              discontinued. Consider importing the sale file first.
            </p>
          </div>
          <DialogFooter className="flex gap-2 sm:justify-between">
            <Button
              variant="outline"
              onClick={() => {
                setSaleImportWarning({
                  show: false,
                  message: "",
                  saleFileName: "",
                  pendingFile: null,
                  pendingDataSourceId: null,
                });
              }}
            >
              Cancel
            </Button>
            <Button
              variant="default"
              onClick={async () => {
                // Proceed with upload, skipping the warning check
                if (
                  saleImportWarning.pendingFile &&
                  saleImportWarning.pendingDataSourceId
                ) {
                  setSaleImportWarning({
                    show: false,
                    message: "",
                    saleFileName: "",
                    pendingFile: null,
                    pendingDataSourceId: null,
                  });
                  await uploadFileToBackend(
                    saleImportWarning.pendingFile,
                    saleImportWarning.pendingDataSourceId,
                    true,
                  );
                }
              }}
            >
              Import Anyway
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* AI Data Source Dialog */}
      <AIDataSourceDialog
        isOpen={isAISourceDialogOpen}
        onClose={() => {
          setIsAISourceDialogOpen(false);
          setAiDialogDataSource(null);
        }}
        onSuccess={() => {
          setIsAISourceDialogOpen(false);
          setAiDialogDataSource(null);
          queryClient.invalidateQueries({ queryKey: ["/api/data-sources"] });
        }}
        existingDataSource={aiDialogDataSource}
      />
    </div>
  );
}
