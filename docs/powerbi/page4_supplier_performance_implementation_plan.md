# Page 4 — Supplier Performance & Procurement: Full Implementation Plan

> **Document Version:** 1.0 — 2026-09-17  
> **Page Reference:** [page4_supplier_performance_spec.md](page4_supplier_performance_spec.md)  
> **Target Tool:** Power BI Desktop (Semantic Model + Report Canvas)  
> **Governing Project Context:** [PROJECT_CONTEXT.md](../planning/PROJECT_CONTEXT.md)  
> **Author Instruction:** When updating this page, edit sections in-place. Never delete historical decisions.

---

## 0. Previous Requirements Context & Governing Rules

Before any visual is built, confirm the following project-wide constraints are already satisfied:

| Requirement | Source Document | Status |
|:---|:---|:---|
| Kimball Star Schema (FactPurchaseOrder, FactSupplierMonthlyPerformance, DimSupplier, DimDate, DimProduct, DimWarehouse) | [`dimensional_model_design.md`](../data-model/dimensional_model_design.md) | **Must exist in BigQuery / data source** |
| Role-playing Date Relationships via `USERELATIONSHIP()` | `dimensional_model_design.md §4` | Applied for PromisedDeliveryDate, ActualDockReceiptDate |
| SCD Type 1 on DimSupplier (current record only, no history rows) | `dimensional_model_design.md §3.3` | Confirm before joining |
| BridgeProductSupplier for single-source risk analysis | `dimensional_model_design.md §6.1` | Optional — used in Dual-Source visual |
| Base procurement measures already defined | [`procurement_measures.dax`](../../powerbi/measures/procurement_measures.dax) | **Extend — do not replace** |
| Base supplier measures already defined | [`supplier_measures.dax`](../../powerbi/measures/supplier_measures.dax) | **Extend — do not replace** |
| DimSupplier.SupplierTier values must be exactly: `'Tier 1 Strategic'`, `'Tier 2 Preferred'`, `'Tier 3 Tactical'` | [`schema_specification.md §1.3`](../data-model/schema_specification.md) | Verify in data generation |
| FactPurchaseOrder.POStatus values: `'Completed'`, `'Open In-Transit'`, `'Overdue'`, `'Cancelled'` | `schema_specification.md §2.3` | Used in PO Milestone visual |
| FactSupplierMonthlyPerformance.SupplierMonthlyRiskRating values: `'Low Risk'`, `'Moderate Risk'`, `'Critical SLA Breach'` | `schema_specification.md §2.8` | Used in SLA Breach Watchlist |
| RLS: Procurement Manager sees all regions; Warehouse Planner sees only assigned warehouse | [`rls_design.md`](rls_design.md) | Applied at semantic model level |

> [!IMPORTANT]
> **Every visual on Page 4 draws from exactly two primary fact tables:**  
> `FactPurchaseOrder` (transactional grain: 1 row per PO line, ~1.1M rows) and  
> `FactSupplierMonthlyPerformance` (pre-aggregated monthly mart, ~11,852 rows).  
> Never aggregate `FactPurchaseOrder` with SUMX unnecessarily — use the pre-aggregated mart for scorecard visuals and `FactPurchaseOrder` only for drill-through and PO-level detail.

---

## 1. Page-Level Configuration

| Property | Value |
|:---|:---|
| **Report Page Name** | `4. Supplier Perf` |
| **Canvas Size** | 1280 × 720 px (16:9 widescreen) |
| **Background** | Theme background token `--card` |
| **Page-level Slicer** | `DimSupplier[SupplierTier]` (single-select dropdown, default = All) |
| **Cross-filtering** | Tier Comparison cards → filter SLA Breach Table |
| **Drill-through Target** | PO Line-Item Detail page (drill-through on `DimSupplier[SupplierCode]`) |

---

## 2. Data Model: Required Tables & Relationships for Page 4

### 2.1 Tables Actively Used on This Page

```
DimDate ──────────────────────────────────────────────────────────────────────────────
  │ (Active: POCreationDateKey)                                                       │
  │ (Inactive: PromisedDeliveryDateKey — activated via USERELATIONSHIP in DAX)        │
  │ (Inactive: ActualDockReceiptDateKey — activated via USERELATIONSHIP in DAX)       │
  │                                                                                   │
  ▼                                                                                   ▼
FactPurchaseOrder ◄──── DimSupplier                          FactSupplierMonthlyPerformance
  │                          │                                          │
  │                          │ (Active: SupplierKey ─── SupplierKey)    │
  ▼                          │                                          ▼
DimProduct                   │                                    DimSupplier
DimWarehouse (Receiving)     │                                    DimDate (Active: YearMonthDateKey)
DimEmployeePlanner (Buyer)   │
                             ▼
                    BridgeProductSupplier (optional, for sourcing risk)
```

### 2.2 Relationship Definitions (Power BI Model View)

| From Table (Many) | From Column | To Table (One) | To Column | Direction | Active? |
|:---|:---|:---|:---|:---|:---|
| `FactPurchaseOrder` | `SupplierKey` | `DimSupplier` | `SupplierKey` | Single → | **Active** |
| `FactPurchaseOrder` | `ProductKey` | `DimProduct` | `ProductKey` | Single → | **Active** |
| `FactPurchaseOrder` | `ReceivingWarehouseKey` | `DimWarehouse` | `WarehouseKey` | Single → | **Active** |
| `FactPurchaseOrder` | `POCreationDateKey` | `DimDate` | `DateKey` | Single → | **Active** |
| `FactPurchaseOrder` | `PromisedDeliveryDateKey` | `DimDate` | `DateKey` | Single → | **Inactive** |
| `FactPurchaseOrder` | `ActualDockReceiptDateKey` | `DimDate` | `DateKey` | Single → | **Inactive** |
| `FactSupplierMonthlyPerformance` | `SupplierKey` | `DimSupplier` | `SupplierKey` | Single → | **Active** |
| `FactSupplierMonthlyPerformance` | `YearMonthDateKey` | `DimDate` | `DateKey` | Single → | **Active** |
| `BridgeProductSupplier` | `SupplierKey` | `DimSupplier` | `SupplierKey` | Single → | **Active** |

> [!WARNING]
> `DimDate` has **two active relationships** simultaneously — one from `FactPurchaseOrder[POCreationDateKey]` and one from `FactSupplierMonthlyPerformance[YearMonthDateKey]`. This is valid because they connect to **different fact tables**. Power BI resolves these correctly because each fact–DimDate relationship is independent.

### 2.3 Required Columns in Fact Tables (Computed in dbt/BigQuery)

These must be computed in **dbt / BigQuery staging** before loading to Power BI (not as Power BI calculated columns — avoids VertiPaq overhead):

| Table | Column | Logic | Already Exists? |
|:---|:---|:---|:---|
| `FactPurchaseOrder` | `LeadTimeVarianceDays` | `ActualLeadTimeDays - PromisedLeadTimeDays` | ✅ Yes |
| `FactPurchaseOrder` | `IsDeliveredOnTimeFlag` | `1 if ActualDockDate ≤ PromisedDate else 0` | ✅ Yes |
| `FactPurchaseOrder` | `IsDeliveredInFullFlag` | `1 if ReceivedQty ≥ OrderedQty else 0` | ✅ Yes |
| `FactPurchaseOrder` | `IsSupplierOTIFFlag` | `1 if OnTime=1 AND InFull=1 else 0` | ✅ Yes |
| `FactPurchaseOrder` | `DelayRootCauseCategory` | See mart model `fact_purchase_order.sql` CASE logic | ✅ **Added in mart layer** |
| `FactSupplierMonthlyPerformance` | `SupplierMonthlyRiskRating` | `'Critical SLA Breach' if OTIFRatePct < 0.15 ...` | ✅ Yes |

---

## 3. Visual Sections: Build Sequence

Build in this order (each section depends on the measures of the prior):

1. **Section 4** — Write all DAX measures first
2. **Section 5** — Headline Metric Cards (5 cards)
3. **Section 6** — Vendor Tier Comparison (Matrix Visual)
4. **Section 7** — Inbound Receiving Quality + Root Cause Progress Bars
5. **Section 8** — Critical SLA Breach Watchlist Table
6. **Section 9** — Slicers & Interactions
7. **Section 10** — Drill-Through Page

---

## 4. DAX Measures for Page 4

### 4.0 Existing Measures — Reference Only

The measures below **already exist** in the documented `.dax` files. Do **not** recreate them in Power BI if they already exist in the model. Verify each one is present before building visuals.

| Measure | File | Validation Benchmark |
|:---|:---|:---|
| `[Total Procurement Spend]` | [`procurement_measures.dax`](../../powerbi/measures/procurement_measures.dax) | ≈ $34.05 B |
| `[Total PO Line Count]` | `procurement_measures.dax` | ≈ 1,110,903 lines |
| `[Completed PO Line Count]` | `procurement_measures.dax` | — |
| `[PO OTIF Line Count]` | `procurement_measures.dax` | — |
| `[Procurement OTIF Rate Pct]` | `procurement_measures.dax` | ≈ 35.8% overall |
| `[Procurement On-Time Rate Pct]` | `procurement_measures.dax` | ≈ 39.9% |
| `[Procurement In-Full Rate Pct]` | `procurement_measures.dax` | ≈ 85.4% |
| `[Average Actual Lead Time Days]` | `procurement_measures.dax` | ≈ 25.8d overall |
| `[Average Contract Lead Time Days]` | `procurement_measures.dax` | ≈ 24.2d |
| `[Average Lead Time Variance Days]` | `procurement_measures.dax` | ≈ +1.6d |
| `[Total PO Received Quantity]` | `procurement_measures.dax` | ≈ 1,364,506,058 units |
| `[Total PO Rejected Quantity]` | `procurement_measures.dax` | ≈ 20,185,926 units |
| `[Inbound Defect Rate Pct]` | `procurement_measures.dax` | ≈ 1.48% |
| `[Active Supplier Count]` | [`supplier_measures.dax`](../../powerbi/measures/supplier_measures.dax) | = 500 |
| `[Supplier Monthly OTIF Avg Pct]` | `supplier_measures.dax` | Tier 1 ≈ 64.9%, Tier 3 ≈ 8.8% |
| `[Scorecard Average Lead Time Days]` | `supplier_measures.dax` | Tier 1 ≈ 8.2d, Tier 3 ≈ 45.8d |
| `[Critical SLA Breach Count]` | `supplier_measures.dax` | — |
| `[Stockout Incident Count]` | [`service_measures.dax`](../../powerbi/measures/service_measures.dax) | — |

---

### 4.1 Net-New Measures to Build for Page 4

> All 11 measures below must be **created in Power BI**, then copied back into the appropriate `.dax` file.  
> Each measure is written to **reuse existing measures** as building blocks — no logic is duplicated.

---

#### Display Folder: `Procurement\OTIF`

---

##### `[Procurement OTIF Target]`
**Why:** Single constant as the composition anchor. Every gap/label/color measure references this instead of hard-coding `0.85` in multiple places. Change the target once here — all derived measures update automatically.

```dax
[Procurement OTIF Target] = 0.85
```

---

##### `[OTIF vs Target Gap]`
**Why:** Drives conditional formatting on the OTIF headline card. Positive = underperforming.  
**Composes:** `[Procurement OTIF Rate Pct]` + `[Procurement OTIF Target]`

```dax
[OTIF vs Target Gap] =
[Procurement OTIF Target] - [Procurement OTIF Rate Pct]
```

**Validation:** At 35.8% OTIF → gap = **+49.2 pp**

---

##### `[OTIF Status Label]`
**Why:** Sub-label text on the OTIF headline card — shows compliance tier in plain language.  
**Composes:** `[Procurement OTIF Rate Pct]` + `[Procurement OTIF Target]`

```dax
[OTIF Status Label] =
VAR OTIF = [Procurement OTIF Rate Pct]
RETURN
SWITCH(
    TRUE(),
    OTIF >= [Procurement OTIF Target],    "✅ SLA Met",
    OTIF >= 0.70,                          "⚠️ Watch Zone",
    OTIF >= 0.50,                          "🔶 At Risk",
                                           "🚨 SLA Breach"
)
```

---

#### Display Folder: `Procurement\Lead Time`

---

##### `[Lead Time Card Subtitle]`
**Why:** Reference label string for the Avg Lead-Time Delay card (Card 5). Saves building a text box manually.  
**Composes:** `[Average Actual Lead Time Days]` + `[Average Contract Lead Time Days]`

```dax
[Lead Time Card Subtitle] =
"Actual "  & FORMAT( [Average Actual Lead Time Days],   "0.0" ) & "d  vs  " &
"Quoted "  & FORMAT( [Average Contract Lead Time Days], "0.0" ) & "d"
```

---

##### `[Lead Time Variance Days (Actual Dock Date)]`
**Why:** Activates the inactive `ActualDockReceiptDateKey → DimDate` relationship so time-intelligence visuals can slice by *when deliveries arrived* rather than when POs were created.  
**Composes:** `[Average Lead Time Variance Days]` wrapped in `USERELATIONSHIP`

```dax
[Lead Time Variance Days (Actual Dock Date)] =
CALCULATE(
    [Average Lead Time Variance Days],
    USERELATIONSHIP(
        FactPurchaseOrder[ActualDockReceiptDateKey],
        DimDate[DateKey]
    )
)
```

> [!NOTE]
> Only place this measure on delivery-period time-intelligence visuals (e.g., trend line on drill-through page). Do **not** use it on the main page summary cards — those should use the active POCreationDate relationship.

---

#### Display Folder: `Procurement\Quality`

---

##### `[Inbound Acceptance Rate Pct]`
**Why:** Complement of the defect rate — used as the sub-label on the "Total Units Received" card.  
**Composes:** `[Inbound Defect Rate Pct]`

```dax
[Inbound Acceptance Rate Pct] =
1 - [Inbound Defect Rate Pct]
```

**Validation:** ≈ **98.52%**

---

##### `[Acceptance Rate Label]`  &  `[Defect Rate Label]`
**Why:** Pre-formatted string sub-labels for the two Quality KPI cards. Keeps formatting logic out of the visual's format pane.  
**Composes:** `[Inbound Acceptance Rate Pct]` + `[Inbound Defect Rate Pct]`

```dax
[Acceptance Rate Label] =
FORMAT( [Inbound Acceptance Rate Pct], "0.00%" ) & " Accepted"

[Defect Rate Label] =
FORMAT( [Inbound Defect Rate Pct], "0.00%" ) & " Inbound Defect Rate"
```

---

##### `[Pct of Late Deliveries by Root Cause]`
**Why:** Bar chart axis measure for the Root Cause breakdown visual. Computes the share of late lines per `DelayRootCauseCategory` group relative to all late lines, so the bars sum to 100%.  
**Composes:** `[Completed PO Line Count]` as denominator anchor; uses `REMOVEFILTERS` to lift the category filter for the total.

```dax
[Pct of Late Deliveries by Root Cause] =
VAR LateInContext =
    CALCULATE(
        [Completed PO Line Count],
        FactPurchaseOrder[LeadTimeVarianceDays] > 0
    )
VAR AllLate =
    CALCULATE(
        [Completed PO Line Count],
        FactPurchaseOrder[LeadTimeVarianceDays] > 0,
        REMOVEFILTERS( FactPurchaseOrder[DelayRootCauseCategory] )
    )
RETURN
    DIVIDE( LateInContext, AllLate, 0 )
```

> **Optimization vs original plan:** Replaced the raw `COUNTROWS( FactPurchaseOrder )` + duplicate `POStatus = "Completed"` filter with `[Completed PO Line Count]` which already encapsulates that filter. Result is identical; logic is DRY.

---

#### Display Folder: `Supplier\Scorecard`

---

##### `[Scorecard Average Lead Time Std Dev]`
**Why:** Shows lead-time *predictability* per tier (not just average). Tier 1 ≈ 0 std dev = JIT-reliable; Tier 3 = high std dev = unpredictable delivery windows.

```dax
[Scorecard Average Lead Time Std Dev] =
AVERAGE( FactSupplierMonthlyPerformance[LeadTimeStdDevDays] )
```

---

##### `[Scorecard Avg Line Fill Rate Pct]`
**Why:** Third metric column in the Tier Comparison matrix, alongside OTIF and Lead Time.

```dax
[Scorecard Avg Line Fill Rate Pct] =
AVERAGE( FactSupplierMonthlyPerformance[LineFillRatePct] )
```

---

##### `[Supplier Vendor Risk Score]`
**Why:** Pre-computed composite risk index (0–100) stored in `DimSupplier`. In a table visual with one supplier per row, `MAX` returns the single row's value. In any other context (card, matrix total), it returns the average across suppliers in context — both are semantically correct.  
**Composes:** `DimSupplier[VendorRiskScore]`

```dax
[Supplier Vendor Risk Score] =
IF(
    HASONEVALUE( DimSupplier[SupplierKey] ),
    MAX( DimSupplier[VendorRiskScore] ),
    AVERAGE( DimSupplier[VendorRiskScore] )
)
```

> **Optimization vs original plan:** Original used bare `AVERAGE()`. The `HASONEVALUE` branch returns the exact supplier score in a table row context rather than averaging a single row — functionally identical result but semantically correct and avoids a misleading "average of one value" calculation.

---

##### `[Tier3 Stockout Attribution Pct]`
**Why:** Backs the alert callout "300 Tier 3 suppliers drive 68% of stockout events".  
**Composes:** `[Stockout Incident Count]` from `service_measures.dax` — reuses the existing row count measure rather than re-writing `COUNTROWS( FactStockout )`.

```dax
[Tier3 Stockout Attribution Pct] =
VAR Tier3Count =
    CALCULATE(
        [Stockout Incident Count],
        FactStockout[StockoutAttributedReason] = "Supplier Delayed Inbound",
        DimSupplier[SupplierTier] = "Tier 3 Tactical"
    )
VAR TotalSupplierCaused =
    CALCULATE(
        [Stockout Incident Count],
        FactStockout[StockoutAttributedReason] = "Supplier Delayed Inbound",
        REMOVEFILTERS( DimSupplier[SupplierTier] )
    )
RETURN
    DIVIDE( Tier3Count, TotalSupplierCaused, 0 )
```

> [!NOTE]
> Requires `FactStockout` to have a resolvable path to `DimSupplier` (via `DimProduct[PrimarySupplierKey]` or a direct `SupplierKey` FK added in dbt). Validate the join path before using. See Open Decision #2.

---

#### Display Folder: `Supplier\Actions`

---

##### `[Supplier Recommended Action]`
**Why:** Drives the "Recommended Action" column in the SLA Breach Watchlist table. Logic is tiered by spend size, sourcing zone, and lead-time variance — all derived from existing measures.  
**Composes:** `[Procurement OTIF Rate Pct]` + `[Total Procurement Spend]` + `[Average Lead Time Variance Days]`

```dax
[Supplier Recommended Action] =
IF(
    NOT HASONEVALUE( DimSupplier[SupplierKey] ),
    "—",
    VAR OTIF     = [Procurement OTIF Rate Pct]
    VAR Spend    = [Total Procurement Spend]
    VAR LTVar    = [Average Lead Time Variance Days]
    VAR Zone     = SELECTEDVALUE( DimSupplier[RegionZone], "Unknown" )
    RETURN
    SWITCH(
        TRUE(),
        OTIF < 0.09 && Spend > 150000000,  "Shift 30% Volume",
        OTIF < 0.10 && Zone = "Overseas Inbound", "Dual-Source Mandate",
        OTIF < 0.10 && LTVar >= 4.0,       "Audit SLA Breach",
        OTIF < 0.10 && LTVar >= 3.5,       "Freeze New POs",
        OTIF < 0.15,                        "Penalty Fee Apply",
        OTIF < 0.40,                        "Improve Action Plan",
                                            "Monitor"
    )
)
```

> **Why `HASONEVALUE` guard:** This measure uses `SELECTEDVALUE()` for zone and references single-row measures. Without the guard, it returns "—" in card/matrix totals rather than evaluating incorrectly.

---

##### `[Action Background Color]`
**Why:** Drives conditional background formatting on the Recommended Action column via "Format by field value". Returns a hex string — no conditional formatting rules dialog needed.  
**Composes:** `[Supplier Recommended Action]`

```dax
[Action Background Color] =
SWITCH(
    [Supplier Recommended Action],
    "Shift 30% Volume",    "#fff1f2",   -- Rose tint   (urgent)
    "Audit SLA Breach",    "#fff1f2",   -- Rose tint   (urgent)
    "Freeze New POs",      "#fff1f2",   -- Rose tint   (urgent)
    "Penalty Fee Apply",   "#fff1f2",   -- Rose tint   (urgent)
    "Dual-Source Mandate", "#fffbeb",   -- Amber tint  (strategic)
    "Improve Action Plan", "#fffbeb",   -- Amber tint  (strategic)
    "Monitor",             "#f0fdf4",   -- Green tint  (healthy)
    "#1e293b"                           -- Dark slate  (guard / blank row)
)
```

---

### 4.2 Measure Dependency Map

```
[Procurement OTIF Target]  ◄─── anchor constant (0.85)
        │
        ├──► [OTIF vs Target Gap]          = Target − [Procurement OTIF Rate Pct]
        └──► [OTIF Status Label]           = SWITCH on [Procurement OTIF Rate Pct] vs Target

[Average Actual Lead Time Days]  ◄─── from procurement_measures.dax
[Average Contract Lead Time Days] ◄─── from procurement_measures.dax
        │
        └──► [Lead Time Card Subtitle]     = formatted string of both

[Average Lead Time Variance Days]  ◄─── from procurement_measures.dax
        └──► [Lead Time Variance Days (Actual Dock Date)]  = CALCULATE(..., USERELATIONSHIP)

[Inbound Defect Rate Pct]  ◄─── from procurement_measures.dax
        │
        ├──► [Inbound Acceptance Rate Pct] = 1 − Defect Rate
        ├──► [Defect Rate Label]           = FORMAT(Defect Rate)
        └──► [Acceptance Rate Label]       = FORMAT(Acceptance Rate)

[Completed PO Line Count]  ◄─── from procurement_measures.dax
        └──► [Pct of Late Deliveries by Root Cause] = Late-in-context / All-late

[Stockout Incident Count]  ◄─── from service_measures.dax
        └──► [Tier3 Stockout Attribution Pct] = Tier3 / TotalSupplierCaused

[Procurement OTIF Rate Pct]  ◄─── from procurement_measures.dax
[Total Procurement Spend]    ◄─── from procurement_measures.dax
[Average Lead Time Variance Days] ◄─── from procurement_measures.dax
        │
        └──► [Supplier Recommended Action]
                    └──► [Action Background Color]
```

---


---

### Display Folder: `Procurement\Spend`

#### `[Total Procurement Spend]` ✅ Already exists
```dax
-- Already in procurement_measures.dax
[Total Procurement Spend] =
SUM( FactPurchaseOrder[ExtendedPOAmount] )
```
**Validation:** Grand total ≈ **$34.05 B** unfiltered.

---

#### `[Total PO Line Count]` ✅ Already exists
```dax
[Total PO Line Count] = COUNTROWS( FactPurchaseOrder )
```
**Validation:** ≈ **1,110,903 lines** unfiltered.

---

### Display Folder: `Procurement\OTIF`

#### `[Completed PO Line Count]` ✅ Already exists
```dax
[Completed PO Line Count] =
CALCULATE(
    COUNTROWS( FactPurchaseOrder ),
    FactPurchaseOrder[POStatus] = "Completed"
)
```
**Business Rule:** Only completed PO lines can have an OTIF outcome. Open/Overdue/Cancelled lines are excluded from the denominator to avoid artificially deflating the rate.

---

#### `[PO OTIF Line Count]` ✅ Already exists
```dax
[PO OTIF Line Count] =
CALCULATE(
    COUNTROWS( FactPurchaseOrder ),
    FactPurchaseOrder[IsSupplierOTIFFlag] = 1
)
```

---

#### `[Procurement OTIF Rate Pct]` ✅ Already exists
**Business Definition:** Percentage of completed PO lines where the supplier delivered both on time and in full.  
**Mathematical Logic:** `OTIF Lines ÷ Completed Lines`
```dax
[Procurement OTIF Rate Pct] =
DIVIDE(
    [PO OTIF Line Count],
    [Completed PO Line Count],
    0
)
```
**Validation:** Unfiltered ≈ **35.8%** | Tier 1 ≈ **64.9%** | Tier 2 ≈ **38.4%** | Tier 3 ≈ **8.8%**

---

#### `[Procurement On-Time Rate Pct]` ✅ Already exists
```dax
[Procurement On-Time Rate Pct] =
DIVIDE(
    CALCULATE(
        COUNTROWS( FactPurchaseOrder ),
        FactPurchaseOrder[IsDeliveredOnTimeFlag] = 1
    ),
    [Completed PO Line Count],
    0
)
```
**Validation:** Unfiltered ≈ **39.9%**

---

#### `[Procurement In-Full Rate Pct]` ✅ Already exists
```dax
[Procurement In-Full Rate Pct] =
DIVIDE(
    CALCULATE(
        COUNTROWS( FactPurchaseOrder ),
        FactPurchaseOrder[IsDeliveredInFullFlag] = 1
    ),
    [Completed PO Line Count],
    0
)
```
**Validation:** Unfiltered ≈ **85.4%**

---

#### `[OTIF vs Target Gap]` ⬜ NEW — Add to procurement_measures.dax
**Business Definition:** How far current OTIF falls below the 85% SLA target. Used for conditional card formatting.
```dax
[OTIF vs Target Gap] =
VAR CurrentOTIF = [Procurement OTIF Rate Pct]
VAR TargetOTIF  = 0.85
RETURN
    TargetOTIF - CurrentOTIF
```
**Validation:** At 35.8% OTIF → gap = **+49.2 pp**

---

#### `[OTIF Status Label]` ⬜ NEW — Add to procurement_measures.dax
**Business Definition:** Human-readable compliance verdict for card sub-labels.
```dax
[OTIF Status Label] =
SWITCH(
    TRUE(),
    [Procurement OTIF Rate Pct] >= 0.85,  "✅ SLA Met",
    [Procurement OTIF Rate Pct] >= 0.70,  "⚠️ Watch Zone",
    [Procurement OTIF Rate Pct] >= 0.50,  "🔶 At Risk",
                                           "🚨 SLA Breach"
)
```

---

### Display Folder: `Procurement\Lead Time`

#### `[Average Actual Lead Time Days]` ✅ Already exists
```dax
[Average Actual Lead Time Days] =
AVERAGEX(
    FILTER(
        FactPurchaseOrder,
        FactPurchaseOrder[POStatus] = "Completed"
    ),
    FactPurchaseOrder[ActualLeadTimeDays]
)
```
**Validation:** Unfiltered ≈ **25.8 days** | Tier 1 ≈ **8.2d** | Tier 2 ≈ **22.4d** | Tier 3 ≈ **45.8d**

---

#### `[Average Contract Lead Time Days]` ✅ Already exists
```dax
[Average Contract Lead Time Days] =
AVERAGEX(
    FILTER(
        FactPurchaseOrder,
        FactPurchaseOrder[POStatus] = "Completed"
    ),
    FactPurchaseOrder[PromisedLeadTimeDays]
)
```
**Validation:** Unfiltered ≈ **24.2 days**

---

#### `[Average Lead Time Variance Days]` ✅ Already exists
**Business Definition:** Mean deviation of actual vs contracted lead time. Positive = late.
```dax
[Average Lead Time Variance Days] =
AVERAGEX(
    FILTER(
        FactPurchaseOrder,
        FactPurchaseOrder[POStatus] = "Completed"
    ),
    FactPurchaseOrder[LeadTimeVarianceDays]
)
```
**Validation:** Unfiltered ≈ **+1.6 days** | Tier 1 ≈ **0.0d** | Tier 3 ≈ **+4.0d**

---

#### `[Lead Time Card Subtitle]` ⬜ NEW — Add to procurement_measures.dax
**Business Definition:** String measure for the Card 5 reference label.
```dax
[Lead Time Card Subtitle] =
"Actual " &
FORMAT( [Average Actual Lead Time Days], "0.0" ) &
"d vs Quoted " &
FORMAT( [Average Contract Lead Time Days], "0.0" ) &
"d"
```

---

#### `[Lead Time Variance Days (Actual Dock Date)]` ⬜ NEW — Add to procurement_measures.dax
**Business Definition:** Lead time variance evaluated by actual dock receipt date (uses inactive relationship). Used on delivery-period time-intelligence visuals.
```dax
[Lead Time Variance Days (Actual Dock Date)] =
CALCULATE(
    [Average Lead Time Variance Days],
    USERELATIONSHIP(
        FactPurchaseOrder[ActualDockReceiptDateKey],
        DimDate[DateKey]
    )
)
```

---

### Display Folder: `Procurement\Quality`

#### `[Total PO Received Quantity]` ✅ Already exists
```dax
[Total PO Received Quantity] = SUM( FactPurchaseOrder[ReceivedQuantity] )
```
**Validation:** ≈ **1,364,506,058 units**

---

#### `[Total PO Rejected Quantity]` ✅ Already exists
```dax
[Total PO Rejected Quantity] = SUM( FactPurchaseOrder[RejectedQuantity] )
```
**Validation:** ≈ **20,185,926 units**

---

#### `[Inbound Defect Rate Pct]` ✅ Already exists
```dax
[Inbound Defect Rate Pct] =
DIVIDE( [Total PO Rejected Quantity], [Total PO Received Quantity], 0 )
```
**Validation:** ≈ **1.48%**

---

#### `[Inbound Acceptance Rate Pct]` ⬜ NEW — Add to procurement_measures.dax
```dax
[Inbound Acceptance Rate Pct] =
1 - [Inbound Defect Rate Pct]
```
**Validation:** ≈ **98.52%**

---

#### `[Acceptance Rate Label]` ⬜ NEW
```dax
[Acceptance Rate Label] =
FORMAT( [Inbound Acceptance Rate Pct], "0.00%" ) & " Accepted"
```

#### `[Defect Rate Label]` ⬜ NEW
```dax
[Defect Rate Label] =
FORMAT( [Inbound Defect Rate Pct], "0.00%" ) & " Inbound Defect Rate"
```

---

### Display Folder: `Supplier\Scorecard`

#### `[Active Supplier Count]` ✅ Already exists
```dax
[Active Supplier Count] =
DISTINCTCOUNT( FactSupplierMonthlyPerformance[SupplierKey] )
```
**Validation:** Unfiltered = **500**

---

#### `[Supplier Monthly OTIF Avg Pct]` ✅ Already exists
```dax
[Supplier Monthly OTIF Avg Pct] =
AVERAGE( FactSupplierMonthlyPerformance[OTIFRatePct] )
```

---

#### `[Scorecard Average Lead Time Days]` ✅ Already exists
```dax
[Scorecard Average Lead Time Days] =
AVERAGE( FactSupplierMonthlyPerformance[AverageLeadTimeDays] )
```

---

#### `[Scorecard Average Lead Time Std Dev]` ⬜ NEW — Add to supplier_measures.dax
**Business Definition:** Average lead-time standard deviation across scorecard months. High values indicate unpredictable delivery. Tier 1 ≈ 0.0; Tier 3 ≈ high variability.
```dax
[Scorecard Average Lead Time Std Dev] =
AVERAGE( FactSupplierMonthlyPerformance[LeadTimeStdDevDays] )
```

---

#### `[Scorecard Avg Line Fill Rate Pct]` ⬜ NEW — Add to supplier_measures.dax
```dax
[Scorecard Avg Line Fill Rate Pct] =
AVERAGE( FactSupplierMonthlyPerformance[LineFillRatePct] )
```

---

#### `[Critical SLA Breach Supplier Count]` ✅ Already exists (as Critical SLA Breach Count — rename)
```dax
[Critical SLA Breach Supplier Count] =
CALCULATE(
    DISTINCTCOUNT( FactSupplierMonthlyPerformance[SupplierKey] ),
    FactSupplierMonthlyPerformance[SupplierMonthlyRiskRating] = "Critical SLA Breach"
)
```

---

#### `[Supplier Vendor Risk Score]` ⬜ NEW — Add to supplier_measures.dax
**Business Definition:** Average composite risk index from `DimSupplier[VendorRiskScore]` (0–100 scale, higher = riskier).
```dax
[Supplier Vendor Risk Score] =
AVERAGE( DimSupplier[VendorRiskScore] )
```
**Note:** `VendorRiskScore` lives in `DimSupplier`. Measures referencing dimension attributes without a fact table are valid in Power BI when the dimension is connected via an active relationship from at least one fact table.

---

#### `[Tier3 Stockout Attribution Pct]` ⬜ NEW — Add to supplier_measures.dax
**Business Definition:** Percentage of supplier-caused stockout events attributable to Tier 3 tactical vendors. Backs the "68% stockout" alert callout.
```dax
[Tier3 Stockout Attribution Pct] =
VAR Tier3Count =
    CALCULATE(
        COUNTROWS( FactStockout ),
        FactStockout[StockoutAttributedReason] = "Supplier Delayed Inbound",
        DimSupplier[SupplierTier] = "Tier 3 Tactical"
    )
VAR TotalSupplierCaused =
    CALCULATE(
        COUNTROWS( FactStockout ),
        FactStockout[StockoutAttributedReason] = "Supplier Delayed Inbound",
        REMOVEFILTERS( DimSupplier[SupplierTier] )
    )
RETURN
    DIVIDE( Tier3Count, TotalSupplierCaused, 0 )
```
> [!NOTE]
> This measure requires `FactStockout` to have a resolvable path to `DimSupplier` — either via `DimProduct[PrimarySupplierKey]` or by adding `SupplierKey` directly to `FactStockout` in the dbt transformation. Confirm this join path before using.

---

#### `[OTIF vs Target Gap]` ⬜ NEW (already listed in Procurement\OTIF above)

---

### Display Folder: `Supplier\Actions`

#### `[Supplier Recommended Action]` ⬜ NEW — Add to supplier_measures.dax
**Business Definition:** Procurement action recommendation based on OTIF rate, spend size, sourcing zone, and lead-time variance. Populates the "Recommended Action" column in the SLA Breach Watchlist.  
**Important:** Only meaningful with `HASONEVALUE(DimSupplier[SupplierKey])` — i.e., in a table row context with one supplier per row.
```dax
[Supplier Recommended Action] =
IF(
    NOT HASONEVALUE( DimSupplier[SupplierKey] ),
    "—",
    VAR SupplierOTIF    = [Procurement OTIF Rate Pct]
    VAR SupplierSpend   = [Total Procurement Spend]
    VAR AvgLeadVar      = [Average Lead Time Variance Days]
    VAR Zone            = SELECTEDVALUE( DimSupplier[RegionZone], "Unknown" )
    RETURN
    SWITCH(
        TRUE(),
        SupplierOTIF < 0.09 && SupplierSpend > 150000000,   "Shift 30% Volume",
        SupplierOTIF < 0.10 && Zone = "Overseas Inbound",    "Dual-Source Mandate",
        SupplierOTIF < 0.10 && AvgLeadVar >= 4.0,           "Audit SLA Breach",
        SupplierOTIF < 0.10 && AvgLeadVar >= 3.5,           "Freeze New POs",
        SupplierOTIF < 0.15,                                  "Penalty Fee Apply",
        SupplierOTIF < 0.40,                                  "Improve Action Plan",
                                                               "Monitor"
    )
)
```

---

#### `[Action Background Color]` ⬜ NEW — Add to supplier_measures.dax
**Business Definition:** Hex color code for conditional formatting of the Recommended Action column in the SLA Breach table.
```dax
[Action Background Color] =
SWITCH(
    [Supplier Recommended Action],
    "Shift 30% Volume",    "#fff1f2",
    "Dual-Source Mandate", "#fffbeb",
    "Audit SLA Breach",    "#fff1f2",
    "Freeze New POs",      "#fff1f2",
    "Penalty Fee Apply",   "#fff1f2",
    "Improve Action Plan", "#fffbeb",
    "#f0fdf4"
)
```

---

#### `[Pct of Late Deliveries by Root Cause]` ⬜ NEW — Add to procurement_measures.dax
**Business Definition:** Fraction of all late completed PO lines attributed to the `DelayRootCauseCategory` in current filter context. Used on the horizontal bar chart in Section 7.
```dax
[Pct of Late Deliveries by Root Cause] =
VAR LateInContext =
    CALCULATE(
        COUNTROWS( FactPurchaseOrder ),
        FactPurchaseOrder[LeadTimeVarianceDays] > 0,
        FactPurchaseOrder[POStatus] = "Completed"
    )
VAR AllLate =
    CALCULATE(
        COUNTROWS( FactPurchaseOrder ),
        FactPurchaseOrder[LeadTimeVarianceDays] > 0,
        FactPurchaseOrder[POStatus] = "Completed",
        REMOVEFILTERS( FactPurchaseOrder[DelayRootCauseCategory] )
    )
RETURN
    DIVIDE( LateInContext, AllLate, 0 )
```

---

## 5. Visual Section 1: Headline Metric Cards

### 5.1 Business Purpose
Provide procurement managers an at-a-glance status of the five most critical supply performance indicators.

### 5.2 Required Visuals
Use **5 × Card (New)** visuals arranged horizontally across the top row.

| Card # | Title | Measure | Format String | Conditional Color Logic |
|:---|:---|:---|:---|:---|
| 1 | Total Procurement Spend | `[Total Procurement Spend]` | `$#,##0.00,,," B"` | Neutral |
| 2 | Supplier OTIF Rate | `[Procurement OTIF Rate Pct]` | `0.0%` | Amber if < 85% |
| 3 | On-Time Delivery Rate | `[Procurement On-Time Rate Pct]` | `0.0%` | Purple (informational) |
| 4 | In-Full Line Fill Rate | `[Procurement In-Full Rate Pct]` | `0.0%` | Emerald (positive) |
| 5 | Avg Lead-Time Delay | `[Average Lead Time Variance Days]` | `"+0.0 Days"` | Rose if > 0 |

### 5.3 Steps to Build

1. Insert **Card (New)** visual. Go to Build → Fields → Callout value → drag measure.
2. **Title:** Format pane → General → Title → enter static label.
3. **Card 2 Conditional Formatting:**
   - Format pane → Callout value → Conditional formatting → Background color
   - Rule: If `[Procurement OTIF Rate Pct]` < 0.85 → Amber `#f59e0b`; else Green `#10b981`.
4. **Card 5 Reference Label:**
   - Reference label field → drag `[Lead Time Card Subtitle]`.
5. **Card 2 Sub-label:** Drag `[OTIF Status Label]` to the Reference label slot.

---

## 6. Visual Section 2: Vendor Tier Comparison Matrix

### 6.1 Business Purpose
Show the performance gap between Tier 1 Strategic, Tier 2 Preferred, and Tier 3 Tactical supplier groups. Justifies budget allocation for supplier development programmes.

### 6.2 Visual Type
**Power BI Matrix Visual** with conditional formatting per column.

### 6.3 Matrix Configuration

| Axis | Field or Measure |
|:---|:---|
| Rows | `DimSupplier[SupplierTier]` |
| Values Col 1 | `[Active Supplier Count]` |
| Values Col 2 | `[Supplier Monthly OTIF Avg Pct]` |
| Values Col 3 | `[Scorecard Average Lead Time Days]` |
| Values Col 4 | `[Scorecard Average Lead Time Std Dev]` |
| Values Col 5 | `[Scorecard Avg Line Fill Rate Pct]` |

**Conditional Formatting — OTIF Column:**
- ≥ 60% → Emerald background `#d1fae5`
- ≥ 30% → Amber background `#fef3c7`
- < 30% → Rose background `#fce7f3`

**Conditional Formatting — Lead Time Std Dev Column:**
- ≤ 0.5 → Emerald
- ≤ 2.0 → Amber
- > 2.0 → Rose

### 6.4 Steps to Build

1. Insert **Matrix** visual.
2. Add `DimSupplier[SupplierTier]` to Rows.
3. Add all five measures to Values in order.
4. Format → Conditional formatting → Background color → Rules for each column.
5. Rename column headers: Format → Specific column → Rename.
6. Format → Style preset → Condensed (compact row height).

### 6.5 Alert Callout Text Box

Add a **Text Box** below the matrix:

> **🚨 Procurement Alert:** 300 Tier 3 tactical suppliers account for **~68% of stockout outage events** due to +4.0-day average delivery delays. (Validated by `[Tier3 Stockout Attribution Pct]`)

---

## 7. Visual Section 3: Inbound Receiving Quality & Root Cause Breakdown

### 7.1 Panel A — Quality KPI Cards

Add two **Card (New)** visuals side by side:

| Card | Measure | Format | Sub-label Measure |
|:---|:---|:---|:---|
| Left — Total Units Received | `[Total PO Received Quantity]` | `#,##0` | `[Acceptance Rate Label]` |
| Right — Dock Rejections | `[Total PO Rejected Quantity]` | `#,##0` | `[Defect Rate Label]` |

Dock Rejections card → set Callout value color to Rose `#f43f5e`.

### 7.2 Panel B — Delay Root Cause Breakdown

**Preferred approach: Live analytics via `FactPurchaseOrder[DelayRootCauseCategory]`**

**dbt SQL column to add to `FactPurchaseOrder` staging model:**
```sql
CASE
  WHEN ds.region_zone = 'Overseas Inbound'
       AND fpo.lead_time_variance_days >= 7
                                       THEN 'Customs & Port Congestion (Overseas)'
  WHEN ds.supplier_tier = 'Tier 3 Tactical'
       AND fpo.lead_time_variance_days BETWEEN 3 AND 5
                                       THEN 'Raw Material Shortage (Tier 3)'
  WHEN fpo.lead_time_variance_days BETWEEN 1 AND 2
                                       THEN 'Carrier Capacity & Freight Inefficiency'
  WHEN fpo.lead_time_variance_days > 0 THEN 'Other Late Delivery'
  ELSE 'On Time or Early'
END AS delay_root_cause_category
```

**Power BI Visual:**
- Insert **Clustered Bar Chart** (horizontal).
- Y-Axis: `FactPurchaseOrder[DelayRootCauseCategory]`
- X-Axis (Value): `[Pct of Late Deliveries by Root Cause]`
- Visual filter: `DelayRootCauseCategory` is not "On Time or Early".
- Data labels: enabled, format `0%`.
- Bar colors: use conditional format rules — Customs = Rose, Shortage = Amber, Carrier = Blue.

**Fallback approach (no dbt column): Static Reference Table**

If dbt cannot be updated, load via Power Query → Enter Data:

| DelayCategory | PctOfDelays |
|:---|---:|
| Customs & Port Congestion (Overseas) | 48 |
| Raw Material Shortage (Tier 3) | 32 |
| Carrier Capacity & Freight Inefficiency | 20 |

Use this static table as the axis of the bar chart instead.

> [!TIP]
> The live dbt column approach is strongly preferred. It lets the chart respond dynamically to vendor tier and date slicers, revealing how root-cause distribution shifts by tier or period — a much stronger analytical insight than static percentages.

---

## 8. Visual Section 4: Critical SLA Breach Watchlist Table

### 8.1 Business Purpose
Surface the highest-spend suppliers with OTIF < 15% to prioritise renegotiation, dual-sourcing, or volume reallocation.

### 8.2 Visual Type
**Power BI Table Visual** (not Matrix — Table displays per-row text measures correctly).

### 8.3 Table Column Configuration

| # | Column | Source | Format | Notes |
|:---|:---|:---|:---|:---|
| 1 | Vendor Code | `DimSupplier[SupplierCode]` | Text | Natural key, e.g., `SUP-0035` |
| 2 | Supplier Name | `DimSupplier[SupplierName]` | Text | |
| 3 | Tier | `DimSupplier[SupplierTier]` | Text | |
| 4 | Sourcing Zone | `DimSupplier[RegionZone]` | Text | |
| 5 | PO Spend | `[Total Procurement Spend]` | `$#,##0.0" M"` | Purple text |
| 6 | Actual Lead Time | `[Average Actual Lead Time Days]` | `0.0"d"` | |
| 7 | Variance | `[Average Lead Time Variance Days]` | `+0.0"d"` | Conditional: Rose |
| 8 | OTIF Rate | `[Procurement OTIF Rate Pct]` | `0.0%` | Conditional: Rose bg |
| 9 | Fill Rate | `[Procurement In-Full Rate Pct]` | `0.0%` | |
| 10 | Risk Score | `[Supplier Vendor Risk Score]` | `0.00` | |
| 11 | Recommended Action | `[Supplier Recommended Action]` | Text | Conditional bg color |

### 8.4 Filters on This Visual

**Visual-level filter:**
- Measure: `[Procurement OTIF Rate Pct]` → **is less than** `0.15`

**Sort:**
- Column: `[Total Procurement Spend]` → **Descending**

### 8.5 Steps to Build

1. Insert **Table** visual.
2. Add all columns/measures in order above.
3. **Visual-level filter:** Format pane → Filters on this visual → `[Procurement OTIF Rate Pct]` → Filter type: Advanced → is less than 0.15.
4. Click `[Total Procurement Spend]` column header in report view → Sort descending.
5. **Conditional Formatting — OTIF Rate:**
   - Format pane → Specific column → `[Procurement OTIF Rate Pct]` → Conditional formatting → Background color
   - Rule: < 0.15 → Background `rgba(244,63,94,0.15)`, Font color `#f43f5e`
6. **Conditional Formatting — Variance:**
   - Rule: > 0 → Rose font `#f43f5e`; ≤ 0 → Emerald `#10b981`
7. **Conditional Formatting — Recommended Action:**
   - Format by: Field value → Select `[Action Background Color]` measure
8. **Drill-Through:** Right-click on Vendor Code → Drill through → select `PO Receiving Detail` page (see Section 10).

---

## 9. Slicers & Cross-Filter Interactions

### 9.1 Page-Level Slicers

| Slicer | Field | Type | Default |
|:---|:---|:---|:---|
| Vendor Tier | `DimSupplier[SupplierTier]` | Dropdown | All |
| Scorecard Period | `DimDate[YearNumber]` | Dropdown or relative date | Rolling 12 months |
| Sourcing Zone | `DimSupplier[RegionZone]` | Dropdown | All |

### 9.2 Visual Interaction Map

| Source Visual | User Action | Target Visual | Interaction Type |
|:---|:---|:---|:---|
| Tier Comparison Matrix | Click a tier row | All 5 Headline Cards | Filter |
| Tier Comparison Matrix | Click a tier row | SLA Breach Watchlist Table | Filter |
| SLA Breach Watchlist | Click any row | All other visuals | **None** (outbound disabled) |
| Vendor Tier Slicer | Select value | All page visuals | Filter (page-level) |
| Sourcing Zone Slicer | Select value | All page visuals | Filter (page-level) |

**How to configure interactions:**
1. Click the Tier Matrix → Format → Edit Interactions → set Cards and Table to **Filter** (funnel icon).
2. Click the SLA Breach Table → Format → Edit Interactions → set all other visuals to **None** (circle icon).

---

## 10. Drill-Through Page: PO Receiving Detail

### 10.1 Page Setup

| Property | Value |
|:---|:---|
| Page name | `PO Receiving Detail` |
| Hidden from nav | Yes (hidden page, only accessible via drill-through) |
| Drill-through field | `DimSupplier[SupplierCode]` (add to the Drill-through well in the Filters pane) |
| Back button | Insert → Buttons → Back (top-left) |

### 10.2 Visuals on Drill-Through Page

**Supplier Header Card Row (3 cards):**
- `DimSupplier[SupplierName]` — supplier name
- `[Total Procurement Spend]` — total spend with this supplier
- `[Procurement OTIF Rate Pct]` + `[Scorecard Average Lead Time Days]`

**PO Line Detail Table:**

| Column | Source |
|:---|:---|
| PO ID | `FactPurchaseOrder[PurchaseOrderID]` |
| PO Created | `DimDate[FullDate]` (via POCreationDateKey — active relationship) |
| Quoted Lead Days | `FactPurchaseOrder[PromisedLeadTimeDays]` |
| Actual Lead Days | `FactPurchaseOrder[ActualLeadTimeDays]` |
| Variance Days | `FactPurchaseOrder[LeadTimeVarianceDays]` |
| Ordered Qty | `FactPurchaseOrder[OrderedQuantity]` |
| Received Qty | `FactPurchaseOrder[ReceivedQuantity]` |
| PO Status | `FactPurchaseOrder[POStatus]` |
| OTIF | `FactPurchaseOrder[IsSupplierOTIFFlag]` (format as Yes/No) |

> [!NOTE]
> To display the **promised delivery date** or **actual dock receipt date** in the table, add them as pre-calculated date columns (e.g., `PromisedDeliveryDate DATE`, `ActualDockReceiptDate DATE`) directly to `FactPurchaseOrder` in dbt. Do not try to pull them via inactive `DimDate` relationships in the table visual.

**Lead Time Trend Line Chart:**
- X-Axis: `DimDate[MonthYear]` (via YearMonthDateKey on `FactSupplierMonthlyPerformance`)
- Y-Axis Line 1: `[Scorecard Average Lead Time Days]` (Actual)
- Y-Axis Line 2: `[Average Contract Lead Time Days]` (Quoted)
- Reference Line at 85% OTIF target (constant line)

---

## 11. Conditional Formatting: Quick Reference Summary

| Visual | Column | Rule | Color |
|:---|:---|:---|:---|
| Card 2 | OTIF Rate | < 0.85 | Amber `#f59e0b` |
| Card 5 | Lead Time Variance | > 0 | Rose `#f43f5e` |
| Tier Matrix | OTIF per Tier | ≥ 0.60 / ≥ 0.30 / < 0.30 | Emerald / Amber / Rose |
| Tier Matrix | Lead Time Std Dev | ≤ 0.5 / ≤ 2.0 / > 2.0 | Emerald / Amber / Rose |
| SLA Breach Table | OTIF Rate | < 0.15 | Rose background |
| SLA Breach Table | LT Variance | > 0 / ≤ 0 | Rose / Emerald font |
| SLA Breach Table | Recommended Action | By `[Action Background Color]` | Background by rule |

---

## 12. Performance Engineering Considerations

| Risk | Mitigation Strategy |
|:---|:---|
| `FactPurchaseOrder` ~1.1M rows — AVERAGEX iterates all rows | Use `FactSupplierMonthlyPerformance` for scorecard/tier visuals; `FactPurchaseOrder` only for drill-through detail tables |
| `[Supplier Recommended Action]` uses `SELECTEDVALUE()` | Wrapped with `HASONEVALUE()` guard — returns "—" in non-row context |
| String/text measures (`[OTIF Status Label]`, `[Supplier Recommended Action]`) | Cannot be aggregated — disable aggregation in column properties; set Data Type = Text |
| `USERELATIONSHIP()` in `[Lead Time Variance Days (Actual Dock Date)]` adds query overhead | Only place this measure on delivery-period time-intelligence visuals, not on the summary page cards |
| `FactSupplierMonthlyPerformance` is ~11,852 rows | No performance concern — all scorecard measures on this table are extremely fast |
| `[Tier3 Stockout Attribution Pct]` joins across `FactStockout → DimSupplier` | Validate join path in dbt. If path is via DimProduct[PrimarySupplierKey], this adds a bridge hop — consider materialising the SupplierKey directly on FactStockout |

---

## 13. Semantic Model Organization

### 13.1 Display Folder Structure

```
📁 Procurement
  📁 Spend
      [Total Procurement Spend]
      [Total PO Line Count]
  📁 OTIF
      [Completed PO Line Count]
      [PO OTIF Line Count]
      [Procurement OTIF Rate Pct]
      [Procurement On-Time Rate Pct]
      [Procurement In-Full Rate Pct]
      [OTIF vs Target Gap]
      [OTIF Status Label]
  📁 Lead Time
      [Average Actual Lead Time Days]
      [Average Contract Lead Time Days]
      [Average Lead Time Variance Days]
      [Lead Time Variance Days (Actual Dock Date)]
      [Lead Time Card Subtitle]
  📁 Quality
      [Total PO Received Quantity]
      [Total PO Rejected Quantity]
      [Inbound Defect Rate Pct]
      [Inbound Acceptance Rate Pct]
      [Acceptance Rate Label]
      [Defect Rate Label]
      [Pct of Late Deliveries by Root Cause]
📁 Supplier
  📁 Scorecard
      [Active Supplier Count]
      [Supplier Monthly OTIF Avg Pct]
      [Scorecard Average Lead Time Days]
      [Scorecard Average Lead Time Std Dev]
      [Scorecard Avg Line Fill Rate Pct]
      [Critical SLA Breach Supplier Count]
      [Supplier Vendor Risk Score]
      [Tier3 Stockout Attribution Pct]
  📁 Actions
      [Supplier Recommended Action]
      [Action Background Color]
```

### 13.2 Hidden Technical Columns (set IsHidden = true in semantic model)

- All `*Key` integer surrogate key columns across all fact and dimension tables
- `DimSupplier[VendorRiskScore]` — exposed via `[Supplier Vendor Risk Score]` measure
- `FactPurchaseOrder[IsDeliveredOnTimeFlag]`, `[IsDeliveredInFullFlag]`, `[IsSupplierOTIFFlag]` — exposed via OTIF measures

---

## 14. Verification Checklist

Run all checks with no slicers selected (All suppliers, full date range):

### 14.1 Data Model Checks
- [ ] `FactPurchaseOrder → DimSupplier` (Active, single-direction) visible in Model View
- [ ] `FactPurchaseOrder → DimDate`: exactly 1 Active (`POCreationDateKey`), 2 Inactive
- [ ] `FactSupplierMonthlyPerformance → DimSupplier` Active relationship exists
- [ ] `DimSupplier[SupplierTier]` has exactly 3 distinct values

### 14.2 Headline Cards
- [ ] `[Total Procurement Spend]` = **$34.05 B**
- [ ] `[Procurement OTIF Rate Pct]` = **35.8%**
- [ ] `[Procurement On-Time Rate Pct]` = **39.9%**
- [ ] `[Procurement In-Full Rate Pct]` = **85.4%**
- [ ] `[Average Lead Time Variance Days]` = **+1.6 days**

### 14.3 Tier Comparison Matrix
- [ ] Tier 1 row: OTIF ≈ **64.9%**, supplier count = **50**, std dev ≈ **0.0**
- [ ] Tier 2 row: OTIF ≈ **38.4%**, supplier count = **150**
- [ ] Tier 3 row: OTIF ≈ **8.8%**, supplier count = **300**, lead time ≈ **45.8d**, variance ≈ **+4.0d**

### 14.4 Quality Panel
- [ ] `[Total PO Received Quantity]` = **1,364,506,058**
- [ ] `[Total PO Rejected Quantity]` = **20,185,926**
- [ ] `[Inbound Defect Rate Pct]` = **1.48%**
- [ ] `[Inbound Acceptance Rate Pct]` = **98.52%**
- [ ] Root cause bars sum to approximately 100% of late deliveries

### 14.5 SLA Breach Watchlist
- [ ] Visual-level OTIF < 15% filter is active
- [ ] Table sorted by spend descending
- [ ] Row 1: **SUP-0035** | $176.6M | 8.5% OTIF | 47.9d actual | +3.9d variance
- [ ] Conditional formatting applied (Rose background on OTIF column, Rose/Emerald on variance)
- [ ] `[Supplier Recommended Action]` returns "Shift 30% Volume" for SUP-0035

### 14.6 Interactions
- [ ] Click Tier 1 in matrix → headline cards update to Tier 1 values
- [ ] Tier dropdown slicer propagates to all page visuals
- [ ] Right-click on SUP-0035 in table → Drill Through → PO Receiving Detail page opens
- [ ] Back button on PO Receiving Detail returns to Page 4

---

## 15. Open Decisions & Next Steps

| # | Item | Decision Required | Owner |
|:---|:---|:---|:---|
| 1 | ~~Root cause breakdown method~~ | **✅ Resolved 2026-09-17** — Live dbt `DelayRootCauseCategory` column added to `fact_purchase_order.sql` mart via `LEFT JOIN stg_dim_supplier`. Use `FactPurchaseOrder[delay_root_cause_category]` as bar chart axis in Power BI. | — |
| 2 | `FactStockout` direct `SupplierKey` FK | Does `FactStockout` need a direct supplier FK to support `[Tier3 Stockout Attribution Pct]`? | Analytics Engineer |
| 3 | Promised/Actual delivery date columns | Add `PromisedDeliveryDate DATE` and `ActualDockReceiptDate DATE` as pre-computed columns in dbt `FactPurchaseOrder` (for drill-through table display) | Analytics Engineer |
| 4 | Rolling scorecard period default | 12 months recommended — confirm with Procurement Manager stakeholder | Business Stakeholder |
| 5 | Custom visual licensing | OKViz Bullet Chart or Zebra BI for progress bars — standard clustered bar is the approved fallback | BI Developer |

---

## 16. Change Log

| Version | Date | Change |
|:---|:---|:---|
| 1.0 | 2026-09-17 | Initial full implementation plan — created from HTML prototype reference, PROJECT_CONTEXT.md, dimensional_model_design.md, schema_specification.md, existing procurement_measures.dax, supplier_measures.dax analysis |
