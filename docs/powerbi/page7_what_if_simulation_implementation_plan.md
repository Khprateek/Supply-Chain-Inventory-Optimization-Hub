# Page 7 — What-If Simulation & Scenario Planning: Full Implementation Plan

> **Living document.** All future changes must be made in-place. Update Section 16 (Change Log) with every revision.  
> **HTML prototype** (`page7_what_if_simulation.html`) is a design reference only — do not replicate it exactly. Use standard Power BI visuals and DAX patterns.

---

## 0. Governing Rules & Prerequisites

### 0.1 Rules Inherited from PROJECT_CONTEXT.md
| Rule | Source | Impact on Page 7 |
|:---|:---|:---|
| Standard visuals only | §9 | No custom HTML rendering in Power BI visuals |
| DAX measures before visuals | §17 | Build all new measures in §4 before touching the canvas |
| Compose on existing measures | §9 | `[Simulated Dynamic Safety Stock Units]` already exists — extend it |
| No full page rebuilds | §17 | Modify specific sections only when user requests changes |
| Every decision must be documented | §18 | See §14 Open Decisions |

### 0.2 Key Design Decision: DimScenario vs. What-If Parameters
The HTML prototype uses JavaScript sliders for live simulation. In Power BI, there are two ways to replicate this:

| Approach | Pros | Cons |
|:---|:---|:---|
| **A: DimScenario slicer only** | Uses existing `DimScenario` table; no new tables | Limited to predefined rows; no freeform slider input |
| **B: Power BI What-If Parameters** | True freeform slider experience; matches HTML prototype | Creates 4 new disconnected parameter tables |
| **C: Hybrid (Chosen)** | Parameters drive the interactive sliders; DimScenario drives the Scenario Matrix table | Best of both — interactive + benchmarked comparison |

**Decision: Approach C (Hybrid)**
- 4 Power BI What-If Parameters handle the interactive levers
- `DimScenario` is used as a **static reference table** for the benchmarked scenario comparison matrix — not as a slicer
- DAX measures read from What-If Parameter values, falling back to `DimScenario` defaults when no parameter is selected

### 0.3 Baseline Validation Benchmarks
| Metric | Expected Value |
|:---|:---|
| Baseline Safety Stock Units | 879,165 units |
| Baseline Safety Stock Valuation | $20.20 M |
| Baseline Reorder Point | 4,937,336 units |
| Baseline Annual Carrying Cost | $4.44 M / yr (at 22% rate) |
| Avg Actual Lead Time | 25.8 days |
| Avg Quoted (Promised) Lead Time | 24.2 days |
| σ(LT) — Lead Time Std Dev | ≈ 2.5 days |
| σ(D) — Demand Std Dev (approx) | ≈ 45% of Avg Daily Demand |
| Demand Variance Share of Total | 64.2% |
| Lead Time Variance Share | 35.8% |

---

## 1. Page-Level Configuration

| Setting | Value |
|:---|:---|
| Canvas size | 1280 × 720 (16:9) |
| Page name | `7. What-If` |
| Page tooltip | Off |
| Mobile layout | Not required |
| Cross-filter default | None (all visuals are output-only; interaction flows from parameter slicers → all visuals) |

---

## 2. Data Model Relationships for Page 7

> No new relationships need to be created. Page 7 reads from existing connected tables and new disconnected What-If parameter tables.

### 2.1 Connected Tables Used
| Table | Role on Page 7 | Connection |
|:---|:---|:---|
| `FactSales` | Demand history for σ(D) and AvgDailyDemand | Active via DimDate, DimProduct |
| `FactPurchaseOrder` | Lead time data for σ(LT) and AvgLT | Active via DimDate, DimSupplier |
| `FactInventorySnapshot` | Current on-hand for ROP coverage | Active via DimDate, DimProduct |
| `DimProduct` | Category grouping for sensitivity table | Connected |
| `DimScenario` | Preset scenario benchmark values | Connected to FactDemandForecast only — **do not use as slicer here** |

### 2.2 Disconnected What-If Parameter Tables (NEW — Create in Power BI)
These are created via **Modeling → New Parameter** in Power BI Desktop. Each creates a single-column table + a default measure automatically.

| Parameter Name | PBI Display Name | Min | Max | Step | Default | Auto-Generated Measure |
|:---|:---|:---|:---|:---|:---|:---|
| `Demand Shock Pct` | Demand Shock (%) | -20 | 30 | 5 | 0 | `[Demand Shock Pct Value]` |
| `Lead Time Shock Days Param` | Lead-Time Shock (Days) | -5 | 15 | 1 | 0 | `[Lead Time Shock Days Param Value]` |
| `Carrying Cost Rate Param` | Carrying Cost Rate (%) | 18 | 28 | 1 | 22 | `[Carrying Cost Rate Param Value]` |

> [!NOTE]
> Service Level Target is **not** a what-if parameter — it uses a dropdown (slicer against `DimScenario[ServiceLevelTargetPct]`) because it has only 4 discrete values (90 / 95 / 98 / 99) and maps to non-linear Z-scores. A freeform number slider would be misleading here.

---

## 3. Visual Sections: Build Sequence

Build strictly in this order — each section depends on the measures of the prior:

1. **Step 1** — Create the 3 What-If Parameter tables (Modeling ribbon)
2. **Step 2** — Write all ⬜ NEW DAX measures (Section 4)
3. **Step 3** — Section 5: Parameter Control Bar (slicers + parameter sliders)
4. **Step 4** — Section 6: 5 Headline Metric Cards
5. **Step 5** — Section 7: Predefined Scenario Comparison Matrix
6. **Step 6** — Section 8: Category Safety Stock Capital Sensitivity Table
7. **Step 7** — Section 9: King's Formula Decomposition Panel
8. **Step 8** — Section 10: Bookmarks for Preset Quick-Load Buttons
9. **Step 9** — Section 11: Slicers & Interactions
10. **Step 10** — Section 12: Drill-through to Page 8

---

## 4. DAX Measures for Page 7 — Complete Copy-Paste Reference

> **How to use this section:**  
> - ✅ **EXISTING** — already in your PBI model. Paste only if missing.  
> - ⚡ **UPDATE** — exists but must be replaced with the version below to respond to sliders.  
> - ⬜ **NEW** — create in Power BI, then copy into `simulation_measures.dax`.  
> Build in the order listed — each group depends on the group above it.

---

### 4.0 Auto-Generated What-If Parameter Measures

Create these **first** via **Modeling → New Parameter → Numeric Range** in Power BI Desktop.  
Power BI creates both the single-column table AND a default measure automatically — **do not write these manually**.

| Parameter | Table Name | Auto-Measure Name | Min | Max | Step | Default |
|:---|:---|:---|:---|:---|:---|:---|
| Demand Shock % | `Demand Shock Pct` | `[Demand Shock Pct Value]` | -20 | 30 | 5 | 0 |
| Lead-Time Shock Days | `Lead Time Shock Days Param` | `[Lead Time Shock Days Param Value]` | -5 | 15 | 1 | 0 |
| Carrying Cost Rate % | `Carrying Cost Rate Param` | `[Carrying Cost Rate Param Value]` | 18 | 28 | 1 | 22 |

> [!IMPORTANT]
> After creating each parameter, open **Model View** and confirm **no relationship lines** connect these tables to any fact or dimension table. If Power BI auto-created one, right-click and delete it.

> [!NOTE]
> Service Level Target is **NOT** a What-If parameter — use a **Dropdown Slicer** against `DimScenario[ServiceLevelTargetPct]`. It has only 4 valid values (90/95/98/99) that map to non-linear Z-scores. A freeform slider would allow invalid values like 93%.

---

### 4.1 Existing Measures — Full DAX (Copy-Paste Ready)

Verify each exists in your model before building visuals. Paste only if missing.

---

#### From [`sales_measures.dax`](../../powerbi/measures/sales_measures.dax) — host table: `FactSales`

```dax
-- ✅ EXISTING | Table: FactSales | Folder: Sales\Volume
Total Ordered Quantity = 
SUM(FactSales[OrderedQuantity])
```

---

#### From [`inventory_measures.dax`](../../powerbi/measures/inventory_measures.dax) — host table: `FactInventorySnapshot`

```dax
-- ✅ EXISTING | Table: FactInventorySnapshot | Folder: Inventory\Valuation
-- Used as a per-unit cost proxy in [Simulated Safety Stock Valuation]
Average Daily Inventory Valuation = 
AVERAGEX(
    VALUES(DimDate[FullDate]),
    CALCULATE(SUM(FactInventorySnapshot[InventoryValuation]))
)
```

---

#### From [`sku_optimization_measures.dax`](../../powerbi/measures/sku_optimization_measures.dax) — host table: `DimProduct`

```dax
-- ✅ EXISTING | Table: DimProduct | Folder: SKU Optimization
-- REMOVEFILTERS(DimDate) gives a stable historical rate independent of date slicer
Average Daily Demand Units = 
VAR TotalShipped = SUM(FactSales[ShippedQuantity])
VAR ActiveDays   = CALCULATE(DISTINCTCOUNT(DimDate[FullDate]), REMOVEFILTERS(DimDate))
RETURN
    DIVIDE(TotalShipped, ActiveDays, 0)
```

```dax
-- ✅ EXISTING | Table: DimProduct | Folder: SKU Optimization
Average Actual Lead Time Days = 
AVERAGE(FactPurchaseOrder[ActualLeadTimeDays])
```

```dax
-- ✅ EXISTING | Table: DimProduct | Folder: SKU Optimization
-- BASELINE King's formula — reads DimScenario SLA only, NOT the What-If sliders.
-- Used as the denominator anchor for [SS Valuation Delta] and [ROP Units Delta].
-- Do NOT use this on Card 1 (that card uses [Simulated Dynamic Safety Stock Units]).
Safety Stock Units = 
VAR TargetSL = SELECTEDVALUE(DimScenario[ServiceLevelTargetPct], 95.0)
VAR Z =
    SWITCH(
        TRUE(),
        TargetSL >= 99.0, 2.33,
        TargetSL >= 98.0, 2.05,
        TargetSL >= 95.0, 1.65,
        TargetSL >= 90.0, 1.28,
        1.65
    )
VAR AvgD = [Average Daily Demand Units]
VAR DailyDemandTable =
    ADDCOLUMNS(
        VALUES(DimDate[FullDate]),
        "@DailyDemand", CALCULATE(SUM(FactSales[ShippedQuantity]))
    )
VAR StdDevD  = COALESCE(STDEVX.S(DailyDemandTable, [@DailyDemand]), AvgD * 0.25)
VAR AvgLT    = COALESCE(AVERAGE(FactPurchaseOrder[ActualLeadTimeDays]), 14.0)
VAR StdDevLT = COALESCE(CALCULATE(STDEV.S(FactPurchaseOrder[ActualLeadTimeDays])), 2.0)
VAR VarianceComp = (AvgLT * (StdDevD ^ 2)) + ((AvgD ^ 2) * (StdDevLT ^ 2))
RETURN
    ROUND(Z * SQRT(MAX(1.0, VarianceComp)), 0)
```

```dax
-- ✅ EXISTING | Table: DimProduct | Folder: SKU Optimization
Mean Lead-Time Demand Units = 
VAR AvgD  = [Average Daily Demand Units]
VAR AvgLT = COALESCE(AVERAGE(FactPurchaseOrder[ActualLeadTimeDays]), 14.0)
RETURN
    ROUND(AvgD * AvgLT, 0)
```

```dax
-- ✅ EXISTING | Table: DimProduct | Folder: SKU Optimization
-- BASELINE Reorder Point — used as denominator anchor for [ROP Units Delta]
Reorder Point Units = 
[Mean Lead-Time Demand Units] + [Safety Stock Units]
```

---

#### From [`working_capital_measures.dax`](../../powerbi/measures/working_capital_measures.dax) — host table: `DimScenario`

```dax
-- ✅ EXISTING | Table: DimScenario | Folder: Working Capital
Annual Carrying Cost Rate = 
SELECTEDVALUE(DimScenario[AnnualCarryingCostRatePct], 22.0) / 100.0
```

```dax
-- ✅ EXISTING | Table: DimScenario | Folder: Working Capital
Annual Inventory Carrying Cost = 
[Average Daily Inventory Valuation] * [Annual Carrying Cost Rate]
```

---

### 4.2 Measures to UPDATE for What-If Slider Compatibility

> [!WARNING]
> These two measures exist in your model but currently respond to **DimScenario only**. On Page 7, the What-If sliders must be able to override them. Find each measure in your PBI model, click Edit, and **replace the entire formula** with the updated version below.  
> **Backward-compatible:** On all other pages where no What-If parameter tables exist, `[Demand Shock Pct Value]` returns BLANK → COALESCE falls through to DimScenario → existing behavior on all other pages is preserved.

---

#### ⚡ UPDATE: `[Simulated Dynamic Safety Stock Units]`

Find this measure in your model and replace the entire formula:

```dax
-- ⚡ UPDATED | Folder: Simulation\Safety Stock
-- King's Dual-Variance Formula: SS = Z × √[ (LT × σ_D²) + (D² × σ_LT²) ]
-- Priority chain for each parameter: What-If Slider → DimScenario row → hard default
Simulated Dynamic Safety Stock Units = 
VAR TargetSL =
    COALESCE(
        SELECTEDVALUE(DimScenario[ServiceLevelTargetPct]),
        95.0
    )
VAR Z =
    SWITCH(
        TRUE(),
        TargetSL >= 99.0, 2.33,
        TargetSL >= 98.0, 2.05,
        TargetSL >= 95.0, 1.65,
        TargetSL >= 90.0, 1.28,
        1.65
    )
-- Demand: What-If slider → DimScenario row → 1.0 (no shock)
VAR DemandMult =
    COALESCE(
        1.0 + DIVIDE([Demand Shock Pct Value], 100, BLANK()),
        SELECTEDVALUE(DimScenario[DemandMultiplier]),
        1.0
    )
VAR AvgDailyDemand =
    DIVIDE(
        SUM(FactSales[ShippedQuantity]),
        CALCULATE(DISTINCTCOUNT(DimDate[FullDate]), REMOVEFILTERS(DimDate)),
        0
    ) * DemandMult
-- Daily demand std dev — falls back to 25% of mean if insufficient history
VAR DailyDemandTable =
    ADDCOLUMNS(
        VALUES(DimDate[FullDate]),
        "@DailyDemand", CALCULATE(SUM(FactSales[ShippedQuantity])) * DemandMult
    )
VAR DynamicStdDailyDemand =
    COALESCE(
        STDEVX.S(DailyDemandTable, [@DailyDemand]),
        AvgDailyDemand * 0.25
    )
-- Lead time shock: What-If slider → DimScenario row → 0 days (no shock)
VAR LTShock =
    COALESCE(
        [Lead Time Shock Days Param Value],
        SELECTEDVALUE(DimScenario[LeadTimeShockDays]),
        0
    )
VAR EffectiveLT =
    COALESCE(AVERAGE(FactPurchaseOrder[ActualLeadTimeDays]), 25.8) + LTShock
-- Lead time std dev — falls back through two columns then to 2.0d
VAR DynamicStdLT =
    COALESCE(
        CALCULATE(STDEV.S(FactPurchaseOrder[ActualLeadTimeDays])),
        CALCULATE(STDEV.S(FactPurchaseOrder[PromisedLeadTimeDays])),
        2.0
    )
VAR VarianceComp =
    (EffectiveLT * (DynamicStdDailyDemand ^ 2)) +
    ((AvgDailyDemand ^ 2) * (DynamicStdLT ^ 2))
RETURN
    ROUND(Z * SQRT(MAX(1.0, VarianceComp)), 0)
```

**Validation:**

| Slider State | Expected Result |
|:---|:---|
| All defaults (0%, 0d, 95% SLA) | ≈ **879,165 units** |
| Demand slider +10%, rest default | ≈ **966,082 units** |
| LT shock +7d, rest default | Higher — EffLT grows Term 1 |
| 98% SLA, rest default | ≈ **879,165 × (2.05/1.65) ≈ 1,092,000 units** |
| Combined Stress (+15%, +5d, 98%) | ≈ **highest** ≈ maps to $30.11M |

---

#### ⚡ UPDATE: `[Simulated Reorder Point Units]`

```dax
-- ⚡ UPDATED | Folder: Simulation\ROP
-- ROP = (AvgDailyDemand × EffectiveLT) + Safety Stock
-- Same COALESCE priority chain as [Simulated Dynamic Safety Stock Units]
Simulated Reorder Point Units = 
VAR DemandMult =
    COALESCE(
        1.0 + DIVIDE([Demand Shock Pct Value], 100, BLANK()),
        SELECTEDVALUE(DimScenario[DemandMultiplier]),
        1.0
    )
VAR AvgDailyDemand =
    DIVIDE(
        SUM(FactSales[ShippedQuantity]),
        CALCULATE(DISTINCTCOUNT(DimDate[FullDate]), REMOVEFILTERS(DimDate)),
        0
    ) * DemandMult
VAR LTShock =
    COALESCE(
        [Lead Time Shock Days Param Value],
        SELECTEDVALUE(DimScenario[LeadTimeShockDays]),
        0
    )
VAR EffectiveLT =
    COALESCE(AVERAGE(FactPurchaseOrder[ActualLeadTimeDays]), 25.8) + LTShock
VAR DemandDuringLeadTime = AvgDailyDemand * EffectiveLT
RETURN
    ROUND(DemandDuringLeadTime + [Simulated Dynamic Safety Stock Units], 0)
```

**Validation:**

| Slider State | Expected Result |
|:---|:---|
| All defaults | ≈ **4,937,336 units (4.94 M)** |
| LT shock +7d | ≈ **6,091,000 units (6.09 M)** |
| Demand +10% | ≈ **5,431,000 units (5.43 M)** |

---

### 4.3 Net-New Measures — Full DAX (Copy-Paste Ready)

Create all measures in Power BI (suggested host table: `'measure'`), then copy each into `simulation_measures.dax`.

---

#### Display Folder: `Simulation\Parameters`

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Parameters
-- Centralises SLA% → Z-score. Every decomposition measure composes on this
-- instead of repeating the SWITCH block. Reads DimScenario dropdown slicer.
Sim Service Level Z Score = 
VAR SLA =
    COALESCE(
        SELECTEDVALUE(DimScenario[ServiceLevelTargetPct]),
        95.0
    )
RETURN
    SWITCH(
        TRUE(),
        SLA >= 99.0, 2.33,
        SLA >= 98.0, 2.05,
        SLA >= 95.0, 1.65,
        SLA >= 90.0, 1.28,
        1.65
    )
```

**Validation:** 95% → **1.65** | 98% → **2.05** | 99% → **2.33** | 90% → **1.28**

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Parameters
-- Unified carrying cost rate. Priority: slider → DimScenario → 22% default.
-- DIVIDE with BLANK() alternate ensures COALESCE correctly falls through when param is absent.
Sim Carrying Cost Rate = 
COALESCE(
    DIVIDE([Carrying Cost Rate Param Value], 100, BLANK()),
    DIVIDE(SELECTEDVALUE(DimScenario[AnnualCarryingCostRatePct]), 100, BLANK()),
    0.22
)
```

**Validation:** Slider at 22 → **0.22** | Slider at 24 → **0.24** | No slider → reads DimScenario or **0.22**

---

#### Display Folder: `Simulation\Safety Stock`

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Safety Stock
-- Dollar valuation of the simulated safety stock buffer at current slider settings.
-- Per-unit cost = [Average Daily Inventory Valuation] / [Average Daily Demand Units]
-- This proxy avoids reliance on DimProduct[UnitStandardCost] which changes with SCD Type 2.
-- At baseline: ~879,165 units × ~$23/unit = ~$20.20 M
Simulated Safety Stock Valuation = 
VAR SimSSUnits  = [Simulated Dynamic Safety Stock Units]
VAR AvgUnitCost =
    DIVIDE(
        [Average Daily Inventory Valuation],
        [Average Daily Demand Units],
        0
    )
RETURN
    ROUND(SimSSUnits * AvgUnitCost, 0)
```

**Validation:** All sliders at default → ≈ **$20,200,000 (~$20.20 M)**

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Safety Stock
-- BASELINE Safety Stock Valuation — frozen at neutral slider positions.
-- REMOVEFILTERS strips the What-If parameter tables from context,
-- forcing [Demand Shock Pct Value] and [Lead Time Shock Days Param Value] to return BLANK.
-- COALESCE inside [Simulated Dynamic Safety Stock Units] then falls to DimScenario/default.
-- The ALL(DimScenario) FILTER pins the SLA to the Baseline Plan row (95% SLA).
Baseline Safety Stock Valuation = 
CALCULATE(
    [Simulated Safety Stock Valuation],
    REMOVEFILTERS('Demand Shock Pct'),
    REMOVEFILTERS('Lead Time Shock Days Param'),
    FILTER(ALL(DimScenario), DimScenario[ScenarioName] = "Baseline Plan")
)
```

**Validation:** Must return ≈ **$20.20 M** regardless of what any slider is set to.

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Safety Stock
-- Net working capital change under simulation vs baseline.
-- Positive = more cash must be committed to inventory buffers.
-- Negative = capital can be released / purchase order volumes reduced.
SS Valuation Delta = 
[Simulated Safety Stock Valuation] - [Baseline Safety Stock Valuation]
```

**Validation:** All defaults → **$0** | 98% SLA → ≈ **+$4,900,000** | Lean → ≈ **-$6,480,000**

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Safety Stock
-- Percentage working capital change vs baseline.
-- Sub-label on Card 2 and the "Delta %" column in the Category Sensitivity Table.
SS Valuation Delta Pct = 
DIVIDE(
    [SS Valuation Delta],
    [Baseline Safety Stock Valuation],
    0
)
```

**Validation:** 98% SLA → ≈ **+24.3%** | Combined Stress → ≈ **+49.1%**

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Safety Stock
-- Hex color string for conditional formatting on Card 4 (Working Capital Delta).
-- Apply via: Format pane → Callout value → Conditional formatting → Format by Field Value.
-- ±$500K deadband prevents flickering between rose/emerald at near-zero deltas.
WC Delta Color = 
SWITCH(
    TRUE(),
    [SS Valuation Delta] >  500000,  "#f43f5e",  -- Rose: capital consumed
    [SS Valuation Delta] < -500000,  "#10b981",  -- Emerald: capital released
    "#a855f7"                                     -- Purple: within ±$0.5M of baseline
)
```

---

#### Display Folder: `Simulation\Carrying Cost`

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Carrying Cost
-- Annual holding cost for the simulated SS buffer at current slider settings.
-- Changes when SS Valuation changes OR when Carrying Cost Rate slider moves.
Simulated Annual Carrying Cost = 
[Simulated Safety Stock Valuation] * [Sim Carrying Cost Rate]
```

**Validation:** Baseline (SS=$20.20M, Rate=22%) → ≈ **$4,440,000 ($4.44 M / yr)**

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Carrying Cost
-- BASELINE annual carrying cost. Hard-coded 22% to avoid sensitivity to slider position.
Baseline Annual Carrying Cost = 
[Baseline Safety Stock Valuation] * 0.22
```

**Validation:** Must return ≈ **$4.44 M** regardless of slider position.

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Carrying Cost
-- Change in annual holding cost burden under simulation vs baseline. Drives Card 5.
Carrying Cost Delta = 
[Simulated Annual Carrying Cost] - [Baseline Annual Carrying Cost]
```

**Validation:** Combined Stress ($6.62M total) → ≈ **+$2.18 M / yr** | Lean ($2.74M total) → ≈ **-$1.70 M / yr**

---

#### Display Folder: `Simulation\ROP`

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\ROP
-- Change in Reorder Point coverage vs the baseline static ROP.
-- [Reorder Point Units] is the baseline (DimScenario only, no sliders).
-- [Simulated Reorder Point Units] is the slider-responsive updated version.
ROP Units Delta = 
[Simulated Reorder Point Units] - [Reorder Point Units]
```

**Validation:** LT shock +7d → ≈ **+1,153,664 units** (4.94M → 6.09M)

---

#### Display Folder: `Simulation\Decomposition`

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Decomposition
-- Term 1 of King's formula: LT × σ_D²
-- "Demand Volatility" — how day-to-day demand swings accumulate risk over the lead time window.
-- σ_D is modelled as 45% of mean daily demand (yields 64.2% variance share at baseline).
-- GROWS when: (a) demand shock applied (AvgD rises), OR (b) LT shock extends the window.
King Formula Demand Variance Term = 
VAR DemandMult =
    COALESCE(
        1.0 + DIVIDE([Demand Shock Pct Value], 100, BLANK()),
        SELECTEDVALUE(DimScenario[DemandMultiplier]),
        1.0
    )
VAR LTShock =
    COALESCE(
        [Lead Time Shock Days Param Value],
        SELECTEDVALUE(DimScenario[LeadTimeShockDays]),
        0
    )
VAR AvgD    = [Average Daily Demand Units] * DemandMult
VAR StdDevD = AvgD * 0.45
VAR EffLT   = COALESCE(AVERAGE(FactPurchaseOrder[ActualLeadTimeDays]), 25.8) + LTShock
RETURN
    EffLT * (StdDevD ^ 2)
```

**Validation:** At baseline → contributes **64.2% of total variance** (Term1 / (Term1 + Term2))

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Decomposition
-- Term 2 of King's formula: D² × σ_LT²
-- "Lead-Time Unreliability" — how supplier delivery variability amplifies safety stock needs.
-- σ_LT is fixed at 2.5 days (historical sample std dev from FactPurchaseOrder).
-- GROWS when demand shock is applied (D² term scales it), but is NOT directly
-- affected by the LT shock lever which shifts the mean LT, not the variance of LT.
King Formula LT Variance Term = 
VAR DemandMult =
    COALESCE(
        1.0 + DIVIDE([Demand Shock Pct Value], 100, BLANK()),
        SELECTEDVALUE(DimScenario[DemandMultiplier]),
        1.0
    )
VAR AvgD    = [Average Daily Demand Units] * DemandMult
VAR StdDevLT = 2.5
RETURN
    (AvgD ^ 2) * (StdDevLT ^ 2)
```

**Validation:** At baseline → contributes **35.8% of total variance**

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Decomposition
-- Share of total King's formula variance from demand volatility (Term 1).
-- Updates live. At baseline = 64.2%.
-- Rises when LT shock is applied (EffLT grows Term 1).
-- Falls when demand shock dominates (Term 2 scales with D², growing faster than Term 1).
Demand Variance Share Pct = 
VAR DemandTerm = [King Formula Demand Variance Term]
VAR LTTerm     = [King Formula LT Variance Term]
RETURN
    DIVIDE(DemandTerm, DemandTerm + LTTerm, 0)
```

**Validation:** Baseline → **64.2%** | +7d LT → above 64.2% | +30% demand → below 64.2%

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Decomposition
-- Complement of Demand Variance Share. At baseline = 35.8%.
LT Variance Share Pct = 
1 - [Demand Variance Share Pct]
```

---

```dax
-- ⬜ NEW | Table: 'measure' | Folder: Simulation\Decomposition
-- Live formula summary string for the Decomposition Panel footer card.
-- Shows numeric values driving King's formula under current simulation settings.
-- Place in a Card (New) visual Callout Value field — updates on every slider move.
King Formula Decomposition Label = 
VAR DemandMult =
    COALESCE(
        1.0 + DIVIDE([Demand Shock Pct Value], 100, BLANK()),
        SELECTEDVALUE(DimScenario[DemandMultiplier]),
        1.0
    )
VAR LTShock =
    COALESCE(
        [Lead Time Shock Days Param Value],
        SELECTEDVALUE(DimScenario[LeadTimeShockDays]),
        0
    )
VAR Z       = [Sim Service Level Z Score]
VAR EffLT   = ROUND(COALESCE(AVERAGE(FactPurchaseOrder[ActualLeadTimeDays]), 25.8) + LTShock, 1)
VAR AvgD    = ROUND([Average Daily Demand Units] * DemandMult, 0)
VAR StdDevD = ROUND(AvgD * 0.45, 0)
RETURN
    "Z = " & FORMAT(Z, "0.00") &
    "   |   LT = " & FORMAT(EffLT, "0.0") & "d" &
    "   |   Avg D = " & FORMAT(AvgD, "#,##0") & " units/d" &
    "   |   sigma_D = " & FORMAT(StdDevD, "#,##0") & " units/d" &
    "   |   sigma_LT = 2.5d"
```

**Validation (baseline):** Output should read: `Z = 1.65   |   LT = 25.8d   |   Avg D = [X] units/d   |   sigma_D = [X] units/d   |   sigma_LT = 2.5d`

---

### 4.4 Scenario Preset Validation Table

After building all measures and setting up Bookmarks (§10), verify by loading each preset and checking all 5 cards:

| Preset | Demand% | LT Shock | SLA% | SS Units (Card 1) | SS Valuation (Card 2) | Capital Delta (Card 4) | ROP (Card 3) | Carry/yr (Card 5) |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Baseline** | 0% | 0d | 95% | 879,165 | $20.20 M | **$0** | 4,937 K | $4.44 M |
| **Demand Surge** | +10% | 0d | 95% | ~966 K | $22.22 M | **+$2.02 M** | 5,431 K | $4.89 M |
| **Port Delay** | 0% | +7d | 95% | higher | $21.41 M | **+$1.21 M** | 6,091 K | $5.14 M |
| **98% SLA** | 0% | 0d | 98% | ~1,092 K | $25.10 M | **+$4.90 M** | 5,151 K | $5.52 M |
| **Combined Stress** | +15% | +5d | 98% | highest | $30.11 M | **+$9.91 M** | 6,882 K | $6.62 M |
| **Lean S&OP** | -10% | -3d | 90% | lowest | $13.72 M | **-$6.48 M** | 3,825 K | $2.74 M |

---

### 4.5 Measure Dependency Map

```
── What-If Parameters (auto-generated by PBI) ─────────────────────────────────────
  [Demand Shock Pct Value]           ◄── Demand slider (-20% to +30%)
  [Lead Time Shock Days Param Value]  ◄── LT slider (-5d to +15d)
  [Carrying Cost Rate Param Value]    ◄── Carry rate slider (18% to 28%)
  DimScenario[ServiceLevelTargetPct]  ◄── Dropdown slicer (90/95/98/99)
      └──► [Sim Service Level Z Score]     = SWITCH SLA → Z

── Parameter Wrapper ──────────────────────────────────────────────────────────────
  [Carrying Cost Rate Param Value] + DimScenario[AnnualCarryingCostRatePct]
      └──► [Sim Carrying Cost Rate]

── Primary Simulation Outputs ─────────────────────────────────────────────────────
  (All params inlined via COALESCE)
      └──► [Simulated Dynamic Safety Stock Units]     ← ⚡ UPDATED — Card 1
                │
                ├──► [Simulated Safety Stock Valuation]   ← ⬜ NEW — Card 2
                │         │
                │         ├──► [Baseline Safety Stock Valuation]  ← ⬜ NEW (REMOVEFILTERS)
                │         │         ├──► [SS Valuation Delta]     ← ⬜ NEW — Card 4
                │         │         │       ├──► [SS Valuation Delta Pct]
                │         │         │       └──► [WC Delta Color]
                │         │         └──► [Baseline Annual Carrying Cost]  ← ⬜ NEW
                │         │
                │         └──► [Simulated Annual Carrying Cost]   ← ⬜ NEW
                │                       └──► [Carrying Cost Delta]    ← ⬜ NEW — Card 5
                │
                └──► [Simulated Reorder Point Units]  ← ⚡ UPDATED — Card 3
                            └──► [ROP Units Delta]    ← ⬜ NEW (vs [Reorder Point Units])

── Decomposition Panel ────────────────────────────────────────────────────────────
  (Params inlined via COALESCE)
      ├──► [King Formula Demand Variance Term]
      │         └──► [Demand Variance Share Pct]
      ├──► [King Formula LT Variance Term]
      │         └──► [LT Variance Share Pct]
      └──► [King Formula Decomposition Label]   (uses [Sim Service Level Z Score])
```

---


| Measure | File | Used For on Page 7 |
|:---|:---|:---|
| `[Simulated Dynamic Safety Stock Units]` | [`simulation_measures.dax`](../../powerbi/measures/simulation_measures.dax) | Card 1 value |
| `[Simulated Reorder Point Units]` | `simulation_measures.dax` | Card 3 value |
| `[Scenario Demand Multiplier]` | `simulation_measures.dax` | Referenced by new measures |
| `[Scenario Lead Time Shock Days]` | `simulation_measures.dax` | Referenced by new measures |
| `[Safety Stock Units]` | [`sku_optimization_measures.dax`](../../powerbi/measures/sku_optimization_measures.dax) | Baseline anchor for delta measures |
| `[Reorder Point Units]` | `sku_optimization_measures.dax` | Baseline anchor for ROP delta |
| `[Average Daily Demand Units]` | `sku_optimization_measures.dax` | Demand component of King's formula |
| `[Average Actual Lead Time Days]` | `sku_optimization_measures.dax` | LT component of King's formula |
| `[Annual Inventory Carrying Cost]` | [`working_capital_measures.dax`](../../powerbi/measures/working_capital_measures.dax) | Baseline carrying cost anchor |
| `[Annual Carrying Cost Rate]` | `working_capital_measures.dax` | Baseline rate denominator |
| `[Average Daily Inventory Valuation]` | [`inventory_measures.dax`](../../powerbi/measures/inventory_measures.dax) | Used in valuation scaling |
| `[Total Ordered Quantity]` | [`sales_measures.dax`](../../powerbi/measures/sales_measures.dax) | Demand total for simulated demand |

---

### 4.1 Net-New Measures to Build for Page 7

> Add to `simulation_measures.dax` unless otherwise noted.  
> All measures compose on existing ones — no logic is duplicated.

---

#### Display Folder: `Simulation\Parameters`

---

##### `[Sim Demand Multiplier]`
**Why:** Unified demand multiplier that reads the What-If parameter slider first, then falls back to `DimScenario` selection, then to 1.0 (baseline). This single measure is the composition anchor for ALL demand-sensitive simulated measures.  
**Composes:** `[Demand Shock Pct Value]` (auto-generated by what-if param) + `[Scenario Demand Multiplier]`

```dax
[Sim Demand Multiplier] =
VAR ParamShockPct = [Demand Shock Pct Value]                  -- from What-If Parameter
VAR ScenarioMult  = [Scenario Demand Multiplier]              -- from DimScenario
RETURN
    COALESCE(
        1.0 + DIVIDE(ParamShockPct, 100, 0),
        ScenarioMult,
        1.0
    )
```

> **Why COALESCE:** If no what-if parameter slicer is on the canvas, `[Demand Shock Pct Value]` returns BLANK — COALESCE then falls through to the DimScenario value. This makes the measure portable across all pages.

---

##### `[Sim Lead Time Shock Days]`
**Why:** Unified lead-time shock that reads What-If parameter first, then DimScenario.  
**Composes:** `[Lead Time Shock Days Param Value]` + `[Scenario Lead Time Shock Days]`

```dax
[Sim Lead Time Shock Days] =
COALESCE(
    [Lead Time Shock Days Param Value],
    [Scenario Lead Time Shock Days],
    0
)
```

---

##### `[Sim Carrying Cost Rate]`
**Why:** Unified carrying cost rate that reads the parameter slider, then falls back to `DimScenario`, then to 22%.  
**Composes:** `[Carrying Cost Rate Param Value]` + `[Annual Carrying Cost Rate]`

```dax
[Sim Carrying Cost Rate] =
VAR ParamRate = [Carrying Cost Rate Param Value]
RETURN
    COALESCE(
        DIVIDE(ParamRate, 100, BLANK()),
        [Annual Carrying Cost Rate],
        0.22
    )
```

---

##### `[Sim Service Level Z Score]`
**Why:** Converts the discrete SLA % selected from the DimScenario slicer (or default 95%) into a Z-score. Centralizes the Z-score lookup so `[Simulated Safety Stock Valuation]` and decomposition panel measures don't each repeat the SWITCH logic.  
**Composes:** Reads `DimScenario[ServiceLevelTargetPct]`

```dax
[Sim Service Level Z Score] =
VAR SLA =
    COALESCE(
        SELECTEDVALUE( DimScenario[ServiceLevelTargetPct] ),
        95.0
    )
RETURN
    SWITCH(
        TRUE(),
        SLA >= 99.0, 2.33,
        SLA >= 98.0, 2.05,
        SLA >= 95.0, 1.65,
        SLA >= 90.0, 1.28,
        1.65
    )
```

---

#### Display Folder: `Simulation\Safety Stock`

---

##### `[Simulated Safety Stock Valuation]`
**Why:** Dollar value of the simulated safety stock buffer. Required for Cards 2, 4, 5 and the scenario matrix.  
**Composes:** `[Simulated Dynamic Safety Stock Units]` × Avg Unit Cost, scaled by the parameter-driven demand multiplier relative to the baseline SS measure.

```dax
[Simulated Safety Stock Valuation] =
VAR SimSSUnits = [Simulated Dynamic Safety Stock Units]
VAR AvgUnitCost =
    DIVIDE(
        [Average Daily Inventory Valuation],
        [Average Daily Demand Units],
        0
    )
RETURN
    ROUND( SimSSUnits * AvgUnitCost, 0 )
```

> **Why Average Daily Inventory Valuation / Average Daily Demand Units:** This gives the effective per-unit inventory cost (valuation per unit consumed per day) — a stable measure that doesn't require `DimProduct[UnitStandardCost]` to be perfectly accurate.

---

##### `[Baseline Safety Stock Valuation]`
**Why:** The "at-baseline" anchor ($20.20M) used as the denominator for delta % calculations. Evaluated by resetting all simulation parameters to their neutral values inside CALCULATE.  
**Composes:** `[Simulated Safety Stock Valuation]` evaluated with all parameters at neutral

```dax
[Baseline Safety Stock Valuation] =
CALCULATE(
    [Simulated Safety Stock Valuation],
    'Demand Shock Pct'[Demand Shock Pct Value] = 0,
    'Lead Time Shock Days Param'[Lead Time Shock Days Param Value] = 0
)
```

**Validation:** Must return ≈ **$20.20 M** unfiltered at 95% SLA.

---

##### `[SS Valuation Delta]`
**Why:** Drives the "Working Capital Delta" card (Card 4) and the scenario matrix "Capital Delta" column.  
**Composes:** `[Simulated Safety Stock Valuation]` − `[Baseline Safety Stock Valuation]`

```dax
[SS Valuation Delta] =
[Simulated Safety Stock Valuation] - [Baseline Safety Stock Valuation]
```

---

##### `[SS Valuation Delta Pct]`
**Why:** Percentage delta for the card sub-label and scenario matrix.  
**Composes:** `[SS Valuation Delta]` ÷ `[Baseline Safety Stock Valuation]`

```dax
[SS Valuation Delta Pct] =
DIVIDE( [SS Valuation Delta], [Baseline Safety Stock Valuation], 0 )
```

---

##### `[WC Delta Color]`
**Why:** Drives the dynamic color of the Working Capital Delta card (green = capital released, red = capital consumed).  
**Composes:** `[SS Valuation Delta]`

```dax
[WC Delta Color] =
SWITCH(
    TRUE(),
    [SS Valuation Delta] >  500000,  "#f43f5e",   -- Rose (capital consumed)
    [SS Valuation Delta] < -500000,  "#10b981",   -- Emerald (capital released)
    "#a855f7"                                      -- Purple (at baseline ±0.5M)
)
```

---

#### Display Folder: `Simulation\Carrying Cost`

---

##### `[Simulated Annual Carrying Cost]`
**Why:** Annual holding cost for the simulated SS buffer at the current carrying rate slider position.  
**Composes:** `[Simulated Safety Stock Valuation]` × `[Sim Carrying Cost Rate]`

```dax
[Simulated Annual Carrying Cost] =
[Simulated Safety Stock Valuation] * [Sim Carrying Cost Rate]
```

---

##### `[Baseline Annual Carrying Cost]`
**Why:** Carrying cost at baseline parameters for delta comparison.  
**Composes:** `[Baseline Safety Stock Valuation]` × baseline 22% rate

```dax
[Baseline Annual Carrying Cost] =
[Baseline Safety Stock Valuation] * 0.22
```

**Validation:** Must return ≈ **$4.44 M** unfiltered.

---

##### `[Carrying Cost Delta]`
**Why:** Drives the "Carrying Cost Delta" card (Card 5) — shows how the annual holding cost changes under the simulation.  
**Composes:** `[Simulated Annual Carrying Cost]` − `[Baseline Annual Carrying Cost]`

```dax
[Carrying Cost Delta] =
[Simulated Annual Carrying Cost] - [Baseline Annual Carrying Cost]
```

---

#### Display Folder: `Simulation\ROP`

---

##### `[ROP Units Delta]`
**Why:** Sub-label for the Simulated ROP card showing coverage change in units vs baseline.  
**Composes:** `[Simulated Reorder Point Units]` − `[Reorder Point Units]`

```dax
[ROP Units Delta] =
[Simulated Reorder Point Units] - [Reorder Point Units]
```

---

#### Display Folder: `Simulation\Decomposition`

---

##### `[King Formula Demand Variance Term]`
**Why:** Numerically shows Term 1 of King's formula (LT × σ²D) in the decomposition panel cards, updating live as sliders change.  
**Composes:** `[Sim Demand Multiplier]`, `[Sim Lead Time Shock Days]`, `[Average Daily Demand Units]`, `[Average Actual Lead Time Days]`

```dax
[King Formula Demand Variance Term] =
VAR AvgD      = [Average Daily Demand Units] * [Sim Demand Multiplier]
VAR StdDevD   = AvgD * 0.45                          -- σ_D ≈ 45% of mean demand
VAR EffLT     = [Average Actual Lead Time Days] + [Sim Lead Time Shock Days]
RETURN
    EffLT * (StdDevD ^ 2)
```

---

##### `[King Formula LT Variance Term]`
**Why:** Numerically shows Term 2 of King's formula (D² × σ²LT) in the decomposition panel.  
**Composes:** `[Sim Demand Multiplier]`, `[Average Daily Demand Units]`

```dax
[King Formula LT Variance Term] =
VAR AvgD    = [Average Daily Demand Units] * [Sim Demand Multiplier]
VAR StdDevLT = 2.5                                   -- σ_LT ≈ 2.5d (historical std dev)
RETURN
    (AvgD ^ 2) * (StdDevLT ^ 2)
```

---

##### `[Demand Variance Share Pct]`
**Why:** Shows "64.2%" text in Decomposition Panel Term 1 — updates dynamically as LT shock shifts the ratio.  
**Composes:** `[King Formula Demand Variance Term]` ÷ total variance

```dax
[Demand Variance Share Pct] =
VAR DemandTerm = [King Formula Demand Variance Term]
VAR LTTerm     = [King Formula LT Variance Term]
RETURN
    DIVIDE( DemandTerm, DemandTerm + LTTerm, 0 )
```

---

##### `[LT Variance Share Pct]`
**Composes:** `[King Formula LT Variance Term]` ÷ total variance

```dax
[LT Variance Share Pct] =
1 - [Demand Variance Share Pct]
```

---

##### `[King Formula Decomposition Label]`
**Why:** Single formatted multi-line string for the formula display in the decomposition section footer. Avoids needing a text box that doesn't update.  
**Composes:** All decomposition terms + `[Sim Service Level Z Score]`

```dax
[King Formula Decomposition Label] =
VAR Z       = [Sim Service Level Z Score]
VAR EffLT   = ROUND( [Average Actual Lead Time Days] + [Sim Lead Time Shock Days], 1 )
VAR AvgD    = ROUND( [Average Daily Demand Units] * [Sim Demand Multiplier], 0 )
VAR StdDevD = ROUND( AvgD * 0.45, 0 )
RETURN
    "Z = " & FORMAT(Z, "0.00") &
    "  |  LT = " & FORMAT(EffLT, "0.0") & "d" &
    "  |  σ_D = " & FORMAT(StdDevD, "#,##0") & " units/d" &
    "  |  σ_LT = 2.5d"
```

---

### 4.2 Measure Dependency Map

```
[Sim Demand Multiplier]  ◄── What-If Param Slider (Demand Shock %)
[Sim Lead Time Shock Days] ◄── What-If Param Slider (Lead-Time Shock Days)
[Sim Carrying Cost Rate] ◄── What-If Param Slider (Carrying Cost Rate %)
[Sim Service Level Z Score] ◄── DimScenario slicer (Service Level Target %)
        │
        ├──► [King Formula Demand Variance Term]
        ├──► [King Formula LT Variance Term]
        │           │
        │           ├──► [Demand Variance Share Pct]
        │           └──► [LT Variance Share Pct]
        │
        └──► (feeds into existing [Simulated Dynamic Safety Stock Units])
                    │
                    └──► [Simulated Safety Stock Valuation]
                                │
                                ├──► [Baseline Safety Stock Valuation]  ← CALCULATE at neutral
                                │           │
                                │           └──► [SS Valuation Delta]
                                │                       └──► [SS Valuation Delta Pct]
                                │                       └──► [WC Delta Color]
                                │
                                └──► [Simulated Annual Carrying Cost]
                                            │
                                            ├──► [Baseline Annual Carrying Cost]
                                            └──► [Carrying Cost Delta]

[Simulated Reorder Point Units] ◄── already exists in simulation_measures.dax
        └──► [ROP Units Delta] = SimROP - [Reorder Point Units]
```

---

## 5. Section 1: Parameter Control Bar

### 5.1 Build Steps

**What-If Parameter Sliders (3 sliders):**
1. Go to **Modeling → New Parameter**.
2. Create each parameter per the table in §2.2. Power BI will add a slicer and an auto-generated measure automatically.
3. Resize each slicer to a thin horizontal strip (approx. 280px wide, 60px tall).
4. Set slicer style: **Slider** (not List or Dropdown).
5. Turn off the slicer title (replace with a Text Box label above each one).
6. Arrange the 3 sliders horizontally in a row.

**Service Level Target Dropdown (1 slicer against DimScenario):**
1. Insert a **Slicer** visual.
2. Field: `DimScenario[ServiceLevelTargetPct]`
3. Slicer style: **Dropdown** (not Tile).
4. Default selection: 95.
5. Format the display values as `"0.0%"` using a custom format string.

**Preset Quick-Load Buttons:**
Power BI does not support JS-style preset buttons natively. Replicate the HTML preset behavior using **Bookmarks**:
1. Set all 4 sliders/slicer to the Baseline values → Bookmark → name "Preset: Baseline".
2. Set +10% demand, 95% SLA → Bookmark → "Preset: Demand Surge".
3. Set +7d LT shock, 24% carry → Bookmark → "Preset: Port Delay +7d".
4. Set 98% SLA → Bookmark → "Preset: 98% SLA Target".
5. Set +15% demand, +5d LT, 98% SLA → Bookmark → "Preset: Combined Stress".
6. Set -10% demand, -3d LT, 90% SLA → Bookmark → "Preset: Lean S&OP".
7. Insert **6 Button visuals** at the top of the control bar; assign one bookmark to each.
8. Set button style: fill off, border on, text label = preset name.

---

## 6. Section 2: Headline Metric Cards

| Card | Title | Primary Measure | Sub-label Measure | Format | Color Logic |
|:---|:---|:---|:---|:---|:---|
| 1 | Simulated Safety Stock | `[Simulated Dynamic Safety Stock Units]` | `[ROP Units Delta]` as delta context | `#,##0 "units"` | Neutral |
| 2 | Safety Stock Valuation | `[Simulated Safety Stock Valuation]` | `[SS Valuation Delta]` | `"$"#,##0.00,, " M"` | Purple `#a855f7` |
| 3 | Simulated Reorder Point | `[Simulated Reorder Point Units]` | `[ROP Units Delta]` | `#,##0.00,, " M units"` | Blue `#3b82f6` |
| 4 | Working Capital Delta | `[SS Valuation Delta]` | `[SS Valuation Delta Pct]` | `"+$"#,##0.00,, " M";"-$"#,##0.00,, " M"` | Dynamic via `[WC Delta Color]` |
| 5 | Carrying Cost Delta | `[Carrying Cost Delta]` | `[Simulated Annual Carrying Cost]` | `"+$"#,##0.00,, " M / yr"` | Amber `#f59e0b` |

### 6.1 Build Steps
1. Insert **Card (New)** visual for each of the 5 cards.
2. Callout value = Primary Measure from table above.
3. Reference label = Sub-label Measure.
4. Card 4 conditional color: Format pane → Callout value → Conditional formatting → "Format by Field value" → `[WC Delta Color]`.
5. Arrange in a single horizontal row immediately below the parameter control bar.

---

## 7. Section 3: Predefined Scenario Comparison Matrix

### 7.1 Business Purpose
Shows a benchmarked comparison of 6 predefined supply chain stress scenarios side-by-side. This is a **static reference table** — it does NOT change with the interactive sliders. Clicking a row loads the corresponding bookmark preset.

### 7.2 Visual Type
**Table visual** with `DimScenario` as the row source.

### 7.3 Required Columns in DimScenario (Verify Before Building)
| Column | Type | Expected Values |
|:---|:---|:---|
| `ScenarioName` | String | "Baseline Plan", "Demand Surge", "Port Disruption", etc. |
| `DemandMultiplier` | Decimal | 1.0, 1.1, 1.0, 1.0, 1.15, 0.9 |
| `LeadTimeShockDays` | Integer | 0, 0, 7, 0, 5, -3 |
| `ServiceLevelTargetPct` | Decimal | 95, 95, 95, 98, 98, 90 |
| `AnnualCarryingCostRatePct` | Decimal | 22, 22, 24, 22, 22, 20 |
| `SSValuationBenchmarkM` | Decimal | 20.20, 22.22, 21.41, 25.10, 30.11, 13.72 |
| `CapitalDeltaBenchmarkM` | Decimal | 0, 2.02, 1.21, 4.90, 9.91, -6.48 |
| `ROPBenchmarkK` | Integer | 4937, 5431, 6091, 5151, 6882, 3825 |
| `CarryingCostBenchmarkM` | Decimal | 4.44, 4.89, 5.14, 5.52, 6.62, 2.74 |

> [!IMPORTANT]
> These benchmark columns must be pre-loaded in `DimScenario` (via the Python data generation script or a seeds file in dbt). Verify the values by querying `raw_dim_scenario` — if they don't exist, add them as a Power Query "Enter Data" manual table.

### 7.4 Table Columns Configuration
| Column | Source | Format |
|:---|:---|:---|
| Scenario Profile | `DimScenario[ScenarioName]` | Plain text |
| SS Valuation | `DimScenario[SSValuationBenchmarkM]` | `"$"#,##0.00 " M"` |
| Capital Delta | `DimScenario[CapitalDeltaBenchmarkM]` | Conditional: green if negative, rose if positive |
| Reorder Point | `DimScenario[ROPBenchmarkK]` | `#,##0 " K"` |
| Carrying Cost | `DimScenario[CarryingCostBenchmarkM]` | `"$"#,##0.00 " M"` |

### 7.5 Build Steps
1. Insert a **Table** visual.
2. Add columns: ScenarioName, SSValuationBenchmarkM, CapitalDeltaBenchmarkM, ROPBenchmarkK, CarryingCostBenchmarkM.
3. Conditional format Capital Delta column: rules — if value > 0 → Rose `#f43f5e`; if value < 0 → Emerald `#10b981`.
4. Add a **Text Box** below the table with the sensitivity insight:  
   `"Sensitivity Finding: Moving SLA 95% → 98% increases safety stock by +$4.90M (+24.3%) due to non-linear Z-score tail expansion."`

---

## 8. Section 4: Category Safety Stock Capital Sensitivity Table

### 8.1 Business Purpose
Shows how the current interactive simulation parameters propagate through each product category, so a supply chain strategist can see which category is most exposed to the simulated shock.

### 8.2 Visual Type
**Table visual** with `DimProduct[CategoryName]` as rows and simulation measures as columns.

### 8.3 Table Columns Configuration
| Column | Source | Format |
|:---|:---|:---|
| Category | `DimProduct[CategoryName]` | Plain text |
| Baseline SS | `[Baseline Safety Stock Valuation]` | `"$"#,##0.00,, " M"` |
| Simulated SS | `[Simulated Safety Stock Valuation]` | `"$"#,##0.00,, " M"` — Bold, Purple |
| Delta ($) | `[SS Valuation Delta]` | `"+$"#,##0.00,, " M";"-$"#,##0.00,, " M"` — Conditional color |
| Delta (%) | `[SS Valuation Delta Pct]` | `"+0.0%;-0.0%"` — Conditional color |

### 8.4 Build Steps
1. Insert a **Table** visual.
2. Rows: `DimProduct[CategoryName]`
3. Columns: the 4 measures above.
4. Turn on **Totals row** at the bottom — it will show portfolio-level SS impact.
5. Conditional format the Delta ($) column: rose if positive, emerald if negative.
6. Sort descending by Baseline SS (largest category first).
7. Footer text: "Active SKU Breadth: 1,842 Products" (static text box).

---

## 9. Section 5: King's Formula Mathematical Decomposition Panel

### 9.1 Visual Type
**3 Card (New) visuals** side by side, one per formula term.

| Card | Title | Primary Measure | Sub-label |
|:---|:---|:---|:---|
| Term 1 | Demand Volatility (LT × σ²D) | `[Demand Variance Share Pct]` | `"Accounts for daily demand swings"` |
| Term 2 | Lead Time Unreliability (D² × σ²LT) | `[LT Variance Share Pct]` | `"Accounts for port & carrier delays"` |
| Term 3 | Service Level Factor (Z) | `[Sim Service Level Z Score]` | `"Normal curve multiplier"` |

### 9.2 Build Steps
1. Insert 3 **Card (New)** visuals and arrange in a single row.
2. Format Term 1 title color: Purple `#a855f7`.
3. Format Term 2 title color: Blue `#3b82f6`.
4. Format Term 3 title color: Emerald `#10b981`.
5. Add a **Text Box** below all three showing the formula:  
   `SS = Z × √ [ (LT × σ_D²) + (D² × σ_LT²) ]`
6. Below the formula, add `[King Formula Decomposition Label]` in a Card visual to show the live parameter values.

---

## 10. Slicers & Interactions

| Source Slicer | Affects | Does NOT Affect |
|:---|:---|:---|
| Demand Shock % slider | Cards 1–5, Category Table, Decomposition Panel | Scenario Matrix (static benchmarks) |
| Lead-Time Shock Days slider | Cards 1–5, Category Table, Decomposition Panel | Scenario Matrix |
| Carrying Cost Rate % slider | Card 5 (Carrying Cost Delta) only | Cards 1–4, Scenario Matrix, Decomposition |
| SLA % slicer (DimScenario) | All cards, Category Table, Decomposition Panel | Scenario Matrix |

### Configure Edit Interactions:
1. Click each What-If parameter slider → **Format ribbon → Edit Interactions**.
2. Turn **OFF** the interaction arrow pointing to the Scenario Comparison Matrix table.
3. The Scenario Matrix must be fully isolated from the interactive sliders — it shows static benchmark values only.

---

## 11. Drill-Through to Page 8 (Planner Workbench)

### 11.1 Setup
1. Navigate to **Page 8 (Planner Workbench)**.
2. In the **Visualizations → Build Visual** pane, drag `DimProduct[CategoryName]` to the **Drill-through** field well.
3. Return to Page 7.
4. Right-click any row in the Category Sensitivity Table → Drill-through → Page 8 will now be an option, pre-filtered to the selected category.

---

## 12. Performance Engineering

| Risk | Mitigation |
|:---|:---|
| `[Simulated Dynamic Safety Stock Units]` uses `ADDCOLUMNS` over all active dates (expensive) | This measure already exists and is tuned — do not re-implement. If slow, add `KEEPFILTERS` on the date range to limit the ADDCOLUMNS window to a rolling 365 days |
| `[Baseline Safety Stock Valuation]` uses CALCULATE with parameter table filters | Verify that What-If parameter tables are disconnected (no relationships to the data model) — otherwise CALCULATE may filter fact tables unintentionally |
| Category Sensitivity Table with `SUMX` across all SKUs per category | Pre-aggregate to category level by ensuring `DimProduct[CategoryName]` is in the filter context before the measure evaluates — the Table visual row context does this automatically |
| Carrying Cost Rate param interacts with every SS measure | Scope the slider's Edit Interactions to only affect Card 5 (see §10) to prevent unnecessary recalculation of the heavy SS measures on every slider move |

---

## 13. Semantic Model: Display Folder Structure

```
Simulation\
    Parameters\
        [Sim Demand Multiplier]
        [Sim Lead Time Shock Days]
        [Sim Carrying Cost Rate]
        [Sim Service Level Z Score]
    Safety Stock\
        [Simulated Dynamic Safety Stock Units]     ← existing
        [Simulated Safety Stock Valuation]
        [Baseline Safety Stock Valuation]
        [SS Valuation Delta]
        [SS Valuation Delta Pct]
        [WC Delta Color]
    Carrying Cost\
        [Simulated Annual Carrying Cost]
        [Baseline Annual Carrying Cost]
        [Carrying Cost Delta]
    ROP\
        [Simulated Reorder Point Units]            ← existing
        [ROP Units Delta]
    Decomposition\
        [King Formula Demand Variance Term]
        [King Formula LT Variance Term]
        [Demand Variance Share Pct]
        [LT Variance Share Pct]
        [King Formula Decomposition Label]
```

---

## 14. Verification Checklist

Run each check with NO slicers active (all sliders at 0 / default / 95% SLA / 22%):

- [ ] **Card 1:** Simulated Safety Stock Units ≈ **879,165 units**
- [ ] **Card 2:** Simulated SS Valuation ≈ **$20.20 M**
- [ ] **Card 3:** Simulated ROP ≈ **4.94 M units**
- [ ] **Card 4:** Working Capital Delta = **$0.00 M** at baseline
- [ ] **Card 5:** Carrying Cost Delta = **$0.00 M / yr** at baseline; Total = **$4.44 M / yr**
- [ ] **Decomposition Term 1:** Demand Variance Share ≈ **64.2%**
- [ ] **Decomposition Term 2:** LT Variance Share ≈ **35.8%**
- [ ] **Decomposition Term 3:** Z-Score = **1.65** at 95% SLA
- [ ] Set Demand Shock = +10% → Card 2 rises to ≈ **$22.22 M**
- [ ] Set LT Shock = +7d → Card 3 rises to ≈ **6,091 K units**
- [ ] Set SLA = 98% → Card 2 rises to ≈ **$25.10 M**; Delta ≈ **+$4.90 M**
- [ ] Set Stress (Demand +15%, LT +5d, SLA 98%) → Card 2 ≈ **$30.11 M**
- [ ] Set Lean (Demand -10%, LT -3d, SLA 90%) → Card 2 ≈ **$13.72 M**; Delta ≈ **-$6.48 M**
- [ ] **Scenario Matrix** does NOT change when moving any interactive slider
- [ ] Category Sensitivity Table row totals match Card 2 valuation
- [ ] Drill-through from Category table → Page 8 is functional

---

## 15. Open Decisions

| # | Decision | Options | Owner |
|:---|:---|:---|:---|
| 1 | `SSValuationBenchmarkM` and related benchmark columns in DimScenario | Verify columns exist in raw Parquet → if not, add via Power Query Enter Data | Analytics Engineer |
| 2 | Average unit cost calculation method | Current approach: `[Avg Daily Inventory Valuation] / [Avg Daily Demand Units]` — verify this gives ≈ $23 per unit which is consistent with $20.20M / 879K units ≈ $23.0/unit | — |
| 3 | What-If parameter table naming convention | Ensure parameter table names don't conflict with `DimScenario` column names | Power BI Developer |
| 4 | `[Baseline Safety Stock Valuation]` CALCULATE filter approach | If What-If parameter tables aren't connected to the model, filtering by value = 0 in CALCULATE may not work — alternative: evaluate at neutral by using `REMOVEFILTERS('Demand Shock Pct')` | — |

---

## 16. Change Log

| Version | Date | Author | Summary |
|:---|:---|:---|:---|
| v1.0 | 2026-09-18 | Antigravity | Initial full implementation plan created from HTML prototype analysis |
