# Automated Data Quality & Referential Integrity Validation Suite
import os
import sys
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

def run_validation(data_dir: str = "data/raw"):
    print("=" * 80)
    print(f"AUTOMATED DATA VALIDATION SUITE")
    print(f"Target Directory: {data_dir}")
    print("=" * 80)
    
    # Load all parquet tables or directories
    tables = {}
    for f in os.listdir(data_dir):
        item_path = os.path.join(data_dir, f)
        t_name = f.replace(".parquet", "") if f.endswith(".parquet") else f
        
        try:
            # pd.read_parquet seamlessly handles both single files and partition directories
            tables[t_name] = pd.read_parquet(item_path)
            print(f"  Loaded {t_name}: {len(tables[t_name]):,} rows")
        except Exception as e:
            # Skip items that are not valid parquet data (like .csv, etc.)
            continue
            
    results = []
    
    def assert_check(test_name: str, passed: bool, details: str = ""):
        status = "PASSED" if passed else "FAILED"
        print(f"[{status}] {test_name}: {details}")
        results.append({"Test": test_name, "Status": status, "Details": details})
        if not passed:
            print(f"  --> FAILURE: {details}")

    print("\n--- 1. Primary Key Uniqueness Checks ---")
    pk_map = {
        "DimDate": "DateKey",
        "DimRegion": "RegionKey",
        "DimWarehouse": "WarehouseKey",
        "DimSupplier": "SupplierKey",
        "DimProduct": "ProductKey",
        "DimCustomerChannel": "CustomerChannelKey",
        "DimEmployeePlanner": "PlannerKey",
        "DimScenario": "ScenarioKey",
        "FactSales": "SalesLineKey",
        "FactInventorySnapshot": "SnapshotKey",
        "FactPurchaseOrder": "POLineKey",
        "FactDemandForecast": "ForecastKey",
        "FactInventoryMovement": "MovementKey",
        "FactStockout": "StockoutEventKey",
        "FactCustomerReturns": "ReturnLineKey",
        "FactSupplierMonthlyPerformance": "SupplierMonthlyKey",
    }
    
    for t_name, pk in pk_map.items():
        if t_name in tables:
            df = tables[t_name]
            dups = df[pk].duplicated().sum()
            assert_check(f"PK Uniqueness on {t_name}.{pk}", dups == 0, f"{dups:,} duplicate keys detected (Total: {len(df):,})")

    print("\n--- 2. Referential Integrity & Foreign Key Checks ---")
    dim_date_keys = set(tables["DimDate"]["DateKey"])
    dim_prod_keys = set(tables["DimProduct"]["ProductKey"])
    dim_supp_keys = set(tables["DimSupplier"]["SupplierKey"])
    dim_wh_keys = set(tables["DimWarehouse"]["WarehouseKey"])
    dim_cust_keys = set(tables["DimCustomerChannel"]["CustomerChannelKey"])
    
    # FactSales FKs
    df_sales = tables["FactSales"]
    bad_order_dates = set(df_sales["OrderDateKey"]) - dim_date_keys
    bad_prod_sales = set(df_sales["ProductKey"]) - dim_prod_keys
    bad_cust_sales = set(df_sales["CustomerChannelKey"]) - dim_cust_keys
    bad_wh_sales = set(df_sales["WarehouseKey"]) - dim_wh_keys
    
    assert_check("FactSales -> DimDate (OrderDateKey)", len(bad_order_dates) == 0, f"{len(bad_order_dates)} orphan dates")
    assert_check("FactSales -> DimProduct (ProductKey)", len(bad_prod_sales) == 0, f"{len(bad_prod_sales)} orphan products")
    assert_check("FactSales -> DimCustomerChannel", len(bad_cust_sales) == 0, f"{len(bad_cust_sales)} orphan customers")
    assert_check("FactSales -> DimWarehouse", len(bad_wh_sales) == 0, f"{len(bad_wh_sales)} orphan warehouses")

    # FactInventorySnapshot FKs
    df_inv = tables["FactInventorySnapshot"]
    bad_snap_dates = set(df_inv["SnapshotDateKey"]) - dim_date_keys
    bad_snap_prods = set(df_inv["ProductKey"]) - dim_prod_keys
    bad_snap_whs = set(df_inv["WarehouseKey"]) - dim_wh_keys
    assert_check("FactInventorySnapshot -> DimDate", len(bad_snap_dates) == 0, f"{len(bad_snap_dates)} orphan dates")
    assert_check("FactInventorySnapshot -> DimProduct", len(bad_snap_prods) == 0, f"{len(bad_snap_prods)} orphan products")
    assert_check("FactInventorySnapshot -> DimWarehouse", len(bad_snap_whs) == 0, f"{len(bad_snap_whs)} orphan warehouses")

    # FactPurchaseOrder FKs
    df_po = tables["FactPurchaseOrder"]
    bad_po_supp = set(df_po["SupplierKey"]) - dim_supp_keys
    bad_po_prod = set(df_po["ProductKey"]) - dim_prod_keys
    bad_po_wh = set(df_po["ReceivingWarehouseKey"]) - dim_wh_keys
    assert_check("FactPurchaseOrder -> DimSupplier", len(bad_po_supp) == 0, f"{len(bad_po_supp)} orphan suppliers")
    assert_check("FactPurchaseOrder -> DimProduct", len(bad_po_prod) == 0, f"{len(bad_po_prod)} orphan products")
    assert_check("FactPurchaseOrder -> DimWarehouse", len(bad_po_wh) == 0, f"{len(bad_po_wh)} orphan warehouses")

    print("\n--- 3. Chronological Validity Checks ---")
    bad_ship_dates = (df_sales["ShipDate"] < df_sales["OrderDate"]).sum()
    bad_deliv_dates = (df_sales["DeliveryDate"] < df_sales["ShipDate"]).sum()
    assert_check("FactSales ShipDate >= OrderDate", bad_ship_dates == 0, f"{bad_ship_dates} premature shipments")
    assert_check("FactSales DeliveryDate >= ShipDate", bad_deliv_dates == 0, f"{bad_deliv_dates} premature deliveries")

    df_po_comp = df_po[df_po["POStatus"] == "Completed"]
    bad_po_dates = (df_po_comp["ActualDockReceiptDate"] < df_po_comp["POCreationDate"]).sum()
    assert_check("FactPurchaseOrder DockReceipt >= POCreationDate", bad_po_dates == 0, f"{bad_po_dates} impossible dock arrivals")

    print("\n--- 4. Quantity & Non-Negative Inventory Checks ---")
    neg_sales_qty = (df_sales["OrderedQuantity"] <= 0).sum()
    neg_ship_qty = (df_sales["ShippedQuantity"] < 0).sum()
    neg_cogs = (df_sales["CostOfGoodsSold"] < 0).sum()
    assert_check("FactSales Positive Order Quantity", neg_sales_qty == 0, f"{neg_sales_qty} non-positive order quantities")
    assert_check("FactSales Non-Negative Shipped Quantity", neg_ship_qty == 0, f"{neg_ship_qty} negative shipped quantities")
    assert_check("FactSales Non-Negative COGS", neg_cogs == 0, f"{neg_cogs} negative COGS amounts")

    neg_on_hand = (df_inv["OnHandQuantity"] < 0).sum()
    neg_avail = (df_inv["AvailableQuantity"] < 0).sum()
    neg_val = (df_inv["InventoryValuation"] < 0).sum()
    assert_check("FactInventorySnapshot Non-Negative OnHand", neg_on_hand == 0, f"{neg_on_hand} negative on-hand balances")
    assert_check("FactInventorySnapshot Non-Negative Available", neg_avail == 0, f"{neg_avail} negative available balances")
    assert_check("FactInventorySnapshot Non-Negative Valuation", neg_val == 0, f"{neg_val} negative valuations")

    print("\n--- 5. Stockout Duration & Lost Sales Realism Checks ---")
    df_stockout = tables["FactStockout"]
    if len(df_stockout) > 0:
        bad_duration = (df_stockout["StockoutDurationDays"] <= 0).sum()
        neg_lost_demand = (df_stockout["EstimatedLostDemandUnits"] <= 0).sum()
        neg_lost_rev = (df_stockout["EstimatedLostRevenueAmount"] < 0).sum()
        bad_so_dates = (df_stockout["EndDate"] < df_stockout["StartDate"]).sum()
        assert_check("FactStockout Valid Positive Duration", bad_duration == 0, f"{bad_duration} non-positive durations")
        assert_check("FactStockout Non-Negative Lost Demand", neg_lost_demand == 0, f"{neg_lost_demand} negative lost demand")
        assert_check("FactStockout Non-Negative Lost Revenue", neg_lost_rev == 0, f"{neg_lost_rev} negative lost revenue")
        assert_check("FactStockout EndDate >= StartDate", bad_so_dates == 0, f"{bad_so_dates} inverted dates")

    print("\n--- 6. Customer Returns Consistency Checks ---")
    df_returns = tables["FactCustomerReturns"]
    bad_ret_qty = (df_returns["ReturnedQuantity"] <= 0).sum()
    bad_ret_disp = ((df_returns["RestockedQuantity"] + df_returns["ScrappedQuantity"]) != df_returns["ReturnedQuantity"]).sum()
    bad_ret_dates = (df_returns["ReturnDate"] < df_returns["OriginalOrderDate"]).sum()
    assert_check("FactCustomerReturns Positive Return Qty", bad_ret_qty == 0, f"{bad_ret_qty} non-positive returns")
    assert_check("FactCustomerReturns Disposition Balance (Restocked + Scrapped == Returned)", bad_ret_disp == 0, f"{bad_ret_disp} unbalanced dispositions")
    assert_check("FactCustomerReturns ReturnDate >= OrderDate", bad_ret_dates == 0, f"{bad_ret_dates} premature returns")

    print("\n--- 7. Supplier Scorecard Reconciliation Checks ---")
    df_supp_perf = tables["FactSupplierMonthlyPerformance"]
    bad_otif_rate = ((df_supp_perf["OTIFRatePct"] < 0) | (df_supp_perf["OTIFRatePct"] > 100)).sum()
    bad_fill_rate = (df_supp_perf["LineFillRatePct"] < 0).sum()
    assert_check("FactSupplierMonthlyPerformance Valid OTIF Rate (0-100%)", bad_otif_rate == 0, f"{bad_otif_rate} out-of-bounds OTIF rates")
    assert_check("FactSupplierMonthlyPerformance Non-Negative Fill Rate", bad_fill_rate == 0, f"{bad_fill_rate} negative fill rates")

    print("=" * 80)
    failed_count = sum([1 for r in results if r["Status"] == "FAILED"])
    passed_count = sum([1 for r in results if r["Status"] == "PASSED"])
    print(f"VALIDATION SUMMARY: {passed_count} PASSED | {failed_count} FAILED")
    print("=" * 80)
    
    return failed_count == 0

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
    success = run_validation(target)
    sys.exit(0 if success else 1)
