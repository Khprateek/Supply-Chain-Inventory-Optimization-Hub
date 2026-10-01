# Stateful Inventory Snapshot Generator
import numpy as np
import pandas as pd
from datetime import date, timedelta
from python.config import ScaleConfig
from python.common import date_to_key

def generate_inventory(
    config: ScaleConfig,
    dimensions: dict,
    df_sales: pd.DataFrame,
    df_po: pd.DataFrame,
    active_pairs: list,
    rng: np.random.Generator,
    df_movements: pd.DataFrame = None,
    **kwargs
):
    print("--- Generating Stateful Inventory Snapshots (FactInventorySnapshot) ---")
    
    start_snapshot_key = kwargs.get("start_snapshot_key", 1)
    prior_state = kwargs.get("prior_state", {})

    dim_product = dimensions["DimProduct"]
    curr_prods = dim_product[dim_product["IsCurrent"]].copy()
    sku_to_pk = dict(zip(curr_prods["ProductSKU"], curr_prods["ProductKey"]))
    sku_to_cost = dict(zip(curr_prods["ProductSKU"], curr_prods["UnitStandardCost"]))
    
    # Pre-aggregate outbound sales demand by (Date, SKU, Warehouse)
    sales_agg = df_sales.groupby(["OrderDate", "ProductSKU", "WarehouseKey"])["ShippedQuantity"].sum().to_dict()
    
    # Pre-aggregate inbound PO receipts by (DockDate, SKU, Warehouse)
    po_completed = df_po[df_po["POStatus"] == "Completed"].copy()
    po_agg = po_completed.groupby(["ActualDockReceiptDate", "ProductSKU", "ReceivingWarehouseKey"])["AcceptedQuantity"].sum().to_dict()
    
    # Pre-aggregate physical inventory movements (Transfers, Damage, Scrap, Cycle Counts)
    movement_agg = {}
    if df_movements is not None and not df_movements.empty:
        valid_types = ["Inter-DC Transfer", "Spoilage Scrap", "Damage Write-off", "Cycle Count Adjustment"]
        valid_moves = df_movements[df_movements["MovementType"].isin(valid_types)].copy()
        
        if not valid_moves.empty:
            # 1. Base origin adjustments
            origin_adj = valid_moves[["MovementDate", "ProductSKU", "OriginWarehouseKey"]].copy()
            origin_adj.columns = ["MovementDate", "ProductSKU", "WarehouseKey"]
            qty = valid_moves["MovementQuantity"].astype(int)
            is_cc = valid_moves["MovementType"] == "Cycle Count Adjustment"
            origin_adj["AdjQty"] = np.where(is_cc, qty, -np.abs(qty))
            
            # 2. Destination adjustments for Transfers
            transfers = valid_moves[
                (valid_moves["MovementType"] == "Inter-DC Transfer") & 
                valid_moves["DestinationWarehouseKey"].notna()
            ].copy()
            
            if not transfers.empty:
                dest_adj = transfers[["MovementDate", "ProductSKU", "DestinationWarehouseKey"]].copy()
                dest_adj.columns = ["MovementDate", "ProductSKU", "WarehouseKey"]
                transit = transfers.get("TransferTransitDays", 1).fillna(1).astype(int)
                dest_adj["MovementDate"] = pd.to_datetime(dest_adj["MovementDate"]) + pd.to_timedelta(transit, unit="D")
                dest_adj["AdjQty"] = np.abs(transfers["MovementQuantity"].astype(int))
                
                all_adjs = pd.concat([origin_adj, dest_adj], ignore_index=True)
            else:
                all_adjs = origin_adj
                
            all_adjs["WarehouseKey"] = all_adjs["WarehouseKey"].astype(int)
            all_adjs["MovementDate"] = pd.to_datetime(all_adjs["MovementDate"])
            
            # Aggregate all movements
            agg = all_adjs.groupby(["MovementDate", "ProductSKU", "WarehouseKey"])["AdjQty"].sum()
            movement_agg = {
                (dt.date(), sku, wh): val 
                for (dt, sku, wh), val in agg.items()
            }
    
    total_days = (config.end_date - config.start_date).days + 1
    dates = [config.start_date + timedelta(days=i) for i in range(total_days)]
    unique_date_keys = [date_to_key(d) for d in dates]
    
    num_pairs = len(active_pairs)
    total_records = num_pairs * total_days
    
    # 1. Base deterministic columns (compute on unique, then broadcast)
    pair_skus = [p[0] for p in active_pairs]
    pair_whs = [p[1] for p in active_pairs]
    pair_pkeys = [sku_to_pk[s] for s in pair_skus]
    pair_costs = [sku_to_cost[s] for s in pair_skus]
    
    skus = np.repeat(pair_skus, total_days)
    whs = np.repeat(pair_whs, total_days)
    p_keys = np.repeat(pair_pkeys, total_days)
    unit_costs = np.repeat(pair_costs, total_days)
    
    date_vals = np.tile(dates, num_pairs)
    snapshot_date_keys = np.tile(unique_date_keys, num_pairs)
    
    snapshot_keys = np.arange(start_snapshot_key, start_snapshot_key + total_records)
    
    # 2. Extract inputs via list comprehension (guaranteed type-safe mapping from dicts)
    inb_arr = np.array([po_agg.get((d, s, w), 0) for d, s, w in zip(date_vals, skus, whs)], dtype=np.int32)
    outb_arr = np.array([sales_agg.get((d, s, w), 0) for d, s, w in zip(date_vals, skus, whs)], dtype=np.int32)
    m_net_arr = np.array([movement_agg.get((d, s, w), 0) for d, s, w in zip(date_vals, skus, whs)], dtype=np.int32)
    
    # Reshape to 2D: (num_pairs, total_days) for vectorized state calculations
    inb_2d = inb_arr.reshape((num_pairs, total_days))
    outb_2d = outb_arr.reshape((num_pairs, total_days))
    m_net_2d = m_net_arr.reshape((num_pairs, total_days))
    
    # 3. Vectorized Stateful Inventory Calculation
    initial_stock = np.zeros((num_pairs, 1), dtype=np.int32)
    initial_stagnant = np.zeros((num_pairs, 1), dtype=np.int32)
    
    for i, (sku, wh_key) in enumerate(active_pairs):
        pair_state = prior_state.get((sku, wh_key))
        if pair_state:
            initial_stock[i, 0] = pair_state["on_hand"]
            initial_stagnant[i, 0] = pair_state["days_stagnant"]
        else:
            initial_stock[i, 0] = int(rng.choice([1500, 3000, 5000, 8000]))
            initial_stagnant[i, 0] = int(rng.integers(0, 15))
            
    # On-Hand Calculation (Lindley's Equation trick for max(0, cumsum))
    net_change_2d = inb_2d - outb_2d + m_net_2d
    net_change_with_init = net_change_2d.copy()
    net_change_with_init[:, 0] += initial_stock[:, 0]
    
    cumsum_2d = np.cumsum(net_change_with_init, axis=1)
    cumsum_with_zero = np.hstack([np.zeros((num_pairs, 1), dtype=np.int32), cumsum_2d])
    min_cumsum = np.minimum.accumulate(cumsum_with_zero, axis=1)
    on_hand_2d = cumsum_2d - min_cumsum[:, 1:]
    
    # Reserved stock calculation
    rand_unif_2d = rng.uniform(0.5, 1.2, size=(num_pairs, total_days))
    reserved_2d = np.round(outb_2d * rand_unif_2d).astype(np.int32)
    reserved_2d = np.minimum(on_hand_2d, reserved_2d)
    avail_2d = on_hand_2d - reserved_2d
    
    # Days Stagnant Calculation (Consecutive counts reset trick)
    is_stagnant_2d = (outb_2d == 0) & (inb_2d == 0) & (m_net_2d == 0)
    is_active_2d = ~is_stagnant_2d
    
    C_stag = np.cumsum(is_stagnant_2d, axis=1)
    reset_vals = np.where(is_active_2d, C_stag, 0)
    max_acc = np.maximum.accumulate(reset_vals, axis=1)
    base_count = C_stag - max_acc
    
    has_reset = np.maximum.accumulate(is_active_2d.astype(int), axis=1) > 0
    days_stagnant_2d = base_count + initial_stagnant * (~has_reset)
    
    # Flatten back to 1D columns for the DataFrame
    curr_on_hand_arr = on_hand_2d.flatten()
    reserved_arr = reserved_2d.flatten()
    avail_arr = avail_2d.flatten()
    days_stagnant_arr = days_stagnant_2d.flatten()
    
    # Update prior_state for next generation block
    final_on_hand = on_hand_2d[:, -1]
    final_stagnant = days_stagnant_2d[:, -1]
    for i, (sku, wh_key) in enumerate(active_pairs):
        prior_state[(sku, wh_key)] = {
            "on_hand": int(final_on_hand[i]),
            "days_stagnant": int(final_stagnant[i])
        }
        
    # Fully vectorized downstream derivations
    is_stockout_arr = np.where(avail_arr <= 0, 1, 0)
    is_dead_arr = np.where(days_stagnant_arr >= 180, 1, 0)
    val_arr = np.round(curr_on_hand_arr * unit_costs, 2)
    in_transit_arr = np.zeros(total_records, dtype=np.int32)
    
    df_snapshot = pd.DataFrame({
        "SnapshotKey": snapshot_keys,
        "SnapshotDateKey": snapshot_date_keys,
        "SnapshotDate": date_vals,
        "ProductKey": p_keys,
        "ProductSKU": skus,
        "WarehouseKey": whs,
        "OnHandQuantity": curr_on_hand_arr,
        "ReservedQuantity": reserved_arr,
        "AvailableQuantity": avail_arr,
        "InTransitInboundQuantity": in_transit_arr,
        "UnitLandedCost": unit_costs,
        "InventoryValuation": val_arr,
        "DaysSinceLastMovement": days_stagnant_arr,
        "IsStockoutFlag": is_stockout_arr,
        "IsDeadStockFlag": is_dead_arr
    })
    
    snapshot_key = start_snapshot_key + total_records
    print(f"Generated {len(df_snapshot):,} inventory snapshot rows.")
    return df_snapshot, snapshot_key, prior_state
