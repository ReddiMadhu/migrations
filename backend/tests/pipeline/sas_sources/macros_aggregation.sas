/*============================================================================
 * macros_aggregation.sas
 * ---------------------------------------------------------------------------
 * Production SAS Aggregation & KPI Macro Library.
 * Contains reusable macros for:
 *   1. %aggregate_sales    – multi-level rollup with sum, mean, count & 2-dec rounding
 *   2. %compute_kpis       – gross margin %, value tiers, and profitability classification
 *   3. %rank_products      – dense ranking of products within regional partitions
 *
 * Usage:  %include "macros_aggregation.sas";
 *============================================================================*/

/* ---- %aggregate_sales --------------------------------------------------
 * Aggregates metric_var grouped by group_vars (e.g. customer_id, region).
 * Applies 2-decimal rounding to sum and mean for clean financial reporting.
 * ------------------------------------------------------------------------- */
%macro aggregate_sales(ds_in=, ds_out=, group_vars=, metric_var=);
    proc sql;
        create table &ds_out as
        select
            &group_vars,
            round(sum(&metric_var), 0.01)  as total_amount format=12.2,
            round(mean(&metric_var), 0.01) as avg_amount   format=12.2,
            count(*)                       as txn_count
        from &ds_in
        group by &group_vars
        order by &group_vars;
    quit;
%mend aggregate_sales;


/* ---- %compute_kpis -----------------------------------------------------
 * Derives business KPIs on transactional line items:
 *   - margin_pct  : (revenue - cost) / revenue * 100 (rounded to 2 decimals)
 *   - value_tier  : PREMIUM (>= $1000), STANDARD (>= $500), ECONOMY (< $500)
 *   - profit_flag : HIGH_MARGIN (>= 30%), MID_MARGIN (>= 10%), LOW_MARGIN (>= 0%), LOSS (< 0%)
 * ------------------------------------------------------------------------- */
%macro compute_kpis(ds_in=, ds_out=, revenue_var=, cost_var=);
    data &ds_out;
        set &ds_in;

        /* Gross margin percentage */
        if &revenue_var > 0 then
            margin_pct = round((&revenue_var - &cost_var) / &revenue_var * 100, 0.01);
        else
            margin_pct = 0.00;

        /* Value tier based on transaction revenue */
        length value_tier $10;
        if &revenue_var >= 1000 then value_tier = 'PREMIUM';
        else if &revenue_var >= 500 then value_tier = 'STANDARD';
        else value_tier = 'ECONOMY';

        /* Profitability classification based on margin percentage */
        length profit_flag $15;
        if margin_pct >= 30 then profit_flag = 'HIGH_MARGIN';
        else if margin_pct >= 10 then profit_flag = 'MID_MARGIN';
        else if margin_pct >= 0  then profit_flag = 'LOW_MARGIN';
        else profit_flag = 'LOSS';

        format margin_pct 8.2;
    run;
%mend compute_kpis;


/* ---- %rank_products ----------------------------------------------------
 * Dense-ranks products within each partition (e.g. region) by order_var descending.
 * Uses native SAS PROC RANK with TIES=DENSE for universal compatibility and high performance.
 * ------------------------------------------------------------------------- */
%macro rank_products(ds_in=, ds_out=, partition_var=, order_var=);
    /* Step 1: Pre-sort data by partition and metric descending */
    proc sort data=&ds_in out=_to_rank_;
        by &partition_var descending &order_var;
    run;

    /* Step 2: Dense rank within partition using native PROC RANK */
    proc rank data=_to_rank_ out=&ds_out descending ties=dense;
        by &partition_var;
        var &order_var;
        ranks product_rank;
    run;

    /* Step 3: Ensure deterministic final sort order */
    proc sort data=&ds_out;
        by &partition_var product_rank descending &order_var;
    run;

    /* Clean up temporary work table */
    proc datasets lib=work nolist;
        delete _to_rank_;
    quit;
%mend rank_products;
