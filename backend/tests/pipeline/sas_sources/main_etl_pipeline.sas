/*============================================================================
 * main_etl_pipeline.sas
 * ---------------------------------------------------------------------------
 * Production SAS ETL Pipeline — Main Entry Point.
 *
 * INPUTS  (3 CSV files):
 *   1. transactions.csv       – raw sales transaction records
 *   2. customers.csv          – customer master profile data
 *   3. products.csv           – product catalog & cost baseline
 *
 * MACRO LIBRARIES (2 files):
 *   1. macros_data_quality.sas – standardize_name, validate_dates, flag_outliers
 *   2. macros_aggregation.sas  – aggregate_sales, compute_kpis, rank_products
 *
 * OUTPUTS (3 CSV files):
 *   1. enriched_transactions  – cleaned, KPI-enriched, outlier-flagged transactions
 *   2. customer_summary       – aggregated revenue & volume by customer and region
 *   3. product_performance    – ranked product revenue within each region
 *
 * EXECUTION FLOW:
 *   S1. Import raw CSV inputs (transactions, customers, products)
 *   S2. Clean & standardize customer names (%standardize_name)
 *   S3. Validate transaction dates against reference horizon (%validate_dates)
 *   S4. Relational inner join (transactions <-> customers <-> products)
 *   S5. Derive margin & profitability KPIs (%compute_kpis)
 *   S6. Aggregate financials by customer & region (%aggregate_sales)
 *   S7. Dense-rank products within regional partitions (%rank_products)
 *   S8. Tag statistical outliers using IQR fencing (%flag_outliers)
 *   S9. Export final production CSV outputs
 *============================================================================*/

options mprint mlogic symbolgen;

/* =========================================================================
   Directory & Environment Configuration
   Adjust these paths as needed for your SAS execution environment.
   Defaults assume running from the sas_sources directory.
   ========================================================================= */
%let script_dir  = .;
%let input_dir   = ../input;
%let output_dir  = ../actual_output;

/* Allow external environment overrides if macro variables are pre-defined */
%macro _init_env;
    %if %symexist(SAS_SCRIPT_DIR) %then %let script_dir = &SAS_SCRIPT_DIR;
    %if %symexist(SAS_INPUT_DIR)  %then %let input_dir  = &SAS_INPUT_DIR;
    %if %symexist(SAS_OUTPUT_DIR) %then %let output_dir = &SAS_OUTPUT_DIR;
%mend _init_env;
%_init_env;

/* ---------- Include Macro Libraries -------------------------------------- */
%include "&script_dir/macros_data_quality.sas";
%include "&script_dir/macros_aggregation.sas";

/* Reference threshold date for validation (default: current system date) */
%let ref_date = %sysfunc(today());


/* =========================================================================
   S1. Import Raw CSV Inputs
   ========================================================================= */
proc import datafile="&input_dir/transactions.csv"
    out=raw_transactions
    dbms=csv
    replace;
    getnames=yes;
    guessingrows=max;
run;

proc import datafile="&input_dir/customers.csv"
    out=raw_customers
    dbms=csv
    replace;
    getnames=yes;
    guessingrows=max;
run;

proc import datafile="&input_dir/products.csv"
    out=raw_products
    dbms=csv
    replace;
    getnames=yes;
    guessingrows=max;
run;


/* =========================================================================
   S2. Standardize Customer Names
   ========================================================================= */
%standardize_name(
    ds_in=raw_customers,
    ds_out=clean_customers,
    name_var=customer_name
);


/* =========================================================================
   S3. Validate Transaction Dates
   ========================================================================= */
%validate_dates(
    ds_in=raw_transactions,
    ds_out=validated_transactions,
    date_var=transaction_date,
    ref_date=&ref_date
);


/* =========================================================================
   S4. Relational Join (Transactions <-> Customers <-> Products)
   ========================================================================= */
proc sql;
    create table joined_data as
    select
        t.transaction_id,
        t.transaction_date,
        t.date_flag,
        t.customer_id,
        c.customer_name,
        c.region,
        c.segment,
        t.product_id,
        p.product_name,
        p.category,
        p.unit_cost                              format=10.2,
        t.quantity,
        t.unit_price                             format=10.2,
        round(t.quantity * t.unit_price, 0.01)   as order_total format=12.2,
        round(t.quantity * p.unit_cost,  0.01)   as total_cost  format=12.2
    from validated_transactions as t
    inner join clean_customers   as c on t.customer_id = c.customer_id
    inner join raw_products      as p on t.product_id  = p.product_id
    order by t.transaction_id;
quit;


/* =========================================================================
   S5. Derive Line-Item KPIs
   ========================================================================= */
%compute_kpis(
    ds_in=joined_data,
    ds_out=kpi_transactions,
    revenue_var=order_total,
    cost_var=total_cost
);


/* =========================================================================
   S6. Aggregate Financials by Customer & Region
   ========================================================================= */
%aggregate_sales(
    ds_in=kpi_transactions,
    ds_out=customer_summary,
    group_vars=%str(customer_id, region),
    metric_var=order_total
);


/* =========================================================================
   S7. Dense-Rank Products Within Regional Partitions
   ========================================================================= */
%rank_products(
    ds_in=kpi_transactions,
    ds_out=product_performance,
    partition_var=region,
    order_var=order_total
);


/* =========================================================================
   S8. Tag Statistical Outliers on Order Total
   ========================================================================= */
%flag_outliers(
    ds_in=kpi_transactions,
    ds_out=enriched_transactions,
    metric_var=order_total,
    multiplier=1.5
);


/* =========================================================================
   S9. Export Final Production CSV Outputs
   ========================================================================= */
proc export data=enriched_transactions
    outfile="&output_dir/enriched_transactions.csv"
    dbms=csv
    replace;
run;

proc export data=customer_summary
    outfile="&output_dir/customer_summary.csv"
    dbms=csv
    replace;
run;

proc export data=product_performance
    outfile="&output_dir/product_performance.csv"
    dbms=csv
    replace;
run;


/* =========================================================================
   Audit & Quality Control Summary
   ========================================================================= */
%put NOTE: ================================================================ ;
%put NOTE: SAS ETL PIPELINE EXECUTION COMPLETED SUCCESSFULLY ;
%put NOTE: Timestamp: %sysfunc(datetime(), datetime20.) ;
%put NOTE: Output 1 (enriched_transactions): %sysfunc(attrn(open(enriched_transactions), nobs)) rows ;
%put NOTE: Output 2 (customer_summary):       %sysfunc(attrn(open(customer_summary), nobs)) rows ;
%put NOTE: Output 3 (product_performance):    %sysfunc(attrn(open(product_performance), nobs)) rows ;
%put NOTE: ================================================================ ;
