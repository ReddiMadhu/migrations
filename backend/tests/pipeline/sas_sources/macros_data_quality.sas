/*============================================================================
 * macros_data_quality.sas
 * ---------------------------------------------------------------------------
 * Production SAS Data-Quality Macro Library.
 * Contains reusable macros for data cleansing and validation:
 *   1. %standardize_name   – trims blanks, collapses spaces, converts to title-case
 *   2. %validate_dates     – validates dates against reference threshold
 *   3. %flag_outliers      – IQR-based statistical outlier detection
 *
 * Usage:  %include "macros_data_quality.sas";
 *============================================================================*/

/* ---- %standardize_name -------------------------------------------------
 * Cleans string names: strips leading/trailing spaces, collapses multiple
 * embedded spaces to single space via COMPBL, and applies PROPCASE title-casing.
 * ------------------------------------------------------------------------- */
%macro standardize_name(ds_in=, ds_out=, name_var=);
    data &ds_out;
        set &ds_in;
        /* Trim leading/trailing blanks and collapse internal spaces */
        &name_var = compbl(strip(&name_var));
        /* Convert to proper Title Case */
        &name_var = propcase(&name_var);
    run;
%mend standardize_name;


/* ---- %validate_dates ---------------------------------------------------
 * Validates transaction dates against business rules:
 *   - MISSING  : Date is null/missing
 *   - FUTURE   : Date is beyond the reference threshold date
 *   - TOO_OLD  : Date is prior to Jan 1, 1900
 *   - VALID    : Date passes all integrity rules
 * ------------------------------------------------------------------------- */
%macro validate_dates(ds_in=, ds_out=, date_var=, ref_date=);
    %if %superq(ref_date) = %then %let ref_date = %sysfunc(today());
    data &ds_out;
        set &ds_in;
        length date_flag $20;
        format &date_var yymmdd10.;

        if missing(&date_var) then date_flag = 'MISSING';
        else if &date_var > &ref_date then date_flag = 'FUTURE';
        else if &date_var < '01JAN1900'd then date_flag = 'TOO_OLD';
        else date_flag = 'VALID';
    run;
%mend validate_dates;


/* ---- %flag_outliers ----------------------------------------------------
 * Computes Q1, Q3 and Interquartile Range (IQR) for a specified metric
 * using PROC MEANS, then tags rows:
 *   - LOW    : metric < Q1 - (multiplier * IQR)
 *   - HIGH   : metric > Q3 + (multiplier * IQR)
 *   - NORMAL : metric within bounds
 * ------------------------------------------------------------------------- */
%macro flag_outliers(ds_in=, ds_out=, metric_var=, multiplier=1.5);
    /* Step 1: Compute Q1 and Q3 quartiles */
    proc means data=&ds_in noprint;
        var &metric_var;
        output out=_iqr_stats_ q1= q3= /autoname;
    run;

    /* Step 2: Merge stats and flag outliers */
    data &ds_out;
        if _n_ = 1 then set _iqr_stats_(keep=&metric_var._Q1 &metric_var._Q3);
        set &ds_in;

        _iqr = &metric_var._Q3 - &metric_var._Q1;
        _lower = &metric_var._Q1 - (&multiplier * _iqr);
        _upper = &metric_var._Q3 + (&multiplier * _iqr);

        length outlier_flag $10;
        if &metric_var < _lower then outlier_flag = 'LOW';
        else if &metric_var > _upper then outlier_flag = 'HIGH';
        else outlier_flag = 'NORMAL';

        drop _iqr _lower _upper &metric_var._Q1 &metric_var._Q3;
    run;

    /* Clean up temporary work table */
    proc datasets lib=work nolist;
        delete _iqr_stats_;
    quit;
%mend flag_outliers;
