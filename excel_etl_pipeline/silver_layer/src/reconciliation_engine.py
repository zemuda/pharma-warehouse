# src/reconciliation_engine.py
"""
Invoice Reconciliation Engine
Reconciles headers with details and credit notes with invoices
"""
import polars as pl
from typing import Dict, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ReconciliationEngine:
    """Reconcile invoices and credit notes"""
    
    def __init__(self):
        self.reconciliation_results = []
    
    def reconcile_invoice_header_detail(
        self,
        headers: pl.DataFrame,
        details: pl.DataFrame
    ) -> Tuple[pl.DataFrame, Dict]:
        """
        Reconcile invoice headers with line item details
        
        Returns:
            Tuple of (reconciliation_df, summary_dict)
        """
        
        logger.info("🔄 Starting invoice header-detail reconciliation...")
        
        # Aggregate details by invoice
        detail_totals = details.group_by('invoice_number').agg([
            pl.col('line_total_amount').sum().alias('detail_total'),
            pl.col('line_discount_amount').sum().alias('detail_discount'),
            pl.col('line_tax_amount').sum().alias('detail_tax'),
            pl.count().alias('line_count')
        ])
        
        # Merge with headers
        reconciliation = headers.join(
            detail_totals,
            on='invoice_number',
            how='outer'
        )
        
        # Calculate variances
        reconciliation = reconciliation.with_columns([
            (pl.col('total_amount') - pl.col('detail_total')).abs().alias('amount_variance'),
        ])
        
        reconciliation = reconciliation.with_columns([
            (pl.col('amount_variance') > 0.01).alias('has_variance')
        ])
        
        # Identify issues
        headers_without_details = reconciliation.filter(pl.col('detail_total').is_null()).height
        details_without_headers = reconciliation.filter(pl.col('total_amount').is_null()).height
        amount_mismatches = reconciliation.filter(pl.col('has_variance') == True).height
        total_variance = reconciliation.select(pl.col('amount_variance').sum()).item()
        
        # Calculate reconciliation rate
        total_invoices = reconciliation.height
        reconciled = total_invoices - headers_without_details - details_without_headers
        reconciliation_rate = (reconciled / total_invoices * 100) if total_invoices > 0 else 0
        
        summary = {
            'total_invoices': total_invoices,
            'headers_without_details': headers_without_details,
            'details_without_headers': details_without_headers,
            'amount_mismatches': amount_mismatches,
            'total_variance': float(total_variance) if total_variance else 0,
            'reconciliation_rate': round(reconciliation_rate, 2),
            'status': 'COMPLETED',
            'timestamp': datetime.now().isoformat()
        }
        
        # Log results
        if headers_without_details > 0:
            logger.error(f"❌ {headers_without_details} invoices have NO line items!")
        
        if details_without_headers > 0:
            logger.error(f"❌ {details_without_headers} line items reference MISSING invoices!")
        
        if amount_mismatches > 0:
            logger.warning(f"⚠️  {amount_mismatches} invoices have amount mismatches")
        
        logger.info(f"✅ Reconciliation rate: {reconciliation_rate:.2f}%")
        
        return reconciliation, summary
    
    def reconcile_credit_notes_with_invoices(
        self,
        credit_notes: pl.DataFrame,
        invoices: pl.DataFrame
    ) -> Tuple[pl.DataFrame, Dict]:
        """
        Reconcile credit notes with original invoices
        
        Returns:
            Tuple of (reconciliation_df, summary_dict)
        """
        
        logger.info("🔗 Starting credit note-invoice reconciliation...")
        
        # Join credit notes with invoices
        reconciliation = credit_notes.join(
            invoices.select([
                pl.col('invoice_number'),
                pl.col('total_amount').alias('invoice_amount'),
                pl.col('customer_code').alias('invoice_customer'),
                pl.col('transaction_date').alias('invoice_date')
            ]),
            left_on='original_invoice_number',
            right_on='invoice_number',
            how='left'
        )
        
        # Calculate credit metrics
        reconciliation = reconciliation.with_columns([
            ((pl.col('credit_amount').abs() / pl.col('invoice_amount')) * 100)
            .alias('refund_percentage')
        ])
        
        reconciliation = reconciliation.with_columns([
            (pl.col('refund_percentage') >= 99).alias('is_full_credit'),
            (pl.col('refund_percentage').is_between(1, 98)).alias('is_partial_credit')
        ])
        
        # Calculate days since invoice
        if 'transaction_date' in reconciliation.columns and 'invoice_date' in reconciliation.columns:
            reconciliation = reconciliation.with_columns([
                (pl.col('transaction_date') - pl.col('invoice_date')).dt.days()
                .alias('days_since_invoice')
            ])
        
        # Identify issues
        credits_without_invoices = reconciliation.filter(pl.col('invoice_amount').is_null()).height
        credits_exceeding_invoice = reconciliation.filter(
            pl.col('credit_amount').abs() > pl.col('invoice_amount')
        ).height
        late_credits = reconciliation.filter(
            pl.col('days_since_invoice') > 30
        ).height if 'days_since_invoice' in reconciliation.columns else 0
        
        # Calculate net revenue impact
        gross_invoice_amount = invoices.select(pl.col('total_amount').sum()).item()
        total_credit_amount = credit_notes.select(pl.col('credit_amount').sum()).item()
        net_revenue = gross_invoice_amount + total_credit_amount  # credit_amount is negative
        credit_rate = (abs(total_credit_amount) / gross_invoice_amount * 100) if gross_invoice_amount > 0 else 0
        
        summary = {
            'total_credit_notes': reconciliation.height,
            'credits_without_invoices': credits_without_invoices,
            'credits_exceeding_invoice': credits_exceeding_invoice,
            'late_credits': late_credits,
            'gross_invoice_amount': float(gross_invoice_amount) if gross_invoice_amount else 0,
            'total_credit_amount': float(total_credit_amount) if total_credit_amount else 0,
            'net_revenue': float(net_revenue) if net_revenue else 0,
            'credit_rate_percent': round(credit_rate, 2),
            'status': 'COMPLETED',
            'timestamp': datetime.now().isoformat()
        }
        
        # Log results
        if credits_without_invoices > 0:
            logger.error(f"❌ {credits_without_invoices} credit notes reference NON-EXISTENT invoices!")
        
        if credits_exceeding_invoice > 0:
            logger.warning(f"⚠️  {credits_exceeding_invoice} credit notes EXCEED original invoice amount")
        
        if late_credits > 0:
            logger.warning(f"⚠️  {late_credits} credit notes issued >30 days after invoice")
        
        logger.info(f"💰 Net revenue after credits: ${net_revenue:,.2f} (credit rate: {credit_rate:.2f}%)")
        
        return reconciliation, summary
    
    def generate_reconciliation_report(
        self,
        reconciliation_df: pl.DataFrame,
        report_type: str
    ) -> pl.DataFrame:
        """Generate reconciliation report for PostgreSQL"""
        
        if report_type == 'INVOICE':
            report = reconciliation_df.select([
                pl.lit(report_type).alias('report_type'),
                pl.col('invoice_number'),
                pl.col('total_amount').is_not_null().alias('has_header'),
                pl.col('detail_total').is_not_null().alias('has_details'),
                pl.col('total_amount').alias('header_amount'),
                pl.col('detail_total'),
                pl.col('amount_variance').alias('variance'),
                pl.col('has_variance'),
                pl.when(pl.col('has_variance'))
                .then(pl.lit('VARIANCE'))
                .when(pl.col('total_amount').is_null())
                .then(pl.lit('MISSING_HEADER'))
                .when(pl.col('detail_total').is_null())
                .then(pl.lit('MISSING_DETAILS'))
                .otherwise(pl.lit('RECONCILED'))
                .alias('reconciliation_status')
            ])
        
        elif report_type == 'CREDIT_NOTE':
            report = reconciliation_df.select([
                pl.lit(report_type).alias('report_type'),
                pl.col('credit_note_number').alias('invoice_number'),
                pl.lit(True).alias('has_header'),
                pl.col('invoice_amount').is_not_null().alias('has_details'),
                pl.col('credit_amount').alias('header_amount'),
                pl.col('invoice_amount').alias('detail_total'),
                (pl.col('credit_amount').abs() - pl.col('invoice_amount')).abs().alias('variance'),
                (pl.col('credit_amount').abs() > pl.col('invoice_amount')).alias('has_variance'),
                pl.when(pl.col('invoice_amount').is_null())
                .then(pl.lit('MISSING_INVOICE'))
                .when(pl.col('credit_amount').abs() > pl.col('invoice_amount'))
                .then(pl.lit('EXCEEDS_INVOICE'))
                .otherwise(pl.lit('RECONCILED'))
                .alias('reconciliation_status')
                ])
        else:
            raise ValueError(f"Unknown report type: {report_type}")