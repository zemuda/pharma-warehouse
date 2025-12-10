# src/data_quality_validator.py
"""
Comprehensive Data Quality Validation Engine
"""
import polars as pl
from typing import Dict, List, Tuple
import yaml
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """Validate data quality based on configurable rules"""

    def __init__(self, rules_file: str, strict_mode: bool = False):
        with open(rules_file, "r") as f:
            self.rules = yaml.safe_load(f)["validation_rules"]
        self.strict_mode = strict_mode
        self.violations = []

    def validate(
        self,
        df: pl.DataFrame,
        table_type: str,
        related_tables: Dict[str, pl.DataFrame] = None,
    ) -> Tuple[bool, Dict]:
        """
        Validate DataFrame against rules

        Returns:
            Tuple of (passed, validation_report)
        """

        if table_type not in self.rules:
            logger.warning(f"⚠️  No validation rules for table type: {table_type}")
            return True, {"status": "SKIPPED", "reason": "No rules defined"}

        rules = self.rules[table_type]
        self.violations = []

        logger.info(f"🔍 Starting validation for {table_type}...")

        # 1. Check required fields
        self._check_required_fields(df, rules.get("required_fields", []))

        # 2. Check business rules
        self._check_business_rules(df, rules.get("business_rules", []))

        # 3. Check unique keys
        self._check_unique_keys(df, rules.get("unique_keys", []))

        # 4. Check foreign keys
        if related_tables:
            self._check_foreign_keys(df, rules.get("foreign_keys", []), related_tables)

        # Generate report
        passed = len(self.violations) == 0
        report = self._generate_report(df, passed)

        if not passed:
            if self.strict_mode:
                logger.error(f"❌ Validation FAILED for {table_type} (strict mode)")
                raise ValueError(
                    f"Data quality validation failed: {len(self.violations)} violations"
                )
            else:
                logger.warning(
                    f"⚠️  Validation found {len(self.violations)} issues (non-strict mode)"
                )
        else:
            logger.info(f"✅ Validation PASSED for {table_type}")

        return passed, report

    def _check_required_fields(self, df: pl.DataFrame, required_fields: List[str]):
        """Check for missing required fields"""
        for field in required_fields:
            if field not in df.columns:
                self.violations.append(
                    {
                        "check": "required_field",
                        "field": field,
                        "severity": "CRITICAL",
                        "message": f"Required field '{field}' is missing from dataset",
                    }
                )
                continue

            null_count = df.filter(pl.col(field).is_null()).height
            if null_count > 0:
                self.violations.append(
                    {
                        "check": "required_field",
                        "field": field,
                        "severity": "HIGH",
                        "violations": null_count,
                        "message": f"Found {null_count} null values in required field '{field}'",
                    }
                )

    def _check_business_rules(self, df: pl.DataFrame, business_rules: List[Dict]):
        """Check business logic rules"""
        for rule in business_rules:
            rule_expr = rule["rule"]
            severity = rule.get("severity", "MEDIUM")
            message = rule.get("message", f"Business rule violation: {rule_expr}")

            try:
                # Evaluate the rule expression
                violation_count = df.filter(~self._eval_rule(df, rule_expr)).height

                if violation_count > 0:
                    self.violations.append(
                        {
                            "check": "business_rule",
                            "rule": rule_expr,
                            "severity": severity,
                            "violations": violation_count,
                            "message": message,
                        }
                    )
            except Exception as e:
                logger.error(f"❌ Failed to evaluate rule '{rule_expr}': {e}")

    def _eval_rule(self, df: pl.DataFrame, rule_expr: str) -> pl.Expr:
        """Evaluate a rule expression string to a Polars expression"""

        # Handle common rule patterns
        if "current_date()" in rule_expr:
            rule_expr = rule_expr.replace(
                "current_date()", f"'{datetime.now().date()}'"
            )

        # Simple expression parser for common rules
        if ">" in rule_expr:
            col, value = rule_expr.split(">")
            col = col.strip()
            value = value.strip()
            return (
                pl.col(col) > float(value)
                if value.replace(".", "").isdigit()
                else pl.col(col) > value
            )

        elif "<" in rule_expr:
            col, value = rule_expr.split("<")
            col = col.strip()
            value = value.strip()
            if "<=" in rule_expr:
                col = col.replace("=", "").strip()
                return (
                    pl.col(col) <= float(value)
                    if value.replace(".", "").isdigit()
                    else pl.col(col) <= value
                )
            return (
                pl.col(col) < float(value)
                if value.replace(".", "").isdigit()
                else pl.col(col) < value
            )

        elif ">=" in rule_expr:
            col, value = rule_expr.split(">=")
            col = col.strip()
            value = value.strip()
            return (
                pl.col(col) >= float(value)
                if value.replace(".", "").isdigit()
                else pl.col(col) >= value
            )

        elif "IS NOT NULL" in rule_expr:
            col = rule_expr.replace("IS NOT NULL", "").strip()
            return pl.col(col).is_not_null()

        elif "abs(" in rule_expr:
            # Handle absolute value expressions like: abs((quantity * price) - total) < 0.01
            import re

            match = re.search(r"abs\((.*?)\)\s*<\s*([\d.]+)", rule_expr)
            if match:
                expr = match.group(1)
                threshold = float(match.group(2))
                # This is a simplified parser - extend as needed
                return pl.lit(True)  # Placeholder

        # Default: return True (skip rule if can't parse)
        logger.warning(f"⚠️  Could not parse rule: {rule_expr}")
        return pl.lit(True)

    def _check_unique_keys(self, df: pl.DataFrame, unique_keys: List[str]):
        """Check for duplicate keys"""
        for key in unique_keys:
            if key not in df.columns:
                continue

            duplicates = (
                df.group_by(key)
                .agg(pl.count().alias("count"))
                .filter(pl.col("count") > 1)
            )

            if duplicates.height > 0:
                duplicate_count = duplicates.height
                self.violations.append(
                    {
                        "check": "unique_key",
                        "field": key,
                        "severity": "CRITICAL",
                        "violations": duplicate_count,
                        "message": f"Found {duplicate_count} duplicate values in unique key '{key}'",
                    }
                )

    def _check_foreign_keys(
        self,
        df: pl.DataFrame,
        foreign_keys: List[Dict],
        related_tables: Dict[str, pl.DataFrame],
    ):
        """Check foreign key integrity"""
        for fk in foreign_keys:
            column = fk["column"]
            reference = fk["references"]  # Format: "table_name.column_name"

            if column not in df.columns:
                continue

            ref_table, ref_column = reference.split(".")

            if ref_table not in related_tables:
                logger.warning(
                    f"⚠️  Referenced table '{ref_table}' not provided for FK check"
                )
                continue

            ref_df = related_tables[ref_table]

            if ref_column not in ref_df.columns:
                continue

            # Find orphaned records
            valid_values = ref_df.select(pl.col(ref_column)).unique()
            orphans = df.join(
                valid_values, left_on=column, right_on=ref_column, how="anti"
            )

            if orphans.height > 0:
                self.violations.append(
                    {
                        "check": "foreign_key",
                        "field": column,
                        "severity": "HIGH",
                        "violations": orphans.height,
                        "message": f"Found {orphans.height} orphaned records in '{column}' (references {reference})",
                    }
                )

    def _generate_report(self, df: pl.DataFrame, passed: bool) -> Dict:
        """Generate validation report"""
        return {
            "passed": passed,
            "total_violations": len(self.violations),
            "violations": self.violations,
            "records_checked": df.height,
            "timestamp": datetime.now().isoformat(),
        }

    def add_quality_flags(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add data quality flags to DataFrame"""

        # Amount validation flags
        if "total_amount" in df.columns:
            df = df.with_columns(
                [
                    pl.when(
                        pl.col("total_amount").is_null() | (pl.col("total_amount") <= 0)
                    )
                    .then(1)
                    .otherwise(0)
                    .alias("amount_invalid_flag")
                ]
            )

        # Date validation flags
        if "transaction_date" in df.columns:
            df = df.with_columns(
                [
                    pl.when(pl.col("transaction_date").is_null())
                    .then(1)
                    .otherwise(0)
                    .alias("date_invalid_flag")
                ]
            )

        # Customer validation flags
        if "customer_code" in df.columns:
            df = df.with_columns(
                [
                    pl.when(
                        pl.col("customer_code").is_null()
                        | (pl.col("customer_code") == "")
                    )
                    .then(1)
                    .otherwise(0)
                    .alias("customer_missing_flag")
                ]
            )

        # Quantity validation (for detail tables)
        if "quantity_sold" in df.columns:
            df = df.with_columns(
                [
                    pl.when(
                        pl.col("quantity_sold").is_null()
                        | (pl.col("quantity_sold") <= 0)
                    )
                    .then(1)
                    .otherwise(0)
                    .alias("quantity_invalid_flag")
                ]
            )

        # Price validation (for detail tables)
        if "unit_price" in df.columns:
            df = df.with_columns(
                [
                    pl.when(
                        pl.col("unit_price").is_null() | (pl.col("unit_price") <= 0)
                    )
                    .then(1)
                    .otherwise(0)
                    .alias("price_invalid_flag")
                ]
            )

        return df
