from src.config import SETTINGS


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors."""
    errors = []

    # Required business key
    if "order_id" not in df.columns:
        errors.append("Missing required column: order_id")
    else:
        if df["order_id"].isna().any():
            errors.append("order_id contains null values")

        if df["order_id"].duplicated().any():
            errors.append("Duplicate order_id values detected")

    # Quantity validation
    if "quantity" not in df.columns:
        errors.append("Missing required column: quantity")
    else:
        min_qty = SETTINGS["quality"]["min_quantity"]
        max_qty = SETTINGS["quality"]["max_quantity"]

        if df["quantity"].isna().any():
            errors.append("quantity contains null values")

        invalid_quantity = (
            df["quantity"].notna()
            & (
                (df["quantity"] < min_qty)
                | (df["quantity"] > max_qty)
            )
        )

        if invalid_quantity.any():
            errors.append(
                f"quantity contains values outside {min_qty}..{max_qty}"
            )

    # Status validation
    if "status" not in df.columns:
        errors.append("Missing required column: status")
    else:
        allowed_statuses = set(
            SETTINGS["quality"]["allowed_order_statuses"]
        )

        invalid_status = ~df["status"].isin(allowed_statuses)

        if invalid_status.any():
            errors.append("status contains invalid values")

    # Amount validation
    amount_columns = [
        "gross_amount",
        "discount_amount",
        "net_amount",
    ]

    for column in amount_columns:
        if column not in df.columns:
            errors.append(f"Missing required column: {column}")
            continue

        if df[column].isna().any():
            errors.append(f"{column} contains null values")

        if (df[column] < 0).any():
            errors.append(f"{column} contains negative values")

    # Required audit columns
    audit_columns = [
        "source_updated_at",
        "pipeline_run_id",
        "processed_at_utc",
        "record_hash",
    ]

    for column in audit_columns:
        if column not in df.columns:
            errors.append(f"Missing required audit column: {column}")
        elif df[column].isna().any():
            errors.append(f"{column} contains null values")

    return errors