"""Mutant: forgets to drop NULL emails (no validation)."""


def clean_customers(df):
    return (
        df.dropDuplicates(["company_id", "customer_id"])
          .select("company_id", "customer_id", "email")  # NULL-email rows survive
    )
