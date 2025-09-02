select *
from "crypto_warehouse"."public_staging"."stg_crypto_prices"

where not (extracted_at <= CURRENT_TIMESTAMP)
