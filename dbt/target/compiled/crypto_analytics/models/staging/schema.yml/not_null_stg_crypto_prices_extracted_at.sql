select *
from "crypto_warehouse"."public_staging"."stg_crypto_prices"
where extracted_at is null
