select *
from "crypto_warehouse"."public_staging"."stg_crypto_prices"
where price_usd is null
