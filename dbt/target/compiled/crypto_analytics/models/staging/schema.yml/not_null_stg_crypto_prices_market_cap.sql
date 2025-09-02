select *
from "crypto_warehouse"."public_staging"."stg_crypto_prices"
where market_cap is null
