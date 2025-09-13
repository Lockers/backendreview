from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv, find_dotenv  # <-- add

# Always load the nearest .env (project root) regardless of CWD
load_dotenv(find_dotenv(), override=False)   # <-- add


class Settings(BaseSettings):
    # Identity / headers
    bm_auth_token: str
    bm_user_agent: str
    bm_accept_language: str = "en-gb"

    # Mongo
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "pricer"
    mongo_coll_listings: str = "bm_listings"
    mongo_coll_skus_good: str = "bm_skus_good"  # destination for passes
    mongo_coll_skus_bad: str = "bm_skus_bad"  # destination for failures
    mongo_coll_buyback_listings: str = "bm_buyback_listings"
    mongo_coll_buyback_skus_good: str = "bm_buyback_skus_good"
    mongo_coll_buyback_skus_bad: str = "bm_buyback_skus_bad"
    mongo_coll_tradein_groups: str = "bm_tradein_groups"
    mongo_coll_endpoint_rates: str = "bm_endpoint_rates"


    # Environment (prod only)
    bm_base_url: str = "https://www.backmarket.co.uk"

    # Timeouts
    connect_timeout_ms: int = 2000
    read_timeout_ms: int = 60_000
    total_timeout_ms: int = 70_000

    # Rates
    seller_mutations_max_per_window: int = 20  # ~20 / 10s is safe
    rate_window_seconds: int = 10
    global_max_concurrency: int = 8

    # Category: seller_generic
    seller_generic_max_per_window: int = 100

    # Optional proxies (used only for outbound to BM)
    http_proxy: str | None = None
    https_proxy: str | None = None
    no_proxy: str | None = None


    # Optional: jitter limits for adaptive ramp
    rl_penalty_factor: float = 0.5  # halve throughput on 429
    rl_recover_after_ok: int = 10  # after N OKs, ramp back up

    model_config = SettingsConfigDict(
        env_file=None,         # dotenv already loaded above
        env_prefix="",         # "BM_AUTH_TOKEN" maps to bm_auth_token
        case_sensitive=False,
    )

