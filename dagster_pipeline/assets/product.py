from __future__ import annotations

from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from dagster_pipeline.resources import PostgresResource


@asset(
    group_name="portfolio",
    deps=[AssetKey("mart_portfolio_ranker_weekly")],
)
def portfolio_ranker_top_20_log(
    context,
    postgres: PostgresResource,
) -> MaterializeResult:
    """Log the current top weekly portfolio ranks after dbt refreshes."""
    client = postgres.get_client()
    rows = client.query(
        """
        SELECT
            ticker,
            market,
            total_score,
            overall_rank
        FROM mart_portfolio_ranker_weekly
        ORDER BY week_start DESC, overall_rank ASC
        LIMIT 20
        """
    ).result_rows

    top_20 = [
        {
            "ticker": ticker,
            "market": market,
            "total_score": float(total_score),
            "overall_rank": int(overall_rank),
        }
        for ticker, market, total_score, overall_rank in rows
    ]
    context.log.info("Current weekly top 20 portfolio ranks: %s", top_20)

    return MaterializeResult(
        metadata={
            "top_20_count": len(top_20),
            "top_20": MetadataValue.json(top_20),
        }
    )
