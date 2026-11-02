{% macro calculate_notional(price_col, quantity_col) %}
ROUND(CAST({{ price_col }} AS NUMERIC) * CAST({{ quantity_col }} AS NUMERIC), 4)
{% endmacro %}

{% macro relative_spread_bps(bid_col, ask_col) %}
ROUND(((CAST({{ ask_col }} AS NUMERIC) - CAST({{ bid_col }} AS NUMERIC)) / NULLIF((CAST({{ bid_col }} AS NUMERIC) + CAST({{ ask_col }} AS NUMERIC)) / 2.0, 0)) * 10000, 2)
{% endmacro %}

{% macro classify_notional_tier(notional_col) %}
CASE
    WHEN {{ notional_col }} >= 500000 THEN 'BLOCK'
    WHEN {{ notional_col }} >= 100000 THEN 'LARGE'
    WHEN {{ notional_col }} >= 10000 THEN 'STANDARD'
    ELSE 'MICRO'
END
{% endmacro %}

{% macro classify_limit_breach(utilization_col) %}
CASE
    WHEN {{ utilization_col }} >= 100.0 THEN 'CRITICAL'
    WHEN {{ utilization_col }} >= 85.0 THEN 'WARNING'
    ELSE 'NORMAL'
END
{% endmacro %}
