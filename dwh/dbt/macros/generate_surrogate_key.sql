{% macro generate_surrogate_key(field_list) %}
{# Generates a deterministic surrogate key across BigQuery and DuckDB SQL dialects #}
MD5(CONCAT(
    {% for field in field_list %}
        COALESCE(CAST({{ field }} AS STRING), '_null_')
        {% if not loop.last %}, '|', {% endif %}
    {% endfor %}
))
{% endmacro %}
