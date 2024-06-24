{% macro get_period_boundaries(target_schema, target_table, timestamp_field, start_date, stop_date, period, backfill, full_refresh_mode) -%}
    {{ return(adapter.dispatch('get_period_boundaries', 'insert_by_period')(target_schema, target_table, timestamp_field, start_date, stop_date, period, backfill, full_refresh_mode)) }}
{% endmacro %}

{% macro default__get_period_boundaries(target_schema, target_table, timestamp_field, start_date, stop_date, period, backfill, full_refresh_mode) -%}

  {% call statement('period_boundaries', fetch_result=True) -%}
    with data as (
      select
          {% if backfill and not full_refresh_mode -%}
            cast('{{start_date}}' as timestamp) as start_timestamp,
          {%- else -%}
            coalesce(max({{timestamp_field}}), cast('{{start_date}}' as timestamp)) as start_timestamp,
          {%- endif %}
          coalesce(
            {{ dateadd('millisecond',
                                -1,
                                "cast(nullif('" ~ stop_date ~ "','') as timestamp)") }},
            {{ dbt.current_timestamp() }}
          ) as stop_timestamp
      from {{adapter.quote(target_schema)}}.{{adapter.quote(target_table)}}
    )

    select
      start_timestamp,
      stop_timestamp,
      {{ datediff('start_timestamp',
                           'stop_timestamp',
                           period) }}  + 1 as num_periods
    from data
  {%- endcall %}

{%- endmacro %}



{% macro teradata__get_period_boundaries(target_schema, target_table, timestamp_field, start_date, stop_date, period, backfill, full_refresh_mode) -%}

  {% call statement('period_boundaries', fetch_result=True) -%}
    WITH data AS (
      SELECT
          {% if backfill and not full_refresh_mode -%}
            CAST('{{ start_date }}' AS TIMESTAMP(0)) AS start_timestamp,
          {%- else -%}
            COALESCE(MAX({{ timestamp_field }}), CAST('{{ start_date }}' AS TIMESTAMP(0))) AS start_timestamp,
          {%- endif %}
          COALESCE(
            -- Subtract 1 second from stop_date if it is not null
            CAST(NULLIF('{{ stop_date }}', '') AS TIMESTAMP(0)) - INTERVAL '0.001' SECOND,
            -- Default to current_timestamp if stop_date is null or empty
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(0))
          ) AS stop_timestamp
      FROM {{ adapter.quote(target_schema) }}.{{ adapter.quote(target_table) }}
    )

    SELECT
      start_timestamp,
      stop_timestamp,
      -- Calculate the difference in the specified period units between start and stop timestamps
      CASE '{{ period }}'
        WHEN 'day' THEN (stop_timestamp - start_timestamp ) DAY
        WHEN 'week' THEN (stop_timestamp - start_timestamp ) DAY / 7
        WHEN 'month' THEN (stop_timestamp - start_timestamp ) MONTH
      END + 1 AS num_periods
    FROM data
  {%- endcall %}

{%- endmacro %}



{% macro bigquery__get_period_boundaries(target_schema, target_table, timestamp_field, start_date, stop_date, period, backfill, full_refresh_mode) -%}

  {% call statement('period_boundaries', fetch_result=True) -%}
    with data as (
      select
          {% if backfill and not full_refresh_mode -%}
          cast('{{start_date}}' as timestamp) as start_timestamp,
          {%- else -%}
          coalesce(max({{timestamp_field}}), cast('{{start_date}}' as timestamp)) as start_timestamp,
          {%- endif %}
          coalesce(datetime_add(cast(nullif('{{stop_date}}','') as timestamp), interval -1 millisecond), {{dbt.current_timestamp()}}) as stop_timestamp
      from {{adapter.quote(target_schema)}}.{{adapter.quote(target_table)}}
    )

    select
      start_timestamp,
      stop_timestamp,
      {{ datediff('start_timestamp',
                           'stop_timestamp',
                           period) }}  + 1 as num_periods
    from data
  {%- endcall %}

{%- endmacro %}