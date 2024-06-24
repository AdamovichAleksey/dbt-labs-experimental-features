{% macro get_period_sql(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}
    {{ return(adapter.dispatch('get_period_sql', 'insert_by_period')(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset)) }}
{% endmacro %}

{% macro default__get_period_sql(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}

  {%- set period_filter -%}
    ({{timestamp_field}} >  '{{start_timestamp}}'::timestamp + interval '{{offset}} {{period}}' and
     {{timestamp_field}} <= '{{start_timestamp}}'::timestamp + interval '{{offset}} {{period}}' + interval '1 {{period}}' and
     {{timestamp_field}} <  '{{stop_timestamp}}'::timestamp)
  {%- endset -%}

  {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}

  select
    {{target_cols_csv}}
  from (
    {{filtered_sql}}
  ) target_cols

{%- endmacro %}



{% macro teradata__get_period_sql(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}

  {%- if period == 'day' -%}
    {%- set interval_expression = 'INTERVAL \'' ~ offset ~ '\' DAY' -%}
  {%- elif period == 'week' -%}
    {%- set interval_expression = 'INTERVAL \'' ~ (offset * 7) ~ '\' DAY' -%}
  {%- elif period == 'month' -%}
    {%- set interval_expression = 'INTERVAL \'' ~ offset ~ '\' MONTH' -%}
  {%- endif %}

  {%- set period_filter -%}
    (
      {{ timestamp_field }} > CAST('{{ start_timestamp }}' AS TIMESTAMP(0)) + {{ interval_expression }} AND
      {{ timestamp_field }} <= CAST('{{ start_timestamp }}' AS TIMESTAMP(0)) + {{ interval_expression }} + 
        CASE '{{ period }}'
          WHEN 'day' THEN INTERVAL '1' DAY
          WHEN 'week' THEN INTERVAL '7' DAYS
          WHEN 'month' THEN INTERVAL '1' MONTH
        END AND
      {{ timestamp_field }} < CAST('{{ stop_timestamp }}' AS TIMESTAMP(0))
    )
  {%- endset -%}

  {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}

  SELECT
    {{target_cols_csv}}
  FROM (
    {{filtered_sql}}
  ) AS target_cols

{%- endmacro %}



{% macro bigquery__get_period_sql(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}

  {%- set period_filter -%}
    ({{timestamp_field}} >  cast(cast(timestamp('{{start_timestamp}}') as datetime) + interval {{offset}} {{period}} as timestamp) and
     {{timestamp_field}} <= cast(cast(timestamp('{{start_timestamp}}') as datetime) + interval {{offset}} {{period}} + interval 1 {{period}} as timestamp) and
     {{timestamp_field}} <  cast('{{stop_timestamp}}' as timestamp))
  {%- endset -%}

  {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}

  select
    {{target_cols_csv}}
  from (
    {{filtered_sql}}
  ) target_cols

{%- endmacro %}