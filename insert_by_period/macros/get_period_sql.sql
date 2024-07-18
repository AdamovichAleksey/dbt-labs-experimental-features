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
    {%- set offset_expression = 'interval \'' ~ offset ~ '\' day' -%}
  {%- elif period == 'week' -%}
    {%- set offset_expression = 'interval \'' ~ ((offset | int) * 7) ~ '\' day' -%}
  {%- elif period == 'month' -%}
    {%- set offset_expression = 'interval \'' ~ offset ~ '\' month' -%}
  {%- endif %}

  {%- if period == 'day' -%}
    {%- set interval_expression = "interval '1' day" -%}
  {%- elif period == 'week' -%}
    {%- set interval_expression = "interval '7' day" -%}
  {%- elif period == 'month' -%}
    {%- set interval_expression = "interval '1' month" -%}
  {%- endif %}

  {%- set period_filter -%}
    (
      {{ timestamp_field }} >= cast('{{ start_timestamp }}' as timestamp(0)) + {{ offset_expression }} 
      and {{ timestamp_field }} < least(
        cast('{{ start_timestamp }}' as timestamp(0)) + {{ offset_expression }} + {{ interval_expression }},
        cast('{{ stop_timestamp }}' as timestamp(0))
      )
    )
  {%- endset -%}

  {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}

  select
    {{target_cols_csv}}
  from (
    {{filtered_sql}}
  ) as target_cols

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
