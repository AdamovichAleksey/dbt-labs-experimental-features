{% macro get_period_boundaries(target_schema, target_table, timestamp_field, timestamp_field_target, start_date, stop_date, period, backfill, full_refresh_mode) -%}
    {{ return(adapter.dispatch('get_period_boundaries', 'insert_by_period')(target_schema, target_table, timestamp_field, timestamp_field_target, start_date, stop_date, period, backfill, full_refresh_mode)) }}
{% endmacro %}



{% macro default__get_period_boundaries(target_schema, target_table, timestamp_field, timestamp_field_target, start_date, stop_date, period, backfill, full_refresh_mode) -%}

  {%- set timestamp_field_target_col_name = timestamp_field_target.split('.')[-1] -%}

  {% call statement('period_boundaries', fetch_result=True) -%}
    with data as (
      select
          {% if backfill and not full_refresh_mode -%}
            cast('{{start_date}}' as timestamp) as start_timestamp,
          {%- else -%}
            coalesce(max({{ timestamp_field_target_col_name }}), cast('{{start_date}}' as timestamp)) as start_timestamp,
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



{% macro teradata__get_period_boundaries(target_schema, target_table, timestamp_field, timestamp_field_target, start_date, stop_date, period, backfill, full_refresh_mode) -%}

  {%- set timestamp_field_target_col_name = timestamp_field_target.split('.')[-1] -%}

  {% call statement('period_boundaries', fetch_result=true) -%}
    with data as (
      select
          {% if backfill and not full_refresh_mode -%}
            cast('{{ start_date }}' as timestamp(0)) as start_timestamp,
          {%- else -%}
            coalesce(
              cast(max({{ timestamp_field_target_col_name }}) as timestamp(0)), 
              cast(date '{{ start_date }}' as timestamp(0))
            ) as start_timestamp,
          {%- endif %}
            cast(date '{{ stop_date }}' as timestamp(0)) - interval '0.001' second as stop_timestamp
      from {{ adapter.quote(target_schema) }}.{{ adapter.quote(target_table) }}
    )

    select
      start_timestamp,
      stop_timestamp,
      -- calculate the difference in the specified period units between start and stop timestamps
      case 
        when '{{ period }}' = 'day' then cast(((stop_timestamp - start_timestamp ) day) as int) + 1
        when '{{ period }}' = 'week' then cast(((stop_timestamp - start_timestamp ) day) as int) / 7 + 1
        when '{{ period }}' = 'month' then cast(((stop_timestamp - start_timestamp ) month) as int) + 1
      end as num_periods
    from data
  {%- endcall %}

{%- endmacro %}



{% macro bigquery__get_period_boundaries(target_schema, target_table, timestamp_field, start_date, stop_date, period, backfill, full_refresh_mode) -%}

  {% call statement('period_boundaries', fetch_result=True) -%}
    with data as (
      select
          {% if backfill and not full_refresh_mode -%}
          cast('{{start_date}}' as timestamp) as start_timestamp,
          {%- else -%}
          coalesce(max({{ timestamp_field_col_name }}), cast('{{start_date}}' as timestamp)) as start_timestamp,
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
