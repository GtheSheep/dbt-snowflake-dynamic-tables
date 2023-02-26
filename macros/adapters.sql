{% macro create_materialized_view_as(relation, sql, config) %}
    {{ return(adapter.dispatch('create_dynamic_table_as', macro_namespace = 'gthesheep_dynamic_tables')(relation, sql, config)) }}
{% endmacro %}

{% macro default__create_dynamic_table_as(relation, sql, config) %}
    {%- set lag = config.get('lag', default='1 minute') -%}
    {%- set warehouse = config.get('warehouse', default=target.warehouse) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create or replace
        dynamic table {{relation}}
        lag = {{lag}}
        warehouse = {{warehouse}}
    as (
        {{sql}}
    );

{% endmacro %}
