{% macro ref() %}

{% set rel = builtins.ref(*varargs, **kwargs) %}

{% set database_override = var('ref_database_override', none) %}
{% set schema_override = var('ref_schema_override', none) %}

{% if database_override %}
    {% set rel = rel.replace_path(database=database_override) %}
{% endif %}

{% if schema_override %}
    {% set rel = rel.replace_path(schema=schema_override) %}
{% endif %}

{% do return(rel) %}

{% endmacro %}
