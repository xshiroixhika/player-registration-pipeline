{#
  In prod, write to the exact schema names (silver, gold) instead of dbt's default
  "<target>_<custom>" naming. In dev and CI, prefix with the target schema so each
  developer and each CI run gets isolated tables.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- elif custom_schema_name is not none -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
