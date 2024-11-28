{% macro generate_schema_name(custom_schema_name=None, node=None) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {%- if target.name == "dev" -%}
            {{ "dev_" ~ custom_schema_name }}
        {%- elif target.name == "prod" -%}
            {{ "prod_" ~ custom_schema_name }}
        {%- else -%}
            {{ default_schema ~ "_" ~ custom_schema_name }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}
