{% materialization dynamic_table, adapter='snowflake' %}

  {% set original_query_tag = set_query_tag() %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {{ run_hooks(pre_hooks) }}

  {% if old_relation.type != 'dynamic_table' %}
    {#-- Need to check what the type actually comes back as --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a " ~ old_relation.type ~ " and this model is a dynamic table.") }}
    {% do adapter.drop_relation(old_relation) %}
  {% endif %}

  {% call statement("main") %}
      {{ gthesheep_dynamic_tables.create_dynamic_table_as(target_relation, sql, config) }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {% do adapter.commit() %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
