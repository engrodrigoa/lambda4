{#- Elementary captures test results (failures, samples) by wrapping the test materialization. dbt >= 1.8 only
    honours a package override of a builtin materialization through a root-project macro like this one. -#}
{% materialization test, default %}
  {{ return(elementary.materialization_test_default()) }}
{% endmaterialization %}
