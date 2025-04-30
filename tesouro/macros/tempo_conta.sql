{% macro tempo_conta(data_coluna, tipo="anos", formato="DD-MM-YYYY") %}
    case
        when {{ data_coluna }} is null then null
        {% if tipo == 'dias' %}
            else (current_date - to_date({{ data_coluna }}, '{{ formato }}'))::int
        {% elif tipo == 'meses' %}
            else date_part('year', age(current_date, to_date({{ data_coluna }}, '{{ formato }}'))) * 12 +
                 date_part('month', age(current_date, to_date({{ data_coluna }}, '{{ formato }}')))
        {% elif tipo == 'anos' %}
            else date_part('year', age(current_date, to_date({{ data_coluna }}, '{{ formato }}')))
        {% else %}
            else null
        {% endif %}
    end
{% endmacro %}
