{% macro tempo_conta(data_coluna, tipo="anos") %}
    case
        when {{ data_coluna }} is null then null
        {% if tipo == 'dias' %}
            else datediff('day', {{ data_coluna }}, current_date)
        {% elif tipo == 'meses' %}
            else datediff('month', {{ data_coluna }}, current_date)
        {% elif tipo == 'anos' %}
            else floor(datediff('day', {{ data_coluna }}, current_date) / 365.25)
        {% else %}
            else null
        {% endif %}
    end
{% endmacro %}
