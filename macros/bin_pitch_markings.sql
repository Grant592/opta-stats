{% macro bin_pitch_markings() %}

{% set bins %}
case 
  when x_coord =< 22 then own_22
  when x_coord > 22 and x_coord <= 50 then own_22_to_50
  when x_coord > 50 and x_coord <= 78 then opp_50_22
  when x_coord > 78 and then opp_22
end
{% endset %}

{{ return(bins) }}

{% endmacro %}