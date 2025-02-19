{# https://github.com/microsoft/dbt-fabric/blob/main/dbt/adapters/fabric/fabric_relation.py #}

{%- macro custom_snapshot(
    source_model_name,
    target_table_name, 
    key_col,
    other_cols,
    timestamp_col
)-%}

    with 
        source_table as (select * from {{ ref(source_model_name) }} ),

    {%- set target_relation_exists, target_relation = get_or_create_relation(
            database = this.database,
            schema = this.schema,
            identifier = this.identifier,
            type='table') -%}

    {%- if not target_relation.is_table -%}
        {% do exceptions.relation_wrong_type(target_relation, 'table') %}
    {%- endif -%}

    {% if target_relation_exists %}

        target_table as (select * from {{ this }} ),

    {% else %}

        target_table as (select top 1 * from source_table where 1=2),

    {%- endif %}


    relevant_records_from_targe as (

        select 
            target_table.{{ key_col }}
            {% for col in other_cols %}, target_table.{{ col }} 
            {% endfor -%}

        from target_table
        inner join source_table
            on target_table.{{ key_col }} = source_table.{{ key_col }}

    ),


    --The difference between row_number()_1 and row_number()_2 of change is used to determine if a change has occurred. 
    --If the change value is 0, the records are considered identical.
    --If the change value is non-zero, it means the value of the other columns (other_cols) has changed for that particular key_col across different timestamps.
    flag_changes as (  
        select 
            {{ key_col }}
            {% for col in other_cols %}, {{ col }} 
            {% endfor -%}
            , {{ timestamp_col }}
            , (
                row_number() over (
                    partition by {{ key_col }} order by {{ timestamp_col }}
                ) 
                - 
                row_number() over (
                    partition by {{ key_col }} {% for col in other_cols %}, {{ col }} {%- endfor %} order by {{timestamp_col}}
                ) 
            ) as change
        from source_table
    ),


    identify_first_row as (
        select
            {{ key_col }}
            {% for col in other_cols %}, {{ col }}
            {% endfor -%}
            , {{timestamp_col}}
            -- Only run_num = 1 are intersting. If there are more records identic to the first one, we can ignore them.
            , row_number() over (
                partition by {{ key_col }}, change order by {{timestamp_col}}
            ) as row_num
            , row_number() over (
                partition by {{ key_col }} order by {{timestamp_col}}
            ) as first_row_num
        from flag_changes
    ),


    add_valid_from_valid_to as (
        select 
            {{ key_col }}
            {% for col in other_cols %},  {{ col }}
            {% endfor -%}
            , {{ timestamp_col }}
            , case first_row_num
                when 1 then '{{var("min_date")}}'
                else {{ timestamp_col }}
            end as valid_from
            , coalesce(
                lead({{ timestamp_col }}) over (
                    partition by {{ key_col }} order by {{ timestamp_col }}
                    )
                , '{{var("max_date")}}'
            ) as valid_to
        from identify_first_row
        where row_num = 1
    ),


    add_current_flag as (
        select 
            {{ key_col }}
            {% for col in other_cols %}, {{ col }}
            {% endfor -%}
            , cast(valid_from as datetime2(6)) as dbt_valid_from
            , cast(valid_to as datetime2(6)) as dbt_valid_to
            , case valid_to
                when '{{ var("max_date") }}' then 'Y'
                else 'N'
            end as dbt_current_flag
        from add_valid_from_valid_to

    ),


    final as (
        select 
            {{ dbt_utils.generate_surrogate_key([ key_col, 'dbt_valid_from']) }} as dbt_scd_id
            , {{ key_col }}
            {% for col in other_cols %}, {{ col }}
            {% endfor -%}
            , dbt_valid_from
            , dbt_valid_to
            , current_timestamp as dbt_updated_at
            , dbt_current_flag
        from add_current_flag
    )

    select * from final

{%- endmacro -%}
