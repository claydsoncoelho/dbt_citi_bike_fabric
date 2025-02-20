{# https://github.com/microsoft/dbt-fabric/blob/main/dbt/adapters/fabric/fabric_relation.py #}

{%- macro custom_snapshot(
    source_model_name,
    target_table_name, 
    key_col,
    other_cols,
    timestamp_col
)-%}

    with 
        source_table as (select * from {{ ref(source_model_name) }}),
    
    {#- Defining target table -#}
    {%- set target_relation_exists, target_relation = get_or_create_relation(
            database = this.database,
            schema = this.schema,
            identifier = this.identifier,
            type='table') -%}

    {%- if not target_relation.is_table -%}
        {% do exceptions.relation_wrong_type(target_relation, 'table') %}
    {%- endif -%}

    {% if target_relation_exists %}

        target_table as (select * from {{ this }} where dbt_current_flag = 'Y'),

    {%- else -%}

        target_table as (select top 1 * from source_table where 1=2 order by {{ key_col }}),

    {% endif %}


    relevant_columns_from_source as (

        select 
            source_table.{{ key_col }}
            {% for col in other_cols %}, source_table.{{ col }} 
            {% endfor -%}
            , source_table.{{ timestamp_col }}
            , 'source' as source_name

        from source_table
        left join target_table
            on source_table.{{ key_col }} = target_table.{{ key_col }}
            {% for col in other_cols %}and source_table.{{ col }} = target_table.{{ col }} 
            {% endfor %}

        where target_table.{{ key_col }} is null
    ),


    relevant_records_from_target as (

        select 
            target_table.{{ key_col }}
            {% for col in other_cols %}, target_table.{{ col }} 
            {% endfor -%}
            , target_table.{{ timestamp_col }}
            , 'target' as source_name

        from target_table
        inner join relevant_columns_from_source
            on target_table.{{ key_col }} = relevant_columns_from_source.{{ key_col }}

    ),


    union_source_target as (
        select *  from relevant_columns_from_source
        union all
        select * from relevant_records_from_target
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
            , source_name
            , (
                row_number() over (
                    partition by {{ key_col }} order by {{ timestamp_col }}
                ) 
                - 
                row_number() over (
                    partition by {{ key_col }} {% for col in other_cols %}, {{ col }} {%- endfor %} order by {{timestamp_col}}
                ) 
            ) as change
        from union_source_target
    ),


    identify_first_row as (
        select
            {{ key_col }}
            {% for col in other_cols %}, {{ col }}
            {% endfor -%}
            , {{timestamp_col}}
            , source_name
            -- Only run_num = 1 are intersting. If there are more records identic to the first one, they can be ignored.
            , row_number() over (
                partition by {{ key_col }}, change order by {{timestamp_col}} asc, source_name desc
            ) as row_num
            , row_number() over (
                partition by {{ key_col }} order by {{timestamp_col}} asc, source_name desc
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
        where row_num = 1 and source_name = 'source'
    ),


    add_current_flag as (
        select 
            {{ key_col }}
            {% for col in other_cols %}, {{ col }}
            {% endfor -%}
            , {{ timestamp_col }}
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
            , {{ timestamp_col }}
            , dbt_valid_from
            , dbt_valid_to
            , cast(current_timestamp as datetime2(6)) as dbt_updated_at
            , dbt_current_flag = 'y'
        from add_current_flag
    )

    select * from final

{%- endmacro -%}
