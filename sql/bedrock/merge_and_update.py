from sqlalchemy import create_engine, text
import boto3, json

# import os, shutil, re
# import time
# import logging
# import traceback

# refine_db = constants.REFINED_DATABASE_NAME
# raw_db = constants.RAW_DATABASE_NAME

datatype_map = {"DOUBLE": "FLOAT", "DOUBLE PRECISION": "FLOAT", "REAL": "FLOAT",
                "INTEGER": "INTEGER", "BIGINT": "NUMBER", "SMALLINT": "NUMBER",
                "TINYINT": "NUMBER", "BYTEINT": "NUMBER", "FIXED": "NUMBER",
                "TEXT": "VARCHAR", "STRING": "VARCHAR", "VARIANT": "VARCHAR",
                "BOOLEAN": "BOOLEAN"}


def get_secret(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name='us-east-1')
    secret = client.get_secret_value(SecretId=secret_name)
    try:
        secret_val = json.loads(secret['SecretString'])
    except:
        secret_val = secret['SecretString']
    return secret_val


def query(env, sql):
    secret_name = 'data/{}/refinement_generator/snowflake_connection_info'.format(env)
    secrets = get_secret(secret_name)
    engine = create_engine('snowflake://{user}:{password}@{account}/'.format(**secrets))
    result = {}
    with engine.connect() as connection:
        q = connection.execute(text(sql))
        for row in q:
            result[row[2]] = json.loads(row[3])
    return result


def merge_and_update(env, tb_details):
    raw_table_name = "RAW_" + env.upper() + ".{schema}.{table}".format(**tb_details)
    refined_table_name = "REFINED_" + env.upper() + ".{schema}.{table}".format(**tb_details)
    raw_columns_dict = query(env, f"SHOW COLUMNS IN {raw_table_name};")
    merge_columns = []
    for k, v in raw_columns_dict.items():
        if k not in ['_FIVETRAN_DELETED', 'RAW_LOAD_TIME_CST', 'RAW_DATA']:
            if 'TIMESTAMP' in v['type']:
                '''CONVERT_TIMEZONE('UTC', 'America/Chicago', SYSTEM_MODSTAMP::TIMESTAMP_NTZ)   AS SYSTEM_MODSTAMP_CST'''
                column_UTC = f"{k}::TIMESTAMP_NTZ AS {k.strip('_')}_UTC"
                column_CST = f"CONVERT_TIMEZONE('UTC', 'America/Chicago', {k}::TIMESTAMP_NTZ)   AS {k.strip('_')+'_CST'}"
                column = column_UTC + "\n," + column_CST
            else:
                column = k
            merge_columns.append(column)
    # raw_columns = raw_columns_dict.keys()
    # refined_table_name = "REFINED_" + env.upper() + ".{schema}.{table}".format(**tb_details)
    '''
MERGE INTO {{ params.refined_database_name }}.BEDROCK.PERIOD tgt
USING (
SELECT
   ID
 , FISCAL_YEAR_SETTINGS_ID
 , TYPE
 , START_DATE
 , END_DATE
 , IS_FORECAST_PERIOD
 , QUARTER_LABEL
 , PERIOD_LABEL
 , NUMBER
 , FULLY_QUALIFIED_LABEL
 , _FIVETRAN_DELETED
 , SYSTEM_MODSTAMP::TIMESTAMP_NTZ                                               AS SYSTEM_MODSTAMP_UTC
 , _FIVETRAN_SYNCED::TIMESTAMP_NTZ                                              AS FIVETRAN_SYNCED_UTC
 , CONVERT_TIMEZONE('UTC', 'America/Chicago', SYSTEM_MODSTAMP::TIMESTAMP_NTZ)   AS SYSTEM_MODSTAMP_CST
 , CONVERT_TIMEZONE('UTC', 'America/Chicago', _FIVETRAN_SYNCED::TIMESTAMP_NTZ)  AS FIVETRAN_SYNCED_CST
 , CURRENT_TIMESTAMP() as REFINED_LOAD_TIME_CST
FROM {{ params.raw_database_name }}.BEDROCK.PERIOD
WHERE _FIVETRAN_SYNCED::TIMESTAMP_NTZ > (
    SELECT nvl(max(FIVETRAN_SYNCED_UTC ),to_timestamp('2000-01-01'))
    FROM {{ params.refined_database_name }}.BEDROCK.PERIOD
  )
) src
ON src.ID = tgt.ID
WHEN MATCHED THEN UPDATE
SET
   tgt.ID                       = src.ID
 , tgt.FISCAL_YEAR_SETTINGS_ID  = src.FISCAL_YEAR_SETTINGS_ID
 , tgt.TYPE                     = src.TYPE
 , tgt.START_DATE               = src.START_DATE
 , tgt.END_DATE                 = src.END_DATE
 , tgt.IS_FORECAST_PERIOD       = src.IS_FORECAST_PERIOD
 , tgt.QUARTER_LABEL            = src.QUARTER_LABEL
 , tgt.PERIOD_LABEL             = src.PERIOD_LABEL
 , tgt.NUMBER                   = src.NUMBER
 , tgt.FULLY_QUALIFIED_LABEL    = src.FULLY_QUALIFIED_LABEL
 , tgt.FIVETRAN_DELETED         = src._FIVETRAN_DELETED
 , tgt.SYSTEM_MODSTAMP_UTC      = src.SYSTEM_MODSTAMP_UTC
 , tgt.FIVETRAN_SYNCED_UTC      = src.FIVETRAN_SYNCED_UTC
 , tgt.SYSTEM_MODSTAMP_CST      = src.SYSTEM_MODSTAMP_CST
 , tgt.FIVETRAN_SYNCED_CST      = src.FIVETRAN_SYNCED_CST
 , tgt.REFINED_LOAD_TIME_CST    = src.REFINED_LOAD_TIME_CST
WHEN NOT MATCHED THEN INSERT (
   ID
 , FISCAL_YEAR_SETTINGS_ID
 , TYPE
 , START_DATE
 , END_DATE
 , IS_FORECAST_PERIOD
 , QUARTER_LABEL
 , PERIOD_LABEL
 , NUMBER
 , FULLY_QUALIFIED_LABEL
 , FIVETRAN_DELETED
 , SYSTEM_MODSTAMP_UTC
 , FIVETRAN_SYNCED_UTC
 , SYSTEM_MODSTAMP_CST
 , FIVETRAN_SYNCED_CST
 , REFINED_LOAD_TIME_CST
)
VALUES (
   src.ID
 , src.FISCAL_YEAR_SETTINGS_ID
 , src.TYPE
 , src.START_DATE
 , src.END_DATE
 , src.IS_FORECAST_PERIOD
 , src.QUARTER_LABEL
 , src.PERIOD_LABEL
 , src.NUMBER
 , src.FULLY_QUALIFIED_LABEL
 , src._FIVETRAN_DELETED
 , src.SYSTEM_MODSTAMP_UTC
 , src.FIVETRAN_SYNCED_UTC
 , src.SYSTEM_MODSTAMP_CST
 , src.FIVETRAN_SYNCED_CST
 , src.REFINED_LOAD_TIME_CST
);

INSERT INTO {{ params.refined_database_name }}.BEDROCK.PERIOD_ARCHIVE (
   ID
 , FISCAL_YEAR_SETTINGS_ID
 , TYPE
 , START_DATE
 , END_DATE
 , IS_FORECAST_PERIOD
 , QUARTER_LABEL
 , PERIOD_LABEL
 , NUMBER
 , FULLY_QUALIFIED_LABEL
 , FIVETRAN_DELETED
 , SYSTEM_MODSTAMP_UTC
 , FIVETRAN_SYNCED_UTC
 , SYSTEM_MODSTAMP_CST
 , FIVETRAN_SYNCED_CST
 , REFINED_LOAD_TIME_CST
)
SELECT
   ID
 , FISCAL_YEAR_SETTINGS_ID
 , TYPE
 , START_DATE
 , END_DATE
 , IS_FORECAST_PERIOD
 , QUARTER_LABEL
 , PERIOD_LABEL
 , NUMBER
 , FULLY_QUALIFIED_LABEL
 , FIVETRAN_DELETED
 , SYSTEM_MODSTAMP_UTC
 , FIVETRAN_SYNCED_UTC
 , SYSTEM_MODSTAMP_CST
 , FIVETRAN_SYNCED_CST
 , REFINED_LOAD_TIME_CST
FROM {{ params.refined_database_name }}.BEDROCK.PERIOD
WHERE REFINED_LOAD_TIME_CST > (
    SELECT nvl(max(REFINED_LOAD_TIME_CST),to_timestamp('2000-01-01'))
    FROM {{ params.refined_database_name }}.BEDROCK.PERIOD_ARCHIVE
  )
;
    '''
    refined_columns = '\n,'.join(merge_columns)
    # print(refined_columns)
    # for c in raw_columns:
    #     c = 1
    #     refined_columns.append(c)
    merge_using = f'MERGE INTO {refined_table_name} TGT \n' \
                  f'USING ( SELECT \n{refined_columns} FROM {raw_table_name} \n' \
                  f'WHERE _FIVETRAN_SYNCED::TIMESTAMP_NTZ > ' \
                  f'(SELECT nvl(max(FIVETRAN_SYNCED_UTC ),to_timestamp("2000-01-01")) FROM {refined_table_name})) SRC\n'
    on_condition = 'ON src.ID = tgt.ID\n'
    when_matched_columns = []
    for c in merge_columns:
        if c != 'ID':
            if 'AS' in c:
                column = c.split('AS')[-1].strip()
            else:
                column = c
        when_matched_columns.append(f'tgt.{column} = src.{column}')
    when_matched = 'WHEN MATCHED THEN UPDATE SET\n' + '\n,'.join(when_matched_columns)
    when_not_matched_columns = []
    for c in merge_columns:
        if 'AS' in c:
            column = c.split('AS')[-1].strip()
        else:
            column = c
        when_not_matched_columns.append(column)
    not_matched_columns = '\n,'.join(when_not_matched_columns)
    values = '\n,src.'.join(when_not_matched_columns)
    when_not_matched = f"\nWHEN NOT MATCHED THEN INSERT (\n{not_matched_columns}\n)\nVALUES(\n{values}\n);"
    merge_cmd = merge_using + on_condition + when_matched + when_not_matched
    print(merge_cmd)





def add_new_columns(env, tb_details):
    raw_table_name = "RAW_" + env.upper() + ".{schema}.{table}".format(**tb_details)
    raw_columns_dict = query(env, f"SHOW COLUMNS IN {raw_table_name};")
    raw_columns = raw_columns_dict.keys()
    refined_table_name = "REFINED_" + env.upper() + ".{schema}.{table}".format(**tb_details)
    refined_columns_dict = query(env, f"SHOW COLUMNS IN {refined_table_name};")
    refined_columns = refined_columns_dict.keys()
    missing_refined_columns = set(raw_columns) - set(refined_columns)
    alt_table = 'ALTER TABLE {}'.format(refined_table_name)
    add_columns = []
    for c in missing_refined_columns:
        c_name = c
        c_type = raw_columns_dict[c]
        if 'TIMESTAMP' in c_type['type']:
            c_name = c.strip('_') + '_UTC' + '\t' + c_type['type'] + ',\n\t' + c.strip('_') + '_CST'
        elif c_type['type'] == 'TEXT':
            c_type['type'] = 'VARCHAR'
        add_columns.append(c_name + '\t' + c_type['type'])
    alter_cmd = alt_table + '\nADD\n\t' + ',\n\t'.join(add_columns) + '\n;'
    print(alter_cmd)
    update = f"UPDATE {refined_table_name} REF\n"
    set_condition = []
    for c in add_columns:
        column = c.split("\t")[0]
        set_condition.append(f"REF.{column} = RAW.{column}\n")
    set_columns = f"SET {','.join(set_condition)}"
    from_condition = f"FROM {raw_table_name} RAW \n WHERE REF.ID = RAW.ID;"
    update_cmd = update + set_columns + from_condition
    print(update_cmd)

add_new_columns('dev', {'schema': 'BEDROCK', 'table': 'AGENT_WORK'})
# merge_and_update('dev', {'schema': 'BEDROCK', 'table': 'AGENT_WORK'})
