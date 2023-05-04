from sqlalchemy import create_engine
import boto3, json, os, shutil, re
import time
import logging
import pandas

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',level=logging.DEBUG)

start = time.time()


def get_secret(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name='us-east-1')
    secret = client.get_secret_value(SecretId=secret_name)
    try:
        secret_val = json.loads(secret['SecretString'])
    except:
        secret_val = secret['SecretString']
    return secret_val


def get_raw_schema(env,db,tb):
    secrets = get_secret('data/{}/refinement_generator/snowflake_connection_info'.format(env))
    connection = create_engine('snowflake://{user}:{password}@{account}/'.format(**secrets)).connect()
    #connect_toDB = connection.execute("use {};".format(db))
    raw_schema = connection.execute("select get_ddl('table','{}.{}') schema;".format(tb)).fetchall()
    #print("RAW SCHEMA : ",list(raw_schema)[0][0])
    return raw_schema


def generate_new_refined_schema(env, db, tb):
    schema = get_raw_schema(env, db, tb)
    ddl = (list(schema)[0])[0].replace('\n\t', ' ').replace('\n', ' ')
    #print("RAW SCHEMA : ", ddl)
    schema_dict = {}
    refine_schema_dict = {}
    prefix_archive = "CREATE TABLE IF NOT EXISTS {}.{}_ARCHIVE \n(\n".format(db.replace('RAW', 'REFINED'), tb)
    prefix = "CREATE TABLE IF NOT EXISTS {}.{} \n(\n".format(db.replace('RAW', 'REFINED'), tb)
    if "primary key" in ddl:
        for x in re.findall('.\((.*)\).', ddl)[0].split(', ')[:-2]:
            schema_dict[x.strip().strip("_").split(' ')[0]] = x.strip().split(' ')[1]
        key = re.findall(".\((.*), primary key (.*)\).", ddl)[0][-1]
        suffix = ',\nCURRENT_TIMESTAMP() as REFINED_LOAD_TIME_CST,\nPRIMARY KEY ({})'.format(key) + '\n);'
    else:
        for x in re.findall('.\((.*)\).', ddl)[0].split(', '):
            schema_dict[x.strip().strip("_").split(' ')[0]] = x.strip().split(' ')[1]
        suffix_archive = ',\nCURRENT_TIMESTAMP() as REFINED_LOAD_TIME_CST,\nPRIMARY KEY ({},{})'.format('ID', 'REFINED_LOAD_TIME_CST') + '\n);'
        suffix = ',\nCURRENT_TIMESTAMP() as REFINED_LOAD_TIME_CST,\nPRIMARY KEY ({})'.format('ID') + '\n);'
    for k,v in schema_dict.items():
        schema_dict[k]=re.sub('\(.*\)','',v)

    for k in schema_dict.keys():
        if 'TIMESTAMP' in schema_dict[k]:
            refine_schema_dict[k + '_UTC'] = "TIMESTAMP"
            refine_schema_dict[k + '_CST'] = "TIMESTAMP"
        else:
            refine_schema_dict[k] = schema_dict[k]
    refined_schema = prefix + ", \n".join([k + " " + v for k, v in refine_schema_dict.items()]) + suffix
    refined_archive_schema = prefix_archive + ", \n".join([k + " " + v for k, v in refine_schema_dict.items()]) + suffix_archive
    return refined_schema#+"\n\n"+refined_archive_schema


#generate_refined_schema(environment,schema,table,primary_key)
'''
sql = 'show columns in table "RAW_DEV"."BEDROCK"."ACCOUNT";'
secrets = get_secret('data/{}/refinement_generator/snowflake_connection_info'.format('dev'))
connection = create_engine('snowflake://{user}:{password}@{account}/'.format(**secrets)).connect()
result = connection.execute(sql).fetchall()
for x in result:
    print(x)

with open("table_names_bedrock") as f:
    for t in f:
        print("REFINED SCHEMA : ", generate_refined_schema("dev", "RAW_DEV.BEDROCK", t.strip()))
        print("\n\n")

'''
print("REFINED SCHEMA :\n" , generate_new_refined_schema("dev","RAW_DEV.GLUE_SERVICE_PUBLIC","ELIGIBILITY_PROGRAM_CALCS"))
end = time.time()
print("\nTime for execution (in SEC) : ", round(end-start,3))
'''
sql = 'show columns in table "RAW_DEV"."BEDROCK"."ACCOUNT";'
secrets = get_secret('data/{}/refinement_generator/snowflake_connection_info'.format('dev'))
connection = create_engine('snowflake://{user}:{password}@{account}/'.format(**secrets)).connect()
result = connection.execute(sql).fetchall()


def alter_refined_schema(env, db, tb):
    secrets = get_secret('data/{}/refinement_generator/snowflake_connection_info'.format(env))
    connection = create_engine('snowflake://{user}:{password}@{account}/'.format(**secrets)).connect()
    schema = get_raw_schema(env, db, tb)
    ddl = (list(schema)[0])[0].replace('\n\t', ' ').replace('\n', ' ')
    schema_dict = {}
    refine_schema_dict = {}
    if "primary key" in ddl:
        for x in re.findall('.\((.*)\).', ddl)[0].split(', ')[:-2]:
            schema_dict[x.strip().strip("_").split(' ')[0]] = x.strip().split(' ')[1]
        key = re.findall(".\((.*), primary key (.*)\).", ddl)[0][-1]
    else:
        for x in re.findall('.\((.*)\).', ddl)[0].split(', '):
            schema_dict[x.strip().strip("_").split(' ')[0]] = x.strip().split(' ')[1]

    for k, v in schema_dict.items():
        schema_dict[k] = re.sub('\(.*\)', '', v)

    for k in schema_dict.keys():
        if 'TIMESTAMP' in schema_dict[k]:
            refine_schema_dict[k + '_UTC'] = "TIMESTAMP"
            refine_schema_dict[k + '_CST'] = "TIMESTAMP"
        else:
            refine_schema_dict[k] = schema_dict[k]

    new_refined_cols = list(refine_schema_dict.keys())
    existing_refined_cols = [x[2] for x in connection.execute('show columns in table "REFINED_{}"."{}"."{}";'.format(env.upper(),db.split(".")[-1],tb)).fetchall()]
    print(new_refined_cols)
    print(existing_refined_cols)
    for c in new_refined_cols:
        if c not in existing_refined_cols:
            print(c)

#alter_refined_schema("dev","RAW_DEV.BEDROCK","ACCOUNT_CONTACT_ROLE")
'''