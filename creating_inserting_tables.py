import os
import pandas as pd
import numpy as np
import unidecode
import psycopg2
import psycopg2.extras as extras
import csv
import json

#Pegando os tipos de dados a partir do csv.
def pairing_data(df):
    columns = df.columns.tolist()
    types = [ str(dtype) for dtype in df.dtypes.tolist() ]
    for i in range(len(types)):
        if types[i] == 'object':
            types[i] = 'text'
        elif 'int' in types[i]:
            types[i] = 'integer'
        elif 'float' in types[i]:
            types[i] = 'float'
    df_dict = {'nome_variavel': columns,
               'tipo_variavel': types}
    
    return df_dict

#Gerando dados a partir dessas tabelas.
def generating_tables_pg(df_dict, table_name):
    colunas = df_dict['nome_variavel']
    tipo = df_dict['tipo_variavel']

    with open("create_table.txt", "w") as file:
        file.write(f"CREATE TABLE {table_name} \n( \n")
        txt = ""
    
    for j in range(len(df_dict['nome_variavel'])):
        if colunas[j] == 'id':
            txt = f'id integer primary key not null,\n'
            continue
        txt += colunas[j] + " " + tipo[j]  + "," + f"\n"
    
    with open('create_table.txt','a') as file:
        file.write(txt + ")")

    with open('create_table.txt','r') as file:
        command = file.readlines()
    
    command[-2] = command[-2].replace(",",'')
    command = "".join(command)
    try:
        executing_pg(command,table_name)
        print("Tabela carregada no banco de dados.")
    except Exception as e:
        print(e)
    return None

def executing_pg(command,table_name):
    #TODO: Transformar essas informações em um arquivo JSON externo.
    postgresConnection = psycopg2.connect(
        host = credentials["host"],
        database= credentials["database"],
        user= credentials["user"],
        password= credentials["password"]
    )

    cursor = postgresConnection.cursor()
    try:
        cursor.execute(command)
        print("Comando executado com sucesso!")
    except Exception as e:
        print(e)
        with open("log_pg_sem_sucesso.txt", 'a') as log:
                        log.write(f"\n ERRO DE GERACAO: {table_name}:\n{e}")
    postgresConnection.commit()
    cursor.close()
    postgresConnection.close()

def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        with open("log_pg_sem_sucesso.txt", 'a') as log:
                        log.write(f"\n ERRO DE INSERCAO:\n {error}\n")
        conn.rollback()
        cursor.close()
        return 1
    print(f"Dataframe {table} inserido no PostgreSQL.")
    cursor.close()

def microdados_to_pg():
    dir = "microdados_tratados"
    microdados_files = [ os.path.join(root,file) for root,dirs,files in os.walk(dir,topdown=False) for file in files ]
    tables_names = [ file.split('.csv')[0] for root,dirs,files in os.walk(dir,topdown=False) for file in files ]
    schema = 'educacao'

    postgresConnection = psycopg2.connect(
        host = credentials["host"],
        database= credentials["database"],
        user= credentials["user"],
        password= credentials["password"]
    )
    
    for i in range(len(microdados_files)):
        df = pd.read_csv(microdados_files[i], sep=';',encoding='utf-8-sig')
        df_dict = pairing_data(df)
        table_name = schema + "." + tables_names[i]
        #print(f'--------\n\n {microdados_files[i]} \n\n',len(df_dict['nome_variavel']),len(df_dict['tipo_variavel']))
        generating_tables_pg(df_dict, table_name=table_name)
        try:
            execute_values(conn=postgresConnection, df=df,table=table_name)
        except Exception as e:
            print(e)
            #O erro <DETAIL:  Key (id)=(1) already exists.> refere-se ao fato de que os dados já estão nas tabelas do PG.
    postgresConnection.close()
    return None

def populacao_to_pg():
    aimed_dir = "população_estimada_IBGE"
    files = [os.path.join(root,file) for root,dirs,files in os.walk(aimed_dir, topdown=False) for file in files]
    tables_names = [ unidecode.unidecode(file.split(os.sep)[1].split('.csv')[0].lower())  for file in files ]
    schema='educacao'

    for i in range(len(tables_names)):
        tables_names[i] = tables_names[i].split('-')
        for j in range(len(tables_names[i])):
            if len(tables_names[i][j]) > 1:
                tables_names[i][j] = tables_names[i][j].split()
                tables_names[i][j] = "_".join(tables_names[i][j])

    for c in range(len(tables_names)):
        tables_names[c]= "_".join(tables_names[c])
        if "(" in tables_names[c]:
            tables_names[c] = tables_names[c].replace("_(entre_a_populacao_de_zero_a_18_anos_de_idade)", "")    

    postgresConnection = psycopg2.connect(
        host = credentials["host"],
        database= credentials["database"],
        user= credentials["user"],
        password= credentials["password"]
    )

    for i in range(len(files)):
        df = pd.read_csv(files[i], sep=';', encoding='utf-8-sig')
        df_dict = pairing_data(df)
        table_name = schema + '.populacao_' + tables_names[i]
        generating_tables_pg(df_dict, table_name=table_name)
        try:
            execute_values(conn=postgresConnection, df=df,table=table_name)
        except Exception as e:
            print(e)
    postgresConnection.close()
    return None

def treat_sinopse(df):
    df.drop(columns=["Unnamed: 0"],inplace=True)
    columns = df.columns.tolist()
    for i in range(len(columns)):
        columns[i] = columns[i].replace("-","_")
        columns[i] = columns[i].replace("/","_or_")
        columns[i] = columns[i].replace('%',"_porcento")
        columns[i] = columns[i].replace('(',"_")
        columns[i] = columns[i].replace(')',"_")
    
        if "educacao_profissional_formacao_inicial_continuada__fic____curso" in columns[i]:
            columns[i] = columns[i].replace('educacao_profissional_formacao_inicial_continuada__fic____curso',
                                            'educ_prof_form_init_cont_fic_curso')
    
    df.columns = columns
    return df

def treat_table_name(table_name):
    table_name = table_name.split('_')
    year = table_name[0]

    table_name.remove(year)
    table_name.append(year)
    table_name = "_".join(table_name)

    if 'pre-escola' in table_name:
        table_name = table_name.replace('pre-escola','pre_escola')

    table_name.replace('-',"_")
    table_name.replace('%',"_porcento")
    table_name.replace('(',"_")
    table_name.replace(')',"_")

    return table_name


def sinopses_to_pg():
    path = "sinopses_estatistica_educacao"
    files = [ os.path.join(root,file) for root, dirs, files in os.walk(path, topdown=False) for file in files if '.csv' in file ]
    tables_names = [ file.lower().replace(' ', "_").replace(".","_").replace("_worksheet","_ws").replace("_csv","") 
                        for root, dirs, files in os.walk(path, topdown=False) for file in files if '.csv' in file ]
    schema="educacao"

    postgresConnection = psycopg2.connect(
        host = credentials["host"],
        database= credentials["database"],
        user= credentials["user"],
        password= credentials["password"]
    )

    with open("log_pg_sem_sucesso.txt", 'w') as log:
        log.write("Arquivo de log para as tabelas nao executadas ao pg:")

    for i in range(len(files)):
        df = pd.read_csv(files[i], sep=';', encoding='utf-8-sig')
        df = treat_sinopse(df)
        df_dict = pairing_data(df)
        table_name = schema + "." + treat_table_name(tables_names[i])
        print(table_name)
        
        try:
            generating_tables_pg(df_dict, table_name=table_name)
            execute_values(conn=postgresConnection, df=df,table=table_name)
        except Exception as e:
            print(e)
            with open("log_pg_sem_sucesso.txt", 'a') as log:
                        log.write(f"\n{table_name, e}")
        
    postgresConnection.close()
        
    return None

with open("credentials.json") as json_file:
    credentials = json.load(json_file)

sinopses_to_pg()