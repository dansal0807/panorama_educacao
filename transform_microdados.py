from csv import excel_tab
import os
import pandas as pd
import numpy as np
import unidecode

aimed_dir = 'arquivos_extraidos'

def get_files(aimed_dir=aimed_dir):
    dirs = [ os.path.join(root,dir_) for root, dirs, files in os.walk(aimed_dir, topdown=False) 
            for dir_ in dirs if "dados" in dir_ ]

    files = [ os.path.join(root,file[-1]) for dir in dirs for root,dirs_,file in os.walk(dir, topdown=False)]

    return files

def treat_csv(df):
    columns = [ column.lower() for column in  df.columns.tolist() ]
    df.columns = columns
    df = df[df['no_uf'] == 'Rio de Janeiro']

    co_entidade = df['co_entidade'].astype(str).tolist()
    no_entidade = df['no_entidade'].astype(str).tolist()
    zip_co_no = list(zip(co_entidade, no_entidade))

    co_no_entidade = [ " ".join(list(i)) for i in zip_co_no]
    df['co_no_entidade'] = co_no_entidade

    index_colunas = list(range(0,len(df.columns)))
    index_co = df.columns.get_loc("co_entidade")
    index_no = df.columns.get_loc("no_entidade")
    index_co_no = df.columns.get_loc("co_no_entidade")

    new_columns = [index_co, index_no, index_co_no]
    columns = [ column for column in index_colunas if column not in new_columns ]
    columns = new_columns + columns
    df = df.iloc[:, columns]

    #TODO: Tratar performance.
    if 'id' not in df.columns:
        df.reset_index(drop=True,inplace=True)
        df['id'] = df.index + 1
        columns = list(df)
        columns.insert(0, columns.pop(columns.index('id')))
        df = df.loc[:,columns]
        df.set_index('id',inplace=True)

    df.fillna(df.dtypes.replace({'float64': 0.0, 'O': 'NULL'}), inplace=True)

    return df

def open_csv():
    files = get_files(aimed_dir)
    dfs = []

    for file in files:
        try:
            df = pd.read_csv(file, sep=';', encoding='mbcs')
            df = treat_csv(df)
            dfs.append(df)
        except Exception as e:
            print(e)
    
    return dfs

def main():
    dir='microdados_tratados'
    cwd = os.getcwd()
    target_dir = os.path.join(cwd,dir)

    dfs = open_csv()
    files = get_files(aimed_dir)
    files_names = [ file.split(os.sep)[-1] for file in files ]
    files_names = [ os.path.join(target_dir,file) for file in files_names ]

    files_names = [ file.split('.csv')[0] for file in files_names ]
    files_names = list(map(lambda x: x + "_tratado.csv", files_names))

    if dir not in os.listdir():
        os.mkdir(target_dir)

    for i in range(len(dfs)):
        dfs[i].to_csv(files_names[i], sep=';', encoding='utf-8-sig')
    
    return None

main()