from csv import excel_tab
import os
import pandas as pd
import numpy as np
import unidecode

#Utilizo um dicionário devido a problemas de encoding ao ler os dicionarios. A string buscada pelo dicionário_sinopse
#Não bate com o nome utilizado nas abas - além mesmo da acentuação. Por este motivo, é sempre preferível utilizar JSON.

dict2 = {0: '1.12',
          1: '1.13',
          2: '1.16',
          3: '1.18',
          4: '1.19',
          5: '1.21',
          6: '1.23',
          7: '1.24',
          8: '1.26',
          9: '1.28',
          10: '1.29',
          11: '1.43',
          12: '1.44',
          13: '1.49',
          14: '1.50',
          15: '1.8',
          16: '1.9',
          17: '2.10',
          18: '2.11',
          19: '2.14',
          20: '2.15',
          21: '2.23',
          22: '2.24',
          23: '2.27',
          24: '2.28',
          25: '2.31',
          26: '2.32',
          27: '3.14',
          28: '3.16',
          29: '3.18',
          30: '3.7',
          31: '3.9',
          32: 'Classes Comuns 1.39',
          33: 'Classes Exclusivas 1.45',
          34: 'Creche 1.6',
          35: 'Educação Básica 3.1',
          36: 'Educação Especial 2.43',
          37: 'Educação Especial 3.25',
          38: 'Educação Profissional 1.30',
          39: 'EJA 1.34',
          40: 'Pré-Escola 1.10'}
                 
def find_excels(aimed_dir='arquivos_extraidos'):
    """ aimed_dir: pasta com os dados brutos que serão transformados ao longo do script.
        A função retorna uma lista com o caminho para os dados brutos."""
    dirs = [ os.path.join(root,dir_) for root, dirs, files in os.walk(aimed_dir, topdown=False) 
            for dir_ in dirs if "Sinopse_Estatistica" in dir_ ]
    files = [ os.path.join(root,file) for dir_ in dirs for root,dirs_,files in os.walk(dir_,topdown=False) 
                for file in files if '.xlsx' in file ]
    
    return files

def get_dict(dict_dir='dicionarios_sinopse'):
    """dict_dir: pasta com os dicionários que informam as colunas desejadas para a transformação.
    A função retorna uma lista com o caminho para os dicionários."""

    guide_dicts = [ os.path.join(root,file) for root,dirs,files in os.walk(dict_dir, topdown=False) 
                for file in files if 'except' not in file]

    return guide_dicts

def treat_exceptions(file_dir, dict_dir):
    """A função trata os arquivos que não foram bem sucedidos em transformar devidamente, seja por alguma
    alteração no dicionário ou pela formação original do dado bruto.
    file_dir: pasta com os dados transformados e em que o log está armazenado.
    dict_dir: pasta com os dicionários que informam as colunas desejadas para a transformação."""

    file = [os.path.join(root,file) for root,dirs,files in os.walk(file_dir, topdown=False) 
                                                        for file in files if '.txt' in file]
    excels = find_excels(aimed_dir='arquivos_extraidos')
    with open(file[0],'r') as file:
        file = file.readlines()
    
    dict_exceptions = [os.path.join(root,x) for root, dirs, files in os.walk(dict_dir, topdown=False) 
                                                                        for x in files if 'except' in x]
    file_txt = [line.replace('\n','').split('.csv')[0] for line in file if 'Arquivo' not in line]

    ws_exceptions = [ x.split('_')[-1] for x in file_txt ]
    ws_dict = { unidecode.unidecode(value): key  for key,value in dict2.items() }
    worksheets = { x[0]:x[1] for x in ws_dict.items() if x[0] in ws_exceptions }

    for x in file_txt:
        try:
            year = x.split("_")[0]
            ws_real = x.split("_")[-1]
            ws_compare = x.split("_")[-1].split(' ')[-1]
            for excel in excels:
                if year in excel:
                    df_sheet = pd.read_excel(excel, sheet_name=dict2[worksheets[ws_real]])
                    for exception in dict_exceptions:
                        if ws_compare in exception:
                            guide_dict_name = exception
                            df_dict = pd.read_excel(exception)
                    df_sheet = compare_len_columns(df_sheet=df_sheet,
                                                        df_dict=df_dict)
                    df_sheet.columns = treat_columns(df_dict)
                    df_sheet = filter_df(df_sheet)
                    df_sheet = treat_id(df_sheet)
                    df_name = export_df_name(path=file_dir,
                                             year = year,
                                             guide_dict_name=guide_dict_name)
                    df_sheet.to_csv(df_name,sep=';', encoding='utf-8')
            with open(f"{file_dir}{os.sep}log_dfs_sem_sucesso.txt", 'w') as log:
                log.write("Arquivo com nenhuma tabela a tratar.")
        except Exception as e:
            print(e)
            with open(f"{file_dir}{os.sep}log_dfs_sem_sucesso.txt", 'a') as log:
                log.write(f"\n{df_name}")
    return None

def treat_columns(df):
    """Função destinada a pegar os nomes das colunas informadas no dicionário.
       df: dado em dataframe destinado a ser transformado em csv."""
    try:
        columns = df["Nome da Variável"].tolist()
        columns_treated = []
        for column in columns:
            column = unidecode.unidecode(column).lower().strip()
            column = column.split()
            if '-' in column:
                column.remove('-')
            column = "_".join(column)
            columns_treated.append(column)
        return columns_treated  
    except Exception as e:
        print(e)
        return None

def treat_id(df):
    #df é o df_sheet
    if 'id' not in df.columns:
        df['id'] = df.index + 1
        columns = list(df)
        columns.insert(0, columns.pop(columns.index('id')))
        df = df.loc[:,columns]
        df.set_index('id')
    return df

def compare_len_columns(df_sheet, df_dict):
    """Função destinada a comparar o tamanho das colunas. O pandas interpretou algumas tabelas do .xlsx 
    com mais colunas do que realmente existem, como o caso da aba 1.34.
    df_sheet: dataframe da aba do excel original.
    df_dict: dataframe do dicionário com os nomes das colunas que irão ser alteradas posteriormente."""
    if len(df_dict['Nome da Variável'].tolist()) !=  len(df_sheet.columns):
        if len(df_dict['Nome da Variável'].tolist()) < len(df_sheet.columns):
            num = len(df_dict['Nome da Variável'].tolist()) - len(df_sheet.columns)
            df_correct_num_columns = df_sheet.iloc[: , : num]
        else:
            num = len(df_sheet.columns) - len(df_dict['Nome da Variável'].tolist())
            df_correct_num_columns = df_sheet.iloc[: , : num]
    else:
        df_correct_num_columns = df_sheet
    return df_correct_num_columns

def filter_df(df):
    """Função destinada a aplicar a selecionar as colunas desejadas, no caso, desejamos dados somente do Rio de Janeiro,
    portanto, filtramos os dados da coluna 'unidade_da_federacao'.
    df: dataframe que iremos filtrar, neste script, será sempre o df_sheet."""
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df.fillna("None", inplace=True)
    df['unidade_da_federacao'] = df['unidade_da_federacao'].apply(lambda x: str(x).strip())
    df = df[df["unidade_da_federacao"] != "None"]
    df = df[df['unidade_da_federacao'] == 'Rio de Janeiro']
    df = df.reset_index(drop=True)

    return df

def export_df_name(path, guide_dict_name, year):
    """Função necessária para termos o nome adequado e padronizado do arquivo .csv que desejamos.
    path: caminho para a pasta que iremos depositar os arquivos. É o mesmo caminho que 'file_dir' da função acima.
    guide_dict_name: nome filtrado do dicionário utilizado para filtrar as colunas.
    year: ano do arquivo excel que estamos tratando."""
    guide_dict_name = guide_dict_name.replace(os.sep, "_").replace('dicionarios_sinopse', 'sinopse_worksheet')
    if 'excecao' in guide_dict_name:
        guide_dict_name = guide_dict_name.split('_')
        guide_dict_name = "sinopse_worksheet_" + guide_dict_name[-1]
    df_name = year + "_" + guide_dict_name
    df_name = os.path.join(path,df_name)
    df_name = df_name.replace('.xlsx','.csv')
    df_name = unidecode.unidecode(df_name)
    print(f'\nWorksheet: {df_name}\n')

    return df_name

def export_worksheets_to_folder(path):
    """Função de transformação principal, onde acoplamos as funções anteriores. 
    Caso esta função não seja bem sucedida iremos utilizar a função 'treat_exceptions'.
    path: caminho onde iremos armazenar os arquivos transformados."""
    worksheets = list(dict2.values())
    guide_dicts = get_dict()
    excel_files = find_excels(aimed_dir='arquivos_extraidos')

    with open(f"{path}{os.sep}log_dfs_sem_sucesso.txt", 'w') as log:
        log.write("Arquivo de log para as tabelas nao executadas:")

    for excel in excel_files:
        year = excel.split('.xlsx')[0].split('_')[-1]

        for i in range(len(worksheets)):
            try:
                df_name = export_df_name(path=path,
                           guide_dict_name = guide_dicts[i],
                           year = year)
                df_name_verify = df_name.split(os.sep)[-1]
                if df_name_verify in os.listdir(path):
                    print(f'----- ARQUIVO JÁ EXISTENTE ------\n')
                    continue
                else:
                    print(f"------- TRANSFORMANDO CSV -------\n")
                    df_sheet = pd.read_excel(excel,sheet_name=dict2[i])
                    df_dict = pd.read_excel(guide_dicts[i])
                    df_sheet.columns  = treat_columns(df_dict)
                    df_sheet = filter_df(df_sheet)
                    df_sheet = treat_id(df_sheet)
                
                    df_sheet.to_csv(df_name,sep=';',encoding='utf-8-sig')

            except Exception as e:
                print(f"\n\n------------- ERROR: {e} ----------\n")
                print(f"---- Error in {excel}: {worksheets[i]}")
                print(f"----- TENTANDO NOVAMENTE ... ----")
                with open(f"{path}{os.sep}log_dfs_sem_sucesso.txt", 'a') as log:
                        log.write(f"\n{df_name_verify}")
    return None

def main():
    """função main em que organizamos as principais funções para serem executadas em uma ordem lógica com
    as principais variáveis utilizadas ao longo do programa. Algumas variáveis são reenforçadas, como uma
    boa prática, ao longo do script para que o garbage colector do python não apague-as."""
    dir = "sinopses_estatistica_educacao"
    cwd = os.getcwd()
    target_dir = os.path.join(cwd,dir)
    dict_dir='dicionarios_sinopse'

    #Criação de uma pasta onde os arquivos serão armazenadas caso não exista uma:
    if dir not in os.listdir():
        os.mkdir(target_dir)

    export_worksheets_to_folder(path = target_dir)
    with open(f"{target_dir}{os.sep}log_dfs_sem_sucesso.txt", 'r') as log:
        verify = log.readlines()
    
    while len(verify) >= 2:
        treat_exceptions(file_dir=target_dir,
                        dict_dir=dict_dir)
        with open(f"{target_dir}{os.sep}log_dfs_sem_sucesso.txt", 'r') as log:
            verify = log.readlines()
    if len(verify) <= 1:
        with open(f"{target_dir}{os.sep}log_dfs_sem_sucesso.txt", 'w') as log:
            log.write("Arquivo com nenhuma tabela a tratar.")
    return None

main()
