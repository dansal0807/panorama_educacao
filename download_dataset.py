import requests
import os
from zipfile import ZipFile
import pandas as pd
import ast
import openpyxl
from xls2xlsx import XLS2XLSX

dir = 'arquivos_extraidos'
cwd = os.getcwd()
aimed_dir = os.path.join(cwd,dir)

if dir not in os.listdir():
    os.mkdir(aimed_dir)

#abrir o arquivo de texto com os links
def open_txt(file_txt):
    print(f"----- ABERTURA DOS LINKS -----\n")
    with open(file_txt,'r') as links:
        links = links.readlines()
        zips = [ link.replace('\n','') for link in links ]
    for link in zips:
        if '.zip' not in link:
            zips.remove(link)
    return zips

#baixar os zips dos sites e extrai-los
def download_zips(zips):
    for link in zips:
        try:
            req = requests.get(link, verify=False)
            filename = link.split('/')[-1]
        except Exception as e:
            print(e)
            continue

        if filename not in os.listdir():
            with open(filename,'wb') as output_file:
                output_file.write(req.content)

        with ZipFile(filename, 'r') as zip:
            zip.extractall(aimed_dir)
    print(f"\n------ ARQUIVOS ZIP BAIXADOS ------\n ")

def remove_excess(aimed_dir, cwd):
    for root,dirs,files in os.walk(cwd, topdown=False):
        files = [ os.remove(file) for file in files if '.zip' in file]
    print(f"---------- ZIPS REMOVIDOS ------- \n")

#transformação de XLS para XLSX (formato moderno de excel)
def xls_to_xlsx(file):
    try:
        x2x = XLS2XLSX(file)
        os.remove(file)
    except Exception as e:
        print(e)


def main():
    print("----- CÓDIGO INICIADO ------\n")
    zips=open_txt('download_inep.txt')
    download_zips(zips)
    remove_excess(aimed_dir, cwd)
    print(f"------ DOWNLOADS CONCLUÍDOS -------")

main()
