import json
import os
from datetime import datetime
from os import listdir, chdir
from os.path import isfile, join


def indentar_por_n(n):
    archivos_normales = "./tweet_files"
    json_normales = [f for f in listdir(archivos_normales) if isfile(join(archivos_normales, f))]
    archivos_indentados = "./tweet_files_indent"
    print(f"Inicio: {datetime.now()}")
    for archivo in json_normales:
        with open(archivos_normales + "/" + archivo, "r", encoding="utf-8") as archivo_json:
            datos = json.load(archivo_json)
        with open(archivos_indentados + "/" + archivo, "w", encoding="utf-8") as archivo_json_indent:
            archivo_json_indent.write(json.dumps(datos, indent=n))
    print(f"Fin: {datetime.now()}")


def calcular_cantidad_de_tweets_en_carpeta(directorio="/tweet_files"):
    total_tweets = 0
    archivos_normales = directorio
    lista_archivos_json = [f for f in listdir(archivos_normales) if isfile(join(archivos_normales, f))]
    os.chdir(archivos_normales)

    for archivo in lista_archivos_json:
        with open(archivo) as archivo_json:
            datos = json.load(archivo_json)
            total_tweets += len(datos)

    print(f"Total de tweets en carpeta: {total_tweets}")


if __name__ == '__main__':
    calcular_cantidad_de_tweets_en_carpeta(directorio="/entrada")
