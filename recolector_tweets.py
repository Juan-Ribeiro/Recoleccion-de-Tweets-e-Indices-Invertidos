# Autor: Juan Ribeiro 2021

import json
import os
import time
import Constantes

import requests.exceptions
import urllib3.exceptions
from TwitterAPI import TwitterOAuth, TwitterAPI, OAuthType, TwitterRequestError, TwitterConnectionError, HydrateType
import datetime


def agregar_rule(RULE):
    """
    RULE: tipo String, una query de Twitter
    Envía una 'request' a la APP de API de Twitter para agregar una rule.
    """
    try:
        o = TwitterOAuth.read_file()
        api = TwitterAPI(o.consumer_key, o.consumer_secret, auth_type=OAuthType.OAUTH2, api_version="2")

        r = api.request("tweets/search/stream/rules", {"add": [{"value": RULE}]})
        print(f'[{r.status_code}] RULES: {json.dumps(r.json(), indent=2)}\n')
        if r.status_code != 201: exit()
    except TwitterConnectionError as e:
        print(e)
    except Exception as e:
        print(e)


def obtener_rules():
    """
    Envía una 'request' a la APP de API de Twitter para que devuelva todas las rules de la APP.
    Y las imprime en pantalla.
    """
    try:
        o = TwitterOAuth.read_file()
        api = TwitterAPI(o.consumer_key, o.consumer_secret, auth_type=OAuthType.OAUTH2, api_version="2")

        r = api.request("tweets/search/stream/rules", method_override="GET")
        print(f'[{r.status_code}] RULES: {json.dumps(r.json(), indent=2)}\n')
        if r.status_code != 200: exit()
        return r
    except TwitterConnectionError as e:
        print(e)
    except Exception as e:
        print(e)


def borrar_rules(LISTA_IDS):
    """
    LISTA_IDS: tipo List, debe contener IDs de rules en tipo Int
    Envía una 'request' a la APP de API de Twitter para borrar rules especificadas por 'LISTA_IDS'.
    """
    try:
        o = TwitterOAuth.read_file()
        api = TwitterAPI(o.consumer_key, o.consumer_secret, auth_type=OAuthType.OAUTH2, api_version="2")

        api.request("tweets/search/stream/rules", {"delete": {"ids": LISTA_IDS}})
    except TwitterConnectionError:
        exit()


def comenzar_stream():
    """
    Envía una 'request' a la APP de API de Twitter y comienza a recolectar tweets de acuerdo a las rules introducidas.
    Al interrumpirse, se guardan los datos en archivos en disco.
    """
    try:
        # Autenticar y crear objeto API
        o = TwitterOAuth.read_file()
        api = TwitterAPI(o.consumer_key, o.consumer_secret, auth_type=OAuthType.OAUTH2, api_version="2")

        # Comenzar el stream
        lista_tweets = {}
        tamanio_al_momento = 0
        contador_tweets = 0

        while True:
            try:
                r = api.request('tweets/search/stream', {
                    'expansions': "author_id",
                    'tweet.fields': "created_at",
                    'user.fields': "created_at",
                }, hydrate_type=HydrateType.APPEND)

                print(
                    f'[{r.status_code}] Comenzando stream el: [{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]')

                if r.status_code == 429 and len(lista_tweets) != 0:
                    raise KeyboardInterrupt

                if r.status_code != 200: exit()

                for tweet in r:
                    tweet = tweet["data"]
                    id_tweet = tweet["id"]

                    # Reemplazar hora de huso 0 a -3
                    hora = tweet["created_at"]
                    hora = datetime.datetime.strptime(hora, '%Y-%m-%dT%H:%M:%S.000Z')
                    nueva_hora = hora - datetime.timedelta(hours=3)
                    timestamp_str = nueva_hora.strftime("%Y-%m-%d %H:%M:%S")
                    tweet["created_at"] = timestamp_str

                    # Guardar en diccionario de tweets
                    lista_tweets.setdefault(id_tweet, tweet)

                    # Tamaño de tweet en bytes
                    tweet_formt_json = json.dumps(tweet)
                    tamanio_tweet = len(tweet_formt_json.encode("utf-8")) + len(
                        id_tweet) + Constantes.BYTES_EXTRA_X_TWEET_JSON

                    tamanio_al_momento += tamanio_tweet

                    # Actualizar contador de tweets
                    contador_tweets += 1

                    # Imprimir mensajes de estado actual
                    print(f'Tweets encontrados hasta el momento: {contador_tweets}')
                    print(f'Tweet recibido el: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                    print(f'Peso del tweet en bytes: {tamanio_tweet} bytes.')
                    print(f'Peso en bytes al momento: {tamanio_al_momento} bytes.\n')

            # Posibles casos que lleven a la interrupción de la ejecución
            except KeyboardInterrupt or TwitterConnectionError or urllib3.exceptions.ReadTimeoutError or urllib3. \
                    exceptions.ProtocolError or ConnectionResetError:
                # Guardar diccionario en archivo .json
                if len(lista_tweets) != 0:
                    archivo = "Tweets_Recolectados_" + str(
                        datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")) + ".json"
                    os.chdir("tweet_files")
                    with open(archivo, "w", encoding="utf-8") as archivo_json:
                        archivo_json.write(json.dumps(lista_tweets, sort_keys=True))

                        # Calcular bytes extra de almacenamiento
                        # Bytes extra al guardar = 2 * (contador_tweets - 1) + corchetes de apertura y cierre
                        tamanio_final = tamanio_al_momento + Constantes.BYTES_EXTRA_X_TWEET_JSON * (
                                contador_tweets - 1) + Constantes.CORCHETES_APERTURA_CIERRE_JSON

                    print(f'\nStream terminado el: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                    print(f'Tweets recolectados: {contador_tweets}')
                    print(f'Tamaño antes del almacenamiento: {tamanio_al_momento} bytes.')
                    print(f'Tamaño extra estimado calculado: {tamanio_final} bytes.\n')
                    print(f'Tamaño final del archivo: {os.path.getsize(archivo)} bytes.')
                else:
                    print(f'\nStream terminado el: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                    print("Interrumpido antes de recolectar tweets. No se guarda archivo.")
                break

            except requests.exceptions.ConnectTimeout:
                time.sleep(900)
                continue

            except TwitterRequestError as e:
                print(f'\n{e.status_code}')
                for msg in iter(e):
                    print(msg)
                if e.status_code < 500:
                    raise
                else:
                    continue

            except Exception as e:
                print(e)

    except TwitterRequestError as e:
        print(f'\n{e.status_code}')
        for msg in iter(e):
            print(msg)

    except TwitterConnectionError as e:
        print(e)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    query = "hola"
    agregar_rule(query)
    obtener_rules()
    comenzar_stream()
