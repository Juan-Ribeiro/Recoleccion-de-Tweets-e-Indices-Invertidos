import json
import os
import string
from datetime import datetime

from nltk.stem import SnowballStemmer
from nltk.corpus import stopwords


class IndiceInvertidoTweets:
    def __init__(self, archivos, salida, temp="./temp", blocksize=102400, language='spanish'):
        """
        :param archivos: carpeta con archivos de tweets a indexar
        :param salida: carpeta donde se guardarán los índices invertidos
        """
        self.archivos = archivos
        self.salida = salida
        self._blocksize = blocksize
        self._temp = temp
        self._stop_words = frozenset(stopwords.words(language))
        self._stemmer = SnowballStemmer(language, ignore_stopwords=False)

        self._term_to_termID = {}
        self._user_to_userID = {}
        self._tweets_de_cada_userID = {}
        self._tweetID_in_fileID = {}
        self._tweetID_to_fecha = {}

        self.__generar_tweetID()
        self.__indexar()

    def __generar_tweetID(self):
        file_to_fileID = {}
        fileID_to_file = {}
        lista_archivos_json = [os.path.join(self.archivos, nombre_json) for nombre_json in os.listdir(self.archivos) if
                               os.path.isfile(os.path.join(self.archivos, nombre_json))]

        for i in range(len(lista_archivos_json)):
            file_to_fileID[lista_archivos_json[i]] = i
            fileID_to_file[i] = lista_archivos_json[i]

        self._lista_archivos = lista_archivos_json
        self._file_to_fileID = file_to_fileID
        self._fileID_to_file = fileID_to_file

    def __lematizar(self, palabra):
        """ Usa el stemmer para lematizar o recortar la palabra, previamente elimina todos
                los signos de puntuación que pueden aparecer. El stemmer utilizado también se
                encarga de eliminar acentos y pasarlo los términos a minúscula, sino habría que hacerlo
                a mano"""

        palabra = palabra.strip(string.punctuation + "»" + "\x97" + "¿" + "¡" + "\u201c" + \
                                "\u201d" + "\u2014" + "\u2014l" + "\u00bf")

        palabra_lematizada = self._stemmer.stem(palabra)
        return palabra_lematizada

    def __indexar(self):
        n = 0
        lista_bloques = []
        for bloque in self.__parse_next_block():
            bloque_invertido = self.__invertir_bloque(bloque)
            lista_bloques.append(self.__guardar_bloque_intermedio(bloque_invertido, n))
            n += 1

        # Se guardan todos los diccionarios en disco
        self.__guardar_diccionario_en_disco(self._term_to_termID, "diccionario_terminos.json")
        self.__guardar_diccionario_en_disco(self._file_to_fileID, "diccionario_archivos.json")
        self.__guardar_diccionario_en_disco(self._fileID_to_file, "diccionario_archivos_inverso.json")
        self.__guardar_diccionario_en_disco(self._user_to_userID, "diccionario_usuarios.json")
        self.__guardar_diccionario_en_disco(self._tweetID_to_fecha, "diccionario_fecha_por_tweet.json")
        self.__guardar_diccionario_en_disco(self._tweets_de_cada_userID, "diccionario_tweets_por_usuario.json")
        self.__guardar_diccionario_en_disco(self._tweetID_in_fileID, "diccionario_apariciones_tweets_por_archivo.json")

        print(f"\nComenzando a intercalar bloques{datetime.now()}")
        self.__intercalar_bloques(lista_bloques, terminos_x_bloque=5000)
        print(f"\nFinalizado intercalar bloques{datetime.now()}")

    def __parse_next_block(self):
        n = self._blocksize
        termID = 0
        userID = 0
        bloque = []
        for archivo in self._lista_archivos:
            with open(archivo, "r", encoding="utf-8") as archivo_json:
                # Se levanta el archivo en memoria
                tweets = json.load(archivo_json)
                for tweetID, tweet_data in tweets.items():

                    # Asignamos un puntero al usuario, así como una lista que contendrá cada uno de sus tweets
                    username = tweet_data["author_id_hydrate"]["username"]
                    if username not in self._user_to_userID:
                        self._user_to_userID[username] = userID
                        self._tweets_de_cada_userID[userID] = []

                    # Agregamos el ID del tweet a la lista del usuario
                    self._tweets_de_cada_userID[self._user_to_userID[username]].append(tweetID)

                    # Aumentar contador
                    if username in self._user_to_userID:
                        userID += 1

                    # Guardamos la fecha de publicación del tweet
                    self._tweetID_to_fecha[tweetID] = tweet_data["created_at"]

                    # Guardamos en qué archivo aparece el tweet
                    self._tweetID_in_fileID[tweetID] = self._file_to_fileID[archivo]

                    # Ahora, empezamos a tratar las apariciones de términos
                    texto = tweet_data["text"]
                    n -= len(texto.encode("utf-8"))
                    palabras = texto.split()

                    for pal in palabras:
                        pal = self.__lematizar(pal)
                        if pal not in self._term_to_termID:
                            self._term_to_termID[pal] = termID
                            termID += 1
                        bloque.append((self._term_to_termID[pal], tweetID))
                    if n <= 0:
                        yield bloque
                        n = self._blocksize
                        bloque = []
                yield bloque

    def __invertir_bloque(self, bloque):
        bloque_invertido = {}
        bloque_ordenado = sorted(bloque, key=lambda tupla: (tupla[0], tupla[1]))
        for par in bloque_ordenado:
            posting = bloque_invertido.setdefault(par[0], set())
            posting.add(par[1])
        return bloque_invertido

    def __guardar_bloque_intermedio(self, bloque, nro_bloque):
        archivo_salida = "b" + str(nro_bloque) + ".json"
        archivo_salida = os.path.join(self._temp, archivo_salida)
        bloque = sorted(bloque)
        for clave in bloque:
            bloque[clave] = list(bloque[clave])
        with open(archivo_salida, "w") as contenedor:
            json.dump(bloque, contenedor)
        return archivo_salida

    def __guardar_diccionario_en_disco(self, diccionario, nombre_archivo):
        path = os.path.join(self.salida, nombre_archivo)
        with open(path, "w") as contenedor:
            json.dump(diccionario, contenedor, indent=2)

    def __intercalar_bloques(self, temp_files, terminos_x_bloque=100):
        lista_termID = [str(i) for i in range(len(self._term_to_termID))]
        posting_file = os.path.join(self.salida, "postings.json")

        open_files = [open(f, "r") for f in temp_files]

        with open(posting_file, "w", encoding="utf-8") as salida:

            cant_terminos = len(lista_termID)

            for i in range(0, cant_terminos, terminos_x_bloque):
                inicial = i

                final = i + (terminos_x_bloque if (i + terminos_x_bloque) <= cant_terminos
                             else cant_terminos % terminos_x_bloque)
                lista_corta = lista_termID[inicial:final]

                for un_file in open_files:
                    un_file.seek(0)
                    bloque = json.load(un_file)
                    postings = {}
                    for termino in lista_corta:
                        posting = set()
                        if termino in bloque.keys():
                            apariciones = bloque[termino]
                        try:
                            posting = posting.union(set(apariciones))
                            postings.setdefault(termino, posting)
                        except:
                            pass

                for posting in postings.values():
                    json.dump(list(posting), salida)
                    salida.write("\n")


if __name__ == '__main__':
    print(f"Inicio: {datetime.now()}")
    IndiceInvertidoTweets("./entrada", "./salida", blocksize=6553600)  # Bloques de 64kb, cambiar si es necesario
    print(f"Fin: {datetime.now()}")
