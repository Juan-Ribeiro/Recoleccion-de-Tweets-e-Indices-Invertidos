import json
import string
import time
import re

from nltk.stem import SnowballStemmer
from nltk.corpus import stopwords

stop_words = frozenset(stopwords.words("spanish"))
stemmer = SnowballStemmer("spanish", ignore_stopwords=False)


class BuscadorException(Exception):
    # Excepción usada para regresar al menú de inicio del buscador.
    pass


class UsuarioInexistenteException(BuscadorException):
    def __init__(self):
        super().__init__("El usuario ingresado no existe.")


class OperacionInvalidaException(BuscadorException):
    def __init__(self):
        super().__init__('El operador introducido no es válido. Debe ser "and, "or" o "and not"')


class MatchInvalidoException(BuscadorException):
    def __init__(self):
        super().__init__('La consulta introducida es inválida. Revisar formato.\n')


class FechaInvalidaException(BuscadorException):
    def __init__(self):
        super().__init__('Formato de fecha introducido inválido.\n')


class Buscador:
    def __init__(self, modo_pruebas=False):
        """
        :param modo_pruebas: Si es False, se accede inmediatamente al Menú del Buscador.
        Si es True, permite instanciar con el fin de hacer pruebas con los métodos de la clase.
        """
        if not modo_pruebas:
            self.__iniciar_menu_buscador()

    def __iniciar_menu_buscador(self):
        print(10 * "-", "BIENVENIDO", 10 * "-")
        while True:
            print("Seleccione una de las siguientes opciones:\n")
            print("1 - Obtener los tweets de un usuario en un rango de fechas.")
            print("2 - Consulta booleana de palabras.")
            print("0 - Salir")

            try:
                opcion = input()

                if opcion == "1":
                    usuario = input("Ingrese el 'username' del usuario de Twitter: ")
                    fecha_inicial = input("Ingrese el rango inicial de fechas (En formato 'AA-MM-DD HH:MM:SS') : ")
                    fecha_final = input("Ingrese el rango final de fechas (En formato 'AA-MM-DD HH:MM:SS') : ")
                    cantidad = int(input("Ingrese cantidad máxima de tweets: "))

                    self.__menu_opcion_1(usuario, fecha_inicial, fecha_final, cantidad)

                elif opcion == "2":
                    print('Búsqueda booleana. Términos entre comillas. Si se busca más de un término, intercalar uno '
                          'de los siguientes operadores: and, or, and not')
                    print("Ingrese la consulta a realizar: ")

                    consulta = input()
                    cantidad = int(input("Ingrese cantidad máxima de tweets: "))

                    self.__menu_opcion_2(consulta, cantidad)
                elif opcion == "0":
                    print("Nos vemos.")
                    exit()
                else:
                    raise BuscadorException("Opción inválida. Intente nuevamente.\n")
            except BuscadorException as e:
                print(e)
                continue
            except KeyboardInterrupt:
                print("Nos vemos.")
                exit()

    def __menu_opcion_1(self, usuario, fecha_inicial, fecha_final, cantidad=None):
        fecha_inicial, fecha_final = self.verificar_fechas(fecha_inicial, fecha_final)

        # Buscar ID del usuario
        usuario_id = self.obtener_id_de_usuario(usuario)

        # Buscar tweets del ID
        lista_tweetIDs = self.obtener_tweets_de_usuario_id(usuario_id)

        # Discriminamos solo los tweets dentro de las fechas dadas
        lista_filtrada = self.filtrar_tweets_entre_fechas(lista_tweetIDs, fecha_inicial, fecha_final)

        # Buscamos los archivos en donde aparecen los tweets
        lista_docIDs = self.obtener_apariciones_de_tweets(lista_filtrada)

        # Buscamos los nombres de los archivos a consultar
        lista_docs = self.obtener_lista_de_documentos(lista_docIDs)

        # Extraemos los tweets de los archivos
        lista_tweets = self.obtener_tweets_desde_archivos(lista_tweetIDs, lista_docs)

        # Limitamos la lista de tweets a la cantidad dada, si es necesario
        lista_tweets = self.truncar_cantidad_de_tweets(lista_tweets, cantidad)

        # Se imprimen los tweets extraídos
        [print(tweet) for tweet in lista_tweets]
        print(f"\nTweets encontrados: {len(lista_tweets)}\n")

    def __menu_opcion_2(self, consulta, cantidad=100, expresion_regular=r'(?:and not|and|or)|\"\w+(?:\s\w*)*\"'):
        match = re.findall(expresion_regular, consulta)

        # Verificamos que los resultados obtenidos sean válidos.
        self.verificar_match(match)

        # Buscar los IDs de los términos encontrados en el match
        ids = self.obtener_id_de_terminos(match)

        # Buscar apariciones de los IDs en archivo 'postings'

        # Ej: "Argentina" and "Gol" and not "En contra"
        # match = ["Argentina", and, "Gol", and not, "En contra"]
        # Indices pares: términos
        # Indices impares: operadores
        # Debería resultar, por ej:
        # [[1, 2, 3], and, [2, 3, 4], and not, [3, 4, 5]]
        match = self.obtener_apariciones_de_terminos(match, ids)

        # Hacemos operaciones de conjuntos
        # 'and' -> .intersection()
        # 'or' -> .union()
        # 'and not' -> .difference()
        lista_tweetIDs = self.realizar_operaciones_sobre_match(match)

        # Buscamos los archivos en donde aparecen los tweets
        lista_docIDs = self.obtener_apariciones_de_tweets(lista_tweetIDs)

        # Buscamos los nombres de los archivos a partir de los ids
        lista_docs = self.obtener_lista_de_documentos(lista_docIDs)

        # Buscamos los tweets en los archivos
        lista_tweets = self.obtener_tweets_desde_archivos(lista_tweetIDs, lista_docs)

        # Limitamos la lista de tweets a la cantidad dada, si es necesario
        lista_tweets = self.truncar_cantidad_de_tweets(lista_tweets, cantidad)

        [print(tweet) for tweet in lista_tweets]
        print(f"\nTweets encontrados: {len(lista_tweets)}\n")

    ####################################################################################################################
    # MÉTODOS DE FUNCIONAMIENTO #
    ####################################################################################################################

    def verificar_match(self, match):

        # Se verifica que el match recibido de la expresión regular sea válido.
        if len(match) == 0 or len(match) % 2 != 1:
            raise MatchInvalidoException

        it_match = iter(match)
        operadores = ["and", "or", "and not"]
        while True:
            try:
                if (next(it_match) in operadores) or (next(it_match) not in operadores):
                    raise MatchInvalidoException
            except StopIteration:
                break

    def verificar_fechas(self, fecha_inicial, fecha_final):
        # Si alguna de las fechas no se especifica, es decir, está vacía, se asigna un valor por default
        try:
            if fecha_inicial == "":
                fecha_inicial = time.strptime("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            if fecha_final == "":
                fecha_final = time.strptime("2050-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            else:
                fecha_inicial = time.strptime(fecha_inicial, "%Y-%m-%d %H:%M:%S")
                fecha_final = time.strptime(fecha_final, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise FechaInvalidaException
        return fecha_inicial, fecha_final

    def obtener_id_de_usuario(self, usuario, file_usuarios="./salida/diccionario_usuarios.json"):
        # Se busca el 'userID' del 'usuario' recibido en el índice de usuarios.
        with open(file_usuarios, "r") as archivo_usuario:
            datos = json.load(archivo_usuario)
            try:
                usuario_id = datos[usuario]
            except KeyError:
                raise UsuarioInexistenteException

            return usuario_id

    def obtener_tweets_de_usuario_id(self, usuario_id,
                                     file_tweets_usuario="./salida/diccionario_tweets_por_usuario.json"):
        # Se busca la lista de 'tweet_id's del usuario en su respectivo diccionario
        with open(file_tweets_usuario, "r") as archivo_t_por_us:
            datos = json.load(archivo_t_por_us)
            try:
                lista_tweetIDs = datos[str(usuario_id)]
                return lista_tweetIDs
            except Exception as e:
                raise e

    def obtener_apariciones_de_tweets(self, lista, file_apariciones_tweets=
    "./salida/diccionario_apariciones_tweets_por_archivo.json"):

        # Se busca la lista de 'docID' en el índice de tweets por archivo
        lista_docIDs = []
        with open(file_apariciones_tweets) as archivo_apariciones_tweets:
            datos = json.load(archivo_apariciones_tweets)
            for tweetID in lista:
                if datos[str(tweetID)] not in lista_docIDs:
                    lista_docIDs.append(datos[tweetID])
        return lista_docIDs

    def filtrar_tweets_entre_fechas(self, lista_tweetIDs, fecha_inicial, fecha_final,
                                    file_fechas_tweets="./salida/diccionario_fecha_por_tweet.json"):
        lista_filtrada = []
        with open(file_fechas_tweets, "r") as archivo_fechas_tweets:
            datos = json.load(archivo_fechas_tweets)

            for tweetID in lista_tweetIDs:
                fecha = time.strptime(datos[tweetID], "%Y-%m-%d %H:%M:%S")
                if fecha_inicial <= fecha <= fecha_final:
                    lista_filtrada.append(tweetID)
        return lista_filtrada

    def obtener_lista_de_documentos(self, lista_docIDs,
                                    file_archivos_inverso="./salida/diccionario_archivos_inverso.json"):
        lista_docs = []
        with open(file_archivos_inverso, "r") as archivo_archivos_inverso:
            datos = json.load(archivo_archivos_inverso)
            for docID in lista_docIDs:
                lista_docs.append(datos[str(docID)])
        return lista_docs

    def obtener_tweets_desde_archivos(self, lista_tweetIDs, lista_docs):
        lista_tweets = []
        open_files = [open(f, "r") for f in lista_docs]
        for tweetID in lista_tweetIDs:
            for open_file in open_files:
                open_file.seek(0)
                datos = json.load(open_file)
                try:
                    lista_tweets.append(json.dumps(datos[str(tweetID)], indent=2))
                    break
                except:
                    pass
        for f in open_files:
            f.close()
        return lista_tweets

    def obtener_id_de_terminos(self, match):
        ids = {}

        with open("./salida/diccionario_terminos.json", "r") as contenedor:
            datos = json.load(contenedor)
            for x in range(0, len(match), 2):
                palabra = match[x]
                palabra_lematizada = self.__lematizar_palabra(palabra)
                if palabra_lematizada in datos:
                    id_palabra = datos[palabra_lematizada]
                    ids[id_palabra] = palabra
                else:
                    print(f'La palabra: "{palabra}" no existe en el diccionario.')

        return ids

    def obtener_apariciones_de_terminos(self, match, ids):
        with open("./salida/postings.json", "r") as contenedor:
            encontrados = 0
            for i, linea in enumerate(contenedor):
                if i in ids:
                    match[match.index(ids[i])] = set(json.loads(linea))
                    encontrados += 1
                if encontrados == len(ids):
                    break
        return match

    def __buscar_palabra(self, palabra):
        palabra_lematizada = self.__lematizar_palabra(palabra)
        with open("salida/diccionario_terminos.json", "r", encoding="utf-8") as archivo_json:
            diccionario = json.load(archivo_json)
            if palabra_lematizada in diccionario:
                id_pal = diccionario[palabra_lematizada]
                return str(id_pal)
            else:
                print(f"La palabra: {palabra} no está en el diccionario de términos.\n")

    def __lematizar_palabra(self, palabra):
        """ Usa el stemmer para lematizar o recortar la palabra, previamente elimina todos
                los signos de puntuación que pueden aparecer. El stemmer utilizado también se
                encarga de eliminar acentos y pasar toda la palabra a minúscula, sino habría
                que hacerlo a mano"""
        # palabra = palabra.decode("utf-8", ignore).encode("utf-8")
        palabra = palabra.strip(string.punctuation + "»" + "\x97" + "¿" + "¡" + "\u201c" + \
                                "\u201d" + "\u2014" + "\u2014l" + "\u00bf")
        # "\x97" representa un guión
        return stemmer.stem(palabra)

    def realizar_operaciones_sobre_match(self, match):
        iter_match = iter(match)
        lista_tweetIDs = next(iter_match)
        while True:
            try:
                lista_tweetIDs = self.efectuar_operacion(lista_tweetIDs, next(iter_match), next(iter_match))
            except StopIteration as e:
                print(e)
                break
        return sorted(lista_tweetIDs, key=lambda x: x)

    def efectuar_operacion(self, conjunto_a, operador, conjunto_b):
        operador.strip()
        if operador == "and":
            return conjunto_a.intersection(conjunto_b)
        elif operador == "or":
            return conjunto_a.union(conjunto_b)
        elif operador == "and not":
            return conjunto_a.difference(conjunto_b)
        else:
            raise OperacionInvalidaException

    def truncar_cantidad_de_tweets(self, lista_tweets, cantidad):
        if cantidad < len(lista_tweets) and cantidad is not None:
            lista_tweets = lista_tweets[-cantidad:]
        return lista_tweets


if __name__ == '__main__':
    Buscador()
