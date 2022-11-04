# Recoleccion-de-Tweets-e-Indices-Invertidos
Programa realizado como Trabajo Práctico de la cátedra 2021 de la materia Estructura de Datos, de la carrera Ingeniería en Computación de la Universidad Nacional de Tres de Febrero.

El mismo consiste en un programa que permite recolectar tweets a partir de una query usando la API de Twitter y almacenarlos en disco.
Luego, puede hacerse uso de Índices Invertidos para indexar toda la información y realizar consultas por medio de un buscador.

Para esta consigna de hizo uso de la herramienta TwitterAPI.

TwitterAPI: https://github.com/geduldig/TwitterAPI

Es menester para el funcionamiento del programa tener acceso a una cuenta de desarrollador de Twitter para poder acceder a la API. De lo contrario no funcionará.


## Consigna 2021:

En este TP vamos a investigar un tema que nos resulte interesante.

El primer paso consiste en seleccionar un tema de actualidad que les gustaría trabajar, por ejemplo elecciones 2021, vacunación contra el covid 2021, Pycon 2021, etc.

Pueden proponer un tema que sea de su interés. El principal requisito es que haya información en Twitter.

Una vez definido el tema deben recopilar información disponible en twitter y portales de noticias sobre el tema, para formar un corpus.

Se debe recopilar una cantidad considerable de información para armar nuestro corpus (idealmente 2 millones de tweets). 

Antes de empezar es necesario planificar el formato en que se va a guardar la información en crudo que se recopile
(por ejemplo formato de archivos JSON para Twitter y XML para RSS) que luego les permita procesarlos para formar índices invertidos.

### Primera parte

Código fuente recopilación de tweets:

* Debe permitir recopilar información del stream en vivo de Twitter y almacenarlos automáticamente en disco.
* El programa debe mostrar por pantalla su estado (fecha y hora de inicio, cantidad de tweets recolectados hasta el
momento, cantidad de bytes recolectados hasta el momento).
* Se debe detener cuando se presione Control-C.
* Para la primera entrega es necesario realizar las siguientes lecturas obligatorias y aplicar sus conceptos.

[Cómo construir una query en Twitter
](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query)

[Construcción de filtros de alta calidad para obtener datos de Twitter
](https://developer.twitter.com/en/docs/tutorials/building-high-quality-filtershttps://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query)

### Segunda parte

Se deberá programar un buscador (y un menú que permita operarlo) de información que permita resolver:

* Consultas por fechas y horas: por ejemplo los **'m'** primeros tweets de un usuario dado en un rango de fechas y horas. Los m primeros tweets de todos los usuarios en un rango de fechas y horas determinados (donde m es un parámetro de la búsqueda)
* Consultas de palabras o frases: se debe permitir consultas booleanas (con los operadores and, not y or) de palabras o frases. Estas consultas deben devolver los m primeros tweets correspondientes, donde m es un parámetro de la búsqueda. Por ejemplo: (“Del Potro” and “Murray” and not “Copa Davis”, 10) debería traer los 10 primeros tweets que mencionan a Del Potro y a Andy Murray y que no mencionen a la Copa Davis.

El buscador se deberá programar sobre uno o varios índices invertidos en disco.
