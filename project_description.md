# Projekt: Wycieczki.

Twoim zadaniem jest zaimplementowanie zdefiniowanego poniżej API.

Ze względu na to, że interesuje nas przede wszystkim tematyka baz danych kolejne wywołania funkcji API należy wczytywać ze standardowego wejścia, a wyniki zapisywać na standardowe wyjście.

## Uwaga
W przykładach była zamieniona długość i szerokość geograficzną, błąd poprawiłem ale efektem poprawki jest zmiana odległości w oczekiwanym wyjściu w przykładzie.

## Pytania i odpowiedzi

> Powiedzmy, że pewien rowerzysta pokonuje trasę [1, 2, 3] zaczynając dnia 23-06-2020. Rozumiem, że oznacza to że nocuje 23-06-2020 w punkcie nr 2 (wnioskując po testach przykładowych). 
Tak,  i to jest jego jedyny nocleg - nie wnikamy co robi przed rozpoczęciem wycieczki i po zakończeniu.

## Opis systemu

Napisz system ułatwiający prowadzenie firmy organizującej wycieczki rowerowe. Firma obsługuje wiele punktów, w których zaczynają się i kończą etapy poszczególnych wycieczek.
Pomiędzy punktami klienci podróżują na własną rękę. 
Trasa wycieczki to lista punktów pomiędzy jej etapami. Każda wycieczka rozpoczyna się pierwszego dnia rano w pierwszym punkcie trasy. 
Następnie klient ma cały dzień na dojechanie do kolejnego punktu trasy, w pobliżu którego spędza noc.
Rano następnego dnia rozpoczyna w tym samym punkcie kolejny etap i tak aż dojedzie do ostatniego punktu trasy.
Uwaga - trasa nie obejmuje punktów przez które klient być może przejeżdża w trakcie etapu ale tam nie nocuje.
Wszystkie trasy są skatalogowane. Klienci mają zawsze cały dzień na przejechanie jednego etapu więc rezerwując wycieczkę podaje się datę jej rozpoczęcia oraz wersję trasy z katalogu.


## Technologie

System Linux. Język programowania - python. Baza danych – PostgreSQL. Testy przeprowadzałem na komputerze z Ubuntu 18.04.4 LTS, PostgreSQL 10, PostGIS 2.4 (domyślne wersje w Ubuntu 18.04.4).
Pakiety `postgresql-10` oraz `postgis`. W celu instalacji PostGIS z domyslnego pakietu musiałem zrobić upgrade starego PostgreSQL do wersji 10: https://stackoverflow.com/questions/47029055/how-do-i-upgrade-my-postgresql-9-5-to-postgresql-10-on-ubuntu-16-04
(dokładne wersje PostgreSQL i PostGIS nie mają znaczenia, muszą jednak do siebie _pasować_, w szczególności być zainstalowane w odpowiednich katalogach).

##  Przechowywanie danych geograficznych

Aby uzyskać pełną liczbę punktów za zadanie użyj rozszerzenia PostGIS, a długość i szerokość geograficzną przechowuj z użyciem typu `geography`.
Alternatywnie możesz użyć innych sposobów np. założyć, że Ziemia jest idealną kulą lub, że jest płaska. 
Użyj odpowiedniego indeksu. Dokumentacja PostGIS 2.4: https://postgis.net/docs/manual-2.4/using_postgis_dbmanagement.html#PostGIS_Geography

Przykład obliczania odległości z użyciem funkcji `ST_Distance`, nie zakłada, że Ziemia jest idealną kulą.

```
select ST_Distance('SRID=4326;POINT(51.107883 17.038538)'::geography,'SRID=4326;POINT(50.671062 17.926126)'::geography, true)
   st_distance
-----------------
 108637.97748665
(1 row)
```
Inne przydatne fragmenty kodu:
```
piotrek=# create table tab (name text, geog geography);
piotrek=# create index on tab using Gist (geog);
piotrek=# insert into tab  values ('wroclaw', 'SRID=4326;POINT(17.038538 51.107883)');
piotrek=# insert into tab  values ('opole', 'SRID=4326;POINT(17.926126 50.671062)');
piotrek=# select name, ST_AsText(geog) from tab limit 5;             
  name   |         st_astext          
---------+----------------------------
 wrocław | POINT(17.038538 51.107883)
 opole   | POINT(17.926126 50.671062)
 
piotrek=# SELECT name FROM tab WHERE ST_DWithin(geog, ST_GeographyFromText('SRID=4326;POINT(17.038538 51.107883)'), 80000);
  name   
---------
 wroclaw
 opole
```

## Punktacja

Maksymalna liczba punktów: **100 pkt.**, 
Uwaga: 
* oceny poniżej 50 pkt. będą obcinane do 0 pkt.,
* nie ma wymogu uzyskania 50% punktów za projekt.

Punktacja:
- Przygotowanie modelu konceptualnego: **do 20 pkt.** 
- Implementacja funkcji `node`, `catalog`, `trip`: **10 pkt.** (muszą być zaimplementowane).
- Implementacja funkcji `closest_nodes`, `party`, `guests`, `cyclists` : **do 40 pkt. (po 10 pkt)**

Punkty można dostać wyłącznie za funkcje, które można przetestować (tzn. aby otrzymać punkty za funkcję `closest_points` funkcja `node` też musi być zaimplementowana).

- Przechowywanie danych geograficznych za pomocą typu `geography` rozszerzenia PostGIS, poprawne ich wykorzystanie do implementacji funkcji `closest_nodes`, `party`, `cyclists`, 
obliczanie odległości z poziomu BD - bez zakładania płaskości lub idealnej kulistości Ziemi, odpowiednie indeksowanie wyszukiwań: **30 pkt.** (punkty będą przyznane wyłącznie za spełnienie wszystkich wymienionych wymagań!).

  W przeciwnym przypadku - tj. spełnienie jedynie niektórych wymienionych powyżej wymagań, wykorzystanie innych (poprawnych) sposobów, np. modułu `earthdistance`, obliczanie odległości na poziomie aplikacji (czyli bez indeksowania), _haversine formula_ itp.: **do 10 pkt.**

## Implementacja

Twój program po uruchomieniu powinien przeczytać ze standardowego wejścia ciąg wywołań funkcji API, a wyniki ich działania wypisać na standardowe wyjście.

Wszystkie dane powinny być przechowywane w bazie danych, efekt działania każdej funkcji modyfikującej bazę, dla której wypisano potwierdzenie wykonania (wartość OK) powinien być utrwalony. 
**Dane dostępowe do bazy danych**: baza danych: `student`, login: `app`, password: `qwerty`.

Program będzie uruchamiany wielokrotnie z następującymi parametrami:

- pierwsze uruchomienie - program wywołany z parametrem `--init`

Wejście puste. 

- kolejne uruchomienia

Wejście zawiera wywołania kolejnych funkcji API.

## Dodatkowe informacje i założenia 
- Można założyć, że przed uruchomieniem z parametrem `--init` baza nie zawiera jakichkolwiek tabel.
- Baza danych `student` oraz użytkownik `app` z hasłem `qwerty` będą istnieli w momencie pierwszego uruchomienia bazy, dostępne będzie również rozszerzenie PostGIS (zainstalowany pakiet `postgis` oraz wydane polecenie `create extension postgis`).
- Przy pierwszym uruchomieniu program powinien utworzyć wszystkie niezbędne elementy bazy danych (tabele, więzy, funkcje, wyzwalacze) zgodnie z przygotowanym przez studenta modelem fizycznym.
- Baza nie będzie modyfikowana pomiędzy kolejnymi uruchomieniami.
- Program nie będzie miał praw do tworzenia i zapisywania jakichkolwiek plików. 
- Program będzie mógł czytać pliki z bieżącego katalogu (np. dołączony do rozwiązania studenta plik .sql zawierający polecenia tworzące niezbędne elementy bazy).
- Wszystkie odległości wypisz po zaookrągleniu do pełnego metra (funkcja `round`).

## Format wejścia

Każda linia pliku wejściowego zawiera obiekt JSON (http://www.json.org/json-pl.html). Każdy z obiektów opisuje wywołanie jednej funkcji API wraz z argumentami.

Przykład: 

Obiekt
```
{ "function": "node", "body": { "node": 12345, "lat": 51.111044, "lon": 17.053423, "description": "a nice place to relax, strongly recommended"}}
```
oznacza wywołanie funkcji o nazwie `node` z argumentem `node` przyjmującym wartość `12345`, argumentami `lat` i `lon` przyjmującymi wartości odpowiednio 51.111044 oraz 17.053423 oraz `description` – wartość `a nice place to relax, strongly recommended`.


## Format wyjścia

Dla każdego wywołania wypisz w osobnej linii obiekt JSON zawierający obiekt z polami: status (zwracane zawsze), data (tylko dla funkcji zwracających krotki), debug (opcjonalnie). 

Wartość pola status to "OK" albo "ERROR".

Tabela `data` zawiera krotki wynikowe. Każda krotka to obiekt zawierający wartości wszystkich podanych w specyfikacji atrybutów.

Dopuszczalne jest dodatkowe pole o kluczu `debug` i wartości typu `string` z ew. informacją przydatną w debugowaniu (jest ona całkowicie dobrowolna i będzie ignorowana w czasie testowania, powinna mieć niewielki rozmiar).


## Przykładowe wejście i wyjście

Pierwsze uruchomienie (z parametrem `--init`): wejście puste (pusty plik).

###### Oczekiwane wyjście
```
{"status": "OK"}
```

###### Kolejne uruchomienie:
```
{ "function": "closest_nodes", "body": { "ilat": 51.107883, "ilon": 17.038538}}
{ "function": "node", "body": { "node": 12345, "lat": 51.111044, "lon": 17.053423, "description": "a nice place to relax, strongly recommended"}}
{ "function": "node", "body": { "node": 12346, "lat": 51.198127, "lon": 16.919484, "description": "another nice place, is a must-see"}}
{ "function": "catalog", "body": { "version": 1, "nodes": [12345, 12345, 12346, 12345]}}
{ "function": "catalog", "body": { "version": 2, "nodes": [12346, 12345, 12346]}}
{ "function": "trip", "body": { "cyclist": "piotrek", "date": "2020-06-16", "version": 1}}
{ "function": "trip", "body": { "cyclist": "paweł", "date": "2020-06-16", "version": 1}}
{ "function": "trip", "body": { "cyclist": "janek", "date": "2020-06-15", "version": 2}}
{ "function": "closest_nodes", "body": { "ilat": 51.107883, "ilon": 17.038538}}
{ "function": "party", "body": { "icyclist": "piotrek", "date": "2020-06-17"}}
{ "function": "guests", "body": { "node": 12345, "date": "2020-06-16"}}
{ "function": "cyclists", "body": { "limit": 2}}
```

###### Oczekiwane wyjście (dla czytelności zawiera znaki nowej linii)
```
{"status": "OK", "data": []}
{"status": "OK"}
{"status": "OK"}
{"status": "OK"}
{"status": "OK"}
{"status": "OK"}
{"status": "OK"}
{"status": "OK"}
{"status": "OK", "data": [
  {"node": 12345, "olat": 51.111044, "olon": 17.053423, "distance": 1100},
  {"node": 12346, "olat": 51.198127, "olon": 16.919484, "distance": 13045}
]}
{"status": "OK", "data": [
  {"ocyclist": "paweł", "node": 12346, "distance": 0}
]}
{"status": "OK", "data": [
  {"cyclist": "paweł"},
  {"cyclist": "piotrek"}
]}
{"status": "OK", "data": [
  {"cyclist": "janek", "no_trips": 1, "distance": 26957,
  {"cyclist": "paweł", "no_trips": 1, "distance": 26957}
]}
```

## Format opisu API

```
<function> <arg1> <arg2> … <argn> // nazwa funkcji oraz nazwy jej argumentów
```
opis działania funkcji

```
// lista atrybutów wynikowych tabeli data lub informacja o braku tego pola
```

## Wywołania API

Identyfikatory `<cyclist>` typu string oraz identyfikatory `<node>`, `<version>` typu number  jednoznacznie identyfikują (kolejno): klientów, punkty na trasie rozdzielające etapy wycieczek oraz wycieczki standardowe. 
**Weryfikację poprawności zapytań przeprowadza inna warstwa systemu i nie musimy się tą weryfikacją przejmować. Można założyć, że wszystkie wywołania będą zawsze w prawidłowym formacie, a wszystkie wartości będą odpowiedniego typu.**


Wartość `<password>` jest typu `string`, jej długość nie przekracza 128 znaków.

Wartość `<date>` jest typu `date` i reprezentuje datę. 


###### Status "ERROR"

Aplikacja będzie testowana wyłącznie na danych spełniających niniejszą specyfikację, jednak w razie wykrycia ew. niezgodności można zwrócić status "ERROR" (nie piszemy wariantów fail-safe itp.).

## Funkcje API

###### node

```
node <node> <lat> <lon> <description>
```

Dodaj nowy punkt z identyfikatorem `<node>`, ulokowany w  miejscu o współrzędnych `<lat>`, `<lon>`. Wartość `<description>` to tekstowy opis dla klienta

// nie zwraca krotek, 

###### catalog

```
catalog <version> <nodes>
```

Dodaje nową standardową wycieczkę o (unikalnym) numerze `<version>`, `<nodes>` to tablica zawierająca identyfikatory kolejnych punktów na trasie wycieczki (tj. identyfikatory `<node>`). 
Załóż, że wszystkie te punkty zostały wcześniej dodane wywołaniami funkcji `node`. Każda wycieczka składa się z co najmniej 2 punktów (niekoniecznie różnych).

```
// nie zwraca krotek
```

###### trip

```
trip <cyclist> <date> <version> 
```

Rezerwacja nowej wycieczki dla klienta `<cyclist>`, `<date>` to data dnia, w której wycieczka się rozpoczyna w pierwszym punkcie trasy, każdy kolejny punkt na trasie to kolejny dzień wycieczki, `<version>` to numer wycieczki z katalogu, 

`<cyclist>` może być nowym klientem lub jednym z dotychczasowych klientów.

Atrybuty zwracanej krotek
```
// nie zwraca krotek
```

###### closest_nodes

```
closest_nodes <ilat> <ilon>
```

Znajdź i zwróć dane 3 punktów położonych najbliżej współrzędnych `<ilat> <ilon>` - dla każdego z tych 3 punktów zwróć identyfikator `<node>`, jego współprzędne `<olat>`, `<olon>` oraz odległość `<distance>`.
W przypadku gdy liczba punktów w bazie jest mniejsza niż 3 to zwróć wszystkie te punkty. Wynik posortuj rosnąco wg `<distance>`, w drugiej kolejności rosnąco wg `<node>`.

```
// <node> <olat> <olon> <distance>
```

###### party

```
party <icyclist> <date>
```

Znajdź i zwróć listę rowerzystów (różnych od `<icyclist>`) nocujących w promieniu **20 km** od miejsca nocowania klienta `<icyclist>` w dniu `<date>`. Jeśli `<icyclist>` nie bierze w dniu `<date>` udziału w wycieczce to zwróć pusty wynik.
Dla każdego rowerzysty podaj jego id `<ocyclist>`, id `<node>` punktu, w którym nocuje oraz odległość `<distance>` pomiędzy tym punktem, a miejscem nocowania rowerzysty `<icyclist>`. 
Wyniki posortuj rosnąco wg `<distance>`, w drugiej kolejności rosnąco wg `<ocyclist>`.

```
// <ocyclist> <node> <distance>
```
###### guests

```
guests <node> <date>
```

Dla punktu `<node>` zwróć listę rowerzystów `<cyclist>`, którzy bedą w nim nocować w dniu `<date>`. Załóż, że `<node>` jest w bazie.
Wyniki posortuj rosnąco wg `<cyclist>`.

```
// <cyclist>
```

##### cyclists
```
cyclists <limit>
```

Zwróć ranking rowerzystów - wynik ogranicz do pierwszych `<limit>` krotek.
Dla każdego rowerzysty `<cyclist>` zwróć ile do tej pory zarezerwował wycieczek `<no_trips>` oraz ile (co najmniej) kilometrów obejmowały łącznie te wycieczki `<distance>`
(zsumuj odległości po linii prostej pomiędzy etapami, nie przejmuj się ew. błędem gdy jakiś punkt na trasie powtarza się). 
Wyniki posortuj rosnąco wg `<distance>`, w drugiej kolejności rosnąco wg `<cyclist>`.

```
// <cyclist> <no_trips> <distance>
```
