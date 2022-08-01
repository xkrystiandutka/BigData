## Definiujemy job jako klasę, która dziedziczy po klasie MRJob
Ta klasa zawiera metody, które pozwalają definiować poszczególne kroki job'a
Jeden krok składa się z trzech etapów:
- mapper
- combiner
- reducer

Wszystkie trzy są opcjonalne, natomiast musimy przekazać co najmniej jeden

## Uruchamianie skryptu
#### Aby uruchomić job'a należy użyć komendy:
```
$ python [nazwa_skryptu.py] [nazwa_pliku_wejsciowego.txt]
$ python simpleMapReduce.py sample01.txt
```
#### Można także przekazać więcej plików wejściowych, np.
```
$ python simpleMapReduce.py sample01.txt sample02.txt
```
