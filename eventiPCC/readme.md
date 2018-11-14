## eventiPCC
Progetto:
Un framework basato sui Linked Data per l’arricchimento semantico dei Dati Aperti sugli eventi 

### Topic: 
Linked Open Data extraction on Palermo Capitale della Cultura

#### Semantic named-entity recognition
Using DBpedia Linked Data as vocabulary for the named entities
Infer concept and knowledge related to the Event description

#### Geocoding enrichment
Using Nominatim Open Street Map API, 
Add the geolocation data to the Event plus Wikidata and Wikipedia references about the place (when present)

#### Spatial interlinking
Using a federated query on the pcc2018 dataset and the Italian DBpedia dataset
Infer via spatial distance (Haversine) point of Interests near the Event location

#### Information expansion
Using federated query add Place description and images

### Ontology:
http://athena.pa.icar.cnr.it/htdocs/PCC2018/ontology/

### SPARQL endpoint:
http://kossyra.pa.icar.cnr.it:8890/sparql

### Enriched resource example:
http://athena.pa.icar.cnr.it:8080/lodview/Event/471.html
