from rdflib.plugin import register, Serializer
register('json-ld', Serializer, 'rdflib_jsonld.serializer', 'JsonLDSerializer')
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import urllib.parse
import json
import time

def dbpediaSpotlight(text):

    subjects = []    
    #*
    data= urllib.parse.urlencode({'text' : text,'confidence':'0.5', 'types' : 'DBpedia:Event, DBpedia:Food, DBpedia:Name, DBpedia:Person, DBpedia:Place, DBpedia:Work'})
 
    data = data.encode('utf-8')
    
    headers = {"Accept": "application/json"}
    
    r = requests.post("http://model.dbpedia-spotlight.org/it/annotate", data=data, headers=headers, timeout=50)
    
    print ("dbpediaspot: "+r.text) 
    # Convert it to a Python dictionary
    
    try:
        json.loads(r.text)
    
    except ValueError:
        return subjects
    
    data = json.loads(r.text)
    
    if ("Resources" in data):
        for item in data["Resources"]:
            #print ("<"+subject['@URI']+">")
            subjects.insert(len(subjects), "<"+item['@URI']+">")
        
        #transforming into set removes duplicates and order    
        subjects == list(set(subjects))

    return subjects 
     #####



sourceGraph= "<http://athena.pa.icar.cnr.it/eventiPCC2018conTesto>"
targetGraph =  "<http://athena.pa.icar.cnr.it/eventiPCC2018linkedData>"
dictEvents = {}

##DATA RETRIEVAL##

result = SPARQLWrapper("http://kossyra.pa.icar.cnr.it:8890/sparql")
result.setTimeout(500000)
result.setQuery("""
                       SELECT DISTINCT * WHERE {
                            GRAPH """+sourceGraph+"""{
                            ?sub a <http://schema.org/Event> .
                            ?sub <http://athena.pa.icar.cnr.it/pcc2018/text> ?text . 
                            ?sub <http://schema.org/title> ?title
                                } 
                             }   
                """)

result.setReturnFormat(JSON)
queryResult = result.query().convert()

 
#print(result.query().convert())
#print("\n ----- \n")
    
#GET TITLE and META


## STORE RETRIEVED DATA ##
for item in queryResult ['results'] ['bindings']:
    event = (item ["sub"]["value"])
    title = (item ["title"]["value"])
    meta_description = (item ["text"]["value"])
    event_description_bag = title+" "+meta_description 
    dictEvents[event] = {"id":event,"title": title, "description_bag": event_description_bag}
    

#print(dictEvents[event])



##DBPEDIA SPOTLIGHT## 
#for each event calls DBpediaSpotlight
for event_key in dictEvents.keys():
#    
    eventTags = dbpediaSpotlight(dictEvents[event_key]["description_bag"])
    #2 secs sleep time to avoid 429 too many request
    time.sleep(2) 
    if eventTags:
        #for each concept of the bag, add data to the endpoint
        for resources in eventTags:        
            #Inferred data upload (per event)
            print ("--- resources: "+resources+" ---")
            update = SPARQLWrapper("http://kossyra.pa.icar.cnr.it:8890/sparql")
            update.setTimeout(500000)
            update.method = 'POST'
            update.setQuery("""
                                   INSERT 
                                         { 
                                            GRAPH """+targetGraph+"""
                                                {
                                                 <"""+event_key+"""> <http://www.w3.org/2000/01/rdf-schema#seeAlso> """+resources+"""  
                                                }
                                                
                                            
                                        }
                                
                  
                            """)
            
            try:
                update.query()
                print ("--- Process done for event: "+event_key+" ---")
            except TimeoutError as e:
                print ("Timeout error catched "+ str(e))
                continue
            except urllib.error.URLError as e:
                print ("URLerror error catched " + str(e))
                continue
                
 
