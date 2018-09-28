import json
from rdflib.plugin import register, Serializer
register('json-ld', Serializer, 'rdflib_jsonld.serializer', 'JsonLDSerializer')
from SPARQLWrapper import SPARQLWrapper, JSON, XML, JSONLD, N3
import random
import googlemaps




sourceGraph= "<http://tesori.fce.pa.icar.cnr.it/eventi>"
targetGraph =  "<http://tesori.fce.pa.icar.cnr.it/eventiGeo>"
dictEvents = {}
streetAddress = ""
addressLocality = ""
postalCode = ""
streetNumber = ""

##DATA RETRIEVAL##

result = SPARQLWrapper("http://kossyra.pa.icar.cnr.it:8890/sparql")
result.setQuery("""
                       SELECT DISTINCT * WHERE {
                            GRAPH """+sourceGraph+"""{
                            ?sub a <http://www.schema.org/Event> .
                            ?sub <http://tesori.fce.pa.icar.cnr.it/place_name> ?place_name . 
                            ?sub <http://tesori.fce.pa.icar.cnr.it/meta_description> ?meta_description .
                            ?sub <http://www.schema.org/title> ?title
                                }
                            } 
                """)

result.setReturnFormat(JSON)
 
queryResult = result.query().convert()
#print(result.query().convert())
#print("\n ----- \n")



for item in queryResult ['results'] ['bindings']:
    event = (item ["sub"]["value"])
    place = (item ["place_name"]["value"])
   # print (event)
    dictEvents[event] = {"id":event,"place": place}
  
#print(dictEvents[event])



##GEOCODING##

gmaps = googlemaps.Client(key='AIzaSyALmYe5DlyyJuL9rAhyWYQuvvBXvUtecz8')

#Geocoding an address
for event_key in dictEvents.keys():
    
    #print (dictEvents[event_key]["place"])
    
    #Request geocode localised IT and withing Palermo place  
    geocode_result = gmaps.geocode(dictEvents[event_key]["place"]+" Palermo",region="IT", language="it")
    
    #Parse geocode response    
    for geocode_content in geocode_result:
        fullAddress = geocode_content["formatted_address"]
        for component in geocode_content["address_components"]:
            if ("postal_code" in component["types"]):
                postalCode = component["long_name"]
            if ("administrative_area_level_3" in component["types"]):
                addressLocality = component["long_name"]
            if ("route" in component["types"]):
                streetAddress = component["long_name"]
            if ("street_number" in component["types"]):
                streetNumber = component["long_name"]
 
        lat = geocode_content['geometry']["location"]["lat"]
        lon = geocode_content['geometry']["location"]["lng"]
        
        
        dictEvents[event_key]["lat"] = lat
        dictEvents[event_key]["lon"] = lon
        dictEvents[event_key] ["address"] = fullAddress
        
    event_id = event_key.split("pcc2018Event/",1)[1]
    location_instance = "<http://tesori.fce.pa.icar.cnr.it/pcc2018Location/"+event_id+">"
    address_instance = "<http://tesori.fce.pa.icar.cnr.it/pcc2018Address/"+event_id+">"
    geoco_instance = "<http://tesori.fce.pa.icar.cnr.it/pcc2018Geo/"+event_id+">"
     
    #Inferred data upload (per event)
    update = SPARQLWrapper("http://kossyra.pa.icar.cnr.it:8890/sparql")
    update.method = 'POST'
    update.setQuery("""
                           INSERT 
                                 { 
                                    GRAPH """+targetGraph+"""
                                        {
                                         <"""+event_key+"""> <http://www.schema.org/location> """+location_instance+""" .
                                         """+location_instance+""" a <http://www.schema.org/Place> .
                                         """+location_instance+""" <http://schema.org/name> \""""+dictEvents[event_key]["place"]+"""\" .
                                         """+location_instance+"""  <http://schema.org/address> """+address_instance+""" .
                                         """+address_instance+""" a <http://www.schema.org/PostalAddress> .
                                         """+address_instance+""" <http://www.schema.org/addressLocality> \""""+addressLocality+"""\" .
                                         """+address_instance+""" <http://www.schema.org/streetAddress> \""""+streetAddress+""", """+streetNumber+"""\" .
                                         """+address_instance+""" <http://www.schema.org/postalCode> \""""+postalCode+"""\" .
                                         """+location_instance+"""  <http://schema.org/geo> """+geoco_instance+""" .
                                         """+geoco_instance+""" a <http://www.schema.org/GeoCoordinates> .
                                         """+geoco_instance+""" <http://www.schema.org/latitude> \""""+str(lat)+"""\" .
                                         """+geoco_instance+""" <http://www.schema.org/longitude> \""""+str(lon)+"""\"    
                                        }
                                        
                                    
                                }
                        
          
                    """)
    update.query()
    print ("--- Process done for event: "+event_key+" ---")
 


#print("\n ----- \n")
     
#print(dictEvents)
        
 



#    
# 
# 
#
##g = Graph().parse(data=result.query().convert(), format='n3')
