from rdflib.plugin import register, Serializer
register('json-ld', Serializer, 'rdflib_jsonld.serializer', 'JsonLDSerializer')
from SPARQLWrapper import SPARQLWrapper, JSON, XML, JSONLD, N3
import googlemaps
import urllib  
import requests
from geopy.geocoders import Nominatim


sourceGraph= "<http://athena.pa.icar.cnr.it/eventiPCC2018conTesto>"
targetGraph =  "<http://athena.pa.icar.cnr.it/eventiPCC2018linkedData>"
dictEvents = {}
streetAddress = ""
addressLocality = ""
postalCode = ""
streetNumber = ""

##DATA RETRIEVAL##

result = SPARQLWrapper("http://kossyra.pa.icar.cnr.it:8890/sparql")
result.setTimeout(15000000)
result.setQuery("""
                       SELECT DISTINCT * WHERE {
                            GRAPH """+sourceGraph+"""{
                            ?sub a <http://schema.org/Event> .
                            ?sub <http://athena.pa.icar.cnr.it/pcc2018/place_name> ?place_name . 
                            ?sub <http://athena.pa.icar.cnr.it/pcc2018/text> ?text . 
                            ?sub <http://schema.org/title> ?title
                                }
                            } 
                """)

result.setReturnFormat(JSON)
try:
    queryResult = result.query().convert()
except TimeoutError as e:
    print ("Timeout error catched "+ str(e))
except urllib.error.URLError as e:
    print ("URLerror" + str(e))



#print(result.query().convert())
#print("\n ----- \n")

#GET TITLE and META
#SELECT DISTINCT * WHERE { ?x <http://tesori.fce.pa.icar.cnr.it/meta_description> ?z . ?x <http://schema.org/title> ?y} 


## STORE RETRIEVED DATA ##
for item in queryResult ['results'] ['bindings']:
    event = (item ["sub"]["value"])
    place = (item ["place_name"]["value"])
    # print (event)
    dictEvents[event] = {"id":event,"place": place}
  
#print(dictEvents[event])



##GEOCODING##

#gmaps = googlemaps.Client(key='AIzaSyALmYe5DlyyJuL9rAhyWYQuvvBXvUtecz8')
for event_key in dictEvents.keys():   
    
    geolocator = Nominatim(user_agent="PCCevents")
    location = geolocator.geocode(dictEvents[event_key]["place"]+" Palermo")
    print(dictEvents[event_key]["place"])
    print(location)

#Geocoding an address
for event_key in dictEvents.keys():
    
    #print ("place " +dictEvents[event_key]["place"])
    
    #Request geocode localised IT and withing Palermo place  
    geocode_result = gmaps.geocode(dictEvents[event_key]["place"]+" Palermo",region="IT", language="it")
    
     
    if geocode_result:
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
            
        event_id = event_key.split("pcc2018/Event/",1)[1]
        location_instance = "<http://athena.pa.icar.cnr.it/pcc2018/Location/"+event_id+">"
        address_instance = "<http://athena.pa.icar.cnr.it/pcc2018/Address/"+event_id+">"
        geoco_instance = "<http://athena.pa.icar.cnr.it/pcc2018/Geo/"+event_id+">"
         
        #Inferred data upload (per event)
        
        update = SPARQLWrapper("http://kossyra.pa.icar.cnr.it:8890/sparql")
        update.setTimeout(15000000)
        update.method = 'POST'
        update.setQuery("""
                           INSERT 
                                 { 
                                    GRAPH """+targetGraph+"""
                                        {
                                         <"""+event_key+"""> <http://schema.org/location> """+location_instance+""" .
                                         """+location_instance+""" a <http://schema.org/Place> .
                                         """+location_instance+""" <http://schema.org/name> \""""+dictEvents[event_key]["place"]+"""\" .
                                         """+location_instance+"""  <http://schema.org/address> """+address_instance+""" .
                                         """+location_instance+"""  <http://www.w3.org/2000/01/rdf-schema#label> \""""+dictEvents[event_key]["place"]+"""\" .
                                         """+address_instance+""" a <http://schema.org/PostalAddress> .
                                         """+address_instance+""" <http://schema.org/addressLocality> \""""+addressLocality+"""\" .
                                         """+address_instance+""" <http://schema.org/streetAddress> \""""+streetAddress+""", """+streetNumber+"""\" .
                                         """+address_instance+""" <http://schema.org/postalCode> \""""+postalCode+"""\" .
                                         """+address_instance+"""  <http://www.w3.org/2000/01/rdf-schema#label> \""""+dictEvents[event_key]["address"]+"""\" .
                                         """+location_instance+"""  <http://schema.org/geo> """+geoco_instance+""" .
                                         """+geoco_instance+""" a <http://schema.org/GeoCoordinates> .
                                         """+geoco_instance+""" <http://schema.org/latitude> \""""+str(lat)+"""\" .
                                         """+geoco_instance+""" <http://schema.org/longitude> \""""+str(lon)+"""\"   .
                                         """+geoco_instance+"""  <http://www.w3.org/2000/01/rdf-schema#label> \""""+str(lat)+","+str(lon)+"""\" .
                                         <"""+event_key+"""> <http://athena.pa.icar.cnr.it/pcc2018/Event/plainAddress> \""""+dictEvents[event_key]["address"]+"""\"
                                        }
                                        
                                    
                                }
                        
          
                    """)
        try:
            update.query()
        except TimeoutError as e:
            print ("Timeout error catched "+ str(e))
            continue
        except urllib.error.URLError as e:
            print ("URLerror error catched " + str(e))
            continue
        print ("--- Process done for event: "+event_key+" ---")
          
     