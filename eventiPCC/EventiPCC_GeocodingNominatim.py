from rdflib.plugin import register, Serializer
register('json-ld', Serializer, 'rdflib_jsonld.serializer', 'JsonLDSerializer')
from SPARQLWrapper import SPARQLWrapper, JSON, XML, JSONLD, N3
import googlemaps
import urllib  
import requests
from geopy.geocoders import Nominatim
 

sourceGraph= "<http://athena.pa.icar.cnr.it/eventiPCC2018conTesto>"
targetGraph =  "<http://athena.pa.icar.cnr.it/eventiPCC2018testNominatim-UPD>"
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

#Nominatim via GeoPy, see https://geopy.readthedocs.io/en/stable/#nominatim
for event_key in dictEvents.keys():   
    #Request geocoder Nominatim to GeoPy    
    geolocator = Nominatim(user_agent="PCCevents")
    #Nominatim call with timeout and extratags localised IT and within Palermo
    location = geolocator.geocode(dictEvents[event_key]["place"]+" Palermo", timeout=100, addressdetails=True, extratags=True)
    #print(dictEvents[event_key]["place"])


    if location:
        geocode_result = location.raw
        print (geocode_result)
        fullAddress = geocode_result["display_name"]
        print (fullAddress)
        lat = geocode_result["lat"]
        lon = geocode_result["lon"]
        if geocode_result["extratags"]:
            if ("wikipedia" in geocode_result["extratags"]):
                wikipedia = geocode_result["extratags"]["wikipedia"].replace(" ", "_").replace("it:","https://it.wikipedia.org/wiki/")
            else:
                wikipedia = ""
            if ("wikidata" in geocode_result["extratags"]):
                wikidata = 'https://www.wikidata.org/wiki/'+geocode_result["extratags"]["wikidata"]
            else:
                wikidata = ""
        #Get streetAddress either from road or pedestrian field
        if ("road" in geocode_result["address"]):
            streetAddress = geocode_result["address"]["road"]
        if ("pedestrian" in geocode_result["address"]):
            streetAddress = geocode_result["address"]["pedestrian"]            
        if ("city" in geocode_result["address"]):
            addressLocality = geocode_result["address"]["city"]
        if ("postcode" in geocode_result["address"]):
            postalCode = geocode_result["address"]["postcode"]
        
 
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
                                         """+address_instance+""" <http://schema.org/postalCode> \""""+postalCode+"""\" .
                                         """+address_instance+""" <http://schema.org/streetAddress> \""""+streetAddress+""", """+streetNumber+"""\" .
                                         """+address_instance+"""  <http://www.w3.org/2000/01/rdf-schema#label> \""""+fullAddress+"""\" .
                                         """+location_instance+"""  <http://schema.org/geo> """+geoco_instance+""" .
                                         """+geoco_instance+""" a <http://schema.org/GeoCoordinates> .
                                         """+geoco_instance+""" <http://schema.org/latitude> \""""+str(lat)+"""\" .
                                         """+geoco_instance+""" <http://schema.org/longitude> \""""+str(lon)+"""\"   .
                                         """+geoco_instance+"""  <http://www.w3.org/2000/01/rdf-schema#label> \""""+str(lat)+","+str(lon)+"""\" .
                                         <"""+event_key+"""> <http://athena.pa.icar.cnr.it/pcc2018/Event/plainAddress> \""""+fullAddress+"""\"
                                         <"""+event_key+"""> <http://www.w3.org/2002/07/owl#sameAs> \""""+wikidata+"""\"
                                         <"""+event_key+"""> <http://athena.pa.icar.cnr.it/pcc2018/wikipedia> \""""+wikipedia+"""\"
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
          
     