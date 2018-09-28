# -*- coding: utf-8 -*-
from rdflib import RDF
from SPARQLWrapper import SPARQLWrapper, JSON
import sys
import json
import math
import requests
import urllib.parse
import pickle
 
#Vector magnitude


def dbpediaSpotlight(text):

    subjects = []    
    
    data= urllib.parse.urlencode({'text' : text,'confidence':'0.5'})
    data = data.encode('utf-8')
    
    headers = {"Accept": "application/json"}
    
    r = requests.post("http://model.dbpedia-spotlight.org/en/annotate", data=data, headers=headers)
    print (r.text) 
    # Convert it to a Python dictionary
    data = json.loads(r.text)
    
    for item in data["Resources"]:
        #print ("<"+subject['@URI']+">")
        subjects.insert(len(subjects), "<"+item['@URI']+">")
    
    
    subjects == list(set(subjects))
    #transforming into set removes duplicates and order

    return subjects 
     
      
def getCategory(subjects):
    "Get DBpedia categories from subjects (list of strings), returns a JSON list"
    sparqlCategories = SPARQLWrapper("http://dbpedia.org/sparql")
    sparqlCategories.setQuery("""SELECT ?source ?category
    WHERE
    {?source dct:subject ?category} 
    VALUES ?source {"""+''.join(subjects)+"""
    }  """)
    sparqlCategories.setReturnFormat(JSON)
    categories = sparqlCategories.query().convert()
    return categories  

def getDist(jsonResult):
    if  jsonResult:
        for test in jsonResult:
            dist = test[0]["dist"]["value"]
        
    else:
        dist = 1000
    return dist
    
def shortestPath( sourceCat, targetCat ):
    "Calculates shortest path between 2 list of JSON lists"
    counter = []
    data = []
    one = 1
    outOfPath = 1000
    for targetCategory in targetCat["results"]["bindings"]:
        counter = 0
        for sourceCategory in sourceCat["results"]["bindings"]:
           
            target = "<"+targetCategory ["category"]["value"]+">"
            source = "<"+sourceCategory ["category"]["value"]+">"
            #DEBUG print ("sourceCat:",source," \ntarget:",target)
            #DEBUG print("resultCat: ", target)
            if (source == target):
                
                
                data.append(one)
                
                print ("-----SAME-----")
                
            else:
                sparqlShortest = SPARQLWrapper("http://dbpedia.org/sparql")
                sparqlShortest.setQuery("""
                SELECT ?in ?dist  ?steps
                 WHERE {
                    ?in <http://www.w3.org/2004/02/skos/core#broader> ?out OPTION(TRANSITIVE,t_max(20),T_DISTINCT, T_DIRECTION 2, T_SHORTEST_ONLY, t_in(?in), t_out(?out), t_step ('step_no') as ?dist, t_step (?in) as ?steps).
                    filter( ?in = """+source+""")
                    filter (?out = """+target+""")
                      }
                    ORDER BY DESC(?dist) LIMIT 1
                """)
                sparqlShortest.setReturnFormat(JSON)
                shortest = sparqlShortest.query().convert()
                if (str(shortest["results"]["bindings"]) == "[]"):
                    print ("-----EMPTY-----")
                    counter = counter+1
                    data.append(outOfPath)
                elif(str(shortest["results"]["bindings"]) != "[]"):
                    print ("-----ADDED-----")
                    sparqlInverseTarget = SPARQLWrapper("http://dbpedia.org/sparql")
                    sparqlInverseTarget.setQuery( """SELECT ?out_branches ?in_branches WHERE {
                        { SELECT  (count(?out) as ?out_branches)  
                          WHERE 
                          {
                           ?res  skos:broader ?out .
                           FILTER( ?res = """+target+""") .
                          }
                        }
                        UNION
                        { SELECT    (count(?in) as ?in_branches)
                          WHERE  
                          {  
                           ?in skos:broader ?res .
                           FILTER( ?res = """+target+""")
                          }}}""")
                    sparqlInverseTarget.setReturnFormat(JSON)
                    resultsInverseTarget = sparqlInverseTarget.query().convert()
                    inConnT = int(resultsInverseTarget['results']["bindings"][0]["out_branches"]["value"])
                    outConnT = int(resultsInverseTarget['results']["bindings"][1]["in_branches"]["value"])    
                    adjustT=(inConnT+outConnT)   
                    
                    print ("HopTarget=",target,"- inverseDist=",adjustT)
                    
                    sparqlInverseSource = SPARQLWrapper("http://dbpedia.org/sparql")
                    sparqlInverseSource.setQuery( """SELECT ?out_branches ?in_branches WHERE {
                        { SELECT  (count(?out) as ?out_branches)  
                          WHERE 
                          {
                           ?res  skos:broader ?out .
                           FILTER( ?res = """+source+""") .
                          }
                        }
                        UNION
                        { SELECT    (count(?in) as ?in_branches)
                          WHERE  
                          {  
                           ?in skos:broader ?res .
                           FILTER( ?res = """+source+""")
                          }}}""")
                    sparqlInverseSource.setReturnFormat(JSON)
                    resultsInverseSource = sparqlInverseSource.query().convert()
                    inConnS = int(resultsInverseSource['results']["bindings"][0]["out_branches"]["value"])
                    outConnS = int(resultsInverseSource['results']["bindings"][1]["in_branches"]["value"])    
                    adjustS=(inConnS+outConnS)   
                    print ("currentHopSource=",source,"- inverseDist=",adjustS)
                        
                    for shortestDist in shortest["results"]["bindings"]:
                        print("shortestDist",int(shortestDist ["dist"]["value"]))
                        
                        adjust = (int(adjustS)+int(adjustT))*0.7
                        print("shortest + adjust target 0.7 of inverse", int(shortestDist ["dist"]["value"])+adjust)
                        
                        data.append(int(shortestDist ["dist"]["value"])+float(adjust))
                        print("appended", int(shortestDist ["dist"]["value"])+float(adjust))
                        
       
       
       
                if (counter > 5):
                    print("-----BREAKING-----")
                    
                    continue

                
#                 
    print ("-----END POI-----\n")
    return data
    
 

#def mag(x): 
#    "Calculates the magnitude (euclidea norm) of a vector"
#    return math.sqrt(sum(i**2 for i in x))
    
#def getVectNormalized( vector ):
#    "Get normalized vector"
#    vectorNorm = []
#    for vectNum in vector:
#       vectorNorm.insert(len(vectorNorm),vectNum/mag(vector))
#    return vectorNorm

#def pnorm(a, p=2):
#    """Returns p-norm of sequence a. By default, p=2. 
#    p must be a positive integer.
#    """
#    tmp = 0
#    for elem in a:
#        tmp = tmp + abs(elem) ** p
#    return tmp ** (1.0 / p)
 
       
#magnitudeShortestNorm = mag(pairDistNorm)
    
#check if command line arg is passed
#if not sys.argv:
#    targetCategory = sys.argv[1]
#    text =sys.argv[2]
#    

#default target
#targetCategory = "<http://dbpedia.org/resource/Category:History_of_Jerusalem>"
 
sourceArtifactTitle = "sharr_yafo_cl-SHORT-NEW"
sourceArtifactText = """
Jaffa Gate
The walls surrounding the Old City of Jerusalem can be seen in this model.  """

sourceArtifactSubjects=dbpediaSpotlight(sourceArtifactText)
 
targetPOIs = ["<http://dbpedia.org/resource/Tower_of_David>"
,"<http://dbpedia.org/resource/Zion_Gate>"
,"<http://dbpedia.org/resource/Zedekiah's_Cave>"
,"<http://dbpedia.org/resource/Wilson's_Arch_(Jerusalem)>"
,"<http://dbpedia.org/resource/Western_Wall_Tunnel>"
,"<http://dbpedia.org/resource/Western_Wall>"
,"<http://dbpedia.org/resource/Well_of_Souls>"
,"<http://dbpedia.org/resource/Warren's_Shaft>"
,"<http://dbpedia.org/resource/Warren's_Gate>"
,"<http://dbpedia.org/resource/Tombs_of_the_Kings_(Jerusalem)>"
,"<http://dbpedia.org/resource/Tomb_of_Zechariah>"
,"<http://dbpedia.org/resource/Tomb_of_the_Prophets_Haggai,_Zechariah_and_Malachi>"
,"<http://dbpedia.org/resource/Tomb_of_Benei_Hezir>"
,"<http://dbpedia.org/resource/Tomb_of_Absalom>"
,"<http://dbpedia.org/resource/Tiferet_Yisrael_Synagogue>"
,"<http://dbpedia.org/resource/The_Garden_Tomb>"
,"<http://dbpedia.org/resource/Templum_Domini>"
,"<http://dbpedia.org/resource/Temple_Mount>"
,"<http://dbpedia.org/resource/Struthion_Pool>"
,"<http://dbpedia.org/resource/St_Anne's_Church,_Jerusalem>"
,"<http://dbpedia.org/resource/Southern_Wall>"
,"<http://dbpedia.org/resource/Solomon's_Stables>"
,"<http://dbpedia.org/resource/Silwan>"
,"<http://dbpedia.org/resource/Siloam_tunnel>"
,"<http://dbpedia.org/resource/Siloam>"
,"<http://dbpedia.org/resource/Russian_Compound>"
,"<http://dbpedia.org/resource/Royal_Stoa_(Jerusalem)>"
,"<http://dbpedia.org/resource/Ramban_Synagogue>"
,"<http://dbpedia.org/resource/Quarries_(biblical)>"
,"<http://dbpedia.org/resource/Ptolemaic_Baris>"
,"<http://dbpedia.org/resource/Pool_of_Siloam>"
,"<http://dbpedia.org/resource/Pool_of_Raranj>"
,"<http://dbpedia.org/resource/Pool_of_Bethesda>"
,"<http://dbpedia.org/resource/Phasael_tower>"
,"<http://dbpedia.org/resource/Ohel_Yitzchak_Synagogue>"
,"<http://dbpedia.org/resource/New_Gate>"
,"<http://dbpedia.org/resource/Mughrabi-Bridge>"
,"<http://dbpedia.org/resource/Moriah>"
,"<http://dbpedia.org/resource/Mishkenot_Sha'ananim>"
,"<http://dbpedia.org/resource/Lions'_Gate>"
,"<http://dbpedia.org/resource/Large_Stone_Structure>"
,"<http://dbpedia.org/resource/Ketef_Hinnom>"
,"<http://dbpedia.org/resource/Jerusalem_Water_Channel>"
,"<http://dbpedia.org/resource/Jerusalem_pilgrim_road>"
,"<http://dbpedia.org/resource/Jaffa_Gate>"
,"<http://dbpedia.org/resource/Israelite_Tower>"
,"<http://dbpedia.org/resource/Hurva_Synagogue>"
,"<http://dbpedia.org/resource/Huldah_Gates>"
,"<http://dbpedia.org/resource/Hezekiah's_Pool>"
,"<http://dbpedia.org/resource/Herod's_Palace_(Jerusalem)>"
,"<http://dbpedia.org/resource/Herod's_Gate>"
,"<http://dbpedia.org/resource/Hasmonean_Baris>"
,"<http://dbpedia.org/resource/Golden_Gate_(Jerusalem)>"
,"<http://dbpedia.org/resource/Gihon_Spring>"
,"<http://dbpedia.org/resource/Four_Sephardic_Synagogues>"
,"<http://dbpedia.org/resource/Foundation_Stone>"
,"<http://dbpedia.org/resource/Excavations_at_the_Temple_Mount>"
,"<http://dbpedia.org/resource/Ecce_Homo_(church)>"
,"<http://dbpedia.org/resource/Dormition_Abbey>"
,"<http://dbpedia.org/resource/Dominus_Flevit_Church>"
,"<http://dbpedia.org/resource/Dome_of_the_Rock>"
,"<http://dbpedia.org/resource/Dome_of_the_Prophet>"
,"<http://dbpedia.org/resource/Dome_of_the_Chain>"
,"<http://dbpedia.org/resource/Dome_of_the_Ascension>"
,"<http://dbpedia.org/resource/Dome_of_al-Khalili>"
,"<http://dbpedia.org/resource/Damascus_Gate>"
,"<http://dbpedia.org/resource/Convent_of_the_Sisters_of_Zion>"
,"<http://dbpedia.org/resource/City_of_David>"
,"<http://dbpedia.org/resource/Church_of_Zion,_Jerusalem>"
,"<http://dbpedia.org/resource/Church_of_the_Flagellation>"
,"<http://dbpedia.org/resource/Church_of_the_Condemnation_and_Imposition_of_the_Cross>"
,"<http://dbpedia.org/resource/Church_of_All_Nations>"
,"<http://dbpedia.org/resource/Cave_of_Nicanor>"
,"<http://dbpedia.org/resource/Cardo>"
,"<http://dbpedia.org/resource/Burnt_House>"
,"<http://dbpedia.org/resource/Broad_Wall_(Jerusalem)>"
,"<http://dbpedia.org/resource/Birket_Israel>"
,"<http://dbpedia.org/resource/Antonia_Fortress>"
,"<http://dbpedia.org/resource/Ancient_city_walls_around_the_City_of_David>"
,"<http://dbpedia.org/resource/American_Colony,_Jerusalem>"
,"<http://dbpedia.org/resource/American_Colony,_Jerusalem>"
,"<http://dbpedia.org/resource/Acra_(fortress)>"
,"<http://dbpedia.org/resource/Tower_of_Siloam>"
,"<http://dbpedia.org/resource/Tombs_of_the_Sanhedrin>"
,"<http://dbpedia.org/resource/Talpiot_Tomb>"
,"<http://dbpedia.org/resource/Sultan's_Pool>"
,"<http://dbpedia.org/resource/Shuafat>"
,"<http://dbpedia.org/resource/Second_Temple>"
,"<http://dbpedia.org/resource/Ramat_Rachel>"
,"<http://dbpedia.org/resource/Monastery_of_the_Virgins>"
,"<http://dbpedia.org/resource/Mamilla_Pool>"
,"<http://dbpedia.org/resource/Jason's_Tomb>"
,"<http://dbpedia.org/resource/Givati_Parking_Lot_dig>"
,"<http://dbpedia.org/resource/Finger_of_Og>"
,"<http://dbpedia.org/resource/Cave_of_the_Ramban>"
,"<http://dbpedia.org/resource/Bir_el_Qutt_inscriptions>"
,"<http://dbpedia.org/resource/Aelia_Capitolina>"
,"<http://dbpedia.org/resource/Jerusalem>"
,"<http://dbpedia.org/resource/Church_of_the_Holy_Sepulchre>"]


sourceArtifactSubjectsTest =["<http://dbpedia.org/resource/Cardo>"]
targetPOIsTest = ["<http://dbpedia.org/resource/Cardo>"]
#targetPOIsTest = ["<http://dbpedia.org/resource/Tower_of_David>"
#,"<http://dbpedia.org/resource/Geography>"
#,"<http://dbpedia.org/resource/Western_Wall>"]

categoriesSource = []
categoriesTarget = []
dictShortest = {}

categoriesSource=getCategory(sourceArtifactSubjects)
#DEBUG print("categoriesSource", categoriesSource)
resultList = []
distancesList = []
distanceListNorm = []
for targetPOI in targetPOIs:
    targetPOIlist = []
    #DEBUG print("targetPOI", targetPOI)
    categoriesTarget=getCategory(targetPOI)
    #DEBUG print("categoriesTarget", categoriesTarget)
    #DEBUG print ("categoriesTargetPOI=",categoriesTarget)
    
    resultData=shortestPath( categoriesSource, categoriesTarget)
    print(resultData)
    
    #metrics for returned shortest path list - 
    minVal=min(resultData)
    #connection weight, in order to take into account the number of valid paths found between the exhibit and the POI 
    connectivity=1
    for value in resultData:
        if value < 1000:
            connectivity=connectivity+(1/value)
            print ("connectivity: ", connectivity)
    
    #evaluates the semantic distance 
    semDistance=min(resultData)/connectivity
    #we normalize the distance between 0-1 (normalized = (x-min(x))/(max(x)-min(x))), min value considered 0 max value considered 1000, and update the dictionary
    dictShortest[targetPOI]=semDistance/1000
print ("Added Keys are:",dictShortest)



#Scrittura file
path=  "/Users/lobuea/Documents/Personale/WorkDir/"
f = open(path+sourceArtifactTitle, "wb")    
pickle.dump(dictShortest, f )
f.close()     
    



