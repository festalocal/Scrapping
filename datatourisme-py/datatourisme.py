import requests
from rdflib import Graph
import json

sparql_query = """
CONSTRUCT { 
  ?res <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:resource>. 
} WHERE { 
<http://www.bigdata.com/queryHints#Query> <http://www.bigdata.com/queryHints#optimizer> "None".
  ?res <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://www.datatourisme.fr/ontology/core#PointOfInterest>.
  ?res <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?649c0139b320a.
  VALUES (?649c0139b320a) {
    (<https://www.datatourisme.fr/ontology/core#EntertainmentAndEvent>)
  }
  ?res <https://www.datatourisme.fr/ontology/core#isLocatedAt> ?649c0139b32b8.
  ?649c0139b32b8 <http://schema.org/address> ?649c0139b332c.
  ?649c0139b332c <https://www.datatourisme.fr/ontology/core#hasAddressCity> ?649c0139b3397.
  ?649c0139b3397 <https://www.datatourisme.fr/ontology/core#isPartOfDepartment> ?649c0139b33f6.
  ?649c0139b33f6 <https://www.datatourisme.fr/ontology/core#isPartOfRegion> ?649c0139b3455.
  FILTER(?649c0139b3455 IN(<https://www.datatourisme.fr/resource/core#France93>, <https://www.datatourisme.fr/resource/core#France94>, <https://www.datatourisme.fr/resource/core#France84>, <https://www.datatourisme.fr/resource/core#France75>, <https://www.datatourisme.fr/resource/core#France76>, <https://www.datatourisme.fr/resource/core#France52>, <https://www.datatourisme.fr/resource/core#France53>, <https://www.datatourisme.fr/resource/core#France44>, <https://www.datatourisme.fr/resource/core#France32>, <https://www.datatourisme.fr/resource/core#France27>, <https://www.datatourisme.fr/resource/core#France28>, <https://www.datatourisme.fr/resource/core#France24>, <https://www.datatourisme.fr/resource/core#France11>))
  ?res <https://www.datatourisme.fr/ontology/core#takesPlaceAt> ?649c0139b3580.
  ?649c0139b3580 <https://www.datatourisme.fr/ontology/core#startDate> ?649c0139b35ee.
  FILTER(?649c0139b35ee > (<http://www.w3.org/2001/XMLSchema#date>(NOW())))
  ?res <http://www.w3.org/2000/01/rdf-schema#label> ?649c0139b365e.
  {
    SERVICE <http://www.bigdata.com/rdf/search#search> { ?649c0139b365e <http://www.bigdata.com/rdf/search#search> "fête;feria;festa;fest-noz". }
    <http://www.bigdata.com/queryHints#SubQuery> <http://www.bigdata.com/queryHints#optimizer> "Static".
  }
}
"""

url = "https://diffuseur.datatourisme.fr/webservice/49eebc93390a819eb8c2a0f95a150916/93cac18d-5c3e-4101-b0f5-db0c94423c88"

response = requests.post(url, data={'query': sparql_query})

if response.status_code == 200:
    print(response.text)  # print the response to debug
    g = Graph()
    try:
        g.parse(data=response.text, format="json-ld")
    except Exception as e:
        print(f"Erreur lors de l'analyse de la réponse : {e}")
    
    json_data = g.serialize(format='json-ld')  # No need to decode
    
    with open('output.json', 'w', encoding='utf-8') as f:
        f.write(json_data)
else:
    print(f"Une erreur s'est produite : {response.status_code}")