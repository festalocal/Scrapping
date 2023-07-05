# On commence par importer certaines bibliothèques nécessaires pour le script.
# `requests` est une bibliothèque pour effectuer des requêtes HTTP, 
# `rdflib` est une bibliothèque pour travailler avec des données RDF (une méthode pour modéliser les données),
# et `json` est utilisé pour travailler avec des données au format JSON.
import requests
from rdflib import Graph
import json

# Ici, nous définissons une chaîne de caractères qui contient une requête SPARQL. 
# SPARQL est un langage de requête utilisé pour interroger les bases de données stockées en RDF.
# Cette requête spécifique est utilisée pour construire un sous-ensemble de données en fonction des critères spécifiés.
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

# Ici, nous définissons l'URL du service web auquel nous allons envoyer la requête SPARQL.
url = "https://diffuseur.datatourisme.fr/webservice/49eebc93390a819eb8c2a0f95a150916/93cac18d-5c3e-4101-b0f5-db0c94423c88"

# Ici, nous utilisons la bibliothèque `requests` pour envoyer une requête HTTP POST à l'URL spécifiée.
# La requête SPARQL est envoyée en tant que données ('data') de cette requête.
response = requests.post(url, data={'query': sparql_query})

# Ici, nous vérifions si la requête a réussi. 
# Un code de statut HTTP de 200 signifie que la requête a réussi.
# Si c'est le cas, nous imprimons le texte de la réponse pour le débogage et nous continuons à traiter la réponse.
if response.status_code == 200:
    #print(response.text)  # print the response to debug
    # Ici, nous créons un nouveau 'Graph' RDFlib pour stocker les données.
    # Ensuite, nous essayons d'analyser les données de la réponse en utilisant le format "json-ld".
    # Si cela échoue pour une raison quelconque, nous imprimons une erreur et arrêtons le script.
    g = Graph()
    try:
        g.parse(data=response.text, format="json-ld")
    except Exception as e:
        print(f"Erreur lors de l'analyse de la réponse : {e}")

    # Ici, nous convertissons les données RDF en format JSON-LD, qui est un format de données structurées spécifique.
    # Nous écrivons ensuite ces données dans un fichier appelé 'output.json'.
    json_data = g.serialize(format='json-ld')
    
    with open('output.json', 'w', encoding='utf-8') as f:
        f.write(json_data)
        print("Ecriture terminée, fichier produit : output.json")
        
# Si la requête n'a pas réussi (c'est-à-dire si le code de statut HTTP n'est pas 200), 
# alors nous imprimons un message d'erreur.
else:
    print(f"Une erreur s'est produite : {response.status_code}")