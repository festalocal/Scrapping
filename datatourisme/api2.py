#===================================================================================================
# *                                         API#2 DATATOURISME
#===================================================================================================
# Importation des bibliothèques nécessaires
import requests
import json
from google.cloud import bigquery
from datetime import datetime
from google.oauth2 import service_account
import uuid

# URL du fichier JSON-LD
url = "https://diffuseur.datatourisme.fr/webservice/49eebc93390a819eb8c2a0f95a150916/93cac18d-5c3e-4101-b0f5-db0c94423c88"

# Les informations d'authentification pour BigQuery sont dans ce fichier
credentials = service_account.Credentials.from_service_account_file('festalocal-fa14bbdaf49c.json')
client = bigquery.Client(credentials=credentials)

# Spécifiez votre dataset et table BigQuery
dataset_id = 'festa'
table_id = 'evenement'

# Cette fonction récupère les données du fichier JSON-LD à l'URL spécifiée
def fetch(url):
    # Récupéeration du fichier JSON-LD
    response = requests.get(url)
    data = response.json()

    # Sauvegarde du fichier JSON-LD dans un fichier data.jsonld
    # with open("data.jsonld", "w", encoding="utf-8") as f:
    #     json.dump(data, f, ensure_ascii=False)
    return data

# Cette fonction adapte les données de l'événement à insérer dans BigQuery
def adapt_event(event):
    # Vérifiez les clés requises
    if not all(key in event for key in ["rdfs:label", "schema:startDate", "schema:endDate", "isLocatedAt"]):
        return None, event
    
    # Création d'un ID unique pour chaque événement à l'aide de uuid
    unique_id = str(uuid.uuid4())

    # D'autres éléments des données sont récupérés, vérifiés et adaptés pour correspondre à la structure de la table BigQuery.
    # Other elements of the data are retrieved, checked, and adapted to fit the BigQuery table structure.
    resource = None
    if "hasMainRepresentation" in event and "ebucore:hasRelatedResource" in event["hasMainRepresentation"]:
        resource = event["hasMainRepresentation"]["ebucore:hasRelatedResource"]
        print(f"Type: {type(resource)}, Value: {resource}")
    if resource and isinstance(resource.get("ebucore:locator", {}), dict):
        image_url = resource.get("ebucore:locator", {}).get("@value", None)
    else:
        image_url = None


    motscles = event.get("@type", None)

    title = event["rdfs:label"].get("@value", None)

    start_date = retrieve_date(event, "schema:startDate")

    end_date = retrieve_date(event, "schema:endDate")

    ville = None
    if "isLocatedAt" in event and "schema:address" in event["isLocatedAt"] and "schema:addressLocality" in event["isLocatedAt"]["schema:address"]:
        if isinstance(event["isLocatedAt"]["schema:address"]["schema:addressLocality"], list):
            ville = event["isLocatedAt"]["schema:address"]["schema:addressLocality"][0]
        else:
            ville = event["isLocatedAt"]["schema:address"]["schema:addressLocality"]
    else:
        ville = "Unknown"

    # Vérifie et récupère la latitude et la longitude
    latitude = event["isLocatedAt"]["schema:geo"]["schema:latitude"].get("@value", None) if "isLocatedAt" in event and "schema:geo" in event["isLocatedAt"] and "schema:latitude" in event["isLocatedAt"]["schema:geo"] else None
    longitude = event["isLocatedAt"]["schema:geo"]["schema:longitude"].get("@value", None) if "isLocatedAt" in event and "schema:geo" in event["isLocatedAt"] and "schema:longitude" in event["isLocatedAt"]["schema:geo"] else None


    # Vérifie et récupère la description
    description = None
    if "rdfs:comment" in event and "@value" in event["rdfs:comment"]:
        description = event["rdfs:comment"]["@value"]

    # Création d'un dictionnaire avec les données adaptées
    adapted_event = {
        "id": unique_id,
        "titre": title,
        "ville": ville,
        "latitude": latitude,
        "longitude": longitude,
        "date_debut": start_date,
        "date_fin": end_date,
        "description": description,
        "type": {"motcle": motscles},
        "score": 0,
        "ts_entree": datetime.now().isoformat(),
        "source": event.get("@id", None),
        "image_url": image_url,
    }
    return adapted_event, None

def retrieve_date(event, date_key):
    if date_key in event:
        if isinstance(event[date_key], list):
            for date_obj in event[date_key]:
                if "@value" in date_obj:
                    date_str = date_obj["@value"]
                    if 'T' in date_str:
                        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S').date().isoformat()
                    else:
                        return datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
        else:
            date_str = event[date_key].get("@value", None)
            if date_str is not None:
                if 'T' in date_str:
                    return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S').date().isoformat()
                else:
                    return datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
    return None

# Cette fonction insère un événement dans BigQuery
def insert_into_bigquery(event):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    rows_to_insert = [
        event,
    ]

    errors = client.insert_rows_json(table, rows_to_insert)  # API request

    if errors != []:
        print(errors)
        assert errors == []

#===================================================================================================
#                                          TEST DE SIMILARITE
#===================================================================================================

# La fonction jaccard_similarity() calcule la similarité de Jaccard entre deux listes.
# La fonction calculate_event_similarity() utilise cette fonction, ainsi que d'autres critères,
# pour calculer un score de similarité global entre deux événements.

from datetime import datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np

def jaccard_similarity(list1, list2):
    s1 = set(list1)
    s2 = set(list2)
    return len(s1.intersection(s2)) / len(s1.union(s2))

def calculate_event_similarity(event1, event2):
    # Comparaison des titres
    title_similarity = jaccard_similarity(event1["titre"].split(), event2["titre"].split())
    
    # Comparaison des villes
    city_similarity = 1 if event1["ville"].lower() == event2["ville"].lower() else 0
    
    # Comparaison des dates de début et de fin
    date_format = "%Y-%m-%d"
    start_date_diff = abs((datetime.strptime(event1["date_debut"], date_format) - datetime.strptime(event2["date_debut"], date_format)).days)
    end_date_diff = abs((datetime.strptime(event1["date_fin"], date_format) - datetime.strptime(event2["date_fin"], date_format)).days)
    
    # Normalisation des différences de dates (supposons qu'une différence de 30 jours est considérée comme une différence maximale)
    start_date_similarity = 1 - (start_date_diff / 30)
    end_date_similarity = 1 - (end_date_diff / 30)
    
    # Calcul du score de similarité final comme la moyenne des scores de similarité individuels
    final_similarity = np.mean([title_similarity, city_similarity, start_date_similarity, end_date_similarity])
    print(f"Le score de similarité entre les deux événements est de : {final_similarity*100:.2f}%")
    return final_similarity

#===================================================================================================
# *                                         MAIN
#===================================================================================================

# Cette fonction est le point d'entrée du programme
def main():
    # Récupération des données JSON
    fetchedData = fetch(url)
    print("Données récupérées!")

    adapted_events = []  # Liste vide pour stocker les événements adaptés
    ignored_events = []  # Liste vide pour stocker les événements ignorés
    # Parcours de chaque événement, adaptation de l'événement et insertion dans BigQuery
    for event in fetchedData["@graph"]:
        adapted_event, ignored_event = adapt_event(event)
        if adapted_event is not None:  # Ignore les valeurs None
            #insert_into_bigquery(adapted_event)
            adapted_events.append(adapted_event)
            #print("Événement adapté")
        if ignored_event is not None:  # Ajoutez l'événement ignoré à la liste si une KeyError s'est produite
            ignored_events.append(ignored_event)

    # Écrire tous les événements adaptés dans un fichier JSON
    with open("fetes.json", "w", encoding="utf-8") as f:
        json.dump(adapted_events, f, ensure_ascii=False)

    # Écrire tous les événements ignorés dans un autre fichier JSON
    # with open("ignored_events.json", "w", encoding="utf-8") as f:
    #     json.dump(ignored_events, f, ensure_ascii=False)

    # print("Tous les événements adaptés ont été écrits dans adapted_events.json")
    # print("Tous les événements ignorés ont été écrits dans ignored_events.json")

#? Exécute main() si ce script est exécuté en utilisant python datatourismev2.py
if __name__ == '__main__':
    
    main()

    #===========================
    #  TEST DE LA SIMILARITE
    #===========================
    # event1 = {
    # "id": None,
    # "titre": "FETE DE LA SAINT MAURICE",
    # "ville": "Épinal",
    # "latitude": "48.1692457",
    # "longitude": "6.448174",
    # "date_debut": "2023-09-23",
    # "date_fin": "2023-10-15",
    # "description": "La fête foraine de la Saint-Maurice prend place au Champ de Mars pour sa 182ème édition...",
    # "motscles": ["schema:Event", "schema:Festival", "CulturalEvent", "EntertainmentAndEvent", "Festival", "PointOfInterest", "urn:resource"],
    # "score": 0,
    # "ts_entree": "2023-07-11T16:09:55.181204",
    # "source": "https://data.datatourisme.fr/10/425800ba-e774-3379-a80f-ab2fa4819ff7",
    # "image_url": "https://opendata.sitlor.fr/photos/778/778000908_4_800x600.jpeg"
    # }

    # event2 = {
    #     "id": None,
    #     "titre": "FERIA DE LA SAINT MAURICE",
    #     "ville": "Épinal",
    #     "latitude": "48.1692457",
    #     "longitude": "6.448174",
    #     "date_debut": "2023-09-24",
    #     "date_fin": "2023-10-16",
    #     "description": "La fête foraine de la Saint-Maurice prend place au Champ de Mars pour sa 182ème édition...",
    #     "motscles": ["schema:Event", "schema:Festival", "CulturalEvent", "EntertainmentAndEvent", "Festival", "PointOfInterest", "urn:resource"],
    #     "score": 0,
    #     "ts_entree": "2023-07-11T16:09:55.181204",
    #     "source": "https://data.datatourisme.fr/10/425800ba-e774-3379-a80f-ab2fa4819ff7",
    #     "image_url": "https://opendata.sitlor.fr/photos/778/778000908_4_800x600.jpeg"
    # }
    
    # calculate_event_similarity(event1, event2)
