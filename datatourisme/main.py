#===================================================================================================
# *                                         IMPORT
#===================================================================================================
from flask import escape                  # Cette bibliothèque permet de sécuriser des caractères spécifiques pour qu'ils ne soient pas interprétés de manière malveillante dans les chaînes HTML.
import requests                           # Utilisé pour envoyer des requêtes HTTP.
import json                               # Permet de travailler avec des objets JSON. Utilisé pour la sérialisation et la désérialisation de JSON.
from google.cloud import bigquery         # Client pour interagir avec l'API BigQuery de Google.
from datetime import datetime             # Utilisé pour manipuler les dates et les heures.
from google.oauth2 import service_account # Utilisé pour l'authentification avec un compte de service Google.
import uuid                               # Utilisé pour générer des identifiants uniques universels.
from flask import jsonify                 # Utilisé pour formater les réponses à renvoyer en tant que JSON.
from sklearn.metrics.pairwise import cosine_similarity # Utilisé pour calculer la similitude cosinus entre les échantillons pour déterminer la similitude des textes.
from sklearn.feature_extraction.text import CountVectorizer # Transforme le texte en vecteur de tokens pour faciliter le calcul de la similarité.
import numpy as np                        # Utilisé pour des calculs scientifiques et la manipulation de structures de données multidimensionnelles.

#===================================================================================================
# *                                         VARIABLES
#===================================================================================================
blackList = []

#===================================================================================================
# *                                         API#2 DATATOURISME
#===================================================================================================

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
    # Vérifier la présence des clés requises
    if not all(key in event for key in ["rdfs:label", "schema:startDate", "schema:endDate", "isLocatedAt"]):
        return None, event

    # Récupération du titre
    titre = event["rdfs:label"].get("@value", None)

    # Si le titre de l'événement contient un mot de la liste noire, retourne None
    if blacklist(titre, blackList):
        return None, event

    # Création d'un identifiant unique pour chaque événement avec uuid
    unique_id = str(uuid.uuid4())

    # Récupération et adaptation d'autres éléments de données pour correspondre à la structure de la table BigQuery
    ressource = None
    if "hasMainRepresentation" in event and "ebucore:hasRelatedResource" in event["hasMainRepresentation"]:
        ressource = event["hasMainRepresentation"]["ebucore:hasRelatedResource"]
        print(f"Type: {type(ressource)}, Value: {ressource}")
    if ressource and isinstance(ressource.get("ebucore:locator", {}), dict):
        image_url = ressource.get("ebucore:locator", {}).get("@value", None)
    else:
        image_url = None

    # Récupération des mots clés
    mots_cles = event.get("@type", None)

    # Récupération des dates de début et de fin
    date_debut = retrieve_date(event, "schema:startDate")
    date_fin = retrieve_date(event, "schema:endDate")

    # Récupération de la ville
    ville = None
    if "isLocatedAt" in event and "schema:address" in event["isLocatedAt"] and "schema:addressLocality" in event["isLocatedAt"]["schema:address"]:
        if isinstance(event["isLocatedAt"]["schema:address"]["schema:addressLocality"], list):
            ville = event["isLocatedAt"]["schema:address"]["schema:addressLocality"][0]
        else:
            ville = event["isLocatedAt"]["schema:address"]["schema:addressLocality"]
    else:
        ville = "Inconnue"

    # Vérification et récupération de la latitude et la longitude
    latitude = event["isLocatedAt"]["schema:geo"]["schema:latitude"].get("@value", None) if "isLocatedAt" in event and "schema:geo" in event["isLocatedAt"] and "schema:latitude" in event["isLocatedAt"]["schema:geo"] else None
    longitude = event["isLocatedAt"]["schema:geo"]["schema:longitude"].get("@value", None) if "isLocatedAt" in event and "schema:geo" in event["isLocatedAt"] and "schema:longitude" in event["isLocatedAt"]["schema:geo"] else None

    # Vérification et récupération de la description
    description = None
    if "rdfs:comment" in event and "@value" in event["rdfs:comment"]:
        description = event["rdfs:comment"]["@value"]

    # Création d'un dictionnaire avec les données adaptées
    adapted_event = {
        "id": unique_id,
        "titre": titre,
        "ville": ville,
        "latitude": latitude,
        "longitude": longitude,
        "date_debut": date_debut,
        "date_fin": date_fin,
        "description": description,
        "type": {"motcle": mots_cles},
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
#                                          BLACKLIST
#===================================================================================================
def blacklist(event_title, list):
    """
    Cette fonction vérifie si l'événement doit être ignoré en fonction du titre.
    Args:
        event_title (str): Le titre de l'événement.
        list (list): Une liste de mots interdits.

    Returns:
        bool: True si l'événement doit être ignoré, False sinon.
    """
    event_title_words = event_title.lower().split()
    for word in event_title_words:
        if word in list:
            return True
    return False

#===================================================================================================
#                                          TEST DE SIMILARITE
#===================================================================================================

# La fonction jaccard_similarity() calcule la similarité de Jaccard entre deux listes.
# La fonction calculate_event_similarity() utilise cette fonction, ainsi que d'autres critères,
# pour calculer un score de similarité global entre deux événements.

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

def main(url):
    # On récupère les données JSON depuis l'URL spécifiée
    fetchedData = fetch(url)
    print("Données récupérées avec succès!")

    # On crée deux listes vides : une pour stocker les événements adaptés et une autre pour ceux qui sont ignorés
    adapted_events = []  
    ignored_events = []  

    # On parcourt chaque événement dans les données récupérées
    for event in fetchedData["@graph"]:
        # On adapte l'événement et on le stocke soit dans la liste des événements adaptés, soit dans celle des événements ignorés
        adapted_event, ignored_event = adapt_event(event)
        if adapted_event is not None:  # On ignore les valeurs None
            #insert_into_bigquery(adapted_event)  # Ligne commentée pour éviter l'insertion dans BigQuery durant les tests
            adapted_events.append(adapted_event)
            #print("Événement adapté et ajouté à la liste des événements adaptés")
        if ignored_event is not None:  # Si une KeyError s'est produite, l'événement est ajouté à la liste des événements ignorés
            ignored_events.append(ignored_event)
    # On retourne la liste des événements adaptés en format JSON
    return jsonify(adapted_events)

#===================================================================================================
# *                                         PROCESS_DATA
#===================================================================================================

# Cette fonction est le point d'entrée de la fonction Cloud. Elle traite la requête HTTP reçue.
def process_data(request):
    
    # On récupère les données JSON de la requête
    request_json = request.get_json(silent=True)
    request_args = request.args

    # On vérifie si l'URL est fournie comme paramètre dans la requête
    if request_json and 'url' in request_json:
        url = request_json['url']
    elif request_args and 'url' in request_args:
        url = request_args['url']
    else:
        # Si l'URL n'est pas fournie, on retourne un message d'erreur en format JSON avec un code de statut HTTP 400
        return jsonify({'error': 'Aucun paramètre url fourni. Veuillez ajouter un paramètre "url" à votre requête.'}), 400

    # On appelle la fonction main avec l'URL pour récupérer les données et on retourne le résultat en format JSON
    data = main(url)
    return data
