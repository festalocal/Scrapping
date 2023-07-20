#===================================================================================================
# *                                         IMPORT
#===================================================================================================
from markupsafe import escape                  # Cette bibliothèque permet de sécuriser des caractères spécifiques pour qu'ils ne soient pas interprétés de manière malveillante dans les chaînes HTML.
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

#Mots interdits à définir 
blackList = [
    "contes",
    "théâtre",
    "lecture",
    "jeux de société",
    "opéra",
    "comédie",
    "visite",
    "exposition",
    "conférence",
    "balade",
    "promenade",
    "randonnée",
    "pédestre",
    "jeux vidéo",
    "nature",
    "théâtralisée",
    "livre",
    "performance",
    "running",
    "crossfit",
    "poésie",
    "collecte de sang",
    "cinéma",
    "goûter",
    "concert njp",
    "bien-être",
    "trail",
    "commémoratif",
    "tous en selle",
    "cérémonie",
    "projection",
    "triathlon",
    "jeu de piste",
    "sortie nature",
    "trésor",
    "abbaye",
    "portes ouvertes",
    "téléthon",
    "trial",
    "art",
    "contes",
    "theatre",
    "lecture",
    "jeux de societe",
    "opera",
    "comedie",
    "visite",
    "exposition",
    "conference",
    "balade",
    "promenade",
    "randonnee",
    "pedestre",
    "jeux video",
    "nature",
    "theatralisee",
    "livre",
    "performance",
    "running",
    "crossfit",
    "poesie",
    "collecte de sang",
    "cinema",
    "gouter",
    "concert",
    "bien-etre",
    "trail",
    "commemoratif",
    "tous en selle",
    "ceremonie",
    "projection",
    "triathlon",
    "jeu de piste",
    "sortie nature",
    "tresor",
    "abbaye",
    "portes ouvertes",
    "telethon",
    "trial",
    "art"
]


# URL du fichier JSON-LD
url = "https://diffuseur.datatourisme.fr/webservice/49eebc93390a819eb8c2a0f95a150916/93cac18d-5c3e-4101-b0f5-db0c94423c88"

# Les informations d'authentification pour BigQuery sont dans ce fichier
credentials = service_account.Credentials.from_service_account_file('festalocal-fa14bbdaf49c.json')
client = bigquery.Client(credentials=credentials)

# Spécifiez votre dataset et table BigQuery
dataset_id = 'festa'
table_id = 'evenement'


#===================================================================================================
# *                                         API#2 DATATOURISME
#===================================================================================================

def fetch(url):
    """
    Cette fonction récupère les données du fichier JSON-LD à l'URL spécifiée.

    Args:
        url (str): L'URL du fichier JSON-LD.

    Returns:
        data (dict): Un dictionnaire contenant les données du fichier JSON-LD.
    """
    # Récupéeration du fichier JSON-LD
    response = requests.get(url)
    data = response.json()

    # Sauvegarde du fichier JSON-LD dans un fichier data.jsonld
    # with open("data.jsonld", "w", encoding="utf-8") as f:
    #     json.dump(data, f, ensure_ascii=False)
    return data

def adapt_event(event):
    """
    Cette fonction adapte les données de l'événement pour être insérées dans BigQuery.

    Args:
        event (dict): Un dictionnaire contenant les détails de l'événement.

    Returns:
        adapted_event (dict) or None: Un dictionnaire contenant les détails de l'événement adaptés pour BigQuery, ou None si l'événement contient un mot de la liste noire dans son titre.
        event (dict) or None: L'événement original non modifié, ou None si les données ont été adaptées avec succès.
    """
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
        #print(f"Type: {type(ressource)}, Value: {ressource}")
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
    """
    Cette fonction extrait une date d'un événement en fonction de la clé de la date donnée.

    Args:
        event (dict): L'événement d'où la date est à extraire.
        date_key (str): La clé utilisée pour récupérer la date dans l'événement.

    Returns:
        str: Une représentation string de la date au format ISO si elle est trouvée, sinon None.
    """
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

def insert_into_bigquery(event):
    """
    Cette fonction insère un événement dans une table spécifique de BigQuery.

    Args:
        event (dict): L'événement à insérer dans la table BigQuery.

    """
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

def jaccard_similarity(list1, list2):
    """
    Cette fonction calcule la similarité de Jaccard entre deux listes.
    
    Args:
        list1 (list): Première liste à comparer.
        list2 (list): Deuxième liste à comparer.

    Returns:
        float: Score de similarité de Jaccard entre les deux listes.
    """
    s1 = set(list1)
    s2 = set(list2)
    return len(s1.intersection(s2)) / len(s1.union(s2))

def calculate_event_similarity(event1, event2):
    """
    Cette fonction calcule un score de similarité global entre deux événements en utilisant la similarité de Jaccard
    et d'autres critères (comparaison des titres, des villes, des dates de début et de fin).
    
    Args:
        event1 (dict): Premier événement à comparer.
        event2 (dict): Deuxième événement à comparer.

    Returns:
        float: Score de similarité entre les deux événements.
    """

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
# *                                    PROCESS_EVENT_DATA
#===================================================================================================

def process_event_data(url):
    """
    Cette fonction récupère les données JSON depuis une URL spécifiée, adapte chaque événement et retourne une liste 
    d'événements adaptés sous forme JSON.
    
    Args:
        url (str): L'URL depuis laquelle récupérer les données JSON.

    Returns:
        json: Liste d'événements adaptés sous forme JSON.
    """
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
    return adapted_events, ignored_events

#===================================================================================================
# *                                         CLOUD_FUNCTION
#===================================================================================================

# Cette fonction est le point d'entrée de la fonction Cloud. Elle traite la requête HTTP reçue.
def cloud_function(request):
    """
    Cette fonction est le point d'entrée pour le service Cloud. Elle traite la requête HTTP reçue,
    extrait l'URL depuis les paramètres de la requête, et récupère les données depuis cette URL.
    
    Args:
        request (flask.Request): La requête HTTP reçue.

    Returns:
        json: Liste d'événements adaptés sous forme JSON ou un message d'erreur si l'URL n'est pas fournie dans la requête.
    """
    
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
    data = process_event_data(url)
    return data

#===================================================================================================
# *                                         TEST DES FONCTIONS
#===================================================================================================
if __name__ == "__main__":
    #---------------------------
    #     TEST DU BLACKLIST
    #---------------------------
    (filtered, not_filtered) = process_event_data(url)

    with open('filtered.json', 'w', encoding='utf-8') as f:
        json.dump(filtered, f, ensure_ascii=False, indent=4)

    with open('not_filtered.json', 'w', encoding='utf-8') as f:
        json.dump(not_filtered, f, ensure_ascii=False, indent=4)

    #---------------------------
    #     TEST DU WHITELIST
    #---------------------------


"""
- faire la meme chose avec la whitelist 
- tout verifier 
- envoyer les json
"""