# ===================================================================================================
# *                                         IMPORT
# ===================================================================================================
from markupsafe import (
    escape,
)  # Cette bibliothèque permet de sécuriser des caractères spécifiques pour qu'ils ne soient pas interprétés de manière malveillante dans les chaînes HTML.
import requests  # Utilisé pour envoyer des requêtes HTTP.
from google.cloud import (
    bigquery,
)  # Client pour interagir avec l'API BigQuery de Google.
from datetime import datetime  # Utilisé pour manipuler les dates et les heures.
from google.cloud.bigquery import SchemaField
from datetime import datetime
import uuid  # Utilisé pour générer des identifiants uniques universels.
from flask import (
    jsonify,
)  # Utilisé pour formater les réponses à renvoyer en tant que JSON.

# from sklearn.metrics.pairwise import cosine_similarity # Utilisé pour calculer la similitude cosinus entre les échantillons pour déterminer la similitude des textes.
# from sklearn.feature_extraction.text import CountVectorizer # Transforme le texte en vecteur de tokens pour faciliter le calcul de la similarité.
import numpy as np  # Utilisé pour des calculs scientifiques et la manipulation de structures de données multidimensionnelles.
import pgeocode

# ===================================================================================================
# *                                         VARIABLES
# ===================================================================================================

# Mots interdits à définir
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
    "art",
]

# Mots autorisés
whiteList = [
    "fest-noz",
    "fest noz",
    "fest",
    "fest -deiz",
    "fest deiz",
    "feria",
    "carnaval",
    "guinguette",
    "bal",
    "bals",
    "variété française",
    "buvette",
    "buvettes",
    "fête",
    "fete",
    "fetes",
    "fête de village",
    "fête du village",
    "fête communale",
    "fanfare",
    "marché nocturne",
    "fanfare",
    "feu de la saint jean",
    "feu de la st jean",
    "feu de la st-jean",
    "feu de la saint-jean",
    "année 80",
    "années 80",
    "apéro",
    "apero",
    "fête municipale",
    "fête de l'été",
    "fête vosgienne",
    "soirée vosgienne",
]

# ===================================================================================================
# *                                         API#2 DATATOURISME
# ===================================================================================================


def fetch(url):
    """
    Cette fonction récupère les données du fichier JSON-LD à l'URL spécifiée.

    Args:
        url (str): L'URL du fichier JSON-LD.

    Returns:
        data (dict): Un dictionnaire contenant les données du fichier JSON-LD.
    """
    try:
        # Récupéeration du fichier JSON-LD
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error while fetching data: {e}")
        return {}  # Return an empty dictionary for failed requests

    except ValueError as ve:
        print(f"Error decoding JSON response: {ve}")
        return {}  # Return an empty dictionary for invalid JSON responses

    return data


# ===================================================================================================
#                                        ADAPT_EVENT
# ===================================================================================================
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
    if not all(
        key in event
        for key in ["rdfs:label", "schema:startDate", "schema:endDate", "isLocatedAt"]
    ):
        return None, event

    # Récupération du titre
    titre = event["rdfs:label"].get("@value", None)

    if not whitelist(titre, whiteList):
        return None, event

    # Si le titre de l'événement contient un mot de la liste noire, retourne None
    if blacklist(titre, blackList):
        return None, event

    # Création d'un identifiant unique pour chaque événement avec uuid
    unique_id = str(uuid.uuid4())

    # Récupération et adaptation d'autres éléments de données pour correspondre à la structure de la table BigQuery
    ressource = None
    if (
        "hasMainRepresentation" in event
        and "ebucore:hasRelatedResource" in event["hasMainRepresentation"]
    ):
        ressource = event["hasMainRepresentation"]["ebucore:hasRelatedResource"]
        # print(f"Type: {type(ressource)}, Value: {ressource}")
    if ressource and isinstance(ressource.get("ebucore:locator", {}), dict):
        image_url = ressource.get("ebucore:locator", {}).get("@value", None)
    else:
        image_url = None

    # Récupération des mots clés
    mots_cles = event.get("@type", None)

    # Récupération des dates de début et de fin
    date_debut = retrieve_date(event, "schema:startDate")
    date_fin = retrieve_date(event, "schema:endDate")

    nomi = pgeocode.Nominatim("fr")

    # Récupération de la ville, du code postal et de la région
    ville = None
    code_postal = None
    region = None
    if "isLocatedAt" in event and "schema:address" in event["isLocatedAt"]:
        address = event["isLocatedAt"]["schema:address"]

        if "schema:addressLocality" in address:
            if isinstance(address["schema:addressLocality"], list):
                ville = address["schema:addressLocality"][0]
            else:
                ville = address["schema:addressLocality"]

        if "schema:postalCode" in address:
            if isinstance(address["schema:postalCode"], list):
                code_postal = address["schema:postalCode"][0]
            else:
                code_postal = address["schema:postalCode"]

        region = nomi.query_postal_code(code_postal)
        region = region["state_name"]

        if (
            "hasAddressCity" in address
            and "isPartOfRegion" in address["hasAddressCity"]
        ):
            region_info = address["hasAddressCity"]["isPartOfRegion"]
            if "rdfs:label" in region_info and "@value" in region_info["rdfs:label"]:
                region = region_info["rdfs:label"]["@value"]

    # If ville is still None, return None, event
    if ville is None:
        return None, event
    if code_postal is None:
        code_postal = "Inconnu"
    if region is None:
        region = "Inconnue"

    # Vérification et récupération de la latitude et la longitude
    latitude = (
        event["isLocatedAt"]["schema:geo"]["schema:latitude"].get("@value", None)
        if "isLocatedAt" in event
        and "schema:geo" in event["isLocatedAt"]
        and "schema:latitude" in event["isLocatedAt"]["schema:geo"]
        else None
    )
    longitude = (
        event["isLocatedAt"]["schema:geo"]["schema:longitude"].get("@value", None)
        if "isLocatedAt" in event
        and "schema:geo" in event["isLocatedAt"]
        and "schema:longitude" in event["isLocatedAt"]["schema:geo"]
        else None
    )

    # Vérification et récupération de la description
    description = None
    if "rdfs:comment" in event and "@value" in event["rdfs:comment"]:
        description = event["rdfs:comment"]["@value"]

    categorie = get_categorie(titre)

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
        "categorie": categorie,
        "cp": code_postal,
        "region": region,
    }
    return adapted_event, None


# ===================================================================================================
#                                      RETRIEVE_DATE
# ===================================================================================================
def retrieve_date(event, date_key):
    """
    Cette fonction extrait une date d'un événement en fonction de la clé de la date donnée.

    Args:
        event (dict): L'événement d'où la date est à extraire.
        date_key (str): La clé utilisée pour récupérer la date dans l'événement.

    Returns:
        str: Une représentation string de la date au format DD-MM-YYYY si elle est trouvée, sinon None.
    """
    if date_key in event:
        if isinstance(event[date_key], list):
            for date_obj in event[date_key]:
                if "@value" in date_obj:
                    date_str = date_obj["@value"]
                    if "T" in date_str:
                        return (
                            datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
                            .date()
                            .isoformat()
                        )
                    else:
                        return (
                            datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
                        )
        else:
            date_str = event[date_key].get("@value", None)
            if date_str is not None:
                if "T" in date_str:
                    return (
                        datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
                        .date()
                        .isoformat()
                    )
                else:
                    return datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
    return None


# ===================================================================================================
#                                      CHECK_FOR_DUPLICATES
# ===================================================================================================


def check_for_duplicates():
    """
    Cette fonction vérifie si un événement est un doublon en le comparant avec les événements existants dans la base de données.
    Utilise une structure de données Set pour un accès plus rapide.

    Returns:
        set: Ensemble des identifiants des événements déjà présents dans la base de données.
    """
    client = bigquery.Client()
    dataset_id = "festa"
    table_id = "evenement"
    query = f"SELECT source FROM `{dataset_id}.{table_id}`"
    result = client.query(query).result()
    # Transforme le résultat en un ensemble (set) pour une vérification plus rapide des doublons
    existing_ids = set(row.get("source", "") for row in result)
    return existing_ids


# ===================================================================================================
#                                      INSERT_INTO_BIGQUERY
# ===================================================================================================


def insert_into_bigquery(event):
    """
    Cette fonction insère un événement dans une table spécifique de BigQuery s'il n'est pas un doublon.

    Args:
        event (dict): L'événement à insérer dans la table BigQuery.

    """

    try:
        # Utiliser les informations d'identification par défaut
        client = bigquery.Client()

        # Spécifiez votre dataset et table BigQuery
        dataset_id = "festa"
        table_id = "evenement"

        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        rows_to_insert = [
            event,
        ]

        errors = client.insert_rows_json(table, rows_to_insert)  # Requête API

        if errors != []:
            print(errors)
            raise Exception(
                "Une erreur s'est produite lors de l'insertion des lignes dans BigQuery."
            )

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")


# ===================================================================================================
#                                       DELETE_EXPIRED_EVENTS
# ===================================================================================================


def delete_expired_events():
    """
    Cette fonction supprime tous les événements de la table BigQuery dont la date de début est strictement inférieure à la date actuelle.

    """

    # Utiliser les informations d'identification par défaut
    client = bigquery.Client()

    # Spécifiez votre dataset et table BigQuery
    dataset_id = "festa"
    table_id = "evenement"

    # Obtenir la date actuelle
    date_actuelle = datetime.now().date().isoformat()

    # Construction de la requête pour compter les événements à supprimer
    query_count = f"""
        SELECT COUNT(*)
        FROM `{dataset_id}.{table_id}`
        WHERE date_fin < DATE '{date_actuelle}' AND DATE(ts_entree) < DATE '{date_actuelle}'
    """

    # Exécution de la requête de comptage
    query_job = client.query(query_count)  # API request
    result = query_job.result()

    # Récupération du nombre d'événements à supprimer
    for row in result:
        count_before = row[0]

    print(f"Il y a {count_before} événements qui vont être supprimés.")

    # Construction de la requête de suppression
    query_delete = f"""
        DELETE FROM `{dataset_id}.{table_id}`
        WHERE date_fin < DATE '{date_actuelle}' AND DATE(ts_entree) < DATE '{date_actuelle}'
    """

    # Exécution de la requête de suppression
    query_job = client.query(query_delete)  # API request
    result = query_job.result()

    # Exécution de nouveau la requête de comptage
    query_job = client.query(query_count)  # API request
    result = query_job.result()

    # Récupération du nombre d'événements après la suppression
    for row in result:
        count_after = row[0]

    print(f"Il y a maintenant {count_after} événements concernés après la suppression.")
    print(
        "Les événements avec une date de début inférieure à la date actuelle ont été supprimés."
    )


# ===================================================================================================
#                                          WHITELIST
# ===================================================================================================
def whitelist(event_title, list_words):
    """
    Cette fonction vérifie si l'événement doit être retenu en fonction du titre.
    Chaque élément dans list_words est considéré en entier.

    Args:
        event_title (str): Le titre de l'événement.
        list_words (list): Une liste de mots ou groupes de mots à vérifier.

    Returns:
        bool: True si l'événement doit être retenu, False sinon.
    """
    lower_event_title = event_title.lower()
    title_words = lower_event_title.split()

    for word_group in list_words:
        words = word_group.lower().split()
        for i in range(len(title_words) - len(words) + 1):
            if title_words[i : i + len(words)] == words:
                return True
    return False


# ===================================================================================================
#                                          BLACKLIST
# ===================================================================================================
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


# ===================================================================================================
#                                          GET_CATEGORIE
# ===================================================================================================
def get_categorie(title):
    """
    Cette fonction répère le mot essentiel dans un titre.

    Args:
        title (str): Le titre à analyser.

    Returns:
        str: Le mot essentiel trouvé dans le titre, sinon "Autre".
    """

    # Liste des mots essentiels
    essential_words = [
        "fest-noz",
        "feria",
        "carnaval",
        "guinguette",
        "festival",
        "foire artisanale",
        "fête du village",
    ]

    # Convertir le titre en minuscules
    lower_title = title.lower()

    # Parcourir chaque phrase dans essential_words
    for phrase in essential_words:
        # Vérifier si la phrase est dans le titre
        if phrase in lower_title:
            return phrase

    # Si aucune phrase essentielle n'a été trouvée, retourner "Autre"
    return "autre"


# ===================================================================================================
#                                          TEST DE SIMILARITE
# ===================================================================================================


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
    title_similarity = jaccard_similarity(
        event1["titre"].split(), event2["titre"].split()
    )

    # Comparaison des villes
    city_similarity = 1 if event1["ville"].lower() == event2["ville"].lower() else 0

    # Comparaison des dates de début et de fin
    date_format = "%Y-%m-%d"
    start_date_diff = abs(
        (
            datetime.strptime(event1["date_debut"], date_format)
            - datetime.strptime(event2["date_debut"], date_format)
        ).days
    )
    end_date_diff = abs(
        (
            datetime.strptime(event1["date_fin"], date_format)
            - datetime.strptime(event2["date_fin"], date_format)
        ).days
    )

    # Normalisation des différences de dates (supposons qu'une différence de 30 jours est considérée comme une différence maximale)
    start_date_similarity = 1 - (start_date_diff / 30)
    end_date_similarity = 1 - (end_date_diff / 30)

    # Calcul du score de similarité final comme la moyenne des scores de similarité individuels
    final_similarity = np.mean(
        [title_similarity, city_similarity, start_date_similarity, end_date_similarity]
    )
    print(
        f"Le score de similarité entre les deux événements est de : {final_similarity*100:.2f}%"
    )
    return final_similarity


# ===================================================================================================
# *                                    PROCESS_EVENT_DATA
# ===================================================================================================


def process_event_data(url):
    """
    Cette fonction récupère les données JSON depuis une URL spécifiée, adapte chaque événement et retourne une liste
    d'événements adaptés sous forme JSON.
    Elle effectue également une vérification des doublons avant d'insérer un événement.

    Args:
        url (str): L'URL depuis laquelle récupérer les données JSON.

    Returns:
        json: Liste d'événements adaptés sous forme JSON.
    """
    fetchedData = fetch(url)
    print("Données récupérées avec succès!")

    existing_ids = check_for_duplicates()

    # Séparation des événements en deux listes : nouveaux événements et événements déjà existants
    new_events = [
        event
        for event in fetchedData["@graph"]
        if event.get("@id", None) not in existing_ids
    ]
    existing_events = [
        event
        for event in fetchedData["@graph"]
        if event.get("@id", None) in existing_ids
    ]

    print(f"Il y a {len(new_events)} événements dans new_events")
    print(f"Il y a {len(existing_events)} événements dans existing_events")

    adapted_events = []
    ignored_events = []

    for event in new_events:
        adapted_event, ignored_event = adapt_event(event)
        if adapted_event is not None:
            # print(adapted_event)
            insert_into_bigquery(adapted_event)
            adapted_events.append(adapted_event)
        if ignored_event is not None:
            # insert_into_bigquery(ignored_event)
            print(ignored_event)
            ignored_events.append(ignored_event)

    # print(ignored_events)
    return "Les événements existants ont été ignorés. L'insertion est terminée !"


# ===================================================================================================
# *                                    CATEGORIZE_FESTIVAL
# ===================================================================================================
def categorize_festival(title: str, description: str) -> str:
    keywords = {
        "Feria": ["feria", "féria", "ferias"],
        "Fest-noz": ["fest-noz", "fest noz", "noz", "deiz"],
        "Carnaval": ["carnaval"],
        "Fête de village": [
            "village",
            "communal",
            "communale",
            "villageois",
            "municipal",
            "municipale",
            "fête locale",
            "fête votive",
            "fête patronale",
            "en fête",
            "fete locale",
            "fete votive",
            "fete patronale",
        ],
        "Festival": ["festival", "estival"],
        "Guinguette": ["guinguette", "apéro", "apero"],
        "Bal populaire": ["bal", "folk", "bals", "folks"],
        "Foire artisanale": ["foire artisanale", "foire", "marché", "marché nocturne"],
        "fête médiévale": ["médievale", "médiévale", "medieval"],
        "Autres fêtes populaires": [],
    }

    food_keywords = [
        "jambon",
        "ananas",
        "vin",
        "fromage",
        "pain",
        "poulet",
        "agneau",
        "fruit",
        "légume",
        "bière",
        "huîtres",
        "saucisson",
        "truffe",
        "charcuterie",
        "chocolat",
        "confiture",
        "miel",
        "moutarde",
        "olive",
        "pâté",
        "pâtisserie",
        "crêpe",
        "galette",
        "gastronomie",
        "cuisine",
        "terroir",
        "châtaigne",
        "champignon",
        "cidre",
        "saucisse",
        "rôti",
        "poisson",
        "seafood",
        "mer",
        "coquillage",
        "crustacé",
        "fruit de mer",
        "moule",
        "huître",
        "poisson",
        "homard",
        "canard",
        "foie gras",
        "porc",
        "veau",
        "boeuf",
        "agneau",
        "mouton",
        "légumes",
        "fruits",
        "salade",
        "tomate",
        "oignon",
        "ail",
        "épice",
        "pomme",
        "poire",
        "cerise",
        "fraise",
        "framboise",
        "mûre",
        "myrtille",
        "pêche",
        "abricot",
        "prune",
        "raisin",
        "vigne",
        "fromage",
        "yaourt",
        "lait",
        "beurre",
        "crème",
        "riz",
        "pâtes",
        "gnocchi",
        "lasagne",
        "pizza",
        "tarte",
        "quiche",
        "cake",
        "biscuit",
        "gâteau",
        "glace",
        "sorbet",
        "miel",
        "sucre",
        "confiserie",
        "bonbon",
        "chocolat",
        "caramel",
        "nougat",
        "praliné",
        "vin",
        "bière",
        "cidre",
        "liqueur",
        "eau-de-vie",
        "spiritueux",
        "cocktail",
        "café",
        "thé",
        "infusion",
        "jus",
        "soda",
        "limonade",
        "eau",
        "boisson",
        "champagne",
        "caviar",
        "truffe",
        "boulangerie",
        "pâtisserie",
    ]

    all_text = (title + " " + description).lower()

    determinants = ["du", "de", "la", "le", "des"]

    #------------------------
    # ? FETE GASTRONOMIQUE
    #---------------------------
    words = all_text.split()
    for i in range(2, len(words)):  # commencer à 2 car on vérifie toujours 2 mots avant
        if (
            words[i] in food_keywords
            and words[i - 1] in determinants
            and words[i - 2] == "fête"
        ):
            return "Fête gastronomique"
    
    #------------------------
    # ?       FETES
    #---------------------------

    for category, keys in keywords.items():
        for key in keys:
            if key in all_text:
                return category
    
    #------------------------
    # ?        SI RIEN
    #---------------------------
    return "Autres fêtes populaires"

# ===================================================================================================
#                                         CLOUD_FUNCTION
# ===================================================================================================
def cloud_function(request):
    """
    The entry point for the Cloud Function. It processes the received HTTP request,
    extracts the API key from the request headers, and fetches data using that API key.

    Args:
        request (flask.Request): The received HTTP request.

    Returns:
        json: List of events in JSON format or an error message if the API key is not provided in the request headers.
    """

    # Base URL
    base_url = "https://diffuseur.datatourisme.fr/webservice/"

    # Extract the API key from the request headers
    key = request.headers.get(
        "X-API-Key"
    )  # Change 'X-API-Key' to the actual header name you want to use

    if not key:
        # If the API key is not provided in the request headers, return an error message
        return (
            jsonify(
                {
                    "error": "API key not provided. Please include your API key in the request headers."
                }
            ),
            400,
        )

    # Concatenate the base URL with the API key to form the complete URL
    url = base_url + key

    # Suppression des événements expirés
    delete_expired_events()

    # Call the main function with the URL to fetch data and return the result in JSON format
    process_event_data(url)
    print("Terminé !")
    return "Terminé !"

if __name__ == '__main__':
    # Test de la fonction
    print(
        categorize_festival(
            "La fête du jambon", "Venez découvrir notre délicieux jambon local!"
        )
    )
    print(
        categorize_festival(
            "Fête de l'ananas", "Une célébration dédiée à l'ananas juteux et délicieux"
        )
    )
