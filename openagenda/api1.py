#===================================================================================================
# *                                         API#1 OPENAGENDA
#===================================================================================================

import json
import requests

def get_agendas(key, size=100, after=None, fields=None, filters=None, sort=None):
    url = "https://api.openagenda.com/v2/agendas"
    params = {"key": key, "size": size, "after": after}

    if fields:
        params["fields"] = fields

    if filters:
        for filter_key, filter_value in filters.items():
            params[filter_key] = filter_value

    if sort:
        params["sort"] = sort

    response = requests.get(url, params=params)
    data = response.json()

    if response.status_code == 200:
        return data
    else:
        print("Error: Failed to retrieve agendas.")
        return None

def get_events(agenda_uid, key, after=None, detailed=False, from_index=None, size=20, include_labels=None, include_fields=None, monolingual=None):
    url = f"https://api.openagenda.com/v2/agendas/{agenda_uid}/events"
    params = {"key": key}

    if after:
        params["after"] = after

    if detailed:
        params["detailed"] = 1

    if from_index:
        params["from"] = from_index

    if size:
        params["size"] = size

    if include_labels:
        params["includeLabels"] = include_labels

    if include_fields:
        params["includeFields"] = include_fields

    if monolingual:
        params["monolingual"] = monolingual

    response = requests.get(url, params=params)
    data = response.json()

    if response.status_code == 200:
        return data
    else:
        print("Error: Failed to retrieve events.")
        return None

if __name__ == '__main__':
    # Exemple d'utilisation
    key = "5e04ebf3b96e413499d131af52874360"
    size = 100
    after = None
    fields = ["summary"]
    filters = {"search": "", "official": 1}
    sort = "createdAt.desc"

    agendas_data = get_agendas(key, size, after, fields, filters, sort)

    # Save agendas_data to a JSON file
    with open('agendas_data.json', 'w') as file:
        json.dump(agendas_data, file)

    if agendas_data:
        # Traitement des données des agendas
        for agenda in agendas_data["agendas"]:
            print(f"Agenda: {agenda['uid']}")
            agenda_uid = agenda["uid"]
            events_data = get_events(agenda_uid, key)
            if events_data and "events" in events_data:
                # Traitement des données des événements
                for event in events_data["events"]:
                    print(f"Event: {event['uid']} - {event['title']}")
            print("--------------------")

    # Save events_data to a JSON file
    with open('events_data.json', 'w') as file:
        json.dump(events_data, file)

    
    
