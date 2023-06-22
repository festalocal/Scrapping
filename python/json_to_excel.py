import openpyxl
from openpyxl.styles import Alignment, Font
import json
from urllib.parse import urljoin

def json_to_excel(json_data, excel_file):
    """
    Cette fonction convertit les données JSON en un fichier Excel.

    :param json_data: Les données JSON à convertir.
    :type json_data: str
    :param excel_file: Le nom du fichier Excel de sortie.
    :type excel_file: str
    :return: None
    """
    
    workbook = openpyxl.Workbook()
    sheet = workbook.active

    # Convertir le JSON en dictionnaire Python
    data = json.loads(json_data)

    # Extraire tous les événements de tous les objets JSON
    all_events = []
    for json_obj in data:
        events = json_obj['events']
        all_events.extend(events)

    # En-têtes de colonne avec leur style
    headers = [
        ('Image', 30),
        ('Titre', 30),
        ('Description', 50),
        ('Lieu', 30),
        ('Adresse', 30),
        ('Ville', 20),
        ('Latitude', 20),
        ('Longitude', 20),
        ('Dates', 20),
        ('Heure de début', 20),
        ('Heure de fin', 20),
        ('Mode de participation', 30),
        ('Mots-clés', 30),
        ('Agenda d\'origine', 30)
    ]

    # Appliquer les en-têtes de colonne et les styles
    for col_num, (header, width) in enumerate(headers, 1):
        cell = sheet.cell(row=1, column=col_num)
        cell.value = header
        cell.font = Font(bold=True)
        sheet.column_dimensions[chr(64 + col_num)].width = width

    # Écrire les valeurs des événements dans les lignes suivantes
    for row_num, event in enumerate(all_events, 2):
        image_filename = event['image']['filename']
        image_base_url = event['image']['base']
        image_url = urljoin(image_base_url, image_filename)
        title_fr = event['title'].get('fr', '')  # Utiliser get() pour éviter les KeyError
        description_fr = event['description'].get('fr', '')  # Utiliser get() pour éviter les KeyError
        location_name = event['location']['name']
        location_address = event['location']['address']
        location_city = event['location']['city']
        location_latitude = event['location']['latitude']
        location_longitude = event['location']['longitude']
        date_range_fr = event['dateRange'].get('fr', '')  # Utiliser get() pour éviter les KeyError
        begin_time = event['lastTiming']['begin'].split('T')[1][:-6]  # Extraire l'heure de début
        end_time = event['lastTiming']['end'].split('T')[1][:-6]  # Extraire l'heure de fin
        attendance_mode = event['attendanceMode']
        keywords_fr = event['keywords'].get('fr', [])  # Utiliser get() pour éviter les KeyError
        if keywords_fr is not None:
            keywords_fr = ', '.join(str(k) for k in keywords_fr if k is not None)
        origin_agenda_title = event['originAgenda']['title']

        row_values = [
            image_url,
            title_fr,
            description_fr,
            location_name,
            location_address,
            location_city,
            location_latitude,
            location_longitude,
            date_range_fr,
            begin_time,
            end_time,
            attendance_mode,
            keywords_fr,
            origin_agenda_title
        ]

        for col_num, value in enumerate(row_values, 1):
            cell = sheet.cell(row=row_num, column=col_num)
            cell.value = value
            cell.alignment = Alignment(wrap_text=True)

    # Ajuster la hauteur des lignes pour s'adapter au contenu
    for row in sheet.iter_rows(min_row=2, max_row=sheet.max_row):
        for cell in row:
            cell.alignment = Alignment(vertical='center')
            cell.alignment = Alignment(wrap_text=True)
        sheet.row_dimensions[row[0].row].height = 50

    # Enregistrer le fichier Excel
    workbook.save(excel_file)
    print("Le fichier Excel a été créé avec succès.")

if __name__ == '__main__':
    with open("events.json", encoding='utf-8') as file:
        json_data = file.read()
    json_to_excel(json_data, "data.xlsx")
