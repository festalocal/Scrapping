import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
# URL du fichier JSON-LD
url = "https://diffuseur.datatourisme.fr/webservice/49eebc93390a819eb8c2a0f95a150916/93cac18d-5c3e-4101-b0f5-db0c94423c88"

cred =credentials.Certificate('festalocal-bd4613184dd8.json')

app = firebase_admin.initialize_app(cred)

db = firestore.client()
doc_ref = db.collection("users").document("alovelace")
doc_ref.set({"first": "Ada", "last": "Lovelace", "born": 1815})
print('user added')
