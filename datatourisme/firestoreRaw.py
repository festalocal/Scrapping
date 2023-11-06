import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import requests
url=''
cred=credentials.Certificate('festalocal-bd4613184dd8.json')
app=firebase_admin.initialize_app(cred)
db=firestore.client()
def fetch(url):
    response = requests.get(url)
    data = response.json()
    return data
url = "https://diffuseur.datatourisme.fr/webservice/49eebc93390a819eb8c2a0f95a150916/93cac18d-5c3e-4101-b0f5-db0c94423c88"

