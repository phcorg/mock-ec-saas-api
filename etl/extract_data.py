import firebase_admin
from firebase_admin import firestore

# Application Default credentials are automatically created.
app = firebase_admin.initialize_app()
db = firestore.client()

#store data in .json according to their original document
def extract_document(event_name):
    data = []
    #get all document from the collection
    docs = db.collection(event_name).stream()
    for doc in docs:
        data.append(doc.to_dict())
    return data
    #print(f"{doc.id} => {doc.to_dict()}")