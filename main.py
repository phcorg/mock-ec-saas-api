import functions_framework
from google.cloud import firestore

# Register an HTTP function with the Functions Framework
@functions_framework.http
def my_http_function(request):
    #only allow POST method
    if request.method == 'POST':
        try:
            data_storage = request.get_json()
            #store into firestore
            db = firestore.Client(project="pho-mock-ec-sass")
            doc_ref = db.collection(data_storage["event_name"]).document(data_storage["uuid"])
            doc_ref.set(data_storage)

            # Return HTTP response
            return "ok", 200
        except:
            return "Internal Service Error", 500
    else:
        return "Method Not Allowed", 405