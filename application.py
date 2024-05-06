from flask import Flask, Response, render_template
from pymongo import MongoClient
from bson.json_util import dumps
from bson.objectid import ObjectId

app = Flask(__name__)

# Establish MongoDB Connection
client = MongoClient("mongodb://localhost:27017/twitterdata.twitterinfo")
db = client["twitterdata"]
collection = db["twitterinfo"]

last_id = None  # Keep track of the last document ID you've processed

@app.route('/')
def index():
    return render_template('index.html')

def stream_data():
    global last_id
    while True:
        query = {"_id": {"$gt": ObjectId(last_id)}} if last_id else {}
        cursor = collection.find(query).sort('_id', 1)
        for doc in cursor:
            last_id = str(doc['_id'])  # Update the last ID processed
            yield f"data:{dumps(doc)}\n\n"
        # Instead of sleeping, rely on the client to handle the update rate.

@app.route('/stream')
def stream():
    return Response(stream_data(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True, threaded=True, port=5001)  # Specify a different port number
