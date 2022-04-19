import base64 as b64
from datetime import time
from typing import BinaryIO
import os
import requests as requests
from flask import Flask, render_template, request, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime
import json
#import spark_clouddb
#from spark_clouddb import write_json
#from PIL import Image
from spark_clouddb_updated import write_json, resultread

# mongoengine.connect(db='SkinConditionIdentification',
#                   host='mongodb+srv://admin:testAdmin@cluster0.k5ld4.mongodb.net/SkinConditionIdentification?retryWrites=true&w=majority')


app = Flask(__name__)

UPLOAD_FOLDER = r'C:\Users\isida\PycharmProjects\FinalProjecttest2\static\uploads'

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


@app.route('/upload')
def upload_file():
    return render_template('upload.html')


@app.route('/uploader', methods=['GET', 'POST'])
def upload_file1():
    if request.method == 'POST':
        f = request.files['file']
        f.save(secure_filename(f.filename))
        username = request.form['email']
        # image_path = os.path.abspath(f.filename)
        # print(image_path)
        # r'C:\Users\isida\PycharmProjects\FinalProjecttest2\'+f.filename
        root = r'C:\Users\isida\PycharmProjects\FinalProjecttest2'

        # As we need to get the provided python file,
        # comparing here like this
        name = f.filename
        image_path = os.path.abspath(os.path.join(root, name))
        # image_path = r'C:\Users\isida\PycharmProjects\FinalProjecttest2\ISIC_0024339.jpg'

        request_headers = {
            'Content-Type': 'application/json'
        }

        # current_time_millis = lambda: int(round(time.time() * 1000))

        def get_base64_encoded_image(image_path):
            with open(image_path, "rb") as img_file:
                return b64.b64encode(img_file.read()).decode('utf-8')

        image_base64 = get_base64_encoded_image(image_path)

        # now = current_time_millis()
        date = datetime.now()
        id = username + "{}".format(date, "%m/%d/%Y,%H:%M:%S")
        json_to_load = {"_id": id,
                        "username": username,
                        "image": image_base64,
                        "image_name": name,
                        "insertdate": date}

        final_json = json.loads(json.dumps(json_to_load, default=str))
        write_json(final_json)

        # response = requests.post( json=image_payload_body, headers=request_headers)
        #return jsonify(json_to_load)
        return resultread("isidaTest@outlook.com2022-04-17 15:55:41.431155")


if __name__ == '__main__':
    app.run(debug=True)
