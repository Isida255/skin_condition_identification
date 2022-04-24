from __future__ import division, print_function
import sys
import os
import glob
import re
from pathlib import Path
from io import BytesIO
import base64
import requests
from datetime import datetime
import pymongo

# Import fast.ai Library
from fastai import *
from fastai.vision import *

# Flask utils
from flask import Flask, redirect, url_for, render_template, request
from PIL import Image as PILImage

# Define a flask app
app = Flask(__name__)

NAME_OF_FILE = 'model_best' # Name of your exported file
PATH_TO_MODELS_DIR = Path('') # by default just use /models in root dir
classes = ['Actinic keratoses', 'Basal cell carcinoma', 'Benign keratosis',
           'Dermatofibroma', 'Melanocytic nevi', 'Melanoma', 'Vascular lesions']

#Pymongo config

myclient = pymongo.MongoClient("mongodb+srv://admin:testAdmin@cluster0.k5ld4.mongodb.net/SkinConditionIdentification?retryWrites=true&w=majority")
#db = client.test
mydb = myclient["SkinConditionIdentification"]
mycol = mydb["UserInformation"]

def setup_model_pth(path_to_pth_file, learner_name_to_load, classes):
    data = ImageDataBunch.single_from_classes(
        path_to_pth_file, classes, ds_tfms=get_transforms(), size=224).normalize(imagenet_stats)
    learn = cnn_learner(data, models.densenet169, model_dir='models')
    learn.load(learner_name_to_load, device=torch.device('cpu'))
    return learn

learn = setup_model_pth(PATH_TO_MODELS_DIR, NAME_OF_FILE, classes)

def encode(img):
    img = (image2np(img.data) * 255).astype('uint8')
    pil_img = PILImage.fromarray(img)
    buff = BytesIO()
    pil_img.save(buff, format="JPEG")


    return base64.b64encode(buff.getvalue()).decode("utf-8")
	
def model_predict(img, username, img_name):
    img = open_image(BytesIO(img))
    pred_class,pred_idx,outputs = learn.predict(img)
    formatted_outputs = ["{:.1f}%".format(value) for value in [x * 100 for x in torch.nn.functional.softmax(outputs, dim=0)]]
    pred_probs = sorted(
            zip(learn.data.classes, map(str, formatted_outputs)),
            key=lambda p: p[1],
            reverse=True
        )
	
    img_data = encode(img)
    result = {"class":pred_class, "probs":pred_probs, "image":img_data}
    final_result = {"class":pred_class, "probs":pred_probs}
    date = datetime.now()
    # write the image in mongo
    userinfo = {"_id": username +"{}".format(date, "%m/%d/%Y,%H:%M:%S"), "username": str(username), "image": img_data, "image_name": img_name, "result":  str(final_result), "uploadDate": format(date, "%m/%d/%Y,%H:%M:%S")}
    x = mycol.insert_one(userinfo)

    # writing done
    return render_template('result.html', result=result)
   

@app.route('/', methods=['GET', "POST"])
def index():
    # Main page
    return render_template('index.html')


@app.route('/upload', methods=["POST", "GET"])
def upload():
    if request.method == 'POST':
        # Get the file from post request
        img = request.files['file'].read()
        username = request.form['email']
        f = request.files['file']
        img_name = f.filename
        

        if img != None:
        # Make prediction
            preds = model_predict(img, username, img_name)
            return preds
    return 'OK'
	


if __name__ == '__main__':
    port = os.environ.get('PORT', 8008)

    if "prepare" not in sys.argv:
        app.run(debug=False, host='0.0.0.0', port=port)