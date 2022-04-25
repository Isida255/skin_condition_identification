import base64 as b64
from datetime import time
from typing import BinaryIO
import os
from flask_restful import Api, Resource
import requests as requests
from flask import Flask, render_template, request, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime
import json

from spark_clouddb import write_json, resultread, result_byusername, resultread_total, resultread_total_grouped 



app = Flask(__name__)

def allresults(username):
    allResult= resultread_total_grouped()
    usernameResult= result_byusername(username)
    return render_template('result_superUser.html', resultall=allResult, result=usernameResult)
    
    




@app.route('/', methods=['GET', "POST"])
def index():
    # Main page

    return render_template('index_superUser.html')
 

@app.route('/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        username = request.form['email']
        
        
        if username != None:
        
            
            allResults = allresults(username)
            return allResults

        
    


if __name__ == '__main__':
    app.run(debug=True)
