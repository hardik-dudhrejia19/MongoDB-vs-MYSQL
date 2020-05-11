# Importing the required libraries
from time import time

import json
import pymysql
from flask import Flask, render_template, request
from pymongo import MongoClient

app = Flask(__name__)


# Creating the initial route for the flask app
@app.route('/')
def home():
    # Connecting to MySQL database to fetch the geo locations and create a .JSON file for it
    connection = pymysql.connect(host="localhost", user="root", passwd="", db="geo_after_norm")
    cursor = connection.cursor()
    cursor.execute("select distinct geo_location from geonorm")
    data = cursor.fetchall()
    listLocation = []
    dict = {}
    for row in data:
        listLocation.append(row[0])
    dict["GEO"] = listLocation

    # Creating a JSON
    j = json.dumps(dict)

    # Writing the json to a .JSON file
    fout = open("dropdown.json", "w")
    fout.write(j)
    fout.close()

    # Again opening the .json file to read the JSON contained within it
    fout = open("dropdown.json", "r")
    line = fout.read()
    fout.close()

    # Loading the json from the file into the variable named jsondata
    jsondata = json.loads(line)
    locations = jsondata["GEO"]

    cursor.close()
    connection.close()

    # Rendering the HTML page and passing the json for the locations
    return render_template("home.html", js=locations)


@app.route("/Enter", methods=["get", "post"])
def function():
    # Fetching the location and the database selected by the user
    location = request.form.get("location")
    database = request.form.get("database")

    if (database == "sqlafter"):

        # Connecting Python Flask to mysql database using the library pymysql
        connection = pymysql.connect(host="localhost", user="root", passwd="", db="geo_after_norm")
        cursor = connection.cursor()

        # Getting the initial time before the query execution starts
        startsql = time()
        cursor.execute(
            "select normdata.*, geonorm.OrderNumber, uomnorm.uom, uomnorm.scale_factor, uomnorm.scale_id from normdata join geonorm on normdata.GEO = geonorm.geo_location join uomnorm on normdata.UOM_ID = uomnorm.uom_id where normdata.GEO = %s",
            location
        )
        # Getting time after the query has been executed
        stopsql = time()

        # Final query execution time is stop - start
        sqltime = stopsql - startsql
        rows = cursor.fetchall()

        return render_template("displaysql.html", allrows=rows, time=sqltime)

    elif (database == "sqlbefore"):

        # Everyting here is perfomred the same way as we performed for mysql db after normalization
        # Only diffrence is in the query
        startsql = time()

        connection = pymysql.connect(host="localhost", user="root", passwd="", db="geo_before_norm")
        cursor = connection.cursor()

        cursor.execute("select * from jobs where GEO = %s", location)

        stopsql = time()

        sqltime = stopsql - startsql
        rows = cursor.fetchall()

        return render_template("displaysqlbefore.html", allrows=rows, time=sqltime)


    elif (database == "mongodb"):

        startmongo = time()

        # Connecting MongoDB with python Flask using pymongo library
        client = MongoClient("mongodb://localhost:27017/")
        db = client["case_study"]
        table = db["geo_locations"]
        query = {"GEO": location}

        result = table.find(query)

        stopmongo = time()

        # Query execution time is start - stop
        mongotime = stopmongo - startmongo

        return render_template("displaymongodb.html", result=result, time=mongotime)
    else:

        # Displaying error message if no database is selected
        return render_template("error.html")


if __name__ == "__main__":
    app.run(debug=True)
