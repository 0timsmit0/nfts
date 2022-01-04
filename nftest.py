import requests
import json
import sqlite3
from sqlite3 import Error
import time

def get_template_hash(template):
    immutable_data = {}
    if "immutable_data" in template:
        immutable_data = template["immutable_data"]
    if not immutable_data:
        return "None"
    if "model" in immutable_data:
        return immutable_data["model"]
    if "image" in immutable_data:
        return immutable_data["image"]
    if "glb" in immutable_data:
        return immutable_data["glb"]
    if "video" in immutable_data:
        return immutable_data["video"]
    print("\n\n\n\nFound template with unknown or no data:\n\n")
    print(template)
    exit()
sql_create_templates_table = """CREATE TABLE IF NOT EXISTS templates (
                                    template_id integer PRIMARY KEY,
                                    created_at_time timestamp NOT NULL,
                                    issued_supply integer NOT NULL,
                                    name text NOT NULL,
                                    collection_name text NOT NULL,
                                    hash text NOT NULL,
                                    author text NOT NULL
                                );"""

sql_create_duplicates_table = """CREATE TABLE IF NOT EXISTS duplicates (
                                    template_id integer PRIMARY KEY,
                                    duplicate_of integer NOT NULL,
                                    author text NOT NULL,
                                    FOREIGN KEY (duplicate_of) REFERENCES templates (template_id)
                                );"""

conn = None
try:
    conn = sqlite3.connect("C:\\Users\\Tim\\Desktop\\nfttest\\nfttest.db")
    print(sqlite3.version)
except Error as e:
    print(e)

if conn:
    try:
        c = conn.cursor()
        c.execute(sql_create_templates_table)
        c.execute(sql_create_duplicates_table)
        print("created tables")
    except Error as e:
        print(e)

page = 300

url = "https://proton.api.atomicassets.io/atomicassets/v1/templates/"

params = {"limit":200,"order":"asc","page":page}

r = requests.get(url,params)

data = r.json()["data"]

skippableTemplates = {"2417","14211","35108","35135","35136"}

while data:
    for template in data:
        #print(template)
        if template["template_id"] in skippableTemplates:
            continue
        template_hash = get_template_hash(template)

        cur = conn.cursor()

        nft = (template["template_id"],template["created_at_time"],template["issued_supply"],template["name"],template["collection"]["collection_name"],template_hash,template["collection"]["author"])
        sql = ''' INSERT OR IGNORE INTO templates(template_id,created_at_time,issued_supply,name,collection_name,hash,author)
                VALUES(?,?,?,?,?,?,?) ''' 
        cur.execute(sql, nft)
        conn.commit()

        print(template["created_at_time"]+","+template["template_id"]+","+template["name"])
    
    time.sleep(1)
    page = page + 1
    params = {"limit":200,"order":"asc","page":page}

    r = requests.get(url,params)

    if r.status_code != 200:
        time.sleep(3)
        r = requests.get(url,params)
        if r.status_code != 200:
            print("\n\n\n\n\n No API response")
            exit()

    data = r.json()["data"]

print("\n\n\n\n\n Checking for duplicates:")

#get duplicate hashes
cur = conn.cursor()
sql = '''   SELECT hash, collection_name, author,COUNT(*)
            FROM templates
            WHERE hash<>""
            GROUP BY hash
            HAVING COUNT(*) > 1'''
c = cur.execute(sql).fetchall()
#conn.commit()
for row in c:
    sql = '''   SELECT hash, collection_name, author, template_id
            FROM templates
            WHERE hash=\"'''+row[0]+"\""
    c2 = cur.execute(sql).fetchall()
    author = c2[0][2]
    template = c2[0][3]
    duplicates = {}
    for row2 in c2:
        if row2[2] != author:
            duplicates[row2[3]]= (row2[2])
    if duplicates:
        print(duplicates)
        for duplicate in duplicates:
            dupe = (duplicate,template,duplicates[duplicate])
            sql = '''INSERT OR IGNORE INTO duplicates(template_id,duplicate_of,author)
                     VALUES(?,?,?) ''' 
            cur.execute(sql, dupe)
            conn.commit()
    
# print(data[0])
# print(data[1])
# print(data[2])

# url = "https://proton.api.atomicassets.io/atomicassets/v1/assets/"
# params = {"collection_name":"354415534331","limit":500} #data[2]["collection_name"]

# r = requests.get(url,params)

# #data = r.json()["data"][0]["name"]
# data = r.json()["data"]
# #print(data)
# for asset in data:
#     if(asset["name"]=="Wix"):
#         print(asset)
#print(data)

#hash = imagehash.average_hash(Image.open('test.png'))
