#! /usr/bin/env python3

import csv
import re
from pymongo import MongoClient

mongo_uri = "mongodb://localhost:27017"
mongo_db_name = "lichess" 
mongo_collection_name = "ublog_post"

client = MongoClient(mongo_uri)
db = client[mongo_db_name]
collection = db[mongo_collection_name]

def title_convert(title_text):
    return re.sub('[^A-Za-z0-9 ]+', '', title_text.lower()).replace(' ', '-')

def make_blog_link(blog_id):
    blog = collection.find_one({"_id": blog_id}, {"blog": 1, "title": 1})
    if blog:
        username = blog.get("blog").split(':')[1]
        title = title_convert(blog.get("title"))
        return f"https://lichess.org/@/{username}/blog/{title}/{blog_id}"
    return ""

csv_file_path = 'output.csv'
output_csv_path = 'links_output.csv'

with open(csv_file_path, 'r') as infile:
    reader = csv.DictReader(infile)
    fieldnames = ['mainblog', 'blog1', 'sim1', 'blog2', 'sim2', 'blog3', 'sim3']

    with open(output_csv_path, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            writedata = {'mainblog': make_blog_link(row['_id'])}
            for n, similar in enumerate(eval(row['similarBlogs'])):
                writedata[f'blog{n+1}'] = make_blog_link(similar['_id'])
                writedata[f'sim{n+1}'] = similar['similarity']
            writer.writerow(writedata)