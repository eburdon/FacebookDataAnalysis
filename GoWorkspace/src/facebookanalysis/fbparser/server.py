from bottle import route, run, template, static_file, abort
from fbparser import FacebookMessageParser
import StringIO
import boto
import os
from subprocess import call

from boto.s3.connection import S3Connection
from boto.s3.connection import Key

@route('/start_job/<name>')
def index(name):

    conn = S3Connection()

    mybucket = conn.get_bucket("facebook-analysis")

    key_name = 'raw/' + name

    k = Key(mybucket, key_name)

    stream = StringIO.StringIO(k.get_contents_as_string())
    out_stream = StringIO.StringIO()

    fbmp = FacebookMessageParser(stream, out_stream)
    fbmp.parser()


    pre_name = 'pre/' + name

    pk = Key(mybucket, pre_name)
    pk.set_contents_from_string(out_stream.getvalue())

    # Fire MR job here


    return template("Success {{name}}", name=name)


@route('/get_job/<name>')
def index(name):
    
    conn = S3Connection()

    mybucket = conn.get_bucket("facebook-analysis")

    prefix = 'post/' + name

    output = ""

    for key in mybucket.list(prefix=prefix):
   
        if "part" not in key.name:
            continue 

        output += key.get_contents_as_string()

    if output == "":
        return abort(404, "Does not exist (yet?)")

    ofh = open(name, "w")
    ofh.write(output)

    call(["./facebookanalysis", "graph", "stack-hist",  name , "3", name])

    return static_file(name+".png", root="")


run(host='localhost', port=8080)
