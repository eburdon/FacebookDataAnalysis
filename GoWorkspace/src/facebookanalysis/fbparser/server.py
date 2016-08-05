from boto.s3.connection import Key, S3Connection
from bottle import response, route, run, template, static_file, abort
from fbparser import FacebookMessageParser
from subprocess import call

import boto
import bottle
import os
import StringIO

BUCKET="facebook-analysis"
BINARY="s3://facebook-analysis/bin/wc"
CLUSTER_ID="j-3BLA7KGI2G8CU"

app = bottle.app()

def enable_cors(fn):
    def _enable_cors(*args, **kwargs):
        # set CORS headers
        response.headers['Access-Control-Allow-Origin'] = '*'  # TODO
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

        if bottle.request.method != 'OPTIONS':
            return fn(*args, **kwargs)

    return _enable_cors

@app.route('/hello', method=['OPTIONS', 'GET'])
@enable_cors
def hello():
    response.headers['Content-type'] = 'application/json'

    return {"message":"Hello, Brandon Flowers!"}

@app.route('/start_job/<name>', method=['OPTIONS', 'GET'])
@enable_cors
def index(name):

    print "Starting job..."

    response.headers['Content-type'] = 'application/json'

    conn = S3Connection()

    print "Connected. Getting bucket..."

    mybucket = conn.get_bucket(BUCKET)

    key_name = 'raw/' + name

    k = Key(mybucket, key_name)

    print "Have bucket and key. Creating stream..."

    # this is getting hung up
    stream = StringIO.StringIO(k.get_contents_as_string())
    out_stream = StringIO.StringIO()

    print "Created stream. Parsing...."

    # call local pre-processing
    fbmp = FacebookMessageParser(stream, out_stream)
    fbmp.parser()

    print "Finished parsing. Uploading to bucket..."

    pre_name = 'pre/' + name

    # hangs up
    pk = Key(mybucket, pre_name)
    pk.set_contents_from_string(out_stream.getvalue())

    print "Done upload. Firing EMR step..."

    # Fire EMR job here; go lang word count is stored on S3
    # to run locally, change first argument to 'aws'
    # /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar
    cmd = [
        '/usr/local/bin/aws', 'emr', 'add-steps',
          '--cluster-id', CLUSTER_ID,
          '--steps', '[{"Args":["-files","' + BINARY + '","-mapper","wc map","-reducer","wc reduce","-input","s3://' + BUCKET + '/pre/' + name + '","-output","s3://' + BUCKET + '/post/' + name + '"], "Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"/home/hadoop/contrib/streaming/hadoop-streaming.jar","Properties":"","Name":"Facebook Analysis MapReduce"}]']

    call(cmd)

    print "Finished!"

    # return template("Success {{name}}", name=name)
    return {"message":"Success!"}


@app.route('/get_job/<name>', method=['OPTIONS', 'GET'])
@enable_cors
def index(name):
    
    response.headers['Content-type'] = 'image/png'

    conn = S3Connection()

    mybucket = conn.get_bucket(BUCKET)

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

    # call local (goLang) post-processing (up root)
    call(["./facebookanalysis", "graph", "stack-hist",  name , "3", name])

    return static_file(name+".png", root="")


run(host='localhost', port=8000)
