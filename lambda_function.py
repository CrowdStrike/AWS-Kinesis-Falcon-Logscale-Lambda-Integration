"""CrowdStrike: AWS Lambda: Kinesis to Falcon LogScale
"""
import os
import time
import base64
import logging
from logscale import IngestApi, HecEvent, Payload

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Required:
#   LOGSCALEHOST - LogScale server
#     REPOSITORY - LogScale repository
#          TOKEN - LogScale ingest token
LOGSCALEHOST = os.environ.get('HOST')
REPOSITORY = os.environ.get('REPOSITORY')
TOKEN = os.environ.get('TOKEN')

def lambda_handler(event, context):
    logscale = IngestApi(host=LOGSCALEHOST, repository=REPOSITORY, token=TOKEN)

    # suggestion: set the source field to uniquely identify the Kinesis stream
    source = "my-kinesis-stream"
    # requirment: set the sourcetype to the target ingest parser name
    sourcetype = "aws-kinesis"
    hec_event = HecEvent(host=LOGSCALEHOST, index=REPOSITORY, source=source, sourcetype=sourcetype)
    payload = Payload()

    # pack records; send batches
    for kinesis_record in event['Records']:
        # decode kinesis event data
        kdata = base64.b64decode(kinesis_record['kinesis']['data']).decode("utf-8") 
        kinesis_record['kinesis'].update({"data": kdata})

        # create base hec event
        hev = hec_event.create(message=kinesis_record)

        # optional: additional HEC fields
        # example:
        #   hev['fields'].update({"my_field": "my field data"})

        # add event to POST payload
        payload.pack(hev)
        if payload.full: # send event batch
            logger.debug("post events: {ec}, payload: {b}".format(ec=payload.event_count, b=payload.size_bytes))
            logscale.send_event("hec", payload.packed)
            payload.reset()

    # send residual events
    if not payload.empty: 
        logger.debug("post events: {ec}, payload: {b}".format(ec=payload.event_count, b=payload.size_bytes))
        logscale.send_event("hec", payload.packed)