from collections import defaultdict, namedtuple
import random
import string
import os
import math
import json
import time
import sys
import traceback
from timeit import default_timer as timer
import numpy as np
import datetime
import threading
import argparse
from pathlib import Path
import signal
from botocore.config import Config
import boto3
import pprint

#######################################
###### Dimension model for schema #####
#######################################

regionDFW = 'Dallas'
regionAUS = 'Austin'
regionNYC = 'NewYork'
regionMIA = 'Miami'
regionLA = 'LosAngeles'

regions = [regionDFW, regionAUS, regionNYC, regionMIA, regionLA]

transportersPerRegion = {
    regionDFW: 20,
    regionAUS: 30,
    regionNYC: 50,
    regionMIA: 10,
    regionLA: 40
}
# TransporterID: generateRandomAlphaNumericString function
# Service area: random num from 0 - 50
# API name [AcceptOffer, RejectOffer, CancelTimeBlock, GetOffers]

getOffer = "GetOffers"
acceptOffer = "AcceptOffer"
rejectOffer = "RejectOffer"
forfeitOffer = "ForfeitOffer"
cancelTimeBlock = "CancelTimeBlock"

apiNames = [getOffer, acceptOffer, rejectOffer, forfeitOffer, cancelTimeBlock]


#######################################
###### Measure model for schema #####
#######################################
# Measures –
# a.isATCTaggedBot – Boolean
# b.Exceptions – Varchar
# c.DemandId – Varchar (Only for Accept/Reject/Forfeit)
# d.NumberOfOffersReturned – BigInt (Only for GetOffersForProvider)
# e.TPS - BigInt
measureTPS = 'TPH'
measureIsATCTaggedBot = 'IsATCTaggedBot'
measureException = 'Exception'
measureDemandId = 'DemandId'
measureNumberOfOffersReturned = 'NumberOfOffersReturned'

measureTaskCompleted = 'task_completed'
measureTaskEndState = 'task_end_state'


measuresForMetrics = [measureTPS, measureIsATCTaggedBot,
                      measureException, measureNumberOfOffersReturned]

measuresForEvents = [measureTaskCompleted, measureTaskEndState]

measureValuesForTaskEndState = ['SUCCESS_WITH_NO_RESULT',
                                'SUCCESS_WITH_RESULT', 'INTERNAL_ERROR', 'USER_ERROR', 'UNKNOWN', 'THROTTLED']
selectionProbabilities = [0.2, 0.7, 0.01, 0.07, 0.01, 0.01]

DimensionsMetric = namedtuple(
    'DimensionsMetric', 'region transporter_id api_name')
DimensionsEvent = namedtuple(
    'DimensionsEvent', 'region transporter_id api_name')


def generateRandomAlphaNumericString(length=5):
    # rand = random.Random(12345)
    x = ''.join(random.choice(string.ascii_letters + string.digits)
                for x in range(length))
    return x


def generateDimensions():
    dimensionsMetrics = list()
    dimenstionsEvents = list()

    # print('Generating dimensions for API {}'.format(apiNames))

    # may be create 10 different transporterID
    for region in regions:
        transportersForRegion = transportersPerRegion[region]
        for transporter in range(1, transportersForRegion + 1):
            for api in apiNames:
                transporterId = generateRandomAlphaNumericString(10)
                # serviceArea = str(random.randint(1, 20))
                metric = DimensionsMetric(region, transporterId, api)
                dimensionsMetrics.append(metric)

                # event = DimensionsEvent(region, transporterId, api)
                # dimenstionsEvents.append(event)

    return (dimensionsMetrics)


print(generateDimensions())
print(len(generateDimensions()))


def createWriteRecordCommonAttributes(dimensions):
    return {"Dimensions": [{"Name": dimName, "Value": getattr(dimensions, dimName), "DimensionValueType": "VARCHAR"} for dimName in dimensions._fields]}


def createRandomMetrics(hostId, timestamp, timeUnit, args):
    records = list()

    # Measures –
    # a.isATCTaggedBot – BIGINT 1: true; 0: false
    # b.Exceptions – Varchar
    # c.DemandId – Varchar (Only for Accept/Reject/Forfeit)
    # d.NumberOfOffersReturned – BigInt (Only for GetOffersForProvider)
    # e.TPS - BigInt

    # NumberOfOffersReturned metrics
    # if apiNames is getOffer
    random_api = random.choice(apiNames)
    if random_api == 'GetOffers':
        num_of_offers_returned = round(20 * random.random())
        records.append(createRecord(measureNumberOfOffersReturned, "{}".format(
            num_of_offers_returned), "BIGINT", timestamp, timeUnit))

    # DemandId metrics
    elif random_api == 'AcceptOffer' or random_api == 'RejectOffer' or random_api == 'ForfeitOffer':
        demand_id = generateRandomAlphaNumericString(20)
        records.append(createRecord(measureDemandId, "{}".format(
            demand_id), "VARCHAR", timestamp, timeUnit))
    else:
        # Exceptions metrics
        exceptions = ['OfferUnavailableException', 'InternalServiceException']
        records.append(createRecord(measureException, "{}".format(
            random.choice(exceptions)), "VARCHAR", timestamp, timeUnit))

        # isATCTaggedBot metrics
        is_atc_tagged_bot = random.randint(0, 1)
        records.append(createRecord(measureIsATCTaggedBot, "{}".format(
            is_atc_tagged_bot), "BIGINT", timestamp, timeUnit))
        # TPS metrics
        tph = round(7000 * random.random())
        records.append(createRecord(measureTPS, tph,
                                    "BIGINT", timestamp, timeUnit))

    return records


def createRandomEvent(hostId, timestamp, timeUnit, args):
    records = list()

    records.append(createRecord(measureTaskCompleted,
                   random.randint(0, 500), "BIGINT", timestamp, timeUnit))
    records.append(createRecord(measureTaskEndState, np.random.choice(
        measureValuesForTaskEndState, p=selectionProbabilities), "VARCHAR", timestamp, timeUnit))

    return records


def createRecord(measureName, measureValue, valueType, timestamp, timeUnit):
    return {
        "MeasureName": measureName,
        "MeasureValue": str(measureValue),
        "MeasureValueType": valueType,
        "Time": str(timestamp),
        "TimeUnit": timeUnit
    }


seriesId = 0
timestamp = int(time.time())
sigInt = False
lock = threading.Lock()
iteration = 0
utilizationRand = random.Random(12345)
lowUtilizationHosts = []
highUtilizationHosts = []


def signalHandler(sig, frame):
    global sigInt
    global lock
    global sigInt

    with lock:
        sigInt = True

#########################################
######### Ingestion Thread ###############
#########################################


class IngestionThread(threading.Thread):
    def __init__(self, tsClient, threadId, args, dimensionMetrics):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.args = args
        self.dimensionMetrics = dimensionMetrics
        # self.dimensionEvents = dimensionEvents
        self.client = tsClient
        self.databaseName = args.databaseName
        self.tableName = args.tableName
        self.numMetrics = len(dimensionMetrics)
        # self.numEvents = len(dimensionEvents)

    def run(self):
        global seriesId
        global timestamp
        global lock
        global iteration

        timings = list()
        success = 0
        idx = 0

        metricCnt = 0
        eventCnt = 0
        recordCnt = 0

        while True:
            # get the seriesId with lock - ensure that no two thread's create records
            # for the same time series identified by an unique dimension set (see Dimensions tuple).
            with lock:
                if sigInt == True:
                    print("Thread {} exiting.".format(self.threadId))
                    break

                if seriesId >= self.numMetrics:
                    seriesId = 0
                    iteration += 1
                    # ensure that each iteration has a "new" timestamp
                    while (timestamp == int(time.time())):
                        time.sleep(0.1)
                    timestamp = int(time.time())
                    print("Resetting to first series from thread: [{}] at time {}. Timestamp set to: {}. Iteration {} finished."
                          .format(self.threadId, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), timestamp, iteration))

                localSeriesId = seriesId
                localTimestamp = timestamp
                seriesId += 1

            if (args.autostop > 0) and (args.autostop <= iteration):
                break

            if localSeriesId < self.numMetrics:
                commonAttributes = createWriteRecordCommonAttributes(
                    self.dimensionMetrics[localSeriesId])
                records = createRandomMetrics(
                    localSeriesId, localTimestamp, "SECONDS", self.args)
                metricCnt += len(records)
            else:
                commonAttributes = createWriteRecordCommonAttributes(
                    self.dimensionEvents[localSeriesId - self.numMetrics])
                records = createRandomEvent(
                    localSeriesId, localTimestamp, "SECONDS", self.args)
                eventCnt += len(records)

            start = timer()
            try:
                if not (args.dryRun):
                    writeResult = writeRecords(
                        self.client, self.databaseName, self.tableName, commonAttributes, records)
                recordCnt += len(records)
                success += 1
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(
                    exc_type, exc_value, exc_traceback, limit=5, file=sys.stdout)

                print("T{:02d}: RequestId: {} for timestamp {}".format(
                    self.threadId, e.response['ResponseMetadata']['RequestId'], localTimestamp))
                if e.response['RejectedRecords']:
                    print(json.dumps(e.response['RejectedRecords'], indent=2))

                print(json.dumps(commonAttributes, indent=2))
                print(json.dumps(records, indent=2))
                continue
            finally:
                end = timer()
                timings.append(end - start)

            if idx % 100 == 0:
                now = datetime.datetime.now()
                print("T{:02d}: {}. Metrics/Events/Total: {}/{}/{}. Time: {}. Loop: {}"
                      .format(self.threadId, now.strftime("%Y-%m-%d %H:%M:%S"), metricCnt, eventCnt, recordCnt, round(end - start, 3), idx))
            idx += 1

        self.success = success
        self.timings = timings


#########################################
######### Ingest load ###################
#########################################

def ingestRecords(tsClient, dimensionsMetrics, args):
    numThreads = args.concurrency
    if numThreads > len(dimensionsMetrics):
        print("Can't have more threads than dimension metrics. Working with {} thread(s).".format(
            len(dimensionsMetrics)))
        numThreads = len(dimensionsMetrics)

    ingestionStart = timer()
    timings = list()
    threads = list()

    for threadId in range(numThreads):
        print("Starting ThreadId: {}".format(threadId + 1))
        thread = IngestionThread(
            tsClient, threadId + 1, args, dimensionsMetrics)
        thread.start()
        threads.append(thread)

    success = 0
    for t in threads:
        t.join()
        success += t.success
        timings.extend(t.timings)

    print("Total={}, Success={}, Avg={}, Stddev={}, 50thPerc={}, 90thPerc={}, 99thPerc={}".format(len(timings), success,
                                                                                                  round(np.average(
                                                                                                      timings), 3),
                                                                                                  round(np.std(timings), 3), round(
                                                                                                      np.percentile(timings, 50), 3),
                                                                                                  round(np.percentile(timings, 90), 3), round(np.percentile(timings, 99), 3)))

    ingestionEnd = timer()
    print("Total time to ingest: {} seconds".format(
        round(ingestionEnd - ingestionStart, 2)))

#########################################
######### Timestream API calls ##########
#########################################


def createWriteClient(region, endpoint_url, profile=None):
    if profile == None:
        print("Using credentials from the environment")

    print("Connecting to timestream ingest in region: ", region)
    config = Config()
    if profile != None:
        session = boto3.Session(profile_name=profile)
        client = session.client(service_name='timestream-write',
                                region_name=region, endpoint_url=endpoint_url, config=config)
    else:
        session = boto3.Session()
        client = session.client(service_name='timestream-write',
                                region_name=region, endpoint_url=endpoint_url, config=config)
    return client


def describeTable(client, databaseName, tableName):
    response = client.describe_table(
        DatabaseName=databaseName, TableName=tableName)
    print("Table Description:")
    pprint.pprint(response['Table'])


def writeRecords(client, databaseName, tableName, commonAttributes, records):
    return client.write_records(DatabaseName=databaseName, TableName=tableName,
                                CommonAttributes=(commonAttributes), Records=(records))


#########################################
######### Main ##########
#########################################
if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='TimestreamSampleContinuousDataIngestorApplication',
                                     description='Execute an application generating and ingesting time series data.')

    parser.add_argument('--region', '-r', '--endpoint', '-e', action="store", required=True,
                        help="Specify the service region. E.g. 'us-east-1'")
    parser.add_argument('--endpoint-url', '-url', action="store", required=False,
                        help="Specify the service endpoint url that you have been mapped to. E.g. 'https://ingest-cell2.timestream.us-east-1.amazonaws.com'")
    parser.add_argument('--profile', action="store", type=str, default=None,
                        help="The AWS Config profile to use.")

    parser.add_argument('--database-name', '-d', dest="databaseName", action="store", required=True,
                        help="The database name in Amazon Timestream - must be already created.")
    parser.add_argument('--table-name', '-t', dest="tableName", action="store", required=True,
                        help="The table name in Amazon Timestream - must be already created.")

    parser.add_argument('--concurrency', '-c', action="store", type=int, default=10,
                        help="Number of concurrent ingestion threads (default: 10)")

    parser.add_argument('--host-scale',  '-s', dest="hostScale", action="store", type=int, default=1,
                        help="The scale factor that determines the number of hosts emitting events and metrics (default: 1).")

    parser.add_argument('--include-region', dest="includeRegion", action="store",
                        help="Comma separated include of regions (default: EMPTY, all)")
    parser.add_argument('--include-ms', dest="includeMs", action="store",
                        help="Comma separated include of microservice (default: EMPTY, all).")

    parser.add_argument('--missing-cpu', dest="missingCpu", action="store", type=int, default=0,
                        help="The percentage of missing values [0-100], (default: 0).")

    parser.add_argument('--sin-signal-cpu', dest="sinSignalCpu", action="store", type=float, default=0,
                        help="The SIN signal to noise ratio, [0-100] no signal to 100 times noise, (default: 0).")
    parser.add_argument('--sin-frq-cpu', dest="sinFrqCpu", action="store", type=str, default="m",
                        help="The SIN signal frequency (m | h | d | we | mo | qu | ye) (default:m)")
    parser.add_argument('--saw-signal-cpu', dest="sawSignalCpu", action="store", type=float, default=0,
                        help="The SAW signal to noise ratio, [0-100] no signal to 100 times noise, (default: 0).")
    parser.add_argument('--saw-frq-cpu', dest="sawFrqCpu", action="store", type=str, default="m",
                        help="The SIN signal frequency (m | h | d | we | mo | qu | ye) (default:m)")

    parser.add_argument('--seed', dest="seed", action="store", type=int, default=int(time.time()),
                        help="The seed with which to initialize random, (default: now()).")
    parser.add_argument('--autostop', action="store", type=int, default=0,
                        help="Stop each ingestor threads after N iterates (=values per time series) records. (default:0, unlimited)")
    parser.add_argument('--dry-run', dest="dryRun", action="store_true",
                        help="Run program without 'actually' writing data to time stream.")

    args = parser.parse_args()
    print(args)

    hostScale = args.hostScale       # scale factor for the hosts.

    regionINC = []
    if args.includeRegion is not None:
        regionINC = args.includeRegion.split(",")

    microserviceINC = []
    if args.includeMs is not None:
        microserviceINC = args.includeMs.split(",")

    random.seed(args.seed)

    dimensionsMetrics = generateDimensions()
    hostIds = list(range(len(dimensionsMetrics)))
    utilizationRand.shuffle(hostIds)
    lowUtilizationHosts = frozenset(
        hostIds[0:math.ceil(int(0.2 * len(hostIds)))])
    highUtilizationHosts = frozenset(hostIds[-int(0.2 * len(hostIds)):])

    print("{} unique metric dimensions.".format(len(dimensionsMetrics)))
    # print("{} unique event dimensions.".format(len(dimensionsEvents)))

    # Register sigint handler
    signal.signal(signal.SIGINT, signalHandler)

    # Verify the table
    try:
        tsClient = any
        if not (args.dryRun):
            tsClient = createWriteClient(
                args.region, args.endpoint_url,  profile=args.profile)
            describeTable(tsClient, args.databaseName, args.tableName)
    except Exception as e:
        print(e)
        sys.exit(0)

    # Run the ingestion load.
    ingestRecords(tsClient, dimensionsMetrics, args)
