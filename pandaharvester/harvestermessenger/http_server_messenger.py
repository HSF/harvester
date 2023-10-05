import json
import os
import os.path
import threading

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

from queue import Queue
from http.server import HTTPServer, BaseHTTPRequestHandler

# try:
#     from urllib.parse import parse_qsl
# except ImportError:
#     from cgi import parse_qsl
from socketserver import ThreadingMixIn
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermessenger import shared_file_messenger

# logger
_logger = core_utils.setup_logger("http_server_messenger")
shared_file_messenger.set_logger(_logger)


def set_logger(master_logger):
    global _logger
    _logger = master_logger
    shared_file_messenger.set_logger(master_logger)


# handler for http front-end
class HttpHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.dbProxy = DBProxy()
        self.tmpLog = None
        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def log_message(self, format, *args):
        # suppress console logging
        pass

    def get_form(self):
        dataStr = self.rfile.read(int(self.headers["Content-Length"]))
        return json.loads(dataStr)

    def do_postprocessing(self, message):
        self.end_headers()
        self.wfile.write(message)

    def do_POST(self):
        # logger
        if self.tmpLog is None:
            self.tmpLog = core_utils.make_logger(_logger)
        toSkip = False
        form = None
        methodName = None
        dataStr = None
        message = ""
        # parse the form data posted
        try:
            form = self.get_form()
        except Exception:
            message = "corrupted json"
            toSkip = True
        # check parameters
        if not toSkip:
            toSkip = True
            # method is not set
            if "methodName" not in form:
                message = "methodName is not given"
                self.send_response(400)
            elif "workerID" not in form:
                message = "workerID is not given"
                self.send_response(400)
            elif "data" not in form:
                message = "data is not given"
                self.send_response(400)
            else:
                toSkip = False
        # get worker
        if not toSkip:
            try:
                workerID = form["workerID"]
                workSpec = self.dbProxy.get_worker_with_id(workerID)
                if workSpec is None:
                    message = "workerID={0} not found in DB".format(workerID)
                    self.send_response(400)
                else:
                    # chose file and operation for each action
                    methodName = form["methodName"]
                    opType = None
                    filePath = ""
                    if methodName == "requestJobs":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.jsonJobRequestFileName)
                        opType = "w"
                    elif methodName == "getJobs":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.jobSpecFileName)
                        opType = "r"
                    elif methodName == "requestEventRanges":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.jsonEventsRequestFileName)
                        opType = "w"
                    elif methodName == "getEventRanges":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.jsonEventsFeedFileName)
                        opType = "r"
                    elif methodName == "updateJobs":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.jsonAttrsFileName)
                        opType = "w"
                    elif methodName == "uploadJobReport":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.jsonJobReport)
                        opType = "w"
                    elif methodName == "uploadEventOutputDump":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.jsonOutputsFileName)
                        opType = "w"
                    elif methodName == "setPandaIDs":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.pandaIDsFile)
                        opType = "w"
                    elif methodName == "killWorker":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.killWorkerFile)
                        opType = "w"
                    elif methodName == "heartbeat":
                        filePath = os.path.join(workSpec.get_access_point(), shared_file_messenger.heartbeatFile)
                        opType = "w"
                    else:
                        self.send_response(501)
                        message = "method not implemented"
                        toSkip = True
                    # take action
                    if not toSkip:
                        # write actions
                        if opType == "w":
                            # check if file exists. Methods such as heartbeat however need to overwrite the file
                            if os.path.exists(filePath) and methodName not in ["heartbeat"]:
                                message = "previous request is not yet processed"
                                self.send_response(503)
                            else:
                                with open(filePath, "w") as fileHandle:
                                    json.dump(form["data"], fileHandle)
                                    message = "OK"
                                    self.send_response(200)
                        else:
                            # read actions
                            if os.path.exists(filePath):
                                with open(filePath) as fileHandle:
                                    try:
                                        _message = json.load(fileHandle)
                                        message = json.dumps(_message)
                                        self.send_header("Content-Type", "application/json")
                                    except JSONDecodeError:
                                        _f_qs = open(filePath).read()
                                        # _message = dict(parse_qsl(_f_qs, keep_blank_values=True))
                                        message = _f_qs
                                        self.send_header("Content-Type", "text/plain")
                                    self.send_response(200)
                            else:
                                message = "previous request is not yet processed"
                                self.send_response(503)
            except Exception:
                self.send_response(500)
                message = core_utils.dump_error_message(_logger)
        if harvester_config.frontend.verbose:
            self.tmpLog.debug("ip={3} - method={0} json={1} msg={2}".format(methodName, dataStr, message, self.client_address[0]))
        # set the response
        self.do_postprocessing(message)
        return


# http front-end with a thread pool
class ThreadedHttpServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True

    # override to make a thread pool
    def serve_forever(self, *args, **kwargs):
        # queue for requests
        self.requests = Queue(harvester_config.frontend.nThreads)
        # make thread pool
        for x in range(harvester_config.frontend.nThreads):
            thr = threading.Thread(target=self.thread_main_loop)
            thr.daemon = True
            thr.start()
        # original server main loop
        HTTPServer.serve_forever(self, *args, **kwargs)

    # main loop of the thread
    def thread_main_loop(self):
        while True:
            # get a request from queue to process
            ThreadingMixIn.process_request_thread(self, *self.requests.get())

    # override to queue requests
    def process_request(self, request, client_address):
        self.requests.put((request, client_address))


# singleton launcher for http front-end
class FrontendLauncher(object):
    instance = None
    lock = threading.Lock()

    # override __new__ to have a singleton
    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            with cls.lock:
                if cls.instance is None:
                    if harvester_config.frontend.type == "simple":
                        httpd = ThreadedHttpServer(("", harvester_config.frontend.portNumber), HttpHandler)
                        thr = threading.Thread(target=httpd.serve_forever)
                        thr.daemon = True
                        thr.start()
                        cls.instance = thr
                    else:
                        cls.instance = 1
        return cls.instance


# start frontend
frontend = FrontendLauncher()


# messenger with http frontend
class HttpServerMessenger(shared_file_messenger.SharedFileMessenger):
    # constructor
    def __init__(self, **kwarg):
        shared_file_messenger.SharedFileMessenger.__init__(self, **kwarg)
