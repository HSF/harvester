import json
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermessenger import http_server_messenger

# logger
_logger = core_utils.setup_logger()
http_server_messenger.set_logger(_logger)


# handler
class ApacheHandler(http_server_messenger.HttpHandler):
    def __init__(self, *args, **kwargs):
        self.responseCode = None
        self.form = dict()
        self.message = None
        self.headerList = [('Content-Type', 'text/plain')]
        http_server_messenger.HttpHandler.__init__(self, *args, **kwargs)

    def setup(self):
        pass

    def handle(self):
        pass

    def finish(self):
        pass

    def send_response(self, code, message=None):
        self.responseCode = code

    def get_form(self):
        return self.form

    def set_form(self, form):
        self.form = form

    def do_postprocessing(self, message):
        self.message = message

    def send_header(self, keyword, value):
        self.headerList = [(keyword, value)]


# application
def application(environ, start_response):
    try:
        # get params
        try:
            request_body_size = int(environ.get('CONTENT_LENGTH', 0))
        except:
            request_body_size = 0
        request_body = environ['wsgi.input'].read(request_body_size)
        params = json.loads(request_body)
        # make handler
        handler = ApacheHandler(None, None, None)
        handler.set_form(params)
        # execute
        handler.do_POST()
        # make response
        _logger.debug("{0} Phrase".format(handler.responseCode))
        start_response("{0} Phrase".format(handler.responseCode), handler.headerList)
        return [handler.message]
    except:
        errMsg = core_utils.dump_error_message(_logger)
        start_response('500 Phrase', [('Content-Type', 'text/plain')])
        return [errMsg]
