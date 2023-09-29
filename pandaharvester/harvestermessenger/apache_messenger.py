import json
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermessenger import http_server_messenger
from pandaharvester.harvestermisc.frontend_utils import HarvesterToken


# logger
_logger = core_utils.setup_logger("apache_messenger")
http_server_messenger.set_logger(_logger)


# handler
class ApacheHandler(http_server_messenger.HttpHandler):
    def __init__(self, *args, **kwargs):
        self.responseCode = None
        self.form = dict()
        self.message = None
        self.headerList = [("Content-Type", "text/plain")]
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
        self.message = message.encode("ascii")

    def send_header(self, keyword, value):
        self.headerList = [(keyword, value)]


# application
def application(environ, start_response):
    try:
        # get params
        try:
            request_body_size = int(environ.get("CONTENT_LENGTH", 0))
        except Exception as e:
            _logger.warning("Zero request body due to {0}: {1}".format(e.__class__.__name__, e))
            request_body_size = 0
        # check token
        if getattr(harvester_config.frontend, "authEnable", True):
            try:
                auth_str = environ.get("HTTP_AUTHORIZATION", "").split()[-1]
                token = HarvesterToken()
                payload = token.get_payload(auth_str)
            except Exception as e:
                _logger.warning("Invalid token due to {0}: {1}".format(e.__class__.__name__, e))
                errMsg = "Auth failed: Invalid token"
                start_response("403 Forbidden", [("Content-Type", "text/plain")])
                return [errMsg.encode("ascii")]
        request_body = environ["wsgi.input"].read(request_body_size)
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
    except Exception:
        errMsg = core_utils.dump_error_message(_logger)
        start_response("500 Phrase", [("Content-Type", "text/plain")])
        return [errMsg]
