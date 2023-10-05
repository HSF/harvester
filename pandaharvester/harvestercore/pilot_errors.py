from pilot.common.errorcodes import ErrorCodes as PilotErrorCodesObj
from pilot.util import auxiliary as PilotAux


class PilotErrors(PilotErrorCodesObj):
    """Pilot error handling"""

    pilot_error_msg = PilotErrorCodesObj._error_messages
    pilot_error_msg.update(
        {
            # can have additional error codes here
        }
    )

    getErrorCodes = [1097, 1099, 1100, 1103, 1107, 1113, 1130, 1145, 1151, 1164, 1167, 1168, 1171, 1175, 1178, 1179, 1180, 1182]
    putErrorCodes = [1101, 1114, 1122, 1131, 1132, 1133, 1134, 1135, 1136, 1137, 1138, 1140, 1141, 1152, 1154, 1155, 1181]
    recoverableErrorCodes = [0] + putErrorCodes

    # Error codes that will issue a Pilot-controlled resubmission
    PilotResubmissionErrorCodes = [
        1008,
        1098,
        1099,
        1110,
        1113,
        1114,
        1115,
        1116,
        1117,
        1137,
        1139,
        1151,
        1152,
        1171,
        1172,
        1177,
        1179,
        1180,
        1181,
        1182,
        1188,
        1189,
        1195,
        1196,
        1197,
        1219,
    ]

    # Error codes used with FAX fail-over (only an error code in this list will allow FAX fail-over)
    PilotFAXErrorCodes = [1103] + PilotResubmissionErrorCodes

    # Mapping between payload exit code and pilot errors
    pilot_code_dict = PilotAux.get_error_code_translation_dictionary()
    avail_exit_codes = [value[0] for value in pilot_code_dict.values()]

    def getPilotErrorDiag(self, code=0):
        """Return text corresponding to error code"""
        pilotErrorDiag = ""
        if code in self.pilot_error_msg.keys():
            pilotErrorDiag = self.pilot_error_msg[code]
        else:
            pilotErrorDiag = "Unknown pilot error code"
        return pilotErrorDiag

    def isGetErrorCode(self, code=0):
        """Determine whether code is in the put error list or not"""
        state = False
        if code in self.getErrorCodes:
            state = True
        return state

    def isPutErrorCode(self, code=0):
        """Determine whether code is in the put error list or not"""
        state = False
        if code in self.putErrorCodes:
            state = True
        return state

    @classmethod
    def isRecoverableErrorCode(self, code=0):
        """Determine whether code is a recoverable error code or not"""
        return code in self.recoverableErrorCodes

    def isPilotResubmissionErrorCode(self, code=0):
        """Determine whether code issues a Pilot-controlled resubmission"""
        state = False
        if code in self.PilotResubmissionErrorCodes:
            state = True
        return state

    def isPilotFAXErrorCode(self, code=0):
        """Determine whether code allows for a FAX fail-over"""
        state = False
        if code in self.PilotFAXErrorCodes:
            state = True
        return state

    @classmethod
    def getErrorStr(self, code):
        """
        Avoids exception if an error is not in the dictionary.
        An empty string is returned if the error is not in the dictionary.
        """
        return self.pilot_error_msg.get(code, "")

    def getErrorName(self, code):
        """From the error code to get the error name"""
        for k in self.__class__.__dict__.keys():
            if self.__class__.__dict__[k] == code:
                return k
        return None

    def convertToPilotErrors(self, exit_code):
        """
        Convert payload exit code to pilot error code and error dialogue message
        """
        pilot_error_code, pilot_error_diag = None, ""
        if exit_code in self.avail_exit_codes:
            pilot_error_code = PilotAux.convert_to_pilot_error_code(exit_code)
            pilot_error_diag = self.getPilotErrorDiag(pilot_error_code)
        return pilot_error_code, pilot_error_diag


class PilotException(Exception):
    def __init__(self, message, code=PilotErrors.GENERALERROR, state="", *args):
        self.code = code
        self.state = state
        self.message = message
        super(PilotException, self).__init__(*args)

    @property
    def code(self):
        return self._code

    @code.setter
    def code(self, code):
        self._code = code
        self.code_description = PilotErrors.getErrorStr(code)

    def __str__(self):
        return "%s: %s: %s%s" % (self.__class__.__name__, self.code, self.message, " : %s" % self.args if self.args else "")

    def __repr__(self):
        return "%s: %s: %s%s" % (self.__class__.__name__, repr(self.code), repr(self.message), " : %s" % repr(self.args) if self.args else "")
