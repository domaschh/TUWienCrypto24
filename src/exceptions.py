class PeerValidationError(Exception):
    def __init__(self, message: str, error_name: str):
        super().__init__(message)
        self.error_name = error_name


class HandshakeException(Exception):
    pass

class MessageException(Exception):
    NETWORK_ERROR_MESSAGE = "Network error"