from .vast_cmd import vast_cmd
import threading

class Vast:
    def __init__(self, url = vast_cmd.server_url_default, key = vast_cmd.api_key_guard):
        self.url = url
        self.key = key
    
