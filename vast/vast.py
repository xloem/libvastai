from .vast_cmd import vast_cmd, server_url_default, api_key_guard
import threading

class Vast:
    def __init__(self, url = server_url_default, key = api_key_guard):
        self.url = url
        self.key = key
    # copy, search offers, show instances, ssh-url, scp-url, show machines, show invoices, show user, generate pdf-invoices, list machine, unlist machine, start instance, stop instance, label instance, destroy instance, set defjob, create instance, change bid, set min-bid, set api-key, create account, login

