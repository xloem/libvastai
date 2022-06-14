import os, requests, threading

import vast
import vast.vast_python.vast
from .vast_python.vast import parser, api_key_guard, api_key_file_base, api_key_file, server_url_default

parser.add_argument('--url', help='server REST api url', default=server_url_default)
parser.add_argument('--raw', action='store_true', help='output machine-readable json');
parser.add_argument('--api-key', help='api key. defaults to using the one stored in {}'.format(api_key_file_base),
                        type=str, required=False, default=api_key_guard)

lock = threading.Lock()

wrap_print_output = []
def wrap_print(*params):
    global wrap_print_output
    vast.logger.info(' '.join((str(param) for param in params)))
    wrap_print_output.append(params)
vast.vast_python.vast.print = wrap_print

wrap_display_table_output = []
def wrap_display_table(records : list, field_details):
    global wrap_display_table_output
    wrap_display_table_output.append((records, field_details))
vast.vast_python.vast.display_table = wrap_display_table

def gather_wrapped(args):
    global wrap_print_output, wrap_display_table_output
    args.func(args)
    result = (list(wrap_print_output), list(wrap_display_table_output))
    wrap_print_output = []
    wrap_display_table_output = []
    return result

def vast_cmd(*argv):
    vast.logger.info('vast.py ' + ' '.join((str(param) for param in argv)))
    with lock:
        args = parser.parse_args(argv=argv)
        if args.api_key is api_key_guard:
            if os.path.exists(api_key_file):
                with open(api_key_file, 'r') as reader:
                    args.api_key = reader.read().strip()
            else:
                args.api_key = None
        try:
            return gather_wrapped(args)
        except requests.exceptions.HTTPError as e:
            try:
                errmsg = e.response.json().get('msg');
            except JSONDecodeError:
                if e.response.status_code == 401:
                    errmsg = 'Please log in or sign up'
                else:
                    errmsg = '(no detail message supplied)'
            raise vast.VastException(f'failed with error {e.response.status_code}: {errmsg}')
