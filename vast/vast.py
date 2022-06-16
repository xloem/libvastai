from . import VastException
from .vast_cmd import vast_cmd, handle_httperror, server_url_default
#from .instance import Instance
import json, requests, threading

# this should change into a VastAPI class, and then a Vast class could model Instances with objects, and update their properties all at once.
class Vast:
    def __init__(self, url = server_url_default, key = None, identity = None):
        self.url = url
        self.key = key
        self.identity = identity

    def copy(self, src, dest, identity = None):
        '''
        Copies a directory from a source location to a target location. Each of source and destination
        directories can be either local or remote, subject to appropriate read and write
        permissions required to carry out the action. The format for both src and dst is (instance_id, path) or
        just plain path.
        '''
        if type(src) is tuple and len(src) == 2:
            src = f'{src[0]}:{src[1]}'
        if type(dest) is tuple and len(dst) == 2:
            dest = f'{dest[0]}:{dest[1]}'
        if identity is None:
            identity = self.identity

        self.cmd('copy', src, dest, identity = identity, expect='Remote to Remote copy initiated')

    def offers(self, instance_type = 'on-demand', bundling = True, pricing_storage_GiB = 5.0, sort = ('score-',), query = 'external=false rentable=true verified=true'):
        '''
        Search for instance types using custom query

        Query syntax:

            query = comparison comparison...
            comparison = field op value
            field = <name of a field>
            op = one of: <, <=, ==, !=, >=, >, in, notin
            value = <bool, int, float, etc> | 'any'

        Available fields:

              Name                  Type       Description

            bw_nvlink               float     bandwidth NVLink
            compute_cap:            int       cuda compute capability*100  (ie:  650 for 6.5, 700 for 7.0)
            cpu_cores:              int       # virtual cpus
            cpu_cores_effective:    float     # virtual cpus you get
            cpu_ram:                float     system RAM in gigabytes
            cuda_vers:              float     cuda version
            direct_port_count       int       open ports on host's router
            disk_bw:                float     disk read bandwidth, in MB/s
            disk_space:             float     disk storage space, in GB
            dlperf:                 float     DL-perf score  (see FAQ for explanation)
            dlperf_usd:             float     DL-perf/$
            dph:                    float     $/hour rental cost
            driver_version          string    driver version in use on a host.
            duration:               float     max rental duration in days
            external:               bool      show external offers
            flops_usd:              float     TFLOPs/$
            gpu_mem_bw:             float     GPU memory bandwidth in GB/s
            gpu_ram:                float     GPU RAM in GB
            gpu_frac:               float     Ratio of GPUs in the offer to gpus in the system
            has_avx:                bool      CPU supports AVX instruction set.
            id:                     int       instance unique ID
            inet_down:              float     internet download speed in Mb/s
            inet_down_cost:         float     internet download bandwidth cost in $/GB
            inet_up:                float     internet upload speed in Mb/s
            inet_up_cost:           float     internet upload bandwidth cost in $/GB
            machine_id              int       machine id of instance
            min_bid:                float     current minimum bid price in $/hr for interruptible
            num_gpus:               int       # of GPUs
            pci_gen:                float     PCIE generation
            pcie_bw:                float     PCIE bandwidth (CPU to GPU)
            reliability:            float     machine reliability score (see FAQ for explanation)
            rentable:               bool      is the instance currently rentable
            rented:                 bool      is the instance currently rented
            storage_cost:           float     storage cost in $/GB/month
            total_flops:            float     total TFLOPs from all GPUs
            verified:               bool      is the machine verified
        '''

        if type(sort) is not str:
            sort = ','.join(sort)

        printlines, tables = self.cmd('search', 'offers', '--no-default', *query.split(' '), disable_bundling = not bundling, type = instance_type, storage = pricing_storage_GiB, order = sort, mutate_hyphens = True)

        if len(printlines):
            raise VastException(*printlines)

        return tables[0][0]

    def instances(self):
        '''The stats on the machines the user is renting.'''
        
        printlines, tables = self.cmd('show', 'instances')       
        result = tables[0][0]
        for instance in result:
            instance['ssh_url'] = f'ssh://root@{instance["ssh_host"]}:{instance["ssh_port"]}'
            instance['scp_url'] = f'scp://root@{instance["ssh_host"]}:{instance["ssh_port"]}'
        return result

    def ssh_url(self):
        '''ssh url helper'''
        printlines, tables = self.cmd('ssh-url')
        
        if 'ssh://' not in printlines[0]:
            raise VastException(*printlines)
        else:
            return printlines[0]

    def scp_url(self):
        '''scp url helper'''
        printlines, tables = self.cmd('ssh-url')
        
        if 'scp://' not in printlines[0]:
            raise VastException(*printlines)
        else:
            return printlines[0]

    def machines(self, ids_only=False):
        '''Show the machines user is offering for rent.'''
        printlines, tables = self.cmd('show', 'machines', quiet=ids_only)

        if ids_only:
            return [int(id) for id in printlines]
        else:
            return [json.loads(line.split(': ', 1)) for line in printlines[1:]]

    def invoices(self, start_date = None, end_date = None, only_charges=False, only_credits=False):
        '''
        Show current payments and charges. Various options available to limit time range and type
        of items. Default is to show everything for user's entire billing history.

        Returns history, current_charges
        '''
        printlines, tables = self.cmd('show', 'invoices', start_date=start_date, end_date=end_date, only_charges=only_charges, only_credits=only_credits)
        current_charges = printlines[-1][1]
        return tables[0][0], current_charges

    def user(self):
        '''Stats for logged-in user.'''
        printlines, tables = self.cmd('show', 'user')
        return tables[0][0][0]

    #def pdf_invoices(self, start_date = None, end_date = None, only_charges=False, only_credits=False):
    #    '''
    #    Makes a PDF version of the data returned by the "show invoices" command. Takes the same args as that
    #    command.
    #    '''

    def list_machine(self, id=None, price_gpu=None, price_disk=None, price_inetu=None, price_inetd=None, min_chunk=None, end_timestamp=None):
        '''[Host] list a machine for rent'''
        self.cmd('list', 'machine',
            id, price_gpu=price_gpu, price_disk=price_disk,
            price_inetu=price_inetu, price_inetd=price_inetd,
            min_chunk=min_chunk, end_date=end_date,
            expect = 'offers created')

    def unlist_machine(self, id):
        '''[Host] Removes machine from list of machines for rent.'''
        self.cmd('unlist', 'machine', id, expect='all offers for machine')

    def remove_defjob(self, id):
        '''[Host] Delete default jobs'''
        self.cmd('remove', 'defjob', id, expect='default instances for machine')

    def start(self, instance_id):
        '''Start a stopped instance'''
        self.cmd('start', 'instance', instance_id, expect='starting instance')

    def stop(self, instance_id):
        '''Stop a running instance'''
        self.cmd('stop', 'instance', instance_id, expect='stopping instance ')

    def label(self, instance_id, label):
        '''Assign a string label to an instance'''
        self.cmd('label', 'instance', instance_id, label, expect='label for ')

    def destroy(self, instance_id):
        '''
        Destroy an instance (irreversible, deletes data)
        Perfoms the same action as pressing the "DESTROY" button on the website at https://vast.ai/console/instances/.
        '''
        self.cmd('destroy', 'instance', instance_id, expect='destroying instance ')

    def destroy_all(self):
        for instance in self.instances():
            self.destroy(instance['id'])

    def set_defjob(self, id, price_gpu=None, price_inetu=None, price_inetd=None, image=None, args=None):
        '''[Host] Create default jobs for a machine'''
        self.cmd('set', 'defjob', id,
            price_gpu=price_gpu, price_inetu=price_inetu, price_inetd=price_inetd, image=image, args=args,
            expect='bids created for machine '
        )

    def create(self, offer_id, image, disk_GB=10, price=None, label=None, onstart='', onstart_cmd=None, jupyter=False, jupyter_dir=None, jupyter_lab=False, lang_utf8=False, python_utf8=False, extra=None, create_from=None, force=False):
        '''
        Create a new instance
        Performs the same action as pressing the "RENT" button on the website at https://vast.ai/console/create/.
        '''
        printlines, tables = self.cmd('create', 'instance', offer_id,
            price=price, disk=disk_GB, image=image, label=label, onstart=onstart, onstart_cmd=onstart_cmd,
            jupyter=jupyter, jupyter_dir=jupyter_dir, jupyter_lab=jupyter_lab, lang_utf8=lang_utf8,
            python_utf8=python_utf8, extra=extra, create_from=create_from, force=force,
            mutate_hyphens=True, expect='Started. ')
        return eval(printlines[-1][0].split(' ',1)[1])['new_contract']

    def change_bid(self, instance_id, price=None):
        '''
        Change the bid price for a spot/interruptible instance

        If PRICE is not specified, then a winning bid price is used as the default.
        '''
        self.cmd('change', 'bid', price=price, expect='Per gpu bid price changed')

    def set_min_bid(self, machine_id, price=None):
        '''
        [Host] Set the minimum bid/rental price for a machine

        Change the current min bid price of machine id to PRICE.
        '''
        self.cmd('set', 'min_bid', machine_id, price=price, expect='Per gpu min bid price changed')

    def set_key(self, new_api_key):
        '''
        Set api-key (get your api-key from the console/CLI)

        Caution: a bad API key will make it impossible to connect to the servers.
        '''
        self.cmd('set', 'api_key', new_api_key, expect='Your api key has been saved in ')
        
    def docker_tags(self, image):
        while True:
            try:
                response = requests.get(f'{self.url}/docker/tags/?repo={image}')
                break
            except requests.exceptions.HTTPError as e:
                if handle_httperror(e):
                    continue
        return response.json()

    def offer_bid_price(self, offer_id):
        while True:
            try:
                response = requests.put(f'{self.url}/bundles_bid_price/{offer_id}/', json={})
            except requests.exceptions.HTTPError as e:
                if handle_httperror(e):
                    continue
        return float(response.text)
        
    def params2args(self, *params, mutate_hyphens = False, **kwparams):
        params = (str(param) for param in params)
    
        if self.key is not None:
            params = ['--url', self.url, '--api_key', self.key, *params]
        else:
            params = ['--url', self.url, *params]

        if mutate_hyphens:
            mutate_hyphens = lambda str: str.replace('_','-')
        else:
            mutate_hyphens = lambda str: str

        params.extend((f'--{mutate_hyphens(key)}' for key, val in kwparams.items() if val is True))

        params.extend((str(param) for key, val in kwparams.items() if val not in (None, True, False) for param in (f'--{mutate_hyphens(key)}', val)))

        return params

    def cmd(self, *params, mutate_hyphens = False, expect = None, **kwparams):
        '''
        Directly executes the passed vast_python library command, returning print and table output as
        a 2-tuple of lists.
        '''

        params = self.params2args(*params, mutate_hyphens = mutate_hyphens, **kwparams)
        
        printlines, tables = vast_cmd(*params)

        if expect is not None and not str(printlines[-1][0]).startswith(expect):
            raise VastException(*printlines)
        return printlines, tables
