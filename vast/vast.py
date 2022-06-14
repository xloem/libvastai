from . import VastException
from .vast_cmd import vast_cmd, server_url_default
import json, threading

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

        printlines, tables = self.cmd('copy', src, dest, identity = identity)

        if not printlines[-1].startswith('Remote to Remote copy initiated'):
            raise VastException(*printlines[-2:])

    def offers(self, type = 'on-demand', bundling = True, pricing_storage_GiB = 5.0, sort_fields = ('score-',), query = 'external=false rentable=true verified=true'):
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

        printlines, tables = self.cmd('search', 'offers', '--no-default', *query.split(' '), disable_bundling = not bundling, type = type, storage = pricing_storage_GiB, order = ','.join(sort_fields), mutate_hyphens = True)

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

    def invoices(self, start_date, end_date, only_charges=False, only_credits=False, ids_only=False):
        '''
        Show current payments and charges. Various options available to limit time range and type
        of items. Default is to show everything for user's entire billing history.

        Returns history, current_charges
        '''
        printlines, tables = self.cmd('show', 'invoices', quiet=ids_only, start_date=start_date, end_date=end_date, only_charges=only_charges, only_credits=only_credits)
        current_charges = json.loads(printlines[-1].split(': ', 1)[1])
        return tables[0][0], current_charges
        

    def cmd(self, *params, mutate_hyphens = False, **kwparams):
        '''
        Directly executes the passed vast_python library command, returning print and table output as
        a 2-tuple of lists.
        '''

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
        
        return vast_cmd(*params)

if __name__ == '__main__':
    for offer in Vast().search_offers():
        print(offer)
