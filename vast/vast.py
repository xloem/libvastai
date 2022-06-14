from . import VastException
from .vast_cmd import vast_cmd, server_url_default
import threading

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

        printlines, table = self.cmd('copy', src, dest, identity = identity)

        if not printlines[-1].startswith('Remote to Remote copy initiated'):
            raise VastException(*printlines[-2:])

    def search_offers(self, type = 'on-demand', bundling = True, pricing_storage_GiB = 5.0, sort_fields = ('score-',), query = 'external=false rentable=true verified=true'):
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

        printlines, table = self.cmd('search', 'offers', '--no-default', *query.split(' '), disable_bundle = not bundling, type = type, storage = pricing_storage_GiB, order = ','.join(sort_fields))

        if len(printlines):
            raise VastException(*printlines)

        return table[0][0]


    def cmd(self, *params, **kwparams):
        '''
        Directly executes the passed vast_python library command, returning print and table output as
        a 2-tuple of lists.
        '''

        params = (str(param) for param in params)
    
        if self.key is not None:
            params = ['--url', self.url, '--api_key', self.key, *params]
        else:
            params = ['--url', self.url, *params]

        params.extend((f'--{key.replace("_","-")}' for key, val in kwparams.items() if val is True))

        params.extend((str(param) for key, val in kwparams.items() if val not in (None, True, False) for param in (f'--{key.replace("_","-")}', val)))
        
        return vast_cmd(*params)

if __name__ == '__main__':
    for offer in Vast().search_offers():
        print(offer)
