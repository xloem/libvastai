from . import Vast, VastException, logger

import asyncio, socket, time

class Instance:
    def __init__(self, instance_id = None, machine_id = None, vast = None, query = 'external=false rentable=true verified=true machine_id!=1742', api_key = None, instance_type = 'interruptible', GiB = 5.0, image = 'pytorch/pytorch:latest'):
        if vast is None:
            vast = Vast(key = api_key)
        
        self.vast = vast
        self.machine_id = machine_id
        self.id = instance_id
        self._query = query
        self._instance_type = instance_type
        self._GiB = GiB
        self._image = image

        if self.id is not None or self.machine_id is not None:
            instances = self.vast.instances()
            if self.id < len(instances) and self.machine_id is None:
                self.id = instances[0]['id']
                self.machine_id = instances[0]['machine_id']
            else:
                assert [instance for instance in instances if instance['id'] == self.id or instance['machine_id'] == self.machine_id]
            self._detached = True
            self.update_attributes()
        else:
            self._detached = False

    @property
    def created(self):
        return self.id is not None

    @property
    def running(self):
        return self.actual_status == 'running'

    @property
    def connectable(self):
        # probe port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            result = sock.connect_ex((self.ssh_host, self.ssh_port))
            return result == 0
        finally:
            sock.close()

    def update_attributes(self):
        if self.created:
            attrs = [instance for instance in self.vast.instances() if instance['machine_id'] == self.machine_id or instance['id'] == self.id][0]
            if attrs['actual_status'] is None:
                attrs['actual_status'] = 'initializing'
            if attrs['status_msg'] is None:
                attrs['status_msg'] = attrs['actual_status'].title() + '...'
            attrs['status_msg'] = attrs['status_msg'].strip()
            if not hasattr(self, 'status_msg') or attrs['status_msg'] != self.status_msg or attrs['actual_status'] != self.actual_status:
                
                status_msg = attrs['status_msg']
                logmsg = f'{attrs["machine_id"]}: {attrs["actual_status"]}->{attrs["next_state"]}: {status_msg}'
                if 'error' in status_msg or 'Error' in status_msg:
                    logger.error(logmsg)
                    raise VastException(status_msg)
                else:
                    logger.info(logmsg)
            for key, value in attrs.items():
                setattr(self, key, value)
            return attrs
        else:
            return None

    @property
    def docker_tags(self):
        image, *version = self._image.split(':',1)
        tags = self.vast.docker_tags(image)
        if not version:
            return tags[0]
        tags = { version_tags['name']: version_tags for version_tags in tags }
        return tags[version[0]]

    @property
    def compatibility_query(self):
        tags = self.docker_tags
        query = []
        min_cuda = tags.get('min_cuda')
        if min_cuda:
            query.append(f'cuda_max_good>={min_cuda}')
        max_cuda = tags.get('max_cuda')
        if max_cuda:
            query.append(f'cuda_max_good<={max_cuda}')
        extra_filters = tags.get('extra_filters', {})
        for prop, filter in extra_filters.items():
            for op, value in filter.items():
                query.append(f'{prop} {op} {value}')
        return ' '.join(query)

    def create(self, price = None, offer = None):
        assert self.id is None
        if offer is None:
            query = self.compatibility_query + ' ' + self._query
            self.offer = self.vast.offers(self._instance_type, pricing_storage_GiB = self._GiB, sort = 'dph_total', query = query)[0]
        else:
            self.offer = offer
        self.machine_id = self.offer['machine_id']
        if self._instance_type == 'on-demand':
            price = None
        else:
            if price is None:
                #price = selef.vast.offer_bid_price(self.offer['id'])
                price = self.offer['min_bid'] + 0.001
        self.id = self.vast.create(self.offer['id'], disk_GB=self._GiB, image=self._image, price=price)
        return self.update_attributes()

    def destroy(self):
        if self.id is not None:
            self.vast.destroy(self.id)
            self.id = None

    def start(self):
        if not self.created:
            self.create()
        self.vast.start(self.id)
        self.update_attributes()

    def stop(self):
        self.vast.stop(self.id)
        self.update_attributes()

    def wait(self, for_status = None):
        if not self.created:
            return
        attrs = None
        while (
                ((for_status is None and self.actual_status != self.intended_status)
                    or (for_status is not None and self.actual_status != for_status)
                    or (self.actual_status == 'running' and not self.connectable)
                )
                and self.created 
        ):
            time.sleep(4)
            attrs = self.update_attributes()
        return attrs

    async def async_wait(self, for_status = None):
        if not self.created:
            return
        attrs = None
        while (
                ((for_status is None and self.actual_status != self.intended_status)
                    or (for_status is not None and self.actual_status != for_status)
                    or (self.actual_status == 'running' and not self.connectable)
                )
                and self.created 
        ):
            await asyncio.sleep(4)
            attrs = self.update_attributes()
        return attrs

    def _getattr__(self, attr):
        if attr[0] == 'a' and hasattr(self, attr[1:]):
            # async method equivalents starting with 'a'
            method = getattr(self, attr[1:])
            async def wrapper(*params, **kwparams):
                result = method(*params, **kwparams)
                try:
                    await self.async_wait()
                except:
                    await self.destroy()
                    raise
                return result
            return wrapper
        else:
            return super().__getattr__(attr)

    def __del__(self):
        if not self._detached:
            self.destroy()
