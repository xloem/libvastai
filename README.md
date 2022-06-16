# libvastai

[https://vast.au/] is service for connecting renters and hosts of machines with powerful GPUs together.

This library provides a partial programmatic interface to their api for renting and selling system time.

## Example

```
import vast, logging, fabric
logging.basicConfig(level=logging.INFO)

instance = vast.Instance(vast = vast.Vast(key = 'your_api_key'))
instance.create()
instance.wait() # poll until the system reaches its target state; there is also wait_async and acreate

with fabric.Connection(instance.ssh_host, 'root', instance.ssh_port) as shell:
    print(shell.run('nvidia-smi').stdout)

instance.destroy()
```
