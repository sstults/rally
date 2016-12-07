import sys
import logging

from esrally import actor
from esrally.utils import console

if __name__ == '__main__':
    local_ip = sys.argv[1]
    coordinator_ip = sys.argv[2]
    # TheSpian writes the following warning upon start (at least) on Mac OS X:
    #
    # WARNING:root:Unable to get address info for address 103.1.168.192.in-addr.arpa (AddressFamily.AF_INET,\
    # SocketKind.SOCK_DGRAM, 17, 0): <class 'socket.gaierror'> [Errno 8] nodename nor servname provided, or not known
    #
    # Therefore, we will not show warnings but only errors.
    logging.basicConfig(level=logging.ERROR)
    actor.bootstrap_actor_system(local_ip=local_ip, coordinator_ip=coordinator_ip)
    console.println("Successfully started local actor system")

