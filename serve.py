import sys

from esrally import actor

if __name__ == '__main__':
    coordinator_ip = sys.argv[1]
    actor.bootstrap_actor_system(coordinator_ip=coordinator_ip)
