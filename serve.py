import sys

from thespian.actors import ActorSystem

ip_capability = sys.argv[1]

ActorSystem('multiprocTCPBase', capabilities={"ip": ip_capability})
