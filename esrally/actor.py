import sys
import logging
import faulthandler
import signal


import thespian.actors

from esrally import exceptions
from esrally.utils import console

logger = logging.getLogger("rally.actor")


class RallyActor(thespian.actors.Actor):
    def __init__(self):
        super().__init__()
        faulthandler.register(signal.SIGQUIT, file=sys.stderr)

    @staticmethod
    def actorSystemCapabilityCheck(capabilities, requirements):
        for name, value in requirements.items():
            current = capabilities.get(name, None)
            if current != value:
                return False
        return True


def configure_actor_logging():
    class ActorLogFilter(logging.Filter):
        def filter(self, logrecord):
            return "actorAddress" in logrecord.__dict__

    class NotActorLogFilter(logging.Filter):
        def filter(self, logrecord):
            return "actorAddress" not in logrecord.__dict__

    # TODO dm: Only stdout logging for the moment.
    actor_log_handler = {"class": "logging.StreamHandler", "stream": sys.stderr}
    actor_messages_handler = {"class": "logging.StreamHandler", "stream": sys.stderr}

    # actor_log_handler = {"class": "logging.FileHandler", "filename": "%s/rally-actors.log" % log_dir}
    # actor_messages_handler = {"class": "logging.FileHandler", "filename": "%s/rally-actor-messages.log" % log_dir}


    # actor_log_handler = {"class": "logging.handlers.SysLogHandler", "address": "/var/run/syslog"}
    # actor_messages_handler = {"class": "logging.handlers.SysLogHandler", "address": "/var/run/syslog"}

    actor_log_handler["formatter"] = "normal"
    actor_log_handler["filters"] = ["notActorLog"]
    actor_log_handler["level"] = logging.INFO

    actor_messages_handler["formatter"] = "actor"
    actor_messages_handler["filters"] = ["isActorLog"]
    actor_messages_handler["level"] = logging.INFO

    return {
        "version": 1,
        "formatters": {
            "normal": {
                "format": "%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s"
            },
            "actor": {
                "format": "%(asctime)s,%(msecs)d %(name)s %(levelname)s %(actorAddress)s => %(message)s"
            }
        },
        "filters": {
            "isActorLog": {
                "()": ActorLogFilter
            },
            "notActorLog": {
                "()": NotActorLogFilter
            }
        },
        "handlers": {
            "h1": actor_log_handler,
            "h2": actor_messages_handler
        },
        "loggers": {
            "": {
                "handlers": ["h1", "h2"], "level": logging.INFO
            }
        }
    }


def my_ip():
    import socket
    #TODO dm: Handle cases without a network card
    #TODO dm: Handle cases with more than one network card...
    #TODO dm: Handle IPv6
    local_ips = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1]
    return local_ips[0]


def bootstrap_actor_system(prefer_local_only=False, coordinator_ip=None, system_base="multiprocTCPBase"):
    try:
        if prefer_local_only:
            coordinator_ip = "127.0.0.1"
            local_ip = "127.0.0.1"
            coordinator = True
        else:
            local_ip = my_ip()
            if not coordinator_ip:
                raise exceptions.SystemSetupError("coordinator IP is required")
            coordinator = local_ip == coordinator_ip

        return thespian.actors.ActorSystem(system_base,
                                           logDefs=configure_actor_logging(),
                                           capabilities={
                                               "coordinator": coordinator,
                                               # just needed to determine whether to run benchmarks locally
                                               "ip": local_ip,
                                               # Make the coordinator node the convention leader
                                               "Convention Address.IPv4": "%s:1900" % coordinator_ip
                                           })
    except thespian.actors.ActorSystemException:
        logger.exception("Could not initialize internal actor system. Terminating.")
        console.error("Could not initialize successfully.\n")
        console.error("Are there are still processes from a previous race?")
        console.error("Please check and terminate related Python processes before running Rally again.\n")
        raise
