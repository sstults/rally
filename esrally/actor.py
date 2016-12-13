import sys
import logging
import faulthandler
import signal
import time

import os

import thespian.actors

from esrally import exceptions
from esrally.utils import console

logger = logging.getLogger("rally.actor")


class RallyActor(thespian.actors.Actor):
    def __init__(self):
        super().__init__()
        # see https://groups.google.com/d/msg/thespianpy/FntU9umtvhc/UYizXz8mDQAJ
        logging.getLogger().setLevel(logging.WARNING)
        faulthandler.register(signal.SIGQUIT, file=sys.stderr)

    @staticmethod
    def actorSystemCapabilityCheck(capabilities, requirements):
        for name, value in requirements.items():
            current = capabilities.get(name, None)
            if current != value:
                return False
        return True


# Defined on top-level to allow pickling
class ActorLogFilter(logging.Filter):
    def filter(self, logrecord):
        return "actorAddress" in logrecord.__dict__

# Defined on top-level to allow pickling
class NotActorLogFilter(logging.Filter):
    def filter(self, logrecord):
        return "actorAddress" not in logrecord.__dict__

# Defined on top-level to allow pickling
def configure_utc_formatter(*args, **kwargs):
    formatter = logging.Formatter(fmt=kwargs["fmt"], datefmt=kwargs["datefmt"])
    formatter.converter = time.gmtime
    return formatter


def configure_actor_logging():
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
                "fmt": "%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "()": configure_utc_formatter
            },
            "actor": {
                "fmt": "%(asctime)s,%(msecs)d %(name)s %(levelname)s %(actorAddress)s => %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "()": configure_utc_formatter
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
            "root": {
                "handlers": ["h1", "h2"],
                "level": logging.INFO
            }
        },
        "disable_existing_loggers": True
    }


def my_ip():
    import socket
    #TODO dm: Handle cases without a network card
    #TODO dm: Handle cases with more than one network card...
    #TODO dm: Handle IPv6
    local_ips = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1]
    return local_ips[0]


def bootstrap_actor_system(prefer_local_only=False, local_ip=None, coordinator_ip=None, system_base="multiprocTCPBase"):
    try:
        if prefer_local_only:
            coordinator_ip = "127.0.0.1"
            local_ip = "127.0.0.1"
            coordinator = True
        else:
            if system_base != "multiprocTCPBase" and system_base != "multiprocUDPBase":
                raise exceptions.SystemSetupError("Rally requires a network-capable system base but got [%s]." % system_base)
            if not coordinator_ip:
                raise exceptions.SystemSetupError("coordinator IP is required")
            if not local_ip:
                raise exceptions.SystemSetupError("local IP is required")
            coordinator = local_ip == coordinator_ip

        return thespian.actors.ActorSystem(system_base,
                                           logDefs=configure_actor_logging(),
                                           capabilities={
                                               "coordinator": coordinator,
                                               # just needed to determine whether to run benchmarks locally
                                               "ip": local_ip,
                                               # Completely isolate Actor processes from each other.
                                               #"Process Startup Method": "spawn",
                                               # Make the coordinator node the convention leader
                                               "Convention Address.IPv4": "%s:1900" % coordinator_ip
                                           })
    except thespian.actors.ActorSystemException:
        logger.exception("Could not initialize internal actor system. Terminating.")
        console.error("Could not initialize successfully.\n")
        console.error("Are there are still processes from a previous race?")
        console.error("Please check and terminate related Python processes before running Rally again.\n")
        raise
