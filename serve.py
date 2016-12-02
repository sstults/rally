import logging
import sys

from thespian.actors import ActorSystem

ip_capability = sys.argv[1]


def configure_actor_logging():
    class ActorLogFilter(logging.Filter):
        def filter(self, logrecord):
            return "actorAddress" in logrecord.__dict__

    class NotActorLogFilter(logging.Filter):
        def filter(self, logrecord):
            return "actorAddress" not in logrecord.__dict__

    # TODO dm: Only stdout logging for the moment.
    actor_log_handler = {"class": "logging.StreamHandler", "stream": sys.stdout}
    actor_messages_handler = {"class": "logging.StreamHandler", "stream": sys.stdout}

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


ActorSystem('multiprocTCPBase',
            logDefs=configure_actor_logging(),
            capabilities={
                "ip": ip_capability,
                "Convention Address.IPv4": (ip_capability, 1900),
            })
