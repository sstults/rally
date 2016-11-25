import logging

import thespian.actors

from esrally import paths, config, metrics, exceptions
from esrally.mechanic import supplier, provisioner, launcher

logger = logging.getLogger("rally.mechanic")


##########
# Messages
##########

class ClusterMetaInfo:
    def __init__(self, hosts, revision, distribution_version):
        self.hosts = hosts
        self.revision = revision
        self.distribution_version = distribution_version


class StartEngine:
    def __init__(self, cfg, sources, build, distribution, external, docker):
        self.cfg = cfg
        self.sources = sources
        self.build = build
        self.distribution = distribution
        self.external = external
        self.docker = docker


class EngineStarted:
    def __init__(self, cluster_meta_info, system_meta_info):
        self.cluster_meta_info = cluster_meta_info
        self.system_meta_info = system_meta_info


class StopEngine:
    pass


class EngineStopped:
    def __init__(self, system_metrics):
        self.system_metrics = system_metrics


class Success:
    pass


class Failure:
    def __init__(self, message, cause):
        self.message = message
        self.cause = cause


class OnBenchmarkStart:
    def __init__(self, lap):
        self.lap = lap


# TODO dm: Add metrics store here as param?
class OnBenchmarkStop:
    pass


class MechanicActor(thespian.actors.Actor):
    """
    This actor coordinates all associated mechanics on remote hosts (which do the actual work).
    """
    def __init__(self):
        super().__init__()
        self.mechanics = []
        self.race_control = None

    def receiveMessage(self, msg, sender):
        try:
            if isinstance(msg, StartEngine):
                self.race_control = sender
                if msg.external:
                    # just create one actor for this special case and run it on the coordinator node (i.e. here)
                    self.mechanics.append(self.createActor(AssistantMechanic, targetActorRequirements={"coordinator": True}))
                else:
                    hosts = msg.cfg.opts("client", "hosts")
                    if len(hosts) == 0:
                        raise exceptions.LaunchError("No target hosts are configured.")
                    for host in hosts:
                        ip = host["host"]
                        # TODO dm: What do we do if the user specifies "localhost" but we've only registered "127.0.0.1"?
                        self.mechanics.append(self.createActor(AssistantMechanic, targetActorRequirements={"ip": ip}))
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, EngineStarted):
                # TODO: wait for all EngineStarted events and notify coordinator as soon as all nodes indicated success (just one slave atm)
                self.send(self.race_control, msg)
            elif isinstance(msg, OnBenchmarkStart):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, Success):
                # TODO: We will need more context here if we have more slave nodes (also: we maybe need to wait here until all messages have arrived)
                self.send(self.race_control, msg)
            elif isinstance(msg, Failure):
                # TODO: We will need more context here if we have more slave nodes
                self.send(self.race_control, msg)
            elif isinstance(msg, OnBenchmarkStop):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, StopEngine):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, EngineStopped):
                # TODO: wait for all and only then send the message to the race_control
                self.send(self.race_control, msg)
                # clear all state as the mechanic might get reused later
                for m in self.mechanics:
                    self.send(m, thespian.actors.ActorExitRequest())
                self.mechanics = []
                # self terminate + slave nodes
                # TODO dm: Should the coordinator node (i.e. race control) decide when to stop the mechanic?
                self.send(self.myAddress, thespian.actors.ActorExitRequest())
        except BaseException as e:
            # Is it ok to send the message always to the coordinator?
            self.send(self.race_control, Failure("Could not execute command", e))


class AssistantMechanic(thespian.actors.Actor):
    """
    One instance of this actor is run on each target host and coordinates the actual work of starting / stopping nodes.
    """
    def __init__(self):
        super().__init__()
        self.metrics_store = None
        self.mechanic = None

    def receiveMessage(self, msg, sender):
        # at the moment, we implement all message handling blocking. This is not ideal but simple to get started with. Besides, the caller
        # needs to block anyway. The only reason we implement mechanic as an actor is to distribute them.
        try:
            if isinstance(msg, StartEngine):
                self.metrics_store = metrics.InMemoryMetricsStore(msg.cfg)
                invocation = msg.cfg.opts("meta", "time.start")
                track_name = msg.cfg.opts("benchmarks", "track")
                challenge_name = msg.cfg.opts("benchmarks", "challenge")
                selected_car_name = msg.cfg.opts("benchmarks", "car")
                self.metrics_store.open(invocation, track_name, challenge_name, selected_car_name)

                self.mechanic = create(msg.cfg, self.metrics_store, msg.sources, msg.build, msg.distribution, msg.external, msg.docker)
                cluster = self.mechanic.start_engine()
                self.send(sender, EngineStarted(
                    ClusterMetaInfo(cluster.hosts, cluster.source_revision, cluster.distribution_version),
                    self.metrics_store.meta_info))
            elif isinstance(msg, OnBenchmarkStart):
                self.metrics_store.lap = msg.lap
                self.mechanic.on_benchmark_start()
                self.send(sender, Success())
            elif isinstance(msg, OnBenchmarkStop):
                self.mechanic.on_benchmark_stop()
                self.send(sender, Success())
            elif isinstance(msg, StopEngine):
                self.mechanic.stop_engine()
                self.send(sender, EngineStopped(self.metrics_store.to_externalizable()))
                # clear all state as the mechanic might get reused later
                self.mechanic = None
                self.metrics_store = None
        except BaseException as e:
            self.send(sender, Failure("Could not execute command", e))


#####################################################
# Internal API (only used by the actor and for tests)
#####################################################

def create(cfg, metrics_store, sources=False, build=False, distribution=False, external=False, docker=False):
    if sources:
        s = lambda: supplier.from_sources(cfg, build)
        p = provisioner.local_provisioner(cfg)
        l = launcher.InProcessLauncher(cfg, metrics_store)
    elif distribution:
        s = lambda: supplier.from_distribution(cfg)
        p = provisioner.local_provisioner(cfg)
        l = launcher.InProcessLauncher(cfg, metrics_store)
    elif external:
        s = lambda: None
        p = provisioner.no_op_provisioner(cfg)
        l = launcher.ExternalLauncher(cfg, metrics_store)
    elif docker:
        s = lambda: None
        p = provisioner.no_op_provisioner(cfg)
        l = launcher.DockerLauncher(cfg, metrics_store)
    else:
        # It is a programmer error (and not a user error) if this function is called with wrong parameters
        raise RuntimeError("One of sources, distribution, docker or external must be True")

    return Mechanic(cfg, s, p, l)


class Mechanic:
    """
    Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
    running the benchmark).
    """

    def __init__(self, cfg, supply, p, l):
        self._config = cfg
        self.supply = supply
        self.provisioner = p
        self.launcher = l
        self.cluster = None

        # TODO dm: Check whether we can remove this completely (-> just create a directory at the top of the race directory)
        # ensure we don't mix ES installs
        track_name = self._config.opts("benchmarks", "track")
        challenge_name = self._config.opts("benchmarks", "challenge")
        race_paths = paths.Paths(self._config)
        self._config.add(config.Scope.challenge, "system", "challenge.root.dir",
                         race_paths.challenge_root(track_name, challenge_name))
        self._config.add(config.Scope.challenge, "system", "challenge.log.dir",
                         race_paths.challenge_logs(track_name, challenge_name))

    def start_engine(self):
        self.supply()
        selected_car = self.provisioner.prepare()
        logger.info("Starting engine.")
        self.cluster = self.launcher.start(selected_car)
        return self.cluster

    def on_benchmark_start(self):
        logger.info("Notifying cluster of benchmark start.")
        self.cluster.on_benchmark_start()

    def on_benchmark_stop(self):
        logger.info("Notifying cluster of benchmark stop.")
        self.cluster.on_benchmark_stop()

    def stop_engine(self):
        logger.info("Stopping engine.")
        self.launcher.stop(self.cluster)
        self.cluster = None
        self.provisioner.cleanup()
