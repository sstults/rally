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

    # TODO #71: When we use this for multi-machine clusters, we need to implement a state-machine (wait for all nodes) and also need
    # TODO #71: more context (e.g. "Success" / "Failure" maybe won't cut it).
    def receiveMessage(self, msg, sender):
        try:
            if isinstance(msg, StartEngine):
                self.race_control = sender
                if msg.external:
                    # just create one actor for this special case and run it on the coordinator node (i.e. here)
                    self.mechanics.append(self.createActor(NodeMechanicActor, targetActorRequirements={"coordinator": True}))
                else:
                    hosts = msg.cfg.opts("client", "hosts")
                    if len(hosts) == 0:
                        raise exceptions.LaunchError("No target hosts are configured.")
                    for host in hosts:
                        ip = host["host"]
                        # user may specify "localhost" on the command line but the problem is that we auto-register the actor system
                        # with "ip": "127.0.0.1" so we convert this special case automatically. In all other cases the user needs to
                        # start the actor system on the other host and is aware that the parameter for the actor system and the
                        # --target-hosts parameter need to match.
                        if ip == "localhost" or ip == "127.0.0.1":
                            self.mechanics.append(self.createActor(LocalNodeMechanicActor, targetActorRequirements={"ip": "127.0.0.1"}))
                        else:
                            self.mechanics.append(self.createActor(RemoteNodeMechanicActor, targetActorRequirements={"ip": ip}))
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, EngineStarted):
                self.send(self.race_control, msg)
            elif isinstance(msg, OnBenchmarkStart):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, Success):
                self.send(self.race_control, msg)
            elif isinstance(msg, Failure):
                self.send(self.race_control, msg)
            elif isinstance(msg, OnBenchmarkStop):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, StopEngine):
                for m in self.mechanics:
                    self.send(m, msg)
            elif isinstance(msg, EngineStopped):
                self.send(self.race_control, msg)
                # clear all state as the mechanic might get reused later
                for m in self.mechanics:
                    self.send(m, thespian.actors.ActorExitRequest())
                self.mechanics = []
                # self terminate + slave nodes
                self.send(self.myAddress, thespian.actors.ActorExitRequest())
        except BaseException as e:
            # Is it ok to send the message always to the coordinator?
            self.send(self.race_control, Failure("Could not execute command", e))


class NodeMechanicActor(thespian.actors.Actor):
    """
    One instance of this actor is run on each target host and coordinates the actual work of starting / stopping nodes.
    """
    def __init__(self, single_machine):
        super().__init__()
        self.config = None
        self.metrics_store = None
        self.mechanic = None
        self.single_machine = single_machine

    @staticmethod
    def actorSystemCapabilityCheck(capabilities, requirements):
        for name, value in requirements.items():
            current = capabilities.get(name, None)
            if current != value:
                return False
        return True

    def receiveMessage(self, msg, sender):
        # at the moment, we implement all message handling blocking. This is not ideal but simple to get started with. Besides, the caller
        # needs to block anyway. The only reason we implement mechanic as an actor is to distribute them.
        # noinspection PyBroadException
        try:
            if isinstance(msg, StartEngine):
                # Load node-specific configuration
                self.config = config.Config(config_name=msg.cfg.name)
                self.config.load_config()
                self.config.add(config.Scope.application, "node", "rally.root", paths.rally_root())
                self.config.add(config.Scope.application, "node", "invocation.root.dir", paths.Paths(msg.cfg).invocation_root())
                # copy only the necessary configuration sections
                self.config.add_all(msg.cfg, "system")
                self.config.add_all(msg.cfg, "client")
                self.config.add_all(msg.cfg, "track")
                self.config.add_all(msg.cfg, "mechanic")

                self.metrics_store = metrics.InMemoryMetricsStore(self.config)
                invocation = self.config.opts("system", "time.start")
                track_name = self.config.opts("track", "track.name")
                challenge_name = self.config.opts("track", "challenge.name")
                selected_car_name = self.config.opts("mechanic", "car.name")
                self.metrics_store.open(invocation, track_name, challenge_name, selected_car_name)

                self.mechanic = create(self.config, self.metrics_store, self.single_machine, msg.sources, msg.build, msg.distribution,
                                       msg.external, msg.docker)
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
                self.config = None
                self.mechanic = None
                self.metrics_store = None
        except BaseException:
            import traceback
            self.send(sender, Failure("Could not execute command", traceback.format_exc()))


class LocalNodeMechanicActor(NodeMechanicActor):
    def __init__(self):
        super().__init__(single_machine=True)


class RemoteNodeMechanicActor(NodeMechanicActor):
    def __init__(self):
        super().__init__(single_machine=False)


#####################################################
# Internal API (only used by the actor and for tests)
#####################################################

def create(cfg, metrics_store, single_machine=True, sources=False, build=False, distribution=False, external=False, docker=False):
    # TODO dm: Check whether we can remove this completely (-> just create a directory at the top of the race directory)
    # ensure we don't mix ES installs
    track_name = cfg.opts("track", "track.name")
    challenge_name = cfg.opts("track", "challenge.name")
    race_paths = paths.Paths(cfg)
    challenge_root_path = race_paths.challenge_root(track_name, challenge_name)
    challenge_log_path = race_paths.challenge_logs(track_name, challenge_name)
    # TODO dm: remove key "provisioning", "local.install.dir" from config (is defined in config file -> migration)
    install_dir = "%s/install" % challenge_root_path

    if sources:
        s = lambda: supplier.from_sources(cfg, build)
        p = provisioner.local_provisioner(cfg, install_dir, single_machine)
        l = launcher.InProcessLauncher(cfg, metrics_store, challenge_root_path, challenge_log_path)
    elif distribution:
        s = lambda: supplier.from_distribution(cfg)
        p = provisioner.local_provisioner(cfg, install_dir, single_machine)
        l = launcher.InProcessLauncher(cfg, metrics_store, challenge_root_path, challenge_log_path)
    elif external:
        s = lambda: None
        p = provisioner.no_op_provisioner(cfg)
        l = launcher.ExternalLauncher(cfg, metrics_store)
    elif docker:
        s = lambda: None
        p = provisioner.docker_provisioner(cfg, install_dir)
        l = launcher.DockerLauncher(cfg, metrics_store)
    else:
        # It is a programmer error (and not a user error) if this function is called with wrong parameters
        raise RuntimeError("One of sources, distribution, docker or external must be True")

    return Mechanic(s, p, l)


class Mechanic:
    """
    Mechanic is responsible for preparing the benchmark candidate (i.e. all benchmark candidate related activities before and after
    running the benchmark).
    """

    def __init__(self, supply, p, l):
        self.supply = supply
        self.provisioner = p
        self.launcher = l
        self.cluster = None

    def start_engine(self):
        binary = self.supply()
        self.provisioner.prepare(binary)
        logger.info("Starting engine.")
        self.cluster = self.launcher.start(self.provisioner.car, self.provisioner.binary_path, self.provisioner.data_paths)
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
