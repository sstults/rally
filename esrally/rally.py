import argparse
import datetime
import logging
import os
import shutil
import sys
import time

import pkg_resources

from esrally import actor, config, paths, racecontrol, reporter, metrics, track, exceptions, PROGRAM_NAME, DOC_LINK
from esrally.mechanic import car, telemetry
from esrally.utils import io, convert, git, process, console, net

__version__ = pkg_resources.require("esrally")[0].version

BANNER = """
    ____        ____
   / __ \____ _/ / /_  __
  / /_/ / __ `/ / / / / /
 / _, _/ /_/ / / / /_/ /
/_/ |_|\__,_/_/_/\__, /
                /____/
"""

logger = logging.getLogger("rally.main")

SKULL = '''
                 uuuuuuu
             uu$$$$$$$$$$$uu
          uu$$$$$$$$$$$$$$$$$uu
         u$$$$$$$$$$$$$$$$$$$$$u
        u$$$$$$$$$$$$$$$$$$$$$$$u
       u$$$$$$$$$$$$$$$$$$$$$$$$$u
       u$$$$$$$$$$$$$$$$$$$$$$$$$u
       u$$$$$$"   "$$$"   "$$$$$$u
       "$$$$"      u$u       $$$$"
        $$$u       u$u       u$$$
        $$$u      u$$$u      u$$$
         "$$$$uu$$$   $$$uu$$$$"
          "$$$$$$$"   "$$$$$$$"
            u$$$$$$$u$$$$$$$u
             u$"$"$"$"$"$"$u
  uuu        $$u$ $ $ $ $u$$       uuu
 u$$$$        $$$$$u$u$u$$$       u$$$$
  $$$$$uu      "$$$$$$$$$"     uu$$$$$$
u$$$$$$$$$$$uu    """""    uuuu$$$$$$$$$$
$$$$"""$$$$$$$$$$uuu   uu$$$$$$$$$"""$$$"
"""      ""$$$$$$$$$$$uu ""$"""
uuuu ""$$$$$$$$$$uuu
u$$$uuu$$$$$$$$$uu ""$$$$$$$$$$$uuu$$$
$$$$$$$$$$""""           ""$$$$$$$$$$$"
   "$$$$$"                      ""$$$$""
     $$$"                         $$$$"
'''


def version():
    release = __version__
    # noinspection PyBroadException
    try:
        if git.is_working_copy(io.normalize_path("%s/.." % paths.rally_root())):
            revision = git.head_revision(paths.rally_root())
            return "%s (git revision: %s)" % (release, revision.strip())
    except BaseException:
        pass
    # cannot determine head revision so user has probably installed Rally via pip instead of git clone
    return release


# we want to use some basic logging even before the output to log file is configured
def pre_configure_logging():
    logging.basicConfig(level=logging.INFO)


def log_file_path(cfg):
    log_dir = paths.Paths(cfg).log_root()
    node_name = cfg.opts("system", "node.name", mandatory=False)
    if node_name:
        return "%s/rally_out_%s.log" % (log_dir, node_name)
    else:
        return "%s/rally_out.log" % log_dir


def configure_logging(cfg):
    # Even if we don't log to a file, other parts of the application rely on this path to exist -> enforce
    log_file = log_file_path(cfg)
    log_dir = os.path.dirname(log_file)
    io.ensure_dir(log_dir)
    cfg.add(config.Scope.application, "system", "log.dir", log_dir)

    logging_output = cfg.opts("system", "logging.output")

    if logging_output == "file":
        console.info("Writing logs to %s" % log_file)
        # there is an old log file lying around -> backup
        if os.path.exists(log_file):
            os.rename(log_file, "%s-bak-%d.log" % (log_file, int(os.path.getctime(log_file))))
        ch = logging.FileHandler(filename=log_file, mode="a")
    else:
        ch = logging.StreamHandler(stream=sys.stdout)

    log_level = logging.INFO
    ch.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    formatter.converter = time.gmtime
    ch.setFormatter(formatter)

    # Remove all handlers associated with the root logger object so we can start over with an entirely fresh log configuration
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.root.addHandler(ch)
    logging.getLogger("elasticsearch").setLevel(logging.WARN)


def parse_args():
    def positive_number(v):
        value = int(v)
        if value <= 0:
            raise argparse.ArgumentTypeError("must be positive but was %s" % value)
        return value

    # try to preload configurable defaults, but this does not work together with `--configuration-name` (which is undocumented anyway)
    cfg = config.Config()
    if cfg.config_present():
        cfg.load_config()
        preserve_install = cfg.opts("defaults", "preserve_benchmark_candidate", default_value=False, mandatory=False)
    else:
        preserve_install = False

    # workaround for http://bugs.python.org/issue13041
    #
    # Set a proper width (see argparse.HelpFormatter)
    try:
        int(os.environ["COLUMNS"])
    except (KeyError, ValueError):
        # noinspection PyBroadException
        try:
            os.environ['COLUMNS'] = str(shutil.get_terminal_size().columns)
        except BaseException:
            # don't fail if anything goes wrong here
            pass

    parser = argparse.ArgumentParser(prog=PROGRAM_NAME,
                                     description=BANNER + "\n\n You know for benchmarking Elasticsearch.",
                                     epilog="Find out more about Rally at %s" % console.format.link(DOC_LINK),
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='version', version="%(prog)s " + version())

    subparsers = parser.add_subparsers(
        title="subcommands",
        dest="subcommand",
        help="")

    race_parser = subparsers.add_parser("race", help="Run the benchmarking pipeline. This sub-command should typically be used.")
    # change in favor of "list telemetry", "list tracks", "list pipelines"
    list_parser = subparsers.add_parser("list", help="List configuration options")
    list_parser.add_argument(
        "configuration",
        metavar="configuration",
        help="The configuration for which Rally should show the available options. "
             "Possible values are: telemetry, tracks, pipelines, races, cars",
        choices=["telemetry", "tracks", "pipelines", "races", "cars"])
    list_parser.add_argument(
        "--limit",
        help="Limit the number of search results for recent races (default: 10).",
        default=10,
    )

    compare_parser = subparsers.add_parser("compare", help="Compare two races")
    compare_parser.add_argument(
        "--baseline",
        help="Race timestamp of the baseline (see %s list races)" % PROGRAM_NAME,
        default="")
    compare_parser.add_argument(
        "--contender",
        help="Race timestamp of the contender (see %s list races)" % PROGRAM_NAME,
        default="")

    config_parser = subparsers.add_parser("configure", help="Write the configuration file or reconfigure Rally")
    for p in [parser, config_parser]:
        p.add_argument(
            "--advanced-config",
            help="show additional configuration options (default: false)",
            default=False,
            action="store_true")
        p.add_argument(
            "--assume-defaults",
            help="Automatically accept all options with default values (default: false)",
            default=False,
            action="store_true")

    for p in [parser, race_parser]:
        p.add_argument(
            "--pipeline",
            help="select the pipeline to run.",
            # the default will be dynamically derived by racecontrol based on the presence / absence of other command line options
            default="")
        p.add_argument(
            "--preserve-install",
            help="keep the benchmark candidate and its index. (default: %s)" % str(preserve_install).lower(),
            default=preserve_install)
        p.add_argument(
            "--telemetry",
            help="enable the provided telemetry devices, provided as a comma-separated list. List possible telemetry devices "
                 "with `%s list telemetry`" % PROGRAM_NAME,
            default="")
        p.add_argument(
            "--revision",
            help="define the source code revision for building the benchmark candidate. 'current' uses the source tree as is,"
                 " 'latest' fetches the latest version on master. It is also possible to specify a commit id or a timestamp."
                 " The timestamp must be specified as: \"@ts\" where \"ts\" must be a valid ISO 8601 timestamp, "
                 "e.g. \"@2013-07-27T10:37:00Z\" (default: current).",
            default="current")  # optimized for local usage, don't fetch sources
        p.add_argument(
            "--track",
            help="define the track to use. List possible tracks with `%s list tracks` (default: geonames)." % PROGRAM_NAME,
            default="geonames")
        p.add_argument(
            "--challenge",
            help="define the challenge to use. List possible challenges for tracks with `%s list tracks`"
                 " (default: append-no-conflicts)." % PROGRAM_NAME,
            default="append-no-conflicts")  # optimized for local usage
        p.add_argument(
            "--car",
            help="define the car to use. List possible cars with `%s list cars` (default: defaults)." % PROGRAM_NAME,
            default="defaults")  # optimized for local usage

        p.add_argument(
            "--target-hosts",
            help="define a comma-separated list of host:port pairs which should be targeted iff using the pipeline 'benchmark-only' "
                 "(default: localhost:9200).",
            default="")  # actually the default is pipeline specific and it is set later
        p.add_argument(
            "--client-options",
            help="define a comma-separated list of client options to use. The options will be passed to the Elasticsearch Python client "
                 "(default: timeout:60000,request_timeout:60000).",
            default="timeout:60000,request_timeout:60000")
        p.add_argument(
            "--user-tag",
            help="define a user-specific key-value pair (separated by ':'). It is added to each metric record as meta info. "
                 "Example: intention:baseline-ticket-12345",
            default="")
        p.add_argument(
            "--report-format",
            help="define the output format for the command line report (default: markdown).",
            choices=["markdown", "csv"],
            default="markdown")
        p.add_argument(
            "--report-file",
            help="write the command line report also to the provided file",
            default="")
        p.add_argument(
            "--quiet",
            help="suppress as much as output as possible (default: false).",
            default=False,
            action="store_true")
        p.add_argument(
            "--laps",
            type=positive_number,
            help="number of laps that the benchmark should run (default: 1).",
            default=1)
        # undocumented for the time being...
        p.add_argument(
            "--test-mode",
            help=argparse.SUPPRESS,
            default=False,
            action="store_true")

    for p in [parser, list_parser, race_parser]:
        p.add_argument(
            "--distribution-version",
            help="define the version of the Elasticsearch distribution to download. "
                 "Check https://www.elastic.co/downloads/elasticsearch for released versions.",
            default="")
        p.add_argument(
            "--distribution-repository",
            help="define the repository from where the Elasticsearch distribution should be downloaded (default: release).",
            choices=["snapshot", "release"],
            default="release")
        p.add_argument(
            "--track-repository",
            help="define the repository from where Rally will load tracks (default: default).",
            default="default")
        p.add_argument(
            "--offline",
            help="assume that Rally has no connection to the Internet (default: false)",
            default=False,
            action="store_true")

    ###############################################################################
    #
    # The options below are undocumented and can be removed or changed at any time.
    #
    ###############################################################################
    for p in [parser, race_parser]:
        # This option is intended to tell Rally to assume a different start date than 'now'. This is effectively just useful for things like
        # backtesting or a benchmark run across environments (think: comparison of EC2 and bare metal) but never for the typical user.
        p.add_argument(
            "--effective-start-date",
            help=argparse.SUPPRESS,
            type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
            default=datetime.datetime.utcnow())
        # This is a highly experimental option and will likely be removed
        p.add_argument(
            "--data-paths",
            help=argparse.SUPPRESS,
            default=None)
        p.add_argument(
            "--override-src-dir",
            help=argparse.SUPPRESS,
            default=None)
        p.add_argument(
            "--cluster-health",
            choices=["red", "yellow", "green"],
            help=argparse.SUPPRESS,
            default="green")

    for p in [parser, config_parser, list_parser, race_parser]:
        # This option is needed to support a separate configuration for the integration tests on the same machine
        p.add_argument(
            "--configuration-name",
            help=argparse.SUPPRESS,
            default=None)
        p.add_argument(
            "--logging",
            choices=["file", "console"],
            help=argparse.SUPPRESS,
            default="file"
        )

    return parser.parse_args()


def derive_sub_command(args, cfg):
    sub_command = args.subcommand
    # first, trust the user...
    if sub_command is not None:
        return sub_command
    # we apply some smarts in case the user did not specify a sub-command
    if cfg.config_present():
        return "race"
    else:
        return "configure"


def ensure_configuration_present(cfg, args, sub_command):
    if sub_command == "configure":
        config.ConfigFactory().create_config(cfg.config_file, advanced_config=args.advanced_config, assume_defaults=args.assume_defaults)
        exit(0)
    else:
        if cfg.config_present():
            cfg.load_config()
            if not cfg.config_compatible():
                cfg.migrate_config()
                # Reload config after upgrading
                cfg.load_config()
        else:
            console.error("No config present. Please run '%s configure' first." % PROGRAM_NAME)
            exit(64)


def list(cfg):
    what = cfg.opts("system", "list.config.option")
    if what == "telemetry":
        telemetry.list_telemetry()
    elif what == "tracks":
        track.list_tracks(cfg)
    elif what == "pipelines":
        racecontrol.list_pipelines()
    elif what == "races":
        metrics.list_races(cfg)
    elif what == "cars":
        car.list_cars()
    else:
        raise exceptions.SystemSetupError("Cannot list unknown configuration option [%s]" % what)


def print_help_on_errors(cfg):
    heading = "Getting further help:"
    console.println(console.format.bold(heading))
    console.println(console.format.underline_for(heading))
    console.println("* Check the log file at %s for errors." % log_file_path(cfg))
    console.println("* Read the documentation at %s" % console.format.link(DOC_LINK))
    console.println("* Ask a question in the forum at %s" % console.format.link("https://discuss.elastic.co/c/rally"))
    console.println("* Raise an issue at %s and include the log file in %s." %
                    (console.format.link("https://github.com/elastic/rally/issues"), log_file_path(cfg)))


def race(cfg):
    try:
        actors = actor.bootstrap_actor_system(prefer_local_only=True)
    except RuntimeError as e:
        logger.exception("Could not bootstrap actor system.")
        if str(e) == "Unable to determine valid external socket address.":
            console.warn("Could not determine a socket address. Are you running without any network?", logger=logger)
            actors = actor.bootstrap_actor_system(system_base="multiprocQueueBase")
        else:
            raise
    try:
        racecontrol.run(cfg)
    finally:
        shutdown_complete = False
        times_interrupted = 0
        while not shutdown_complete and times_interrupted < 2:
            try:
                logger.info("Attempting to shutdown internal actor system.")
                actors.shutdown()

                shutdown_complete = True
                logger.info("Shutdown completed.")
            except KeyboardInterrupt:
                times_interrupted += 1
                logger.warning("User interrupted shutdown of internal actor system.")
                console.info("Please wait a moment for Rally's internal components to shutdown.")
        if not shutdown_complete and times_interrupted > 0:
            logger.warning("Terminating after user has interrupted actor system shutdown explicitly for [%d] times." % times_interrupted)
            console.println("")
            console.warn("Terminating now at the risk of leaving child processes behind.")
            console.println("")
            console.warn("The next race may fail due to an unclean shutdown.")
            console.println("")
            console.println(SKULL)
            console.println("")


def dispatch_sub_command(cfg, sub_command):
    try:
        if sub_command == "compare":
            reporter.compare(cfg)
        elif sub_command == "list":
            list(cfg)
        elif sub_command == "race":
            race(cfg)
        else:
            raise exceptions.SystemSetupError("Unknown subcommand [%s]" % sub_command)
        return True
    except exceptions.RallyError as e:
        logging.exception("Cannot run subcommand [%s]." % sub_command)
        console.error("Cannot %s. %s." % (sub_command, e))
        console.println("")
        print_help_on_errors(cfg)
        return False
    except BaseException as e:
        logging.exception("A fatal error occurred while running subcommand [%s]." % sub_command)
        console.error("Cannot %s. %s." % (sub_command, e))
        console.println("")
        print_help_on_errors(cfg)
        return False


def csv_to_list(csv):
    if csv is None:
        return None
    elif len(csv.strip()) == 0:
        return []
    else:
        return [e.strip() for e in csv.split(",")]


def kv_to_map(kvs):
    def convert(v):
        # string
        if v.startswith("'"):
            return v[1:-1]

        # int
        try:
            return int(v)
        except ValueError:
            pass

        # float
        try:
            return float(v)
        except ValueError:
            pass

        # boolean
        if v.lower() == "false":
            return False
        elif v.lower() == "true":
            return True
        else:
            raise ValueError("Could not convert value '%s'" % v)

    result = {}
    for kv in kvs:
        k, v = kv.split(":")
        # key is always considered a string, value needs to be converted
        result[k.strip()] = convert(v.strip())
    return result


def convert_hosts(configured_host_list):
    hosts = []
    try:
        for authority in configured_host_list:
            host, port = authority.split(":")
            hosts.append({"host": host, "port": port})
        return hosts
    except ValueError:
        msg = "Could not convert hosts [%s] (expected a list of host:port pairs e.g. host1:9200,host2:9200)." % configured_host_list
        logger.exception(msg)
        raise exceptions.SystemSetupError(msg)


def main():
    start = time.time()
    # Early init of console output so we start to show everything consistently.
    console.init(quiet=False)

    pre_configure_logging()
    args = parse_args()

    console.init(quiet=args.quiet)
    console.println(BANNER)

    cfg = config.Config(config_name=args.configuration_name)
    sub_command = derive_sub_command(args, cfg)
    ensure_configuration_present(cfg, args, sub_command)

    cfg.add(config.Scope.application, "system", "time.start", args.effective_start_date)
    cfg.add(config.Scope.applicationOverride, "system", "quiet.mode", args.quiet)

    # per node?
    cfg.add(config.Scope.applicationOverride, "system", "offline.mode", args.offline)
    cfg.add(config.Scope.applicationOverride, "system", "logging.output", args.logging)

    # Local config per node
    cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
    cfg.add(config.Scope.application, "node", "rally.cwd", os.getcwd())
    cfg.add(config.Scope.application, "node", "invocation.root.dir", paths.Paths(cfg).invocation_root())

    cfg.add(config.Scope.applicationOverride, "mechanic", "source.revision", args.revision)
    #TODO dm: Consider renaming this one. It's used by different modules
    cfg.add(config.Scope.applicationOverride, "mechanic", "distribution.version", args.distribution_version)
    cfg.add(config.Scope.applicationOverride, "mechanic", "distribution.repository", args.distribution_repository)
    cfg.add(config.Scope.applicationOverride, "mechanic", "car.name", args.car)
    cfg.add(config.Scope.applicationOverride, "mechanic", "node.datapaths", csv_to_list(args.data_paths))
    cfg.add(config.Scope.applicationOverride, "mechanic", "preserve.install", convert.to_bool(args.preserve_install))
    cfg.add(config.Scope.applicationOverride, "mechanic", "telemetry.devices", csv_to_list(args.telemetry))
    if args.override_src_dir is not None:
        cfg.add(config.Scope.applicationOverride, "mechanic", "local.src.dir", args.override_src_dir)

    cfg.add(config.Scope.applicationOverride, "race", "pipeline", args.pipeline)
    cfg.add(config.Scope.applicationOverride, "race", "laps", args.laps)
    cfg.add(config.Scope.applicationOverride, "race", "user.tag", args.user_tag)

    cfg.add(config.Scope.applicationOverride, "track", "repository.name", args.track_repository)
    cfg.add(config.Scope.applicationOverride, "track", "track.name", args.track)
    cfg.add(config.Scope.applicationOverride, "track", "challenge.name", args.challenge)
    cfg.add(config.Scope.applicationOverride, "track", "test.mode.enabled", args.test_mode)

    cfg.add(config.Scope.applicationOverride, "reporting", "format", args.report_format)
    cfg.add(config.Scope.applicationOverride, "reporting", "output.path", args.report_file)
    if sub_command == "compare":
        cfg.add(config.Scope.applicationOverride, "reporting", "baseline.timestamp", args.baseline)
        cfg.add(config.Scope.applicationOverride, "reporting", "contender.timestamp", args.contender)

    ################################
    # new section name: driver
    ################################
    cfg.add(config.Scope.applicationOverride, "benchmarks", "cluster.health", args.cluster_health)
    # Also needed by mechanic (-> telemetry) - duplicate by module?
    cfg.add(config.Scope.applicationOverride, "client", "hosts", convert_hosts(csv_to_list(args.target_hosts)))
    cfg.add(config.Scope.applicationOverride, "client", "options", kv_to_map(csv_to_list(args.client_options)))

    # split by component?
    if sub_command == "list":
        cfg.add(config.Scope.applicationOverride, "system", "list.config.option", args.configuration)
        cfg.add(config.Scope.applicationOverride, "system", "list.races.max_results", args.limit)

    configure_logging(cfg)
    logger.info("Rally version [%s]" % version())
    logger.info("Command line arguments: %s" % args)
    # Configure networking
    net.init()
    if not args.offline:
        if not net.has_internet_connection():
            console.warn("No Internet connection detected. Automatic download of track data sets etc. is disabled.",
                         logger=logger)
            cfg.add(config.Scope.applicationOverride, "system", "offline.mode", True)
        else:
            logger.info("Detected a working Internet connection.")

    # Kill any lingering Rally processes before attempting to continue - the actor system needs to be a singleton on this machine
    # noinspection PyBroadException
    try:
        process.kill_running_rally_instances()
    except BaseException:
        logger.exception("Could not terminate potentially running Rally instances correctly. Attempting to go on anyway.")

    success = dispatch_sub_command(cfg, sub_command)

    end = time.time()
    if success:
        console.println("")
        console.info("SUCCESS (took %d seconds)" % (end - start), overline="-", underline="-")
    else:
        console.println("")
        console.info("FAILURE (took %d seconds)" % (end - start), overline="-", underline="-")
        sys.exit(64)


if __name__ == "__main__":
    main()
