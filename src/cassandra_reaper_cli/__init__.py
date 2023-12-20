# SPDX-FileCopyrightText: 2023-present Timur Isanov <tisanov@evolution.com>
#
# SPDX-License-Identifier: MIT
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
import shtab
from cassandra_reaper_api import CassandraReaper

log = logging.getLogger('cassandra-reaper-cli')
log_stream_handler = logging.StreamHandler()
log.addHandler(log_stream_handler)
log.setLevel(logging.INFO)


def main():
    parser = argparse.ArgumentParser(description='Cassandra Reaper CLI')
    parser.add_argument('--url', action='store',
                        default=os.environ.get('REAPER_URL'), help='Reaper URL (default value from env REAPER_URL)')
    parser.add_argument('--username', action='store',
                        default=os.environ.get('REAPER_USER'), help='Reaper username (default value from env REAPER_USER)')
    parser.add_argument('--password', action='store',
                        default=os.environ.get('REAPER_PASSWORD'), help='Reaper password (default value from env REAPER_PASSWORD)')
    parser.add_argument('--disable-ssl-verify',
                        action='store_true', help='Disable SSL verification')

    subparsers = parser.add_subparsers(help='Supported commands')

    parser_schedule_list = subparsers.add_parser(
        'schedule-list', help='Repair Schedule list')
    parser_schedule_list.add_argument(
        'cluster', action='store', help='Cluster name', nargs='?', default='ALL')
    parser_schedule_list.add_argument('--show-id', '-i',
                                      action='store_true', help='Show Repair Schedule IDs')
    parser_schedule_list.add_argument('--json', '-j',
                                      action='store_true', help='Print in json format')
    parser_schedule_list.set_defaults(func=schedule_list)

    parser_schedule_info = subparsers.add_parser(
        'schedule-info', help='Information about Schedule by ID')
    parser_schedule_info.add_argument('id', action='store', help='Schedule ID')
    parser_schedule_info.set_defaults(func=schedule_info)

    parser_schedule_disable = subparsers.add_parser(
        'schedule-disable', help='Disable Repair Schedule by ID')
    parser_schedule_disable.add_argument(
        'id', action='store', help='Schedule ID')
    parser_schedule_disable.set_defaults(func=schedule_disable)

    parser_schedule_enable = subparsers.add_parser(
        'schedule-enable', help='Enable Repair Schedule by ID')
    parser_schedule_enable.add_argument(
        'id', action='store', help='Schedule ID')
    parser_schedule_enable.set_defaults(func=schedule_enable)

    parser_schedule_start = subparsers.add_parser(
        'schedule-start', help='Start Repair Schedule by ID')
    parser_schedule_start.add_argument(
        'id', action='store', help='Schedule ID')
    parser_schedule_start.set_defaults(func=schedule_start)

    parser_schedule_delete = subparsers.add_parser(
        'schedule-delete', help='Delete Repair Schedule by ID')
    parser_schedule_delete.add_argument(
        'id', action='store', help='Schedule ID')
    parser_schedule_delete.set_defaults(func=schedule_delete)

    parser_repair_list = subparsers.add_parser(
        'repair-list', help='Repairs list')
    parser_repair_list.add_argument('cluster', action='store',
                                    help='Cluster name', nargs='?', default='ALL')
    parser_repair_list.add_argument(
        '--states', choices=['RUNNING', 'PAUSED', 'NOT_STARTED', 'DONE', 'ERROR', 'ABORTED', 'DELETED', 'ALL'], nargs='+', help='Repair states (Default RUNNING, PAUSED and NOT_STARTED)', default=['RUNNING', 'PAUSED', 'NOT_STARTED'])
    parser_repair_list.add_argument('--show-id', '-i',
                                    action='store_true', help='Show repair IDs')
    parser_repair_list.add_argument('--json', '-j',
                                    action='store_true', help='Print in json format')
    parser_repair_list.set_defaults(func=repair_list)

    parser_repair_info = subparsers.add_parser(
        'repair-info', help='Information about Reapair by ID')
    parser_repair_info.add_argument('id', action='store', help='Repair ID')
    parser_repair_info.set_defaults(func=repair_info)

    parser_repair_pause = subparsers.add_parser(
        'repair-pause', help='Pause running repair by ID')
    parser_repair_pause.add_argument('id', action='store', help='Repair ID')
    parser_repair_pause.set_defaults(func=repair_pause)

    parser_repair_abort = subparsers.add_parser(
        'repair-abort', help='Abort repair by ID')
    parser_repair_abort.add_argument('id', action='store', help='Repair ID')
    parser_repair_abort.set_defaults(func=repair_abort)

    parser_repair_delete = subparsers.add_parser(
        'repair-delete', help='Delete repair by ID')
    parser_repair_delete.add_argument('id', action='store', help='Repair ID')
    parser_repair_delete.set_defaults(func=repair_delete)

    parser_repair_resume = subparsers.add_parser(
        'repair-resume', help='Resume paused repair by ID')
    parser_repair_resume.add_argument('id', action='store', help='Repair ID')
    parser_repair_resume.set_defaults(func=repair_resume)

    parser_repair_intensity_change = subparsers.add_parser(
        'repair-intensity-change', help='Change PAUSED repair intensity')
    parser_repair_intensity_change.add_argument(
        'id', action='store', help='Repair ID')
    parser_repair_intensity_change.add_argument(
        'intensity', action='store', type=repair_intensity, help='Intensity (from 0.0 to 1.0)')
    parser_repair_intensity_change.set_defaults(func=repair_intensity_change)

    parser_repair_segments_list = subparsers.add_parser(
        'repair-segment-list', help='List repair segments by ID')
    parser_repair_segments_list.add_argument(
        'id', action='store', help='Repair ID')
    parser_repair_segments_list.add_argument('--show-id', '-i',
                                             action='store_true', help='Show Segment IDs')
    parser_repair_segments_list.add_argument('--show-token-ranges', '-t',
                                             action='store_true', help='Show Segment Token Ranges')
    parser_repair_segments_list.add_argument('--json', '-j',
                                             action='store_true', help='Print in json format')
    parser_repair_segments_list.set_defaults(func=repair_segments_list)

    parser_repair_segment_abort = subparsers.add_parser(
        'repair-segment-abort', help='Aborts a running segment and puts it back in NOT_STARTED state. The segment will be processed again later during the lifetime of the repair run')
    parser_repair_segment_abort.add_argument(
        'id', action='store', help='Repair ID')
    parser_repair_segment_abort.add_argument(
        'segment_id', action='store', help='Segment ID')
    parser_repair_segment_abort.set_defaults(func=repair_segment_abort)

    parser_cluster_list = subparsers.add_parser(
        'cluster-list', help='Clusters list')
    parser_cluster_list.add_argument('--json', '-j',
                                     action='store_true', help='Print in json format')
    parser_cluster_list.set_defaults(func=cluster_list)

    parser_cluster_table_list = subparsers.add_parser(
        'cluster-table-list', help='Tables list of a Cluster (keyspace)')
    parser_cluster_table_list.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_table_list.add_argument('keyspace', action='store',
                                           nargs='?', help='Keyspace name')
    parser_cluster_table_list.add_argument('--show-id', '-i',
                                           action='store_true', help='Show Repair Schedule IDs')
    parser_cluster_table_list.add_argument('--json', '-j',
                                           action='store_true', help='Print in json format')
    parser_cluster_table_list.set_defaults(func=cluster_tables_list)

    parser_cluster_schedule_list = subparsers.add_parser(
        'cluster-schedule-list', help='Repair Schedules list of a Cluster')
    parser_cluster_schedule_list.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_schedule_list.add_argument('--show-id', '-i',
                                              action='store_true', help='Show Repair Schedule IDs')
    parser_cluster_schedule_list.add_argument('--json', '-j',
                                              action='store_true', help='Print in json format')
    parser_cluster_schedule_list.set_defaults(func=schedule_list)

    parser_cluster_schedule_enable = subparsers.add_parser(
        'cluster-schedule-enable', help='Enable all repair schedules of a cluster')
    parser_cluster_schedule_enable.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_schedule_enable.set_defaults(func=cluster_schedules_enable)

    parser_cluster_schedule_disable = subparsers.add_parser(
        'cluster-schedule-disable', help='Disable all repair schedules of a cluster')
    parser_cluster_schedule_disable.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_schedule_disable.set_defaults(
        func=cluster_schedules_disable)

    parser_cluster_schedule_delete = subparsers.add_parser(
        'cluster-schedule-delete', help='Disable and delete all repair schedules of a cluster')
    parser_cluster_schedule_delete.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_schedule_delete.set_defaults(
        func=cluster_schedules_delete)

    parser_cluster_repair_list = subparsers.add_parser(
        'cluster-repair-list', help='Cluster Repairs list')
    parser_cluster_repair_list.add_argument('cluster', action='store',
                                            help='Cluster name', nargs='?', default='ALL')
    parser_cluster_repair_list.add_argument(
        '--states', choices=['RUNNING', 'PAUSED', 'NOT_STARTED', 'DONE', 'ERROR', 'ABORTED', 'DELETED', 'ALL'], nargs='+', help='Repair states (Default RUNNING, PAUSED and NOT_STARTED)', default=['RUNNING', 'PAUSED', 'NOT_STARTED'])
    parser_cluster_repair_list.add_argument('--show-id', '-i',
                                            action='store_true', help='Show repair IDs')
    parser_cluster_repair_list.add_argument('--json', '-j',
                                            action='store_true', help='Print in json format')
    parser_cluster_repair_list.set_defaults(func=repair_list)

    parser_cluster_repair_pause = subparsers.add_parser(
        'cluster-repair-pause', help='Pause all running repairs of a cluster')
    parser_cluster_repair_pause.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_repair_pause.set_defaults(func=cluster_repairs_pause)

    parser_cluster_repair_resume = subparsers.add_parser(
        'cluster-repair-resume', help='Resume all paused repairs of a cluster')
    parser_cluster_repair_resume.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_repair_resume.set_defaults(func=cluster_repairs_resume)

    parser_cluster_repair_abort = subparsers.add_parser(
        'cluster-repair-abort', help='Abort all running and paused repairs of a cluster')
    parser_cluster_repair_abort.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_repair_abort.set_defaults(func=cluster_repairs_abort)

    parser_cluster_repair_delete = subparsers.add_parser(
        'cluster-repair-delete', help='Delete all repairs of a cluster')
    parser_cluster_repair_delete.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_repair_delete.set_defaults(func=cluster_repairs_delete)

    parser_cluster_enable = subparsers.add_parser(
        'cluster-enable', help='Enable all repair schedules and resume all paused repairs of a cluster')
    parser_cluster_enable.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_enable.set_defaults(func=cluster_enable)

    parser_cluster_disable = subparsers.add_parser(
        'cluster-disable', help='Disable all repair schedules and pause all running repairs of a cluster')
    parser_cluster_disable.add_argument(
        'cluster', action='store', help='Cluster name')
    parser_cluster_disable.set_defaults(func=cluster_disable)

    parser_print_completion = subparsers.add_parser(
        'completion-print', help='Print completion for shell')
    parser_print_completion.add_argument(
        'shell', choices=('bash', 'zsh'), help='For which shell to print completion')

    args = parser.parse_args()
    if hasattr(args, 'shell'):
        print(shtab.complete(parser, shell=args.shell))
        exit(0)
    if not all((args.url, args.username, args.password)) or not hasattr(args, 'func'):
        exit(parser.print_help())
    args.func(args)


def repair_intensity(arg):
    """ Type function for argparse - a float within some predefined bounds"""
    try:
        f = float(arg)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "must be a floating point number")
    if not (0.0 < f <= 1.0):
        raise argparse.ArgumentTypeError("must be < 0.0 and >= 1.0")
    return f


def positive_int(arg):
    iarg = int(arg)
    if iarg >= 0:
        return arg
    else:
        raise argparse.ArgumentTypeError(
            f"{arg} is an invalid positive int value")


def cluster_list(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    clusters = sorted(r.get_clusters())
    if args.json:
        print(json.dumps(clusters, indent=4))
    else:
        for cluster in clusters:
            print(cluster)


def cluster_enable(args):
    log.info(f"Enabling {args.cluster} cluster")
    cluster_schedules_enable(args)
    cluster_repairs_resume(args)


def cluster_disable(args):
    log.info(f"Disabling {args.cluster} cluster")
    cluster_schedules_disable(args)
    cluster_repairs_pause(args)


def repair_list(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    cluster = '' if args.cluster == 'ALL' else args.cluster
    states = [] if 'ALL' in args.states else args.states
    repairs = sorted(r.get_repairs(cluster, states),
                     key=lambda i: i['creation_time'])
    if args.json:
        print(json.dumps(repairs, indent=4))
    else:
        header = f"{'CREATION_TIME':22}{'ETA':22}"
        header = f"{header}{'KEYSPACE':40}" if cluster else f"{header}{'CLUSTER':30}{'KEYSPACE':40}"
        header = f"{header}{'ID':40}" if args.show_id else header
        header = f"{header}{'STATE':10}{'REPAIRED':10}{'LAST_EVENT'}"
        print(header)
        for repair in repairs:
            eta = repair['end_time'] if repair['end_time'] else repair[
                'estimated_time_of_arrival'] if repair['estimated_time_of_arrival'] else 'TBD'
            msg = f"{repair['creation_time']:22}{eta:22}"
            msg = f"{msg}{repair['keyspace_name']:40}" if cluster else f"{msg}{repair['cluster_name']:30}{repair['keyspace_name']:40}"
            msg = f"{msg}{repair['id']:40}" if args.show_id else msg
            repaired = f"{repair['segments_repaired']}/{repair['total_segments']}"
            msg = f"{msg}{repair['state']:10}{repaired:10}{repair['last_event']}"
            print(msg)


def cluster_tables_list(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    tables = r.get_cluster_tables(args.cluster)
    if args.json:
        if args.keyspace:
            print(json.dumps(tables.get(args.keyspace)))
        else:
            print(json.dumps(tables, indent=4))
    else:
        if args.keyspace:
            for t in tables.get(args.keyspace):
                print(t)
        else:
            for k in tables:
                print(f"{k}:")
                for t in tables[k]:
                    print(f"  {t}")


def schedule_list(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    cluster = '' if args.cluster == 'ALL' else args.cluster
    schedules = sorted(r.get_schedules(cluster),
                       key=lambda i: i['next_activation'])
    if args.json:
        print(json.dumps(schedules, indent=4))
    else:
        header = f"{'KEYSPACE':40}" if cluster else f"{'CLUSTER':30}{'KEYSPACE':40}"
        header = f"{header}{'ID':40}" if args.show_id else header
        header = f"{header}{'STATE':10}{'NEXT_ACTIVATION'}"
        print(header)
        for s in schedules:
            msg = f"{s['keyspace_name']:40}" if cluster else f"{s['cluster_name']:30}{s['keyspace_name']:40}"
            msg = f"{msg}{s['id']:40}" if args.show_id else msg
            msg = f"{msg}{s['state']:10}{s['next_activation']}"
            print(msg)


def schedule_start(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    s = r.get_schedule(args.id)
    log.info(
        f"Starting {s['cluster_name']} cluster {s['keyspace_name']} keyspace repair schedule...")
    r.start_schedule(args.id)


def schedule_disable(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    s = r.get_schedule(args.id)
    log.info(
        f"Disabling {s['cluster_name']} cluster {s['keyspace_name']} keyspace repair schedule...")
    r.disable_schedule(args.id)


def schedule_enable(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    s = r.get_schedule(args.id)
    log.info(
        f"Enabling {s['cluster_name']} cluster {s['keyspace_name']} keyspace repair schedule...")
    r.enable_schedule(args.id)


def schedule_delete(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    s = r.get_schedule(args.id)
    if s['state'] == 'PAUSED':
        log.info(
            f"Deleting {s['cluster_name']} cluster {s['keyspace_name']} keyspace repair schedule...")
        r.delete_schedule(args.id)
    else:
        log.error(
            f"Can't delete {s['cluster_name']} cluster {s['keyspace_name']} keyspace repair schedule because it's ACTIVE. Must be disabled first")
        exit(1)


def repair_pause(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repair = r.get_repair(args.id)
    log.info(
        f"Pausing {repair['cluster_name']} cluster {repair['keyspace_name']} keyspace repair...")
    r.pause_repair(args.id)


def repair_intensity_change(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repair = r.get_repair(args.id)
    if repair['state'] == 'PAUSED':
        log.info(
            f"Changing {repair['cluster_name']} cluster {repair['keyspace_name']} keyspace repair intensity from {repair['intensity']} to {args.intensity}")
        r.change_repair_intensity(args.id, args.intensity)
    else:
        log.error(
            f"Can't change {repair['cluster_name']} cluster {repair['keyspace_name']} keyspace repair intensity because it's not in PAUSED state")
        exit(1)


def repair_abort(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repair = r.get_repair(args.id)
    log.info(
        f"Aborting {repair['cluster_name']} cluster {repair['keyspace_name']} keyspace repair...")
    r.abort_repair(args.id)


def repair_delete(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repair = r.get_repair(args.id)
    log.info(
        f"Deleting {repair['cluster_name']} cluster {repair['keyspace_name']} keyspace repair {repair['id']}...")
    r.delete_repair(args.id)


def repair_resume(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repair = r.get_repair(args.id)
    log.info(
        f"Resuming {repair['cluster_name']} cluster {repair['keyspace_name']} keyspace repair...")
    r.resume_repair(args.id)


def repair_segments_list(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    segments = sorted(r.get_repair_segments(args.id),
                      key=lambda i: i['startTime'] if i['startTime'] else sys.maxsize)
    if args.json:
        print(json.dumps(segments, indent=4))
    else:
        header = f"{'START_TOKEN':>22}{'END_TOKEN':>22}{'ID':>40}" if args.show_id else f"{'START_TOKEN':>22}{'END_TOKEN':>22}"
        header = f"{header}{'FAIL_COUNT':^15}{'STATE':<15}{'REPLICAS':<92}{'SEGMENT_DURATION'}"
        print(header)
        for s in segments:
            start_token = s['tokenRange']['baseRange']['start']
            end_token = s['tokenRange']['baseRange']['end']
            msg = f"{start_token:>22}{end_token:>22}\t{s['id']:<40}" if args.show_id else f"{start_token:>22}{end_token:>22}"
            msg = f"{msg}{s['failCount']:^15}{s['state']:<15}{s['replicas']}"
            if s['startTime']:
                end_time = s['endTime'] if s['endTime'] else datetime.now(
                ).timestamp() * 1000
                segment_duration = timedelta(
                    milliseconds=(end_time-s['startTime']))
                msg = f"{msg}\t{segment_duration}"
            print(msg)
            if args.show_token_ranges:
                print(f"{'RANGE_START':>30}{'RANGE_END':>22}")
                for range in sorted(s['tokenRange']['tokenRanges'], key=lambda i: i['start']):
                    print(f"{range['start']:30}{range['end']:22}")


def repair_info(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    info = r.get_repair(args.id)
    print(json.dumps(info, indent=4))


def schedule_info(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    info = r.get_schedule(args.id)
    print(json.dumps(info, indent=4))


def cluster_schedules_enable(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    schedules = r.get_cluster_schedules(args.cluster)
    for schedule in schedules:
        id = schedule['id']
        keyspace = schedule['keyspace_name']
        log.info(f"Enabling {keyspace} keyspace repair schedule...")
        r.enable_schedule(id)


def cluster_schedules_disable(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    schedules = r.get_cluster_schedules(args.cluster)
    for schedule in schedules:
        id = schedule['id']
        keyspace = schedule['keyspace_name']
        log.info(f"Disabling {keyspace} keyspace repair schedule...")
        r.disable_schedule(id)


def cluster_schedules_delete(args):
    cluster_schedules_disable(args)
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    schedules = r.get_cluster_schedules(args.cluster)
    for schedule in schedules:
        id = schedule['id']
        keyspace = schedule['keyspace_name']
        log.info(f"Deleting {keyspace} keyspace repair schedule...")
        r.delete_schedule(id)


def cluster_repairs_resume(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repairs = r.get_repairs(args.cluster, ['PAUSED'])
    if len(repairs) == 0:
        log.info(f"No paused repairs of {args.cluster} cluster")
    else:
        for repair in repairs:
            id = repair['id']
            keyspace = repair['keyspace_name']
            log.info(f"Resuming {keyspace} keyspace repair...")
            r.resume_repair(id)


def cluster_repairs_pause(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repairs = r.get_repairs(args.cluster, ['RUNNING'])
    if len(repairs) == 0:
        log.info(f"No running repairs of {args.cluster} cluster")
    else:
        for repair in repairs:
            id = repair['id']
            keyspace = repair['keyspace_name']
            log.info(f"Pausing {keyspace} keyspace repair...")
            r.pause_repair(id)


def cluster_repairs_abort(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repairs = r.get_repairs(args.cluster, ['RUNNING', 'PAUSED'])
    if len(repairs) == 0:
        log.info(f"No running or paused repairs of {args.cluster} cluster")
    else:
        for repair in repairs:
            id = repair['id']
            keyspace = repair['keyspace_name']
            log.info(f"Aborting {keyspace} keyspace repair...")
            r.abort_repair(id)


def cluster_repairs_delete(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    repairs = r.get_repairs(args.cluster)
    if len(repairs) == 0:
        log.info(f"{args.cluster} cluster has no repairs")
    else:
        for repair in repairs:
            id = repair['id']
            keyspace = repair['keyspace_name']
            log.info(f"Deleting {keyspace} keyspace repair...")
            r.delete_repair(id)


def repair_segment_abort(args):
    r = CassandraReaper(args.url, args.username, args.password,
                        not args.disable_ssl_verify)
    r.abort_repair_segment(args.id, args.segment_id)


if __name__ == "__main__":
    main()
