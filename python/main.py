import os
import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, RawTextHelpFormatter

from backup import DataBackup, LogsBackup


def parse_args(argv):
    parser = ArgumentParser(
        description='\n'.join([
            'MySQL周度全量、增量、日志备份，使用xtrabackup+zstd最佳组合，Version=v2.0.0',
            '数据备份：mysql_backup --bak-mode=0 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf',
            '日志备份：mysql_backup --bak-mode=1 --bak-dir=/backup --log-bin=/var/lib/mysql/mysql-bin',
            '混合备份：mysql_backup --bak-mode=2 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf --log-bin=/var/lib/mysql/mysql-bin',
        ]),
        formatter_class=type('CustomFormatter', (ArgumentDefaultsHelpFormatter, RawTextHelpFormatter), {}),
    )
    base_group = parser.add_argument_group(title='基础备份选项')
    base_group.add_argument('--bak-mode', dest='bak_mode', type=int, default=0, help='0=数据，1=日志，2=数据+日志')
    base_group.add_argument('--bak-dir', dest='bak_dir', type=str, help='备份文件目录')
    base_group.add_argument('--keep', dest='keep', type=int, default=2, help='保留几周(>=1)')
    base_group.add_argument('--dry-run', dest='dry_run', action='store_true', help='打印命令而不实际执行')
    data_group = parser.add_argument_group(title='数据备份选项')
    data_group.add_argument('--weekday', dest='weekday', type=int, default=6, help='周几全量备份(1~7)')
    data_group.add_argument('--my-cnf', dest='my_cnf', type=str, default='/etc/my.cnf', help='配置文件路径')
    data_group.add_argument('--executor', dest='executor', type=str, default='xtrabackup', help='可执行文件：mariabackup, /usr/bin/xtrabackup')
    logs_group = parser.add_argument_group(title='日志备份选项')
    logs_group.add_argument('--log-bin', dest='log_bin', type=str, help="日志读取路径，show variables like 'log_bin_basename'")
    args = parser.parse_args(argv)
    return parser, args


def run(args):
    parser, args = parse_args(args)
    if not 0 <= args.bak_mode <= 2:
        print(f'error: --bak-mode={args.bak_mode}')
        parser.print_help()
        sys.exit(1)
    if args.bak_dir is None or not os.path.exists(args.bak_dir):
        print(f'error: --bak-dir={args.bak_dir}')
        parser.print_help()
        sys.exit(1)
    if args.keep < 1:
        print(f'error: --keep={args.keep}')
        parser.print_help()
        sys.exit(1)
    if os.system('which zstd') != 0:
        print("error: command 'zstd' is missing")
        parser.print_help()
        sys.exit(1)
    if args.bak_mode in (0, 2):
        if not 1 <= args.weekday <= 7:
            print(f'error: --weekday={args.weekday}')
            parser.print_help()
            sys.exit(1)
        if not os.path.exists(args.my_cnf):
            print(f'error: --my-cnf={args.my_cnf}')
            parser.print_help()
            sys.exit(1)
        if not (os.system(f'which {args.executor}') == 0 or os.path.exists(args.executor)):
            print(f"error: command '{args.executor}' is missing")
            parser.print_help()
            sys.exit(1)
        backup = DataBackup(args.bak_dir, args.keep, args.dry_run, args.weekday, args.my_cnf, args.executor)
        backup.run()
    if args.bak_mode in (1, 2):
        if args.log_bin is None:
            print(f'error: --log-bin={args.log_bin}')
            parser.print_help()
            sys.exit(1)
        backup = LogsBackup(args.bak_dir, args.keep, args.dry_run, args.log_bin)
        backup.run()


if __name__ == '__main__':
    run(sys.argv[1:])
