import os
import re
import sys
import datetime
import subprocess
from collections import namedtuple
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, RawTextHelpFormatter

BackupType = namedtuple('BackupType', ['full', 'incr', 'logs'])(full='FULL', incr='INCR', logs='LOGS')
TODAY = datetime.date.today()
FORMAT = '%Y%m%d'


class Backup:

    def __init__(self, bak_dir: str, keep: int):
        self.bak_dir = bak_dir  # 备份目录
        self.keep = keep  # 保留几周
        self.create_dir()
        self.last_name = self._get_last_name()

    def run(self):
        self.remove_old()
        self.backup_cmd()

    def create_dir(self):
        # 创建目录
        os.makedirs(self.base_dir, mode=0o644, exist_ok=True)

    def remove_old(self):
        # 删除历史
        date = (TODAY - datetime.timedelta(days=self.keep * 7)).strftime(FORMAT)
        for file in self.history:
            if file.split('_')[0] > date:
                return
            os.remove(os.path.join(self.base_dir, file))

    def backup_cmd(self):
        # 执行命令
        raise

    @property
    def base_dir(self):
        # 备份目录
        raise

    @property
    def name_tpl(self):
        # 命名模板
        raise

    @property
    def backup_type(self):
        # 备份类型
        raise

    @property
    def file_type(self):
        # 文件类型
        raise

    def _get_last_name(self):
        # 上次备份
        raise

    def filter(self, name: str):
        # 备份过滤
        raise

    @property
    def history(self):
        # 历史备份
        return sorted(filter(self.filter, os.listdir(self.base_dir)))


class DataBackup(Backup):
    def __init__(self, bak_dir: str, keep: int, weekday: int, my_cnf: str, executor: str):
        super().__init__(bak_dir, keep)
        self.weekday = weekday
        self.my_cnf = my_cnf
        self.executor = executor

    def backup_cmd(self):
        tmp_name = os.path.join(self.base_dir, 'tmp_backup' + self.file_type)
        if self.backup_type == BackupType.full:
            f, t = '0', ''
            incr = ''
        else:
            f, t = self.last_name.split('_')[3], ''
            incr = f"--incremental-lsn={f}"
        cmd = f'{self.executor} --defaults-file={self.my_cnf} --backup --parallel=4 --stream=xbstream --target-dir=/tmp {incr}| zstd -fkT4 -o {tmp_name}'
        print(datetime.datetime.now(), 'execute', cmd, flush=True)
        shell = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
        while True:
            output = shell.stdout.readline().rstrip()
            print(output)
            if 'The latest check point (for incremental)' in output:
                t = output.split("'")[1]
            if shell.poll() is not None:
                break
        if t == '':
            os.remove(tmp_name)
            print(datetime.datetime.now(), 'FAILURE', tmp_name, flush=True)
            sys.exit(1)
        bak_name = os.path.join(self.base_dir, self.name_tpl.format(date=TODAY.strftime(FORMAT), type=self.backup_type, f=f, t=t))
        os.rename(tmp_name, bak_name)
        print(datetime.datetime.now(), 'SUCCESS', bak_name, flush=True)

    @property
    def base_dir(self):
        return os.path.join(self.bak_dir, 'data')

    @property
    def name_tpl(self):
        return '{date}_{type}_{f}_{t}' + self.file_type

    @property
    def backup_type(self):
        weekday = TODAY.isoweekday()
        days = weekday - self.weekday if weekday >= self.weekday else weekday - self.weekday + 7
        full_date = (TODAY - datetime.timedelta(days=days)).strftime(FORMAT)
        if self.last_name.split('_')[0] < full_date:
            return BackupType.full
        return BackupType.incr

    @property
    def file_type(self):
        return '.xb.zst'

    def _get_last_name(self):
        for name in reversed(self.history):
            if BackupType.full in name:
                return name.replace(self.file_type, '')
        return ''

    def filter(self, name: str) -> bool:
        return name.endswith(self.file_type) and len(name.split('_')) == 4


class LogsBackup(Backup):
    def __init__(self, bak_dir: str, keep: int, log_bin: str):
        super().__init__(bak_dir, keep)
        self.log_bin = log_bin

    def backup_cmd(self):
        log_dir, basename = os.path.dirname(self.log_bin), os.path.basename(self.log_bin)
        log_files = list(sorted(filter(lambda x: re.compile(basename + r'\.\d{6}').match(x), os.listdir(log_dir))))
        last_max = self.last_name.split('_')[-1]
        if last_max == '':
            log_files = log_files[-1:]
        else:
            log_files = list(filter(lambda x: x >= last_max, log_files))
        print(datetime.datetime.now(), f'find {len(log_files)} binlogs', flush=True)
        for file in log_files:
            bak_name = os.path.join(self.base_dir, self.name_tpl.format(date=TODAY.strftime(FORMAT), type=self.backup_type, name=file))
            cmd = f'zstd -fkT4 {os.path.join(log_dir, file)} -o {bak_name}'
            os.system(cmd)
            print(datetime.datetime.now(), 'SUCCESS', cmd, flush=True)
            if file == last_max:
                os.remove(os.path.join(self.base_dir, self.last_name + self.file_type))

    @property
    def base_dir(self):
        return os.path.join(self.bak_dir, 'logs')

    @property
    def name_tpl(self):
        return '{date}_{type}_{name}' + self.file_type

    @property
    def backup_type(self):
        return BackupType.logs

    @property
    def file_type(self):
        return '.zst'

    def _get_last_name(self):
        if len(self.history):
            return self.history[-1].replace(self.file_type, '')
        return ''

    def filter(self, name: str) -> bool:
        return name.endswith(self.file_type) and len(name.split('_')) == 3


def parse_args(argv):
    parser = ArgumentParser(
        description='\n'.join([
            'MySQL周度全量、增量、日志备份，使用xtrabackup+zstd最佳组合，Version=v1.1.4',
            '数据备份：mysql_backup --bak-mode=0 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf',
            '日志备份：mysql_backup --bak-mode=1 --bak-dir=/backup --log-bin=/var/lib/mysql/mysql-bin',
            '混合备份：mysql_backup --bak-mode=2 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf --log-bin=/var/lib/mysql/mysql-bin',
        ]),
        formatter_class=type('CustomFormatter', (ArgumentDefaultsHelpFormatter, RawTextHelpFormatter), {}),
    )
    required = parser.add_argument_group(title='基础选项')
    required.add_argument('--bak-mode', dest='bak_mode', type=int, default=0, help='0=数据，1=日志，2=数据+日志')
    required.add_argument('--bak-dir', dest='bak_dir', type=str, help='备份文件目录')
    required.add_argument('--keep', dest='keep', type=int, default=2, help='保留几周(>=1)')
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
    print('input:', args.__dict__)
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
    if args.bak_mode in (0, 2):
        if not 1 <= args.weekday <= 7:
            print(f'error: --weekday={args.weekday}')
            parser.print_help()
            sys.exit(1)
        if not os.path.exists(args.my_cnf):
            print(f'error: --my-cnf={args.my_cnf}')
            parser.print_help()
            sys.exit(1)
        backup = DataBackup(args.bak_dir, args.keep, args.weekday, args.my_cnf, args.executor)
        backup.run()
    if args.bak_mode in (1, 2):
        if args.log_bin is None:
            print(f'error: --log-bin={args.log_bin}')
            parser.print_help()
            sys.exit(1)
        backup = LogsBackup(args.bak_dir, args.keep, args.log_bin)
        backup.run()


if __name__ == '__main__':
    run(sys.argv[1:])
