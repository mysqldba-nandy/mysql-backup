import os
import re
import sys
import datetime
import argparse
import subprocess
from collections import namedtuple

BackupType = namedtuple('BackupType', ['full', 'incr', 'logs'])(full='FULL', incr='INCR', logs='LOGS')
TODAY = datetime.date.today()
FORMAT = '%Y%m%d'


class Backup:

    def __init__(self, bak_dir: str, keep: int):
        self.bak_dir = bak_dir  # 备份目录
        self.keep = keep  # 保留几周

    def run(self):
        self.create_dir()
        self.remove_old()
        self.backup_cmd()

    def create_dir(self):
        # 创建目录
        os.makedirs(self.base_dir, mode=0o644, exist_ok=True)

    def remove_old(self):
        # 删除历史
        date = (TODAY - datetime.timedelta(days=self.keep * 7)).strftime(FORMAT)
        for file in self.history:
            if file.split('_')[0] < date:
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

    @property
    def last_name(self):
        # 上次备份
        raise

    def filter(self, name: str):
        # 备份过滤
        raise

    @property
    def history(self):
        # 历史备份
        return list(filter(self.filter, os.listdir(self.base_dir)))


class DataBackup(Backup):
    def __init__(self, bak_dir: str, keep: int, weekday: int, my_cnf: str):
        super().__init__(bak_dir, keep)
        self.my_cnf = my_cnf
        self.weekday = weekday

    def backup_cmd(self):
        tmp_name = os.path.join(self.base_dir, 'tmp_backup' + self.file_type)
        if self.backup_type == BackupType.full:
            f, t = '0', ''
            incr = ''
        else:
            f, t = self.last_name.split('_')[3], ''
            incr = f"--incremental-lsn={f}"
        cmd = f'xtrabackup --defaults-file={self.my_cnf} --backup --parallel=4 --stream=xbstream --target-dir=/tmp {incr}| zstd -fkT4 -o {tmp_name}'
        print(datetime.datetime.now(), 'execute', cmd, flush=True)
        shell = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
        while True:
            output = shell.stdout.readline().rstrip()
            print(output)
            if output.startswith('xtrabackup: The latest check point'):
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

    @property
    def last_name(self):
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
        if self.last_name == '':
            log_files = log_files[-1:]
        else:
            log_files = list(filter(lambda x: x >= self.last_name.split('_')[-1], log_files))
        print(datetime.datetime.now(), f'find {len(log_files)} binlogs', flush=True)
        for file in log_files:
            bak_name = os.path.join(self.base_dir, self.name_tpl.format(date=TODAY.strftime(FORMAT), type=self.backup_type, name=file))
            cmd = f'zstd -fkT4 {os.path.join(log_dir, file)} -o {bak_name}'
            os.system(cmd)
            print(datetime.datetime.now(), 'SUCCESS', cmd, flush=True)

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

    @property
    def last_name(self):
        if len(self.history):
            return self.history[-1].replace(self.file_type, '')
        return ''

    def filter(self, name: str) -> bool:
        return name.endswith(self.file_type) and len(name.split('_')) == 3


def parse_args(argv):
    class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter):
        pass

    parser = argparse.ArgumentParser(
        description='\n'.join([
            'MySQL周度全量、增量、日志备份，使用xtrabackup+zstd最佳组合，Version=v1.0.0',
            '混合备份：mysql_backup --bak-mode=0 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf --log-bin=/var/lib/mysql/mysql-bin',
            '数据备份：mysql_backup --bak-mode=1 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf',
            '日志备份：mysql_backup --bak-mode=2 --bak-dir=/backup --log-bin=/var/lib/mysql/mysql-bin',
        ]),
        formatter_class=CustomFormatter,
    )
    required = parser.add_argument_group(title='基础选项')
    required.add_argument('--bak-mode', dest='bak_mode', type=int, default=0, help='0=数据+日志，1=数据，2=日志')
    required.add_argument('--bak-dir', dest='bak_dir', type=str, help='备份文件目录')
    required.add_argument('--keep', dest='keep', type=int, default=2, help='保留几周(>=1)')
    data_group = parser.add_argument_group(title='数据备份选项')
    data_group.add_argument('--weekday', dest='weekday', type=int, help='周几全量备份(1~7)')
    data_group.add_argument('--my-cnf', dest='my_cnf', type=str, default='/etc/my.cnf', help='配置文件路径')
    logs_group = parser.add_argument_group(title='日志备份选项')
    logs_group.add_argument('--log-bin', dest='log_bin', type=str, help="日志读取路径，show variables like 'log_bin_basename'")
    r = parser.parse_args(argv)
    print('input:', r.__dict__)
    if not 0 <= r.bak_mode <= 2:
        print(f'error: --bak-mode={r.bak_mode}')
        parser.print_help()
        sys.exit(1)
    if r.bak_dir is None or not os.path.exists(r.bak_dir):
        print(f'error: --bak-dir={r.bak_dir}')
        parser.print_help()
        sys.exit(1)
    if r.keep < 1:
        print(f'error: --keep={r.keep}')
        parser.print_help()
        sys.exit(1)
    if r.bak_mode in (0, 1):
        if r.weekday is None or not 1 <= r.weekday <= 7:
            print(f'error: --weekday={r.weekday}')
            parser.print_help()
            sys.exit(1)
        if not os.path.exists(r.my_cnf):
            print(f'error: --my-cnf={r.my_cnf}')
            parser.print_help()
            sys.exit(1)
    if r.bak_mode in (0, 2):
        if r.log_bin is None:
            print(f'error: --log-bin={r.log_bin}')
            parser.print_help()
            sys.exit(1)
    return r


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])
    if args.bak_mode in (0, 1):
        backup = DataBackup(args.bak_dir, args.keep, args.weekday, args.my_cnf)
        backup.run()
    if args.bak_mode in (0, 2):
        backup = LogsBackup(args.bak_dir, args.keep, args.log_bin)
        backup.run()
