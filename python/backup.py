import os
import re
import sys
import datetime
import subprocess
from collections import namedtuple

BackupType = namedtuple('BackupType', ['full', 'incr', 'logs'])(full='FULL', incr='INCR', logs='LOGS')
TODAY = datetime.date.today()
FORMAT = '%Y%m%d'


class Backup:

    def __init__(self, bak_dir: str, keep: int, dry_run: bool):
        self.bak_dir = bak_dir  # 备份目录
        self.keep = keep  # 保留几周
        self.dry_run = dry_run

    def run(self):
        self.create_dir()
        self.remove_old()
        self.backup_cmd()

    def create_dir(self):
        # 创建目录
        os.makedirs(self.base_dir, mode=0o644, exist_ok=True)

    def remove_old(self):
        # 删除历史
        if self.dry_run:
            return
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
    def file_type(self):
        # 文件类型
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
    def last_name(self):
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
    def __init__(self, bak_dir: str, keep: int, dry_run: bool, weekday: int, my_cnf: str, executor: str):
        super().__init__(bak_dir, keep, dry_run)
        self.weekday = weekday
        self.my_cnf = my_cnf
        self.executor = executor

    def backup_cmd(self):
        tmp_name = os.path.join(self.base_dir, 'tmp_backup' + self.file_type)
        f, t = '0', ''
        if self.backup_type == BackupType.incr:
            f = self.last_name.split('_')[3]
        cmd = f'{self.executor} --defaults-file={self.my_cnf} --backup --parallel=4 --stream=xbstream --target-dir=/tmp --incremental-lsn={f} | zstd -fkT4 -o {tmp_name}'
        print(datetime.datetime.now(), 'execute', cmd, flush=True)
        if self.dry_run:
            return
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
    def file_type(self):
        return '.xb.zst'

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
    def last_name(self):
        for name in reversed(self.history):
            if BackupType.full in name:
                return name.replace(self.file_type, '')
        return ''

    def filter(self, name: str) -> bool:
        return name.endswith(self.file_type) and len(name.split('_')) == 4


class LogsBackup(Backup):
    def __init__(self, bak_dir: str, keep: int, dry_run: bool, log_bin: str):
        super().__init__(bak_dir, keep, dry_run)
        self.log_bin = log_bin

    def backup_cmd(self):
        log_dir, basename = os.path.split(self.log_bin)
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
            print(datetime.datetime.now(), 'SUCCESS', cmd, flush=True)
            if self.dry_run:
                return
            os.system(cmd)
            if file == last_max:
                os.remove(os.path.join(self.base_dir, self.last_name + self.file_type))

    @property
    def base_dir(self):
        return os.path.join(self.bak_dir, 'logs')

    @property
    def file_type(self):
        return '.zst'

    @property
    def name_tpl(self):
        return '{date}_{type}_{name}' + self.file_type

    @property
    def backup_type(self):
        return BackupType.logs

    @property
    def last_name(self):
        if len(self.history):
            return self.history[-1].replace(self.file_type, '')
        return ''

    def filter(self, name: str) -> bool:
        return name.endswith(self.file_type) and len(name.split('_')) == 3
