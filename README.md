# mysql-backup

weekly mysql data(full+incr)/logs backup using xtrabackup

## 准备

```shell
# 打包（也可以直接python3 mysql_backup.py，但打包成可执行文件，就可以在其它服务器上独立运行而不依赖python）
pip3 install pyinstaller
# 编译
sh mysql_backup_build.sh
# 授权
chmod +x mysql_backup
# 安装
cp mysql_backup /usr/bin/mysql_backup
```

## 示例

```shell
$ yum install xtrabackup zstd # 安装依赖
$ mysql_backup

input: {'bak_mode': 0, 'bak_dir': None, 'keep': 2, 'weekday': 6, 'my_cnf': '/etc/my.cnf', 'log_bin': None}
error: --bak-dir=None
usage: mysql_backup [-h] [--bak-mode BAK_MODE] [--bak-dir BAK_DIR] [--keep KEEP] [--weekday WEEKDAY] [--my-cnf MY_CNF] [--log-bin LOG_BIN]

MySQL周度全量、增量、日志备份，使用xtrabackup+zstd最佳组合，Version=v1.1.2
数据备份：mysql_backup --bak-mode=0 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf
日志备份：mysql_backup --bak-mode=1 --bak-dir=/backup --log-bin=/var/lib/mysql/mysql-bin
混合备份：mysql_backup --bak-mode=2 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf --log-bin=/var/lib/mysql/mysql-bin

options:
  -h, --help           show this help message and exit

基础选项:
  --bak-mode BAK_MODE  0=数据，1=日志，2=数据+日志 (default: 0)
  --bak-dir BAK_DIR    备份文件目录 (default: None)
  --keep KEEP          保留几周(>=1) (default: 2)

数据备份选项:
  --weekday WEEKDAY    周几全量备份(1~7) (default: 6)
  --my-cnf MY_CNF      配置文件路径 (default: /etc/my.cnf)

日志备份选项:
  --log-bin LOG_BIN    日志读取路径，show variables like 'log_bin_basename' (default: None)
```

## 定时

```crontab
0 0 20 * * mysql_backup --bak-mode=1 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf
0 0  * * * mysql_backup --bak-mode=2 --bak-dir=/backup --log-bin=/var/lib/mysql/mysql-bin
```

## 效果

- 数据`3G -> 681M`
- 日志`1G -> 80M`

```shell
# cd $bak-dir
$ ll -hR

total 0
drw-r--r--. 2 root root 173 Jul 17 00:00 data
drw-r--r--. 2 root root 240 Jul 17 00:00 logs

./data:
total 708M
-rw-r--r--. 1 root root 681M Jul 15 00:00 20240715_FULL_0_345489064825.xb.zst
-rw-r--r--. 1 root root  13M Jul 16 00:00 20240716_INCR_345489064825_346086160276.xb.zst
-rw-r--r--. 1 root root  15M Jul 17 00:00 20240717_INCR_345489064825_347259222639.xb.zst

./logs:
total 288M
-rw-r-----. 1 root root 80M Jul 15 00:00 20240716_LOGS_mysql-bin.000302.zst
-rw-r-----. 1 root root 80M Jul 16 00:00 20240717_LOGS_mysql-bin.000303.zst
-rw-r-----. 1 root root 36M Jul 17 00:00 20240717_LOGS_mysql-bin.000304.zst

# 增量备份依赖上一个全量备份，增量文件的from_lsn即是全量文件的to_lsn，通过lsn检索历史备份
$ ll -h data | grep 345489064825
-rw-r--r--. 1 root root 681M Jul 15 00:00 20240715_FULL_0_345489064825.xb.zst
-rw-r--r--. 1 root root  13M Jul 16 00:00 20240716_INCR_345489064825_346086160276.xb.zst
-rw-r--r--. 1 root root  15M Jul 17 00:00 20240717_INCR_345489064825_347259222639.xb.zst
```

## 其它

### 方案

- v1，弃用，表级文件备份存储（随机写速度慢），单文件压缩，且压缩率较低（依赖qpress）
    - 全量：`--backup --parallel --compress --compress-threads --target-dir=full_backup`
    - 增量：`--backup --parallel --compress --compress-threads --target-dir=incr_backup --incremental-basedir=full_backup`
- v2，当前，流式打包压缩存储（顺序写速度快），备份效率（大小、时间）相比提升30%左右
    - 全量：`--backup --parallel --stream=xbstream | zstd -o full_backup_lsn.xb.zst`
    - 增量：`--backup --parallel --stream=xbstream --incremental-lsn=full_backup_lsn | zstd -o incr_backup_lsn.xb.zst`

### 备注

```
文件命名：
  data: {date}_{type}_{from_lsn}_{to_lsn}.xb.zst
  logs: {date}_{type}_{name}.zst

最佳实践：
  --bak-mode：小实例，数据日志统一备份；大实例，数据每天一次、日志每时一次，独立备份
  --weekday：存在多实例时，受存储（例如：NFS）磁盘IO限制，通过weekday指定全量哪一天备份
  --keep：默认保留2周（14天），按天滚动删除历史备份文件，视存储容量、归档策略而定

备份还原：
  mkdir backup && zstd -d -c backup.xb.zst | xbstream -v -x -C ./backup
```
