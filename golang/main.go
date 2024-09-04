package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/hellflame/argparse"

	"mysql_backup/backup"
)

var (
	parser *argparse.Parser
	option *backup.Backup
)

func init() {
	parser = argparse.NewParser("mysql_backup", strings.Join([]string{
		"MySQL周度全量、增量、日志备份，使用xtrabackup+zstd最佳组合，Version=v2.0.0",
		"数据备份：mysql_backup --bak-mode=0 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf",
		"日志备份：mysql_backup --bak-mode=1 --bak-dir=/backup --log-bin=/var/lib/mysql/mysql-bin",
		"混合备份：mysql_backup --bak-mode=2 --bak-dir=/backup --weekday=7 --my-cnf=/etc/my.cnf --log-bin=/var/lib/mysql/mysql-bin",
	}, "\n"), &argparse.ParserConfig{DisableDefaultShowHelp: true})
	bakMode := parser.Int("", "bak-mode", &argparse.Option{Group: "基础备份选项", Help: "0=数据，1=日志，2=数据+日志", Default: "0"})
	bakDir := parser.String("", "bak-dir", &argparse.Option{Group: "基础备份选项", Help: "备份文件目录", Default: ""})
	keep := parser.Int("", "keep", &argparse.Option{Group: "基础备份选项", Help: "保留几周(>=1)", Default: "2"})
	dryRun := parser.Flag("", "dry-run", &argparse.Option{Group: "基础备份选项", Help: "打印命令而不实际执行"})

	weekday := parser.Int("", "weekday", &argparse.Option{Group: "数据备份选项", Help: "周几全量备份(1~7)", Default: "6"})
	myCnf := parser.String("", "my-cnf", &argparse.Option{Group: "数据备份选项", Help: "配置文件路径", Default: "/etc/my.cnf"})
	executor := parser.String("", "executor", &argparse.Option{Group: "数据备份选项", Help: "可执行文件：mariabackup, /usr/bin/xtrabackup", Default: "xtrabackup"})

	logBin := parser.String("", "log-bin", &argparse.Option{Group: "日志备份选项", Help: "日志文件路径 show variables like 'log_bin_basename'", Default: ""})
	err := parser.Parse(func() (args []string) {
		for _, arg := range os.Args[1:] {
			args = append(args, strings.Split(arg, "=")...)
		}
		return
	}())
	if err != nil {
		if err != argparse.BreakAfterHelpError {
			fmt.Println(err.Error())
			parser.PrintHelp()
		}
		os.Exit(1)
	}
	option = &backup.Backup{
		BakMode:  *bakMode,
		BakDir:   *bakDir,
		Keep:     *keep,
		Weekday:  *weekday,
		MyCnf:    *myCnf,
		Executor: *executor,
		LogBin:   *logBin,
		DryRun:   *dryRun,
	}
}

func main() {
	if !(option.BakMode >= 0 && option.BakMode <= 2) {
		fmt.Printf("error: --bak-mode=%d\n", option.BakMode)
		parser.PrintHelp()
		os.Exit(1)
	}
	if _, err := os.Stat(option.BakDir); err != nil {
		fmt.Printf("error: --bak-dir=%s\n", option.BakDir)
		parser.PrintHelp()
		os.Exit(1)
	}
	if option.Keep < 1 {
		fmt.Printf("error: --keep=%d\n", option.Keep)
		parser.PrintHelp()
		os.Exit(1)
	}
	if err := exec.Command("which", "zstd").Run(); err != nil {
		fmt.Println("error: command zstd is missing")
		parser.PrintHelp()
		os.Exit(1)
	}
	if option.BakMode == 0 || option.BakMode == 2 {
		if !(option.Weekday >= 0 && option.Weekday <= 7) {
			fmt.Printf("error: --weekday=%d\n", option.Weekday)
			parser.PrintHelp()
			os.Exit(1)
		}
		if _, err := os.Stat(option.MyCnf); err != nil {
			fmt.Printf("error: --my-cnf=%s\n", option.MyCnf)
			parser.PrintHelp()
			os.Exit(1)
		}
		if err := exec.Command("which", option.Executor).Run(); err != nil {
			if _, err := os.Stat(option.Executor); err != nil {
				fmt.Printf("error: command %s is missing\n", option.Executor)
				parser.PrintHelp()
				os.Exit(1)
			}
		}
		backup := backup.NewDataBackup(option)
		backup.Run()
	}
	if option.BakMode == 1 || option.BakMode == 2 {
		if option.LogBin == "" {
			fmt.Printf("error: --log-bin=%s\n", option.LogBin)
			parser.PrintHelp()
			os.Exit(1)
		}
		backup := backup.NewLogsBackup(option)
		backup.Run()
	}
}
