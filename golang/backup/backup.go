package backup

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

var (
	today      = time.Now()
	format     = "20060102"
	backupType = struct {
		full string
		incr string
		logs string
	}{
		full: "FULL",
		incr: "INCR",
		logs: "LOGS",
	}
)

type Backup struct {
	BakMode  int
	BakDir   string
	Keep     int
	Weekday  int
	MyCnf    string
	Executor string
	LogBin   string
	DryRun   bool

	baseDir  string
	fileType string
	nameTpl  string

	filter        func(string) bool
	getLastName   func() string
	getBackupType func() string
	backupCmd     func()
}

func (backup *Backup) Run() {
	backup.createDir()
	backup.removeOld()
	backup.backupCmd()
}

func (backup *Backup) createDir() {
	os.Mkdir(backup.baseDir, 0o644)
}

func (backup *Backup) removeOld() {
	if backup.DryRun {
		return
	}
	date := today.AddDate(0, 0, int(-backup.Keep*7)).Format(format)
	for _, name := range backup.getHistory() {
		if strings.Split(name, "_")[0] > date {
			return
		}
		os.Remove(filepath.Join(backup.baseDir, name))
	}
}

func (backup *Backup) getHistory() (names []string) {
	files, err := os.ReadDir(backup.baseDir)
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		if name := file.Name(); backup.filter(name) {
			names = append(names, name)
		}
	}
	return
}

func NewDataBackup(backup *Backup) *Backup {
	backup.baseDir = filepath.Join(backup.BakDir, "data")
	backup.fileType = ".xb.zst"
	backup.nameTpl = "%s_%s_%s_%s" + backup.fileType
	backup.filter = func(name string) bool {
		return strings.HasSuffix(name, backup.fileType) && len(strings.Split(name, "_")) == 4
	}
	backup.getLastName = func() string {
		for _, name := range slices.Backward(backup.getHistory()) {
			if strings.Contains(name, backupType.full) {
				return strings.Replace(name, backup.fileType, "", 1)
			}
		}
		return ""
	}
	backup.getBackupType = func() string {
		weekday := IF(today.Weekday() == 0, 7, int(today.Weekday())) // ISOWeekday
		days := IF(weekday >= backup.Weekday, weekday-backup.Weekday, weekday-backup.Weekday+7)
		fullDate := today.AddDate(0, 0, int(-days)).Format(format)
		if strings.Split(backup.getLastName(), "_")[0] < fullDate {
			return backupType.full
		}
		return backupType.incr
	}
	backup.backupCmd = func() {
		tmpName := filepath.Join(backup.baseDir, "tmp_backup"+backup.fileType)
		f, t := "0", ""
		if backup.getBackupType() == backupType.incr {
			f = strings.Split(backup.getLastName(), "_")[3]
		}
		args := []string{backup.Executor, fmt.Sprintf("--defaults-file=%s", backup.MyCnf), "--backup", "--parallel=4", "--stream=xbstream", "--target-dir=/tmp", fmt.Sprintf("--incremental-lsn=%s", f), "|", "zstd", "-fkT4", "-o", tmpName}
		cmd := exec.Command("bash", "-c", strings.Join(args, " "))
		stdout, _ := cmd.StdoutPipe()
		cmd.Stderr = cmd.Stdout
		fmt.Println(time.Now().Format(time.DateTime), "EXECUTE:", strings.Join(args, " "))
		if backup.DryRun {
			return
		}
		if err := cmd.Start(); err != nil {
			fmt.Println(err.Error())
			return
		}
		reader := bufio.NewReader(stdout)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Println(err)
				}
				break
			}
			fmt.Print(line)
			if strings.Contains(line, "The latest check point (for incremental)") {
				t = strings.Split(line, "'")[1]
			}
		}
		if t == "" {
			os.Remove(tmpName)
			fmt.Println(time.Now().Format(time.DateTime), "FAILURE:", tmpName)
			os.Exit(1)
		}
		bakName := filepath.Join(backup.baseDir, fmt.Sprintf(backup.nameTpl, today.Format(format), backup.getBackupType(), f, t))
		os.Rename(tmpName, bakName)
		fmt.Println(time.Now().Format(time.DateTime), "SUCCESS:", bakName)
	}
	return backup
}

func NewLogsBackup(backup *Backup) *Backup {
	backup.baseDir = filepath.Join(backup.BakDir, "logs")
	backup.fileType = ".zst"
	backup.nameTpl = "%s_%s_%s" + backup.fileType
	backup.filter = func(name string) bool {
		return strings.HasSuffix(name, backup.fileType) && len(strings.Split(name, "_")) == 3
	}
	backup.getLastName = func() string {
		if history := backup.getHistory(); history != nil {
			return strings.Replace(history[len(history)-1], backup.fileType, "", 1)
		}
		return ""
	}
	backup.getBackupType = func() string {
		return backupType.logs
	}
	backup.backupCmd = func() {
		logDir, basename := filepath.Split(backup.LogBin)
		var logFiles []string
		files, err := os.ReadDir(logDir)
		if err != nil {
			panic(err)
		}
		parts := strings.Split(backup.getLastName(), "_")
		lastMax := parts[len(parts)-1]
		r, _ := regexp.Compile(basename + `\.\d{6}`)
		for _, file := range files {
			if name := file.Name(); r.MatchString(name) && name >= lastMax {
				logFiles = append(logFiles, file.Name())
			}
		}
		if lastMax == "" {
			logFiles = logFiles[len(logFiles)-1:]
		}
		fmt.Printf("find %d binlogs\n", len(logFiles))
		for _, file := range logFiles {
			bakName := filepath.Join(backup.baseDir, fmt.Sprintf(backup.nameTpl, today.Format(format), backup.getBackupType(), file))
			cmd := exec.Command("zstd", "-fkT4", filepath.Join(logDir, file), "-o", bakName)
			fmt.Println(time.Now().Format(time.DateTime), "EXECUTE:", strings.Join(cmd.Args, " "))
			if backup.DryRun {
				return
			}
			cmd.Run()
			fmt.Println(time.Now().Format(time.DateTime), "SUCCESS:", bakName)
			if file == lastMax {
				os.Remove(filepath.Join(backup.baseDir, backup.getLastName(), backup.fileType))
			}
		}
	}
	return backup
}

func IF[T any](cond bool, tureValue, falseValue T) T {
	if cond {
		return tureValue
	}
	return falseValue
}
