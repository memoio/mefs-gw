package cmd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"syscall"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/memoio/mefs-gateway/miniogw"
	"github.com/memoio/mefs-gateway/utils"
)

var (
	username   string
	pwd        string
	configPath string
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a mefs s3 gateway",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(username) == 0 || len(pwd) == 0 {
			return errors.New("wrong input")
		}

		var terminate = make(chan os.Signal, 1)
		signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(terminate)

		confPath, err := homedir.Expand(configPath)
		if err != nil {
			return err
		}

		err = utils.ReadConfig(confPath)
		if err != nil {
			return err
		}

		// 设置根路径
		var rootDir string
		if rootDir = viper.GetString("common.root_dir"); rootDir != "" {
			utils.DefaultPathRoot = rootDir
		} else {
			log.Panic("rootdir is null")
			return errors.New("rootdir is null")
		}

		pidpath, _ := homedir.Expand("~/.mefs_gw")
		pid := os.Getpid()
		log.Println("pid is ", pid)
		pids := []byte(strconv.Itoa(pid))
		log.Println(path.Join(rootDir, "pid"))
		err = os.WriteFile(path.Join(pidpath, "pid"), pids, 0644)
		if err != nil {
			log.Println(err)
		}

		endPoint := viper.GetString("common.endpoint")
		consoleAddress := viper.GetString("common.console")

		err = miniogw.Start(username, pwd, endPoint, consoleAddress)
		if err != nil {
			return err
		}

		// 收到信号，退出
		<-terminate
		log.Println("received shutdown signal")
		log.Println("shutdown...")

		return nil
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop a mefs s3 gateway",
	RunE: func(cmd *cobra.Command, args []string) error {
		rootDir, err := homedir.Expand("~/.mefs_gw")
		if err != nil {
			return err
		}

		pd, _ := ioutil.ReadFile(path.Join(rootDir, "pid"))
		if err != nil {
			return errors.New("stop failed")
		}
		err = kill(string(pd))
		if err != nil {
			return err
		}

		log.Println("gateway gracefully exit...")

		return nil
	},
}

func kill(pid string) error {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("kill", "-15", pid).Run()
	case "windows":
		return exec.Command("kill", "/F", "/T", "/PID", pid).Run()
	default:
		return fmt.Errorf("unsupported platform %s", runtime.GOOS)
	}
}

func init() {
	runCmd.Flags().StringVarP(&username, "name", "n", "", "input your user name")
	runCmd.Flags().StringVarP(&pwd, "password", "p", "", "input your password")
	runCmd.Flags().StringVarP(&configPath, "config-path", "", ".", "the path of config.toml")
}
