package cmd

import (
	"errors"
	"log"
	"os"
	"os/signal"
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
		if rootDir := viper.GetString("common.root_dir"); rootDir != "" {
			utils.DefaultPathRoot = rootDir
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

func init() {
	runCmd.Flags().StringVarP(&username, "name", "n", "", "input your user name")
	runCmd.Flags().StringVarP(&pwd, "password", "p", "", "input your password")
	runCmd.Flags().StringVarP(&configPath, "config-path", "", ".", "the path of config.toml")
}
