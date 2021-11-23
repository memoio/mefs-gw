package cmd

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/memoio/mefs-gateway/miniogw"
)

var (
	username string
	pwd      string
	endPoint string
	consoleAddress string
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

		err := miniogw.Start(username, pwd, endPoint, consoleAddress)
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
	runCmd.Flags().StringVarP(&endPoint, "endpoint", "e", "0.0.0.0:5080", "input the s3 endpoint")
	runCmd.Flags().StringVarP(&consoleAddress, "console-address", "c", ":8080", "console-address")
}
