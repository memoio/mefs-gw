package cmd

import "github.com/spf13/cobra"

func init() {
	rootCmd.AddCommand(runCmd)
}

var rootCmd = &cobra.Command{
	Use:   "",
	Short: "",
	Long:  "",
}

func Execute() error {
	return rootCmd.Execute()
}
