package utils

import (
	"github.com/spf13/viper"
)

func ReadConfig(p string) error {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(p)
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}
	return nil
}
