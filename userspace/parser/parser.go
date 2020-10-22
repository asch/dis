package parser

import (
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	envPrefix  = "dis"
	configType = "toml"
	configName = "config"
)

var v *viper.Viper

func Init() {
	config := flag.StringP("config", "c", configName+"."+configType, "Path to config file")
	flag.Parse()

	v = viper.New()
	v.SetConfigFile(*config)
	v.SetConfigType(configType)
	v.SetEnvPrefix(envPrefix)

	err := v.ReadInConfig()
	if err != nil {
		panic(err)
	}
}

func Sub(section string) *viper.Viper {
	return v.Sub(section)
}
