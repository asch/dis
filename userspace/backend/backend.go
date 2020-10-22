package backend

import (
	"dis/backend/file"
	"dis/backend/s3"
	"dis/extent"
	"dis/parser"
	"reflect"
)

const (
	configSection = "backend"
	envPrefix     = "dis_backend"
)

var (
	enabled  string
	instance backend

	backendMap = map[string]reflect.Type{
		"file": reflect.TypeOf(file.FileBackend{}),
		"s3":   reflect.TypeOf(s3.S3Backend{}),
	}
)

type backend interface {
	Init()
	Read(*[]extent.Extent)
	Write(*[]extent.Extent)
}

func Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)
	enabled = v.GetString("enabled")

	T := backendMap[enabled]
	instance = reflect.New(T).Interface().(backend)
	instance.Init()
}

func Read(e *[]extent.Extent) {
	instance.Read(e)
}

func Write(e *[]extent.Extent) {
	instance.Write(e)
}
