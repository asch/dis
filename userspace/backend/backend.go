// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package backend

import (
	"dis/backend/file"
	"dis/backend/null"
	"dis/backend/object"
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
		"file":   reflect.TypeOf(file.FileBackend{}),
		"null":   reflect.TypeOf(null.NullBackend{}),
		"object": reflect.TypeOf(object.ObjectBackend{}),
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
	v.BindEnv("enabled")
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
