package ccfg

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"github.com/spf13/viper"
)

type Model struct {
	Name       string
	Community  string
	Oids       map[string]string
}

type Def struct {
	Name       string
	Community  string
	Oids       map[string]string
}

type Cfg struct {
	Dbuser         string
	Dbpassword     string
	Dbsid          string
	Dbname         string
	Dbdomain       []string
	WorkersCount   int
	Models         map[string]Model
	Hosts          []string
	DefModel Def
}

func New(path *string) *Cfg {

	viper.SetConfigFile(*path)
	err := viper.ReadInConfig()
	if err != nil {
		panic(err.Error())
	}

	c := new(Cfg)
	c.Dbuser = viper.GetString("db.user")
	c.Dbpassword = viper.GetString("db.password")
	c.Dbsid = viper.GetString("db.sid")
	c.Dbname = viper.GetString("db.name")
	c.Dbdomain = viper.GetStringSlice("db.domain")
	c.WorkersCount = viper.GetInt("workers")

	if c.Dbuser == "" || c.Dbpassword == "" || c.Dbsid == "" || c.Dbname == "" || c.WorkersCount == 0  {
		panic(fmt.Errorf("Fatal: missing mandatory config parameters."))
	}

	hosts := viper.Get("influx.hosts").([]interface{})
	hostsCount := len(hosts)
	if hostsCount < 1 {
		panic("No influx hosts defined in config!")
	}

	c.Hosts = make([]string, hostsCount)
	for idx := 0; idx < hostsCount; idx++ {
		c.Hosts[idx] = hosts[idx].(string)
	}

	c.DefModel.Community = viper.GetString("default.community")

	c.DefModel.Oids = make(map[string]string)
	c.DefModel.Oids = viper.GetStringMapString("default.oids")

	c.Models = make(map[string]Model)

	modelsCfg := viper.Get("models").([]interface{})
	for _, v := range modelsCfg {
		value := v.(map[string]interface{})
		name, ok := value["name"].(string)
		if !ok {
			continue
		}

		var Mdl Model
		Mdl.Name = name

		community, ok := value["community"].(string)
		if ok {
			Mdl.Community = community
		} else {
			Mdl.Community = c.DefModel.Community
		}

		// Hard: oids
		oidsVal, ok := value["oids"]
		if !ok {
			Mdl.Oids = c.DefModel.Oids
		} else {
			oids := oidsVal.(map[string]interface{})
			Mdl.Oids = make(map[string]string)
			for x, y := range oids {
				Mdl.Oids[x] = y.(string)
			}
		}

		c.Models[name] = Mdl
	}
	return c
}
