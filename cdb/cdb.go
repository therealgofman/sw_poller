package cdb

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/inconshreveable/log15"
	"os"
	"strings"
	"time"
)

type Switch struct {
	Id        int
	Ip        string
	Model     string
	Community string
	Domain    int
	Portinfo  map[int]map[int]string
}

type Cdb struct {
	user       string
	password   string
	sid        string
	name       string
	domain     []string
	lastDbTime int64
	Log        log.Logger
	switches   map[int]Switch
}

func New(dbuser string, dbpassword string, dbsid string, dbname string, dbdomain []string, logger log.Logger) *Cdb {
	os.Setenv("NLS_LANG", "AMERICAN_AMERICA.AL32UTF8")

	c := new(Cdb)
	c.user = dbuser
	c.password = dbpassword
	c.sid = dbsid
	c.name = dbname
	c.domain = dbdomain
	c.lastDbTime = 0
	c.switches = make(map[int]Switch)
	c.Log = logger
	return c
}

func (c *Cdb) GetSwitches() map[int]Switch {
	// if switches is empty, OR if more then 1hour past from prev DB query, refresh map
	nowSwitches := len(c.switches)

	if nowSwitches <= 0 || (time.Now().Unix()-3600) > c.lastDbTime {
		c.Log.Info("Updating switches from DB")

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", c.user, c.password, c.sid, c.name))
		if err != nil {
			c.Log.Error("Error connecting to DB: %s", err)
			time.Sleep(300 * time.Second)
			return c.GetSwitches()
		}
		defer db.Close()

		// clear old map
		c.switches = make(map[int]Switch)

		// get all switches in domain
		switches, err := db.Query(`SELECT sw.d_switch_id, INET_NTOA(sw.ip) as ip, m.title, IF(comm>"",comm,"vista") as comm, sw.domain_id 
					FROM 
					    d_switch AS sw, d_model AS m 
					WHERE 
					    sw.domain_id IN (` + strings.Join(c.domain, `","`) + `) and m.d_model_id=sw.d_model_id`)

		if err != nil {
			c.Log.Error("Error fetching switches from DB: %s", err)
			time.Sleep(300 * time.Second)
			return c.GetSwitches()
		}

		num := 1
		for switches.Next() {
			var sw_id int
			var ip string
			var model string
			var community string
			var domain_id int
			switches.Scan(&sw_id, &ip, &model, &community, &domain_id)

			// Format model name
			model = strings.Replace(model, " ", "_", -1)
			model = strings.Replace(model, ".", "_", -1)

			var sw Switch
			sw.Id = sw_id
			sw.Ip = ip
			sw.Portinfo = make(map[int]map[int]string)
			sw.Model = model
			sw.Community = community
			sw.Domain = domain_id
			c.switches[num] = sw

			//add port_info
			portinfo, _ := db.Query(`
						SELECT d_switch_id, ifindex, name 
						FROM 
						    d_port 
						WHERE 
						    last_upd > ( unix_timestamp(curdate())-86400*10 ) 
						    AND d_switch_id = ? ORDER BY d_switch_id ASC, ifindex ASC`, sw_id)

			for portinfo.Next() {
				var port_id int
				var index int
				var name string
				portinfo.Scan(&port_id, &index, &name)
				if _, ok := sw.Portinfo[port_id]; ok {
					sw.Portinfo[port_id][index] += name
				} else {
					sw.Portinfo[port_id] = make(map[int]string)
					sw.Portinfo[port_id][index] += name
				}
			}
			num++
		}

		c.lastDbTime = time.Now().Unix()
		switches.Close()
	}
	return c.switches
}
