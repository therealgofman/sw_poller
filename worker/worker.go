package worker

import (
	"../ccfg"
	"../cdb"
	"fmt"
	log "github.com/inconshreveable/log15"
	gsnmp "github.com/soniah/gosnmp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Worker struct {
	Todo    map[int]cdb.Switch
	LastKey int
	KeyLock sync.Mutex
	MapLock sync.Mutex
	Log     log.Logger
}

func New(logger log.Logger) *Worker {
	w := new(Worker)
	w.Todo = make(map[int]cdb.Switch)
	w.LastKey = 0
	w.Log = logger

	return w
}

func (w *Worker) NewKey() int {
	w.KeyLock.Lock()
	defer w.KeyLock.Unlock()
	w.LastKey++
	return w.LastKey
}

func (w *Worker) GetTodoLen() int {
	w.MapLock.Lock()
	defer w.MapLock.Unlock()
	size := len(w.Todo)
	return size
}

func (w *Worker) AddToQueue(s cdb.Switch) {
	NewKey := w.NewKey()
	w.MapLock.Lock()
	defer w.MapLock.Unlock()
	w.Todo[NewKey] = s
	return
}

func (w *Worker) RemoveFromQueue(DeletKey int) {
	w.MapLock.Lock()
	defer w.MapLock.Unlock()
	delete(w.Todo, DeletKey)
	return
}

func (w *Worker) GetNextKey() int {
	var ReturnKey int
	w.MapLock.Lock()
	defer w.MapLock.Unlock()
	for key, _ := range w.Todo {
		ReturnKey = key
		break
	}
	return ReturnKey
}

func (w *Worker) Start(models map[string]ccfg.Model, chanUdp chan<- string) {
	for {
		if w.GetTodoLen() == 0 {
			time.Sleep(3 * time.Second)
			continue
		}

		JobKey := w.GetNextKey()

		w.MapLock.Lock()
		sw := w.Todo[JobKey]
		w.MapLock.Unlock()

		model, ok := models[sw.Model]
		if !ok {
			//w.Log.Warn("Skipping unknown model:", "model", sw.Model, "ip", sw.Ip)
			w.RemoveFromQueue(JobKey)
			continue
		}

		snmp := &gsnmp.GoSNMP{
			Target:    sw.Ip,
			Port:      161,
			Community: sw.Community,
			Version:   gsnmp.Version2c,
			Timeout:   time.Duration(5) * time.Second,
			Retries:   1,
		}

		err := snmp.Connect()
		if err != nil {
			w.Log.Error("Error connect to:", sw.Ip, "err: ", err)
		}


		// do snmp stuff
		for key, oid := range model.Oids {

			var table []gsnmp.SnmpPDU
			table, err = snmp.BulkWalkAll(oid)

			if err != nil {
				w.Log.Error("Walk Error:", sw.Ip, err)
			}

		Table:
			for i := 0; i < len(table); i++ {
				spl := strings.Split(table[i].Name, ".")
				j := spl[len(spl)-1]
				index, err := strconv.ParseInt(j, 10, 64)

				if err != nil {
					fmt.Printf("Error parsing index->int64\n")
					continue
				}

				//convert int64 to int
				index_int := int(index)
				if _, ok := sw.Portinfo[sw.Id][index_int]; ok {
					port := sw.Portinfo[sw.Id][index_int]

					var toSend string
					switch table[i].Type {
					case gsnmp.Counter64:
						strVal := fmt.Sprintf("%d", gsnmp.ToBigInt(table[i].Value))
						strPort := fmt.Sprintf("%s", port)
						strDomain := fmt.Sprintf("%d",sw.Domain)
						tm := time.Now().UnixNano()
						toSend = key + ",domain_id=" + strDomain + ",host=" + sw.Ip + ",port=" + strPort + " value=" + strVal + "i " + fmt.Sprintf("%d", tm)
						//w.Log.Info("Walk data from: " + sw.Ip + " " +  "key: " + key + " " + "port:" + strPort + " " +  "value: " + strVal)
						chanUdp <- toSend
					case gsnmp.Counter32:
						strVal := fmt.Sprintf("%d", gsnmp.ToBigInt(table[i].Value))
						strPort := fmt.Sprintf("%s", port)
						strDomain := fmt.Sprintf("%d",sw.Domain)
						tm := time.Now().UnixNano()
						toSend = key + ",domain_id=" + strDomain + ",host=" + sw.Ip + ",port=" + strPort + " value=" + strVal + "i " + fmt.Sprintf("%d", tm)
						//w.Log.Info("Walk data from: " + sw.Ip + " " +  "key: " + key + " " + "port:" + strPort + " " +  "value: " + strVal)
						chanUdp <- toSend
					default:
						continue Table
					}

				} else {
					continue Table
				}

			}
		}
		w.RemoveFromQueue(JobKey)
	}
}
