package main

import (
	"os"
	"log"
	"io"
	"math/rand"
	"time"
	"flag"
	"fmt"
)

import (
	marklogic "github.com/ryanjdew/go-marklogic-go"
	handle "github.com/ryanjdew/go-marklogic-go/handle"
	"io/ioutil"
	"encoding/json"
	"github.com/davecgh/go-spew/spew"
	"runtime"
	"github.com/ryanjdew/go-marklogic-go/documents"
	"strconv"
)

type MarkLogicBentch struct {
	host     string
	port     int64
	userName string

	passWord string
	//dname           string
	//cname           string
	cpunum  int
	datanum int
	procnum int
	//geofield        string
	//mongoClient     *mgo.Session
	//mongoDatabase   *mgo.Database
	//mongoCollection *mgo.Collection
}

type Datainfo struct {
	Name string
	Num  int
	Lng  float64
	Lat  float64
}

func (marklogicbench *MarkLogicBentch) InsertData(client *marklogic.Client, jsoninfo string, ch chan int) {
	//datainfo := Datainfo{"Edison", 1, 117.867589, 35.895416}
	for i := 0; i < marklogicbench.datanum; i++ {

		//a := datainfo.Num * r.Intn(marklogicbench.datanum)
		//lng := datainfo.Lng + float64(r.Intn(10))*r.Float64()
		//lat := datainfo.Lat + float64(r.Intn(10))*r.Float64()
		if jsoninfo == "no" {
			//err := mongobench.mongoCollection.Insert(bson.M{"Name": datainfo.Name, "Num": a})
			//docSrv := client.Documents()
			//mapHandle := handle.RawHandle{
			//	Format: handle.XML,
			//}
			//fmt.Println(spew.Sdump(mapHandle.Serialized()))
			//docSrv.Write()

			//loc := bson.M{"type": "Point", "coordinates": []float64{lng, lat}}
			//err := mongobench.mongoCollection.Insert(bson.M{"Name": datainfo.Name, "Num": a, mongobench.geofield: loc})
			//
			//if err != nil {
			//	logger.Println("insert failed:", err)
			//	os.Exit(1)
			//}
		} else if jsoninfo == "yes" {
			docSrv := client.Documents()
			mapHandle := handle.RawHandle{
				Format: handle.XML,
			}
			//bts, _ := json.Marshal(jsonMap)
			//reader := bytes.NewReader(bts)
			//

			fmt.Println(spew.Sdump(mapHandle.Serialized()))
			docDescription := documents.DocumentDescription{
				URI:     "/test" + strconv.Itoa(i) + ".json",
				Content: nil,
			}

			docs := [] documents.DocumentDescription{docDescription}
			err := docSrv.Write(docs, nil, &mapHandle)

			//err := mongobench.mongoCollection.Insert(jsonMap)
			if err != nil {
				logger.Println("insert failed:", err)
				os.Exit(1)
			}
		}

	}
	ch <- 1
}

func NewMarkLogicBentch(host, userName, passWord string, port int64, cpunum, datanum, procnum int) *MarkLogicBentch {
	mongobench := &MarkLogicBentch{host, port, userName, passWord, cpunum, datanum, procnum}
	return mongobench
}

var logfile *os.File
var logger *log.Logger
var jsondata = make(map[string]interface{})
var jsonMap = make(map[string]interface{})
var r *rand.Rand

func ReadJson(filename string) (map[string]interface{}, error) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Println("ReadFile:", err.Error())
		return nil, err
	}
	if err := json.Unmarshal(bytes, &jsondata); err != nil {
		logger.Println("unmarshal:", err.Error())
		return nil, err
	}
	return jsondata, nil

}

func main() {
	var host, username, password, auth, logpath, jsonfile string
	var port int64
	var operation string
	var queryall, clean bool
	var cpunum, datanum, procnum int
	var err error
	var multi_logfile []io.Writer

	r = rand.New(rand.NewSource(time.Now().UnixNano()))

	/** 解析命令行*/
	flag.StringVar(&host, "host", "localhost", "MarkLogic REST Host")
	flag.Int64Var(&port, "port", 8000, "MarkLogic REST Port")
	flag.StringVar(&username, "username", "admin", "MarkLogic REST Username")
	flag.StringVar(&password, "password", "heng130509", "MarkLogic REST Password")
	flag.StringVar(&auth, "auth", "digest", "MarkLogic REST Authentication method")
	// flag.StringVar(&queryStr, "query", "query", "Search query file")
	flag.IntVar(&cpunum, "cpunum", 1, "The cpu number wanna use")
	flag.IntVar(&datanum, "datanum", 2, "The data count per proc")
	flag.IntVar(&procnum, "procnum", 4, "The proc num ")
	flag.StringVar(&logpath, "logpath", "./log.log", "the log path ")
	flag.StringVar(&jsonfile, "jsonfile", "", "the json file u wanna insert(only one json )")
	flag.StringVar(&operation, "operation", "", "the operation ")
	flag.BoolVar(&queryall, "queryall", false, "query all or limit one")
	flag.BoolVar(&clean, "clean", false, "Drop the Database which --db given")

	flag.Parse()

	// 准备日志
	logfile, err = os.OpenFile(logpath, os.O_RDWR|os.O_CREATE, 0666)
	defer logfile.Close()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	multi_logfile = []io.Writer{
		logfile,
		os.Stdout,
	}
	logfiles := io.MultiWriter(multi_logfile...)
	logger = log.New(logfiles, "\r\n", log.Ldate|log.Ltime|log.Lshortfile)

	// 创建MarkLogic客户端
	client := newMarkLogicClient(host, username, password, auth, port)

	if host != "" && operation != "" && clean == false {
		logger.Println("=====job start.=====")
		logger.Println("start init colletion")
		logger.Println(datanum, operation)
		marklogicbench := NewMarkLogicBentch(host, username, password, port, cpunum, datanum, procnum)

		if jsonfile != "" {
			var err error
			jsonMap, err = ReadJson(jsonfile)
			logger.Println(jsonMap)
			if err != nil {
				logger.Println(err)
			}
		}

		if operation == "read" {
			//transform := util.Transform{
			//	//Name: "",
			//}
			cate := []string{"content"}

			uid := []string{`/cluster-ui-settings.xml`}

			docSrv := client.Documents()
			mapHandle := handle.RawHandle{
				Format: handle.XML,
			}
			fmt.Println(spew.Sdump(mapHandle.Serialized()))
			e := docSrv.Read(uid, cate, nil, &mapHandle)
			if e != nil {
				log.Printf("Document Service Error,%v\n", e)
				os.Exit(1)
			}
			fmt.Println(mapHandle.Get())
		} else if operation == "insert" {
			chs := make([]chan int, marklogicbench.procnum)
			runtime.GOMAXPROCS(marklogicbench.cpunum)
			for i := 0; i < marklogicbench.procnum; i++ {
				fmt.Println(i)

				chs[i] = make(chan int)

				if jsonfile == "" {
					go marklogicbench.InsertData(client, "no", chs[i])
				} else {
					go marklogicbench.InsertData(client, "yes", chs[i])
				}
			}

			for _, cha := range chs {
				<-cha

			}
		}else if operation == "delete"{
			cate := []string{"content"}

			uid := []string{`/test.json802`,`/test.json803`,`/test.json804`}

			docSrv := client.Documents()
			mapHandle := handle.RawHandle{
				Format: handle.JSON,
			}
			fmt.Println(spew.Sdump(mapHandle.Serialized()))
			e := docSrv.Delete(uid, cate, &mapHandle)
			if e != nil {
				log.Printf("Document Service Error,%v\n", e)
				os.Exit(1)
			}
			fmt.Println(mapHandle.Get())
		}else if host != "" && clean == true {
			logger.Println("=====job start.=====")
			logger.Println("start init colletion")
			//mongobench := NewMarkLogicBentch(host, username, password, port, cpunum, datanum, procnum)
			//mongobench.CleanJob()
		} else {
			fmt.Println("Please use -help to check the usage")
			fmt.Println("At least need host parameter")
		}


	}
}

func newMarkLogicClient(host, username, password, auth string, port int64) *marklogic.Client {
	var authType int
	if auth == "basic" {
		authType = marklogic.BasicAuth
	} else if auth == "digest" {
		authType = marklogic.DigestAuth
	} else {
		authType = marklogic.None
	}
	client, err := marklogic.NewClient(host, port, username, password, authType)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	return client
}
