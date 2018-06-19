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
	"runtime"
	"github.com/ryanjdew/go-marklogic-go/documents"
	"strconv"
	"github.com/ryanjdew/go-marklogic-go/search"
)

type MarkLogicBentch struct {
	host     string
	port     int64
	userName string
	passWord string
	cpunum  int
	datanum int
	procnum int

}

type Datainfo struct {
	Name string
	Num  int
	Lng  float64
	Lat  float64
}

func (marklogicbench *MarkLogicBentch) InsertData(client *marklogic.Client,jsonFile string, jsoninfo string, ch chan int) {
	//datainfo := Datainfo{"Edison", 1, 117.867589, 35.895416}
	for i := 0; i < marklogicbench.datanum; i++ {
		if jsoninfo == "no" {
			docSrv := client.Documents()
			mapHandle := handle.RawHandle{
				Format: handle.JSON,
			}
			file, _ := os.OpenFile("./test.json", os.O_RDWR|os.O_CREATE, 0666)
			defer file.Close()

			docDescription := documents.DocumentDescription{
				URI:     "/test" + strconv.Itoa(i) + ".json",
				Content: file,
			}

			docs := [] documents.DocumentDescription{docDescription}
			err := docSrv.Write(docs, nil, &mapHandle)

			if err != nil {
				logger.Println("insert failed:", err)
				os.Exit(1)
			}

		} else if jsoninfo == "yes" {
			docSrv := client.Documents()
			mapHandle := handle.RawHandle{
				Format: handle.JSON,
			}
			file, _ := os.OpenFile(jsonFile, os.O_RDWR|os.O_CREATE, 0666)
			defer file.Close()

			docDescription := documents.DocumentDescription{
				URI:     "/test" + strconv.Itoa(i) + ".json",
				Content: file,
			}

			docs := [] documents.DocumentDescription{docDescription}
			err := docSrv.Write(docs, nil, &mapHandle)

			if err != nil {
				logger.Println("insert failed:", err)
				os.Exit(1)
			}
		}

	}
	ch <- 1
}

func (marklogicbench *MarkLogicBentch) UpdateData(client *marklogic.Client,ch chan int) {
	for i := 0; i < marklogicbench.datanum; i++ {
		docSrv := client.Documents()

		mapHandle := handle.RawHandle{
			Format: handle.JSON,
		}
		file, _ := os.OpenFile("E:\\gopath\\src\\ml-load\\ml-example\\patch.json", os.O_RDWR|os.O_CREATE, 0666)
		defer file.Close()

		docDescription := documents.DocumentDescription{
			URI:     "/test" + strconv.Itoa(i) + ".json",
			Content: file,
		}

		docs := [] documents.DocumentDescription{docDescription}
		err := docSrv.Update(docs, nil, &mapHandle)

		if err != nil {
			logger.Println("update failed:", err)
			os.Exit(1)
		}
	}
	ch <- 1
}
func (marklogicbench *MarkLogicBentch) QueryData(client *marklogic.Client,queryStr string, all bool, ch chan int) {
	//datainfo := Datainfo{"Edison", 1, 117.867589, 35.895416}
	if all == true && queryStr!="" {
		for i := 0; i < marklogicbench.datanum; i++ {
			query := search.Query{Format: handle.JSON}
			query.Queries = []interface{}{
				search.TermQuery{
					Terms: []string{queryStr},
				},
			}

			qh := search.QueryHandle{}
			qh.Serialize(query)
			//fmt.Print("Serialized query:\n")
			//fmt.Print(spew.Sdump(qh.Serialized()))
			respHandle := search.ResponseHandle{}
			_ = client.Search().StructuredSearch(&qh, 1, 10, &respHandle)
			//resp := respHandle.Get()
			//fmt.Print("Serialized response:\n")
			//fmt.Print(spew.Sdump(resp))
			sugRespHandle := search.SuggestionsResponseHandle{}
			_ = client.Search().StructuredSuggestions(&qh, queryStr, 10, "", &sugRespHandle)
			//sugResp := sugRespHandle.Serialized()
			//fmt.Print("Serialized response:\n")
			//fmt.Print(spew.Sdump(sugResp))
			//fmt.Println(sugResp)
		}

	} else {

		//var result1 interface{}
		//for i := 0; i < marklogicbench.datanum; i++ {
		//	//var query bson.M
		//
		//		b := datainfo.Num * r.Intn(marklogicbench.datanum)
		//		query = bson.M{"Num": b}
		//
		//}
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



func main() {
	var host, username, password, auth, logpath, jsonfile, queryStr string
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
	flag.IntVar(&cpunum, "cpunum", 1, "The cpu number wanna use")
	flag.IntVar(&datanum, "datanum", 10000, "The data count per proc")
	flag.IntVar(&procnum, "procnum", 4, "The proc num ")
	flag.StringVar(&logpath, "logpath", "./log.log", "the log path ")
	flag.StringVar(&jsonfile, "jsonfile", "./test.json", "the json file u wanna insert(only one json )")
	flag.StringVar(&operation, "operation", "", "the operation ")
	flag.BoolVar(&queryall, "queryall", true, "query all or limit one")
	flag.StringVar(&queryStr, "query", "query", "Search query file")
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

		if operation == "read" {
			cate := []string{"content"}
			uid := []string{`/cluster-ui-settings.xml`}
			docSrv := client.Documents()
			mapHandle := handle.RawHandle{
				Format: handle.XML,
			}
			//fmt.Println(spew.Sdump(mapHandle.Serialized()))
			e := docSrv.Read(uid, cate, nil, &mapHandle)
			if e != nil {
				log.Printf("Document read Error,%v\n", e)
				os.Exit(1)
			}
			//fmt.Println(mapHandle.Get())
		} else if operation == "query" {

			chs := make([]chan int, marklogicbench.procnum)
			runtime.GOMAXPROCS(marklogicbench.cpunum)
			for i := 0; i < marklogicbench.procnum; i++ {
				fmt.Println(i)

				chs[i] = make(chan int)

				go marklogicbench.QueryData(client,queryStr,queryall, chs[i])

			}

			for _, cha := range chs {
				<-cha

			}

		}else if operation == "insert" {
			chs := make([]chan int, marklogicbench.procnum)
			runtime.GOMAXPROCS(marklogicbench.cpunum)
			for i := 0; i < marklogicbench.procnum; i++ {
				fmt.Println(i)

				chs[i] = make(chan int)

				if jsonfile == "" {
					go marklogicbench.InsertData(client, jsonfile,"no", chs[i])
				} else {
					go marklogicbench.InsertData(client, jsonfile,"yes", chs[i])
				}
			}

			for _, cha := range chs {
				<-cha

			}
		}else if operation == "update" {
			ch := make([]chan int, marklogicbench.procnum)
			runtime.GOMAXPROCS(marklogicbench.cpunum)
			for i := 0; i < marklogicbench.procnum; i++ {
				fmt.Println(i)

				ch[i] = make(chan int)

				go marklogicbench.UpdateData(client, ch[i])

			}

			for _, cha := range ch {
				<-cha

			}

		}else if operation == "delete"{
			cate := []string{"content"}

			uid := []string{`/test.json802`,`/test.json803`,`/test.json804`}

			docSrv := client.Documents()
			mapHandle := handle.RawHandle{
				Format: handle.JSON,
			}
			//fmt.Println(spew.Sdump(mapHandle.Serialized()))
			e := docSrv.Delete(uid, cate, &mapHandle)
			if e != nil {
				log.Printf("document delete Error,%v\n", e)
				os.Exit(1)
			}
			//fmt.Println(mapHandle.Get())
		}else if host != "" && clean == true {
			logger.Println("=====job start.=====")
			logger.Println("start init colletion")
			//mongobench := NewMarkLogicBentch(host, username, password, port, cpunum, datanum, procnum)
			//mongobench.CleanJob()
		} else {
			fmt.Println("Please use -help to check the usage")
			fmt.Println("At least need host parameter")
		}
		logger.Println("=====Done.=====")


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
