package main
import (
	"strings"
	"fmt"
	"time"
	"os"
	"bufio"
	"io"
	"regexp"
	"strconv"
	"net/url"
	"github.com/influxdata/influxdb/client/v2"
	"flag"
	"net/http"
	"runtime"
	"encoding/json"
	"github.com/sirupsen/logrus"
)

type Reader interface {
	Read(rc chan []byte)
}

type Writer interface {
	Write(wc chan *Message)
} 

type LogProcess struct {
	rc chan []byte //从读取模块到解析模块进行通信
	wc chan *Message //从写入模块到解析模块进行通信
	read Reader
	write Writer
}

type ReadFromFile struct {
	path string
}

type WriteToInfluxDB struct {
	influxDBDsn string
}

type Message struct {
	TimeLocal time.Time
	BytesSent int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime float64
}

type SystemInfo struct {
	GOOS string `json:"运行平台"`
	GOARCH string `json:"平台架构"`
	GOMAXPROCS int `json:"CPU数量"`
	HOSTNAME string `json:"平台名称"`

	StartRunTime string `json:"系统开始运行时间"`
	RunTime string `json:"系统运行总时间"` //系统运行总时间
	ErrNum int `json:"发生错误数量"` //错误数

	ReadHandleLine int `json:"读取日志总行数"` //读取日志处理行数
	ProcessHandleLine int `json:"处理日志总行数"` //中间件处理条数
	WriteHandleLine int `json:"持久化数据到influxdb记录数"` // 写入influxdb条数

	ReadTps float64 `json:"读取日志TPS"`  //系统吞吐量
	ProcessTps float64 `json:"处理日志TPS"`  //系统吞吐量
	WriteTps float64 `json:"持久化日志TPS"`  //系统吞吐量

	ReadChanLen int `json:"读取队列长度"` //readChan 长度
	WriteChanLen int `json:"持久化队列长度"`  //writeChan 长度
	TypeMonitorChanLen int `json:"监控队列长度"`  //监控队列的长度

	ReadChanCap int `json:"读取队列容量"`
	WriteChanCap int `json:"持久化队列容量"`
	TypeMonitorChanCap int `json:"监控队列容量"`

	ReadGoroutineNum int `json:"读取日志任务数量"`
	ProcessGoroutineNum int `json:"处理日志任务数量"`
	WriteGoroutineNum int `json:"持久化数据任务数量"`
	AuthInfo string `json:"作者信息:"`
}

const (
	TypeReadHandleLine = 0   //信号标志位
	TypeProcessHandleLine = 1   //信号标志位
	TypeWriteHandleLine = 2   //信号标志位
	TypeErrNum = 9   //信号标志位
	TpsInterval = 3  //tps统计时间间隔
	ReadChanCap = 100
	WriteChanCap = 100
	TypeMonitorChanCap = 50
	ReadGoroutineNum = 20
	ProcessGoroutineNum = 20
	WriteGoroutineNum = 20
)
var log = logrus.New()
var TypeMonitorChan = make(chan byte, TypeMonitorChanCap)

type Monitor struct {
	data SystemInfo
	startTime time.Time
	ReadtpSli []int
	ProcesstpSli []int
	WritetpSli []int
}

func (m *Monitor) start(lp *LogProcess) {

	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeErrNum:
				m.data.ErrNum += 1
			case TypeReadHandleLine:
				m.data.ReadHandleLine += 1
			case TypeProcessHandleLine:
				m.data.ProcessHandleLine += 1
			case TypeWriteHandleLine:
				m.data.WriteHandleLine += 1
			}
		}
	}()

	go func() {
		ReadTicker := time.NewTicker(TpsInterval * time.Second) //开启一个TpsInterval 秒的定时器
		for {
			<- ReadTicker.C  //从定时器里接收消息 ping
			m.ReadtpSli = append(m.ReadtpSli, m.data.ReadHandleLine)
			// 防止tpSli的长度无限增长,进行切割
			if len(m.ReadtpSli) >2 {
				m.ReadtpSli = m.ReadtpSli[1:] //长度保持为2
			}
		}
	}()

	go func() {
		ProcessTicker := time.NewTicker(TpsInterval * time.Second) //开启一个TpsInterval 秒的定时器
		for {
			<- ProcessTicker.C  //从定时器里接收消息 ping
			m.ProcesstpSli = append(m.ProcesstpSli, m.data.ProcessHandleLine)
			// 防止tpSli的长度无限增长,进行切割
			if len(m.ProcesstpSli) >2 {
				m.ProcesstpSli = m.ProcesstpSli[1:] //长度保持为2
			}
		}
	}()

	go func() {
		WriteTicker := time.NewTicker(TpsInterval * time.Second) //开启一个TpsInterval 秒的定时器
		for {
			<- WriteTicker.C  //从定时器里接收消息 ping
			m.WritetpSli = append(m.WritetpSli, m.data.WriteHandleLine)
			if len(m.WritetpSli) >2 {
				m.WritetpSli = m.WritetpSli[1:] //长度保持为2
			}
		}
	}()

	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		m.data.GOOS = runtime.GOOS
		m.data.GOARCH = runtime.GOARCH
		m.data.GOMAXPROCS = runtime.GOMAXPROCS(0)
		m.data.HOSTNAME, _ = os.Hostname()
		m.data.AuthInfo = "github.com/Yq2"
		m.data.StartRunTime = m.startTime.Format("2006-01-02 15:04:05")
		m.data.RunTime = time.Now().Sub(m.startTime).String()
		m.data.ReadChanCap = cap(lp.rc)
		m.data.WriteChanCap = cap(lp.wc)
		m.data.TypeMonitorChanCap = cap(TypeMonitorChan)
		m.data.WriteGoroutineNum = WriteGoroutineNum
		m.data.ReadGoroutineNum = ReadGoroutineNum
		m.data.ProcessGoroutineNum = ProcessGoroutineNum
		m.data.ReadChanLen = len(lp.rc)  //读取通道长度
		m.data.WriteChanLen = len(lp.wc)  //写通道长度
		m.data.TypeMonitorChanLen = len(TypeMonitorChan)  //监控程序本身通道长度
		if len(m.ReadtpSli) >= 2 {
			m.data.ReadTps = float64(m.ReadtpSli[1]- m.ReadtpSli[0]) / TpsInterval
		}
		if len(m.ProcesstpSli) >= 2 {
			m.data.ProcessTps = float64(m.ProcesstpSli[1]- m.ProcesstpSli[0]) / TpsInterval
		}
		if len(m.WritetpSli) >= 2 {
			m.data.WriteTps = float64(m.WritetpSli[1]- m.WritetpSli[0]) / TpsInterval
		}
		ret ,_ := json.MarshalIndent(m.data, "", "\t")
		io.WriteString(writer,string(ret))
	})

	http.ListenAndServe(":9193", nil)
}

func (r *ReadFromFile) Read(rc chan []byte) {
	f, err := os.Open(r.path)
	if err != nil {
		TypeMonitorChan <- TypeErrNum
		log.Errorf("open file error:%s",err.Error())
		panic(fmt.Sprintf("open file error:%s", err.Error()))
	}
	//把文件的字符指针移动到末尾
	f.Seek(0,2)
	// 2 从文件末尾开始逐行读取内容
	rd := bufio.NewReader(f)
	for {
		//从buf里面读取文件，直到\n结束
		line, err := rd.ReadBytes('\n')
		if err == io.EOF {
			time.Sleep(500 * time.Millisecond)
			continue
		} else if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Errorf("ReadBytes error:%s",err.Error())
			panic(fmt.Sprintf("ReadBytes error:%s", err.Error()))
		}
		TypeMonitorChan <- TypeReadHandleLine
		rc <- line[:len(line)-1]
	}
}

func (w *WriteToInfluxDB) Write(wc chan *Message) {
	infSli := strings.Split(w.influxDBDsn, "@")
	// 创建一个 influxdb客户端，配置http参数
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: infSli[0],
		Username: infSli[1],
		Password: infSli[2],
	})

	if err != nil {
		TypeMonitorChan <- TypeErrNum
		log.Errorf("Create influxdb HTTPClient error:%s", err.Error())
		//log.Fatal(err)
	}

	for v := range wc {
		// 传递过来的是Message指针类型数据
		// create a BatchPoints
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database: infSli[3], //指定数据库
			Precision:infSli[4], //指定精度,这里是秒
		})
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Errorf("influxdb NewBatchPoints error:%s", err.Error())
			//log.Fatalln(err)
		}
		tags := map[string]string{"Path": v.Path, "Method": v.Method, "Scheme":v.Scheme, "Status":v.Status}
		fields := map[string]interface{}{
			"UpstreamTime": v.UpstreamTime,
			"RequestTime": v.RequestTime,
			"BytesSent": v.BytesSent, //流量
		}
		// 对原始写入进行疯转
		pt, err := client.NewPoint("nginx_log", tags, fields, v.TimeLocal)
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Errorf("NewPoint nginx_log error:%s",err.Error())
			//log.Fatal(err)
		}
		bp.AddPoint(pt)
		if err := c.Write(bp); err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Errorf("Write bp error:%s", err.Error())
			//log.Fatal(err)
		}
		TypeMonitorChan <- TypeWriteHandleLine

	}
}


func (l *LogProcess) Process() {
	//解析模块
	// 1 从Read Channel 中读取每行日志数据
	// 2 正则提取所需的监控数据 （path, status, method等）
	// 2 写入Write Channel
	// ([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)
	/**
	172.0.0.12 - - [04/Mar/2018:13:49:52 +0000] http "GET /foo?query=t HTTP/1.0" 200 2133 "-" "KeepAliveClient" "-" 1.005 1.854
   */
	//fmt.Println("Process:")
	// 编译正则表达式
	r := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)
	// 设置时区
	loc, _ := time.LoadLocation("Asia/Shanghai")
	for v := range l.rc {
		// 根据正则表达式去匹配字符串 返回一个字符串slice
		ret := r.FindStringSubmatch(string(v))
		if len(ret) != 14 {
			TypeMonitorChan <- TypeErrNum
			log.Errorln("FindStringSubmatch fail:", string(v))
			continue
		}
		message := &Message{}
		t, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Errorln("ParseInLocation fail:", err.Error(), ret[4])
		}
		message.TimeLocal = t
		byteSent, _ := strconv.Atoi(ret[8])
		message.BytesSent = byteSent
		reqSli := strings.Split(ret[6]," ")
		if len(reqSli) != 3 {
			TypeMonitorChan <- TypeErrNum
			log.Errorln("strings.Split fail", ret[6])
			continue
		}
		message.Method = reqSli[0]
		u, err := url.Parse(reqSli[1])
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Errorln("url parse fail:", err.Error())
			continue
		}
		message.Path = u.Path
		message.Scheme = ret[5]
		message.Status = ret[7]
		upstreamTime, _ := strconv.ParseFloat(ret[12], 64)
		requestTime , _ := strconv.ParseFloat(ret[13], 64)
		message.UpstreamTime = upstreamTime
		message.RequestTime = requestTime
		TypeMonitorChan <- TypeProcessHandleLine
		l.wc <- message
	}
}


func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Out = os.Stdout
	log.SetLevel( logrus.DebugLevel )
}

func main () {
	initEnv()
	var path, influxDsn,logPath string
	flag.StringVar(&path, "nginx_log_path", "./access.log","read config file")
	flag.StringVar(&logPath, "log_path","./log_monitor","log file path")
	flag.StringVar(&influxDsn, "influxDsn", "http://39.107.77.94:8086@yq@07030501310@tg_agency@s", "influxdb server address")
	flag.Parse()
	r := &ReadFromFile {
		path: path,
	}
	w := &WriteToInfluxDB {
		influxDBDsn: influxDsn,
	}
	lp := &LogProcess {
		rc:make(chan []byte, ReadChanCap),
		wc:make(chan *Message, WriteChanCap),
		read:r,
		write:w,
	}
	logFd, err := os.OpenFile( logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644 )
	defer logFd.Close()
	if err == nil {
		log.Out = logFd
	} else {
		log.Infof("open logFilePath err:%s", err.Error())
	}
	log.Infoln("Exec start.")
	for rc:=0; rc < ReadGoroutineNum; rc++ {
		go lp.read.Read(lp.rc)
	}
	for pc:=0; pc < ProcessGoroutineNum; pc++ {
		go lp.Process()
	}
	for wc:=0; wc < WriteGoroutineNum; wc++ {
		go lp.write.Write(lp.wc)
	}

	m := Monitor{
		startTime:time.Now(),
		data:SystemInfo{},
	}
	m.start(lp)
	//time.Sleep(1000 * time.Hour)
}

