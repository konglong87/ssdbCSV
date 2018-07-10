package main

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"fmt"
	"bytes"
	"errors"
	"strconv"
	"gossdb/ssdb"
	"os"
	"flag"
)


//1.增加seText参数(索引文本)
//2.向ssdb保存数据
func  SaveToSsdbAndPse(key string, seText string,data string) error {
	//fmt.Println("这是向1-ssdb保存数据 2-索引文本的方法")
	//获取数据库连接
	Porti,_ := strconv.Atoi(p)
	db, err := ssdb.Connect(host, Porti)
	if err != nil {
		//错误处理
		errMsg := "    open ssdb failed:" + err.Error() + "\n"
		return errors.New(errMsg)
	}
	defer db.Close()
	//向数据库插入新数据
	result,err := db.Set(key, data)
	if result != true {
		fmt.Println("result:",result)
	}
	//fmt.Println("result:",result)
	if err != nil {
		return err
	}
	//新增索引
	/*
		   需要的参数：
			com             服务区分标识   com=index
			indexOperation  索引操作类型   indexOperation=POST
			key             需要增加索引的key-value对中的key
			value           需要增加索引的key-value对中的value
	*/
	//realData := strings.Replace(data, "\"", "", -1)
	addValue := "{\"com\":\"index\",\"data\":{\"indexOperation\":\"POST\",\"tableName\":\"" + t + "\",\"key\":\"" + key + "\",\"value\":\"" + seText + "\"}}"
	var value = []byte(addValue)
	////pseResp,err3:= http.Post("http://172.16.0.14:31007/pse", "application/json; charset=utf-8", bytes.NewBuffer(pseRcptStr))
	resp, err2 := http.Post("http://"+pseIp+":"+psePort+"/pse", "application/json;charset=utf-8", bytes.NewBuffer(value))
	if err2 != nil {
		fmt.Println("err:", err2)
		return err2
	}
	//fmt.Println("http.Post成功，tableName====", t)
	//fmt.Println("http.Post成功，seText====", seText)
	defer resp.Body.Close()
	return nil
}
//export func
func exportToCsv(key string){
	fmt.Println("=================================[function-export]===============================================")
	// [1]--firstly,we get data from ssdb according key from flag key ("-k")
	pInt ,_ := strconv.Atoi(p)
	db,err := ssdb.Connect(host, pInt)//connect to ssdb
	if err != nil {
		fmt.Println("{************}open ssdb err:",err)
	}
	//	args := "scan ",key+"! "+key+"~ 999999"
	data , err := db.Do("scan",key+"!",key+"~",999999) //get data by scan command
	if err != nil {
		fmt.Println("{************}get data from ssdb err:",err)
	}
	
	data = data[1:] //去掉ok第一个字符串
	//fmt.Println("得到的数据是：：：",data)
	if data == nil || len(data) == 0{
		fmt.Println("{************}Data is Null,please try other key...")
		return
	}
	//[2] format data to [][]map[string]string 
	var dataFormat [][]map[string]string
	var MapSli []map[string]string
	for k,dataLine := range data{//反序列化data
	//fmt.Println("=====dataLine=====",dataLine)
		if k%2 == 0{ //跳过结果集大json串中的key
			continue
		}
		err = json.Unmarshal([]byte(dataLine), &MapSli)
			if err != nil {
				fmt.Println("{!!!}Unmarshal data  err:",err)
				continue
			}
		dataFormat = append(dataFormat,MapSli)
	}
	//fmt.Println("dataForMat======[][][][][]",dataFormat)
	
	firstLine := dataFormat[0] // 获取第一行数据
	var columnsName []string
	var contentFile []string
	for _,oneMap := range firstLine {
		for k,v := range oneMap{
			columnsName = append(columnsName,k)
			contentFile = append(contentFile,v)//第一行的值~数据
		}
	}
		//fmt.Println("columnsName======[][][][][]",columnsName)
	columns := len(columnsName) //列的个数，字段的个数
 	//[3] start writing to csv file 
	fileName := f //文件名称，含目录，例如：../sns.csv。
	rwFile , err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)//create file
	rwFile.WriteString("\xEF\xBB\xBF") // 写入UTF-8 BOM
	defer rwFile.Close() 
	if err != nil {
		fmt.Println("创建文件出现error:",err)
	}
	csvWriter := csv.NewWriter(rwFile)//create a csv input writer
	var spaceSli []string //第一行全是空格，方便写中文名称
	for ii := 0; ii < columns ;ii ++ {
		spaceSli = append(spaceSli,"")
	}
	
	err = csvWriter.Write(spaceSli)//第一行，写入空格
			checkErr(err)
	err = csvWriter.Write(columnsName)//第二行，写入英文列名
			checkErr(err)
	err = csvWriter.Write(contentFile)//第三行，写入查询出来的第一行值内容
			checkErr(err)
//...................................................................................................
//....................................前三行写入之后...................................................
//...................................................................................................
		dataFormat = dataFormat[1:]
		var allDataLeft [][]string  //剩下的所有的数据
	for _,dataLines := range dataFormat {
		var contentLine []string
		for _,aMap := range dataLines {
			for _,v := range aMap{
				contentLine = append(contentLine,v)//每行的值~数据
			}
		}
		allDataLeft = append(allDataLeft,contentLine)
	}
	csvWriter.WriteAll(allDataLeft)//写入剩下的所有的数据
	csvWriter.Flush()
}
	func checkErr (err error ){
		if err != nil {
			fmt.Println("***错误信息是：",err)
		}
	}
//usage 用法
func use() {
	fmt.Fprintf(os.Stderr, 
	`[**********************************************************************^_^******************************************************************************]
[**********************************************************************^_^******************************************************************************]
    XXXX导入csv文件程序（version 2.0.1）:
    Usage of ssdbImEx : 		
    ***[-help 帮助信息] [-t 表名称名][-p ssdb端口号] [-k 组成key的字段] [-i 组成pse索引的字段]
    ***[-mode 工作模式] [-f 文件名称][-h ssdb的 IP] [-g 机构编码] [-psePort 搜索引擎的 Port]  
    ***
Arguments(参数详细说明如下):

`)
	flag.PrintDefaults()
}
