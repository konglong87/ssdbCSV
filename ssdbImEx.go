package main

import (
	// "net/http"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"flag"
	"strings"
)

/**main()
*  程序运行的入口
*  命令行参数说明：
*    host: 数据库的IP 地址
*    p: 数据库的端口
 */
var (
	help    bool   //help帮助信息
	mode    string //工作模式：取值范围：import-导入，export-导出
	host    string //ssdb的 IP
	p       string //ssdb的端口
	g       string //机构编码
	t       string //表名称
	f       string //文件名称，含目录，例如：../sns.csv。
	k       string //组成 key 的字段，从第一列开始，个数是几就表示，key 由几列组成。它们在文件中的从左到右的顺序就是 key 的顺序。
	i       string //pse索引字段的组成，用逗号分隔的数列表示，例如：“2，3，6，7”，表示 csv 中从第一列开始数的：第 2，3，6，7列数据组成搜索引擎文本，其他列不参与索引
	pseIp   string //搜索引擎的 IP
	psePort string //搜索引擎的 Port
)

func main() {

	flag.Parse()
	if mode == "" {
		fmt.Println("   功能模式mode不能为空！输入-help/-h查看帮助信息")
		return
	}
	fmt.Println("===================================[check_param]=================================================")
	fmt.Println("=================================================================================================")
	for _, arg := range flag.Args() {
		fmt.Println("arg==", arg)
		if arg == "" || arg == "--" || arg == "-" {
			fmt.Println(arg, "不能为空\n")
			break
			return
		}
	}
	switch mode {
	case "import":
		if !checkImport() {
			return
		}
		ImportSsdb()
	case "export":
		if !checkExport() {
			return
		}
		exportToCsv(k)
	default:
		fmt.Println("           不支持的功能模式：[-mode=" + mode + "],输入-help/-h查看帮助信息")
		return
	}
		go func() {
			fmt.Println(".................正在导入，请稍等..............")
			fmt.Println(".................正在导入，请稍等..............")
		}()

	fmt.Println("====================================[time_over]==================================================")
	/*  默认csv文件的第一行 是  中文版本的column name，
				    第二行 是  英文版本的column name，
	   后面各行都是合法的记录。
	*/


}
func checkImport() bool {
	if flag.NFlag() < 10 || help {
		fmt.Println("			输入参数有误，请重输......")
		flag.Usage()
		return false
	}
	return true
}
func checkExport() bool {
	if flag.NFlag() < 5 || help {
		fmt.Println("			输入参数有误，请重输......")
		flag.Usage()
		return false
	}
	return true
}

func ImportSsdb() {
	// 解析csv
	file, err := ioutil.ReadFile(f)
		if err != nil {
				fmt.Println("err=======", err)
				return
		}
	r := csv.NewReader(strings.NewReader(string(file)))
	chineseName, err := r.Read()
	fmt.Println("中文列名：\n", chineseName)

	//keys是map的key
	keys, err := r.Read()
	fmt.Println("英文列名==\n", keys)
	count := 0
	fmt.Println("------------------------------------------------------------------------------")
	fmt.Println("------------------------------------------------------------------------------")
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		m := make(map[string]string)
		//sliM2 := make([]map[string]string,0)
		var sliM2 []map[string]string

		if len(record) < len(keys) { //用null补全缺省的字段
			sss := len(keys) - len(record)
			for s := 0; s < sss; s++ {
				record = append(record, "")
			}
		}
		for i := 0; i < len(keys); i++ {
			keym := keys[i]
			m[keym] = record[i]
			m2 := make(map[string]string)
			//fmt.Printf("===i===%d,key===%s", i, key)
			m2[keym] = record[i]
			sliM2 = append(sliM2, m2)
		}
		//fmt.Println("=======map========", m)
		//jsonStr, err := json.Marshal(m)
		jsonStr2, err := json.Marshal(sliM2)
		if err != nil {
			fmt.Println("Marshal错误:", err)
			return
			//panic(err)
		}
		//fmt.Println("========json串22222是：", string(jsonStr2))
		//组合key，pseText,向ssdb存储
		kInt, err := strconv.Atoi(k)
		if err != nil {
			fmt.Println("err======", err)
		}
		keySli := keys[:kInt] //key是前k列组成
		key := ""
		for _, keyM := range keySli {
			key += m[keyM]
		}
		//fmt.Println("key是：",key,"--VS--",record[:kInt])
		pse := ""
		pseText := ""
		pseSli := strings.Split(i, ",")
		for _, strIndex := range pseSli { //组成pse索引的字段，用逗号分隔的数列表示，例如：'2，3，6，7'，csv中从第一列开始
			index, _ := strconv.Atoi(strIndex)
			//fmt.Print("|index:",index)
			pseText = keys[index-1]
			//fmt.Print("|pseText:",pseText)
			pse += m[pseText]
			//fmt.Print("=m[pseText]=",m[pseText],"-VS-",record[index-1])
		}
		//fmt.Println("。。。。。pse是：",pse)
		//调用SaveToSsdbAndPse
		err = SaveToSsdbAndPse(key, pse, string(jsonStr2))
		if err != nil {
			fmt.Println("调用SaveToSsdbAndPse函数错误", err)
		}
		count++
	}

	fmt.Println("……………………………………………………总共导入数据", count, "行数据…………………………………………………………")
}

func init() {
	flag.StringVar(&mode, "mode", "", "-mode  选择工作模式：取值范围：import-导入，export-导出")
	flag.StringVar(&host, "host", "", "-host  选择连接的数据库ip，例如-dbip=172.16.1.18")
	flag.StringVar(&p, "p", "", "-p  连接数据库的端口号，例如-p=6088")
	flag.StringVar(&t, "t", "", "-t  要导入表的名称，例如-t=price_list")
	flag.StringVar(&g, "g", "", "-g  用户的机构编号，例如-g=10001")
	flag.StringVar(&f, "f", "", "-f  文件名称，含目录，例如：../sns.csv。")
	flag.StringVar(&k, "k", "", "-k  组成 key 的字段，从第一列开始，个数是几就表示，key 由几列组成。它们在文件中的从左到右的顺序就是 key 的顺序。")
	flag.StringVar(&i, "i", "", "-i  组成pse索引的字段，用逗号分隔的数列表示，例如：'2，3，6，7'，表示csv中从第一列开始数的：第 2，3，6，7列数据组成搜索引擎文本，其他列不参与索引")
	flag.StringVar(&pseIp, "pseIp", "", "-pseIp  搜索引擎的 IP")
	flag.StringVar(&psePort, "psePort", "", "-psePort 搜索引擎的 Port")
	flag.BoolVar(&help, "help", false, " -help 将会展示帮助信息和详细参数说明：  ")
	flag.Usage = use
}
