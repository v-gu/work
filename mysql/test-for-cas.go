package main

import "fmt"
import "os"
import "rand"
import mysql "github.com/Philio/GoMySQL"


const (
	HOST = "localhost:3306"
	USER = "crm_test"
	PASSWD = "crm_test"
	DB = "crm_test"
)


func newDBConn() *mysql.Client {
	client, err := mysql.DialTCP(HOST, USER, PASSWD, DB)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	
	return client
}

func query1(cli *mysql.Client) {
	loop_count := 100
	type row struct {
		// PK: (userid, roleid, server, date)
		userid		int32
		roleid		int32
		server		int8
		date		string
		name		string
	}
	rowgen := make(chan *row)

	go func() {
		for i := 0; i < loop_count; i++ {
			newrow := &row{}
			newrow.userid = rand.Int31n(100000)
			newrow.roleid = rand.Int31n(10000)
			newrow.server = int8(rand.Intn(100))
			newrow.date = "2011-06-26 10:20:24"
			newrow.name = "testname"
			
			rowgen <- newrow
		}
	}()
	
	fmt.Println("query1 start...")
	for {
		newrow := <-rowgen
		stmt, err := cli.Prepare("INSERT INTO role_info VALUES(?,?,?,?,?)")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		err = stmt.BindParams(newrow.userid, newrow.roleid, newrow.server,
			newrow.date, newrow.name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		err = stmt.Execute()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Println("query1 end.")
}

func main() {
	fmt.Println("Test start...")
	
	queries := 1
	workpipe := make(chan int)

	// init query1
	go func() {
		query1(newDBConn())
		workpipe <- 1
	} ()

	for i := 0; i < queries; i += <-workpipe { }

	// end
	fmt.Println("Test end.")
}

func mysql_init() {
	db, err := mysql.DialTCP("localhost:33060", "root", "", "crm_test")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	err = db.Query("update testtbl set idd = 14")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	err = db.Query("select * from testtbl")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	
	// Get result set  
	result, err := db.UseResult()  
	if err != nil {  
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)  
	}  

	for {
		row := result.FetchRow()  
		if row == nil { break }  
		fmt.Println(row)
	}
}

