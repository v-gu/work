package main

import "fmt"
import "os"
import "rand"
import mysql "github.com/Philio/GoMySQL"


const (
	HOST := "localhost:33060"
	USER := "root"
	PASSWD := ""
	DB := "crm_test"
)


func newDBConn() *Client {
	client, err := mysql.DialTCP(HOST, USER, PASSWD, DB)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	
	return client
}

func query1(cli *Client) {
	loop_count := 100
	type row struct {
		// PK: (userid, roleid, server, date)
		userid		uint32
		roleid		uint32
		server		uint8
		date		string
		name		string
	}
	rowgen := make(chan row)

	go func() {
		for i := 0; i < loop_count; i++ {
			newrow := new(row)
			newrow.userid = rand.Int31n(100000)
			newrow.roleid = rand.Int31n(10000)
			newrow.server = rand.Intn(100)
			newrow.date = "2011-06-26 10:20:24"
			newrow.name = "testname"
			
			rowgen <- newrow
		}
	}()

	for {
		fmt.Println("query1 start...")
		newrow := <-rowgen
		stmt, err := cli.Prepare("INSERT INTO role_info VALUES(?,?,?,?,?)")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		err = stmt.BindParams(newrow.userid, newrow.rowid, newrow.server,
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
		fmt.Println("query1 end.")
	}
}

func main() {
	fmt.Println("Test start...")
	
	go query1(newDBConn())

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

