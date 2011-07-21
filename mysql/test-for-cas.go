package main

import "fmt"
import "os"
import "rand"
import "time"
//import "strconv"
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


/*
 Query1, DML(INSERT) on role_info.
 */
func query1(cli *mysql.Client) {
	query_name := "[query1]"
	loop_count := 1000
	type row struct {
		// PK: (userid, roleid, server, date)
		userid		int32
		roleid		int32
		server		int8
		date		string
		name		string
	}
	
	fmt.Printf("%s started...\n", query_name)
	starttime := time.Nanoseconds()
	for i := 0; i < loop_count; i++ {
		newrow := &row{}
		newrow.userid = rand.Int31n(100000)
		newrow.roleid = rand.Int31n(10000)
		newrow.server = int8(rand.Intn(100))
		newrow.date = "2011-06-26 10:20:24"
		newrow.name = "testname"

		cli.Start()
		stmt, err := cli.Prepare("INSERT IGNORE INTO " +
			"role_info VALUES(?,?,?,?,?)")
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		err = stmt.BindParams(newrow.userid, newrow.roleid, newrow.server,
			newrow.date, newrow.name)
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		err = stmt.Execute()
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		cli.Rollback()
	}
	endtime := time.Nanoseconds()
	fmt.Printf("%s ended. Averange query time: %d nanosecs.\n", query_name,
		(endtime-starttime)/((int64)(loop_count)))
}

/*
 Query2, DML(UPDATE) on role_info.
 */
func query2(cli *mysql.Client) {
	query_name := "[query2]"
	loop_count := 10

	fmt.Printf("%s started...\n", query_name)
	starttime := time.Nanoseconds()
	for i := 0; i < loop_count; i++ {

		cli.Start()
		stmt, err := cli.Prepare("UPDATE role_info SET NAME = ? " +
			"WHERE name IS NULL")
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		err = stmt.BindParams("'testname'")
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		err = stmt.Execute()
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		cli.Rollback()
	}
	endtime := time.Nanoseconds()
	fmt.Printf("%s ended. Averange query time: %d nanosecs.\n", query_name,
		(endtime-starttime)/((int64)(loop_count)))
}


/*
 Query3, DML(DELETE) on role_info.
 */
func query3(cli *mysql.Client) {
	query_name := "[query3]"
	loop_count := 1000

	fmt.Printf("%s started...\n", query_name)
	starttime := time.Nanoseconds()
	for i := 0; i < loop_count; i++ {

		cli.Start()
		stmt, err := cli.Prepare("DELETE FROM role_info SET NAME = ? " +
			"WHERE name IS NULL")
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		err = stmt.BindParams("'testname'")
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		err = stmt.Execute()
		if err != nil {
			fmt.Fprintln(os.Stderr, query_name + err.String())
			os.Exit(1)
		}
		cli.Rollback()
	}
	endtime := time.Nanoseconds()
	fmt.Printf("%s ended. Averange query time: %d nanosecs.\n", query_name,
		(endtime-starttime)/((int64)(loop_count)))
}


func main() {
	fmt.Println("Test suite started...")

	fmt.Println("indivisual test:")
	// query1(newDBConn())
	// query2(newDBConn())
	// query3(newDBConn())
	
	queries := 2
	workpipe := make(chan int)

	// // do query1
	// go func() {
	// 	query1(newDBConn())
	// 	workpipe <- 1
	// } ()

	// // do query2
	// go func() {
	// 	query2(newDBConn())
	// 	workpipe <- 1
	// } ()

	

	for i := 0; i < queries; i += <-workpipe { }
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

