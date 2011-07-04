package main

import "fmt"
import "os"
import mysql "github.com/Philio/GoMySQL"


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

func query1() {
	
}

func main() {
	fmt.Println("mysql library test")
	
	mysql_init()
}

