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

var (
	DRYRUN_MODE = true
	VERBOSE_MODE = false
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
func queryRoleInfo1(cli *mysql.Client) {
	query_name := "[RoleInfo.query1]"
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
		if VERBOSE_MODE {
			fmt.Printf("%s INSERT IGNORE INTO role_info " +
				"VALUES(%d, %d, %d, %q, %q)\n",
				query_name, newrow.userid, newrow.roleid, newrow.server,
				newrow.date, newrow.name)
		} else {
			if i == 0 {
				fmt.Printf("%s INSERT IGNORE INTO role_info " +
					"VALUES(%d, %d, %d, %q, %q)\n",
					query_name, newrow.userid, newrow.roleid, newrow.server,
					newrow.date, newrow.name)
				fmt.Printf("%s ...(%d lines)\n", query_name, loop_count - 1)
			}
		}
		if !DRYRUN_MODE {
			cli.Start()
			stmt, err := cli.Prepare("INSERT IGNORE INTO role_info " +
				"VALUES(?,?,?,?,?)")
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
	}
	endtime := time.Nanoseconds()
	if !DRYRUN_MODE {
		fmt.Printf("%s ended. Averange query time: %d nanosecs.\n", query_name,
			(endtime-starttime)/((int64)(loop_count)))
	} else {
		fmt.Printf("%s ended.\n", query_name)
	}
}

/*
 Query2, DML(UPDATE) on role_info.
 */
func queryRoleInfo2(cli *mysql.Client) {
	query_name := "[RoleInfo.query2]"
	loop_count := 10
	fmt.Printf("%s started...\n", query_name)
	starttime := time.Nanoseconds()
	for i := 0; i < loop_count; i++ {
		if VERBOSE_MODE {
			fmt.Printf("%s UPDATE role_info SET NAME = %s " +
				"WHERE name IS NULL\n",
				query_name, "'testname'")
		} else {
			if i == 0 {
				fmt.Printf("%s UPDATE role_info SET NAME = %s " +
					"WHERE name IS NULL\n",
					query_name, "'testname'")
				fmt.Printf("%s ...(%d lines)\n", query_name, loop_count - 1)
			}
		}
		if !DRYRUN_MODE {
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
	}
	endtime := time.Nanoseconds()
	if !DRYRUN_MODE {
		fmt.Printf("%s ended. Averange query time: %d nanosecs.\n", query_name,
			(endtime-starttime)/((int64)(loop_count)))
	} else {
		fmt.Printf("%s ended.\n", query_name)
	}
}


/*
 Query3, DML(DELETE) on role_info by server and roleid.
 */
func queryRoleInfo3(cli *mysql.Client) {
	queryName := "[RoleInfo.query3]"
	roleCount := 1000
	fmt.Printf("%s started...\n", queryName)
	starttime := time.Nanoseconds()
	// retrieve records base
	if VERBOSE_MODE {
		fmt.Printf("%s SELECT server, roleid FROM role_info LIMIT %d",
			queryName, roleCount)
	}
	cli.Start()
	stmt, err := cli.Prepare(
		fmt.Sprintf("SELECT server, roleid FROM role_info LIMIT %d", roleCount))
	if err != nil {
		fmt.Fprintln(os.Stderr, queryName + err.String())
		return
	}
	err = stmt.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, queryName + err.String())
		return
	}
	err = stmt.StoreResult()
	if err != nil {
		fmt.Fprintln(os.Stderr, queryName + err.String())
		return
	}
	fmt.Printf("%s got records seed: %d\n", queryName, stmt.RowCount())
	// store records base
	type role struct {
		server int8
		roleid int32
	}
	var (
		roles = make([]role, 0, 1000)
		server int8
		roleid int32
	)
	err = stmt.BindResult(&server, &roleid)
	if err != nil {
		fmt.Fprintln(os.Stderr, queryName + err.String())
		return
	}
	for {
		eof, err := stmt.Fetch(); 
		if err != nil {
			fmt.Fprintln(os.Stderr, queryName + err.String())
			continue
		}
		if eof { break }
		role := &role{server, roleid}
		roles = append(roles, *role)
	}
	stmt.FreeResult()
	stmt.Reset()
	var i int = 0;
	var rolesCount = len(roles)
	for _, role := range roles {
		if VERBOSE_MODE {
			fmt.Printf("%s DELETE FROM role_info WHERE server=%d " +
				"AND roleid=%d\n",
				queryName, role.server, role.roleid)
		} else {
 			if i == 0 {
				fmt.Printf("%s DELETE FROM role_info WHERE server=%d AND " +
					"roleid=%d\n",
					queryName, role.server, role.roleid)
				fmt.Printf("%s ...(%d lines)\n", queryName, rolesCount - 1)
			}
			i++
		}
		if !DRYRUN_MODE {
			err := stmt.Prepare("DELETE FROM role_info WHERE server=? " +
				"AND roleid=?")
			if err != nil {
				fmt.Fprintln(os.Stderr, queryName + err.String())
				os.Exit(1)
			}
			err = stmt.BindParams(role.server, role.roleid)
			if err != nil {
				fmt.Fprintln(os.Stderr, queryName + err.String())
				os.Exit(1)
			}
			err = stmt.Execute()
			if err != nil {
				fmt.Fprintln(os.Stderr, queryName + err.String())
				os.Exit(1)
			}
		}
	}
	endtime := time.Nanoseconds()
	if !DRYRUN_MODE {
		fmt.Printf("%s ended. Averange query time: %d nanosecs.\n", queryName,
			(endtime-starttime)/((int64)(roleCount)))
	} else {
		fmt.Printf("%s ended.\n", queryName)
	}
}


func queryRoleLogin1(cli *mysql.Client) {
	queryName := "[RoleLogin.query1]"
	loopCount := 1000
	type row struct {
		// PK: (date, server, roleid)
		date		string
		server		int8
		roleid		int64
		money		int32
	}
	fmt.Println(queryName, "started...")
	startTime := time.Nanoseconds()
	for i := 0; i < loopCount; i++ {
		newRow := &row{}
		newRow.date = fmt.Sprintf("2011-%02d-%02d %02d:%02d:%02d",
			rand.Intn(12) + 1,	// month
			rand.Intn(30) + 1,	// day
			rand.Intn(24),		// hour
			rand.Intn(60),		// minute
			rand.Intn(60))		// second
		newRow.server = int8 (rand.Intn(127))
		newRow.roleid = rand.Int63()
		newRow.money = rand.Int31()
		if VERBOSE_MODE {
			fmt.Printf("%s INSERT IGNORE INTO role_login VALUES(%s, %d, %d, %d)\n",
				queryName, newRow.date, newRow.server, newRow.roleid, newRow.money)
		} else {
			if i == 0 {
				fmt.Printf("%s INSERT IGNORE INTO role_login " +
					"VALUES(%s, %d, %d, %d)\n",
					queryName, newRow.date, newRow.server,
					newRow.roleid, newRow.money)
				fmt.Printf("%s ...(%d lines)\n",
					queryName, loopCount - 1)
			}
		}
		if !DRYRUN_MODE {
			cli.Start()
			stmt, err := cli.Prepare("INSERT IGNORE INTO role_login " +
				"VALUES(?,?,?,?)")
			if err != nil {
				fmt.Fprintln(os.Stderr, queryName + err.String())
				os.Exit(1)
			}
			err = stmt.BindParams(newRow.date, newRow.server, newRow.roleid,
				newRow.money)
			if err != nil {
				fmt.Fprintln(os.Stderr, queryName + err.String())
				os.Exit(1)
			}
			err = stmt.Execute()
			if err != nil {
				fmt.Fprintln(os.Stderr, queryName + err.String())
				os.Exit(1)
			}
			cli.Rollback()
		}
		endTime := time.Nanoseconds()
		if !DRYRUN_MODE {
			fmt.Printf("%s ended. Averange query time: %d nanosecs.\n", queryName,
				(endTime-startTime)/((int64)(loopCount)))
		} else {
			fmt.Printf("%s ended.\n", queryName)
		}
	}
}


func main() {
	fmt.Println("Test suite started...")

	fmt.Println("indivisual test:")
	// queryRoleInfo1(newDBConn())
	// queryRoleInfo2(newDBConn())
	// queryRoleInfo3(newDBConn())
	queryRoleLogin1(newDBConn())
	return
	
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
