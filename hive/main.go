package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/beltran/gohive"
	"github.com/morikuni/failure"
	"log"
	"strings"
)

type tableInfo struct {
	db       string
	name     string
	location string
	size     int
}

type errorTableInfo struct {
	db   string
	name string
	err  error
}

var (
	zookeeperQuorum = "common1:2181,common2:2181,common3:2181"
	username        = "ods"
	password        = ""

	dbBlackList = []string{
		"stg_stream",
	}
)

// todo 添加请求的 retry
// todo 改为多线程

func main() {
	configuration := gohive.NewConnectConfiguration()
	configuration.Username = username
	configuration.Password = password

	connect, err := gohive.ConnectZookeeper(zookeeperQuorum, "NONE", configuration)
	if err != nil {
		log.Fatal("创建客户端错误: " + err.Error())
	}
	defer connect.Close()

	cursor := connect.Cursor()
	defer cursor.Close()

	tableInfos, errorTableInfos, err := fetch(cursor)
	if err != nil {
		log.Fatal(fmt.Sprintf("%+v", err))
	}

	fmt.Println(fmt.Sprintf("%+v", tableInfos))
	fmt.Println(fmt.Sprintf("%+v", errorTableInfos))
}

func fetch(cursor *gohive.Cursor) (tableInfos []tableInfo, errorTableInfos []errorTableInfo, err error) {
	var (
		ctx = context.Background()
	)

	dbs, err := listDbs(ctx, cursor)
	if err != nil {
		return nil, nil, err
	}

	for _, db := range dbs {
		if inBlackList(db) {
			continue
		}

		tables, err := listTables(ctx, cursor, db)
		if err != nil {
			return nil, nil, err
		}

		for _, table := range tables {
			location, err := getLocation(ctx, cursor, db, table)
			if err != nil {
				errorTableInfos = append(errorTableInfos, errorTableInfo{
					db:   db,
					name: table,
					err:  err,
				})
				continue
			}

			// todo 获取大小

			tableInfos = append(tableInfos, tableInfo{
				db:       db,
				name:     table,
				location: location,
				size:     0,
			})
		}
	}

	return
}

func listDbs(ctx context.Context, cursor *gohive.Cursor) (dbs []string, err error) {
	cursor.Exec(ctx, "SHOW DATABASES")
	if cursor.Err != nil {
		err = failure.Wrap(cursor.Err)
		return
	}

	var db string
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &db)
		if cursor.Err != nil {
			err = failure.Wrap(cursor.Err)
			return
		}
		dbs = append(dbs, db)
	}

	return
}

func listTables(ctx context.Context, cursor *gohive.Cursor, db string) (tables []string, err error) {
	cursor.Exec(ctx, "USE "+db)
	if cursor.Err != nil {
		err = failure.Wrap(cursor.Err)
		return
	}
	cursor.Exec(ctx, "SHOW TABLES")
	if cursor.Err != nil {
		err = failure.Wrap(cursor.Err)
		return
	}

	var table string
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &table)
		if cursor.Err != nil {
			err = failure.Wrap(cursor.Err)
			return
		}
		tables = append(tables, table)
	}

	return
}

func getLocation(ctx context.Context, cursor *gohive.Cursor, db, table string) (location string, err error) {
	cursor.Exec(ctx, "SHOW CREATE TABLE "+db+"."+table)
	if cursor.Err != nil {
		err = failure.Wrap(cursor.Err)
		return
	}

	var createSql string
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &createSql)
		if cursor.Err != nil {
			err = failure.Wrap(cursor.Err)
			return
		}
		if createSql == "LOCATION" {
			cursor.FetchOne(ctx, &location)
			if cursor.Err != nil {
				err = failure.Wrap(cursor.Err)
				return
			}
			break
		}
	}

	if location == "" {
		err = errors.New("have no location")
		return
	}

	location = strings.Split(location, "'")[1]
	return
}

func getSize(path string) (size int, err error) {
	return
}

func inBlackList(db string) bool {
	for _, s := range dbBlackList {
		if db == s {
			return true
		}
	}
	return false
}
