package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/beltran/gohive"
	"github.com/colinmarc/hdfs"
	"github.com/morikuni/failure"
	"log"
	"strings"
)

type hiveTableStorageInfo struct {
	db       string
	table    string
	location string
	size     int64
	desc     string
}

func (info *hiveTableStorageInfo) String() string {
	return fmt.Sprintf("Database: %s\nTable: %s\nLocation: %s\nSize: %d bytes\nDescription: %s",
		info.db, info.table, info.location, info.size, info.desc)
}

const (
	hdfsFlag = "hdfs://"
)

var (
	// hive
	hiveZookeeperQuorum = "common1:2181,common2:2181,common3:2181"
	hiveUsername        = "ods"
	hivePassword        = ""

	// hdfs
	hadoopConfDir = "/etc/hadoop/conf"
	hdfsUsername  = "ods"

	// filter
	dbBlackList = []string{
		"stg_stream",
	}
)

// TODO 添加错误请求的 retry
// TODO 改为多线程

func main() {
	// hive
	hiveConnectConfiguration := gohive.NewConnectConfiguration()
	hiveConnectConfiguration.Username = hiveUsername
	hiveConnectConfiguration.Password = hivePassword

	hiveConnection, err := gohive.ConnectZookeeper(hiveZookeeperQuorum, "NONE", hiveConnectConfiguration)
	if err != nil {
		log.Fatal("创建 hive 连接错误: " + err.Error())
	}
	defer hiveConnection.Close()

	hiveCursor := hiveConnection.Cursor()
	defer hiveCursor.Close()

	// hdfs
	hadoopConf := hdfs.LoadHadoopConf(hadoopConfDir)
	namenodes, err := hadoopConf.Namenodes()
	if err != nil {
		log.Fatal("获取 NameNode 列表错误: " + err.Error())
	}

	hdfsClient, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: namenodes,
		User:      hdfsUsername,
	})
	if err != nil {
		log.Fatal("创建 hdfs 客户端错误: " + err.Error())
	}
	defer hdfsClient.Close()

	// fetch
	infos, err := fetch(hiveCursor)
	if err != nil {
		log.Fatal(fmt.Sprintf("%+v", err))
	}

	for _, info := range infos {
		if strings.Contains(info.location, hdfsFlag) {
			size, err := getHdfsSize(hdfsClient, info.location)
			if err != nil {
				info.size = -1
				info.desc = err.Error()
				continue
			}
			info.size = size
		}
	}

	for _, info := range infos {
		fmt.Println(info)
		fmt.Println()
	}
}

func fetch(hiveCursor *gohive.Cursor) ([]*hiveTableStorageInfo, error) {
	var (
		infos []*hiveTableStorageInfo
		ctx   = context.Background()
	)

	dbs, err := listDbs(ctx, hiveCursor)
	if err != nil {
		return nil, err
	}

	for _, db := range dbs {
		if inBlackList(db) {
			continue
		}

		tables, err := listTables(ctx, hiveCursor, db)
		if err != nil {
			return nil, err
		}

		for _, table := range tables {
			location, err := getLocation(ctx, hiveCursor, db, table)
			if err != nil {
				infos = append(infos, &hiveTableStorageInfo{
					db:       db,
					table:    table,
					location: "",
					size:     -1,
					desc:     err.Error(),
				})
				continue
			}

			infos = append(infos, &hiveTableStorageInfo{
				db:       db,
				table:    table,
				location: location,
			})
		}
	}

	return infos, err
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
		err = failure.Wrap(errors.New("have no location"))
		return
	}

	location = strings.Split(location, "'")[1]
	return
}

func getHdfsSize(client *hdfs.Client, location string) (size int64, err error) {
	path := parseHdfsLocation(location)
	summary, err := client.GetContentSummary(path)
	if err != nil {
		err = failure.Wrap(err)
		return
	}
	size = summary.Size()
	fmt.Println(size)
	return
}

func parseHdfsLocation(location string) string {
	// TODO 这里现在返回的是 path，后续还可以解析出来 hdfs 集群名称
	return "/" + strings.SplitN(strings.Split(location, hdfsFlag)[1], "/", 2)[1] + "/"
}

func inBlackList(db string) bool {
	for _, s := range dbBlackList {
		if db == s {
			return true
		}
	}
	return false
}
