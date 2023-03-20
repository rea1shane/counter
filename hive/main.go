package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/beltran/gohive"
	"github.com/colinmarc/hdfs"
	"github.com/morikuni/failure"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"strings"
)

type Hive struct {
	Db       string
	Table    string
	Location string
	Size     int64
	Desc     string
}

func (h *Hive) String() string {
	return fmt.Sprintf("Database: %s\nTable: %s\nLocation: %s\nSize: %d bytes\nDescription: %s",
		h.Db, h.Table, h.Location, h.Size, h.Desc)
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

	// mysql
	mysqlDsn = ""

	// filter
	dbBlackList = []string{
		"stg_stream",
	}
)

// TODO 添加失败请求的 retry
// TODO 改为多线程

func main() {
	// hive
	hiveConnectConfiguration := gohive.NewConnectConfiguration()
	hiveConnectConfiguration.Username = hiveUsername
	hiveConnectConfiguration.Password = hivePassword

	hiveConnection, err := gohive.ConnectZookeeper(hiveZookeeperQuorum, "NONE", hiveConnectConfiguration)
	if err != nil {
		log.Fatal("创建 hive 连接失败: " + err.Error())
	}
	defer hiveConnection.Close()

	hiveCursor := hiveConnection.Cursor()
	defer hiveCursor.Close()

	// hdfs
	hadoopConf := hdfs.LoadHadoopConf(hadoopConfDir)
	namenodes, err := hadoopConf.Namenodes()
	if err != nil {
		log.Fatal("获取 NameNode 列表失败: " + err.Error())
	}

	hdfsClient, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: namenodes,
		User:      hdfsUsername,
	})
	if err != nil {
		log.Fatal("创建 hdfs 客户端失败: " + err.Error())
	}
	defer hdfsClient.Close()

	// mysql
	db, err := gorm.Open(mysql.Open(mysqlDsn), &gorm.Config{})
	if err != nil {
		log.Fatal("创建 MySQL 连接失败: " + err.Error())
	}
	err = db.AutoMigrate(&Hive{})
	if err != nil {
		log.Fatal("自动创建 MySQL 表失败: " + err.Error())
	}

	// fetch
	entities, err := fetch(hiveCursor)
	if err != nil {
		log.Fatal(fmt.Sprintf("%+v", err))
	}

	for _, entity := range entities {
		if strings.Contains(entity.Location, hdfsFlag) {
			size, err := getHdfsSize(hdfsClient, entity.Location)
			if err != nil {
				entity.Size = -1
				entity.Desc = err.Error()
				continue
			}
			entity.Size = size
		}
	}

	for _, entity := range entities {
		fmt.Println(entity)
		fmt.Println()
	}
}

func fetch(hiveCursor *gohive.Cursor) ([]*Hive, error) {
	var (
		entities []*Hive
		ctx      = context.Background()
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
				entities = append(entities, &Hive{
					Db:       db,
					Table:    table,
					Location: "",
					Size:     -1,
					Desc:     err.Error(),
				})
				continue
			}

			entities = append(entities, &Hive{
				Db:       db,
				Table:    table,
				Location: location,
			})
		}
	}

	return entities, err
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
