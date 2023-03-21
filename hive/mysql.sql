CREATE TABLE IF NOT EXISTS `hive` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `db` VARCHAR(128) NOT NULL COMMENT '库名',
    `table` VARCHAR(128) NOT NULL COMMENT '表名',
    `location` VARCHAR(4000) NOT NULL DEFAULT "" COMMENT '路径，为空代表没有路径',
    `size` BIGINT NOT NULL DEFAULT -1 COMMENT '占用存储空间大小，单位 bytes，-1 表示没有路径或者获取路径错误',
    `desc` VARCHAR(4096) NOT NULL DEFAULT "" COMMENT '备注',
    `date` DATE COMMENT '抓取数据时间',
    PRIMARY KEY (`id`),
    KEY `record` (`db`, `table`, `date`)
) ENGINE = InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4;
