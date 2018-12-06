CREATE KEYSPACE IF NOT EXISTS ecommercelog
WITH replication = {
'class': 'SimpleStrategy', 'replication_factor': 1
};

USE ecommercelog;

CREATE TABLE IF NOT EXISTS page_views (page VARCHAR, count COUNTER, PRIMARY KEY(page));
CREATE TABLE IF NOT EXISTS log_volume_by_minute (timestamp VARCHAR, count COUNTER, PRIMARY KEY(timestamp));
CREATE TABLE IF NOT EXISTS status_counter (status_code VARCHAR, count COUNTER, PRIMARY KEY(status_code));
CREATE TABLE IF NOT EXISTS visits_by_country (country VARCHAR, count COUNTER, PRIMARY KEY(country));
CREATE TABLE IF NOT EXISTS referrer_counter (referrer VARCHAR, count COUNTER, PRIMARY KEY(referrer));
CREATE TABLE IF NOT EXISTS kafka_offset (partition int, offset bigint, PRIMARY KEY(partition));
