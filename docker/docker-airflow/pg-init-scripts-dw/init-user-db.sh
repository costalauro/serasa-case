#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "dwhdb" --dbname "dwhdb" <<-EOSQL		

	CREATE SCHEMA twitter;

	CREATE TABLE twitter.tweet (
		id VARCHAR(20) PRIMARY KEY,
		text VARCHAR(10000),
		created_at TIMESTAMP,
		author_id VARCHAR(20),
		conversation_id VARCHAR(20),
		in_reply_to_user_id VARCHAR(20),		
		like_count BIGINT,
		quote_count BIGINT,
		reply_count BIGINT,
		processed_at varchar(30),
		updated_at TIMESTAMP NOT NULL DEFAULT NOW()
	);

	CREATE TABLE twitter.user (
		id VARCHAR(20) PRIMARY KEY,
		name VARCHAR(50),
		username VARCHAR(15),
		created_at TIMESTAMP,				
		processed_at varchar(30),
		updated_at TIMESTAMP NOT NULL DEFAULT NOW()
	);	

	CREATE SCHEMA twitter_staging;

	CREATE TABLE twitter_staging.tweet (
		id VARCHAR(20),
		text VARCHAR(10000),
		created_at TIMESTAMP,
		author_id VARCHAR(20),
		conversation_id VARCHAR(20),
		in_reply_to_user_id VARCHAR(20),
		like_count BIGINT,
		quote_count BIGINT,
		reply_count BIGINT,
		processed_at varchar(30)
	);

	CREATE TABLE twitter_staging.user (
		id VARCHAR(20),
		name VARCHAR(50),
		username VARCHAR(15),
		created_at TIMESTAMP,
		processed_at varchar(30)
	);
	
EOSQL
