#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER test WITH PASSWORD 'postgres';
	CREATE DATABASE twitter;
	CREATE DATABASE twitter_staging;

	CREATE TABLE twitter.tweet (
		id VARCHAR(20) PRIMARY KEY,
		text VARCHAR(500),
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

	CREATE TABLE twitter_staging.tweet (
		id VARCHAR(20),
		text VARCHAR(500),
		created_at TIMESTAMP,
		author_id VARCHAR(20),
		conversation_id VARCHAR(20),
		in_reply_to_user_id VARCHAR(20),
		possibly_sensitive BOOLEAN,
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

	GRANT ALL PRIVILEGES ON DATABASE twitter TO test;
	GRANT ALL PRIVILEGES ON DATABASE twitter_staging TO test;
EOSQL