#!/bin/bash
set -e

# 데이터베이스 초기화
airflow db init

# 데이터베이스 마이그레이션
airflow db migrate

# 관리자 사용자 생성 (이미 존재할 경우 무시)
airflow users create --username airflow --firstname airflow --lastname airflow --role Admin --email airflow@gmail.com --password airflow || true

# 스케줄러 시작
exec airflow scheduler
