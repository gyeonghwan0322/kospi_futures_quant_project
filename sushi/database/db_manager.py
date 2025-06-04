import os
import yaml
import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Union
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime, date

logger = logging.getLogger(__name__)


class DBManager:
    """데이터베이스 관리자 클래스

    PostgreSQL 데이터베이스 연결 관리 및
    피처 데이터 저장을 담당합니다.
    """

    def __init__(self, db_config_path: str = "config/db_config.yaml"):
        """DBManager 생성자

        Args:
            db_config_path: 데이터베이스 설정 파일 경로
        """
        self.db_config = self._load_db_config(db_config_path)
        self.conn = None
        self.cursor = None

    def _load_db_config(self, config_path: str) -> Dict[str, Any]:
        """데이터베이스 설정 파일 로드

        Args:
            config_path: 설정 파일 경로

        Returns:
            데이터베이스 설정 정보
        """
        try:
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                logger.info(f"Loaded database configuration from {config_path}")
                return config
            else:
                logger.warning(f"Database config file not found: {config_path}")
                # 기본 설정
                return {
                    "host": "localhost",
                    "port": 5432,
                    "database": "ignacio",
                    "user": "postgres",
                    "password": "postgres",
                }
        except Exception as e:
            logger.error(f"Error loading database configuration: {str(e)}")
            raise

    def connect(self) -> None:
        """데이터베이스 연결"""
        try:
            if self.conn is None or self.conn.closed:
                self.conn = psycopg2.connect(
                    host=self.db_config.get("host", "localhost"),
                    port=self.db_config.get("port", 5432),
                    database=self.db_config.get("database", "ignacio"),
                    user=self.db_config.get("user", "postgres"),
                    password=self.db_config.get("password", "postgres"),
                )
                self.cursor = self.conn.cursor()
                logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    def disconnect(self) -> None:
        """데이터베이스 연결 종료"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn and not self.conn.closed:
                self.conn.close()
                logger.info("Disconnected from PostgreSQL database")
        except Exception as e:
            logger.error(f"Error disconnecting from database: {str(e)}")

    def create_schema_if_not_exists(self, schema_name: str = "ignacio") -> None:
        """스키마 생성 (없는 경우)

        Args:
            schema_name: 스키마 이름
        """
        try:
            self.connect()
            self.cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(schema_name)
                )
            )
            self.conn.commit()
            logger.info(f"Schema '{schema_name}' created or already exists")
        except Exception as e:
            logger.error(f"Error creating schema '{schema_name}': {str(e)}")
            raise

    def create_table_if_not_exists(
        self,
        table_name: str,
        schema_name: str = "ignacio",
        columns: Dict[str, str] = None,
    ) -> None:
        """테이블 생성 (없는 경우)

        Args:
            table_name: 테이블 이름
            schema_name: 스키마 이름
            columns: 컬럼 정의 (이름: 타입)
        """
        if columns is None:
            # 피처 타입에 따른 기본 컬럼 정의
            if "daily" in table_name or "investor" in table_name:
                columns = {
                    "id": "SERIAL PRIMARY KEY",
                    "code": "VARCHAR(20)",
                    "date": "DATE",
                    "data": "JSONB",
                    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                }
            elif "minute" in table_name:
                columns = {
                    "id": "SERIAL PRIMARY KEY",
                    "code": "VARCHAR(20)",
                    "date": "DATE",
                    "time": "TIME",
                    "data": "JSONB",
                    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                }
            elif "options" in table_name:
                columns = {
                    "id": "SERIAL PRIMARY KEY",
                    "code": "VARCHAR(20)",
                    "date": "DATE",
                    "time": "TIME",
                    "strike": "NUMERIC",
                    "call_put": "VARCHAR(4)",
                    "data": "JSONB",
                    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                }
            elif "cot" in table_name:
                columns = {
                    "id": "SERIAL PRIMARY KEY",
                    "code": "VARCHAR(20)",
                    "date": "DATE",
                    "data": "JSONB",
                    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                }
            else:
                columns = {
                    "id": "SERIAL PRIMARY KEY",
                    "code": "VARCHAR(20)",
                    "date": "DATE",
                    "data": "JSONB",
                    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                }

        try:
            self.connect()
            # 스키마 생성
            self.create_schema_if_not_exists(schema_name)

            # 컬럼 정의 구성
            columns_sql = [
                sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type))
                for col_name, col_type in columns.items()
            ]

            # 테이블 생성 SQL
            create_table_sql = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    {}
                )
            """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.SQL(", ").join(columns_sql),
            )

            self.cursor.execute(create_table_sql)
            self.conn.commit()
            logger.info(f"Table '{schema_name}.{table_name}' created or already exists")
        except Exception as e:
            logger.error(f"Error creating table '{schema_name}.{table_name}': {str(e)}")
            if self.conn:
                self.conn.rollback()
            raise

    def save_daily_price_data(
        self, table_name: str, data: Dict[str, Any], schema_name: str = "ignacio"
    ) -> None:
        """일봉 가격 데이터 저장

        Args:
            table_name: 테이블 이름
            data: 데이터 (코드별 딕셔너리)
            schema_name: 스키마 이름
        """
        try:
            self.connect()
            self.create_table_if_not_exists(table_name, schema_name)

            for code, code_data in data.items():
                # pandas DataFrame 타입 체크 (비어있는지 확인)
                if hasattr(code_data, "empty") and code_data.empty:
                    logger.warning(f"Empty DataFrame for code '{code}', skipping")
                    continue
                elif code_data is None:
                    logger.warning(f"No data for code '{code}', skipping")
                    continue

                # pandas DataFrame인 경우 records 형식으로 변환
                if hasattr(code_data, "to_dict"):
                    items = code_data.to_dict("records")
                # 데이터 형태 변환 (출력2 데이터 포맷)
                elif (
                    isinstance(code_data, dict)
                    and "output2" in code_data
                    and isinstance(code_data["output2"], list)
                ):
                    items = code_data["output2"]
                else:
                    items = code_data if isinstance(code_data, list) else [code_data]

                # 데이터 삽입 준비
                values = []
                for item in items:
                    # pandas Timestamp 객체를 문자열로 변환 (JSON 직렬화를 위해)
                    item_copy = self._serialize_item(item)

                    # 날짜 추출 (YYYYMMDD 형식)
                    stck_bsop_date = item.get("stck_bsop_date")
                    if stck_bsop_date:
                        # pandas Timestamp 처리
                        if hasattr(stck_bsop_date, "strftime"):
                            item_date = stck_bsop_date.date()
                        # 날짜 객체로 변환
                        else:
                            try:
                                item_date = date(
                                    year=int(stck_bsop_date[:4]),
                                    month=int(stck_bsop_date[4:6]),
                                    day=int(stck_bsop_date[6:8]),
                                )
                            except (ValueError, TypeError):
                                logger.warning(
                                    f"Invalid date format: {stck_bsop_date}, using current date"
                                )
                                item_date = date.today()
                    else:
                        item_date = date.today()

                    values.append((code, item_date, item_copy))

                # 데이터 삽입 쿼리
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.{} (code, date, data)
                    VALUES %s
                    ON CONFLICT (code, date) DO UPDATE 
                    SET data = EXCLUDED.data,
                        created_at = CURRENT_TIMESTAMP
                """
                ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

                # 중복 방지를 위한 제약 조건 확인 및 추가
                self._ensure_unique_constraint(
                    table_name, schema_name, ["code", "date"]
                )

                # 데이터 삽입
                execute_values(
                    self.cursor,
                    insert_sql.as_string(self.conn).replace("%s", "%s"),
                    [(v[0], v[1], psycopg2.extras.Json(v[2])) for v in values],
                )

                self.conn.commit()
                logger.info(
                    f"Saved {len(values)} records for code '{code}' to '{schema_name}.{table_name}'"
                )

        except Exception as e:
            logger.error(
                f"Error saving daily price data to '{schema_name}.{table_name}': {str(e)}"
            )
            if self.conn:
                self.conn.rollback()
            raise

    def _serialize_item(self, item):
        """
        pandas의 Timestamp 객체 등을 JSON 직렬화 가능한 형태로 변환

        Args:
            item: 직렬화할 아이템 (딕셔너리)

        Returns:
            dict: 직렬화 가능한 형태로 변환된 아이템
        """
        if isinstance(item, dict):
            result = {}
            for key, value in item.items():
                # pandas Timestamp 객체 처리
                if hasattr(value, "strftime"):
                    result[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                # 내부 딕셔너리 처리
                elif isinstance(value, dict):
                    result[key] = self._serialize_item(value)
                # 내부 리스트 처리
                elif isinstance(value, list):
                    result[key] = [
                        self._serialize_item(i) if isinstance(i, dict) else i
                        for i in value
                    ]
                else:
                    result[key] = value
            return result
        return item

    def save_minute_price_data(
        self, table_name: str, data: Dict[str, Any], schema_name: str = "ignacio"
    ) -> None:
        """분봉 가격 데이터 저장

        Args:
            table_name: 테이블 이름
            data: 데이터 (코드별 딕셔너리)
            schema_name: 스키마 이름
        """
        try:
            self.connect()
            self.create_table_if_not_exists(table_name, schema_name)

            for code, code_data in data.items():
                if code_data is None or code_data.empty:
                    logger.warning(f"No data for code '{code}', skipping")
                    continue

                # 데이터 형태 변환 (출력2 데이터 포맷)
                if (
                    isinstance(code_data, dict)
                    and "output2" in code_data
                    and isinstance(code_data["output2"], list)
                ):
                    items = code_data["output2"]
                else:
                    items = code_data if isinstance(code_data, list) else [code_data]

                # 데이터 삽입 준비
                values = []
                for item in items:
                    # 날짜와 시간 추출 (YYYYMMDD 형식, HHMMSS 형식)
                    stck_bsop_date = item.get("stck_bsop_date")
                    stck_cntg_hour = item.get(
                        "stck_cntg_hour", ""
                    )  # 분봉 조회에서는 stck_cntg_hour를 사용

                    item_date = date.today()  # 기본값
                    if stck_bsop_date is not None:
                        # pandas Series인 경우 처리
                        if hasattr(stck_bsop_date, "iloc"):
                            try:
                                stck_bsop_date = (
                                    stck_bsop_date.iloc[0]
                                    if len(stck_bsop_date) > 0
                                    else None
                                )
                            except:
                                stck_bsop_date = None

                        # 날짜 변환
                        if stck_bsop_date is not None and (
                            isinstance(stck_bsop_date, str)
                            or hasattr(stck_bsop_date, "strftime")
                        ):
                            try:
                                if isinstance(stck_bsop_date, str):
                                    item_date = date(
                                        year=int(stck_bsop_date[:4]),
                                        month=int(stck_bsop_date[4:6]),
                                        day=int(stck_bsop_date[6:8]),
                                    )
                                elif hasattr(stck_bsop_date, "strftime"):
                                    # 이미 날짜 객체인 경우
                                    item_date = (
                                        stck_bsop_date.date()
                                        if hasattr(stck_bsop_date, "date")
                                        else stck_bsop_date
                                    )
                            except (ValueError, TypeError, IndexError) as e:
                                logger.warning(
                                    f"Invalid date format: {stck_bsop_date}, using current date. Error: {e}"
                                )

                    # 시간 처리
                    item_time = datetime.now().time()  # 기본값

                    # Series 객체 처리
                    if hasattr(stck_cntg_hour, "iloc"):
                        try:
                            stck_cntg_hour = (
                                stck_cntg_hour.iloc[0]
                                if len(stck_cntg_hour) > 0
                                else None
                            )
                        except:
                            stck_cntg_hour = None

                    # 시간 변환
                    if stck_cntg_hour is not None and isinstance(stck_cntg_hour, str):
                        try:
                            item_time = datetime.strptime(
                                stck_cntg_hour, "%H%M%S"
                            ).time()
                        except (ValueError, TypeError) as e:
                            logger.warning(
                                f"Invalid time format: {stck_cntg_hour}, using current time. Error: {e}"
                            )

                    # 아이템이 DataFrame 행일 경우 딕셔너리로 변환
                    if hasattr(item, "to_dict"):
                        item = item.to_dict()

                    values.append((code, item_date, item_time, item))

                # 데이터가 없으면 다음 코드로 넘어감
                if not values:
                    logger.warning(f"No valid data to save for code '{code}', skipping")
                    continue

                # 데이터 삽입 쿼리
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.{} (code, date, time, data)
                    VALUES %s
                    ON CONFLICT (code, date, time) DO UPDATE 
                    SET data = EXCLUDED.data,
                        created_at = CURRENT_TIMESTAMP
                """
                ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

                # 중복 방지를 위한 제약 조건 확인 및 추가
                self._ensure_unique_constraint(
                    table_name, schema_name, ["code", "date", "time"]
                )

                # 데이터 삽입 전에 직렬화 (JSON 직렬화가 불가능한 데이터 타입 처리)
                serialized_values = []
                for v in values:
                    try:
                        # 딕셔너리를 Json 타입으로 직렬화
                        json_data = self._serialize_item(v[3])
                        serialized_values.append(
                            (v[0], v[1], v[2], psycopg2.extras.Json(json_data))
                        )
                    except Exception as e:
                        logger.warning(f"Error serializing data for code '{code}': {e}")
                        continue

                # 데이터 삽입
                if serialized_values:
                    execute_values(
                        self.cursor,
                        insert_sql.as_string(self.conn).replace("%s", "%s"),
                        serialized_values,
                    )

                    self.conn.commit()
                    logger.info(
                        f"Saved {len(serialized_values)} records for code '{code}' to '{schema_name}.{table_name}'"
                    )
                else:
                    logger.warning(f"No serialized data to save for code '{code}'")

        except Exception as e:
            logger.error(
                f"Error saving minute price data to '{schema_name}.{table_name}': {str(e)}"
            )
            if self.conn:
                self.conn.rollback()
            raise

    def save_options_data(
        self, table_name: str, data: List[Dict[str, Any]], schema_name: str = "ignacio"
    ) -> None:
        """옵션 데이터 저장

        Args:
            table_name: 테이블 이름
            data: 옵션 데이터 목록
            schema_name: 스키마 이름
        """
        try:
            self.connect()

            # 옵션 데이터용 컬럼 정의
            columns = {
                "id": "SERIAL PRIMARY KEY",
                "code": "VARCHAR(20)",
                "date": "DATE",
                "time": "TIME",
                "strike": "NUMERIC",
                "call_put": "VARCHAR(4)",
                "expire_date": "DATE",
                "data": "JSONB",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }

            self.create_table_if_not_exists(table_name, schema_name, columns)

            if not data:
                logger.warning(f"No options data to save, skipping")
                return

            # 데이터 삽입 준비
            today = date.today()
            current_time = datetime.now().time()
            values = []

            for item in data:
                # 옵션 코드 및 속성 추출
                code = item.get("code", "")
                strike = item.get("strike_price", 0)
                call_put = item.get("call_put_type", "")
                expire_date_str = item.get("expire_date", "")

                if expire_date_str:
                    try:
                        expire_date = date(
                            year=int(expire_date_str[:4]),
                            month=int(expire_date_str[4:6]),
                            day=int(expire_date_str[6:8]),
                        )
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Invalid expire date format: {expire_date_str}, using None"
                        )
                        expire_date = None
                else:
                    expire_date = None

                values.append(
                    (code, today, current_time, strike, call_put, expire_date, item)
                )

            # 데이터 삽입 쿼리
            insert_sql = sql.SQL(
                """
                INSERT INTO {}.{} (code, date, time, strike, call_put, expire_date, data)
                VALUES %s
                ON CONFLICT (code, date, time) DO UPDATE 
                SET strike = EXCLUDED.strike,
                    call_put = EXCLUDED.call_put,
                    expire_date = EXCLUDED.expire_date,
                    data = EXCLUDED.data,
                    created_at = CURRENT_TIMESTAMP
            """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

            # 중복 방지를 위한 제약 조건 확인 및 추가
            self._ensure_unique_constraint(
                table_name, schema_name, ["code", "date", "time"]
            )

            # 데이터 삽입
            execute_values(
                self.cursor,
                insert_sql.as_string(self.conn).replace("%s", "%s"),
                [
                    (v[0], v[1], v[2], v[3], v[4], v[5], psycopg2.extras.Json(v[6]))
                    for v in values
                ],
            )

            self.conn.commit()
            logger.info(
                f"Saved {len(values)} options records to '{schema_name}.{table_name}'"
            )

        except Exception as e:
            logger.error(
                f"Error saving options data to '{schema_name}.{table_name}': {str(e)}"
            )
            if self.conn:
                self.conn.rollback()
            raise

    def save_investor_trends_data(
        self, table_name: str, data: Dict[str, Any], schema_name: str = "ignacio"
    ) -> None:
        """투자자 동향 데이터 저장

        Args:
            table_name: 테이블 이름
            data: 데이터 (시장별 딕셔너리)
            schema_name: 스키마 이름
        """
        try:
            self.connect()

            # 투자자 동향 데이터용 컬럼 정의
            columns = {
                "id": "SERIAL PRIMARY KEY",
                "market": "VARCHAR(20)",
                "date": "DATE",
                "data": "JSONB",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }

            self.create_table_if_not_exists(table_name, schema_name, columns)

            # 중복 방지를 위한 제약 조건 확인 및 추가
            self._ensure_unique_constraint(table_name, schema_name, ["market", "date"])

            for market, market_data in data.items():
                if not market_data:
                    logger.warning(f"No data for market '{market}', skipping")
                    continue

                # 데이터 형태 변환 (출력1 데이터 포맷)
                if (
                    isinstance(market_data, dict)
                    and "output1" in market_data
                    and isinstance(market_data["output1"], list)
                ):
                    items = market_data["output1"]
                else:
                    items = (
                        market_data if isinstance(market_data, list) else [market_data]
                    )

                processed_count = 0

                # 각 항목을 개별적으로 처리하여 충돌 오류 방지
                for item in items:
                    # 날짜 추출 (YYYYMMDD 형식)
                    date_str = item.get("stck_bsop_date", item.get("indd_date", ""))

                    if date_str:
                        try:
                            # 날짜 형식 변환
                            if len(date_str) == 8:  # YYYYMMDD 형식
                                item_date = date(
                                    year=int(date_str[:4]),
                                    month=int(date_str[4:6]),
                                    day=int(date_str[6:8]),
                                )
                            else:
                                logger.warning(
                                    f"Unexpected date format: {date_str}, using current date"
                                )
                                item_date = date.today()
                        except (ValueError, TypeError):
                            logger.warning(
                                f"Invalid date format: {date_str}, using current date"
                            )
                            item_date = date.today()
                    else:
                        item_date = date.today()

                    # 개별 삽입 쿼리 실행
                    insert_sql = sql.SQL(
                        """
                        INSERT INTO {}.{} (market, date, data)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (market, date) DO UPDATE 
                        SET data = EXCLUDED.data,
                            created_at = CURRENT_TIMESTAMP
                    """
                    ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

                    self.cursor.execute(
                        insert_sql, (market, item_date, psycopg2.extras.Json(item))
                    )
                    processed_count += 1

                self.conn.commit()
                logger.info(
                    f"Saved {processed_count} records for market '{market}' to '{schema_name}.{table_name}'"
                )

        except Exception as e:
            logger.error(
                f"Error saving investor trends data to '{schema_name}.{table_name}': {str(e)}"
            )
            if self.conn:
                self.conn.rollback()
            raise

    def _ensure_unique_constraint(
        self, table_name: str, schema_name: str = "ignacio", columns: List[str] = None
    ) -> None:
        """유니크 제약 조건 확인 및 추가

        Args:
            table_name: 테이블 이름
            schema_name: 스키마 이름
            columns: 유니크 제약조건 컬럼 목록
        """
        if not columns:
            return

        constraint_name = f"{table_name}_{'_'.join(columns)}_key"

        try:
            # 제약 조건 존재 여부 확인
            check_sql = sql.SQL(
                """
                SELECT 1 FROM pg_constraint 
                WHERE conname = %s
            """
            )

            self.cursor.execute(check_sql, [constraint_name])

            if self.cursor.fetchone() is None:
                # 제약 조건 추가
                add_constraint_sql = sql.SQL(
                    """
                    ALTER TABLE {}.{} 
                    ADD CONSTRAINT {} UNIQUE ({})
                """
                ).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.Identifier(constraint_name),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                )

                self.cursor.execute(add_constraint_sql)
                self.conn.commit()
                logger.info(
                    f"Added unique constraint '{constraint_name}' to '{schema_name}.{table_name}'"
                )

        except Exception as e:
            logger.error(f"Error ensuring unique constraint: {str(e)}")
            # 이미 제약 조건이 있는 경우 무시
            self.conn.rollback()

    def __enter__(self):
        """컨텍스트 매니저 진입"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.disconnect()
