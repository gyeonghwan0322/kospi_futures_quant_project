#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
한국 주식시장 거래일 계산 유틸리티

한국 주식시장의 영업일, 공휴일, 장시간을 고려하여
정확한 거래일자를 계산하는 함수들을 제공합니다.
"""

from datetime import datetime, timedelta, time
from typing import List
import pandas as pd


class KoreanTradingCalendar:
    """한국 주식시장 거래일 계산 클래스"""

    def __init__(self):
        # 한국 주식시장 장시간 (9:00 - 15:30)
        self.market_open = time(9, 0)  # 09:00
        self.market_close = time(15, 30)  # 15:30

        # 2024-2025 한국 공휴일 (주요 공휴일만)
        self.holidays_2024_2025 = [
            # 2024년
            "2024-01-01",  # 신정
            "2024-02-09",  # 설날 연휴
            "2024-02-10",  # 설날
            "2024-02-11",  # 설날 연휴
            "2024-02-12",  # 설날 대체휴일
            "2024-03-01",  # 삼일절
            "2024-04-10",  # 국회의원선거
            "2024-05-01",  # 근로자의날
            "2024-05-05",  # 어린이날
            "2024-05-06",  # 어린이날 대체휴일
            "2024-05-15",  # 부처님오신날
            "2024-06-06",  # 현충일
            "2024-08-15",  # 광복절
            "2024-09-16",  # 추석 연휴
            "2024-09-17",  # 추석
            "2024-09-18",  # 추석 연휴
            "2024-10-03",  # 개천절
            "2024-10-09",  # 한글날
            "2024-12-25",  # 크리스마스
            "2024-12-31",  # 연말휴장
            # 2025년
            "2025-01-01",  # 신정
            "2025-01-28",  # 설날 연휴
            "2025-01-29",  # 설날
            "2025-01-30",  # 설날 연휴
            "2025-03-01",  # 삼일절
            "2025-03-03",  # 삼일절 대체휴일 (토요일이므로)
            "2025-05-01",  # 근로자의날
            "2025-05-05",  # 어린이날
            "2025-05-13",  # 부처님오신날
            "2025-06-03",  # 대통령 선거
            "2025-06-06",  # 현충일
            "2025-08-15",  # 광복절
            "2025-10-03",  # 개천절
            "2025-10-06",  # 추석 연휴
            "2025-10-07",  # 추석
            "2025-10-08",  # 추석 연휴
            "2025-10-09",  # 한글날
            "2025-12-25",  # 크리스마스
            "2025-12-31",  # 연말휴장
        ]

        # 문자열을 datetime 객체로 변환
        self.holiday_dates = [
            datetime.strptime(date_str, "%Y-%m-%d").date()
            for date_str in self.holidays_2024_2025
        ]

    def is_trading_day(self, target_date: datetime) -> bool:
        """특정 날짜가 거래일인지 확인

        Args:
            target_date: 확인할 날짜

        Returns:
            bool: 거래일 여부
        """
        date_only = target_date.date()

        # 주말 체크 (토요일: 5, 일요일: 6)
        if target_date.weekday() >= 5:
            return False

        # 공휴일 체크
        if date_only in self.holiday_dates:
            return False

        return True

    def get_previous_trading_day(self, target_date: datetime) -> datetime:
        """이전 거래일을 찾기

        Args:
            target_date: 기준 날짜

        Returns:
            datetime: 이전 거래일
        """
        current_date = target_date - timedelta(days=1)

        while not self.is_trading_day(current_date):
            current_date -= timedelta(days=1)

        return current_date

    def get_next_trading_day(self, target_date: datetime) -> datetime:
        """다음 거래일을 찾기

        Args:
            target_date: 기준 날짜

        Returns:
            datetime: 다음 거래일
        """
        current_date = target_date + timedelta(days=1)

        while not self.is_trading_day(current_date):
            current_date += timedelta(days=1)

        return current_date

    def is_market_open(self, current_time: datetime) -> bool:
        """현재 시장이 열려있는지 확인

        Args:
            current_time: 확인할 시간

        Returns:
            bool: 시장 개장 여부
        """
        if not self.is_trading_day(current_time):
            return False

        current_time_only = current_time.time()
        return self.market_open <= current_time_only <= self.market_close

    def get_current_trading_date(self, current_time: datetime = None) -> str:
        """현재 시점의 거래일자를 계산

        Args:
            current_time: 현재 시간 (None이면 현재 시간 사용)

        Returns:
            str: 거래일자 (YYYY-MM-DD 형식)
        """
        if current_time is None:
            current_time = datetime.now()

        # 1. 현재 날짜가 거래일인지 확인
        if self.is_trading_day(current_time):
            current_time_only = current_time.time()

            # 장 시작 전 (09:00 이전)이면 전 거래일
            if current_time_only < self.market_open:
                trading_date = self.get_previous_trading_day(current_time)
            else:
                # 장중 또는 장후 (09:00 이후)면 당일
                trading_date = current_time
        else:
            # 주말이나 공휴일이면 이전 거래일
            trading_date = self.get_previous_trading_day(current_time)

        return trading_date.strftime("%Y-%m-%d")

    def get_trading_session_info(self, current_time: datetime = None) -> dict:
        """현재 거래 세션 정보 반환

        Args:
            current_time: 현재 시간 (None이면 현재 시간 사용)

        Returns:
            dict: 거래 세션 정보
        """
        if current_time is None:
            current_time = datetime.now()

        trading_date = self.get_current_trading_date(current_time)
        is_trading_day = self.is_trading_day(current_time)
        is_market_open = self.is_market_open(current_time)

        if is_trading_day and current_time.time() < self.market_open:
            session = "pre_market"
        elif is_market_open:
            session = "market_hours"
        elif is_trading_day and current_time.time() > self.market_close:
            session = "after_market"
        else:
            session = "non_trading_day"

        return {
            "trading_date": trading_date,
            "is_trading_day": is_trading_day,
            "is_market_open": is_market_open,
            "session": session,
            "current_time": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        }


# 전역 인스턴스
trading_calendar = KoreanTradingCalendar()


def get_current_trading_date() -> str:
    """현재 거래일자를 반환하는 편의 함수"""
    return trading_calendar.get_current_trading_date()


def get_trading_session_info() -> dict:
    """현재 거래 세션 정보를 반환하는 편의 함수"""
    return trading_calendar.get_trading_session_info()


if __name__ == "__main__":
    # 테스트 코드
    print("📅 한국 주식시장 거래일 계산 테스트")
    print("=" * 50)

    session_info = get_trading_session_info()
    print(f"현재 시간: {session_info['current_time']}")
    print(f"거래일자: {session_info['trading_date']}")
    print(f"거래일 여부: {session_info['is_trading_day']}")
    print(f"시장 개장 여부: {session_info['is_market_open']}")
    print(f"거래 세션: {session_info['session']}")
