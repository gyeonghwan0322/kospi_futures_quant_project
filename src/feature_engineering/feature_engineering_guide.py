#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
KOSPI 선물 퀀트 투자를 위한 피처 엔지니어링 가이드

이 모듈은 수집된 원시 데이터를 머신러닝 모델에 적합한 피처로 변환하는
다양한 기법과 함수들을 제공합니다.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import warnings

warnings.filterwarnings("ignore")


class FeatureEngineer:
    """피처 엔지니어링을 위한 메인 클래스"""

    def __init__(self):
        self.features_created = []

    def create_technical_indicators(
        self, df: pd.DataFrame, price_col: str = "close"
    ) -> pd.DataFrame:
        """기술적 지표 생성

        Args:
            df: 가격 데이터가 포함된 DataFrame
            price_col: 가격 컬럼명

        Returns:
            기술적 지표가 추가된 DataFrame
        """
        result_df = df.copy()

        # 1. 이동평균선 (SMA)
        for window in [5, 10, 20, 60]:
            col_name = f"sma_{window}"
            result_df[col_name] = result_df[price_col].rolling(window=window).mean()
            self.features_created.append(col_name)

        # 2. 지수이동평균 (EMA)
        for window in [5, 10, 20]:
            col_name = f"ema_{window}"
            result_df[col_name] = result_df[price_col].ewm(span=window).mean()
            self.features_created.append(col_name)

        # 3. RSI (상대강도지수)
        delta = result_df[price_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        result_df["rsi_14"] = 100 - (100 / (1 + rs))
        self.features_created.append("rsi_14")

        # 4. 볼린저 밴드
        sma_20 = result_df[price_col].rolling(window=20).mean()
        std_20 = result_df[price_col].rolling(window=20).std()
        result_df["bb_upper"] = sma_20 + (std_20 * 2)
        result_df["bb_lower"] = sma_20 - (std_20 * 2)
        result_df["bb_position"] = (result_df[price_col] - result_df["bb_lower"]) / (
            result_df["bb_upper"] - result_df["bb_lower"]
        )
        self.features_created.extend(["bb_upper", "bb_lower", "bb_position"])

        # 5. MACD
        ema_12 = result_df[price_col].ewm(span=12).mean()
        ema_26 = result_df[price_col].ewm(span=26).mean()
        result_df["macd"] = ema_12 - ema_26
        result_df["macd_signal"] = result_df["macd"].ewm(span=9).mean()
        result_df["macd_histogram"] = result_df["macd"] - result_df["macd_signal"]
        self.features_created.extend(["macd", "macd_signal", "macd_histogram"])

        return result_df

    def create_volatility_features(
        self, df: pd.DataFrame, price_col: str = "close"
    ) -> pd.DataFrame:
        """변동성 관련 피처 생성

        Args:
            df: 가격 데이터가 포함된 DataFrame
            price_col: 가격 컬럼명

        Returns:
            변동성 피처가 추가된 DataFrame
        """
        result_df = df.copy()

        # 1. 수익률 계산
        result_df["return_1d"] = result_df[price_col].pct_change()
        result_df["return_5d"] = result_df[price_col].pct_change(5)
        result_df["return_20d"] = result_df[price_col].pct_change(20)
        self.features_created.extend(["return_1d", "return_5d", "return_20d"])

        # 2. 변동성 (Rolling Standard Deviation)
        for window in [5, 10, 20]:
            col_name = f"volatility_{window}d"
            result_df[col_name] = result_df["return_1d"].rolling(window=window).std()
            self.features_created.append(col_name)

        # 3. 가격 범위 지표
        if all(col in result_df.columns for col in ["high", "low"]):
            result_df["price_range"] = (
                result_df["high"] - result_df["low"]
            ) / result_df[price_col]
            result_df["price_range_ma"] = (
                result_df["price_range"].rolling(window=20).mean()
            )
            self.features_created.extend(["price_range", "price_range_ma"])

        return result_df

    def create_investor_behavior_features(
        self, investor_df: pd.DataFrame
    ) -> pd.DataFrame:
        """투자자 매매동향 관련 피처 생성

        Args:
            investor_df: 투자자 매매동향 데이터

        Returns:
            투자자 행동 피처가 추가된 DataFrame
        """
        result_df = investor_df.copy()

        # 1. 순매수 비율 (외국인, 개인, 기관)
        for investor_type in ["frgn", "prsn", "orgn"]:
            # 순매수량 비율
            buy_col = f"{investor_type}_buy_amount"
            sell_col = f"{investor_type}_sell_amount"

            if buy_col in result_df.columns and sell_col in result_df.columns:
                total_amount = result_df[buy_col] + result_df[sell_col]
                result_df[f"{investor_type}_net_buy_ratio"] = (
                    (result_df[buy_col] - result_df[sell_col]) / total_amount
                ).fillna(0)
                self.features_created.append(f"{investor_type}_net_buy_ratio")

        # 2. 투자자 간 상대적 강도
        if all(
            f"{inv}_net_buy_ratio" in result_df.columns
            for inv in ["frgn", "prsn", "orgn"]
        ):
            # 외국인 vs 개인
            result_df["frgn_vs_prsn"] = (
                result_df["frgn_net_buy_ratio"] - result_df["prsn_net_buy_ratio"]
            )
            # 외국인 vs 기관
            result_df["frgn_vs_orgn"] = (
                result_df["frgn_net_buy_ratio"] - result_df["orgn_net_buy_ratio"]
            )
            self.features_created.extend(["frgn_vs_prsn", "frgn_vs_orgn"])

        # 3. 투자자 행동의 이동평균 (트렌드 파악)
        for investor_type in ["frgn", "prsn", "orgn"]:
            ratio_col = f"{investor_type}_net_buy_ratio"
            if ratio_col in result_df.columns:
                for window in [5, 10, 20]:
                    ma_col = f"{ratio_col}_ma_{window}"
                    result_df[ma_col] = (
                        result_df[ratio_col].rolling(window=window).mean()
                    )
                    self.features_created.append(ma_col)

        return result_df

    def create_lagged_features(
        self, df: pd.DataFrame, columns: List[str], lags: List[int] = [1, 2, 3, 5]
    ) -> pd.DataFrame:
        """시차 피처 생성

        Args:
            df: 원본 DataFrame
            columns: 시차를 적용할 컬럼 리스트
            lags: 시차 기간 리스트

        Returns:
            시차 피처가 추가된 DataFrame
        """
        result_df = df.copy()

        for col in columns:
            if col in result_df.columns:
                for lag in lags:
                    lag_col = f"{col}_lag_{lag}"
                    result_df[lag_col] = result_df[col].shift(lag)
                    self.features_created.append(lag_col)

        return result_df

    def create_rolling_statistics(
        self, df: pd.DataFrame, columns: List[str], windows: List[int] = [5, 10, 20]
    ) -> pd.DataFrame:
        """롤링 통계량 피처 생성

        Args:
            df: 원본 DataFrame
            columns: 통계량을 계산할 컬럼 리스트
            windows: 롤링 윈도우 크기 리스트

        Returns:
            롤링 통계량 피처가 추가된 DataFrame
        """
        result_df = df.copy()

        for col in columns:
            if col in result_df.columns:
                for window in windows:
                    # 평균
                    mean_col = f"{col}_mean_{window}"
                    result_df[mean_col] = result_df[col].rolling(window=window).mean()
                    self.features_created.append(mean_col)

                    # 표준편차
                    std_col = f"{col}_std_{window}"
                    result_df[std_col] = result_df[col].rolling(window=window).std()
                    self.features_created.append(std_col)

                    # 최소값, 최대값
                    min_col = f"{col}_min_{window}"
                    max_col = f"{col}_max_{window}"
                    result_df[min_col] = result_df[col].rolling(window=window).min()
                    result_df[max_col] = result_df[col].rolling(window=window).max()
                    self.features_created.extend([min_col, max_col])

        return result_df

    def create_interaction_features(
        self, df: pd.DataFrame, feature_pairs: List[Tuple[str, str]]
    ) -> pd.DataFrame:
        """상호작용 피처 생성

        Args:
            df: 원본 DataFrame
            feature_pairs: 상호작용을 계산할 피처 쌍 리스트

        Returns:
            상호작용 피처가 추가된 DataFrame
        """
        result_df = df.copy()

        for feat1, feat2 in feature_pairs:
            if feat1 in result_df.columns and feat2 in result_df.columns:
                # 곱셈 상호작용
                mult_col = f"{feat1}_x_{feat2}"
                result_df[mult_col] = result_df[feat1] * result_df[feat2]
                self.features_created.append(mult_col)

                # 비율 상호작용
                ratio_col = f"{feat1}_div_{feat2}"
                result_df[ratio_col] = result_df[feat1] / (
                    result_df[feat2] + 1e-8
                )  # 0으로 나누기 방지
                self.features_created.append(ratio_col)

        return result_df

    def create_target_encoding(
        self, df: pd.DataFrame, categorical_cols: List[str], target_col: str
    ) -> pd.DataFrame:
        """타겟 인코딩 (범주형 변수 → 연속형 변수)

        Args:
            df: 원본 DataFrame
            categorical_cols: 범주형 컬럼 리스트
            target_col: 타겟 컬럼명

        Returns:
            타겟 인코딩이 적용된 DataFrame
        """
        result_df = df.copy()

        if target_col not in result_df.columns:
            print(f"Warning: Target column '{target_col}' not found")
            return result_df

        for col in categorical_cols:
            if col in result_df.columns:
                # 각 카테고리의 평균 타겟값으로 인코딩
                mean_target = result_df.groupby(col)[target_col].mean()
                encoded_col = f"{col}_target_encoded"
                result_df[encoded_col] = result_df[col].map(mean_target)
                self.features_created.append(encoded_col)

        return result_df

    def remove_highly_correlated_features(
        self, df: pd.DataFrame, threshold: float = 0.95
    ) -> pd.DataFrame:
        """높은 상관관계를 가진 피처 제거

        Args:
            df: 원본 DataFrame
            threshold: 상관계수 임계값

        Returns:
            높은 상관관계 피처가 제거된 DataFrame
        """
        numeric_df = df.select_dtypes(include=[np.number])
        corr_matrix = numeric_df.corr().abs()

        # 상삼각 행렬만 고려 (중복 제거)
        upper_tri = corr_matrix.where(
            np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
        )

        # 높은 상관관계를 가진 피처 찾기
        to_drop = [
            column for column in upper_tri.columns if any(upper_tri[column] > threshold)
        ]

        result_df = df.drop(columns=to_drop)
        print(f"제거된 피처 ({len(to_drop)}개): {to_drop}")

        return result_df

    def get_feature_importance_summary(self) -> Dict[str, Any]:
        """생성된 피처들의 요약 정보 반환"""
        return {
            "total_features_created": len(self.features_created),
            "feature_categories": {
                "technical_indicators": len(
                    [
                        f
                        for f in self.features_created
                        if any(
                            indicator in f
                            for indicator in ["sma", "ema", "rsi", "bb", "macd"]
                        )
                    ]
                ),
                "volatility_features": len(
                    [
                        f
                        for f in self.features_created
                        if "volatility" in f or "return" in f
                    ]
                ),
                "investor_behavior": len(
                    [
                        f
                        for f in self.features_created
                        if any(inv in f for inv in ["frgn", "prsn", "orgn"])
                    ]
                ),
                "lagged_features": len(
                    [f for f in self.features_created if "lag" in f]
                ),
                "rolling_statistics": len(
                    [
                        f
                        for f in self.features_created
                        if any(stat in f for stat in ["mean", "std", "min", "max"])
                    ]
                ),
                "interaction_features": len(
                    [f for f in self.features_created if "_x_" in f or "_div_" in f]
                ),
            },
            "created_features": self.features_created,
        }


def create_comprehensive_features(
    price_df: pd.DataFrame, investor_df: pd.DataFrame = None
) -> pd.DataFrame:
    """종합적인 피처 엔지니어링 파이프라인

    Args:
        price_df: 가격 데이터
        investor_df: 투자자 매매동향 데이터 (선택사항)

    Returns:
        모든 피처가 적용된 DataFrame
    """
    fe = FeatureEngineer()

    # 1. 기술적 지표 생성
    result_df = fe.create_technical_indicators(price_df)

    # 2. 변동성 피처 생성
    result_df = fe.create_volatility_features(result_df)

    # 3. 시차 피처 생성 (주요 지표들에 대해)
    key_features = ["return_1d", "rsi_14", "volatility_5d", "macd"]
    result_df = fe.create_lagged_features(result_df, key_features)

    # 4. 롤링 통계량 생성
    rolling_features = ["return_1d", "volatility_5d"]
    result_df = fe.create_rolling_statistics(result_df, rolling_features)

    # 5. 투자자 매매동향 피처 (데이터가 있는 경우)
    if investor_df is not None:
        investor_features = fe.create_investor_behavior_features(investor_df)
        # 날짜 기준으로 병합 (날짜 컬럼이 있다고 가정)
        if "date" in result_df.columns and "date" in investor_features.columns:
            result_df = pd.merge(result_df, investor_features, on="date", how="left")

    # 6. 상호작용 피처 생성
    interaction_pairs = [
        ("return_1d", "volatility_5d"),
        ("rsi_14", "bb_position"),
        ("sma_5", "sma_20"),
    ]
    result_df = fe.create_interaction_features(result_df, interaction_pairs)

    # 7. 높은 상관관계 피처 제거
    result_df = fe.remove_highly_correlated_features(result_df, threshold=0.95)

    # 요약 정보 출력
    summary = fe.get_feature_importance_summary()
    print(f"✅ 피처 엔지니어링 완료!")
    print(f"📊 총 생성된 피처: {summary['total_features_created']}개")
    print(f"📈 기술적 지표: {summary['feature_categories']['technical_indicators']}개")
    print(f"📉 변동성 피처: {summary['feature_categories']['volatility_features']}개")
    print(f"👥 투자자 행동: {summary['feature_categories']['investor_behavior']}개")
    print(f"⏰ 시차 피처: {summary['feature_categories']['lagged_features']}개")

    return result_df
