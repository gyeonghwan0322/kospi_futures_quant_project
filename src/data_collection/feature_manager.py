#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
피처 관리자 모듈

다양한 피처 클래스의 인스턴스 생성 및 관리를 담당합니다.
설정 파일을 기반으로 피처 객체를 동적으로 생성하고,
이벤트 핸들링 및 데이터 분배를 관리합니다.
"""

import os
import yaml
import logging
import importlib
import traceback
from typing import Dict, List, Any, Optional, Union, Type

from src.data_collection.abstract_feature import Feature
from src.data_collection.api_client import APIClient

logger = logging.getLogger(__name__)


class FeatureManager:
    """피처 관리자 클래스

    설정 파일을 기반으로 피처 객체를 생성하고 관리합니다.
    피처 클래스는 설정 파일에 정의된 대로 동적으로 가져와서 인스턴스화합니다.
    """

    def __init__(
        self,
        features_yaml_path: str = "config/features.yaml",
        params_yaml_path: str = "config/params.yaml",
        api_config_yaml_path: str = "config/api_config.yaml",
    ):
        """FeatureManager 생성자

        Args:
            features_yaml_path: 피처 설정 파일 경로
            params_yaml_path: 파라미터 설정 파일 경로
            api_config_yaml_path: API 설정 파일 경로
        """
        self.features_yaml_path = features_yaml_path
        self.params_yaml_path = params_yaml_path
        self.api_config_yaml_path = api_config_yaml_path

        # 설정 파일 로드
        self.features_config = self._load_yaml(features_yaml_path)
        self.params_config = self._load_yaml(params_yaml_path)
        self.api_config = self._load_yaml(api_config_yaml_path)

        # APIClient 생성 (스키마 파일 사용하지 않음)
        self.api_client = APIClient(api_config=self.api_config)

        # 피처 인스턴스 저장 딕셔너리
        self.features: Dict[str, Feature] = {}

        # 피처 초기화
        self._initialize_features()

    def _load_yaml(self, file_path: str) -> Dict:
        """YAML 설정 파일 로드

        Args:
            file_path: YAML 파일 경로

        Returns:
            Dict: 로드된 설정 정보
        """
        if not os.path.exists(file_path):
            logger.warning(f"설정 파일이 존재하지 않습니다: {file_path}")
            return {}

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file)
            logger.info(f"설정 파일 로드 성공: {file_path}")
            return config
        except Exception as e:
            logger.error(f"설정 파일 로드 중 오류 발생: {file_path}, {str(e)}")
            return {}

    def _initialize_features(self):
        """features.yaml 설정에 따라 피처 객체 초기화"""
        if not self.features_config or "features" not in self.features_config:
            logger.warning("피처 설정을 찾을 수 없습니다.")
            return

        for feature_name, feature_config in self.features_config["features"].items():
            try:
                # 피처 클래스 로드 - 여러 형식 지원
                feature_class = None

                # 기존 방식 (class 키 사용)
                if "class" in feature_config:
                    feature_class = self._import_feature_class(
                        feature_config.get("class", "")
                    )

                # 새로운 방식 (module_path와 feature_class 키 사용)
                elif (
                    "module_path" in feature_config
                    and "feature_class" in feature_config
                ):
                    module_path = feature_config.get("module_path", "")
                    class_name = feature_config.get("feature_class", "")
                    feature_class = self._import_feature_class(
                        f"{module_path}.{class_name}"
                    )

                if not feature_class:
                    logger.error(f"피처 클래스를 로드할 수 없습니다: {feature_name}")
                    continue

                # 파라미터 설정 로드
                param_key = feature_config.get("param_key", "")
                params = self.params_config.get(param_key, {}) if param_key else {}

                # 코드 리스트 설정
                code_list = feature_config.get("code_list", [])
                # config에 code_list가 없으면 params에서 가져옴
                if not code_list and "code_list" in params:
                    code_list = params.get("code_list", [])

                # API 설정 추가
                if "api_config" not in params:
                    params["api_config"] = self.api_config

                quote_connect = feature_config.get("quote_connect", False)

                # 피처 인스턴스 생성
                feature_instance = feature_class(
                    _feature_name=feature_name,
                    _code_list=code_list,
                    _feature_query=self.api_client,  # APIClient 인스턴스 전달
                    _quote_connect=quote_connect,
                    _params=params,
                )

                # 피처 리스트에 추가
                self.features[feature_name] = feature_instance
                logger.info(f"피처 초기화 성공: {feature_name}")

            except Exception as e:
                logger.error(f"피처 초기화 중 오류 발생: {feature_name}, {str(e)}")
                logger.error(traceback.format_exc())

    def _import_feature_class(self, class_path: str) -> Optional[Type[Feature]]:
        """클래스 경로를 기반으로 피처 클래스 동적 로드

        Args:
            class_path: 모듈 경로와 클래스 이름 (예: "sushi.feature.example_feature.ExampleFeature")

        Returns:
            Optional[Type[Feature]]: 로드된 피처 클래스
        """
        if not class_path:
            return None

        try:
            # 모듈 경로와 클래스 이름 분리
            module_path, class_name = class_path.rsplit(".", 1)

            # 모듈 동적 로드
            module = importlib.import_module(module_path)

            # 클래스 가져오기
            feature_class = getattr(module, class_name)

            return feature_class
        except (ImportError, AttributeError, ValueError) as e:
            logger.error(f"피처 클래스 로드 중 오류 발생: {class_path}, {str(e)}")
            return None

    def get_feature(self, feature_name: str) -> Optional[Feature]:
        """피처 이름으로 피처 인스턴스 조회

        Args:
            feature_name: 피처 이름

        Returns:
            Optional[Feature]: 해당 이름의 피처 인스턴스
        """
        return self.features.get(feature_name)

    def get_all_features(self) -> Dict[str, Feature]:
        """모든 피처 인스턴스 반환

        Returns:
            Dict[str, Feature]: 피처 이름을 키로 하는 피처 인스턴스 딕셔너리
        """
        return self.features

    def call_feature(self, feature_name: str, **kwargs) -> Any:
        """피처 호출 메서드

        Args:
            feature_name: 피처 이름
            **kwargs: 피처 호출에 필요한 추가 파라미터

        Returns:
            Any: 피처 호출 결과
        """
        feature = self.get_feature(feature_name)
        if not feature:
            logger.error(f"피처를 찾을 수 없습니다: {feature_name}")
            return None

        try:
            # 피처 객체의 call_feature 메서드 호출
            result = feature.call_feature(**kwargs)
            return result
        except Exception as e:
            logger.error(f"피처 호출 중 오류 발생: {feature_name}, {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def check_health(self) -> Dict[str, str]:
        """피처 상태 확인

        Returns:
            피처별 상태 정보
        """
        health_status = {}

        for feature_name, feature in self.features.items():
            try:
                status = feature.health_check_value
                health_status[feature_name] = status or "OK"
            except Exception as e:
                health_status[feature_name] = f"Error: {str(e)}"

        return health_status

    def __str__(self) -> str:
        """문자열 표현"""
        return f"FeatureManager with {len(self.features)} features"

    def __repr__(self) -> str:
        """개발자 표현"""
        return f"FeatureManager(features={list(self.features.keys())})"
