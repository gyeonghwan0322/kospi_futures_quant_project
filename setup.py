#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [
        line.strip() for line in fh if line.strip() and not line.startswith("#")
    ]

setup(
    name="sushi-futures-quant",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="KOSPI 선물 정량 거래 시스템 (Sushi)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/kospi_futures_quant_project",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Office/Business :: Financial :: Investment",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.5.0",
        ],
        "analysis": [
            "matplotlib>=3.7.0",
            "seaborn>=0.12.0",
            "jupyter>=1.0.0",
            "plotly>=5.0.0",
        ],
        "advanced": [
            "pandas-ta>=0.3.14b0",
            "numpy-financial>=1.0.0",
            "scikit-learn>=1.3.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "sushi-collect=scripts.run_data_collector:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["config/*.yaml", "data/*.csv"],
    },
)
