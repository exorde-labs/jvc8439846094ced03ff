from setuptools import find_packages, setup

setup(
    name="jvc8439846094ced03ff",
    version="0.0.2",
    packages=find_packages(),
    install_requires=[
        "exorde_data",
        "aiohttp",
        "beautifulsoup4>=4.11"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
