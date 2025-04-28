from setuptools import setup, find_packages

setup(
    name="auction_project",
    version="0.1.0",
    description="A Raft‐backed auction system",
    packages=find_packages(),
    install_requires=[
        "grpcio",
        "grpcio-tools",
        "protobuf",
        "requests",
    ],
    entry_points={
        "console_scripts": [
            "auction-node = auction_project.main:main"
        ]
    }
)
