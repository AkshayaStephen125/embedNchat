from setuptools import setup, find_packages

setup(
    name="shared",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[],

    author="Akshaya Stephen",
    author_email="akshayastephen125@gmail.com",
    description="Shared utilities and common modules for FastAPI microservices (DB, logger, utils, schemas)",
    long_description="A reusable Python package containing shared components like database connections, logging, utilities, and schemas used across multiple FastAPI microservices.",
    long_description_content_type="text/plain",


    classifiers=[
        "Programming Language :: Python :: 3",
        "Framework :: FastAPI",
        "Operating System :: OS Independent",
    ],

    python_requires=">=3.8",
)