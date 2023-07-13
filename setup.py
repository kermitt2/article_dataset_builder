from setuptools import setup, find_packages

setup(
    name="article_dataset_builder",
    version="0.2.5",
    author="Patrice Lopez",
    author_email="patrice.lopez@science-miner.com",
    description="Open Access scholar PDF harvester, metadata aggregator and full-text ingester",
    long_description=open("Readme.md", encoding='utf-8').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kermitt2/article_dataset_builder",
    packages=find_packages(exclude=['test', '*.test', '*.test.*']),  
    include_package_data=True,
    python_requires='>=3.5',
    install_requires=[
        'boto3',
        'python-magic==0.4.15',
        'lmdb==1.4.1',
        'tqdm==4.21',
        'requests',
        'cloudscraper==1.2.69',
        'beautifulsoup4==4.11.2'
    ],
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX",
    ],
)
