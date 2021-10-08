from setuptools import setup, find_packages

setup(
    name='waste',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        'click',
        'pandas',
        'numpy',
        'pm4py',
        'openpyxl',
        'pytest'
    ],
    entry_points={
        'console_scripts': [
            'waste = waste.main:main',
        ]
    }
)