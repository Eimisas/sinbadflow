import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = ["mock>=2", "setuptools>=40.8",
                "forbiddenfruit>=0.1.3"]

setuptools.setup(
    name='Sinbadflow',
    version='0.1',
    author="Eimantas Jazonis, Robertas Sys",
    author_email="eimant.jaz@gmail.com, robertas.sys@gmail.com",
    description="A simple pipeline tool for Databricks notebooks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="[]",
    packages=setuptools.find_packages(exclude=('test',)),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
