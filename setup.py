import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = ["mock>=2", "setuptools>=40.8",
                "forbiddenfruit>=0.1.3"]

setuptools.setup(
    name='sinbadflow',
    version='0.6',
    author="Eimantas Jazonis, Robertas Sys, Emilija Lamanauskaite",
    author_email="eimant.jaz@gmail.com, robertas.sys@gmail.com, emilijalamanauskaite@gmail.com",
    description="A simple pipeline creation and execution tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Eimisas/sinbadflow",
    packages=setuptools.find_packages(exclude=('test',)),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
