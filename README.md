# Precision 100 Operators
Library of commonly used Precision 100 Operators

## Building the project
In order to build the project from source code and to make changes

1. Checkout the source code
```
git clone github.com/Evolvus/precision-100-operators
```

2. Create a python3 virtual environment
```
cd precision-100-operators
python3 -m venv app
source app/bin/activate
```

3. Install the tools necessary to build and deploy the application to pypi
```
python3 -m pip install --upgrade build
python3 -m pip install --upgrade twine
```

4. Build the project
   
To build the project navigate to the folder containing the toml file. Upon executing the following commands the built artifacts are added to the  *dist* folder

```
cd app
python3 -m build
```

5. Deploy the project to PyPi repository

Before deploy the artifacts to PyPi we need to make sure the version is updated properly. The version of the release is updated by editing the version in the toml file.
```
python3 -m twine upload --repository testpypi dist/*
```

## Deploying the operators
To deploy the operators from the repository, execute the following,

```
python3 -m pip install --index-url https://test.pypi.org/simple precision_100_operators
```
