#Autokeras Queen

##Install
Enter the pipevn virtual environement
```
pipenv shell
```

The install all the dependancies form the Pipfile
```
pipenv install .
```

Download the submodules. This is to download autokeras.
```
git submodule init
git submodule update
```

Autokeras doesn't work very well with pipenv so pip install it.

```
cd autokeras
pip install -e .
```

Generate protobuf files.

```
./generate.sh
```

Run the project.

```
python facade.py
# or `pipenv run python facade.py`, if you don't have pipenv shell running
```
