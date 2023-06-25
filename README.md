# Python-API-Development-Course
D:\Project_Steven\steven\Scripts\activate.bat
uvicorn main:app --host 0.0.0.0 --port 30 --reload

## Step
### 1. install virtual env
pip3 install virtualenv
python3 -m venv fastapi_env
fastapi_env\Scripts\activate.bat
### 2. install packages
pip install -r requirements.txt

### 3. run app 
uvicorn main:app --host 0.0.0.0 --port 3000 --reload

### 4. open 
http://localhost:3000/docs

login: 
Admin
username = steven
password = 123
Read
username = steven_read
password = 123

## share
### 1. fastapi
### 2. path function
### 3. get/ post/ put/ delete
### 4. body
### 5. Depends
### 6. connect sql
### 7. authorization
### 7.1 access role
### 8. request to datatp db



# structure
├── alembic/
├── src
│   ├── auth
│   │   ├── router.py
│   │   ├── schemas.py  # pydantic models
│   │   ├── models.py  # db models
│   │   ├── dependencies.py
│   │   ├── config.py  # local configs
│   │   ├── constants.py
│   │   ├── exceptions.py
│   │   ├── service.py
│   │   └── utils.py
│   ├── aws
│   │   ├── client.py  # client model for external service communication
│   │   ├── schemas.py
│   │   ├── config.py
│   │   ├── constants.py
│   │   ├── exceptions.py
│   │   └── utils.py
│   └── posts
│   │   ├── router.py
│   │   ├── schemas.py
│   │   ├── models.py
│   │   ├── dependencies.py
│   │   ├── constants.py
│   │   ├── exceptions.py
│   │   ├── service.py
│   │   └── utils.py
│   ├── config.py  # global configs
│   ├── models.py  # global models
│   ├── exceptions.py  # global exceptions
│   ├── pagination.py  # global module e.g. pagination
│   ├── database.py  # db connection related stuff
│   └── main.py
├── tests/
│   ├── auth
│   ├── aws
│   └── posts
├── templates/
│   └── index.html
├── requirements
│   ├── base.txt
│   ├── dev.txt
│   └── prod.txt
├── .env
├── .gitignore
├── logging.ini
└── alembic.ini