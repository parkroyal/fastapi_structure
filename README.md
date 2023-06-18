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
uvicorn main_crud_auth:app --host 0.0.0.0 --port 3000 --reload

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
