import uvicorn
from datetime import datetime, timedelta
from io import BytesIO
from fastapi import Depends, FastAPI, HTTPException, Query, UploadFile, File, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from enum import Enum
from typing import Any, List, Optional, Union, Set
from pydantic import BaseModel, HttpUrl
from pydantic.dataclasses import dataclass
from fastapi.params import Body
from jose import JWTError, jwt
from passlib.context import CryptContext
from sql_app.database import Base, SessionLocal, engine, create_user_row, get_db  # 導入sql連線資訊
from sql_app.schemas import User, UserInDB
from sql_app.crud import get_user, create_user
from sqlalchemy.orm import Session

# Create the database tables
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)
create_user_row()
# Hash加密
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_password_hash(password):
    return pwd_context.hash(password)


app = FastAPI()


@app.get("/users/{username}", response_model=User)
def read_user(username: str, db: Session = Depends(get_db)):
    db_user = get_user(db, username=username)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


# create a nuser users
@app.post("/users/", tags=["User"])
def create_user(user: User, password: str = Body(...)):  # # 認證(最高權限才能創建新user)
    # 將密碼加密(使用Hash)
    user_dict = user.dict()
    hashed_password = get_password_hash(password)
    user = UserInDB(hashed_password=hashed_password, **user_dict)
    # 存庫
    # Open a connection to the SQLite database

    return {"message": "User created successfully"}


if __name__ == "__main__":
    uvicorn.run("main_crud:app", host="127.0.0.1", port=3000, reload=True)
