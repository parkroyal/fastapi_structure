from sqlite3 import IntegrityError
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
from sql_app.database import Base, SessionLocal, engine
from sql_app.models import User as DB_User  # 導入sql連線資訊
from sql_app.schemas import User, UserInDB, Token, TokenData  # Pydantic Model
from sql_app.crud import get_user, create_user, create_user_row, get_db
from sqlalchemy.orm import Session
from auth import (
    SECRET_KEY,
    ALGORITHM,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    get_password_hash,
    verify_password,
    authenticate_user,
    create_access_token,
)

# Create the database tables
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)
create_user_row(
    newuser=DB_User(
        username="steven",
        email="steven@example.com",
        hashed_password="$2b$12$K/CUNFaAjNJBQV8MME9s4eVL0MHHOxKBPSMbZvppW4EouTNxVoK4G",
        disabled=False,
        permissions="admin",
    )
)

# 授權登入
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


app = FastAPI()


async def get_current_user(
    db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = get_user(db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


# 登入認證
@app.post("/token", response_model=Token, tags=["auth"])
def login_for_access_token(
    db: Session = Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()
):
    user = get_user(db, username=form_data.username)  # 從db拿取該username資料
    # user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if user:  # 確認密碼是否正確
        check_password = verify_password(form_data.password, user.hashed_password)
        if check_password:
            pass
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/users/{username}", response_model=User)
def read_user(username: str, db: Session = Depends(get_db)):
    db_user = get_user(db, username=username)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


# user info
@app.get("/users/me/", response_model=User, tags=["User", "Auth"])
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user


# create a nuser users
@app.post("/users/", tags=["User", "Auth"])
def create_user(
    user: User,
    password: str = Body(...),
    current_user: User = Depends(get_current_user),
):  # # 認證(最高權限才能創建新user)
    # 將密碼加密(使用Hash)
    user_dict = user.dict()
    hashed_password = get_password_hash(password)
    user = DB_User(hashed_password=hashed_password, **user_dict)
    # 存庫
    if current_user.permissions != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Insufficient permission.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    else:
        try:
            create_user_row(newuser=user)
        except IntegrityError as e:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"IntegrityError: {e}",
                headers={"WWW-Authenticate": "Bearer"},
            )
        except Exception as err:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error: {err}",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return {"message": "User created successfully"}


if __name__ == "__main__":
    uvicorn.run("main_crud_auth:app", host="127.0.0.1", port=3000, reload=True)
