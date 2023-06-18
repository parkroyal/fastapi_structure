from enum import Enum
from io import BytesIO
from typing import Optional, Union
import pandas as pd
import uvicorn
from sqlite3 import IntegrityError
from datetime import datetime, timedelta
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Path,
    Query,
    UploadFile,
    File,
    status,
)
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic.dataclasses import dataclass
from fastapi.params import Body
from jose import JWTError, jwt

# from package.db_helper import get_data
# from package.utils import to_sql
from sql_app.database import Base, SessionLocal, engine
from sql_app.models import User as DB_User  # 導入sql連線資訊
from sql_app.schemas import User, UserInDB, Token, TokenData  # Pydantic Model
from sql_app.crud import (
    get_user,
    create_user,
    create_user_row,
    get_db,
    get_password_hash,
    verify_password,
    authenticate_user,
    create_access_token,
)
from sqlalchemy.orm import Session
from auth.crypt import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES

# Create the database tables
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)
# Insert Admin
create_user_row(
    newuser=DB_User(
        username="steven",
        email="steven@example.com",
        hashed_password="$2b$12$K/CUNFaAjNJBQV8MME9s4eVL0MHHOxKBPSMbZvppW4EouTNxVoK4G",  # password = 123
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


# Access Roles Checker
# class RoleChecker:
#     def __init__(self, allowed_roles: List):
#         self.allowed_roles = allowed_roles

#     def __call__(self, user: User = Depends(get_current_active_user)):
#         if user.role not in self.allowed_roles:
#             # logger.debug(f"User with role {user.role} not in {self.allowed_roles}")
#             raise HTTPException(status_code=403, detail="Operation not permitted")


# allow_create_resource = RoleChecker(["admin"])


# Path Example
@app.get("/path/{path_id}")
async def read_items(
    path_id: Optional[str] = Path(
        default=...,  # 必須
        min_length=2,
        max_length=50,
        regex="^path",
        title="tewt",
        description="path description",
        deprecated=True,
    )
):
    return {"item_id": path_id}


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


# 抓帳號資訊
class MT4Server(str, Enum):
    enfaureport = "AU"
    enfau2report = "AU2"


@app.get("/accounts_info/", tags=["Accounts", "Auth"])
def search_accounts(
    server: MT4Server, login: int, current_user: User = Depends(get_current_user)
):  # # 認證(最高權限才能創建新user)
    # 存庫
    if current_user.permissions != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Insufficient permission.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    else:
        try:
            server = server.name
            # query = f"""
        #     select *
        #     from mt4_users
        #     where login = {login}
        #     """
        #     df = get_data(query, db="mt4", schema=server)
        except Exception as err:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error: {err}",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return {"server": server}

        # if df.shape[0] == 0:
        #     raise HTTPException(
        #         status_code=status.HTTP_404_NOT_FOUND,
        #         detail=f"Error: Cannot find the account.",
        #         headers={"WWW-Authenticate": "Bearer"},
        #     )

        # return df.to_dict(orient="records")


# batch
@app.post("/accounts_info/batch", tags=["Accounts", "Auth"])
async def search_accounts(
    file: UploadFile = File(...), current_user: User = Depends(get_current_user)
):  # # 認證(最高權限才能創建新user)
    # 存庫
    if current_user.permissions != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Insufficient permission.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    else:
        try:
            contents = await file.read()
            file_size = file.file.tell()
            # size validation
            if file_size > 2 * 1024 * 1024:
                raise HTTPException(status_code=400, detail="File too large.")

            # type validation
            if file.filename.endswith(".csv"):
                df = pd.read_csv(BytesIO(contents))
            elif file.filename.endswith(".xlsx"):
                df = pd.read_excel(BytesIO(contents))
            else:
                raise HTTPException(status_code=400, detail="Invalid file type.")

            # # check db & login
            # logins_sql = to_sql(df.login.tolist(), type=int)
            # servers = df.server.unique().tolist()
            # query = f"""
            # select database() as server, login, `group`, name, currency, balance, equity
            # from mt4_users
            # where login in ({logins_sql})
            # """
            # result = get_data(query, "mt4", servers)

            # df = df.merge(result, how="left", on=["server", "login"])

        except Exception as err:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error: {err}",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if df.shape[0] == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Error: Cannot find the account.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return df.to_dict(orient="records")


if __name__ == "__main__":
    uvicorn.run("main_crud_auth:app", host="127.0.0.1", port=3000, reload=True)
