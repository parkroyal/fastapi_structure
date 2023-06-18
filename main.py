import uvicorn
from fastapi import FastAPI
from sql_app.database import Base, engine
import routers

app = FastAPI()

# Create the database tables
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=3000, reload=True)

# from datetime import datetime, timedelta
# from io import BytesIO
# from fastapi import Depends, FastAPI, HTTPException, Query, UploadFile, File, status
# from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
# from enum import Enum
# from typing import Any, List, Optional, Union, Set
# from pydantic import BaseModel, HttpUrl
# from pydantic.dataclasses import dataclass
# from fastapi.params import Body
# from package.db_helper import config, get_data
# from jose import JWTError, jwt
# from passlib.context import CryptContext
# import os
# import uvicorn
# import pandas as pd

# from datetime import datetime, timedelta
# from typing import Union

# from fastapi import Depends, FastAPI, HTTPException, status
# from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
# from jose import JWTError, jwt
# from passlib.context import CryptContext
# from pydantic import BaseModel

# # to get a string like this run:
# # openssl rand -hex 32
# SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
# ALGORITHM = "HS256"
# ACCESS_TOKEN_EXPIRE_MINUTES = 30


# fake_users_db = {
#     "steven": {
#         "username": "steven",
#         "full_name": "steven",
#         "email": "steven@example.com",
#         "hashed_password": "$2b$12$K/CUNFaAjNJBQV8MME9s4eVL0MHHOxKBPSMbZvppW4EouTNxVoK4G",
#         "disabled": False,
#         "permissions": "admin",
#     },
#     "normal_acc": {
#         "username": "normal_acc",
#         "full_name": "normal_acc",
#         "email": "normal_acc@example.com",
#         "hashed_password": "$2b$12$K/CUNFaAjNJBQV8MME9s4eVL0MHHOxKBPSMbZvppW4EouTNxVoK4G",
#         "disabled": False,
#         "permissions": "read",
#     },
# }


# class Token(BaseModel):
#     access_token: str
#     token_type: str


# class TokenData(BaseModel):
#     username: Union[str, None] = None


# class User(BaseModel):
#     username: str
#     email: Union[str, None] = None
#     full_name: Union[str, None] = None
#     disabled: Union[bool, None] = None
#     permissions: List


# class UserInDB(User):
#     hashed_password: str


# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# print(pwd_context.hash("123"))

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# app = FastAPI()


# # response model
# class Item(BaseModel):
#     name: str
#     description: Union[str, None] = None
#     price: float
#     tax: Union[float, None] = None
#     tags: List[str] = []


# @app.post("/items/", response_model=Item, tags=["all"])
# async def create_item(item: Item) -> Any:
#     return item


# # @app.get("/items/", response_model=List[Item])
# # async def read_items() -> Any:
# #     return [
# #         {"name": "Portal Gun", "price": 42.0},
# #         {"name": "Plumbus", "price": 32.0},
# #     ]


# def verify_password(plain_password, hashed_password):
#     return pwd_context.verify(plain_password, hashed_password)


# def get_password_hash(password):
#     return pwd_context.hash(password)


# def get_user(db, username: str):
#     if username in db:
#         user_dict = db[username]
#         return UserInDB(**user_dict)


# def authenticate_user(fake_db, username: str, password: str):
#     user = get_user(fake_db, username)
#     if not user:
#         return False
#     if not verify_password(password, user.hashed_password):
#         return False
#     return user


# def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
#     to_encode = data.copy()
#     if expires_delta:
#         expire = datetime.utcnow() + expires_delta
#     else:
#         expire = datetime.utcnow() + timedelta(minutes=15)
#     to_encode.update({"exp": expire})
#     encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
#     return encoded_jwt


# async def get_current_user(token: str = Depends(oauth2_scheme)):
#     credentials_exception = HTTPException(
#         status_code=status.HTTP_401_UNAUTHORIZED,
#         detail="Could not validate credentials",
#         headers={"WWW-Authenticate": "Bearer"},
#     )
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         username: str = payload.get("sub")
#         if username is None:
#             raise credentials_exception
#         token_data = TokenData(username=username)
#     except JWTError:
#         raise credentials_exception
#     user = get_user(fake_users_db, username=token_data.username)
#     if user is None:
#         raise credentials_exception
#     return user


# async def get_current_active_user(current_user: User = Depends(get_current_user)):
#     if current_user.disabled:
#         raise HTTPException(status_code=400, detail="Inactive user")
#     return current_user


# # class RoleChecker:
# #     def __init__(self, allowed_roles: List):
# #         self.allowed_roles = allowed_roles

# #     def __call__(self, user: User = Depends(get_current_active_user)):
# #         if user.role not in self.allowed_roles:
# #             # logger.debug(f"User with role {user.role} not in {self.allowed_roles}")
# #             raise HTTPException(status_code=403, detail="Operation not permitted")


# # allow_create_resource = RoleChecker(["admin"])


# # @app.post("/some-resource/", dependencies=[Depends(allow_create_resource)])
# # def add_resource():
# #     # Some validation like resource does not already exist
# #     # Create the resource
# #     pass


# @app.post("/token", response_model=Token, tags=["auth"])
# async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
#     user = authenticate_user(fake_users_db, form_data.username, form_data.password)
#     if not user:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Incorrect username or password",
#             headers={"WWW-Authenticate": "Bearer"},
#         )
#     access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
#     access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)
#     return {"access_token": access_token, "token_type": "bearer"}


# @app.get("/users/me/", response_model=User, tags=["protected"])
# async def read_users_me(current_user: User = Depends(get_current_active_user)):
#     return current_user


# @app.get("/users/me/items/", tags=["protected"])
# async def read_own_items(current_user: User = Depends(get_current_active_user)):
#     return [{"item_id": "Foo", "owner": current_user.username}]


# class PermissionChecker:
#     def __init__(self, required_permissions: list = []) -> None:
#         self.required_permissions = required_permissions

#     def __call__(self, user: UserInDB = Depends(get_current_user)) -> bool:
#         if any(permission in self.required_permissions for permission in user.permissions):
#             return True
#         else:
#             return False


# test = PermissionChecker(required_permissions=["123"])


# # 需要admin才能get
# @app.get("/items", tags=["protected"])
# def items(
#     authorize: bool = Depends(
#         PermissionChecker(
#             required_permissions=[
#                 "admin",
#             ]
#         )
#     )
# ):
#     return "items"


# # @dataclass
# # class Login:
# #     server: str
# #     login: int


# # @app.post("/accounts")
# # async def find_accounts_info(payload: Login = Body(...)):
# #     query = f"""
# #     select *
# #     from mt4_user
# #     where login = {Login.login}
# #     """
# #     df = get_data(query, "mt4", Login.server)

# #     return df


# # # path operation decorator
# # @app.get("/")
# # # path operation function
# # async def root():
# #     files = UploadFile(file=r"D:\report_r_steven\FileOutput\Group EOD Exchange Rate 2023-05-22.csv")
# #     content = files.read()
# #     df = pd.read_csv(content)
# #     return {"message": "Hello World"}


# # class MT4_Server(str, Enum):
# #     enfaureport = "enfaureport"


# # @app.get("/symbol_table/{server_name}")
# # async def get_model(server_name: MT4_Server):
# #     print(server_name)
# #     query = f"""
# #     select distinct server, symbol, symbol_type
# #     from symbol_table
# #     where server = '{server_name}'
# #     limit 5
# #     """
# #     df = get_data(query, "datatp", "symbol")
# #     # df.to_dict()
# #     return df.to_dict()


# # from fastapi.responses import FileResponse


# # @app.get("/download_symbol_table/{server_name}")
# # async def download_table(server_name: MT4_Server):
# #     print(server_name)
# #     query = f"""
# #     select distinct server, symbol, symbol_type
# #     from symbol_table
# #     where server = '{server_name}'
# #     limit 5
# #     """
# #     df = get_data(query, "datatp", "symbol")
# #     excel_file_path = os.path.join(config["output_folder"], "df_fastapi.xlsx")
# #     df.to_excel(excel_file_path)
# #     # df.to_dict()
# #     # return df.to_dict()

# #     headers = {"Content-Disposition": 'attachment; filename="Book.xlsx"'}
# #     return FileResponse(excel_file_path, headers=headers)


# # @app.post("/uploadfile/")
# # async def create_upload_file(file: UploadFile = File(...)):
# #     contents = await file.read()
# #     file_size = file.file.tell()
# #     # size validation
# #     if file_size > 50 * 1024 * 1024:
# #         raise HTTPException(status_code=400, detail="File too large.")

# #     # type validation
# #     if file.content_type in ["text/csv"]:
# #         df = pd.read_csv(BytesIO(contents))
# #     elif file.content_type in ["text/xlsx"]:
# #         df = pd.read_excel(BytesIO(contents))
# #     else:
# #         raise HTTPException(status_code=400, detail="Invalid file type.")
# #     # df = pd.read_excel(contents)
# #     # process the contents of the Excel file here
# #     return df.to_dict()


# # 增加OAuth2安全性認證 2022.10.24
# # from fastapi.security import OAuth2PasswordBearer
# # oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
# # @app.get("/check_oauth/")
# # async def read_items(token: str = Depends(oauth2_scheme)):
# #     return {"token": token}


# # class Image(BaseModel):
# #     url: HttpUrl
# #     name: str


# # class Item(BaseModel):
# #     name: str
# #     description: Union[str, None] = None
# #     price: float
# #     tax: Union[float, None] = None
# #     tags: Set[str] = set()
# #     images: Union[List[Image], None] = None


# # @app.put("/items123/{item_id}")
# # async def update_item(item_id: int, item: Item):
# #     results = {"item_id": item_id, "item": item}
# #     return results


# # # # path parameters
# # # @app.get("/items/{item_id}")
# # # async def read_item(item_id):
# # #     return {"item_id": item_id}

# # # Path parameters with types
# # @app.get("/items_int/{item_id}")
# # async def read_item(item_id: int):
# #     return {"item_id": item_id}

# # # 預設值路徑參數
# # # Enum
# # class WebsiteUserTypeEnum(str, Enum):
# #     DEMO_NAME = "DEMO_VALUE"
# #     INDIVIDUAL_NAME = "INDIVIDUAL_VALUE"

# # @app.get("/WebsiteUserTypeEnum/{enum_value}")
# # async def get_model(enum_value: WebsiteUserTypeEnum):
# #     test = WebsiteUserTypeEnum(enum_value).name
# #     return {"model_value": test}

# # class ModelName(str, Enum):
# #     alexnet = "alexnet1"
# #     resnet = "resnet2"
# #     lenet = "lenet3"

# # @app.get("/models/{model_name}")
# # async def get_model(model_name: ModelName):
# #     print(model_name)
# #     if model_name == ModelName.alexnet:
# #         return {"model_name": model_name, "message": "Deep Learning FTW!"}

# #     if model_name.value == "lenet":
# #         return {"model_name": model_name, "message": "LeCNN all the images"}

# #     return {"model_name": model_name, "message": "Have some residuals"}


# # # 查詢參數
# # fake_items_db = [{"item_name": "Foo"}, {"item_name": "Bar"}, {"item_name": "Baz"}]


# # @app.get("/items_query/")
# # async def read_item(skip: int = 0, limit: int = 10):
# #     return fake_items_db[skip : skip + limit]
# # # 可選參數、查詢參數類型轉換
# # @app.get("/items/{item_id}")
# # async def read_item(item_id: str, q: Optional[str] = None, short: bool = False):
# #     item = {"item_id": item_id}
# #     if q:
# #         item.update({"q": q})
# #     if not short:
# #         item.update(
# #             {"description": "This is an amazing item that has a long description"}
# #         )
# #     return item

# # # 多路徑參數
# # async def read_user_item(
# #     user_id: int, item_id: str, q: Optional[str] = None, short: bool = False
# # ):
# #     item = {"item_id": item_id, "owner_id": user_id}
# #     if q:
# #         item.update({"q": q})
# #     if not short:
# #         item.update(
# #             {"description": "This is an amazing item that has a long description"}
# #         )
# #     return item

# # # POST
# # # BaseModel:

# # class Item(BaseModel):
# #     name: str
# #     description: Optional[str] = None
# #     price: float
# #     tax: Optional[float] = None


# # @app.post("/items/")
# # async def create_item(item:Item):
# #     item_dict = item.dict()
# #     if item.tax:
# #         price_with_tax = item.price + item.tax
# #         item_dict.update({"price_with_tax": price_with_tax})
# #     return item_dict

# # # POST + 路徑參數 + 查詢參數
# # @app.put("/items/{item_id}")
# # async def create_item(item_id: int, item: Item, q: Optional[str] = None):
# #     result = {"item_id": item_id, **item.dict()}
# #     if q:
# #         result.update({"q": q})
# #     return result

# # # 額外訊息與查對
# # @app.get("/items/")
# # async def read_items(q: Optional[str] = None):
# #     results = {"items": [{"item_id": "Foo"}, {"item_id": "Bar"}]}
# #     if q:
# #         results.update({"q": q})
# #     return results


# # @app.get("/items3/")
# # async def read_items(
# #     q: Optional[str] = Query(None, min_length=3, max_length=50, regex="^fixedquery$")
# # ):
# #     results = {"items": [{"item_id": "Foo"}, {"item_id": "Bar"}]}
# #     if q:
# #         results.update({"q": q})
# #     return results


# # from package.db_helper import config, get_data

# # 練習連接mysql db symbol.symbol_table
# # 手動建立class

# # @app.get("/symbol_table/")
# # async def get_symbol_table(server):
# #     return fake_items_db[skip : skip + limit]


# # from typing import List, Set, Union

# # from fastapi import FastAPI
# # from pydantic import BaseModel, HttpUrl

# # app = FastAPI()


# # class Image(BaseModel):
# #     url: HttpUrl
# #     name: str


# # class Item(BaseModel):
# #     name: str
# #     description: Union[str, None] = None
# #     price: float
# #     tax: Union[float, None] = None
# #     tags: Set[str] = set()
# #     images: Union[List[Image], None] = None


# # @app.put("/items/{item_id}")
# # async def update_item(item_id: int, item: Item):
# #     results = {"item_id": item_id, "item": item}
# #     return results

# if __name__ == "__main__":
#     uvicorn.run("main:app", host="127.0.0.1", port=3000, reload=True)
