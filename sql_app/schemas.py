# Pydantic Model
from pydantic import BaseModel
from typing import Union, List


# User Class
class User(BaseModel):
    username: str
    email: Union[str, None] = None
    disabled: Union[bool, None] = None
    permissions: str
    # brand: List[str]

    class Config:
        orm_mode = True  # 導入SQL ORM


class UserInDB(User):
    hashed_password: str


# Auth Token
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None
