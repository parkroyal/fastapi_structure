# 管理授權認證的檔案
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from auth.config import ACCESS_TOKEN_EXPIRE_MINUTES
from auth.dependencies import (
    create_access_token,
    get_db,
    get_user,
    verify_password,
    get_current_user,
)
from auth.schemas import Token, User

router = APIRouter(tags=["auth"])

# 授權登入
from auth.dependencies import oauth2_scheme

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# 登入認證
@router.post("/token", response_model=Token, tags=["auth"])
def login_for_access_token(
    db: Session = Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()
):
    user = get_user(db, username=form_data.username)  # 從db拿取該username資料

    if user:
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


# user info
@router.get("/users/me/", response_model=User, tags=["User", "Auth"])
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user
