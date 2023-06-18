# 管理授權認證的檔案
from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from auth.crypt import ACCESS_TOKEN_EXPIRE_MINUTES

from sql_app.crud import create_access_token, get_db, get_user, verify_password
from sql_app.schemas import Token

router = APIRouter()

# 授權登入
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# 登入認證
@router.post("/token", response_model=Token, tags=["auth"])
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
