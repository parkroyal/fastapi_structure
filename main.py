import uvicorn
from fastapi import FastAPI  # import database
from db.database import Base, engine

# import auth
from auth.router import router as auth_router
from auth.dependencies import model_create_user
from auth.models import Admin


app = FastAPI()

# auth router
app.include_router(router=auth_router)
# symbol router

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=3000, reload=True)
