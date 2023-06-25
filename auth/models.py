# SQL ORM Model
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, BOOLEAN
from sqlalchemy.orm import relationship

from db.database import Base


# User model
class User(Base):
    __tablename__ = "users"
    username = Column(String, primary_key=True, index=True)
    email = Column(String, unique=True)
    hashed_password = Column(String)
    disabled = Column(BOOLEAN)
    permissions = Column(String)


Admin = User(
    username="steven",
    email="steven@example.com",
    hashed_password="$2b$12$K/CUNFaAjNJBQV8MME9s4eVL0MHHOxKBPSMbZvppW4EouTNxVoK4G",  # password = 123
    disabled=False,
    permissions="admin",
)
