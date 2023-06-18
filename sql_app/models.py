# SQL ORM Model
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, BOOLEAN
from sqlalchemy.orm import relationship

from .database import Base


# User model
class User(Base):
    __tablename__ = "users"
    username = Column(String, primary_key=True, index=True)
    email = Column(String, unique=True)
    hashed_password = Column(String)
    disabled = Column(BOOLEAN)
    permissions = Column(String)
