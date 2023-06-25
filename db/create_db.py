from db.database import Base, engine
from auth.models import Admin
from auth.dependencies import model_create_user

# Create the database tables
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)

# insert admin
model_create_user(Admin)
