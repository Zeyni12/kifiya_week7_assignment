from sqlalchemy import Column, Integer, String
from database import Base

class Item(Base):
    __tablename__ = "yolo_detection"
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    description = Column(String, index=True)