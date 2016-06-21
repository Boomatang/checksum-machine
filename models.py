from sqlalchemy import Column, String, DateTime, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
engine = create_engine('sqlite:///data1.db')


class CheckObjs(Base):
    __tablename__ = "check_objects"

    id = Column(Integer, primary_key=True)
    file_name = Column(String)
    file_ext = Column(String)
    full_path = Column(String)
    added_date = Column(DateTime)
    check_count = Column(Integer, default=0)
    checksum = Column(String)
    last_checked = Column(DateTime)


class Folders(Base):
    __tablename__ = 'folder_names'

    id = Column(Integer, primary_key=True)
    folder_path = Column(String)
    added_date = Column(DateTime)
    check_count = Column(Integer, default=0)
    last_checked = Column(DateTime)


if __name__ == '__main__':
    Base.metadata.create_all(engine)
