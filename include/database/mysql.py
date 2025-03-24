from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from airflow.hooks.base import BaseHook


config = BaseHook.get_connection('mysql')
DATABASE_URL = f"mysql+pymysql://{config.login}:{config.password}@{config.host}:{config.port}/{config.schema}"
engine = create_engine(
    DATABASE_URL, echo=False, pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=3600,
    connect_args={'connect_timeout': 10}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db_session = scoped_session(SessionLocal)
Base = declarative_base()