import os
import ssl
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

# Load environmental variables
load_dotenv()

DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_USER     = os.getenv("DB_USER",     "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_NAME     = os.getenv("DB_NAME",     "tamil_movies_db")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_SSL      = os.getenv("DB_SSL",      "false").lower() == "true"  # set true for Aiven

# Auto-create the database only for local MySQL (Aiven/cloud DBs pre-create it)
if not DB_SSL:
    try:
        conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
        conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.commit()
        conn.close()
    except Exception:
        pass

# Connection string for MySQL using pymysql
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SSL connect args — required for Aiven MySQL
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE
connect_args = {"ssl": _ssl_ctx} if DB_SSL else {}

# Create engine & session
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
