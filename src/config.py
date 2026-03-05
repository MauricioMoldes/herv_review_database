from pydantic import BaseSettings

class Settings(BaseSettings):
    # Database
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # JWT / Auth
    secret_key: str
    access_token_expire_minutes: int = 60
    algorithm: str = "HS256"

    # Admin
    admin_username: str
    admin_password: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Load settings once
settings = Settings()
