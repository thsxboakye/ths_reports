from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MS_SQL_DB: str 
    SERVER_PATH: str
    REMOTE_PATH:str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        
config=Settings()
    
