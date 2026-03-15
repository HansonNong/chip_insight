class Config:
    """Application configuration constants."""
    
    APP_TITLE: str = "ChipInSight - 股票交易记录管理"
    HOST: str = "0.0.0.0"
    PORT: int = 8080
    
    LOG_FILE: str = "chipinsight.log"
    LOG_MAX_BYTES: int = 5 * 1024 * 1024
    LOG_BACKUP_COUNT: int = 3
    
    RELOAD: bool = False
    SHOW: bool = False