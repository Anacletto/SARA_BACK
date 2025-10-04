import os
from dotenv import load_dotenv

load_dotenv()

class NASAConfig:
    # Primary API Token (Preferred)
    EARTHDATA_API_TOKEN = os.getenv('NASA_EARTHDATA_API_TOKEN')
    
    # Service-specific tokens (Fallback)
    GPM_API_TOKEN = os.getenv('NASA_GPM_API_TOKEN')
    MODIS_API_TOKEN = os.getenv('NASA_MODIS_API_TOKEN') 
    VIIRS_API_TOKEN = os.getenv('NASA_VIIRS_API_TOKEN')
    
    # Legacy credentials (Last resort)
    EARTHDATA_USERNAME = os.getenv('NASA_EARTHDATA_USERNAME')
    EARTHDATA_PASSWORD = os.getenv('NASA_EARTHDATA_PASSWORD')
    
    # API Endpoints
    GPM_BASE_URL = "https://gpm1.gesdisc.eosdis.nasa.gov/opendap"
    FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api"
    LADS_BASE_URL = "https://ladsweb.modaps.eosdis.nasa.gov/api/v2"
    WORLDVIEW_BASE_URL = "https://wvs.earthdata.nasa.gov/api/v1"
    GIBS_BASE_URL = "https://gibs.earthdata.nasa.gov"
    SEDAC_BASE_URL = "https://sedac.ciesin.columbia.edu/arcgis/rest/services"
    
    # Luanda Province bounding box (covers all municipalities)
    LUANDA_BBOX = [12.8, -9.2, 13.8, -8.5]
    
    # Authentication method to use
    AUTH_METHOD = "token" if EARTHDATA_API_TOKEN else "legacy" if EARTHDATA_USERNAME and EARTHDATA_PASSWORD else "none"

    # New data sources for environmental monitoring
    AIR_QUALITY_SOURCES = ["MODIS_Aerosol", "VIIRS_Aerosol", "TROPOMI"]
    WATER_QUALITY_SOURCES = ["Landsat_8", "MODIS_Ocean", "VIIRS_Ocean"]
    POPULATION_SOURCES = ["SEDAC_Population", "VIIRS_Night_Lights"]


class AppConfig:
    CACHE_TIMEOUT = 300  # 5 minutes
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')


# For backward compatibility
Config = NASAConfig