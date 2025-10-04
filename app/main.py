from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import logging
from .config import AppConfig
from .geo_data import load_provinces, load_municipalities
from .nasa_data_service import RealNASADataService
from .risk_engine import RiskEngine
import os

logging.basicConfig(level=AppConfig.LOG_LEVEL)
logger = logging.getLogger(__name__)

app = FastAPI(title="SIGA-Angola Unified Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

provinces, municipalities = {}, {}
nasa_service, risk_engine = None, None

@app.on_event("startup")
async def startup_event():
    global provinces, municipalities, nasa_service, risk_engine
    provinces = load_provinces()
    municipalities = load_municipalities()
    nasa_service = RealNASADataService()
    risk_engine = RiskEngine()
    logger.info("🚀 SIGA-Angola Unified Dashboard API Starting Up...")
    logger.info(f"📊 Monitoring {len(municipalities)} municipalities and {len(provinces)} provinces")

@app.on_event("shutdown")
async def shutdown_event():
    if nasa_service:
        await nasa_service.close()
    logger.info("🛑 API shutdown complete")

@app.get("/", response_class=HTMLResponse)
async def root():
    with open(os.path.join("templates", "index.html")) as f:
        return HTMLResponse(content=f.read())

@app.get("/api/status")
async def get_status():
    return {
        "status": "running",
        "environment": AppConfig.ENVIRONMENT,
        "provinces": len(provinces),
        "municipalities": len(municipalities),
        "auth_method": nasa_service.auth_method if nasa_service else "unknown"
    }


@app.get("/api/provinces")
async def get_all_provinces():
    province_list = []
    for province_id, province_data in provinces.items():
        province_list.append({
            "id": province_id,
            "name": province_data["name"],
            "capital": province_data["capital"],
            "population": province_data["population"],
            "area_km2": province_data["area_km2"],
            "municipality_count": len(province_data["municipalities"]),
            "municipalities": province_data["municipalities"]
        })
    return province_list

@app.get("/api/{province}")
async def get_province(province: str):
    if province not in provinces:
        raise HTTPException(status_code=404, detail="Province not found")
    return {"province": province, "municipalities": municipalities.get(province, [])}



@app.get("/api/{province}/{municipality}")
async def get_municipality(province: str, municipality: str):
    if province not in provinces or municipality not in municipalities.get(province, []):
        raise HTTPException(status_code=404, detail="Municipality not found")
    data = await get_location_alerts(province, municipality)
    return {"province": province, "municipality": municipality, "alerts": data}

async def get_location_alerts(province, municipality):
    rainfall = await nasa_service.get_real_rainfall(province, municipality)
    fire = await nasa_service.get_real_fire_data(province, municipality)
    airq = await nasa_service.get_real_air_quality(province, municipality)
    waterq = await nasa_service.get_real_water_quality(province, municipality)
    pop = await nasa_service.get_real_population_data(province, municipality)

    risk_engine.update_historical_data(rainfall, fire, airq, waterq, pop)

    return {
        "flood_risk": risk_engine.calculate_flood_risk(rainfall, waterq),
        "fire_risk": risk_engine.calculate_fire_risk(fire),
        "drought_risk": risk_engine.calculate_drought_risk(rainfall),
        "air_quality_risk": risk_engine.calculate_air_quality_risk(airq),
        "water_quality_risk": risk_engine.calculate_water_quality_risk(waterq),
        "population_pressure": risk_engine.calculate_population_pressure(pop)
    }
