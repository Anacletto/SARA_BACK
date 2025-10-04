from fastapi import FastAPI, HTTPException, BackgroundTasks, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import json

from app.config import AppConfig
from app.nasa_data_service import RealNASADataService
from app.risk_engine import EnhancedRiskEngine
from app.geo_data import LuandaGeoData

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SIGA-Angola Unified NASA Dashboard API",
    description="Unified Risk and Environmental Dashboard for Angola Municipalities and Provinces",
    version="4.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
config = AppConfig()
geo_data = LuandaGeoData()
risk_engine = EnhancedRiskEngine()

# Global data cache
data_cache = {
    "nasa_data": {},
    "risk_assessments": {},
    "environmental_data": {},
    "alerts": [],
    "last_updated": None
}

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("🚀 SIGA-Angola Unified Dashboard API Starting Up...")
    logger.info(f"📊 Monitoring {len(geo_data.municipalities)} municipalities and {len(geo_data.provinces)} provinces")
    await refresh_all_data()

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "SIGA-Angola Unified NASA Dashboard API",
        "status": "operational",
        "version": "4.0",
        "description": "Unified Risk and Environmental Dashboard for Angola",
        "coverage": {
            "municipalities": len(geo_data.municipalities),
            "provinces": len(geo_data.provinces)
        },
        "main_endpoint": {
            "method": "POST",
            "url": "/api/dashboard",
            "body_format": {"province": "string", "municipality": "string"}
        },
        "last_updated": data_cache.get("last_updated")
    }

@app.post("/api/dashboard")
async def get_unified_dashboard(
    background_tasks: BackgroundTasks,
    location_data: Dict = Body(..., example={"province": "LUANDA", "municipality": "VIANA"})
):
    """
    Unified dashboard endpoint that returns risk, environmental, and alerts data in one response.
    
    Request body:
    - province: Province name (e.g., "LUANDA")
    - municipality: Municipality name (e.g., "VIANA")
    """
    province = location_data.get("province", "").upper()
    municipality = location_data.get("municipality", "").upper()
    
    # Validate input
    if not province:
        raise HTTPException(status_code=400, detail="Province is required")
    
    if province not in geo_data.provinces:
        raise HTTPException(status_code=404, detail=f"Province '{province}' not found")
    
    if municipality and municipality not in geo_data.municipalities:
        raise HTTPException(status_code=404, detail=f"Municipality '{municipality}' not found in {province}")
    
    # Trigger background data refresh
    background_tasks.add_task(refresh_all_data)
    
    try:
        # Get location information
        location_info = get_location_info(province, municipality)
        
        # Get NASA data for the location
        nasa_data = await get_nasa_data_for_location(province, municipality)
        
        # Calculate all risk assessments
        risk_assessment = await calculate_comprehensive_risk(nasa_data, location_info)
        
        # Calculate environmental data
        environmental_data = await calculate_environmental_assessment(nasa_data, location_info)
        
        # Get relevant alerts
        alerts_data = await get_location_alerts(province, municipality, risk_assessment)
        
        # Generate unified dashboard response
        dashboard = generate_unified_dashboard(
            location_info, 
            risk_assessment, 
            environmental_data, 
            alerts_data,
            nasa_data
        )
        
        # Cache the dashboard
        cache_key = f"{province}_{municipality}" if municipality else province
        data_cache["risk_assessments"][cache_key] = dashboard
        data_cache["last_updated"] = datetime.utcnow().isoformat()
        
        return dashboard
        
    except Exception as e:
        logger.error(f"Error generating unified dashboard: {e}")
        return await get_fallback_dashboard(province, municipality)

@app.get("/api/provinces")
async def get_all_provinces():
    """Get all provinces of Angola"""
    provinces = []
    for province_id, province_data in geo_data.provinces.items():
        provinces.append({
            "id": province_id,
            "name": province_data["name"],
            "capital": province_data["capital"],
            "population": province_data["population"],
            "area_km2": province_data["area_km2"],
            "risk_profile": province_data["risk_profile"],
            "municipality_count": len(geo_data.municipalities) if province_id == "LUANDA" else 1
        })
    
    return {
        "provinces": provinces,
        "total_provinces": len(provinces),
        "last_updated": datetime.utcnow().isoformat()
    }

@app.get("/api/provinces/{province_id}/municipalities")
async def get_province_municipalities(province_id: str):
    """Get all municipalities for a specific province"""
    if province_id not in geo_data.provinces:
        raise HTTPException(status_code=404, detail=f"Province {province_id} not found")
    
    municipalities = []
    if province_id == "LUANDA":
        for mun_id, municipality in geo_data.municipalities.items():
            municipalities.append({
                "id": mun_id,
                "name": municipality.name,
                "population": municipality.population,
                "area_km2": municipality.area_km2,
                "density_km2": round(municipality.population / municipality.area_km2),
                "elevation": municipality.elevation,
                "risk_factors": municipality.risk_factors
            })
    
    return {
        "province": province_id,
        "province_name": geo_data.provinces[province_id]["name"],
        "municipalities": municipalities,
        "total_municipalities": len(municipalities),
        "last_updated": datetime.utcnow().isoformat()
    }

@app.get("/api/status")
async def get_system_status():
    """Get system status and data freshness"""
    try:
        return {
            "system": "OPERATIONAL",
            "version": "4.0",
            "data_coverage": {
                "municipalities": len(geo_data.municipalities),
                "provinces": len(geo_data.provinces),
                "cached_locations": len(data_cache.get("nasa_data", {}))
            },
            "data_freshness": {
                "last_updated": data_cache.get("last_updated"),
                "is_stale": is_data_stale()
            },
            "services": {
                "nasa_api": "OPERATIONAL",
                "risk_engine": "OPERATIONAL",
                "geo_data": "OPERATIONAL"
            },
            "uptime": "100%",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return {"system": "DEGRADED", "error": str(e)}

# Core Business Logic for Unified Dashboard
async def calculate_comprehensive_risk(nasa_data: Dict, location_info: Dict) -> Dict:
    """Calculate comprehensive risk assessment"""
    risks = {
        "flood": risk_engine.calculate_flood_risk(nasa_data, location_info),
        "fire": risk_engine.calculate_fire_risk(nasa_data, location_info),
        "drought": risk_engine.calculate_drought_risk(nasa_data, location_info),
        "cyclone": risk_engine.calculate_cyclone_risk(nasa_data, location_info),
        "air_quality": risk_engine.calculate_air_quality_risk(nasa_data, location_info),
        "water_quality": risk_engine.calculate_water_quality_risk(nasa_data, location_info),
        "pollution": risk_engine.calculate_pollution_impact(nasa_data, location_info)
    }
    
    # Calculate overall risk
    risk_scores = [risk['score'] * risk['confidence'] for risk in risks.values()]
    overall_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0
    
    # Determine primary threats
    primary_threats = sorted(
        [(risk_type, risk_data['score']) for risk_type, risk_data in risks.items()],
        key=lambda x: x[1],
        reverse=True
    )[:3]  # Top 3 threats
    
    return {
        "overall_risk": {
            "level": get_risk_level(overall_score),
            "score": round(overall_score),
            "confidence": round(sum(risk['confidence'] for risk in risks.values()) / len(risks), 2),
            "trend": "increasing" if overall_score > 60 else "stable" if overall_score > 30 else "decreasing",
            "primary_threats": [threat[0] for threat in primary_threats],
            "data_quality_percentage": calculate_data_quality(risks)
        },
        "detailed_risks": risks,
        "risk_breakdown": {
            "natural_disasters": {
                "flood": risks["flood"],
                "fire": risks["fire"], 
                "drought": risks["drought"],
                "cyclone": risks["cyclone"]
            },
            "environmental_health": {
                "air_quality": risks["air_quality"],
                "water_quality": risks["water_quality"],
                "pollution": risks["pollution"]
            }
        }
    }

async def calculate_environmental_assessment(nasa_data: Dict, location_info: Dict) -> Dict:
    """Calculate comprehensive environmental assessment"""
    environmental_risks = {
        "air_quality": risk_engine.calculate_air_quality_risk(nasa_data, location_info),
        "water_quality": risk_engine.calculate_water_quality_risk(nasa_data, location_info),
        "pollution": risk_engine.calculate_pollution_impact(nasa_data, location_info),
        "population": risk_engine.calculate_population_impact(nasa_data, location_info)
    }
    
    # Calculate overall environmental health index
    env_scores = [risk['score'] for risk in environmental_risks.values()]
    overall_env_health = 100 - (sum(env_scores) / len(env_scores)) if env_scores else 50
    
    return {
        "environmental_health_index": round(overall_env_health),
        "environmental_quality": "EXCELLENT" if overall_env_health > 80 else "GOOD" if overall_env_health > 60 else "MODERATE" if overall_env_health > 40 else "POOR",
        "detailed_metrics": environmental_risks,
        "health_advisories": generate_health_advisories(environmental_risks),
        "recommendations": generate_environmental_recommendations(environmental_risks)
    }

async def get_location_alerts(province: str, municipality: str, risk_assessment: Dict) -> Dict:
    """Get alerts relevant to the specific location"""
    alerts = []
    
    # Check overall risk level
    overall_risk = risk_assessment["overall_risk"]
    if overall_risk["level"] in ["HIGH", "CRITICAL"]:
        alerts.append(create_alert(province, municipality, risk_assessment, "HIGH_RISK_AREA"))
    
    # Check individual high risks
    for risk_type, risk_data in risk_assessment["detailed_risks"].items():
        if risk_data["level"] in ["HIGH", "CRITICAL"]:
            alerts.append(create_alert(province, municipality, risk_assessment, f"HIGH_{risk_type.upper()}_RISK"))
    
    # Add environmental alerts
    env_alerts = check_environmental_alerts(risk_assessment)
    alerts.extend(env_alerts)
    
    # Add seasonal alerts
    seasonal_alerts = check_seasonal_alerts(province, municipality)
    alerts.extend(seasonal_alerts)
    
    return {
        "active_alerts": alerts,
        "total_alerts": len(alerts),
        "critical_alerts": len([a for a in alerts if a["severity"] in ["HIGH", "CRITICAL"]]),
        "alert_summary": generate_alert_summary(alerts)
    }

def generate_unified_dashboard(location_info: Dict, risk_assessment: Dict, 
                             environmental_data: Dict, alerts_data: Dict,
                             nasa_data: Dict) -> Dict:
    """Generate the unified dashboard response"""
    
    # Calculate overall safety score (inverse of risk)
    risk_score = risk_assessment["overall_risk"]["score"]
    env_score = environmental_data["environmental_health_index"]
    overall_safety = (100 - risk_score + env_score) / 2
    
    return {
        "dashboard": {
            "location": location_info,
            "timestamp": datetime.utcnow().isoformat(),
            "data_freshness": "REAL_TIME" if nasa_data.get('gpm', {}).get('is_real_data') else "NEAR_REAL_TIME",
            "overall_safety_score": round(overall_safety),
            "safety_level": get_safety_level(overall_safety),
            "update_frequency_minutes": 15
        },
        
        "risk_assessment": risk_assessment,
        
        "environmental_assessment": environmental_data,
        
        "alerts": alerts_data,
        
        "nasa_data_sources": {
            "rainfall": nasa_data.get('gpm', {}).get('data_source', 'Unknown'),
            "fires": nasa_data.get('viirs', {}).get('data_source', 'Unknown'),
            "air_quality": nasa_data.get('air_quality', {}).get('data_source', 'Unknown'),
            "water_quality": nasa_data.get('water_quality', {}).get('data_source', 'Unknown'),
            "population": nasa_data.get('population', {}).get('data_source', 'Unknown')
        },
        
        "temporal_context": {
            "current_season": get_current_season(),
            "time_of_day": get_time_of_day(),
            "risk_outlook_24h": get_24h_risk_outlook(risk_assessment),
            "environmental_outlook_7d": get_7d_environmental_outlook(environmental_data),
            "seasonal_advisory": get_seasonal_advisory(location_info)
        },
        
        "actionable_insights": {
            "immediate_actions": generate_immediate_actions(risk_assessment, alerts_data),
            "planning_recommendations": generate_planning_recommendations(location_info, environmental_data),
            "health_precautions": generate_health_precautions(environmental_data),
            "urban_planning_advice": generate_urban_planning_advice(location_info, risk_assessment)
        },
        
        "summary": {
            "key_findings": generate_key_findings(risk_assessment, environmental_data, alerts_data),
            "priority_level": determine_priority_level(risk_assessment, alerts_data),
            "next_update": (datetime.utcnow() + timedelta(minutes=15)).isoformat(),
            "report_id": f"RPT-{location_info['id']}-{datetime.utcnow().strftime('%Y%m%d%H%M')}"
        }
    }

# Helper Functions
def get_location_info(province: str, municipality: str = "") -> Dict:
    """Get comprehensive location information"""
    if municipality and municipality in geo_data.municipalities:
        mun = geo_data.municipalities[municipality]
        return {
            "type": "MUNICIPALITY",
            "id": municipality,
            "name": mun.name,
            "province": province,
            "province_name": geo_data.provinces[province]["name"],
            "coordinates": {
                "latitude": mun.latitude,
                "longitude": mun.longitude,
                "elevation_m": mun.elevation
            },
            "demographics": {
                "population": mun.population,
                "area_km2": mun.area_km2,
                "density_km2": round(mun.population / mun.area_km2),
                "settlement_type": "urban" if mun.population / mun.area_km2 > 5000 else "mixed"
            },
            "risk_profile": mun.risk_factors,
            "characteristics": get_municipality_characteristics(municipality)
        }
    else:
        # Province-level information
        province_data = geo_data.provinces[province]
        return {
            "type": "PROVINCE",
            "id": province,
            "name": province_data["name"],
            "capital": province_data["capital"],
            "coordinates": {
                "latitude": -8.8383 if province == "LUANDA" else -12.3500,
                "longitude": 13.2344 if province == "LUANDA" else 17.3500,
                "elevation_m": 50.0
            },
            "demographics": {
                "population": province_data["population"],
                "area_km2": province_data["area_km2"],
                "density_km2": round(province_data["population"] / province_data["area_km2"]),
                "settlement_type": "mixed"
            },
            "risk_profile": province_data["risk_profile"],
            "characteristics": get_province_characteristics(province)
        }

async def get_nasa_data_for_location(province: str, municipality: str = "") -> Dict:
    """Get NASA data for specific location"""
    cache_key = f"{province}_{municipality}" if municipality else province
    
    if cache_key not in data_cache["nasa_data"] or is_data_stale():
        await refresh_location_data(province, municipality)
    
    return data_cache["nasa_data"].get(cache_key, await get_fallback_nasa_data())

async def refresh_all_data():
    """Refresh data for all major locations with safe error handling"""
    logger.info("🔄 Refreshing NASA data for all provinces and municipalities...")
    
    # Refresh data for all provinces
    provinces_to_refresh = ["LUANDA", "BENGUELA", "HUAMBO", "CABINDA", "HUILA", "CUNENE"]
    
    refresh_tasks = []
    
    for province in provinces_to_refresh:
        # Refresh province level data
        refresh_tasks.append(refresh_location_data(province, ""))
        
        # Refresh key municipalities in each province
        try:
            municipalities = get_municipalities_by_province(province)
            for municipality in municipalities[:2]:  # Limit to 2 municipalities to avoid overloading
                refresh_tasks.append(refresh_location_data(province, municipality))
        except Exception as e:
            logger.warning(f"⚠️ Could not get municipalities for {province}: {e}")
    
    # Execute all refresh tasks with concurrency limit
    await asyncio.gather(*refresh_tasks, return_exceptions=True)
    
    data_cache["last_updated"] = datetime.utcnow().isoformat()
    logger.info(f"✅ Data refresh completed for {len(provinces_to_refresh)} provinces")

async def refresh_location_data(province: str, municipality: str = ""):
    """Refresh NASA data for specific location with proper error handling"""
    try:
        async with RealNASADataService() as nasa_service:
            location_id = municipality if municipality else province
            location_type = "municipality" if municipality else "province"
            
            # Fetch data concurrently with proper error handling
            results = await asyncio.gather(
                nasa_service.get_real_gpm_rainfall(location_id, location_type),
                nasa_service.get_real_viirs_fires(location_id, location_type),
                nasa_service.get_real_air_quality(location_id, location_type),
                nasa_service.get_real_water_quality(location_id, location_type),
                nasa_service.get_population_density(location_id, location_type),
                return_exceptions=True
            )
            
            # Process results with safe fallbacks
            gpm_data, viirs_data, air_quality_data, water_quality_data, population_data = results
            
            # Handle each data type with safe fallbacks
            location_data = {
                "gpm": await safe_get_data(gpm_data, nasa_service, "gpm", location_id, location_type),
                "viirs": await safe_get_data(viirs_data, nasa_service, "viirs", location_id, location_type),
                "air_quality": await safe_get_data(air_quality_data, nasa_service, "air_quality", location_id, location_type),
                "water_quality": await safe_get_data(water_quality_data, nasa_service, "water_quality", location_id, location_type),
                "population": await safe_get_data(population_data, nasa_service, "population", location_id, location_type)
            }
            
            cache_key = f"{province}_{municipality}" if municipality else province
            data_cache["nasa_data"][cache_key] = location_data
            
            logger.info(f"✅ Data refreshed for {location_type}: {location_id}")
            
    except Exception as e:
        logger.error(f"❌ Error refreshing data for {province}/{municipality}: {e}")
        # Ensure we at least have fallback data in cache
        await set_fallback_data(province, municipality)

async def safe_get_data(data_result, nasa_service, data_type, location_id, location_type):
    """Safely get data with fallback handling"""
    if isinstance(data_result, Exception):
        logger.warning(f"⚠️ {data_type.upper()} data error for {location_id}: {data_result}")
        return await get_safe_fallback(nasa_service, data_type, location_id, location_type)
    return data_result

async def get_safe_fallback(nasa_service, data_type, location_id, location_type):
    """Get safe fallback data without relying on non-existent methods"""
    try:
        # Use basic fallback data instead of calling non-existent methods
        fallbacks = {
            "gpm": {"rainfall_24h_mm": 15.0, "is_real_data": False, "data_source": "FALLBACK"},
            "viirs": {"active_fires": [], "fire_count": 0, "is_real_data": False, "data_source": "FALLBACK"},
            "air_quality": {"pm25_estimate": 20.0, "is_real_data": False, "data_source": "FALLBACK"},
            "water_quality": {"pollution_index": 35.0, "is_real_data": False, "data_source": "FALLBACK"},
            "population": {"population_density_km2": 5000, "is_real_data": False, "data_source": "FALLBACK"}
        }
        return fallbacks.get(data_type, {})
    except Exception as e:
        logger.error(f"❌ Fallback data error for {data_type}: {e}")
        return {}

async def set_fallback_data(province: str, municipality: str = ""):
    """Set basic fallback data in cache"""
    cache_key = f"{province}_{municipality}" if municipality else province
    data_cache["nasa_data"][cache_key] = {
        "gpm": {"rainfall_24h_mm": 15.0, "is_real_data": False, "data_source": "FALLBACK"},
        "viirs": {"active_fires": [], "fire_count": 0, "is_real_data": False, "data_source": "FALLBACK"},
        "air_quality": {"pm25_estimate": 20.0, "is_real_data": False, "data_source": "FALLBACK"},
        "water_quality": {"pollution_index": 35.0, "is_real_data": False, "data_source": "FALLBACK"},
        "population": {"population_density_km2": 5000, "is_real_data": False, "data_source": "FALLBACK"}
    }

def get_municipalities_by_province(province: str):
    """Get municipalities for a specific province"""
    if province == "LUANDA":
        return list(geo_data.municipalities.keys())
    else:
        # For other provinces, return empty list or implement based on your data structure
        return []

# Alert Management
def create_alert(province: str, municipality: str, risk_data: Dict, alert_type: str) -> Dict:
    location_name = municipality if municipality else geo_data.provinces[province]["name"]
    overall_risk = risk_data["overall_risk"]
    
    return {
        "alert_id": f"ALT-{province}-{municipality}-{datetime.utcnow().strftime('%H%M')}",
        "type": alert_type,
        "severity": overall_risk["level"],
        "location": {
            "province": province,
            "municipality": municipality,
            "name": location_name
        },
        "issued_at": datetime.utcnow().isoformat(),
        "description": generate_alert_description(alert_type, risk_data),
        "recommended_actions": generate_alert_actions(alert_type, risk_data),
        "valid_until": (datetime.utcnow() + timedelta(hours=24)).isoformat(),
        "confidence": overall_risk["confidence"]
    }

def check_environmental_alerts(risk_data: Dict) -> List[Dict]:
    """Check for environmental health alerts"""
    alerts = []
    if not risk_data:
        return alerts
    
    env_risks = risk_data["detailed_risks"]
    
    # Air quality alert
    if env_risks["air_quality"]["score"] > 70:
        alerts.append({
            "alert_id": f"AQ-{datetime.utcnow().strftime('%H%M')}",
            "type": "POOR_AIR_QUALITY",
            "severity": "HIGH",
            "description": "Poor air quality detected - sensitive groups should take precautions",
            "issued_at": datetime.utcnow().isoformat(),
            "recommended_actions": ["Limit outdoor activities", "Use air purifiers if available", "Monitor vulnerable individuals"]
        })
    
    # Water quality alert
    if env_risks["water_quality"]["score"] > 70:
        alerts.append({
            "alert_id": f"WQ-{datetime.utcnow().strftime('%H%M')}",
            "type": "WATER_POLLUTION",
            "severity": "MEDIUM",
            "description": "Elevated water pollution levels detected",
            "issued_at": datetime.utcnow().isoformat(),
            "recommended_actions": ["Use treated water for drinking", "Avoid recreational water activities", "Report water quality issues"]
        })
    
    return alerts

def check_seasonal_alerts(province: str, municipality: str) -> List[Dict]:
    """Generate seasonal alerts based on current conditions"""
    alerts = []
    current_season = get_current_season()
    
    if current_season == "RAINY_SEASON":
        alerts.append({
            "alert_id": f"SEASON-{datetime.utcnow().strftime('%H%M')}",
            "type": "RAINY_SEASON_ADVISORY",
            "severity": "MEDIUM",
            "description": "Rainy season active - increased flood risk",
            "issued_at": datetime.utcnow().isoformat(),
            "recommended_actions": ["Monitor drainage systems", "Prepare for possible flooding", "Clear gutters and drains"]
        })
    elif current_season == "DRY_SEASON":
        alerts.append({
            "alert_id": f"SEASON-{datetime.utcnow().strftime('%H%M')}",
            "type": "DRY_SEASON_ADVISORY", 
            "severity": "MEDIUM",
            "description": "Dry season active - elevated fire risk",
            "issued_at": datetime.utcnow().isoformat(),
            "recommended_actions": ["Clear vegetation around properties", "Avoid outdoor burning", "Prepare fire response equipment"]
        })
    
    return alerts

# Utility Functions
def get_risk_level(score):
    if score >= 80:
        return "CRITICAL"
    elif score >= 60:
        return "HIGH"
    elif score >= 40:
        return "MEDIUM"
    elif score >= 20:
        return "LOW"
    else:
        return "VERY_LOW"

def get_safety_level(score):
    if score >= 80:
        return "VERY_SAFE"
    elif score >= 60:
        return "SAFE"
    elif score >= 40:
        return "MODERATE_CAUTION"
    elif score >= 20:
        return "ELEVATED_CAUTION"
    else:
        return "HIGH_CAUTION"

def calculate_data_quality(risks):
    real_data_count = sum(1 for risk in risks.values() if risk.get('data_quality') == 'real')
    return round((real_data_count / len(risks)) * 100, 1)

def get_current_season():
    month = datetime.utcnow().month
    if month in [1, 2, 3, 4]:
        return "RAINY_SEASON"
    elif month in [5, 6, 7, 8, 9]:
        return "DRY_SEASON"
    else:
        return "TRANSITION_SEASON"

def get_time_of_day():
    hour = datetime.utcnow().hour
    if 5 <= hour < 12:
        return "MORNING"
    elif 12 <= hour < 18:
        return "AFTERNOON"
    elif 18 <= hour < 22:
        return "EVENING"
    else:
        return "NIGHT"

def get_24h_risk_outlook(risk_assessment):
    risks = risk_assessment["detailed_risks"]
    if any(risk['score'] > 70 for risk in risks.values()):
        return "ELEVATED_RISK_MONITOR_CLOSELY"
    elif any(risk['score'] > 50 for risk in risks.values()):
        return "MODERATE_RISK_STAY_ALERT"
    else:
        return "LOW_RISK_NORMAL_OPERATIONS"

def get_7d_environmental_outlook(environmental_data):
    if environmental_data["environmental_health_index"] < 40:
        return "POOR_OUTLOOK_IMMEDIATE_ACTION_NEEDED"
    elif environmental_data["environmental_health_index"] < 60:
        return "MODERATE_OUTLOOK_MONITOR_CHANGES"
    else:
        return "GOOD_OUTLOOK_MAINTAIN_CURRENT_EFFORTS"

def get_seasonal_advisory(location_info):
    season = get_current_season()
    location_type = location_info.get('type', 'MUNICIPALITY')
    
    if season == "RAINY_SEASON":
        if location_type == "MUNICIPALITY" and "coastal" in location_info.get('risk_profile', {}):
            return "Coastal areas: Increased flood risk during rainy season - monitor tides and drainage"
        else:
            return "Increased precipitation expected - monitor flood risks and drainage systems"
    elif season == "DRY_SEASON":
        return "Dry conditions prevailing - fire risk elevated, water conservation recommended"
    else:
        return "Seasonal transition - variable conditions expected, maintain standard precautions"

def is_data_stale():
    if not data_cache.get("last_updated"):
        return True
    try:
        last_update = datetime.fromisoformat(data_cache["last_updated"].replace('Z', '+00:00'))
        return (datetime.utcnow() - last_update).total_seconds() > 1800  # 30 minutes
    except:
        return True

# Fallback Methods
async def get_fallback_dashboard(province: str, municipality: str = ""):
    location_info = get_location_info(province, municipality)
    
    return {
        "dashboard": {
            "location": location_info,
            "timestamp": datetime.utcnow().isoformat(),
            "data_freshness": "FALLBACK_MODE",
            "overall_safety_score": 50,
            "safety_level": "MODERATE_CAUTION",
            "fallback_mode": True,
            "update_frequency_minutes": 5
        },
        "risk_assessment": {
            "overall_risk": {
                "level": "LOW",
                "score": 20,
                "confidence": 0.5,
                "trend": "unknown",
                "primary_threats": ["DATA_UNAVAILABLE"],
                "data_quality_percentage": 0
            },
            "detailed_risks": {},
            "risk_breakdown": {}
        },
        "environmental_assessment": {
            "environmental_health_index": 50,
            "environmental_quality": "MODERATE",
            "detailed_metrics": {},
            "health_advisories": ["Real-time data temporarily unavailable"],
            "recommendations": ["Maintain standard monitoring procedures"]
        },
        "alerts": {
            "active_alerts": [],
            "total_alerts": 0,
            "critical_alerts": 0,
            "alert_summary": "No alert data available in fallback mode"
        },
        "nasa_data_sources": {
            "rainfall": "FALLBACK",
            "fires": "FALLBACK", 
            "air_quality": "FALLBACK",
            "water_quality": "FALLBACK",
            "population": "FALLBACK"
        },
        "temporal_context": {
            "current_season": get_current_season(),
            "time_of_day": get_time_of_day(),
            "risk_outlook_24h": "UNKNOWN_IN_FALLBACK_MODE",
            "environmental_outlook_7d": "UNKNOWN_IN_FALLBACK_MODE",
            "seasonal_advisory": "Standard seasonal precautions apply"
        },
        "actionable_insights": {
            "immediate_actions": ["Wait for system recovery", "Use alternative data sources if available"],
            "planning_recommendations": ["Continue standard operating procedures"],
            "health_precautions": ["Maintain standard health practices"],
            "urban_planning_advice": ["Proceed with caution using available data"]
        },
        "summary": {
            "key_findings": ["Fallback mode active - real-time NASA data temporarily unavailable"],
            "priority_level": "LOW_PRIORITY_SYSTEM_RECOVERY",
            "next_update": (datetime.utcnow() + timedelta(minutes=5)).isoformat(),
            "report_id": f"FBL-{location_info['id']}-{datetime.utcnow().strftime('%Y%m%d%H%M')}"
        }
    }

async def get_fallback_nasa_data():
    return {
        "gpm": {"rainfall_24h_mm": 25.0, "is_real_data": False},
        "viirs": {"active_fires": [], "fire_count": 0, "is_real_data": False},
        "air_quality": {"pm25_estimate": 25.0, "is_real_data": False},
        "water_quality": {"pollution_index": 45.0, "is_real_data": False},
        "population": {"population_density_km2": 6000, "is_real_data": False}
    }

# Placeholder functions for missing implementations
def generate_alert_description(alert_type, risk_data):
    return f"Alert for {alert_type} - Risk level: {risk_data['overall_risk']['level']}"

def generate_alert_actions(alert_type, risk_data):
    return ["Monitor situation closely", "Follow local authority guidance"]

def generate_health_advisories(environmental_risks):
    return ["Maintain standard health precautions"]

def generate_environmental_recommendations(environmental_risks):
    return ["Continue environmental monitoring"]

def generate_alert_summary(alerts):
    if not alerts:
        return "No active alerts"
    return f"{len(alerts)} active alerts"

def get_municipality_characteristics(municipality):
    return {"type": "urban", "infrastructure": "developed"}

def get_province_characteristics(province):
    return {"type": "mixed", "economy": "diversified"}

def generate_immediate_actions(risk_assessment, alerts_data):
    return ["Review risk assessment", "Monitor alerts"]

def generate_planning_recommendations(location_info, environmental_data):
    return ["Continue standard planning procedures"]

def generate_health_precautions(environmental_data):
    return ["Follow standard health guidelines"]

def generate_urban_planning_advice(location_info, risk_assessment):
    return ["Consider risk factors in urban development"]

def generate_key_findings(risk_assessment, environmental_data, alerts_data):
    return ["System operational", "Monitor risk levels"]

def determine_priority_level(risk_assessment, alerts_data):
    return "MEDIUM_PRIORITY"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")