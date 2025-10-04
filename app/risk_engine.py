import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class EnhancedRiskEngine:
    def __init__(self):
        self.historical_data = self._initialize_historical_data()
    
    def _initialize_historical_data(self):
        """Initialize with Luanda's actual historical environmental data"""
        return {
            "flood_events": [
                {"date": "2023-12-20", "rainfall": 185, "impact": "high", "zone": "LUANDA_ILHA"},
                {"date": "2023-11-15", "rainfall": 120, "impact": "medium", "zone": "LUANDA_CACUACO"},
                {"date": "2023-03-10", "rainfall": 210, "impact": "high", "zone": "LUANDA_BAIXA"}
            ],
            "air_quality_events": [
                {"date": "2023-08-15", "aqi": 156, "pollutant": "PM2.5", "zone": "LUANDA_VIANA"},
                {"date": "2023-09-20", "aqi": 143, "pollutant": "PM10", "zone": "LUANDA_CACUACO"}
            ],
            "water_quality_events": [
                {"date": "2023-04-10", "turbidity": 0.8, "impact": "high", "zone": "LUANDA_ILHA"}
            ],
            "population_trends": [
                {"year": 2023, "growth_rate": 3.5, "density_increase": 4.2}
            ]
        }
    
    def calculate_flood_risk(self, nasa_data, location_info):
        """Calculate flood risk using real NASA data"""
        try:
            rainfall_data = nasa_data['gpm']
            rainfall_24h = rainfall_data['rainfall_24h_mm']
            forecast_48h = rainfall_data.get('forecast_48h_mm', 0)
            confidence = rainfall_data.get('confidence', 0.7)
            
            # Base risk from current rainfall
            if rainfall_24h > 100:
                base_score = 80 + min(20, (rainfall_24h - 100) / 5)
            elif rainfall_24h > 50:
                base_score = 55 + min(25, (rainfall_24h - 50) / 2)
            elif rainfall_24h > 25:
                base_score = 35 + min(20, rainfall_24h - 25)
            else:
                base_score = max(10, rainfall_24h / 2)
            
            # Forecast adjustment
            forecast_boost = min(25, forecast_48h / 4)
            
            # Location vulnerability adjustment
            location_boost = self._get_location_vulnerability(location_info, "flood")
            
            # Data confidence adjustment
            confidence_adjustment = confidence * 10
            
            total_score = base_score + forecast_boost + location_boost + confidence_adjustment
            
            risk_level = self._get_risk_level(total_score)
            
            return {
                "level": risk_level,
                "score": min(98, int(total_score)),
                "confidence": confidence,
                "factors": {
                    "current_rainfall": rainfall_24h,
                    "forecast_rainfall": forecast_48h,
                    "location_vulnerability": location_boost,
                    "data_confidence": confidence_adjustment
                },
                "prediction_horizon_hours": 48,
                "flood_type": self._get_flood_type(rainfall_24h, location_info),
                "expected_impact_areas": self._get_impact_areas(location_info, total_score),
                "data_quality": "real" if nasa_data['gpm'].get('is_real_data', False) else "simulated"
            }
            
        except Exception as e:
            logger.error(f"Flood risk calculation error: {e}")
            return self._get_flood_fallback(location_info)
    
    def calculate_fire_risk(self, nasa_data, location_info):
        """Calculate fire risk using real VIIRS data"""
        try:
            fire_data = nasa_data['viirs']
            vegetation_data = nasa_data.get('modis', {})
            
            fire_count = fire_data['fire_count']
            fire_risk_score = fire_data.get('fire_risk_score', 0)
            vegetation_health = vegetation_data.get('vegetation_health', 'moderate')
            ndvi = vegetation_data.get('ndvi', 0.5)
            
            # Base score from active fires
            if fire_count > 0:
                base_score = 85 + min(15, fire_count * 3)
                confidence = 0.95
            else:
                base_score = fire_risk_score
                confidence = 0.75
            
            # Vegetation dryness adjustment
            if vegetation_health == "poor":
                base_score += 20
            elif vegetation_health == "critical":
                base_score += 30
            
            # NDVI-based adjustment
            if ndvi < 0.3:
                base_score += 25
            elif ndvi < 0.4:
                base_score += 15
            elif ndvi < 0.5:
                base_score += 5
            
            # Seasonal adjustment
            seasonal_boost = self._get_seasonal_fire_boost()
            base_score += seasonal_boost
            
            # Location-specific risk
            location_boost = self._get_location_vulnerability(location_info, "fire")
            base_score += location_boost
            
            risk_level = self._get_risk_level(base_score)
            
            return {
                "level": risk_level,
                "score": min(95, int(base_score)),
                "confidence": confidence,
                "details": {
                    "active_fires": fire_count,
                    "fire_intensity": fire_data.get('total_brightness', 0),
                    "vegetation_condition": vegetation_health,
                    "seasonal_risk": seasonal_boost,
                    "location_risk": location_boost
                },
                "data_quality": "real" if nasa_data['viirs'].get('is_real_data', False) else "simulated"
            }
            
        except Exception as e:
            logger.error(f"Fire risk calculation error: {e}")
            return self._get_fire_fallback(location_info)
    
    def calculate_drought_risk(self, nasa_data, location_info):
        """Calculate drought risk using real MODIS data"""
        try:
            modis_data = nasa_data.get('modis', {})
            vegetation_data = nasa_data.get('vegetation', {})
            
            ndvi = modis_data.get('ndvi', 0.5)
            drought_index = modis_data.get('drought_index', 0)
            vegetation_health = vegetation_data.get('vegetation_health', 'moderate')
            
            # Composite drought score
            base_score = drought_index
            
            # NDVI-based adjustment
            if ndvi < 0.3:
                base_score += 30
            elif ndvi < 0.4:
                base_score += 20
            elif ndvi < 0.5:
                base_score += 10
            
            # Vegetation health adjustment
            if vegetation_health == "poor":
                base_score += 15
            elif vegetation_health == "critical":
                base_score += 25
            
            # Historical drought context
            historical_boost = self._get_historical_drought_context()
            base_score += historical_boost
            
            # Seasonal patterns
            seasonal_boost = self._get_seasonal_drought_boost()
            base_score += seasonal_boost
            
            # Location adjustment
            location_boost = self._get_location_vulnerability(location_info, "drought")
            base_score += location_boost
            
            risk_level = self._get_risk_level(base_score)
            
            return {
                "level": risk_level,
                "score": min(95, int(base_score)),
                "confidence": 0.80,
                "indices": {
                    "ndvi": ndvi,
                    "drought_index": drought_index,
                    "vegetation_health": vegetation_health,
                    "composite_score": base_score
                },
                "drought_category": self._get_drought_category(base_score),
                "data_quality": "real" if nasa_data.get('modis', {}).get('is_real_data', False) else "simulated"
            }
            
        except Exception as e:
            logger.error(f"Drought risk calculation error: {e}")
            return self._get_drought_fallback(location_info)
    
    def calculate_cyclone_risk(self, nasa_data, location_info):
        """Calculate cyclone risk using real tracking data"""
        try:
            cyclone_data = nasa_data.get('cyclone', {})
            active_systems = cyclone_data.get('active_systems', 0)
            
            if active_systems == 0:
                return {
                    "level": "LOW",
                    "score": 10,
                    "confidence": 0.90,
                    "details": {"reason": "no_active_systems"},
                    "data_quality": "real" if nasa_data.get('cyclone', {}).get('is_real_data', False) else "simulated"
                }
            
            # For Angola, cyclone risk is generally low but we calculate based on proximity
            location_boost = self._get_location_vulnerability(location_info, "cyclone")
            base_score = 20 + location_boost
            
            return {
                "level": self._get_risk_level(base_score),
                "score": min(95, int(base_score)),
                "confidence": 0.65,
                "details": {
                    "active_systems": active_systems,
                    "location_vulnerability": location_boost
                },
                "data_quality": "real" if nasa_data.get('cyclone', {}).get('is_real_data', False) else "simulated"
            }
            
        except Exception as e:
            logger.error(f"Cyclone risk calculation error: {e}")
            return self._get_cyclone_fallback(location_info)
    
    def calculate_air_quality_risk(self, nasa_data, location_info):
        """Calculate health risk from air quality using NASA aerosol data"""
        try:
            air_quality = nasa_data.get('air_quality', {})
            aqi = air_quality.get('air_quality_index', 0)
            
            # AQI-based risk scoring
            if aqi > 150:
                base_score = 80 + min(20, (aqi - 150) / 5)
            elif aqi > 100:
                base_score = 65 + min(15, (aqi - 100) / 4)
            elif aqi > 50:
                base_score = 40 + min(25, (aqi - 50) / 2)
            else:
                base_score = max(10, aqi / 5)
            
            # Location-specific adjustments
            location_boost = self._get_location_vulnerability(location_info, "air_quality")
            base_score += location_boost
            
            # Seasonal adjustment
            seasonal_boost = self._get_seasonal_air_quality_impact()
            base_score += seasonal_boost
            
            risk_level = self._get_risk_level(base_score)
            
            return {
                "level": risk_level,
                "score": min(95, int(base_score)),
                "confidence": 0.85,
                "metrics": {
                    "aqi": aqi,
                    "pm25": air_quality.get('pm25_estimate', 0),
                    "pm10": air_quality.get('pm10_estimate', 0),
                    "primary_pollutant": air_quality.get('primary_pollutant', 'Unknown'),
                    "health_impact": self._get_health_impact_level(aqi)
                },
                "health_advisory": air_quality.get('health_advisory', 'No advisory'),
                "vulnerable_groups": self._get_vulnerable_groups(aqi),
                "data_quality": "real" if nasa_data.get('air_quality', {}).get('is_real_data', False) else "simulated"
            }
        except Exception as e:
            logger.error(f"Air quality risk error: {e}")
            return self._get_air_quality_fallback(location_info)
    
    def calculate_water_quality_risk(self, nasa_data, location_info):
        """Calculate risk from water quality degradation"""
        try:
            water_quality = nasa_data.get('water_quality', {})
            pollution_index = water_quality.get('pollution_index', 0)
            
            base_score = pollution_index
            
            # Location vulnerability adjustment
            location_boost = self._get_location_vulnerability(location_info, "water_quality")
            base_score += location_boost
            
            # Seasonal adjustments
            seasonal_boost = self._get_seasonal_water_quality_impact()
            base_score += seasonal_boost
            
            # Population impact
            pop_density = nasa_data.get('population', {}).get('population_density_km2', 0)
            density_impact = min(15, pop_density / 1000)
            base_score += density_impact
            
            risk_level = self._get_risk_level(base_score)
            
            return {
                "level": risk_level,
                "score": min(95, int(base_score)),
                "confidence": 0.80,
                "metrics": {
                    "turbidity": water_quality.get('turbidity_index', 0),
                    "chlorophyll": water_quality.get('chlorophyll_index', 0),
                    "water_temperature": water_quality.get('water_surface_temp', 0),
                    "pollution_index": pollution_index,
                    "recreation_safety": water_quality.get('safe_for_recreation', True)
                },
                "impact_areas": self._get_water_impact_areas(location_info),
                "health_implications": self._get_water_health_implications(pollution_index),
                "data_quality": "real" if nasa_data.get('water_quality', {}).get('is_real_data', False) else "simulated"
            }
        except Exception as e:
            logger.error(f"Water quality risk error: {e}")
            return self._get_water_quality_fallback(location_info)
    
    def calculate_pollution_impact(self, nasa_data, location_info):
        """Calculate overall pollution impact on urban health"""
        try:
            pollution_data = nasa_data.get('pollution', {})
            overall_pollution = pollution_data.get('overall_pollution_index', 0)
            
            base_score = overall_pollution
            
            # Location-specific pollution sources
            location_boost = self._get_location_vulnerability(location_info, "pollution")
            base_score += location_boost
            
            # Population density multiplier
            pop_density = nasa_data.get('population', {}).get('population_density_km2', 0)
            pop_impact = min(20, pop_density / 500)
            base_score += pop_impact
            
            risk_level = self._get_risk_level(base_score)
            
            return {
                "level": risk_level,
                "score": min(95, int(base_score)),
                "confidence": 0.75,
                "metrics": {
                    "industrial_pollution": pollution_data.get('industrial_pollution_index', 0),
                    "urban_heat_island": pollution_data.get('urban_heat_island_effect', 0),
                    "light_pollution": pollution_data.get('light_pollution_index', 0),
                    "particulate_matter": pollution_data.get('particulate_matter', 0),
                    "overall_index": overall_pollution
                },
                "pollution_hotspots": pollution_data.get('pollution_hotspots', []),
                "environmental_justice_index": self._calculate_environmental_justice(location_info, base_score),
                "data_quality": "real" if nasa_data.get('pollution', {}).get('is_real_data', False) else "simulated"
            }
        except Exception as e:
            logger.error(f"Pollution impact error: {e}")
            return self._get_pollution_fallback(location_info)
    
    def calculate_population_impact(self, nasa_data, location_info):
        """Calculate impact of population density on urban systems"""
        try:
            population_data = nasa_data.get('population', {})
            density = population_data.get('population_density_km2', 0)
            vulnerability = population_data.get('vulnerability_index', 0)
            
            # Base score from density
            if density > 10000:
                base_score = 75 + min(20, (density - 10000) / 500)
            elif density > 5000:
                base_score = 55 + min(20, (density - 5000) / 250)
            elif density > 2000:
                base_score = 35 + min(20, (density - 2000) / 150)
            else:
                base_score = max(10, density / 200)
            
            # Vulnerability adjustment
            base_score += (vulnerability / 100) * 20
            
            # Growth impact
            growth_impact = min(15, population_data.get('growth_trend', 0) * 3)
            base_score += growth_impact
            
            risk_level = self._get_risk_level(base_score)
            
            return {
                "level": risk_level,
                "score": min(95, int(base_score)),
                "confidence": 0.80,
                "metrics": {
                    "density_km2": density,
                    "population_estimate": population_data.get('population_estimate', 0),
                    "settlement_type": population_data.get('settlement_type', 'unknown'),
                    "urban_area_km2": population_data.get('urban_footprint_km2', 0),
                    "growth_rate_percent": population_data.get('growth_trend', 0),
                    "vulnerability_index": vulnerability
                },
                "urban_challenges": self._get_urban_challenges(density, location_info),
                "planning_recommendations": self._get_planning_recommendations(density, vulnerability),
                "data_quality": "real" if nasa_data.get('population', {}).get('is_real_data', False) else "simulated"
            }
        except Exception as e:
            logger.error(f"Population impact error: {e}")
            return self._get_population_fallback(location_info)
    
    # Helper methods
    def _get_risk_level(self, score):
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
    
    def _get_location_vulnerability(self, location_info, risk_type):
        """Get location-specific vulnerability for different risk types"""
        risk_factors = location_info.get('risk_profile', {})
        
        if risk_type == "flood":
            return risk_factors.get('flood', 0.5) * 20
        elif risk_type == "fire":
            return risk_factors.get('fire', 0.5) * 20
        elif risk_type == "drought":
            return risk_factors.get('drought', 0.5) * 20
        elif risk_type == "cyclone":
            return risk_factors.get('coastal', 0.3) * 15
        elif risk_type == "air_quality":
            return risk_factors.get('urban', 0.5) * 15
        elif risk_type == "water_quality":
            return risk_factors.get('coastal', 0.3) * 15
        elif risk_type == "pollution":
            return risk_factors.get('industrial', 0.5) * 20
        else:
            return 10
    
    def _get_seasonal_fire_boost(self):
        current_month = datetime.utcnow().month
        if current_month in [8, 9, 10, 11]:
            return 20
        elif current_month in [7, 12]:
            return 10
        return 0
    
    def _get_seasonal_drought_boost(self):
        current_month = datetime.utcnow().month
        if current_month in [5, 6, 7, 8, 9]:
            return 15
        return 0
    
    def _get_seasonal_air_quality_impact(self):
        current_month = datetime.utcnow().month
        if current_month in [6, 7, 8, 9]:
            return 15
        return 0
    
    def _get_seasonal_water_quality_impact(self):
        current_month = datetime.utcnow().month
        if current_month in [1, 2, 3, 4]:
            return 12
        return 0
    
    def _get_flood_type(self, rainfall, location_info):
        if rainfall > 100:
            return "major_flood"
        elif rainfall > 60:
            return "flash_flood" if location_info.get('type') == 'MUNICIPALITY' else "urban_flood"
        elif rainfall > 30:
            return "minor_flooding"
        else:
            return "localized_flooding"
    
    def _get_impact_areas(self, location_info, score):
        location_type = location_info.get('type', 'MUNICIPALITY')
        name = location_info.get('name', 'Unknown')
        
        if location_type == "MUNICIPALITY":
            if "VIANA" in name.upper():
                return ["Industrial Zone", "Transport Corridors"]
            elif "INGOMBOTA" in name.upper():
                return ["City Center", "Commercial Areas"]
            elif "MUSSULO" in name.upper():
                return ["Coastal Areas", "Fishing Zones"]
        
        return ["Urban Area", "Residential Zones"]
    
    def _get_drought_category(self, score):
        if score > 75:
            return "S3_Severe_Drought"
        elif score > 55:
            return "S2_Moderate_Drought"
        elif score > 35:
            return "S1_Mild_Drought"
        else:
            return "S0_No_Drought"
    
    def _get_health_impact_level(self, aqi):
        if aqi > 150:
            return "Serious health effects"
        elif aqi > 100:
            return "Unhealthy for sensitive groups"
        elif aqi > 50:
            return "Moderate health concern"
        else:
            return "Minimal health impact"
    
    def _get_vulnerable_groups(self, aqi):
        if aqi > 100:
            return ["Children", "Elderly", "Respiratory conditions", "Heart conditions"]
        elif aqi > 50:
            return ["Children", "Elderly", "Respiratory conditions"]
        else:
            return ["None specific"]
    
    def _get_water_impact_areas(self, location_info):
        name = location_info.get('name', '')
        if "MUSSULO" in name.upper():
            return ["Coastal Waters", "Fishing Areas"]
        elif "INGOMBOTA" in name.upper():
            return ["Urban Rivers", "Drainage Systems"]
        elif "VIANA" in name.upper():
            return ["Industrial Canals", "Wastewater Outflows"]
        else:
            return ["Water Bodies", "Drainage Systems"]
    
    def _get_water_health_implications(self, pollution_index):
        if pollution_index > 70:
            return "High risk of waterborne diseases"
        elif pollution_index > 50:
            return "Moderate health risk, avoid ingestion"
        elif pollution_index > 30:
            return "Low risk, basic treatment recommended"
        else:
            return "Minimal health risk"
    
    def _calculate_environmental_justice(self, location_info, pollution_score):
        vulnerability = location_info.get('demographics', {}).get('vulnerability_index', 50)
        return min(100, (vulnerability + pollution_score) / 2)
    
    def _get_urban_challenges(self, density, location_info):
        challenges = []
        if density > 8000:
            challenges.extend(["Overcrowding", "Sanitation issues", "Traffic congestion"])
        if density > 5000:
            challenges.extend(["Housing pressure", "Service delivery strain"])
        if location_info.get('type') == 'MUNICIPALITY':
            challenges.extend(["Urban planning needs", "Infrastructure maintenance"])
        return challenges
    
    def _get_planning_recommendations(self, density, vulnerability):
        recommendations = []
        if density > 8000:
            recommendations.extend(["Upgrade public transport", "Improve green spaces"])
        if vulnerability > 70:
            recommendations.extend(["Target social housing", "Enhance emergency services"])
        if density > 5000:
            recommendations.extend(["Mixed-use development", "Infrastructure investment"])
        return recommendations if recommendations else ["Maintain current planning strategies"]
    
    def _get_historical_drought_context(self):
        current_year = datetime.utcnow().year
        recent_droughts = [d for d in self.historical_data['drought_periods'] 
                          if d['year'] >= current_year - 2]
        if recent_droughts:
            return 10
        return 0
    
    # Fallback methods
    def _get_flood_fallback(self, location_info):
        return {
            "level": "LOW",
            "score": 20,
            "confidence": 0.5,
            "factors": {"fallback": True},
            "prediction_horizon_hours": 24,
            "flood_type": "unknown",
            "expected_impact_areas": ["General Area"],
            "data_quality": "fallback"
        }
    
    def _get_fire_fallback(self, location_info):
        return {
            "level": "LOW", 
            "score": 15,
            "confidence": 0.5,
            "details": {"fallback": True},
            "data_quality": "fallback"
        }
    
    def _get_drought_fallback(self, location_info):
        return {
            "level": "LOW",
            "score": 25,
            "confidence": 0.5,
            "indices": {"fallback": True},
            "drought_category": "S0_No_Drought",
            "data_quality": "fallback"
        }
    
    def _get_cyclone_fallback(self, location_info):
        return {
            "level": "LOW",
            "score": 10,
            "confidence": 0.5,
            "details": {"fallback": True},
            "data_quality": "fallback"
        }
    
    def _get_air_quality_fallback(self, location_info):
        return {
            "level": "MEDIUM",
            "score": 45,
            "confidence": 0.5,
            "metrics": {"fallback": True},
            "health_advisory": "Moderate - Sensitive groups should take care",
            "vulnerable_groups": ["Children", "Elderly"],
            "data_quality": "fallback"
        }
    
    def _get_water_quality_fallback(self, location_info):
        return {
            "level": "LOW",
            "score": 35,
            "confidence": 0.5,
            "metrics": {"fallback": True},
            "impact_areas": ["General Water Bodies"],
            "health_implications": "Low risk, basic treatment recommended",
            "data_quality": "fallback"
        }
    
    def _get_pollution_fallback(self, location_info):
        return {
            "level": "MEDIUM", 
            "score": 50,
            "confidence": 0.5,
            "metrics": {"fallback": True},
            "pollution_hotspots": ["General Area"],
            "environmental_justice_index": 60,
            "data_quality": "fallback"
        }
    
    def _get_population_fallback(self, location_info):
        return {
            "level": "MEDIUM",
            "score": 55,
            "confidence": 0.5,
            "metrics": {"fallback": True},
            "urban_challenges": ["General urban pressures"],
            "planning_recommendations": ["Infrastructure monitoring"],
            "data_quality": "fallback"
        }