import numpy as np
from typing import Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Municipality:
    id: str
    name: str
    province: str
    latitude: float
    longitude: float
    population: int
    area_km2: float
    elevation: float
    risk_factors: Dict
    economic_activity: str
    infrastructure_level: str
    climate_zone: str

class LuandaGeoData:
    def __init__(self):
        self.municipalities = self._initialize_municipalities()
        self.provinces = self._initialize_provinces()
    
    def _initialize_municipalities(self) -> Dict[str, Municipality]:
        """Initialize all municipalities with realistic data"""
        return {
            # Luanda Province Municipalities
            "BELAS": Municipality(
                id="BELAS", name="Belas", province="LUANDA",
                latitude=-8.998, longitude=13.257, population=1250000, area_km2=1.076, elevation=45.0,
                risk_factors={"flood": 0.4, "fire": 0.3, "drought": 0.2, "urban_heat": 0.7, "landslide": 0.1},
                economic_activity="commercial", infrastructure_level="high", climate_zone="coastal"
            ),
            "CAZENGA": Municipality(
                id="CAZENGA", name="Cazenga", province="LUANDA", 
                latitude=-8.885, longitude=13.318, population=950000, area_km2=3.72, elevation=35.0,
                risk_factors={"flood": 0.8, "fire": 0.5, "drought": 0.4, "urban_heat": 0.8, "pollution": 0.7},
                economic_activity="mixed", infrastructure_level="medium", climate_zone="urban"
            ),
            "INGOMBOTA": Municipality(
                id="INGOMBOTA", name="Ingombota", province="LUANDA",
                latitude=-8.813, longitude=13.230, population=400000, area_km2=4.42, elevation=12.0,
                risk_factors={"flood": 0.9, "fire": 0.3, "drought": 0.2, "urban_heat": 0.6, "coastal_erosion": 0.8},
                economic_activity="commercial", infrastructure_level="high", climate_zone="coastal"
            ),
            "KILAMBA_KIAXI": Municipality(
                id="KILAMBA_KIAXI", name="Kilamba Kiaxi", province="LUANDA",
                latitude=-8.930, longitude=13.267, population=750000, area_km2=5.42, elevation=55.0,
                risk_factors={"flood": 0.5, "fire": 0.6, "drought": 0.5, "urban_heat": 0.7, "infrastructure": 0.4},
                economic_activity="residential", infrastructure_level="medium", climate_zone="urban"
            ),
            "MAIANGA": Municipality(
                id="MAIANGA", name="Maianga", province="LUANDA",
                latitude=-8.830, longitude=13.240, population=350000, area_km2=3.55, elevation=25.0,
                risk_factors={"flood": 0.7, "fire": 0.4, "drought": 0.3, "urban_heat": 0.8, "traffic": 0.6},
                economic_activity="commercial", infrastructure_level="medium", climate_zone="urban"
            ),
            "RANGEL": Municipality(
                id="RANGEL", name="Rangel", province="LUANDA",
                latitude=-8.820, longitude=13.250, population=280000, area_km2=2.15, elevation=30.0,
                risk_factors={"flood": 0.6, "fire": 0.5, "drought": 0.4, "urban_heat": 0.7, "informal_settlements": 0.8},
                economic_activity="mixed", infrastructure_level="low", climate_zone="urban"
            ),
            "SAMBA": Municipality(
                id="SAMBA", name="Samba", province="LUANDA",
                latitude=-8.950, longitude=13.200, population=600000, area_km2=4.82, elevation=40.0,
                risk_factors={"flood": 0.4, "fire": 0.7, "drought": 0.6, "urban_heat": 0.8, "vegetation_fire": 0.6},
                economic_activity="mixed", infrastructure_level="medium", climate_zone="suburban"
            ),
            "SANDA": Municipality(
                id="SANDA", name="Sanda", province="LUANDA",
                latitude=-8.900, longitude=13.350, population=450000, area_km2=3.25, elevation=38.0,
                risk_factors={"flood": 0.5, "fire": 0.6, "drought": 0.5, "urban_heat": 0.7, "industrial_risk": 0.4},
                economic_activity="industrial", infrastructure_level="medium", climate_zone="urban"
            ),
            "VIANA": Municipality(
                id="VIANA", name="Viana", province="LUANDA",
                latitude=-8.893, longitude=13.370, population=2000000, area_km2=42.55, elevation=45.0,
                risk_factors={"flood": 0.3, "fire": 0.8, "drought": 0.7, "urban_heat": 0.6, "industrial_pollution": 0.9},
                economic_activity="industrial", infrastructure_level="medium", climate_zone="industrial"
            ),
            "CACUACO": Municipality(
                id="CACUACO", name="Cacuaco", province="LUANDA",
                latitude=-8.783, longitude=13.367, population=1275000, area_km2=571.0, elevation=28.0,
                risk_factors={"flood": 0.7, "fire": 0.5, "drought": 0.8, "urban_heat": 0.5, "agricultural_drought": 0.7},
                economic_activity="agricultural", infrastructure_level="low", climate_zone="rural"
            ),
            "CAMAMA": Municipality(
                id="CAMAMA", name="Camama", province="LUANDA",
                latitude=-8.917, longitude=13.283, population=300000, area_km2=2.85, elevation=52.0,
                risk_factors={"flood": 0.4, "fire": 0.5, "drought": 0.6, "urban_heat": 0.7, "water_scarcity": 0.5},
                economic_activity="residential", infrastructure_level="medium", climate_zone="suburban"
            ),
            "CAPALANCA": Municipality(
                id="CAPALANCA", name="Capalanca", province="LUANDA",
                latitude=-8.867, longitude=13.317, population=180000, area_km2=1.92, elevation=32.0,
                risk_factors={"flood": 0.8, "fire": 0.3, "drought": 0.4, "urban_heat": 0.8, "drainage_issues": 0.7},
                economic_activity="mixed", infrastructure_level="low", climate_zone="urban"
            ),
            "FUTUNGODE_BELAS": Municipality(
                id="FUTUNGODE_BELAS", name="Futungo de Belas", province="LUANDA",
                latitude=-9.017, longitude=13.167, population=150000, area_km2=3.45, elevation=65.0,
                risk_factors={"flood": 0.3, "fire": 0.7, "drought": 0.8, "urban_heat": 0.6, "vegetation_fire": 0.8},
                economic_activity="residential", infrastructure_level="high", climate_zone="suburban"
            ),
            "MUSSULO": Municipality(
                id="MUSSULO", name="Mussulo", province="LUANDA",
                latitude=-9.067, longitude=13.117, population=50000, area_km2=15.25, elevation=2.0,
                risk_factors={"flood": 0.9, "fire": 0.2, "drought": 0.3, "urban_heat": 0.4, "coastal_flooding": 0.9},
                economic_activity="tourism", infrastructure_level="low", climate_zone="coastal"
            ),
            "QUICOLO": Municipality(
                id="QUICOLO", name="Quicolo", province="LUANDA",
                latitude=-8.833, longitude=13.400, population=220000, area_km2=2.75, elevation=25.0,
                risk_factors={"flood": 0.8, "fire": 0.4, "drought": 0.5, "urban_heat": 0.7, "river_flooding": 0.7},
                economic_activity="mixed", infrastructure_level="low", climate_zone="riverine"
            ),
            "QUILAMBA_QUIAXE": Municipality(
                id="QUILAMBA_QUIAXE", name="Quilamba Quiaxe", province="LUANDA",
                latitude=-8.967, longitude=13.233, population=350000, area_km2=3.15, elevation=48.0,
                risk_factors={"flood": 0.5, "fire": 0.6, "drought": 0.7, "urban_heat": 0.8, "water_quality": 0.6},
                economic_activity="residential", infrastructure_level="medium", climate_zone="suburban"
            ),
            "TALATONA": Municipality(
                id="TALATONA", name="Talatona", province="LUANDA",
                latitude=-8.917, longitude=13.183, population=800000, area_km2=111.0, elevation=60.0,
                risk_factors={"flood": 0.3, "fire": 0.7, "drought": 0.8, "urban_heat": 0.7, "urban_sprawl": 0.5},
                economic_activity="commercial", infrastructure_level="high", climate_zone="suburban"
            ),
            "ZANGO": Municipality(
                id="ZANGO", name="Zango", province="LUANDA",
                latitude=-8.883, longitude=13.450, population=900000, area_km2=2.95, elevation=35.0,
                risk_factors={"flood": 0.6, "fire": 0.5, "drought": 0.7, "urban_heat": 0.8, "population_pressure": 0.8},
                economic_activity="residential", infrastructure_level="medium", climate_zone="urban"
            ),
            "CAXITO": Municipality(
                id="CAXITO", name="Caxito", province="LUANDA",
                latitude=-8.583, longitude=13.667, population=180000, area_km2=3.25, elevation=85.0,
                risk_factors={"flood": 0.4, "fire": 0.8, "drought": 0.9, "urban_heat": 0.6, "agricultural_fire": 0.7},
                economic_activity="agricultural", infrastructure_level="low", climate_zone="rural"
            ),
            
            # Benguela Province Municipalities
            "BENGUELA_CITY": Municipality(
                id="BENGUELA_CITY", name="Benguela City", province="BENGUELA",
                latitude=-12.576, longitude=13.405, population=650000, area_km2=2100, elevation=25.0,
                risk_factors={"flood": 0.6, "fire": 0.4, "drought": 0.5, "coastal_erosion": 0.7, "urban_heat": 0.6},
                economic_activity="port_commercial", infrastructure_level="medium", climate_zone="coastal"
            ),
            "LOBITO": Municipality(
                id="LOBITO", name="Lobito", province="BENGUELA",
                latitude=-12.348, longitude=13.546, population=450000, area_km2=3648, elevation=15.0,
                risk_factors={"flood": 0.5, "fire": 0.3, "drought": 0.4, "coastal_flooding": 0.8, "industrial_risk": 0.6},
                economic_activity="port_industrial", infrastructure_level="medium", climate_zone="coastal"
            ),
            
            # Huambo Province Municipalities
            "HUAMBO_CITY": Municipality(
                id="HUAMBO_CITY", name="Huambo City", province="HUAMBO",
                latitude=-12.767, longitude=15.733, population=1200000, area_km2=2600, elevation=1700.0,
                risk_factors={"flood": 0.4, "fire": 0.5, "drought": 0.7, "landslide": 0.6, "frost": 0.4},
                economic_activity="agricultural", infrastructure_level="medium", climate_zone="highland"
            ),
            
            # Cabinda Province Municipalities
            "CABINDA_CITY": Municipality(
                id="CABINDA_CITY", name="Cabinda City", province="CABINDA",
                latitude=-5.550, longitude=12.200, population=800000, area_km2=280, elevation=20.0,
                risk_factors={"flood": 0.7, "fire": 0.3, "drought": 0.2, "oil_pollution": 0.8, "coastal_erosion": 0.6},
                economic_activity="oil_industrial", infrastructure_level="high", climate_zone="coastal_forest"
            ),
            
            # Huíla Province Municipalities
            "LUBANGO": Municipality(
                id="LUBANGO", name="Lubango", province="HUILA",
                latitude=-14.917, longitude=13.500, population=1000000, area_km2=3140, elevation=1760.0,
                risk_factors={"flood": 0.3, "fire": 0.6, "drought": 0.8, "landslide": 0.5, "water_scarcity": 0.7},
                economic_activity="agricultural", infrastructure_level="medium", climate_zone="highland"
            ),
            
            # Cunene Province Municipalities
            "ONDJIVA": Municipality(
                id="ONDJIVA", name="Ondjiva", province="CUNENE",
                latitude=-17.067, longitude=15.733, population=300000, area_km2=15874, elevation=1090.0,
                risk_factors={"flood": 0.2, "fire": 0.7, "drought": 0.9, "desertification": 0.8, "water_scarcity": 0.9},
                economic_activity="pastoral", infrastructure_level="low", climate_zone="semi_arid"
            )
        }
    
    def _initialize_provinces(self) -> Dict[str, Dict]:
        """Initialize all provinces with realistic data"""
        return {
            "LUANDA": {
                "name": "Luanda", "capital": "Luanda", "population": 9000000, "area_km2": 2418,
                "municipalities": ["BELAS", "CAZENGA", "INGOMBOTA", "KILAMBA_KIAXI", "MAIANGA", "RANGEL", 
                                 "SAMBA", "SANDA", "VIANA", "CACUACO", "CAMAMA", "CAPALANCA", 
                                 "FUTUNGODE_BELAS", "MUSSULO", "QUICOLO", "QUILAMBA_QUIAXE", "TALATONA", "ZANGO", "CAXITO"],
                "risk_profile": {"coastal": 0.8, "urban": 0.9, "industrial": 0.7, "flood": 0.6, "drought": 0.4},
                "economic_base": "oil_commerce", "development_index": "high", "climate_zone": "coastal"
            },
            "BENGUELA": {
                "name": "Benguela", "capital": "Benguela", "population": 2200000, "area_km2": 31788,
                "municipalities": ["BENGUELA_CITY", "LOBITO"],
                "risk_profile": {"coastal": 0.7, "agricultural": 0.6, "drought": 0.5, "industrial": 0.5},
                "economic_base": "port_agriculture", "development_index": "medium", "climate_zone": "coastal"
            },
            "HUAMBO": {
                "name": "Huambo", "capital": "Huambo", "population": 2000000, "area_km2": 34274,
                "municipalities": ["HUAMBO_CITY"],
                "risk_profile": {"highland": 0.7, "agricultural": 0.8, "landslide": 0.6, "drought": 0.7},
                "economic_base": "agriculture", "development_index": "medium", "climate_zone": "highland"
            },
            "CABINDA": {
                "name": "Cabinda", "capital": "Cabinda", "population": 800000, "area_km2": 7283,
                "municipalities": ["CABINDA_CITY"],
                "risk_profile": {"coastal": 0.8, "oil": 0.9, "forest": 0.6, "political": 0.7},
                "economic_base": "oil", "development_index": "high", "climate_zone": "coastal_forest"
            },
            "ZAIRE": {
                "name": "Zaire", "capital": "Mbanza Kongo", "population": 600000, "area_km2": 40030,
                "municipalities": [],
                "risk_profile": {"coastal": 0.6, "agricultural": 0.7, "drought": 0.4, "infrastructure": 0.5},
                "economic_base": "agriculture_fishing", "development_index": "low", "climate_zone": "coastal"
            },
            "UIGE": {
                "name": "Uíge", "capital": "Uíge", "population": 1500000, "area_km2": 58698,
                "municipalities": [],
                "risk_profile": {"forest": 0.8, "agricultural": 0.7, "malaria": 0.6, "infrastructure": 0.6},
                "economic_base": "agriculture", "development_index": "low", "climate_zone": "forest"
            },
            "MALANJE": {
                "name": "Malanje", "capital": "Malanje", "population": 1100000, "area_km2": 97602,
                "municipalities": [],
                "risk_profile": {"plateau": 0.6, "agricultural": 0.7, "drought": 0.5, "water_scarcity": 0.6},
                "economic_base": "agriculture", "development_index": "low", "climate_zone": "plateau"
            },
            "LUNDA_NORTE": {
                "name": "Lunda Norte", "capital": "Dundo", "population": 900000, "area_km2": 103760,
                "municipalities": [],
                "risk_profile": {"mining": 0.8, "forest": 0.7, "infrastructure": 0.7, "social": 0.6},
                "economic_base": "diamond_mining", "development_index": "medium", "climate_zone": "forest"
            },
            "LUNDA_SUL": {
                "name": "Lunda Sul", "capital": "Saurimo", "population": 600000, "area_km2": 45610,
                "municipalities": [],
                "risk_profile": {"mining": 0.7, "forest": 0.6, "infrastructure": 0.6, "water_quality": 0.5},
                "economic_base": "diamond_mining", "development_index": "medium", "climate_zone": "forest"
            },
            "BIE": {
                "name": "Bié", "capital": "Cuíto", "population": 1600000, "area_km2": 70314,
                "municipalities": [],
                "risk_profile": {"plateau": 0.7, "agricultural": 0.8, "drought": 0.6, "food_security": 0.7},
                "economic_base": "agriculture", "development_index": "low", "climate_zone": "plateau"
            },
            "MOXICO": {
                "name": "Moxico", "capital": "Luena", "population": 800000, "area_km2": 223023,
                "municipalities": [],
                "risk_profile": {"floodplain": 0.8, "agricultural": 0.6, "malaria": 0.7, "infrastructure": 0.8},
                "economic_base": "agriculture", "development_index": "low", "climate_zone": "floodplain"
            },
            "NAMIBE": {
                "name": "Namibe", "capital": "Namibe", "population": 600000, "area_km2": 57191,
                "municipalities": [],
                "risk_profile": {"coastal": 0.7, "desert": 0.8, "drought": 0.9, "water_scarcity": 0.8},
                "economic_base": "fishing_mining", "development_index": "low", "climate_zone": "desert"
            },
            "HUILA": {
                "name": "Huíla", "capital": "Lubango", "population": 2800000, "area_km2": 75002,
                "municipalities": ["LUBANGO"],
                "risk_profile": {"highland": 0.7, "agricultural": 0.8, "drought": 0.7, "water_scarcity": 0.6},
                "economic_base": "agriculture", "development_index": "medium", "climate_zone": "highland"
            },
            "CUNENE": {
                "name": "Cunene", "capital": "Ondjiva", "population": 1200000, "area_km2": 87342,
                "municipalities": ["ONDJIVA"],
                "risk_profile": {"drought": 0.9, "pastoral": 0.8, "desertification": 0.8, "food_security": 0.9},
                "economic_base": "pastoral", "development_index": "low", "climate_zone": "semi_arid"
            },
            "CUANZA_NORTE": {
                "name": "Cuanza Norte", "capital": "N'dalatando", "population": 500000, "area_km2": 24110,
                "municipalities": [],
                "risk_profile": {"agricultural": 0.7, "forest": 0.6, "infrastructure": 0.7, "water_quality": 0.5},
                "economic_base": "agriculture", "development_index": "low", "climate_zone": "forest"
            },
            "CUANZA_SUL": {
                "name": "Cuanza Sul", "capital": "Sumbe", "population": 2000000, "area_km2": 55660,
                "municipalities": [],
                "risk_profile": {"coastal": 0.6, "agricultural": 0.8, "drought": 0.5, "infrastructure": 0.6},
                "economic_base": "agriculture", "development_index": "medium", "climate_zone": "coastal"
            },
            "BENGUELA": {
                "name": "Benguela", "capital": "Benguela", "population": 2200000, "area_km2": 31788,
                "municipalities": ["BENGUELA_CITY", "LOBITO"],
                "risk_profile": {"coastal": 0.7, "agricultural": 0.7, "drought": 0.5, "industrial": 0.5},
                "economic_base": "port_agriculture", "development_index": "medium", "climate_zone": "coastal"
            },
            "BIC": {
                "name": "Bié", "capital": "Cuíto", "population": 1600000, "area_km2": 70314,
                "municipalities": [],
                "risk_profile": {"plateau": 0.7, "agricultural": 0.8, "drought": 0.6, "food_security": 0.7},
                "economic_base": "agriculture", "development_index": "low", "climate_zone": "plateau"
            },
            "CUANDO_CUBANGO": {
                "name": "Cuando Cubango", "capital": "Menongue", "population": 600000, "area_km2": 199049,
                "municipalities": [],
                "risk_profile": {"floodplain": 0.8, "wildlife": 0.7, "malaria": 0.8, "infrastructure": 0.9},
                "economic_base": "tourism_agriculture", "development_index": "low", "climate_zone": "floodplain"
            }
        }
    
    def get_municipality_bbox(self, municipality_id: str) -> List[float]:
        """Get realistic bounding box for a specific municipality"""
        mun = self.municipalities.get(municipality_id)
        if mun:
            # Create realistic bounding box based on municipality size and location
            lat_range = 0.1 if mun.area_km2 > 100 else 0.05
            lon_range = 0.1 if mun.area_km2 > 100 else 0.05
            return [
                mun.longitude - lon_range,
                mun.latitude - lat_range,  
                mun.longitude + lon_range,
                mun.latitude + lat_range
            ]
        return self.get_province_bbox("LUANDA")
    
    def get_province_bbox(self, province_id: str) -> List[float]:
        """Get realistic bounding box for a province"""
        bboxes = {
            "LUANDA": [12.8, -9.2, 13.8, -8.5],
            "BENGUELA": [12.5, -13.5, 14.5, -11.5],
            "HUAMBO": [14.5, -13.5, 16.5, -11.5],
            "CABINDA": [12.0, -5.5, 13.5, -3.5],
            "ZAIRE": [12.5, -7.5, 14.5, -5.5],
            "UIGE": [14.5, -8.0, 16.0, -5.5],
            "MALANJE": [15.5, -11.0, 18.0, -8.5],
            "LUNDA_NORTE": [18.0, -9.0, 21.0, -6.5],
            "LUNDA_SUL": [20.0, -11.0, 22.0, -8.5],
            "BIE": [16.0, -13.5, 18.5, -11.5],
            "MOXICO": [18.0, -14.5, 22.0, -11.5],
            "NAMIBE": [12.0, -16.5, 13.5, -14.0],
            "HUILA": [13.5, -16.0, 16.5, -13.5],
            "CUNENE": [14.5, -17.5, 16.5, -15.5],
            "CUANZA_NORTE": [14.0, -10.0, 15.5, -8.0],
            "CUANZA_SUL": [13.5, -11.5, 15.5, -9.5]
        }
        return bboxes.get(province_id, [11.0, -18.0, 24.0, -4.0])
    
    def get_municipalities_by_province(self, province_id: str) -> List[Municipality]:
        """Get all municipalities for a province"""
        if province_id in self.provinces:
            mun_ids = self.provinces[province_id].get("municipalities", [])
            return [self.municipalities[mun_id] for mun_id in mun_ids if mun_id in self.municipalities]
        return []
    
    def get_all_municipalities(self) -> List[Municipality]:
        """Get all municipalities across all provinces"""
        return list(self.municipalities.values())
    
    def get_province_for_municipality(self, municipality_id: str) -> str:
        """Get province for a municipality"""
        mun = self.municipalities.get(municipality_id)
        return mun.province if mun else "UNKNOWN"