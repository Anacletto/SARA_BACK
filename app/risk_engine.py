import datetime
import logging

logger = logging.getLogger(__name__)

class RiskEngine:
    def __init__(self):
        self.historical_data = self._initialize_historical_data()

    def _initialize_historical_data(self):
        return {
            "rainfall": [],
            "fires": [],
            "air_quality": [],
            "water_quality": [],
            "population": [],
            "drought_periods": []  
        }

    def update_historical_data(self, rainfall, fire, air, water, pop):
        self.historical_data["rainfall"].append(rainfall)
        self.historical_data["fires"].append(fire)
        self.historical_data["air_quality"].append(air)
        self.historical_data["water_quality"].append(water)
        self.historical_data["population"].append(pop)

    def calculate_flood_risk(self, rainfall, water):
        score = min(98, (rainfall["rainfall_mm"] / 200) * 100 + water["turbidity_NTU"])
        return {"level": self._score_to_level(score), "score": round(score, 2), "confidence": 0.9}

    def calculate_fire_risk(self, fire):
        score = min(95, fire["fire_count"] * 10 + fire["avg_intensity"] * 50)
        return {"level": self._score_to_level(score), "score": round(score, 2), "confidence": 0.85}

    def calculate_drought_risk(self, rainfall):
        score = max(0, 100 - rainfall["rainfall_mm"])
        if rainfall["season"] == "dry": score *= 1.2
        return {"level": self._score_to_level(score), "score": round(score, 2), "confidence": 0.8}

    def calculate_air_quality_risk(self, air):
        score = min(95, (air["AQI"] / 150) * 100)
        return {"level": self._score_to_level(score), "score": round(score, 2), "confidence": 0.85}

    def calculate_water_quality_risk(self, water):
        score = min(95, abs(7 - water["pH"]) * 10 + water["turbidity_NTU"] * 2)
        return {"level": self._score_to_level(score), "score": round(score, 2), "confidence": 0.8}

    def calculate_population_pressure(self, pop):
        score = min(95, (pop["population"] / 2000000) * 100 + pop["growth_trend"] * 100)
        return {"level": self._score_to_level(score), "score": round(score, 2), "confidence": 0.75}

    def _score_to_level(self, score):
        if score < 20: return "Low"
        elif score < 50: return "Moderate"
        elif score < 75: return "High"
        return "Severe"
