import datetime
import logging
from typing import Dict, List

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
        level = self._score_to_level(score)
        
        return {
            "level": level,
            "score": round(score, 2), 
            "confidence": 0.9,
            "recommendations": self._get_flood_recommendations(score, rainfall["rainfall_mm"]),
            "community_impact": self._get_flood_community_impact(score),
            "basic_needs": self._get_flood_basic_needs(score),
            "flood_type": self._get_flood_type(rainfall["rainfall_mm"]),
            "expected_impact_areas": self._get_flood_impact_areas(score)
        }

    def calculate_fire_risk(self, fire):
        score = min(95, fire["fire_count"] * 10 + fire["avg_intensity"] * 50)
        level = self._score_to_level(score)
        
        return {
            "level": level,
            "score": round(score, 2),
            "confidence": 0.85,
            "recommendations": self._get_fire_recommendations(score, fire["fire_count"]),
            "community_impact": self._get_fire_community_impact(score),
            "basic_needs": self._get_fire_basic_needs(score),
            "fire_intensity": self._get_fire_intensity_level(fire["avg_intensity"]),
            "evacuation_zones": self._get_fire_evacuation_zones(score)
        }

    def calculate_drought_risk(self, rainfall):
        score = max(0, 100 - rainfall["rainfall_mm"])
        if rainfall["season"] == "dry": 
            score *= 1.2
        score = min(95, score)
        level = self._score_to_level(score)
        
        return {
            "level": level,
            "score": round(score, 2),
            "confidence": 0.8,
            "recommendations": self._get_drought_recommendations(score),
            "community_impact": self._get_drought_community_impact(score),
            "basic_needs": self._get_drought_basic_needs(score),
            "drought_category": self._get_drought_category(score),
            "water_restrictions": self._get_water_restrictions(score)
        }

    def calculate_air_quality_risk(self, air):
        score = min(95, (air["AQI"] / 150) * 100)
        level = self._score_to_level(score)
        
        return {
            "level": level,
            "score": round(score, 2),
            "confidence": 0.85,
            "recommendations": self._get_air_quality_recommendations(score),
            "community_impact": self._get_air_quality_community_impact(score),
            "basic_needs": self._get_air_quality_basic_needs(score),
            "health_advisory": self._get_health_advisory(air["AQI"]),
            "vulnerable_groups": self._get_vulnerable_groups(air["AQI"])
        }

    def calculate_water_quality_risk(self, water):
        score = min(95, abs(7 - water["pH"]) * 10 + water["turbidity_NTU"] * 2)
        level = self._score_to_level(score)
        
        return {
            "level": level,
            "score": round(score, 2),
            "confidence": 0.8,
            "recommendations": self._get_water_quality_recommendations(score, water["pH"]),
            "community_impact": self._get_water_quality_community_impact(score),
            "basic_needs": self._get_water_quality_basic_needs(score),
            "health_implications": self._get_water_health_implications(score),
            "water_safety": self._get_water_safety_level(score)
        }

    def calculate_population_pressure(self, pop):
        score = min(95, (pop["population"] / 2000000) * 100 + pop["growth_trend"] * 100)
        level = self._score_to_level(score)
        
        return {
            "level": level,
            "score": round(score, 2),
            "confidence": 0.75,
            "recommendations": self._get_population_recommendations(score, pop["population"]),
            "community_impact": self._get_population_community_impact(score),
            "basic_needs": self._get_population_basic_needs(score),
            "urban_challenges": self._get_urban_challenges(score),
            "infrastructure_needs": self._get_infrastructure_needs(score)
        }

    def _score_to_level(self, score):
        if score < 20: 
            return "Low"
        elif score < 50: 
            return "Moderate"
        elif score < 75: 
            return "High"
        return "Severe"

    # Basic Needs Assessment Methods
    def _get_flood_basic_needs(self, score):
        needs = {
            "water": "Normal supply",
            "energy": "Normal supply", 
            "hospitals": "Fully operational",
            "food": "Normal availability",
            "shelter": "Adequate",
            "transportation": "Normal operations"
        }
        
        if score >= 75:
            needs.update({
                "water": "EMERGENCY - Contaminated, need bottled water",
                "energy": "CRITICAL - Widespread outages expected",
                "hospitals": "OVERWHELMED - Emergency cases only",
                "food": "SHORTAGE - Distribution disrupted",
                "shelter": "URGENT - Evacuation shelters needed",
                "transportation": "SEVERE DISRUPTION - Most routes impassable"
            })
        elif score >= 50:
            needs.update({
                "water": "BOIL ADVISORY - Treatment required",
                "energy": "PARTIAL OUTAGES - Some areas affected",
                "hospitals": "INCREASED DEMAND - Prepare for emergencies",
                "food": "LIMITED - Stock essential supplies",
                "shelter": "AT RISK - Prepare evacuation plans",
                "transportation": "MAJOR DISRUPTIONS - Many routes affected"
            })
        elif score >= 20:
            needs.update({
                "water": "MONITOR QUALITY - Possible contamination",
                "energy": "STABLE - Minor localized issues",
                "hospitals": "NORMAL - Increased readiness",
                "food": "ADEQUATE - Maintain supplies",
                "shelter": "SECURE - Monitor conditions",
                "transportation": "MINOR DISRUPTIONS - Some routes affected"
            })
        
        return needs

    def _get_fire_basic_needs(self, score):
        needs = {
            "water": "Normal supply",
            "energy": "Normal supply",
            "hospitals": "Fully operational", 
            "food": "Normal availability",
            "shelter": "Adequate",
            "transportation": "Normal operations"
        }
        
        if score >= 75:
            needs.update({
                "water": "CRITICAL - Firefighting priority, restrictions",
                "energy": "WIDESPREAD OUTAGES - Grid damage",
                "hospitals": "OVERLOADED - Respiratory emergencies",
                "food": "DISRUPTED - Evacuation impacts",
                "shelter": "EMERGENCY - Mass evacuations needed",
                "transportation": "SEVERELY DISRUPTED - Road closures, poor visibility"
            })
        elif score >= 50:
            needs.update({
                "water": "HIGH DEMAND - Firefighting needs",
                "energy": "RISK OF OUTAGES - Grid instability",
                "hospitals": "INCREASED LOAD - Respiratory cases",
                "food": "STABLE - Prepare for disruptions",
                "shelter": "PREPARE EVACUATION - At-risk areas",
                "transportation": "SIGNIFICANT DISRUPTIONS - Smoke, closures"
            })
        elif score >= 20:
            needs.update({
                "water": "ADEQUATE - Monitor supply",
                "energy": "STABLE - Watch for outages", 
                "hospitals": "NORMAL - Prepare for smoke-related cases",
                "food": "NORMAL - Maintain supplies",
                "shelter": "SECURE - Monitor fire progression",
                "transportation": "MINOR DISRUPTIONS - Some smoke impacts"
            })
        
        return needs

    def _get_drought_basic_needs(self, score):
        needs = {
            "water": "Normal supply",
            "energy": "Normal supply",
            "hospitals": "Fully operational",
            "food": "Normal availability",
            "shelter": "Adequate", 
            "transportation": "Normal operations"
        }
        
        if score >= 75:
            needs.update({
                "water": "CRITICAL SHORTAGE - Rationing implemented",
                "energy": "REDUCED CAPACITY - Hydropower limited",
                "hospitals": "STRESSED - Water-dependent services affected",
                "food": "SHORTAGES - Crop failures, price increases",
                "shelter": "AT RISK - Water scarcity issues",
                "transportation": "IMPACTED - Dust storms, reduced visibility"
            })
        elif score >= 50:
            needs.update({
                "water": "RESTRICTED - Conservation measures",
                "energy": "STABLE - Some hydropower reduction",
                "hospitals": "OPERATIONAL - Water conservation needed",
                "food": "INCREASING PRICES - Supply chain stress",
                "shelter": "ADEQUATE - Water efficiency important",
                "transportation": "MINOR IMPACTS - Dust concerns"
            })
        elif score >= 20:
            needs.update({
                "water": "ADEQUATE - Conservation encouraged",
                "energy": "NORMAL - Monitor hydropower levels",
                "hospitals": "NORMAL - Water efficiency practices",
                "food": "STABLE - Monitor supplies",
                "shelter": "SECURE - Water conservation",
                "transportation": "NORMAL - Minimal impacts"
            })
        
        return needs

    def _get_air_quality_basic_needs(self, score):
        needs = {
            "water": "Normal supply",
            "energy": "Normal supply",
            "hospitals": "Fully operational",
            "food": "Normal availability",
            "shelter": "Adequate",
            "transportation": "Normal operations"
        }
        
        if score >= 75:
            needs.update({
                "water": "INDOOR USE RECOMMENDED - Avoid outdoor collection",
                "energy": "INCREASED DEMAND - Indoor air filtration",
                "hospitals": "OVERWHELMED - Respiratory emergencies surge",
                "food": "DELIVERY RECOMMENDED - Limit outdoor exposure",
                "shelter": "SEALED ENVIRONMENTS - Air filtration needed",
                "transportation": "REDUCED SERVICES - Poor visibility, health risks"
            })
        elif score >= 50:
            needs.update({
                "water": "ADEQUATE - Limit outdoor exposure",
                "energy": "STABLE - Some increased usage",
                "hospitals": "INCREASED LOAD - Respiratory cases rising",
                "food": "STABLE - Delivery options recommended",
                "shelter": "INDOOR FOCUS - Limit outdoor time",
                "transportation": "MODERATE DISRUPTIONS - Reduced outdoor activity"
            })
        elif score >= 20:
            needs.update({
                "water": "NORMAL - Sensitive groups take care",
                "energy": "NORMAL - Minimal impact",
                "hospitals": "NORMAL - Monitor sensitive patients",
                "food": "NORMAL - Outdoor activities limited for sensitive groups",
                "shelter": "SECURE - Normal operations",
                "transportation": "MINOR IMPACTS - Some visibility issues"
            })
        
        return needs

    def _get_water_quality_basic_needs(self, score):
        needs = {
            "water": "Normal supply",
            "energy": "Normal supply",
            "hospitals": "Fully operational",
            "food": "Normal availability",
            "shelter": "Adequate",
            "transportation": "Normal operations"
        }
        
        if score >= 75:
            needs.update({
                "water": "UNSAFE - Emergency distribution needed",
                "energy": "STABLE - Water treatment plants stressed",
                "hospitals": "OVERLOADED - Waterborne illness cases",
                "food": "CONTAMINATION RISK - Agriculture affected",
                "shelter": "WATER CRISIS - Alternative sources needed",
                "transportation": "MINIMAL IMPACT - Waterway transport affected"
            })
        elif score >= 50:
            needs.update({
                "water": "TREATMENT REQUIRED - Boil water advisory",
                "energy": "STABLE - Increased treatment costs",
                "hospitals": "INCREASED CASES - Gastrointestinal illnesses",
                "food": "MONITOR QUALITY - Irrigation concerns",
                "shelter": "WATER TREATMENT NEEDED - Filtration systems",
                "transportation": "MINIMAL IMPACT - Some waterway issues"
            })
        elif score >= 20:
            needs.update({
                "water": "MONITOR QUALITY - Some treatment advised",
                "energy": "NORMAL - Minimal impact",
                "hospitals": "NORMAL - Watch for water-related illnesses",
                "food": "ADEQUATE - Monitor irrigation water",
                "shelter": "SECURE - Water testing recommended",
                "transportation": "NORMAL - Minimal impact"
            })
        
        return needs

    def _get_population_basic_needs(self, score):
        needs = {
            "water": "Normal supply",
            "energy": "Normal supply",
            "hospitals": "Fully operational",
            "food": "Normal availability",
            "shelter": "Adequate",
            "transportation": "Normal operations"
        }
        
        if score >= 75:
            needs.update({
                "water": "INFRASTRUCTURE STRAIN - Supply shortages possible",
                "energy": "OVERLOADED - Frequent outages, capacity issues",
                "hospitals": "OVERWHELMED - Long wait times, resource shortages",
                "food": "SHORTAGES - Supply chain strain, price inflation",
                "shelter": "CRITICAL SHORTAGE - Housing crisis, overcrowding",
                "transportation": "SEVERE CONGESTION - Gridlock, extended commute times"
            })
        elif score >= 50:
            needs.update({
                "water": "INCREASING DEMAND - Infrastructure stress",
                "energy": "GROWING DEMAND - Occasional brownouts",
                "hospitals": "UNDER PRESSURE - Extended wait times",
                "food": "INFLATION PRESSURES - Supply chain stress",
                "shelter": "SHORTAGE DEVELOPING - Affordable housing crisis",
                "transportation": "HEAVY CONGESTION - Delays, infrastructure strain"
            })
        elif score >= 20:
            needs.update({
                "water": "ADEQUATE - Monitor demand growth",
                "energy": "STABLE - Plan for capacity increases",
                "hospitals": "ADEQUATE - Prepare for increased demand",
                "food": "SUFFICIENT - Monitor supply chains",
                "shelter": "ADEQUATE - Plan for growth management",
                "transportation": "MANAGEABLE - Some congestion issues"
            })
        
        return needs

    # [Keep all the previous recommendation and impact methods from the previous version]
    # Flood-related methods
    def _get_flood_recommendations(self, score, rainfall_mm):
        recommendations = []
        if score >= 75:
            recommendations.extend([
                "Activate emergency flood response plan",
                "Evacuate low-lying areas immediately",
                "Deploy sandbags and flood barriers",
                "Close flood-prone roads and bridges"
            ])
        elif score >= 50:
            recommendations.extend([
                "Monitor water levels continuously",
                "Prepare emergency shelters",
                "Alert residents in flood-prone zones",
                "Clear drainage systems"
            ])
        elif score >= 20:
            recommendations.extend([
                "Check local drainage systems",
                "Monitor weather forecasts",
                "Prepare emergency kits",
                "Identify evacuation routes"
            ])
        else:
            recommendations.append("Normal conditions - maintain routine monitoring")
        return recommendations

    def _get_flood_community_impact(self, score):
        if score >= 75:
            return "Severe disruption to transportation, potential property damage, health risks from contaminated water"
        elif score >= 50:
            return "Moderate disruption, some road closures, possible property damage in low-lying areas"
        elif score >= 20:
            return "Minor disruptions, localized flooding in poor drainage areas"
        else:
            return "Minimal impact on community activities"

    def _get_flood_type(self, rainfall_mm):
        if rainfall_mm > 100:
            return "Major flood event"
        elif rainfall_mm > 60:
            return "Flash flooding"
        elif rainfall_mm > 30:
            return "Urban flooding"
        else:
            return "Localized flooding"

    def _get_flood_impact_areas(self, score):
        if score >= 75:
            return ["Low-lying residential areas", "Urban centers", "Transportation corridors", "Commercial districts"]
        elif score >= 50:
            return ["Low-lying areas", "Poor drainage zones", "Some transportation routes"]
        elif score >= 20:
            return ["Localized poor drainage areas"]
        else:
            return ["Minimal impact areas"]

    # [Keep all other existing methods for fire, drought, air quality, water quality, population...]
    # Fire-related methods
    def _get_fire_recommendations(self, score, fire_count):
        recommendations = []
        if score >= 75:
            recommendations.extend([
                "Activate emergency fire response",
                "Evacuate high-risk areas immediately",
                "Close affected parks and natural areas",
                "Issue total fire ban"
            ])
        elif score >= 50:
            recommendations.extend([
                "Increase fire patrols",
                "Prepare evacuation plans",
                "Restrict outdoor burning",
                "Alert nearby communities"
            ])
        elif score >= 20:
            recommendations.extend([
                "Monitor fire weather conditions",
                "Clear vegetation around properties",
                "Prepare emergency water supplies",
                "Review evacuation routes"
            ])
        else:
            recommendations.append("Maintain normal fire safety protocols")
        return recommendations

    def _get_fire_community_impact(self, score):
        if score >= 75:
            return "High risk of property damage, potential evacuations, health impacts from smoke, transportation disruptions"
        elif score >= 50:
            return "Moderate risk to properties, possible evacuations, air quality concerns"
        elif score >= 20:
            return "Localized impacts, minor smoke concerns, outdoor activity restrictions"
        else:
            return "Minimal community impact"

    def _get_fire_intensity_level(self, intensity):
        if intensity > 0.8:
            return "Extreme"
        elif intensity > 0.6:
            return "High"
        elif intensity > 0.4:
            return "Moderate"
        else:
            return "Low"

    def _get_fire_evacuation_zones(self, score):
        if score >= 75:
            return ["Wildland-urban interface", "Dense vegetation areas", "All residential zones within 5km"]
        elif score >= 50:
            return ["High-risk vegetation zones", "Outlying residential areas"]
        elif score >= 20:
            return ["Immediate fire perimeter"]
        else:
            return ["No evacuation zones identified"]

    # Drought-related methods
    def _get_drought_recommendations(self, score):
        recommendations = []
        if score >= 75:
            recommendations.extend([
                "Implement strict water rationing",
                "Prioritize essential water use only",
                "Activate emergency water supplies",
                "Support agricultural communities"
            ])
        elif score >= 50:
            recommendations.extend([
                "Voluntary water conservation",
                "Monitor reservoir levels",
                "Implement water-saving measures",
                "Prepare drought contingency plans"
            ])
        elif score >= 20:
            recommendations.extend([
                "Promote water conservation",
                "Monitor drought indicators",
                "Prepare for potential restrictions"
            ])
        else:
            recommendations.append("Normal water management practices")
        return recommendations

    def _get_drought_community_impact(self, score):
        if score >= 75:
            return "Severe water shortages, agricultural losses, potential economic impacts, health concerns"
        elif score >= 50:
            return "Water restrictions likely, agricultural stress, increased fire risk"
        elif score >= 20:
            return "Minor water supply concerns, outdoor water use restrictions possible"
        else:
            return "Minimal impact on water resources"

    def _get_drought_category(self, score):
        if score >= 75:
            return "Exceptional Drought"
        elif score >= 50:
            return "Severe Drought"
        elif score >= 20:
            return "Moderate Drought"
        else:
            return "No Drought"

    def _get_water_restrictions(self, score):
        if score >= 75:
            return "Stage 4: Essential use only, no outdoor watering"
        elif score >= 50:
            return "Stage 3: Limited outdoor watering, car washing restrictions"
        elif score >= 20:
            return "Stage 2: Voluntary conservation, reduced outdoor watering"
        else:
            return "No restrictions"

    # Air quality methods
    def _get_air_quality_recommendations(self, score):
        recommendations = []
        if score >= 75:
            recommendations.extend([
                "Stay indoors with windows closed",
                "Use air purifiers if available",
                "Limit physical exertion outdoors",
                "Wear masks if going outside"
            ])
        elif score >= 50:
            recommendations.extend([
                "Sensitive groups should reduce outdoor activity",
                "Keep windows closed during peak pollution",
                "Monitor air quality updates"
            ])
        elif score >= 20:
            recommendations.extend([
                "Sensitive individuals should take precautions",
                "Moderate outdoor activities acceptable"
            ])
        else:
            recommendations.append("Air quality is good - normal activities")
        return recommendations

    def _get_air_quality_community_impact(self, score):
        if score >= 75:
            return "Serious health risks for all residents, school and business disruptions, transportation impacts"
        elif score >= 50:
            return "Health concerns for sensitive groups, reduced outdoor activities"
        elif score >= 20:
            return "Minor impacts for sensitive individuals only"
        else:
            return "Minimal health impacts"

    def _get_health_advisory(self, aqi):
        if aqi > 150:
            return "HEALTH WARNING: Serious effects for everyone"
        elif aqi > 100:
            return "HEALTH ALERT: Unhealthy for sensitive groups"
        elif aqi > 50:
            return "Moderate health concern"
        else:
            return "Good air quality"

    def _get_vulnerable_groups(self, aqi):
        if aqi > 100:
            return ["Children", "Elderly", "Pregnant women", "Respiratory conditions", "Heart conditions"]
        elif aqi > 50:
            return ["Children", "Elderly", "Respiratory conditions"]
        else:
            return ["None specific"]

    # Water quality methods
    def _get_water_quality_recommendations(self, score, ph_level):
        recommendations = []
        if score >= 75:
            recommendations.extend([
                "Boil water before consumption",
                "Avoid recreational water activities",
                "Use alternative water sources",
                "Report water quality concerns to authorities"
            ])
        elif score >= 50:
            recommendations.extend([
                "Use water filters for drinking",
                "Limit recreational water contact",
                "Monitor water quality updates"
            ])
        elif score >= 20:
            recommendations.extend([
                "Consider water treatment for sensitive uses",
                "Follow local water advisories"
            ])
        else:
            recommendations.append("Water quality is good - normal use")
        
        if ph_level < 6.5 or ph_level > 8.5:
            recommendations.append("pH levels outside optimal range - consider treatment")
        return recommendations

    def _get_water_quality_community_impact(self, score):
        if score >= 75:
            return "Serious health risks from water consumption, recreational closures, economic impacts on fishing/tourism"
        elif score >= 50:
            return "Health concerns for vulnerable groups, some recreational restrictions"
        elif score >= 20:
            return "Minor water quality concerns, precautionary measures advised"
        else:
            return "Minimal impact on water uses"

    def _get_water_health_implications(self, score):
        if score >= 75:
            return "High risk of waterborne diseases, gastrointestinal illnesses"
        elif score >= 50:
            return "Moderate health risk, avoid ingestion without treatment"
        elif score >= 20:
            return "Low risk, basic treatment recommended for sensitive uses"
        else:
            return "Minimal health risk"

    def _get_water_safety_level(self, score):
        if score >= 75:
            return "UNSAFE - Do not consume without treatment"
        elif score >= 50:
            return "CAUTION - Treat before consumption"
        elif score >= 20:
            return "MODERATE - Safe with basic treatment"
        else:
            return "SAFE - Generally safe for consumption"

    # Population pressure methods
    def _get_population_recommendations(self, score, population):
        recommendations = []
        if score >= 75:
            recommendations.extend([
                "Implement urban planning reforms",
                "Invest in public transportation infrastructure",
                "Develop affordable housing programs",
                "Expand social services capacity"
            ])
        elif score >= 50:
            recommendations.extend([
                "Monitor urban growth patterns",
                "Plan infrastructure upgrades",
                "Develop mixed-use zoning",
                "Improve public services"
            ])
        elif score >= 20:
            recommendations.extend([
                "Conduct growth impact studies",
                "Maintain infrastructure maintenance",
                "Monitor service delivery quality"
            ])
        else:
            recommendations.append("Sustainable growth patterns - continue current planning")
        return recommendations

    def _get_population_community_impact(self, score):
        if score >= 75:
            return "Severe strain on infrastructure, housing shortages, traffic congestion, service delivery challenges"
        elif score >= 50:
            return "Moderate infrastructure pressure, rising housing costs, transportation challenges"
        elif score >= 20:
            return "Minor growth pressures, manageable service demands"
        else:
            return "Sustainable community development"

    def _get_urban_challenges(self, score):
        challenges = []
        if score >= 75:
            challenges.extend(["Overcrowding", "Housing affordability", "Traffic congestion", "Service delivery strain"])
        elif score >= 50:
            challenges.extend(["Urban sprawl", "Infrastructure maintenance", "Public service demands"])
        elif score >= 20:
            challenges.extend(["Growth management", "Infrastructure planning"])
        else:
            challenges.append("Balanced development")
        return challenges

    def _get_infrastructure_needs(self, score):
        if score >= 75:
            return ["Major transportation upgrades", "Housing development", "Social service expansion", "Utility capacity increases"]
        elif score >= 50:
            return ["Infrastructure maintenance", "Public transit improvements", "Service capacity planning"]
        elif score >= 20:
            return ["Routine infrastructure updates", "Growth monitoring systems"]
        else:
            return ["Maintain current infrastructure"]