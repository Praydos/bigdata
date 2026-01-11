import json
from datetime import datetime
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass

@dataclass
class TrafficMetrics:
    """Métriques de trafic calculées"""
    zone: str
    avg_vehicle_count: float
    avg_speed: float
    avg_occupancy_rate: float
    congestion_rate: float
    total_vehicles: int
    road_count: int
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'zone': self.zone,
            'avg_vehicle_count': round(self.avg_vehicle_count, 2),
            'avg_speed': round(self.avg_speed, 2),
            'avg_occupancy_rate': round(self.avg_occupancy_rate, 2),
            'congestion_rate': round(self.congestion_rate, 2),
            'total_vehicles': self.total_vehicles,
            'road_count': self.road_count,
            'timestamp': self.timestamp,
            'processing_time': datetime.now().isoformat()
        }

@dataclass
class RoadMetrics:
    """Métriques par route"""
    road_id: str
    road_type: str
    avg_speed: float
    avg_vehicle_count: float
    congestion_count: int
    total_measurements: int
    zone: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'road_id': self.road_id,
            'road_type': self.road_type,
            'avg_speed': round(self.avg_speed, 2),
            'avg_vehicle_count': round(self.avg_vehicle_count, 2),
            'congestion_percentage': round((self.congestion_count / self.total_measurements) * 100, 2) if self.total_measurements > 0 else 0,
            'total_measurements': self.total_measurements,
            'zone': self.zone,
            'processing_time': datetime.now().isoformat()
        }

@dataclass
class CongestionAlert:
    """Alerte de congestion"""
    zone: str
    road_id: str
    congestion_level: str  # LOW, MEDIUM, HIGH, CRITICAL
    current_occupancy: float
    avg_speed: float
    vehicle_count: int
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'zone': self.zone,
            'road_id': self.road_id,
            'congestion_level': self.congestion_level,
            'current_occupancy': round(self.current_occupancy, 2),
            'avg_speed': round(self.avg_speed, 2),
            'vehicle_count': self.vehicle_count,
            'timestamp': self.timestamp,
            'alert_time': datetime.now().isoformat()
        }

class TrafficAnalyzer:
    """Analyseur de données de trafic"""
    
    @staticmethod
    def calculate_congestion_level(occupancy_rate: float, avg_speed: float, road_type: str) -> str:
        """Calcule le niveau de congestion"""
        if road_type == "autoroute":
            if occupancy_rate > 85 or avg_speed < 40:
                return "CRITICAL"
            elif occupancy_rate > 70 or avg_speed < 60:
                return "HIGH"
            elif occupancy_rate > 50 or avg_speed < 80:
                return "MEDIUM"
            else:
                return "LOW"
        elif road_type == "boulevard":
            if occupancy_rate > 80 or avg_speed < 20:
                return "CRITICAL"
            elif occupancy_rate > 65 or avg_speed < 30:
                return "HIGH"
            elif occupancy_rate > 45 or avg_speed < 40:
                return "MEDIUM"
            else:
                return "LOW"
        else:  # rue, avenue
            if occupancy_rate > 75 or avg_speed < 10:
                return "CRITICAL"
            elif occupancy_rate > 60 or avg_speed < 15:
                return "HIGH"
            elif occupancy_rate > 40 or avg_speed < 20:
                return "MEDIUM"
            else:
                return "LOW"
    
    @staticmethod
    def is_congested(occupancy_rate: float, avg_speed: float, road_type: str) -> bool:
        """Détermine si un tronçon est congestionné"""
        congestion_level = TrafficAnalyzer.calculate_congestion_level(occupancy_rate, avg_speed, road_type)
        return congestion_level in ["HIGH", "CRITICAL"]
    
    @staticmethod
    def calculate_zone_metrics(events: List[Dict[str, Any]]) -> TrafficMetrics:
        """Calcule les métriques pour une zone"""
        if not events:
            return None
        
        zone = events[0].get('zone', 'Unknown')
        total_vehicles = 0
        total_speed = 0
        total_occupancy = 0
        congestion_count = 0
        roads = set()
        
        for event in events:
            total_vehicles += event.get('vehicle_count', 0)
            total_speed += event.get('average_speed', 0)
            total_occupancy += event.get('occupancy_rate', 0)
            roads.add(event.get('road_id', ''))
            
            # Vérifier la congestion
            if TrafficAnalyzer.is_congested(
                event.get('occupancy_rate', 0),
                event.get('average_speed', 0),
                event.get('road_type', 'rue')
            ):
                congestion_count += 1
        
        num_events = len(events)
        return TrafficMetrics(
            zone=zone,
            avg_vehicle_count=total_vehicles / num_events if num_events > 0 else 0,
            avg_speed=total_speed / num_events if num_events > 0 else 0,
            avg_occupancy_rate=total_occupancy / num_events if num_events > 0 else 0,
            congestion_rate=(congestion_count / num_events) * 100 if num_events > 0 else 0,
            total_vehicles=total_vehicles,
            road_count=len(roads),
            timestamp=datetime.now().isoformat()
        )