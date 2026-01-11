import json
import logging
from datetime import datetime
from typing import Dict, Any
from dataclasses import dataclass

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TrafficEvent:
    """Classe pour valider et structurer les événements de trafic"""
    sensor_id: str
    road_id: str
    road_type: str
    zone: str
    lane: str
    vehicle_count: int
    average_speed: float
    occupancy_rate: float
    event_time: str
    timestamp: int
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TrafficEvent':
        """Crée un TrafficEvent depuis un dictionnaire"""
        return cls(
            sensor_id=str(data['sensor_id']),
            road_id=str(data['road_id']),
            road_type=str(data['road_type']),
            zone=str(data['zone']),
            lane=str(data.get('lane', 'Voie_1')),
            vehicle_count=int(data['vehicle_count']),
            average_speed=float(data['average_speed']),
            occupancy_rate=float(data['occupancy_rate']),
            event_time=str(data['event_time']),
            timestamp=int(data['timestamp'])
        )
    
    def validate(self) -> bool:
        """Valide les données de l'événement"""
        try:
            # Validation des types
            assert isinstance(self.sensor_id, str) and len(self.sensor_id) > 0
            assert isinstance(self.road_id, str) and len(self.road_id) > 0
            assert self.road_type in ['autoroute', 'boulevard', 'avenue', 'rue']
            assert isinstance(self.zone, str) and len(self.zone) > 0
            assert 0 <= self.vehicle_count <= 500  # Valeur maximale réaliste
            assert 0 <= self.average_speed <= 200  # km/h
            assert 0 <= self.occupancy_rate <= 100  # pourcentage
            
            # Validation du timestamp
            datetime.fromisoformat(self.event_time.replace('Z', '+00:00'))
            
            return True
        except (AssertionError, ValueError, KeyError) as e:
            logger.warning(f"Événement invalide: {e} - Données: {self.__dict__}")
            return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire"""
        return {
            'sensor_id': self.sensor_id,
            'road_id': self.road_id,
            'road_type': self.road_type,
            'zone': self.zone,
            'lane': self.lane,
            'vehicle_count': self.vehicle_count,
            'average_speed': self.average_speed,
            'occupancy_rate': self.occupancy_rate,
            'event_time': self.event_time,
            'timestamp': self.timestamp,
            'ingestion_time': datetime.now().isoformat(),
            'year': datetime.fromisoformat(self.event_time.replace('Z', '+00:00')).year,
            'month': datetime.fromisoformat(self.event_time.replace('Z', '+00:00')).month,
            'day': datetime.fromisoformat(self.event_time.replace('Z', '+00:00')).day,
            'hour': datetime.fromisoformat(self.event_time.replace('Z', '+00:00')).hour
        }


class KafkaConfig:
    """Configuration Kafka"""
    
    @staticmethod
    def get_consumer_config(group_id: str = "traffic-ingestion-group"):
        """Retourne la configuration du consumer Kafka"""
        return {
            'bootstrap_servers': 'kafka:9092',
            'group_id': group_id,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'auto_commit_interval_ms': 5000,
            'max_poll_records': 100,
            'session_timeout_ms': 30000,
            'heartbeat_interval_ms': 10000
        }
    
    @staticmethod
    def get_producer_config():
        """Retourne la configuration du producer Kafka"""
        return {
            'bootstrap_servers': 'kafka:9092',
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 1,
            'compression_type': 'snappy'
        }
    
    @staticmethod
    def create_topic_config():
        """Configuration pour créer des topics"""
        return {
            'num_partitions': 3,
            'replication_factor': 1,
            'config': {
                'retention.ms': 604800000,  # 7 jours
                'segment.bytes': 1073741824,  # 1GB
                'cleanup.policy': 'delete'
            }
        }