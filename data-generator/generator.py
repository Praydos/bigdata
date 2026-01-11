import json
import time
import random
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TrafficDataGenerator:
    def __init__(self, kafka_broker='kafka:9092', topic='traffic-events'):
        self.faker = Faker('fr_FR')
        self.kafka_broker = kafka_broker
        self.topic = topic
        
        # Configuration des capteurs
        self.sensors = self._initialize_sensors()
        
        # Configuration Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        
        logger.info(f"Générateur de données initialisé. Connexion à Kafka: {kafka_broker}")
    
    def _initialize_sensors(self):
        """Initialise un réseau de capteurs réalistes"""
        sensors = []
        sensor_id = 1000
        
        # Définition des routes et zones
        routes = [
            {"road_id": "A1", "road_type": "autoroute", "zone": "Nord"},
            {"road_id": "A6", "road_type": "autoroute", "zone": "Sud"},
            {"road_id": "BD01", "road_type": "boulevard", "zone": "Centre"},
            {"road_id": "AV001", "road_type": "avenue", "zone": "Est"},
            {"road_id": "RUE042", "road_type": "rue", "zone": "Ouest"},
            {"road_id": "A4", "road_type": "autoroute", "zone": "Est"},
            {"road_id": "RUE101", "road_type": "rue", "zone": "Centre"},
            {"road_id": "AV202", "road_type": "avenue", "zone": "Nord"},
            {"road_id": "BD303", "road_type": "boulevard", "zone": "Sud"},
            {"road_id": "A10", "road_type": "autoroute", "zone": "Ouest"}
        ]
        
        for i, route in enumerate(routes):
            for j in range(3):  # 3 capteurs par route
                sensors.append({
                    "sensor_id": f"SENSOR_{sensor_id}",
                    "road_id": route["road_id"],
                    "road_type": route["road_type"],
                    "zone": route["zone"],
                    "lane": f"Voie_{j+1}"
                })
                sensor_id += 1
        
        logger.info(f"Initialisation de {len(sensors)} capteurs")
        return sensors
    
    def _get_time_pattern_factor(self, hour):
        """Retourne un facteur multiplicatif basé sur l'heure pour simuler les variations de trafic"""
        # Heures de pointe: 7h-9h et 17h-19h
        if (7 <= hour < 9) or (17 <= hour < 19):
            return random.uniform(2.0, 3.0)  # Fort trafic
        elif (9 <= hour < 12) or (14 <= hour < 17):
            return random.uniform(1.2, 1.8)  # Trafic moyen
        elif (12 <= hour < 14):
            return random.uniform(1.0, 1.5)  # Heure de déjeuner
        else:
            return random.uniform(0.3, 0.8)  # Faible trafic (nuit)
    
    def _generate_realistic_values(self, road_type, hour):
        """Génère des valeurs réalistes basées sur le type de route et l'heure"""
        base_values = {
            "autoroute": {
                "min_speed": 60,
                "max_speed": 130,
                "base_vehicles": 50,
                "max_vehicles": 200
            },
            "boulevard": {
                "min_speed": 30,
                "max_speed": 70,
                "base_vehicles": 20,
                "max_vehicles": 100
            },
            "avenue": {
                "min_speed": 20,
                "max_speed": 50,
                "base_vehicles": 15,
                "max_vehicles": 80
            },
            "rue": {
                "min_speed": 10,
                "max_speed": 30,
                "base_vehicles": 5,
                "max_vehicles": 40
            }
        }
        
        pattern_factor = self._get_time_pattern_factor(hour)
        road_config = base_values[road_type]
        
        # Génération des valeurs avec variabilité réaliste
        vehicle_count = int(road_config["base_vehicles"] * pattern_factor + random.uniform(-5, 5))
        vehicle_count = max(5, min(vehicle_count, road_config["max_vehicles"]))
        
        # La vitesse diminue quand le trafic augmente
        speed_factor = max(0.3, 1.0 - (vehicle_count / road_config["max_vehicles"]) * 0.7)
        average_speed = random.uniform(
            road_config["min_speed"] * speed_factor,
            road_config["max_speed"] * speed_factor
        )
        
        # Taux d'occupation basé sur le nombre de véhicules
        max_occupancy = 100 if road_type == "rue" else 80
        occupancy_rate = min(max_occupancy, (vehicle_count / road_config["max_vehicles"]) * 100)
        occupancy_rate += random.uniform(-5, 5)
        occupancy_rate = max(0, min(100, occupancy_rate))
        
        return vehicle_count, average_speed, occupancy_rate
    
    def generate_event(self, sensor):
        """Génère un événement de trafic pour un capteur spécifique"""
        now = datetime.now()
        hour = now.hour
        
        vehicle_count, average_speed, occupancy_rate = self._generate_realistic_values(
            sensor["road_type"], hour
        )
        
        event = {
            "sensor_id": sensor["sensor_id"],
            "road_id": sensor["road_id"],
            "road_type": sensor["road_type"],
            "zone": sensor["zone"],
            "lane": sensor["lane"],
            "vehicle_count": vehicle_count,
            "average_speed": round(average_speed, 1),
            "occupancy_rate": round(occupancy_rate, 1),
            "event_time": now.isoformat(),
            "timestamp": int(time.time() * 1000)
        }
        
        return event
    
    def send_to_kafka(self, event):
        """Envoie l'événement à Kafka"""
        try:
            self.producer.send(self.topic, event)
            self.producer.flush()
            logger.debug(f"Événement envoyé: {event['sensor_id']} - {event['event_time']}")
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi à Kafka: {str(e)}")
    
    def run(self, interval_seconds=5):
        """Exécute le générateur en continu"""
        logger.info("Démarrage du générateur de données de trafic...")
        logger.info(f"Intervalle d'envoi: {interval_seconds} secondes")
        logger.info(f"Topic Kafka: {self.topic}")
        
        try:
            while True:
                for sensor in self.sensors:
                    event = self.generate_event(sensor)
                    self.send_to_kafka(event)
                    
                    # Log périodique pour monitoring
                    if random.random() < 0.1:  # 10% des événements sont loggés
                        logger.info(f"Événement généré: {json.dumps(event, indent=2)}")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Arrêt du générateur...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    # Récupération des variables d'environnement
    import os
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
    
    # Création et exécution du générateur
    generator = TrafficDataGenerator(kafka_broker=kafka_broker)
    generator.run(interval_seconds=5)