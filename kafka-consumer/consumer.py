import json
import time
import logging
import os
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka_utils import TrafficEvent, KafkaConfig

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrafficDataIngestor:
    """Classe principale pour l'ingestion des données de trafic"""
    
    def __init__(self):
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'traffic-events')
        self.consumer_group = os.getenv('CONSUMER_GROUP', 'traffic-ingestion-group')
        
        # Statistiques
        self.stats = {
            'messages_processed': 0,
            'valid_messages': 0,
            'invalid_messages': 0,
            'last_processed_time': None,
            'start_time': datetime.now()
        }
        
        self.consumer = None
        self.producer = None  # Pour éventuellement retransmettre des messages valides
        self.running = True
        
        logger.info("Initialisation de l'ingestor de données de trafic")
        logger.info(f"Broker Kafka: {self.kafka_broker}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Consumer Group: {self.consumer_group}")
    
    def connect_kafka(self, max_retries=10, retry_delay=5):
        """Établit la connexion à Kafka avec retry"""
        retries = 0
        
        while retries < max_retries:
            try:
                logger.info(f"Tentative de connexion à Kafka (tentative {retries + 1}/{max_retries})...")
                
                # Création du consumer
                self.consumer = KafkaConsumer(
                    self.topic,
                    **KafkaConfig.get_consumer_config(self.consumer_group)
                )
                
                # Création d'un producer pour éventuellement publier des messages traités
                self.producer = KafkaProducer(
                    **KafkaConfig.get_producer_config(),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                
                logger.info("Connexion à Kafka établie avec succès!")
                logger.info(f"Abonnement au topic: {self.topic}")
                logger.info(f"Consumer Group: {self.consumer_group}")
                logger.info(f"Partitions assignées: {self.consumer.assignment()}")
                
                return True
                
            except NoBrokersAvailable:
                retries += 1
                logger.warning(f"Brokers Kafka non disponibles. Nouvelle tentative dans {retry_delay}s...")
                time.sleep(retry_delay)
            except Exception as e:
                retries += 1
                logger.error(f"Erreur de connexion Kafka: {str(e)}. Nouvelle tentative dans {retry_delay}s...")
                time.sleep(retry_delay)
        
        logger.error(f"Échec de connexion à Kafka après {max_retries} tentatives")
        return False
    
    def process_message(self, message):
        """Traite un message Kafka individuel"""
        try:
            # Désérialisation du message
            data = json.loads(message.value.decode('utf-8'))
            
            # Création et validation de l'événement
            event = TrafficEvent.from_dict(data)
            
            if event.validate():
                # Traitement de l'événement valide
                self.handle_valid_event(event)
                self.stats['valid_messages'] += 1
                
                # Enregistrer dans un topic de validation si nécessaire
                # self.producer.send('traffic-events-validated', event.to_dict())
                
                return event
            else:
                self.stats['invalid_messages'] += 1
                logger.warning(f"Message invalide rejeté: {data.get('sensor_id', 'Unknown')}")
                
                # Enregistrer dans un topic d'erreur si nécessaire
                # error_data = {
                #     'original_data': data,
                #     'error': 'Validation failed',
                #     'timestamp': datetime.now().isoformat()
                # }
                # self.producer.send('traffic-events-errors', error_data)
                
                return None
                
        except json.JSONDecodeError as e:
            self.stats['invalid_messages'] += 1
            logger.error(f"Erreur de décodage JSON: {str(e)}")
            return None
        except Exception as e:
            self.stats['invalid_messages'] += 1
            logger.error(f"Erreur de traitement: {str(e)}")
            return None
    
    def handle_valid_event(self, event):
        """Gère un événement valide"""
        # Ici, nous pourrions:
        # 1. Écrire dans HDFS (étape suivante)
        # 2. Publier dans un autre topic
        # 3. Effectuer un traitement en temps réel
        
        # Pour l'instant, nous allons juste logger et stocker en mémoire
        processed_data = event.to_dict()
        
        # Log des statistiques périodiques
        if self.stats['valid_messages'] % 100 == 0:
            logger.info(f"Statistiques: {self.stats['valid_messages']} messages valides traités")
            
            # Affichage d'un exemple de données
            logger.info(f"Exemple d'événement: Capteur={event.sensor_id}, "
                       f"Route={event.road_id}, "
                       f"Véhicules={event.vehicle_count}, "
                       f"Vitesse={event.average_speed}km/h")
        
        return processed_data
    
    def calculate_throughput(self):
        """Calcule le débit de traitement"""
        if self.stats['last_processed_time']:
            elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
            if elapsed > 0:
                throughput = self.stats['messages_processed'] / elapsed
                return round(throughput, 2)
        return 0
    
    def print_statistics(self):
        """Affiche les statistiques de traitement"""
        logger.info("=" * 50)
        logger.info("STATISTIQUES D'INGESTION")
        logger.info("=" * 50)
        logger.info(f"Durée d'exécution: {datetime.now() - self.stats['start_time']}")
        logger.info(f"Messages traités: {self.stats['messages_processed']}")
        logger.info(f"Messages valides: {self.stats['valid_messages']}")
        logger.info(f"Messages invalides: {self.stats['invalid_messages']}")
        logger.info(f"Débit: {self.calculate_throughput()} messages/sec")
        
        if self.stats['messages_processed'] > 0:
            valid_percentage = (self.stats['valid_messages'] / self.stats['messages_processed']) * 100
            logger.info(f"Taux de validité: {valid_percentage:.2f}%")
        logger.info("=" * 50)
    
    def run(self, batch_size=100):
        """Exécute le consumer en continu"""
        logger.info("Démarrage du consumer d'ingestion...")
        
        # Connexion à Kafka
        if not self.connect_kafka():
            logger.error("Impossible de se connecter à Kafka. Arrêt.")
            return
        
        try:
            batch_counter = 0
            
            while self.running:
                # Poll des messages avec timeout
                raw_messages = self.consumer.poll(timeout_ms=1000, max_records=batch_size)
                
                for topic_partition, messages in raw_messages.items():
                    logger.debug(f"Traitement de {len(messages)} messages depuis {topic_partition}")
                    
                    for message in messages:
                        self.stats['messages_processed'] += 1
                        self.process_message(message)
                        
                        # Commit asynchrone périodique
                        batch_counter += 1
                        if batch_counter % 100 == 0:
                            self.consumer.commit_async()
                    
                    # Affichage des statistiques toutes les 1000 messages
                    if self.stats['messages_processed'] % 1000 == 0:
                        self.print_statistics()
                
                # Traitement des callbacks de commit
                self.consumer._coordinator.poll_heartbeat()
                
        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur...")
        except Exception as e:
            logger.error(f"Erreur inattendue: {str(e)}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Arrêt propre du consumer"""
        logger.info("Arrêt du consumer d'ingestion...")
        self.running = False
        
        if self.consumer:
            self.consumer.commit_sync()
            self.consumer.close()
            logger.info("Consumer Kafka fermé")
        
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer Kafka fermé")
        
        # Affichage final des statistiques
        self.print_statistics()
        
        logger.info("Ingestion terminée")


def main():
    """Point d'entrée principal"""
    logger.info("=" * 60)
    logger.info("SYSTÈME D'INGESTION DE DONNÉES DE TRAFIC URBAN")
    logger.info("=" * 60)
    
    # Configuration des paramètres d'ingestion
    import argparse
    parser = argparse.ArgumentParser(description='Consumer Kafka pour données de trafic')
    parser.add_argument('--batch-size', type=int, default=100, help='Taille du batch de messages')
    parser.add_argument('--verbose', action='store_true', help='Mode verbeux')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Création et exécution de l'ingestor
    ingestor = TrafficDataIngestor()
    
    try:
        ingestor.run(batch_size=args.batch_size)
    except Exception as e:
        logger.error(f"Erreur fatale: {str(e)}")
        raise


if __name__ == "__main__":
    main()