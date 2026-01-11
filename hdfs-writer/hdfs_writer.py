import json
import time
import logging
import os
import signal
from datetime import datetime
from kafka import KafkaConsumer
from hdfs_utils import HDFSManager

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSWriter:
    """Consumer Kafka qui écrit les données dans HDFS"""
    
    def __init__(self):
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'traffic-events')
        self.consumer_group = os.getenv('CONSUMER_GROUP', 'hdfs-writer-group')
        self.hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://namenode:9870')
        self.hdfs_base_path = os.getenv('HDFS_BASE_PATH', '/data/raw/traffic')
        
        # Configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.flush_interval = int(os.getenv('FLUSH_INTERVAL', '30'))  # secondes
        
        # Statistiques
        self.stats = {
            'start_time': datetime.now(),
            'messages_received': 0,
            'messages_written': 0,
            'messages_failed': 0,
            'batches_written': 0,
            'last_flush_time': datetime.now(),
            'hdfs_connection_status': 'disconnected'
        }
        
        # Buffers
        self.event_buffer = []
        self.hdfs_manager = None
        self.consumer = None
        self.running = True
        
        # Gestion des signaux
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        logger.info("Initialisation du HDFS Writer")
        logger.info(f"Kafka Broker: {self.kafka_broker}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"HDFS NameNode: {self.hdfs_namenode}")
        logger.info(f"HDFS Base Path: {self.hdfs_base_path}")
        logger.info(f"Batch Size: {self.batch_size}")
        logger.info(f"Flush Interval: {self.flush_interval}s")
    
    def signal_handler(self, signum, frame):
        """Gère les signaux d'arrêt"""
        logger.info(f"Signal {signum} reçu, arrêt en cours...")
        self.running = False
    
    def connect_kafka(self):
        """Établit la connexion à Kafka"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Tentative de connexion à Kafka (tentative {attempt + 1}/{max_retries})...")
                
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.kafka_broker,
                    group_id=self.consumer_group,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=5000,
                    max_poll_records=self.batch_size,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                
                logger.info("Connexion à Kafka établie")
                logger.info(f"Abonné au topic: {self.topic}")
                logger.info(f"Consumer Group: {self.consumer_group}")
                return True
                
            except Exception as e:
                logger.error(f"Erreur de connexion Kafka: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Nouvelle tentative dans {retry_delay} secondes...")
                    time.sleep(retry_delay)
        
        logger.error("Échec de connexion à Kafka")
        return False
    
    def connect_hdfs(self):
        """Établit la connexion à HDFS"""
        try:
            logger.info("Connexion à HDFS...")
            self.hdfs_manager = HDFSManager(
                namenode_url=self.hdfs_namenode,
                base_path=self.hdfs_base_path
            )
            
            self.stats['hdfs_connection_status'] = 'connected'
            logger.info("Connexion à HDFS établie")
            
            # Afficher la structure HDFS
            self._log_hdfs_structure()
            return True
            
        except Exception as e:
            logger.error(f"Erreur de connexion HDFS: {str(e)}")
            self.stats['hdfs_connection_status'] = 'disconnected'
            return False
    
    def _log_hdfs_structure(self):
        """Affiche la structure HDFS"""
        try:
            stats = self.hdfs_manager.get_storage_stats()
            logger.info(f"HDFS Storage: {stats}")
            
            # Lister les répertoires de base
            base_dirs = self.hdfs_manager.list_files(self.hdfs_base_path)
            if base_dirs:
                logger.info(f"Répertoires dans {self.hdfs_base_path}: {', '.join(base_dirs)}")
                
        except Exception as e:
            logger.warning(f"Impossible d'afficher la structure HDFS: {str(e)}")
    
    def process_batch(self):
        """Traite un batch d'événements"""
        if not self.event_buffer:
            return
        
        try:
            logger.info(f"Traitement d'un batch de {len(self.event_buffer)} événements...")
            
            # Écrire le batch dans HDFS
            batch_stats = self.hdfs_manager.write_batch(self.event_buffer)
            
            # Mettre à jour les statistiques
            self.stats['messages_written'] += batch_stats['success']
            self.stats['messages_failed'] += batch_stats['failed']
            self.stats['batches_written'] += 1
            
            if batch_stats['success'] > 0:
                logger.info(f"Batch écrit avec succès: {batch_stats['success']}/{len(self.event_buffer)} événements")
            
            if batch_stats['failed'] > 0:
                logger.warning(f"Échecs dans le batch: {batch_stats['failed']} événements")
            
            # Vider le buffer
            self.event_buffer = []
            self.stats['last_flush_time'] = datetime.now()
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement du batch: {str(e)}")
            self.stats['messages_failed'] += len(self.event_buffer)
            self.event_buffer = []  # Vider le buffer même en cas d'erreur
    
    def should_flush(self):
        """Détermine si le buffer doit être vidé"""
        # Vérifier la taille du buffer
        if len(self.event_buffer) >= self.batch_size:
            return True
        
        # Vérifier l'intervalle de temps
        current_time = datetime.now()
        time_since_flush = (current_time - self.stats['last_flush_time']).total_seconds()
        
        if time_since_flush >= self.flush_interval and self.event_buffer:
            return True
        
        return False
    
    def print_statistics(self):
        """Affiche les statistiques"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        if elapsed > 0:
            throughput = self.stats['messages_received'] / elapsed
        else:
            throughput = 0
        
        logger.info("=" * 60)
        logger.info("STATISTIQUES HDFS WRITER")
        logger.info("=" * 60)
        logger.info(f"Durée: {elapsed:.0f}s")
        logger.info(f"Messages reçus: {self.stats['messages_received']}")
        logger.info(f"Messages écrits: {self.stats['messages_written']}")
        logger.info(f"Messages échoués: {self.stats['messages_failed']}")
        logger.info(f"Batches écrits: {self.stats['batches_written']}")
        logger.info(f"Débit: {throughput:.2f} msg/sec")
        logger.info(f"Statut HDFS: {self.stats['hdfs_connection_status']}")
        logger.info(f"Buffer actuel: {len(self.event_buffer)} événements")
        logger.info("=" * 60)
    
    def run(self):
        """Exécute le writer HDFS"""
        logger.info("Démarrage du HDFS Writer...")
        
        # Connexion à Kafka
        if not self.connect_kafka():
            logger.error("Impossible de se connecter à Kafka")
            return
        
        # Connexion à HDFS avec retry
        hdfs_connected = False
        for _ in range(3):
            if self.connect_hdfs():
                hdfs_connected = True
                break
            time.sleep(10)
        
        if not hdfs_connected:
            logger.error("Impossible de se connecter à HDFS après plusieurs tentatives")
            return
        
        logger.info("HDFS Writer prêt. Début du traitement...")
        
        try:
            while self.running:
                # Poll des messages Kafka
                raw_messages = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        self.stats['messages_received'] += 1
                        self.event_buffer.append(message.value)
                        
                        # Affichage périodique
                        if self.stats['messages_received'] % 100 == 0:
                            logger.info(f"Messages reçus: {self.stats['messages_received']}")
                        
                        # Traiter le batch si nécessaire
                        if self.should_flush():
                            self.process_batch()
                
                # Vérifier le buffer basé sur le temps (au cas où nous n'avons pas de nouveaux messages)
                if self.should_flush():
                    self.process_batch()
                
                # Afficher les statistiques toutes les 60 secondes
                if int(time.time()) % 60 == 0:
                    self.print_statistics()
        
        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur...")
        except Exception as e:
            logger.error(f"Erreur inattendue: {str(e)}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Arrêt propre du service"""
        logger.info("Arrêt du HDFS Writer...")
        self.running = False
        
        # Traiter les événements restants dans le buffer
        if self.event_buffer:
            logger.info(f"Traitement des {len(self.event_buffer)} événements restants...")
            self.process_batch()
        
        # Fermer les connexions
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer Kafka fermé")
        
        # Statistiques finales
        self.print_statistics()
        logger.info("HDFS Writer arrêté")

def main():
    """Point d'entrée principal"""
    logger.info("=" * 70)
    logger.info("HDFS WRITER - Stockage des données de trafic dans HDFS")
    logger.info("=" * 70)
    
    writer = HDFSWriter()
    writer.run()

if __name__ == "__main__":
    main()