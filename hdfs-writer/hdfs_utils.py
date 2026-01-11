import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from hdfs import InsecureClient, HdfsError
import time

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSManager:
    """Gestionnaire HDFS pour écrire les données de trafic"""
    
    def __init__(self, namenode_url: str = "http://namenode:9870", base_path: str = "/data/raw/traffic"):
        self.namenode_url = namenode_url
        self.base_path = base_path
        self.client = None
        self._connect_retries = 5
        self._connect_delay = 10  # secondes
        
        self._connect()
    
    def _connect(self):
        """Établit la connexion à HDFS avec retry"""
        for attempt in range(self._connect_retries):
            try:
                logger.info(f"Tentative de connexion à HDFS (tentative {attempt + 1}/{self._connect_retries})...")
                self.client = InsecureClient(self.namenode_url, root='/', timeout=30)
                
                # Tester la connexion
                self.client.status('/')
                logger.info(f"Connexion à HDFS établie: {self.namenode_url}")
                logger.info(f"Base path: {self.base_path}")
                
                # Créer la structure de base
                self._create_base_structure()
                return True
                
            except Exception as e:
                logger.warning(f"Échec de connexion à HDFS: {str(e)}")
                if attempt < self._connect_retries - 1:
                    logger.info(f"Nouvelle tentative dans {self._connect_delay} secondes...")
                    time.sleep(self._connect_delay)
        
        logger.error(f"Impossible de se connecter à HDFS après {self._connect_retries} tentatives")
        return False
    
    def _create_base_structure(self):
        """Crée la structure de dossiers HDFS de base"""
        try:
            # Créer le répertoire racine des données brutes
            if not self.client.status(self.base_path, strict=False):
                self.client.makedirs(self.base_path)
                logger.info(f"Créé: {self.base_path}")
            
            # Créer les sous-répertoires
            subdirs = [
                f"{self.base_path}/_tmp",
                f"{self.base_path}/_error",
                f"{self.base_path}/_archive"
            ]
            
            for subdir in subdirs:
                if not self.client.status(subdir, strict=False):
                    self.client.makedirs(subdir)
                    logger.info(f"Créé: {subdir}")
            
            # Définir les permissions
            self.client.set_permission(self.base_path, '755')
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de la structure HDFS: {str(e)}")
            raise
    
    def _get_hdfs_path(self, event: Dict[str, Any]) -> str:
        """Génère le chemin HDFS basé sur la date et la zone"""
        try:
            # Parser le timestamp de l'événement
            event_time = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
            
            # Extraire les composants de date
            year = event_time.year
            month = f"{event_time.month:02d}"
            day = f"{event_time.day:02d}"
            hour = f"{event_time.hour:02d}"
            
            # Nettoyer la zone (remplacer les caractères spéciaux)
            zone = event.get('zone', 'unknown').replace('/', '_').replace(' ', '_')
            
            # Construire le chemin
            path = f"{self.base_path}/year={year}/month={month}/day={day}/zone={zone}/"
            
            return path
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération du chemin HDFS: {str(e)}")
            # Chemin par défaut en cas d'erreur
            return f"{self.base_path}/_error/unknown_date/"
    
    def _get_filename(self, event: Dict[str, Any]) -> str:
        """Génère le nom de fichier"""
        try:
            timestamp = int(event.get('timestamp', time.time() * 1000))
            sensor_id = event.get('sensor_id', 'unknown').replace('/', '_')
            
            # Format: sensorid_timestamp.json
            filename = f"{sensor_id}_{timestamp}.json"
            return filename
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération du nom de fichier: {str(e)}")
            return f"unknown_{int(time.time() * 1000)}.json"
    
    def write_event(self, event: Dict[str, Any], batch_mode: bool = False) -> bool:
        """Écrit un événement dans HDFS"""
        try:
            # Valider l'événement
            if not self._validate_event(event):
                logger.warning(f"Événement invalide, ignoré: {event.get('sensor_id', 'unknown')}")
                return False
            
            # Obtenir le chemin et nom de fichier
            hdfs_path = self._get_hdfs_path(event)
            filename = self._get_filename(event)
            full_path = hdfs_path + filename
            
            # S'assurer que le chemin existe
            if not self.client.status(hdfs_path, strict=False):
                self.client.makedirs(hdfs_path)
                logger.debug(f"Créé le chemin HDFS: {hdfs_path}")
            
            # Convertir en JSON
            event_json = json.dumps(event, ensure_ascii=False, indent=None)
            
            # Écrire dans HDFS
            if batch_mode:
                # Mode batch: écrire en append
                self.client.write(full_path, data=event_json + '\n', append=True, encoding='utf-8')
            else:
                # Mode single: créer un nouveau fichier
                self.client.write(full_path, data=event_json, encoding='utf-8', overwrite=True)
            
            # Log périodique pour monitoring
            if int(time.time()) % 60 == 0:  # Toutes les 60 secondes
                logger.info(f"Écrit dans HDFS: {full_path}")
            
            return True
            
        except HdfsError as e:
            logger.error(f"Erreur HDFS: {str(e)}")
            # Tentative de reconnexion
            if "Connection refused" in str(e) or "timed out" in str(e):
                self._connect()
            return False
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture dans HDFS: {str(e)}")
            return False
    
    def write_batch(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Écrit un batch d'événements dans HDFS"""
        stats = {
            'total': len(events),
            'success': 0,
            'failed': 0,
            'errors': []
        }
        
        try:
            # Grouper les événements par chemin HDFS
            grouped_events = {}
            for event in events:
                if self._validate_event(event):
                    hdfs_path = self._get_hdfs_path(event)
                    if hdfs_path not in grouped_events:
                        grouped_events[hdfs_path] = []
                    grouped_events[hdfs_path].append(event)
                else:
                    stats['failed'] += 1
            
            # Écrire chaque groupe
            for hdfs_path, path_events in grouped_events.items():
                # S'assurer que le chemin existe
                if not self.client.status(hdfs_path, strict=False):
                    self.client.makedirs(hdfs_path)
                
                # Générer un nom de fichier batch
                timestamp = int(time.time() * 1000)
                batch_filename = f"batch_{timestamp}.json"
                full_path = hdfs_path + batch_filename
                
                # Convertir tous les événements en JSON
                events_json = '\n'.join([json.dumps(e, ensure_ascii=False, indent=None) for e in path_events])
                
                # Écrire le batch
                self.client.write(full_path, data=events_json, encoding='utf-8', overwrite=True)
                stats['success'] += len(path_events)
                
                logger.info(f"Batch écrit: {full_path} ({len(path_events)} événements)")
            
            return stats
            
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture du batch: {str(e)}")
            stats['errors'].append(str(e))
            return stats
    
    def _validate_event(self, event: Dict[str, Any]) -> bool:
        """Valide un événement avant écriture"""
        try:
            required_fields = ['sensor_id', 'road_id', 'vehicle_count', 
                             'average_speed', 'occupancy_rate', 'event_time']
            
            # Vérifier les champs requis
            for field in required_fields:
                if field not in event:
                    logger.warning(f"Champ manquant: {field}")
                    return False
            
            # Vérifier les types
            if not isinstance(event['vehicle_count'], (int, float)):
                return False
            if not isinstance(event['average_speed'], (int, float)):
                return False
            if not isinstance(event['occupancy_rate'], (int, float)):
                return False
            
            # Vérifier les plages
            if event['vehicle_count'] < 0 or event['vehicle_count'] > 1000:
                return False
            if event['average_speed'] < 0 or event['average_speed'] > 300:
                return False
            if event['occupancy_rate'] < 0 or event['occupancy_rate'] > 100:
                return False
            
            # Valider le timestamp
            try:
                datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
            except:
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Erreur de validation: {str(e)}")
            return False
    
    def list_files(self, path: str = None) -> List[str]:
        """Liste les fichiers dans un chemin HDFS"""
        try:
            target_path = path or self.base_path
            files = self.client.list(target_path)
            return files
        except Exception as e:
            logger.error(f"Erreur lors du listing: {str(e)}")
            return []
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Récupère les statistiques de stockage"""
        try:
            # Note: Cette implémentation est basique, on pourrait utiliser
            # hdfs dfs -du pour des stats plus précises
            stats = {
                'base_path': self.base_path,
                'available': True,
                'timestamp': datetime.now().isoformat()
            }
            
            # Vérifier l'état du NameNode
            status = self.client.status(self.base_path)
            stats.update({
                'path': status['path'],
                'type': status['type'],
                'length': status['length'] if 'length' in status else 0,
                'modificationTime': status['modificationTime']
            })
            
            return stats
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des stats: {str(e)}")
            return {'available': False, 'error': str(e)}