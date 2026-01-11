import subprocess
import time

def check_hdfs_status():
    """Vérifie l'état de HDFS"""
    print("=== VÉRIFICATION HDFS ===")
    
    # Attendre que HDFS soit prêt
    print("Attente que HDFS soit prêt...")
    time.sleep(30)
    
    # Vérifier le NameNode
    print("\n1. Vérification du NameNode...")
    try:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfsadmin", "-report"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("✓ NameNode fonctionnel")
            # Extraire les infos importantes
            for line in result.stdout.split('\n'):
                if "Live datanodes" in line:
                    print(f"  {line}")
        else:
            print("✗ NameNode non disponible")
            print(f"  Erreur: {result.stderr}")
    except Exception as e:
        print(f"✗ Erreur: {e}")
    
    # Lister les répertoires
    print("\n2. Structure des répertoires...")
    try:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("✓ Structure HDFS:")
            print(result.stdout)
        else:
            print("✗ Impossible de lister les répertoires")
    except Exception as e:
        print(f"✗ Erreur: {e}")
    
    # Vérifier l'écriture
    print("\n3. Test d'écriture...")
    try:
        test_file = "/test_hdfs.txt"
        test_content = "Test HDFS from Docker\n"
        
        # Écrire
        subprocess.run(
            ["docker", "exec", "namenode", "bash", "-c", f"echo '{test_content}' | hdfs dfs -put - {test_file}"],
            capture_output=True
        )
        
        # Lire
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", test_file],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and test_content.strip() in result.stdout:
            print("✓ Écriture/Lecture HDFS fonctionnelle")
        else:
            print("✗ Problème avec l'écriture/lecture")
        
        # Nettoyer
        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-rm", test_file],
            capture_output=True
        )
        
    except Exception as e:
        print(f"✗ Erreur: {e}")
    
    print("\n=== VÉRIFICATION TERMINÉE ===")

if __name__ == "__main__":
    check_hdfs_status()