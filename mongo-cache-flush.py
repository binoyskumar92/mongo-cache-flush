from requests.auth import HTTPDigestAuth
import requests
from pymongo import MongoClient
import logging
from typing import List, Dict

# Configuration
PUBLIC_KEY = 'xrnzzfvp'
PRIVATE_KEY = '3e8b5708-5a84-40b6-a413-11e332783037'
ORG_ID = '6408b431da61be13461e54c3'
NAMESPACE = 'sample.coll'

# MongoDB user config
MONGO_ADMIN_USER = 'mongoadmin'  # Your admin username
MONGO_ADMIN_PASSWORD = 'passwordone'  # Your admin password
NEW_USER = 'mongops'
NEW_USER_PASSWORD = 'mongops123'

# API Setup
BASE_URL = 'https://cloud.mongodb.com/api/public/v1.0'
DIGEST_AUTH = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_user_and_role(node: Dict) -> bool:
    """Setup user and required role on a primary node."""
    try:
        client_url = f'mongodb://{MONGO_ADMIN_USER}:{MONGO_ADMIN_PASSWORD}@{node["hostname"]}:{node["port"]}/admin'
        client = MongoClient(client_url, 
                           directConnection=True,
                           connectTimeoutMS=5000, 
                           serverSelectionTimeoutMS=5000)
        
        admin_db = client.admin

        # 1. Create user
        try:
            admin_db.command('createUser', NEW_USER, 
                           pwd=NEW_USER_PASSWORD,
                           roles=[{'role': 'clusterManager', 'db': 'admin'}])
            logger.info(f"Created user {NEW_USER} on {node['hostname']}")
        except Exception as e:
            if 'already exists' not in str(e):
                raise
            logger.info(f"User {NEW_USER} already exists on {node['hostname']}")

        # 2. Create role
        try:
            admin_db.command('createRole', 'flush_routing_table_cache_updates',
                           privileges=[{
                               'resource': {'cluster': True},
                               'actions': ['internal']
                           }],
                           roles=[])
            logger.info(f"Created role flush_routing_table_cache_updates on {node['hostname']}")
        except Exception as e:
            if 'already exists' not in str(e):
                raise
            logger.info(f"Role already exists on {node['hostname']}")

        # 3. Grant role to user
        admin_db.command('grantRolesToUser', NEW_USER, 
                        roles=['flush_routing_table_cache_updates'])
        logger.info(f"Granted role to user {NEW_USER} on {node['hostname']}")

        return True

    except Exception as e:
        logger.error(f"Error setting up user/role on {node['hostname']}: {e}")
        return False
    finally:
        client.close()

def get_all_hosts() -> List[Dict]:
    """Get all MongoDB hosts across all projects in the organization."""
    all_hosts = []
    
    # Get all groups (projects)
    group_url = f'{BASE_URL}/orgs/{ORG_ID}/groups'
    group_response = requests.get(group_url, auth=DIGEST_AUTH)
    print(group_response)
    groups = group_response.json()['results']
    
    
    for group in groups:
        group_id = group['id']
        
        # Get hosts for each group
        host_url = f'{BASE_URL}/groups/{group_id}/hosts'
        host_response = requests.get(host_url, auth=DIGEST_AUTH)
        hosts = host_response.json()['results']
        
        all_hosts.extend(hosts)
    
    return all_hosts

def get_mongos_and_primaries(hosts: List[Dict]) -> tuple:
    """Separate mongos routers and shard primary nodes (excluding config servers)."""
    mongos_nodes = []
    primary_nodes = []
    
    for host in hosts:
        hostname = host['hostname']
        port = host['port']
        
        # Check if it's a mongos
        if 'MONGOS' in host['typeName']:
            mongos_nodes.append({
                'hostname': hostname,
                'port': port
            })
        # Check if it's a shard primary (exclude config servers)
        elif 'PRIMARY' in host['typeName'] and 'CONFIG' not in host['typeName']:
            primary_nodes.append({
                'hostname': hostname,
                'port': port
            })
    
    logger.info(f"Found {len(primary_nodes)} shard primaries (excluding config servers) and {len(mongos_nodes)} mongos routers")
    return mongos_nodes, primary_nodes

def flush_cache_on_node(node: Dict) -> bool:
    """Execute cache flush command on a specific node."""
    try:
        # Now using the new user credentials
        client_url = f'mongodb://{NEW_USER}:{NEW_USER_PASSWORD}@{node["hostname"]}:{node["port"]}/admin'
        client = MongoClient(client_url, 
                           directConnection=True,
                           connectTimeoutMS=5000, 
                           serverSelectionTimeoutMS=5000)
        
        result = client.admin.command({
            '_flushRoutingTableCacheUpdatesWithWriteConcern': NAMESPACE,
            'writeConcern': {'w': 'majority'}
        })
        
        if result.get('ok') == 1:
            logger.info(f"Successfully flushed cache on {node['hostname']}")
            return True
        else:
            logger.error(f"Failed to flush cache on {node['hostname']}: {result}")
            return False
            
    except Exception as e:
        logger.error(f"Error flushing cache on {node['hostname']}: {e}")
        return False
    finally:
        client.close()

def setup_on_primary(primary_node: Dict) -> bool:
    """Setup user and role only on primary nodes using admin credentials."""
    try:
        # Connect with admin privileges
        client_url = f'mongodb://{MONGO_ADMIN_USER}:{MONGO_ADMIN_PASSWORD}@{primary_node["hostname"]}:{primary_node["port"]}/admin'
        client = MongoClient(client_url, 
                           directConnection=True,
                           connectTimeoutMS=5000, 
                           serverSelectionTimeoutMS=5000)
        
        admin_db = client.admin

        # Verify we're on primary
        if not client.is_primary:
            logger.error(f"Node {primary_node['hostname']} is not primary, skipping setup")
            return False

        logger.info(f"Connected to primary {primary_node['hostname']}, setting up user and role...")
        
        # Create mongops user with clusterManager role
        try:
            admin_db.command('createUser', NEW_USER, 
                           pwd=NEW_USER_PASSWORD,
                           roles=[{'role': 'clusterManager', 'db': 'admin'}])
            logger.info(f"Created user {NEW_USER}")
        except Exception as e:
            if 'already exists' not in str(e):
                raise
            logger.info(f"User {NEW_USER} already exists")

        # Create the special role for flush command
        try:
            admin_db.command('createRole', 'flush_routing_table_cache_updates',
                           privileges=[{
                               'resource': {'cluster': True},
                               'actions': ['internal']
                           }],
                           roles=[])
            logger.info("Created flush_routing_table_cache_updates role")
        except Exception as e:
            if 'already exists' not in str(e):
                raise
            logger.info("Role already exists")

        # Grant the special role to mongops user
        admin_db.command('grantRolesToUser', NEW_USER, 
                        roles=['flush_routing_table_cache_updates'])
        logger.info(f"Granted flush role to user {NEW_USER}")

        return True

    except Exception as e:
        logger.error(f"Error during primary setup on {primary_node['hostname']}: {e}")
        return False
    finally:
        client.close()

def perform_findAll_on_allMongos(mongos_node: Dict, namespace: str) -> bool:
    """Run a findOne command on mongos using admin credentials."""
    try:
        # Split namespace into database and collection
        db_name, collection_name = namespace.split('.')
        
        # Use admin credentials instead of the new mongops user
        client_url = f'mongodb://{MONGO_ADMIN_USER}:{MONGO_ADMIN_PASSWORD}@{mongos_node["hostname"]}:{mongos_node["port"]}/admin'
        client = MongoClient(client_url, 
                           directConnection=True,
                           connectTimeoutMS=5000, 
                           serverSelectionTimeoutMS=5000)
        
        db = client[db_name]
        collection = db[collection_name]
        
        # Try to find one document
        doc = collection.find_one()
        
        if doc is not None:
            logger.info(f"Successfully queried one document from {namespace} via mongos {mongos_node['hostname']}")
            return True
        else:
            logger.info(f"Collection {namespace} is empty on mongos {mongos_node['hostname']}")
            return True
            
    except Exception as e:
        logger.error(f"Error querying collection via mongos {mongos_node['hostname']}: {e}")
        return False
    finally:
        client.close()

def main():
    try:
        logger.info("Fetching cluster hosts...")
        all_hosts = get_all_hosts()
        
        mongos_nodes, primary_nodes = get_mongos_and_primaries(all_hosts)
        logger.info(f"Found {len(primary_nodes)} primaries and {len(mongos_nodes)} mongos routers")

        if not primary_nodes:
            logger.error("No shard primaries found")
            return False

        if not mongos_nodes:
            logger.error("No mongos nodes found")
            return False         
        
        # First, setup user and role on ONE primary only
        setup_completed = False
        for primary in primary_nodes:
            if setup_on_primary(primary):
                setup_completed = True
                break
        
        if not setup_completed:
            logger.error("Failed to setup user and role on any primary")
            return False

        # Now use mongops user to run flush commands
        logger.info("Setup complete, proceeding with cache flush operations...")
        
        # Flush on primaries
        for primary in primary_nodes:
            if not flush_cache_on_node(primary):
                logger.error(f"Failed to flush cache on primary {primary['hostname']}")
                
        # Flush on mongos
        if mongos_nodes:
            logger.info("Performing find all on mongos to make sure routers have also flushed their cache...")
            perform_findAll_on_allMongos(mongos_nodes[0], NAMESPACE)
        
        logger.info("All operations completed")
        return True
        
    except Exception as err:
        logger.error(f"Script failed: {err}")
        return False


if __name__ == "__main__":
    main()