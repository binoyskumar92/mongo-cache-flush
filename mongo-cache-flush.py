from getpass import getpass
import time
from requests.auth import HTTPDigestAuth
import requests
from pymongo import MongoClient
import logging
from typing import List, Dict
import json
import os

# Configuration
PUBLIC_KEY = os.environ.get('PUBLIC_KEY')
PRIVATE_KEY = os.environ.get('PRIVATE_KEY')
PROJECT_ID = os.environ.get('PROJECT_ID')
CLUSTER_ID = os.environ.get('CLUSTER_ID')

# MongoDB user config
MONGO_ADMIN_USER = 'binoymdb'  # Your admin username
MONGO_ADMIN_PASSWORD = getpass("Enter admin user password: ")
NEW_USER = 'mongops'
NEW_USER_PASSWORD = getpass("Enter flush user password: ")
NAMESPACE='fortnite-service-prod11.profile_v2'

# API Setup
BASE_URL = 'https://cloud.mongodb.com/api/public/v1.0'
DIGEST_AUTH = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_all_hosts() -> List[Dict]:
    """Get MongoDB hosts from the specified project and cluster with pagination."""
    try:
        logger.info("Fetching all the hosts...")
        all_hosts = []
        page_num = 1
        items_per_page = 200
        
        while True:
            host_url = f'{BASE_URL}/groups/{PROJECT_ID}/hosts?clusterId={CLUSTER_ID}&pageNum={page_num}&itemsPerPage={items_per_page}'
            host_response = requests.get(host_url, auth=DIGEST_AUTH)
            host_response.raise_for_status()
            
            response_data = host_response.json()
            hosts = response_data['results']
            all_hosts.extend(hosts)
            
            total_count = response_data.get('totalCount', 0)
            logger.info(f"Fetched page {page_num}, got {len(hosts)} hosts (Total: {len(all_hosts)}/{total_count})")
            
            if len(all_hosts) >= total_count:
                break
                
            page_num += 1
            
        logger.info(f"Found total of {len(all_hosts)} hosts in project")
        return all_hosts

    except requests.exceptions.RequestException as e:
        logger.error(f"API connection error: {e}")
        return []

def get_cluster_topology(hosts: List[Dict]) -> tuple:
    """Extract detailed cluster topology including shard names and their primaries."""
    mongos_nodes = []
    shard_primaries = {}  # Dictionary to store shard name -> primary node mapping
    
    for host in hosts:
        hostname = host['hostname']
        port = host['port']
        
        # Handle mongos routers
        if 'MONGOS' in host['typeName']:
            mongos_nodes.append({
                'hostname': hostname,
                'port': port
            })
        # Handle shard primaries (excluding config servers)
        elif 'PRIMARY' in host['typeName'] and 'CONFIG' not in host['typeName']:
            # Extract shard name from the hostname or replicaSetName if available
            shard_name = host.get('replicaSetName', hostname.split('-')[0])
            shard_primaries[shard_name] = {
                'hostname': hostname,
                'port': port
            }
    
    return mongos_nodes, shard_primaries

def save_topology_info(mongos_nodes: List[Dict], shard_primaries: Dict):
    """Save cluster topology information to a file."""
    topology = {
        'mongos_routers': mongos_nodes,
        'shard_primaries': shard_primaries
    }
    
    # Write the file and set permissions to 640
    with open('cluster_topology.json', 'w') as f:
        json.dump(topology, f, indent=2)
    os.chmod('cluster_topology.json', 0o640)
    
    logger.info("Cluster topology has been saved to cluster_topology.json")

def display_topology(mongos_nodes: List[Dict], shard_primaries: Dict):
    """Display cluster topology in a readable format."""
    print("\n=== Cluster Topology ===")
    print("\nMongos Routers:")
    for idx, mongos in enumerate(mongos_nodes, 1):
        print(f"{idx}. {mongos['hostname']}:{mongos['port']}")
    
    print("\nShard Primaries:")
    for shard_name, primary in shard_primaries.items():
        print(f"Shard: {shard_name}")
        print(f"Primary: {primary['hostname']}:{primary['port']}")
    
    print("\nThis topology has been saved to 'cluster_topology.json'")

def wait_for_confirmation():
    """Wait for user confirmation to proceed."""
    while True:
        response = input("\nPress 'C' to continue with setup operations or 'Q' to quit: ").upper()
        if response == 'C':
            return True
        elif response == 'Q':
            return False
        else:
            print("Invalid input. Please press 'C' to continue or 'Q' to quit.")

def perform_findAll_on_allMongos(mongos_nodes: List[Dict], namespace: str) -> bool:
    """Run a findOne command on all mongos nodes using admin credentials."""
    # Split namespace into database and collection
    db_name, collection_name = namespace.split('.')
    
    for mongos_node in mongos_nodes:
        try:
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
            else:
                logger.info(f"Collection {namespace} is empty on mongos {mongos_node['hostname']}")
                
        except Exception as e:
            logger.error(f"Error querying collection via mongos {mongos_node['hostname']}: {e}")
            return False
        finally:
            if client:
                client.close()
    
    return True

def process_shard(shard_name: str, primary: Dict) -> bool:
    """Process all operations for a shard using a single admin client connection."""
    admin_client = None
    try:
        # Create single admin client for all operations
        client_url = f'mongodb://{MONGO_ADMIN_USER}:{MONGO_ADMIN_PASSWORD}@{primary["hostname"]}:{primary["port"]}/admin'
        admin_client = MongoClient(client_url, 
                                 directConnection=True,
                                 connectTimeoutMS=5000, 
                                 serverSelectionTimeoutMS=5000)
        
        admin_db = admin_client.admin

        # Verify we're on primary
        if not admin_client.is_primary:
            logger.error(f"Node {primary['hostname']} is not primary, skipping")
            return False

        # 1. Get pre-flush metrics
        status = admin_db.command('serverStatus')
        before_metrics = status.get('metrics', {}).get('commands', {}).get('_flushRoutingTableCacheUpdatesWithWriteConcern', {})
        logger.info(f"Pre-flush metrics on {primary['hostname']}: {before_metrics}")

        # 2. Setup user and role
        logger.info(f"Setting up user and role on {primary['hostname']}...")
        try:
            # Create user
            admin_db.command('createUser', NEW_USER, 
                           pwd=NEW_USER_PASSWORD,
                           roles=[{'role': 'clusterManager', 'db': 'admin'}])
            logger.info(f"Created user {NEW_USER}")
        except Exception as e:
            if 'already exists' not in str(e):
                raise
            logger.info(f"User {NEW_USER} already exists")

        try:
            # Create role
            admin_db.command('createRole', 'flush_routing_table_cache_updates',
                           privileges=[{
                               'resource': {'cluster': True},
                               'actions': ['internal']
                           }],
                           roles=[])
            logger.info(f"Created role flush_routing_table_cache_updates")
        except Exception as e:
            if 'already exists' not in str(e):
                raise
            logger.info(f"Role already exists")

        # Grant role
        admin_db.command('grantRolesToUser', NEW_USER, 
                        roles=['flush_routing_table_cache_updates'])
        logger.info(f"Granted role to user {NEW_USER}")

        # 3. Perform flush using new user
        flush_client = None
        try:
            flush_client = MongoClient(f'mongodb://{NEW_USER}:{NEW_USER_PASSWORD}@{primary["hostname"]}:{primary["port"]}/admin',
                                     directConnection=True,
                                     connectTimeoutMS=5000,
                                     serverSelectionTimeoutMS=5000)
            
            result = flush_client.admin.command({
                '_flushRoutingTableCacheUpdatesWithWriteConcern': NAMESPACE,
                'writeConcern': {'w': 'majority'}
            })
        finally:
            if flush_client:
                flush_client.close()

        # Small delay to ensure metrics are updated
        time.sleep(0.2)

        # 4. Verify flush success using metrics
        status = admin_db.command('serverStatus')
        after_metrics = status.get('metrics', {}).get('commands', {}).get('_flushRoutingTableCacheUpdatesWithWriteConcern', {})
        logger.info(f"Post-flush metrics on {primary['hostname']}: {after_metrics}")

        total_increase = int(after_metrics.get('total', 0)) - int(before_metrics.get('total', 0))
        failed_increase = int(after_metrics.get('failed', 0)) - int(before_metrics.get('failed', 0))

        if result.get('ok') != 1 or total_increase == 0 or failed_increase > 0:
            logger.error(f"Flush verification failed on {primary['hostname']}")
            return False

        # 5. Cleanup
        admin_db.command('revokeRolesFromUser', NEW_USER,
                        roles=['flush_routing_table_cache_updates'])
        admin_db.command('dropUser', NEW_USER)
        admin_db.command('dropRole', 'flush_routing_table_cache_updates')
        logger.info(f"Cleaned up user and role on {primary['hostname']}")

        return True

    except Exception as e:
        logger.error(f"Error processing shard {shard_name} on {primary['hostname']}: {e}")
        return False
    finally:
        if admin_client:
            admin_client.close()

def main():
    try:
        logger.info("Fetching cluster hosts...")
        all_hosts = get_all_hosts()
        
        # Get and display cluster topology
        mongos_nodes, shard_primaries = get_cluster_topology(all_hosts)
        
        if not shard_primaries:
            logger.error("No shard primaries found")
            return False

        if not mongos_nodes:
            logger.error("No mongos nodes found")
            return False
        
        # Save and display topology
        save_topology_info(mongos_nodes, shard_primaries)
        display_topology(mongos_nodes, shard_primaries)
        
        # Wait for user confirmation
        if not wait_for_confirmation():
            logger.info("Operation cancelled by user")
            return False
        
        # Setup tracking variables
        total_operations = len(shard_primaries) + len(mongos_nodes)
        shard_successes = 0
        mongos_successes = 0

        # Process each shard
        for idx, (shard_name, primary) in enumerate(shard_primaries.items(), 1):
            logger.info(f"Processing shard: {shard_name} ({idx}/{len(shard_primaries)})")
            
            if process_shard(shard_name, primary):
                shard_successes += 1
            
            time.sleep(0.2)
            
            if idx % 10 == 0:
                completion_rate = (idx / len(shard_primaries)) * 100
                logger.info(f"Progress: {completion_rate:.1f}% ({idx}/{len(shard_primaries)} shards)")

        # Verify mongos nodes
        if mongos_nodes:
            logger.info("Verifying mongos nodes...")
            for idx, mongos in enumerate(mongos_nodes, 1):
                if perform_findAll_on_allMongos([mongos], NAMESPACE):
                    mongos_successes += 1
                time.sleep(0.2)
                
                if idx % 10 == 0:
                    logger.info(f"Completed mongos verification: {idx}/{len(mongos_nodes)}")
        
        # Calculate totals
        successful_operations = shard_successes + mongos_successes
        failed_operations = total_operations - successful_operations
        
        # Display final summary
        print("\n=== Operation Summary ===")
        print(f"Total operations attempted: {total_operations}")
        print(f"Successful operations: {successful_operations}")
        print(f"Failed operations: {failed_operations}")
        print(f"Success rate: {(successful_operations/total_operations)*100:.2f}%")
        print("\nBreakdown:")
        print(f"- Shard operations (setup + flush): {shard_successes}/{len(shard_primaries)} successful")
        print(f"- Mongos verify: {mongos_successes}/{len(mongos_nodes)} successful\n")
        
        logger.info("All operations completed")
        return successful_operations > 0
        
    except Exception as err:
        logger.error(f"Script failed: {err}")
        return False

if __name__ == "__main__":
    main()