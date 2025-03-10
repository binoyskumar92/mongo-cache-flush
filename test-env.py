import os
from requests.auth import HTTPDigestAuth
import requests
from pymongo import MongoClient
import logging
from typing import List, Dict, Tuple
from getpass import getpass
import time

# Configuration
PUBLIC_KEY = os.environ.get('PUBLIC_KEY')
PRIVATE_KEY = os.environ.get('PRIVATE_KEY')
PROJECT_ID = os.environ.get('PROJECT_ID')
CLUSTER_ID = os.environ.get('CLUSTER_ID')

# MongoDB admin credentials only
MONGO_ADMIN_USER = 'binoymdb'
MONGO_ADMIN_PASSWORD = getpass("Enter MongoDB admin password: ")

# API Setup
BASE_URL = 'https://cloud.mongodb.com/api/public/v1.0'
DIGEST_AUTH = HTTPDigestAuth(PUBLIC_KEY, PRIVATE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_node_connectivity(node: Dict) -> bool:
    """Test connectivity to a MongoDB node and get server status."""
    client = None
    try:
        client_url = f'mongodb://{MONGO_ADMIN_USER}:{MONGO_ADMIN_PASSWORD}@{node["hostname"]}:{node["port"]}/admin'
        client = MongoClient(client_url, 
                           directConnection=True,
                           connectTimeoutMS=10000,           # Increased timeout
                           serverSelectionTimeoutMS=10000,   # Increased timeout
                           maxPoolSize=1,                    # Limit connection pool
                           minPoolSize=0)                    # Don't keep connections
        
        # Get basic server status without full details
        server_status = client.admin.command('serverStatus', {'recordStats': 0, 'metrics': 0})
        
        # Log minimal information
        logger.info(f"Connected to {node['hostname']}:{node['port']} - version: {server_status['version']}")
        return True

    except Exception as e:
        logger.error(f"Failed to connect to {node['hostname']}:{node['port']}: {str(e)[:100]}...")  # Truncate long error messages
        return False
    finally:
        if client:
            client.close()

def get_all_hosts() -> List[Dict]:
    """Get MongoDB hosts from the specified project and cluster with pagination."""
    try:
        logger.info("Testing MongoDB Atlas API connectivity...")
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
    """Extract cluster topology including mongos and shard primaries."""
    mongos_nodes = []
    shard_primaries = {}
    config_servers = []
    
    for host in hosts:
        hostname = host['hostname']
        port = host['port']
        type_name = host['typeName']
        
        node_info = {
            'hostname': hostname,
            'port': port,
            'type': type_name
        }
        
        if 'MONGOS' in type_name:
            mongos_nodes.append(node_info)
        elif 'PRIMARY' in type_name:
            if 'CONFIG' in type_name:
                config_servers.append(node_info)
            else:
                shard_name = host.get('replicaSetName', hostname.split('-')[0])
                shard_primaries[shard_name] = node_info
    
    logger.info(f"After filtering:")
    logger.info(f"  Mongos nodes: {len(mongos_nodes)}")
    logger.info(f"  Shard primaries: {len(shard_primaries)}")

    return mongos_nodes, shard_primaries, config_servers

def display_topology(mongos_nodes: List[Dict], shard_primaries: Dict, config_servers: List[Dict]) -> Tuple[int, int]:
    """Display cluster topology and connection test results. Returns (success_count, failure_count)."""
    success_count = 0
    failure_count = 0
    
    print(f"\n=== Testing Cluster with {len(mongos_nodes)} mongos, {len(shard_primaries)} shards ===")
    
    # Get user confirmation before proceeding
    input(f"\nAbout to test {len(mongos_nodes)} mongos and {len(shard_primaries)} shard primaries. Press Enter to continue...")
    
    print("\nTesting Mongos Routers...")
    for idx, mongos in enumerate(mongos_nodes, 1):
        if test_node_connectivity(mongos):
            success_count += 1
        else:
            failure_count += 1
        time.sleep(0.5)  # Add delay between tests
        
        # Progress indicator every 10 nodes
        if idx % 10 == 0:
            print(f"Completed testing {idx}/{len(mongos_nodes)} mongos nodes")
    
    print("\nTesting Shard Primaries...")
    total_shards = len(shard_primaries)
    for idx, (shard_name, primary) in enumerate(shard_primaries.items(), 1):
        if test_node_connectivity(primary):
            success_count += 1
        else:
            failure_count += 1
        time.sleep(0.2)  # Add delay between tests
        
        # Progress indicator every 10 shards
        if idx % 10 == 0:
            print(f"Completed testing {idx}/{total_shards} shard primaries")
    
    return success_count, failure_count

def main():
    try:
        logger.info("Starting cluster connectivity test...")
        
        # Get all hosts from Atlas API
        all_hosts = get_all_hosts()
        if not all_hosts:
            logger.error("Failed to fetch hosts from Atlas API")
            return False
        
        # Get cluster topology
        mongos_nodes, shard_primaries, config_servers = get_cluster_topology(all_hosts)
        
        if not shard_primaries:
            logger.error("No shard primaries found")
            return False

        if not mongos_nodes:
            logger.error("No mongos nodes found")
            return False
        
        # Display topology and test connections
        success_count, failure_count = display_topology(mongos_nodes, shard_primaries, config_servers)
        
        # Display final summary
        total_nodes = len(mongos_nodes) + len(shard_primaries)
        print("\n=== Test Summary ===")
        print(f"Total nodes tested: {total_nodes}")
        print(f"Successful connections: {success_count}")
        print(f"Failed connections: {failure_count}")
        print(f"Success rate: {(success_count/total_nodes)*100:.2f}%")
        
        logger.info("Connectivity test completed")
        return True
        
    except Exception as err:
        logger.error(f"Script failed: {err}")
        return False

if __name__ == "__main__":
    main()