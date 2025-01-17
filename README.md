---

# Automating Incremental Refresh on Primary Nodes in a Sharded MongoDB Cluster

This repository contains scripts to automate the process of running incremental refresh on primary nodes of all the shards in a sharded MongoDB cluster. The following steps will guide you through setting up your environment and preparing to run the scripts.

## Scripts Overview

- **Test Script**: `test-env.py`
- **Main Script**: `mongo-cache-flush.py`

## Prerequisites

- Python 3.x installed on your system.
- User with `userAdmin` privileges for running the main script.
- User with `clusterMonitor` privileges for running the test script.
- An API key for MongoDB Cloud Manager.
- Your public IP address added to the Cloud Manager API key for secure access.

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Create a Virtual Environment**:
   Create a Python virtual environment to manage dependencies:
   ```bash
   python3 -m venv venv
   ```

3. **Activate the Virtual Environment**:
   Activate the virtual environment with the following command:
   ```bash
   source venv/bin/activate
   ```

4. **Uninstall Existing `urllib3` Version**:(step for amazon linux2)
   If you have a different version of `urllib3` installed, uninstall it:
   ```bash
   pip3 uninstall urllib3
   ```

5. **Install Specific `urllib3` Version**:
   Install the required version of `urllib3`:
   ```bash
   pip3 install urllib3==1.26.6
   ```

6. **Install Dependencies**:
   Install the necessary dependencies listed in the `requirements.txt` file:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

### API Key for Cloud Manager

1. **Create an API Key**:
   - Log in to your MongoDB Cloud Manager account.
   - Navigate to the API Access section and generate an API key.

2. **Add Public IP to API Key**:
   - Ensure your public IP address is added to the API key settings to allow secure access.

3. **Define these env variables with appropriate values** 
   ```bash
   export PUBLIC_KEY=''
   export PRIVATE_KEY=''
   export PROJECT_ID=''
   export CLUSTER_ID=''
   ```

## Running the Scripts

- **Main Script**: Run `mongo-cache-flush.py` using a user with `userAdmin` privileges.
- **Test Script**: Run `test-env.py` using a user with `clusterMonitor` privileges.

Ensure you follow the above steps and configurations to successfully execute the scripts.

## Notes

- It's recommended to deactivate the virtual environment when done:
  ```bash
  deactivate
  ```