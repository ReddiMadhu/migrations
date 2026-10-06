"""Azure Cosmos DB setup script for development and CI environments.

Idempotently provisions the database and required containers:
- Database: sas2pyspark (or AZURE_COSMOS_DATABASE)
- Container: langgraph_checkpoints (PK: /thread_id)
- Container: sessions (PK: /session_id)
"""

from __future__ import annotations

import os
import sys

from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError


def setup_cosmos() -> None:
    endpoint = os.environ.get("AZURE_COSMOS_ENDPOINT")
    key = os.environ.get("AZURE_COSMOS_KEY")
    database_name = os.environ.get("AZURE_COSMOS_DATABASE", "sas2pyspark")

    if not endpoint or not key:
        print("ERROR: AZURE_COSMOS_ENDPOINT and AZURE_COSMOS_KEY must be set.", file=sys.stderr)
        sys.exit(1)

    print(f"Connecting to Cosmos DB endpoint: {endpoint}")
    client = CosmosClient(url=endpoint, credential=key)

    try:
        database = client.create_database_if_not_exists(id=database_name)
        print(f"Database verified/created: {database_name}")

        checkpoints_container = database.create_container_if_not_exists(
            id="langgraph_checkpoints",
            partition_key=PartitionKey(path="/thread_id"),
        )
        print("Container verified/created: langgraph_checkpoints (PK: /thread_id)")

        sessions_container = database.create_container_if_not_exists(
            id="sessions",
            partition_key=PartitionKey(path="/session_id"),
        )
        print("Container verified/created: sessions (PK: /session_id)")

        print("Cosmos DB provisioning completed successfully.")
    except CosmosHttpResponseError as exc:
        print(f"Cosmos DB provisioning failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    setup_cosmos()
