"""MongoDB database connection and operations module.

This module provides centralized MongoDB connection management and CRUD operations
compatible with Prefect task workflows. It uses environment variables for configuration
and follows best practices with context managers and proper error handling.

Environment Variables Required:
    MONGO_URI: MongoDB connection string (e.g., 'mongodb://localhost:27017/' or Atlas URI)
    MONGO_DATABASE: Default database name
    MONGO_COLLECTION: Default collection name

Example Usage:
    ```python
    from valerie.db import MongoDBClient, insert_document, find_documents
    from prefect import flow, task

    # Direct usage with context manager
    with MongoDBClient() as db:
        result = db.collection.insert_one({'name': 'Alice', 'age': 30})
        print(f"Inserted ID: {result.inserted_id}")

    # Using as Prefect tasks
    @flow
    def my_flow():
        doc_id = insert_document.submit({'name': 'Bob', 'age': 25}).result()
        docs = find_documents.submit({'age': {'$gt': 20}}).result()
    ```
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import DeleteResult, InsertManyResult, InsertOneResult, UpdateResult
from prefect import get_run_logger, task


class MongoDBClient:
    """MongoDB client wrapper with context manager support.

    This class provides a clean interface for MongoDB connections using
    context managers (with statements) to ensure proper resource cleanup.

    Args:
        mongo_uri: MongoDB connection string. Defaults to MONGO_URI env var.
        database_name: Database name. Defaults to MONGO_DATABASE env var.
        collection_name: Collection name. Defaults to MONGO_COLLECTION env var.
        timeout_ms: Server selection timeout in milliseconds. Defaults to 5000.

    Example:
        ```python
        with MongoDBClient() as db:
            result = db.collection.insert_one({'key': 'value'})
        ```
    """

    def __init__(
        self,
        mongo_uri: str | None = None,
        database_name: str | None = None,
        collection_name: str | None = None,
        timeout_ms: int = 5000,
    ) -> None:
        """Initialize MongoDB client configuration."""
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI")
        self.database_name = database_name or os.getenv("MONGO_DATABASE")
        self.collection_name = collection_name or os.getenv("MONGO_COLLECTION")
        self.timeout_ms = timeout_ms

        if not self.mongo_uri:
            raise ValueError("MONGO_URI must be provided or set as environment variable")
        if not self.database_name:
            raise ValueError("MONGO_DATABASE must be provided or set as environment variable")
        if not self.collection_name:
            raise ValueError("MONGO_COLLECTION must be provided or set as environment variable")

        self.client: MongoClient | None = None
        self.database: Database | None = None
        self.collection: Collection | None = None

    def __enter__(self) -> MongoDBClient:
        """Enter context manager and establish MongoDB connection."""
        self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=self.timeout_ms)
        self.database = self.client[self.database_name]
        self.collection = self.database[self.collection_name]
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close MongoDB connection."""
        if self.client:
            self.client.close()


@contextmanager
def get_mongodb_connection(
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
    timeout_ms: int = 5000,
) -> Iterator[MongoDBClient]:
    """Context manager for MongoDB connections.

    Convenience function for quick database access with automatic cleanup.

    Args:
        mongo_uri: MongoDB connection string. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.
        timeout_ms: Server selection timeout in milliseconds.

    Yields:
        MongoDBClient: Configured MongoDB client with active connection.

    Example:
        ```python
        with get_mongodb_connection() as db:
            docs = list(db.collection.find({'status': 'active'}))
        ```
    """
    with MongoDBClient(mongo_uri, database, collection, timeout_ms) as client:
        yield client


# ============================================================================
# CRUD Operations as Prefect Tasks
# ============================================================================

@task(name="Insert one document to MongoDB", retries=2, retry_delay_seconds=5)
def insert_document(
    document: dict[str, Any],
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Insert a single document into MongoDB.

    Args:
        document: The document to insert (dictionary).
        mongo_uri: MongoDB URI. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.

    Returns:
        str: The inserted document's ObjectId as a string.

    Example:
        ```python
        doc_id = insert_document({'name': 'Alice', 'age': 30})
        print(f"Inserted document ID: {doc_id}")
        ```
    """
    logger = get_run_logger()

    with get_mongodb_connection(mongo_uri, database, collection) as db:
        result: InsertOneResult = db.collection.insert_one(document)
        inserted_id = str(result.inserted_id)

    logger.info("Inserted document with ID: %s", inserted_id)
    return inserted_id


@task(name="Insert multiple documents to MongoDB", retries=2, retry_delay_seconds=5)
def insert_documents(
    documents: list[dict[str, Any]],
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> list[str]:
    """Insert multiple documents into MongoDB.

    Args:
        documents: List of documents to insert.
        mongo_uri: MongoDB URI. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.

    Returns:
        list[str]: List of inserted document ObjectIds as strings.

    Example:
        ```python
        docs = [
            {'name': 'Bob', 'age': 25},
            {'name': 'Charlie', 'age': 35}
        ]
        doc_ids = insert_documents(docs)
        print(f"Inserted {len(doc_ids)} documents")
        ```
    """
    logger = get_run_logger()

    with get_mongodb_connection(mongo_uri, database, collection) as db:
        result: InsertManyResult = db.collection.insert_many(documents)
        inserted_ids = [str(id_) for id_ in result.inserted_ids]

    logger.info("Inserted %d documents", len(inserted_ids))
    return inserted_ids


@task(name="Find one document in MongoDB")
def find_one_document(
    query: dict[str, Any],
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> dict[str, Any] | None:
    """Find a single document matching the query.

    Args:
        query: MongoDB query filter (dictionary).
        mongo_uri: MongoDB URI. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.

    Returns:
        dict | None: The matching document or None if not found.

    Example:
        ```python
        doc = find_one_document({'name': 'Alice'})
        if doc:
            print(f"Found: {doc}")
        ```
    """
    logger = get_run_logger()

    with get_mongodb_connection(mongo_uri, database, collection) as db:
        document = db.collection.find_one(query)

    if document:
        logger.info("Found document matching query")
    else:
        logger.info("No document found matching query")

    return document


@task(name="Find documents in MongoDB")
def find_documents(
    query: dict[str, Any] | None = None,
    limit: int | None = None,
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> list[dict[str, Any]]:
    """Find all documents matching the query.

    Args:
        query: MongoDB query filter. Defaults to {} (all documents).
        limit: Maximum number of documents to return. Defaults to None (no limit).
        mongo_uri: MongoDB URI. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.

    Returns:
        list[dict]: List of matching documents.

    Example:
        ```python
        # Find all documents where age > 25
        docs = find_documents({'age': {'$gt': 25}})

        # Find all documents with a limit
        docs = find_documents({}, limit=10)
        ```
    """
    logger = get_run_logger()
    query = query or {}

    with get_mongodb_connection(mongo_uri, database, collection) as db:
        cursor = db.collection.find(query)
        if limit:
            cursor = cursor.limit(limit)
        documents = list(cursor)

    logger.info("Found %d documents matching query", len(documents))
    return documents


@task(name="Update one document in MongoDB", retries=2, retry_delay_seconds=5)
def update_one_document(
    query: dict[str, Any],
    update: dict[str, Any],
    upsert: bool = False,
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> int:
    """Update a single document matching the query.

    Args:
        query: MongoDB query filter to find the document.
        update: Update operations to apply (e.g., {'$set': {'age': 31}}).
        upsert: If True, insert if no document matches. Defaults to False.
        mongo_uri: MongoDB URI. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.

    Returns:
        int: Number of documents modified.

    Example:
        ```python
        # Update Alice's age
        modified = update_one_document(
            {'name': 'Alice'},
            {'$set': {'age': 31}}
        )
        print(f"Modified {modified} document(s)")
        ```
    """
    logger = get_run_logger()

    with get_mongodb_connection(mongo_uri, database, collection) as db:
        result: UpdateResult = db.collection.update_one(query, update, upsert=upsert)
        modified_count = result.modified_count

    logger.info("Modified %d document(s)", modified_count)
    return modified_count


@task(name="Update multiple documents in MongoDB", retries=2, retry_delay_seconds=5)
def update_many_documents(
    query: dict[str, Any],
    update: dict[str, Any],
    upsert: bool = False,
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> int:
    """Update all documents matching the query.

    Args:
        query: MongoDB query filter to find documents.
        update: Update operations to apply (e.g., {'$set': {'status': 'active'}}).
        upsert: If True, insert if no documents match. Defaults to False.
        mongo_uri: MongoDB URI. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.

    Returns:
        int: Number of documents modified.

    Example:
        ```python
        # Mark all young people as 'junior'
        modified = update_many_documents(
            {'age': {'$lt': 30}},
            {'$set': {'status': 'junior'}}
        )
        print(f"Modified {modified} document(s)")
        ```
    """
    logger = get_run_logger()

    with get_mongodb_connection(mongo_uri, database, collection) as db:
        result: UpdateResult = db.collection.update_many(query, update, upsert=upsert)
        modified_count = result.modified_count

    logger.info("Modified %d document(s)", modified_count)
    return modified_count


@task(name="Delete one document from MongoDB", retries=2, retry_delay_seconds=5)
def delete_one_document(
    query: dict[str, Any],
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> int:
    """Delete a single document matching the query.

    Args:
        query: MongoDB query filter to find the document.
        mongo_uri: MongoDB URI. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.

    Returns:
        int: Number of documents deleted (0 or 1).

    Example:
        ```python
        deleted = delete_one_document({'name': 'Alice'})
        print(f"Deleted {deleted} document(s)")
        ```
    """
    logger = get_run_logger()

    with get_mongodb_connection(mongo_uri, database, collection) as db:
        result: DeleteResult = db.collection.delete_one(query)
        deleted_count = result.deleted_count

    logger.info("Deleted %d document(s)", deleted_count)
    return deleted_count


@task(name="Delete multiple documents from MongoDB", retries=2, retry_delay_seconds=5)
def delete_many_documents(
    query: dict[str, Any],
    mongo_uri: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> int:
    """Delete all documents matching the query.

    Args:
        query: MongoDB query filter to find documents.
        mongo_uri: MongoDB URI. Defaults to MONGO_URI env var.
        database: Database name. Defaults to MONGO_DATABASE env var.
        collection: Collection name. Defaults to MONGO_COLLECTION env var.

    Returns:
        int: Number of documents deleted.

    Example:
        ```python
        # Delete all documents where age > 30
        deleted = delete_many_documents({'age': {'$gt': 30}})
        print(f"Deleted {deleted} document(s)")
        ```
    """
    logger = get_run_logger()

    with get_mongodb_connection(mongo_uri, database, collection) as db:
        result: DeleteResult = db.collection.delete_many(query)
        deleted_count = result.deleted_count

    logger.info("Deleted %d document(s)", deleted_count)
    return deleted_count


# ============================================================================
# Example Usage (for reference)
# ============================================================================

"""
EXAMPLE 1: Direct usage with context manager
-----------------------------------------------
from valerie.db import MongoDBClient

with MongoDBClient() as db:
    # Insert documents
    result = db.collection.insert_one({'name': 'Alice', 'age': 30})
    print(f"Inserted ID: {result.inserted_id}")

    # Find documents
    for doc in db.collection.find({'age': {'$gt': 25}}):
        print(doc)

    # Update documents
    db.collection.update_one(
        {'name': 'Alice'},
        {'$set': {'age': 31}}
    )

    # Delete documents
    db.collection.delete_one({'name': 'Alice'})


EXAMPLE 2: Using as Prefect tasks in a flow
---------------------------------------------
from prefect import flow
from valerie.db import (
    insert_document,
    insert_documents,
    find_documents,
    update_one_document,
    delete_one_document,
)

@flow(name="MongoDB operations flow")
def my_database_flow():
    # Insert single document
    doc_id = insert_document.submit({'name': 'Alice', 'age': 30}).result()
    print(f"Inserted document: {doc_id}")

    # Insert multiple documents
    docs = [
        {'name': 'Bob', 'age': 25},
        {'name': 'Charlie', 'age': 35}
    ]
    doc_ids = insert_documents.submit(docs).result()
    print(f"Inserted {len(doc_ids)} documents")

    # Find documents
    all_docs = find_documents.submit({'age': {'$gt': 20}}).result()
    print(f"Found {len(all_docs)} documents")

    # Update a document
    modified = update_one_document.submit(
        {'name': 'Alice'},
        {'$set': {'age': 31}}
    ).result()
    print(f"Modified {modified} document(s)")

    # Delete a document
    deleted = delete_one_document.submit({'name': 'Alice'}).result()
    print(f"Deleted {deleted} document(s)")

if __name__ == '__main__':
    my_database_flow()


EXAMPLE 3: Using with custom connection parameters
----------------------------------------------------
from valerie.db import insert_document

# Use custom database and collection
doc_id = insert_document(
    document={'key': 'value'},
    mongo_uri='mongodb://localhost:27017/',
    database='my_custom_db',
    collection='my_custom_collection'
)


EXAMPLE 4: MongoDB Atlas (cloud) connection
---------------------------------------------
import os
from valerie.db import MongoDBClient

# Set environment variables for Atlas
os.environ['MONGO_URI'] = 'mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority'
os.environ['MONGO_DATABASE'] = 'production_db'
os.environ['MONGO_COLLECTION'] = 'users'

with MongoDBClient() as db:
    # Your operations here
    users = list(db.collection.find({'status': 'active'}))
"""
