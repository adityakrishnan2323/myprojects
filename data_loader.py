import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import time
import os


class DataLoader:

    def __init__(self, uri, user, password):
        """
        Connect to the Neo4j database and other init steps
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.driver.verify_connectivity()
        self.create_constraints()

    def close(self):
        """
        Close the connection to the Neo4j database
        """
        self.driver.close()

    def create_constraints(self):
        """
        Create uniqueness constraints for nodes
        """
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run("""
                CREATE CONSTRAINT IF NOT EXISTS FOR (p:PickupLocation) REQUIRE p.id IS UNIQUE;
            """))
            session.execute_write(lambda tx: tx.run("""
                CREATE CONSTRAINT IF NOT EXISTS FOR (d:DropoffLocation) REQUIRE d.id IS UNIQUE;
            """))
            session.execute_write(lambda tx: tx.run("""
                CREATE CONSTRAINT IF NOT EXISTS FOR (t:Trip) REQUIRE t.pickup_datetime IS UNIQUE;
            """))

    def delete_existing_data(self):
        """
        Delete existing data from the Neo4j database
        """
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))

    def load_transform_file(self, file_path):
        """
        Load the parquet file, transform it into a CSV file, and then load it into Neo4j.
        """
        trips = pq.read_table(file_path).to_pandas()
        trips = trips[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']]

        # Filter Bronx locations
        bronx = [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259]
        trips = trips[trips['PULocationID'].isin(bronx) & trips['DOLocationID'].isin(bronx)]
        trips = trips[trips['trip_distance'] > 0.1]
        trips = trips[trips['fare_amount'] > 2.5]

        trips['tpep_pickup_datetime'] = trips['tpep_pickup_datetime'].astype(str)
        trips['tpep_dropoff_datetime'] = trips['tpep_dropoff_datetime'].astype(str)

        # Construct correct save location
        neo4j_import_dir = r"C:\Users\My PC\.Neo4jDesktop\relate-data\dbmss\dbms-6d74b52b-117b-4ad4-b766-e235cfc2a1b2\import"
        os.makedirs(neo4j_import_dir, exist_ok=True)  # Ensure directory exists
        csv_filename = os.path.basename(file_path).replace('.parquet', '.csv')
        save_loc = os.path.join(neo4j_import_dir, csv_filename)

        trips.to_csv(save_loc, index=False)
        print(f"CSV file saved at {save_loc}. Now loading into Neo4j...")

        self.delete_existing_data()
        with self.driver.session() as session:
            session.execute_write(self.insert_data, csv_filename)
        print("Data successfully loaded into Neo4j.")

    @staticmethod
    def insert_data(tx, file_name):
        """
        Load data from CSV into Neo4j and create nodes and relationships.
        """
        query = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{file_name}' AS row
        MERGE (p:PickupLocation {{id: toInteger(row.PULocationID)}})
        MERGE (d:DropoffLocation {{id: toInteger(row.DOLocationID)}})
        CREATE (t:Trip {{
            pickup_datetime: row.tpep_pickup_datetime,
            dropoff_datetime: row.tpep_dropoff_datetime,
            trip_distance: toFloat(row.trip_distance),
            fare_amount: toFloat(row.fare_amount)
        }})
        MERGE (p)-[:STARTS_TRIP]->(t)
        MERGE (t)-[:ENDS_TRIP]->(d)
        """
        tx.run(query)


def main():
    total_attempts = 10
    attempt = 0

    while attempt < total_attempts:
        try:
            data_loader = DataLoader("neo4j://localhost:7687", "neo4j", "Aditya@2002")
            data_loader.load_transform_file(r"D:\ASU-Data Processing at Scale\Project Phase-1\yellow_tripdata_2023-03.parquet")
            data_loader.close()
            attempt = total_attempts
        except Exception as e:
            print(f"(Attempt {attempt+1}/{total_attempts}) Error: ", e)
            attempt += 1
            time.sleep(10)


if __name__ == "__main__":
    main()
