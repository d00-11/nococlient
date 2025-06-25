"""
NocoDBClient Demo Harness
A modular, feature-flagged demonstration harness for NocoDBClient.
"""

import logging
import os
import uuid
from pathlib import Path
from typing import Callable

# Assuming NocoDBClient is in the same directory or installed
try:
    from nococlient import NocoDBClient
except ImportError:
    raise ImportError("NocoDBClient not found. Make sure nococlient.py is in the same directory or installed.")

# Attempt to import dotenv for environment variable loading
try:
    from dotenv import load_dotenv
except ImportError:
    # Define a simple fallback if dotenv is not installed
    def load_dotenv(dotenv_path=None):
        """Simple dotenv fallback that does nothing but log a warning."""
        logging.warning("python-dotenv not installed. Using environment variables directly.")


class DemoRunner:
    """
    Demonstration harness for NocoDBClient with feature flags to control which demos are run.
    """

    def __init__(self):
        """
        Initialize the demo runner with a dictionary of enabled demos.
        """
        self.enabled = {}
        self.client = None

        # Demo data for reuse across methods
        self.demo_base_name = "DemoBase"
        self.demo_base_id = None

    def __enter__(self):
        # Initialize client when entering context
        self._setup_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Close client when exiting context
        if hasattr(self.client, '__exit__'):
            self.client.__exit__(exc_type, exc_val, exc_tb)

    def _setup_client(self):
        """
        Set up the NocoDBClient with proper logging and environment variables.
        """
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Load environment variables
        load_dotenv()

        # Verify required environment variables are present
        required_vars = ['NOCODB_BASE_URL', 'NOCODB_API_KEY']
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            logging.info("Please create a .env file with NOCODB_BASE_URL and NOCODB_API_KEY")
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

        # Create client
        client_instance = NocoDBClient(
        )

        # Enter context
        self.client = client_instance.__enter__()
        logging.info("NocoDBClient initialized")

    def _demo_methods(self) -> dict[str, Callable]:
        """
        Maps feature names to corresponding demo methods.

        Returns:
            Dictionary mapping feature names to demo method references
        """
        return {
            "connection": self.demo_validate_connection,
            "bases": self.demo_list_and_create_base,
            "tables": self.demo_table_crud,
            "columns": self.demo_column_crud,
            "records": self.demo_record_crud,
            "schema": self.demo_schema_fetch,
            "upload": self.demo_file_upload,
            "uuid": self.demo_uuid_as_pk,
            "clear_cache": self.demo_clear_cache
        }

    def run(self, demos=["connection"]) -> None:
        """
        Run the specified demos, printing headers and catching exceptions.

        Args:
            demos: List of demo names to run, or ["all"] to run all demos
        """
        logging.info("Starting NocoDBClient Demo Runner")

        demo_methods = self._demo_methods()
        all_demos = list(demo_methods.keys())

        # Build the enabled dictionary
        if "all" in demos:
            self.enabled = {demo: True for demo in all_demos}
        else:
            self.enabled = {demo: demo in demos for demo in all_demos}

        # Print summary of what will be run
        print("NocoDBClient Demo Harness")
        print("------------------------")
        print("Enabled demos:")
        for demo, is_enabled in self.enabled.items():
            status = "✅" if is_enabled else "❌"
            print(f"  {status} {demo}")
        print("------------------------")

        for feature_name, method in demo_methods.items():
            if self.enabled.get(feature_name, False):
                header = f"===== DEMO: {feature_name.upper()} ====="
                logging.info(header)

                try:
                    method()
                    logging.info(f"{'-' * len(header)}")
                except Exception as e:
                    logging.exception(f"Error in {feature_name} demo: {str(e)}")
                    logging.info(f"{'-' * len(header)}")

        logging.info("Demo Runner completed")

    def get_table_dict(self) -> dict:

        tables = {}
        try:
            table_list = self.client.list_tables(self.demo_base_id)
            tables = {item["table_name"]: item["id"] for item in table_list}
            if len(tables) > 0:
                logging.info(f"✅ {len(tables)} tables were found: {', '.join(tables)}")
            else:
                logging.warning(f"⚠️ No Tables where found")
        except Exception as e:
            logging.error(f"❌ Failed to list all Tables: {e}")

        return tables

    def test_column_creation(self, tables_dict, table, payload)-> None:

        logging.info(f"Adding {payload['title']} column with uidt {payload['uidt']} to the {table} table...")

        try:
            result = self.client.create_column(tables_dict[table], payload)
            column_id = result.get('id')
            logging.info(f"✅ Created {payload['title']} column in Posts table with ID: {column_id}")
        except Exception as e:
            # Extract the full response if available
            if hasattr(e, 'response'):
                response_content = e.response.text if hasattr(e.response, 'text') else str(e.response)
                logging.error(f"❌ Failed to create {payload['title']} column: {response_content}")
            else:
                logging.error(f"❌ Failed to create {payload['title']} column: {e}")

    def demo_validate_connection(self) -> None:
        """
        Validate connection to NocoDB API.
        """
        logging.info("Validating connection to NocoDB API...")
        result = self.client.validate_connection()

        if result:
            logging.info("✅ Connection validation successful!")
        else:
            logging.error("❌ Connection validation failed!")

    def demo_list_and_create_base(self) -> None:
        """
        List bases and create a demo base if it doesn't exist.
        """
        # List existing bases
        logging.info("Listing existing bases...")
        bases = self.client.list_bases()

        if bases:
            base_names = [base.get('title', 'Unknown') for base in bases]
            logging.info(f"Found {len(bases)} existing bases: {', '.join(base_names)}")
        else:
            logging.info("No existing bases found or could not retrieve bases")

        # Get or create the demo base
        logging.info(f"Looking for demo base '{self.demo_base_name}'...")
        self.demo_base_id = self.client.get_base_id(self.demo_base_name)

        if self.demo_base_id:
            logging.info(f"✅ Found existing demo base with ID: {self.demo_base_id}")
        else:
            logging.info(f"Creating demo base '{self.demo_base_name}'...")
            result = self.client.create_base(self.demo_base_name, prevent_duplicates=True)

            if result:
                self.demo_base_id = result.get('id')
                logging.info(f"✅ Created demo base with ID: {self.demo_base_id}")
            else:
                logging.error("❌ Failed to create demo base")

    def test_table_creation(self, payload: dict) -> list:

        result = []
        logging.info(f"Creating or checking '{payload['title']}' table...")
        try:
            result = self.client.create_table(self.demo_base_id, payload)
            table_id = result.get('id')
            table_name = result.get('table_name')
            logging.info(f"✅ Table '{table_name}' created or already exists with ID: {table_id}")
        except Exception as e:
            logging.error(f"❌ Failed to create table '{payload['title']}': {e}")

        return result

    def demo_table_crud(self) -> None:
        """
        Perform CRUD operations on tables.
        """

        # Check if the DemoBase exists
        if not self.demo_base_id:
            logging.error("❌ Demo base ID not set. Please run demo_list_and_create_base first.")
        # List existing tables
        logging.info(f"Listing tables in base '{self.demo_base_name}'...")
        self.get_table_dict()

        demo_tables = {
            "Users":{
                    "title": "Users",
                    "columns": [
                        {"column_name": "Id", "title": "Id", "uidt": "ID"},
                        {"column_name": "name", "title": "Name", "uidt": "SingleLineText"},
                        {"column_name": "email", "title": "Email", "uidt": "Email"}
                    ]
            },
            "Posts":{
                    "title": "Posts",
                    "columns": [
                        {"column_name": "Id", "title": "Id", "uidt": "ID"},
                        {"column_name": "title", "title": "Title", "uidt": "SingleLineText"},
                        {"column_name": "content", "title": "Content", "uidt": "LongText"}
                    ]
            },
            "Delete":{
                    "title": "Delete",
                    "columns": [
                        {"column_name": "Id", "title": "Id", "uidt": "ID"},
                        {"column_name": "name", "title": "Name", "uidt": "SingleLineText"},
                    ]
            }
        }
        # Create demo tables
        self.test_table_creation(demo_tables["Users"])
        self.test_table_creation(demo_tables["Posts"])
        self.test_table_creation(demo_tables["Delete"])


        # List tables again to verify
        logging.info("Listing tables after creation...")
        self.get_table_dict()

        # Delete tables
        logging.info("Deleting table after creation...")
        try:
            del_table_id = self.client.get_table_id(self.demo_base_id, "Delete")
            del_msg = self.client.delete_table(table_id=del_table_id, require_confirmation=False)
            if del_msg:
                logging.info(f"✅ Tables 'Delete' has been deleted")
            else:
                logging.warning("⚠️ No Table 'Delete' found!")
        except Exception as e:
            logging.warning(f"❌ Table 'Delete' was not deleted: {e}")

    def demo_column_crud(self) -> None:
        """
        Perform CRUD operations on columns, including creating a linking column between tables.
        """
        # Check if the DemoBase exists
        if not self.demo_base_id:
            logging.error("❌ Demo base ID not set. Please run demo_list_and_create_base first.")

        tables = self.get_table_dict()
        if tables:
            # Column creation payload for a LinkToAnotherRecord type
            link_column_payload = {
                "title": "Author",
                "uidt": "Links",
                "parentId": tables["Posts"],
                "childId": tables["Users"],
                "type": "mm"  # many-to-many relationship
            }
            self.test_column_creation(tables_dict=tables, table="Posts", payload=link_column_payload)

            age_column_payload = {
                "title": "Age",
                "column_name": "age",
                "uidt": "Number",
            }
            self.test_column_creation(tables_dict=tables, table="Users", payload=age_column_payload)

            uploads_column_payload= {
                "title": "Uploads",
                "column_name": "uploads",
                "uidt": "Attachment",
            }
            self.test_column_creation(tables_dict=tables, table="Users", payload=uploads_column_payload)

        else:
            logging.error(f"❌ tables variable is empty")

    def test_record_upload(self, tables: dict, table: str, payload: list) -> list:

        logging.info(f"Uploading records to table {table} with id: {tables[table]} ")
        result = []
        try:
            result = self.client.create_records(tables[table], payload)
            if result:
                logging.info(f"✅ Created {len(result)} records in table {table} ")
            else:
                logging.error(f"❌ Failed to create records in table {table} ")
                return result
        except Exception as e:
            logging.error(f"❌ Exception while creating records: {e}")
            return result

    def demo_record_crud(self) -> None:
        """
        Perform CRUD operations on records.
        """

        if not self.demo_base_id:
            logging.error("❌ Demo base ID not set. Please run demo_list_and_create_base first.")

        # Fetch table IDs
        tables = self.get_table_dict()

        # Create sample user records
        user_records = [
            {"name": "Alice Johnson", "email": "alice@example.com", "age": 28},
            {"name": "Bob Smith",     "email": "bob@example.com",   "age": 34},
            {"name": "Charlie Davis", "email": "charlie@example.com","age": 42}
        ]
        user_upload = self.test_record_upload(tables, "Users", user_records)

        # Create sample post records
        post_records = [
            {
                "title":   "Getting Started with NocoDB",
                "content": "NocoDB is an open-source Airtable alternative..."
            },
            {
                "title":   "Advanced NocoDB Features",
                "content": "Let's explore some advanced features of NocoDB..."
            }
        ]
        posts_upload = self.test_record_upload(tables,"Posts", post_records)

        records = self.client.list_records(table_id=tables["Users"])

        # Link Posts records to Users
        logging.info("Linking posts to user records...")
        try:
            link_field_id = self.client.get_column_id(tables["Posts"], "Author")
            users = self.client.link_records(tables["Posts"], link_field_id, "1", [{"Id": "2"}])
            if users and "list" in users:
                count = len(users["list"])
                logging.info(f"✅ Retrieved {count} user records")
                for i, user in enumerate(users["list"][:3]):
                    logging.info(f"User {i+1}: {user.get('name', 'Unknown')} ({user.get('email', 'No email')})")
            else:
                logging.warning("⚠️ Could not retrieve user records")
        except Exception as e:
            logging.error(f"❌ Exception while listing user records: {e}")

    def demo_schema_fetch(self) -> None:
        """
        Fetch and display the schema for the demo base.
        """
        if not self.demo_base_id:
            logging.error("Demo base ID not set. Please run demo_list_and_create_base first.")
            return

        logging.info(f"Fetching schema for base '{self.demo_base_name}'...")
        schema = self.client.fetch_schema(self.demo_base_id)
        print(schema)

        if schema:
            table_count = len(schema)
            table_names = [table.get('title', 'Unknown') for table in schema]

            logging.info(f"✅ Retrieved schema with {table_count} tables: {', '.join(table_names)}")

            # Log some details about each table
            for table in schema:
                table_name = table.get('title', 'Unknown')
                columns = table.get('columns', [])
                column_names = [col.get('title', 'Unknown') for col in columns]

                logging.info(f"Table '{table_name}' has {len(columns)} columns: {', '.join(column_names)}")
        else:
            logging.error("❌ Failed to fetch schema")

    def demo_file_upload(self) -> None:
        """
        Demonstrate file upload functionality.
        """
        # Create a temporary test file
        temp_file_path = Path("./demo_upload_file.txt")
        file_content = "This is a test file for NocoDB upload demo."
        users_table_id = self.client.get_table_id(self.demo_base_id, "Users")

        try:
            # Try to create a real file for testing
            logging.info(f"Creating temporary file at {temp_file_path}...")
            with open(temp_file_path, "w") as f:
                f.write(file_content)

            logging.info("Uploading file to NocoDB...")
            result = self.client.upload_file(
                file_path=temp_file_path,
                title="Demo Upload File"
            )
            logging.info(f"✅ File uploaded successfully with result: {result} ")

            #connect file to entry
            payload = [
                    {
                        "Id": "1",
                        "Uploads": result
                    },
                    {
                        "Id": "2",
                        "Uploads": result
                    }
                ]

            try:
                connect_upload = self.client.update_record(users_table_id, payload)
                logging.info(f"✅ File Upload assigned correctly: {connect_upload} ")
            except Exception as e:
                logging.error(f"❌ Assigning file upload failed: {e}")

        except FileNotFoundError:
            logging.warning("File not found. This is a simulated file upload.")
            logging.info("In a real scenario, you would provide a valid file path.")
        except Exception as e:
            logging.exception(f"Error during file upload: {str(e)}")
        finally:
            # Clean up the temporary file
            if temp_file_path.exists():
                temp_file_path.unlink()
                logging.info(f"Temporary file {temp_file_path} removed")

    def demo_uuid_as_pk(self):

        if not self.demo_base_id:
            logging.error("❌ Demo base ID not set. Please run demo_list_and_create_base first.")

        demo_tables = {
            "UUID":{
                "title": "UUID",
                "columns": [
                    {"column_name": "Id", "title": "Id", "uidt": "SingleLineText", "pk": True},
                    {"column_name": "name", "title": "Name", "uidt": "SingleLineText", "pv": True},
                    {"column_name": "email", "title": "Email", "uidt": "Email"}
                ]
            },
            "UUID_2":{
                "title": "UUID_2",
                "columns": [
                    {"column_name": "Id", "title": "Id", "uidt": "SingleLineText", "pk": True},
                    {"column_name": "title", "title": "Title", "uidt": "SingleLineText", "pv": True},
                    {"column_name": "content", "title": "Content", "uidt": "LongText"}
                ]
            }
        }
        # Create demo tables
        self.test_table_creation(demo_tables["UUID"])
        self.test_table_creation(demo_tables["UUID_2"])


        # Fetch table IDs
        logging.info("Listing tables after creation...")
        tables = self.get_table_dict()

        # Create sample user records
        user_records = [
            {"Id": str(uuid.uuid4()), "name": "Alice Johnson", "email": "alice@example.com", "age": 28},
            {"Id": str(uuid.uuid4()), "name": "Bob Smith", "email": "bob@example.com", "age": 34},
            {"Id": str(uuid.uuid4()), "name": "Charlie Davis", "email": "charlie@example.com", "age": 42}
        ]
        user_upload = self.test_record_upload(tables, "UUID", user_records)

        # Create sample post records
        post_records = [
            {
                "Id": str(uuid.uuid4()),
                "title": "Getting Started with NocoDB",
                "content": "NocoDB is an open-source Airtable alternative..."
            },
            {
                "Id": str(uuid.uuid4()),
                "title": "Advanced NocoDB Features",
                "content": "Let's explore some advanced features of NocoDB..."
            }
        ]
        posts_upload = self.test_record_upload(tables, "UUID_2", post_records)

        # Column creation payload for a LinkToAnotherRecord type
        link_column_payload = {
            "title": "Links",
            "uidt": "Links",
            "parentId": tables["UUID"],
            "childId": tables["UUID_2"],
            "type": "mm"  # many-to-many relationship
        }
        self.test_column_creation(tables_dict=tables, table="UUID", payload=link_column_payload)

        # Link UUID record to UUID_2
        logging.info("Linking UUID to UUID_2 records...")
        try:
            link_field_id = self.client.get_column_id(tables["UUID"], "Links")
            uuid_output = self.client.link_records(tables["UUID"], link_field_id, user_records[0]["Id"], [{"Id": post_records[0]["Id"]}])
            logging.info(uuid_output)
        except Exception as e:
            logging.error(f"❌ Exception while listing user records: {e}")

    def demo_clear_cache(self) -> None:
        """
        Demonstrate cache clearing functionality.
        """
        logging.info("Clearing NocoDBClient cache...")
        self.client.clear_cache()
        logging.info("✅ Cache cleared successfully")

        # Optionally clear specific session
        logging.info("Clearing cache for specific session...")
        self.client.clear_cache(session_key="default")
        logging.info("✅ Session cache cleared successfully")



def main():
    """
    Main function to run the demo harness.
    Can be called directly or via command line.
    """

    # Run the demo
    with DemoRunner() as runner:
        runner.run(["bases","uuid", "schema"])

if __name__ == "__main__":
    main()
