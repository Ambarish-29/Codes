import subprocess
import os

def dump_postgres_ddl_and_data(host, port, dbname, user, password, output_file, is_table_dump=False, tables=None):
    # Set the PGPASSWORD environment variable for pg_dump authentication
    os.environ['PGPASSWORD'] = password

    pg_dump_path = r"C:\Program Files\PostgreSQL\16\bin\pg_dump.exe"

    if is_table_dump and tables:
        # Define the command to dump both schema and data for specified tables
        table_dump_command = [
            pg_dump_path,
            "--host", host,
            "--port", str(port),
            "--username", user,
            "--dbname", dbname,
            "--no-owner",  # Do not output commands to set ownership
            "--no-acl"  # Do not output commands to set access privileges
        ]

        for table in tables:
            table_dump_command.extend(["--table", table])

        try:
            # Run the table dump command and write to the output file
            with open(output_file, "w") as f:
                subprocess.run(table_dump_command, stdout=f, check=True)
            print(f"Table DDL and data dump completed successfully and saved to {output_file}")

        except subprocess.CalledProcessError as e:
            print(f"Error during table dump: {e}")

    else:
        # Define the command to dump the entire schema (DDL)
        ddl_dump_command = [
            pg_dump_path,
            "--host", host,
            "--port", str(port),
            "--username", user,
            "--dbname", dbname,
            "--schema-only",  # Only dump the schema (DDL)
            "--no-owner",  # Do not output commands to set ownership
            "--no-acl"  # Do not output commands to set access privileges
        ]

        try:
            # Run the DDL dump command and write to the output file
            with open(output_file, "w") as f:
                subprocess.run(ddl_dump_command, stdout=f, check=True)
            print(f"DDL dump completed successfully and saved to {output_file}")

        except subprocess.CalledProcessError as e:
            print(f"Error during DDL dump: {e}")

    # Clean up the PGPASSWORD environment variable
    del os.environ['PGPASSWORD']

if __name__ == "__main__":
    # Database connection details
    host = "localhost"
    port = 5432
    dbname = "postgres"  # Replace with your database name
    user = "postgres"  # Replace with your username
    password = "postgres"  # Replace with your password
    output_file = "pg_ddl_dump.sql"  # Output file for the DDL dump
    is_table_dump = True  # Set to True to include data dump for specified tables
    tables = ["test"]  # List of tables for which data dump is required

    # Call the function to dump the DDL with or without table data based on the flag
    dump_postgres_ddl_and_data(host, port, dbname, user, password, output_file, is_table_dump, tables)
