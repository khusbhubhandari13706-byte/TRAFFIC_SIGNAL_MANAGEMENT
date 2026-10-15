
import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        port=3307,
        user="root",
        password="Root@12345",
        database="traffic_manager"
    )

if __name__ == "__main__":
    connection = None

    try:
        connection = get_connection()

        if connection.is_connected():
            print("MySQL connection successful!")

            cursor = connection.cursor()
            cursor.execute("SELECT direction, current_state FROM signals")

            for row in cursor.fetchall():
                print(row)

            cursor.close()

    except mysql.connector.Error as error:
        print("Database connection failed:", error)

    finally:
        if connection is not None and connection.is_connected():
            connection.close()
