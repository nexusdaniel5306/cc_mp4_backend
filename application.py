from flask import Flask, jsonify, request
import os
import pymysql
from pymysql.err import OperationalError
import logging
from flask_cors import CORS

application = Flask(__name__)
CORS(application)
logging.basicConfig(level=logging.INFO)

#Endpoint: Health Check
@application.route('/health', methods=['GET'])
def health():
    """
    This endpoint is used by the autograder to confirm that the backend deployment is healthy.
    """
    return jsonify({"status": "healthy"}), 200

#endpoint events get for testing
@application.route('/events', methods=['GET'])
def list_events():
    try:
        data = fetch_data_from_db()
        return jsonify(data), 200   # returns a JSON array: [...]
    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during events retrieval")
        return jsonify({"error": "During events retrieval", "detail": str(e)}), 500

#Endpoint: Data Insertion
@application.route('/events', methods=['POST'])
def create_event():
    try:
        payload = request.get_json(force=True, silent=True) or {}

        title = payload.get("title")
        date = payload.get("date")
        if not title or not date:
            return jsonify({"error": "Missing required fields: 'title' and 'date'"}), 400

        insert_data_into_db(payload)
        return jsonify({"message": "Event created successfully"}), 201

    except Exception as e:
        logging.exception("Error occurred during event creation")
        return jsonify({"error": "During event creation", "detail": str(e)}), 500

#Endpoint: Data Retrieval
@application.route('/data', methods=['GET'])
def get_data():
    try:
        data = fetch_data_from_db()
        return jsonify({"data": data}), 200
    except Exception as e:
        logging.exception("Error occurred during data retrieval")
        return jsonify({"error": "During data retrieval", "detail": str(e)}), 500

def get_db_connection():
    """
    Establish and return a connection to the RDS MySQL database.
    The following variables should be added to the Elastic Beanstalk Environment Properties for better security. Follow guidelines for more info.
      - DB_HOST
      - DB_USER
      - DB_PASSWORD
      - DB_NAME
    """
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        msg = f"Missing environment variables: {', '.join(missing)}"
        logging.error(msg)
        raise EnvironmentError(msg)
    try:
        connection = pymysql.connect(
            host=os.environ.get("DB_HOST"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            db=os.environ.get("DB_NAME")
        )
        return connection
    except OperationalError as e:
        raise ConnectionError(f"Failed to connect to the database: {e}")

def create_db_table():
    connection = get_db_connection()
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS events (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    description TEXT,
                    image_url VARCHAR(255),
                    date DATE NOT NULL,
                    location VARCHAR(255)
                )
                """
                cursor.execute(create_table_sql)
            connection.commit()
            logging.info("Events table created or already exists")
    except Exception as e:
        logging.exception("Failed to create or verify the events table")
        raise RuntimeError(f"Table creation failed: {str(e)}")

def insert_data_into_db(payload):
    create_db_table()

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO events (title, description, image_url, date, location)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                payload.get("title"),
                payload.get("description"),
                payload.get("image_url"),
                payload.get("date"),
                payload.get("location"),
            ))
        connection.commit()
    finally:
        connection.close()

def fetch_data_from_db():
    create_db_table()

    connection = get_db_connection()
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT
                    id, title, description, image_url,
                    DATE_FORMAT(`date`, '%a, %d %b %Y 00:00:00 GMT') AS `date`
                    location
                FROM events
                ORDER BY date ASC, id ASC
            """
            "date": row['date'].strftime('%a, %d %b %Y 00:00:00 GMT') if row.get("date") else None,

            cursor.execute(sql)
            rows = cursor.fetchall()
        return rows
    finally:
        connection.close()

if __name__ == '__main__':
    application.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
