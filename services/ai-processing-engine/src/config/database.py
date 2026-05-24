import logging
import pymysql
from config.settings import AppSettings

logger = logging.getLogger("AIProcessingEngine.Database")

def get_db_connection():
    return pymysql.connect(
        host=AppSettings.DB_HOST,
        user=AppSettings.DB_USER,
        password=AppSettings.DB_PASS,
        database=AppSettings.DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

def fetch_customer_context(customer_id):
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            sql = "SELECT * FROM customers WHERE customer_id = %s"
            cursor.execute(sql, (customer_id,))
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Database query failed for customer {customer_id}: {e}")
        return None
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()