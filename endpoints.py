# endpoints.py

import pymysql
import json
from db_credentials import rds_host, db_username, db_password, db_name

# Database connection function
def connect_to_db():
    try:
        connection = pymysql.connect(host=rds_host, user=db_username, passwd=db_password, db=db_name, connect_timeout=5)
        return connection
    except pymysql.MySQLError as e:
        print(f"ERROR: Unable to connect to MySQL. {e}")
        return None

def handle_insert(body):
    first_name = body.get('firstName')
    last_name = body.get('lastName')
    if not first_name or not last_name:
        return {
            'statusCode': 400,
            'body': json.dumps('Both "firstName" and "lastName" are required')
        }

    connection = connect_to_db()
    if connection is None:
        return {
            'statusCode': 500,
            'body': json.dumps('Database connection failed')
        }

    try:
        with connection.cursor() as cursor:
            insert_query = "INSERT INTO users (firstName, lastName) VALUES (%s, %s)"
            cursor.execute(insert_query, (first_name, last_name))
            connection.commit()
            return {
                'statusCode': 200,
                'body': json.dumps('User inserted successfully!')
            }
    except pymysql.MySQLError as e:
        print(f"ERROR: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to insert data into database')
        }
    finally:
        connection.close()

def handle_select(body):
    first_name = body.get('firstName')
    last_name = body.get('lastName')
    limit = body.get('limit', None)

    select_query = "SELECT firstName, lastName FROM users WHERE 1=1"
    query_params = []

    if first_name:
        select_query += " AND firstName = %s"
        query_params.append(first_name)
    if last_name:
        select_query += " AND lastName = %s"
        query_params.append(last_name)
    if limit:
        try:
            limit = int(limit)
            select_query += " LIMIT %s"
            query_params.append(limit)
        except ValueError:
            return {
                'statusCode': 400,
                'body': json.dumps('Limit must be a valid integer')
            }

    connection = connect_to_db()
    if connection is None:
        return {
            'statusCode': 500,
            'body': json.dumps('Database connection failed')
        }

    try:
        with connection.cursor() as cursor:
            cursor.execute(select_query, query_params)
            result = cursor.fetchall()
            users_list = [{'firstName': row[0], 'lastName': row[1]} for row in result]
            return {
                'statusCode': 200,
                'body': json.dumps(users_list)
            }
    except pymysql.MySQLError as e:
        print(f"ERROR: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to retrieve data from database')
        }
    finally:
        connection.close()
