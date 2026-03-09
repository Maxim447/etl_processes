from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import psycopg2
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

POSTGRES_CONN = "host=etl_postgres port=5432 dbname=etl_db user=airflow password=airflow"
MONGO_URI = "mongodb://admin:admin@etl_mongodb:27017/?authSource=admin"

def get_pg_conn():
    return psycopg2.connect(POSTGRES_CONN)


def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client['etl_source'], client


def transform_session(doc):
    start = doc.get('start_time')
    end = doc.get('end_time')

    if start and end and end < start:
        start, end = end, start

    device = str(doc.get('device', 'unknown')).lower().strip()

    if device not in ('mobile', 'desktop', 'tablet'):
        device = 'unknown'

    return {
        'session_id': doc['session_id'],
        'user_id': doc['user_id'],
        'start_time': start,
        'end_time': end,
        'pages_visited': doc.get('pages_visited', []),
        'device': device,
        'actions': doc.get('actions', [])
    }


def transform_event(doc):
    return {
        'event_id': doc['event_id'],
        'timestamp': doc.get('timestamp'),
        'event_type': str(doc.get('event_type', 'unknown')).lower().strip(),
        'details': str(doc.get('details', ''))
    }


def transform_ticket(doc):
    valid_statuses = ('open', 'closed', 'in_progress', 'pending')
    status = doc.get('status', 'open').lower().strip()
    if status not in valid_statuses:
        status = 'open'
    return {
        'ticket_id': doc['ticket_id'],
        'user_id': doc['user_id'],
        'status': status,
        'issue_type': str(doc.get('issue_type', 'other')).lower().strip(),
        'messages': json.dumps(doc.get('messages', []), default=str),
        'created_at': doc.get('created_at'),
        'updated_at': doc.get('updated_at')
    }


def replicate_user_sessions(**context):
    db, client = get_mongo_db()
    pg = get_pg_conn()
    cursor = pg.cursor()

    cursor.execute("TRUNCATE TABLE user_sessions;")

    docs = list(db['UserSessions'].find({}))
    inserted = 0
    skipped = 0

    for doc in docs:
        try:
            t = transform_session(doc)
            cursor.execute("""
                           INSERT INTO user_sessions
                           (session_id, user_id, start_time, end_time,
                            pages_visited, device, actions, loaded_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, NOW()) ON CONFLICT (session_id) DO
                           UPDATE SET
                               user_id = EXCLUDED.user_id,
                               start_time = EXCLUDED.start_time,
                               end_time = EXCLUDED.end_time,
                               pages_visited = EXCLUDED.pages_visited,
                               device = EXCLUDED.device,
                               actions = EXCLUDED.actions,
                               loaded_at = NOW()
                           """, (
                               t['session_id'], t['user_id'],
                               t['start_time'], t['end_time'],
                               t['pages_visited'], t['device'], t['actions']
                           ))
            inserted += 1
        except Exception as e:
            skipped += 1

    pg.commit()
    cursor.close()
    pg.close()
    client.close()

def replicate_event_logs(**context):
    db, client = get_mongo_db()
    pg = get_pg_conn()
    cursor = pg.cursor()

    cursor.execute("TRUNCATE TABLE event_logs;")

    docs = list(db['EventLogs'].find({}))
    inserted = 0
    skipped = 0

    for doc in docs:
        try:
            t = transform_event(doc)
            cursor.execute("""
                           INSERT INTO event_logs
                               (event_id, timestamp, event_type, details, loaded_at)
                           VALUES (%s, %s, %s, %s, NOW()) ON CONFLICT (event_id) DO
                           UPDATE SET
                               timestamp = EXCLUDED.timestamp,
                               event_type = EXCLUDED.event_type,
                               details = EXCLUDED.details,
                               loaded_at = NOW()
                           """, (t['event_id'], t['timestamp'], t['event_type'], t['details']))
            inserted += 1
        except Exception as e:
            skipped += 1

    pg.commit()
    cursor.close()
    pg.close()
    client.close()

def replicate_support_tickets(**context):
    db, client = get_mongo_db()
    pg = get_pg_conn()
    cursor = pg.cursor()

    cursor.execute("TRUNCATE TABLE support_tickets;")

    docs = list(db['SupportTickets'].find({}))
    inserted = 0
    skipped = 0

    for doc in docs:
        try:
            t = transform_ticket(doc)
            cursor.execute("""
                           INSERT INTO support_tickets
                           (ticket_id, user_id, status, issue_type,
                            messages, created_at, updated_at, loaded_at)
                           VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, NOW()) ON CONFLICT (ticket_id) DO
                           UPDATE SET
                               status = EXCLUDED.status,
                               issue_type = EXCLUDED.issue_type,
                               messages = EXCLUDED.messages,
                               updated_at = EXCLUDED.updated_at,
                               loaded_at = NOW()
                           """, (
                               t['ticket_id'], t['user_id'], t['status'],
                               t['issue_type'], t['messages'],
                               t['created_at'], t['updated_at']
                           ))
            inserted += 1
        except Exception as e:
            skipped += 1

    pg.commit()
    cursor.close()
    pg.close()
    client.close()

def replicate_recommendations(**context):
    db, client = get_mongo_db()
    pg = get_pg_conn()
    cursor = pg.cursor()

    cursor.execute("TRUNCATE TABLE user_recommendations;")

    docs = list(db['UserRecommendations'].find({}))
    inserted = 0

    for doc in docs:
        cursor.execute("""
                       INSERT INTO user_recommendations
                           (user_id, recommended_products, last_updated, loaded_at)
                       VALUES (%s, %s, %s, NOW()) ON CONFLICT (user_id) DO
                       UPDATE SET
                           recommended_products = EXCLUDED.recommended_products,
                           last_updated = EXCLUDED.last_updated,
                           loaded_at = NOW()
                       """, (
                           doc['user_id'],
                           doc.get('recommended_products', []),
                           doc.get('last_updated')
                       ))
        inserted += 1

    pg.commit()
    cursor.close()
    pg.close()
    client.close()

def replicate_moderation_queue(**context):
    db, client = get_mongo_db()
    pg = get_pg_conn()
    cursor = pg.cursor()

    cursor.execute("TRUNCATE TABLE moderation_queue;")

    docs = list(db['ModerationQueue'].find({}))
    inserted = 0

    for doc in docs:
        rating = doc.get('rating', 3)
        if not isinstance(rating, int) or rating < 1 or rating > 5:
            rating = 3

        cursor.execute("""
                       INSERT INTO moderation_queue
                       (review_id, user_id, product_id, review_text,
                        rating, moderation_status, flags, submitted_at, loaded_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW()) ON CONFLICT (review_id) DO
                       UPDATE SET
                           moderation_status = EXCLUDED.moderation_status,
                           flags = EXCLUDED.flags,
                           loaded_at = NOW()
                       """, (
                           doc['review_id'], doc['user_id'], doc.get('product_id'),
                           doc.get('review_text', ''), rating,
                           doc.get('moderation_status', 'pending'),
                           doc.get('flags', []),
                           doc.get('submitted_at')
                       ))
        inserted += 1

    pg.commit()
    cursor.close()
    pg.close()
    client.close()

def validate_data(**context):
    pg = get_pg_conn()
    cursor = pg.cursor()

    tables = [
        'user_sessions',
        'event_logs',
        'support_tickets',
        'user_recommendations',
        'moderation_queue'
    ]

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} строк")

    cursor.execute("""
                   SELECT COUNT(*) - COUNT(DISTINCT session_id)
                   FROM user_sessions;
                   """)
    dups = cursor.fetchone()[0]

    cursor.close()
    pg.close()


with DAG(
        dag_id='replicate_to_postgres',
        default_args=default_args,
        description='Репликация данных из MongoDB в PostgreSQL',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['etl', 'replication', 'postgres'],
) as dag:
    t1 = PythonOperator(
        task_id='replicate_user_sessions',
        python_callable=replicate_user_sessions,
    )
    t2 = PythonOperator(
        task_id='replicate_event_logs',
        python_callable=replicate_event_logs,
    )
    t3 = PythonOperator(
        task_id='replicate_support_tickets',
        python_callable=replicate_support_tickets,
    )
    t4 = PythonOperator(
        task_id='replicate_recommendations',
        python_callable=replicate_recommendations,
    )
    t5 = PythonOperator(
        task_id='replicate_moderation_queue',
        python_callable=replicate_moderation_queue,
    )
    t6 = PythonOperator(
        task_id='validate_replicated_data',
        python_callable=validate_data,
    )

    [t1, t2, t3, t4, t5] >> t6
