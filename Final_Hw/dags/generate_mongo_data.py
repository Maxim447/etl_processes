from datetime import datetime, timedelta
import random

from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def get_mongo_client():
    return MongoClient(
        host='etl_mongodb',
        port=27017,
        username='admin',
        password='admin',
        authSource='admin'
    )


def generate_user_sessions(**context):
    client = get_mongo_client()
    db = client['etl_source']
    collection = db['UserSessions']
    collection.drop()

    pages = [
        '/home',
        '/products',
        '/products/42',
        '/cart',
        '/checkout',
        '/profile',
        '/orders',
        '/search'
    ]
    actions = [
        'login',
        'view_product',
        'add_to_cart',
        'remove_from_cart',
        'checkout',
        'logout',
        'search'
    ]
    devices = [
        'mobile',
        'desktop',
        'tablet'
    ]

    sessions = []
    for i in range(1, 201):
        start = datetime(2024, 1, 1) + timedelta(
            days=random.randint(0, 89),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        duration = timedelta(minutes=random.randint(2, 120))
        end = start + duration

        visited = random.sample(pages, k=random.randint(1, 5))
        user_actions = random.sample(actions, k=random.randint(1, 4))

        sessions.append({
            'session_id': f'sess_{i:04d}',
            'user_id': f'user_{random.randint(1, 50):03d}',
            'start_time': start,
            'end_time': end,
            'pages_visited': visited,
            'device': random.choice(devices),
            'actions': user_actions
        })

    collection.insert_many(sessions)
    collection.create_index('session_id', unique=True)
    client.close()


def generate_event_logs(**context):
    from datetime import datetime, timedelta
    import random

    client = get_mongo_client()
    db = client['etl_source']
    collection = db['EventLogs']
    collection.drop()

    event_types = [
        'click',
        'view',
        'purchase',
        'search',
        'scroll',
        'hover'
    ]
    details_pool = [
        '/products/42',
        '/home',
        '/cart',
        '/checkout',
        'search_query: laptop',
        'banner_click',
        '/profile'
    ]

    events = []
    for i in range(1, 301):
        ts = datetime(2024, 1, 1) + timedelta(
            days=random.randint(0, 89),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        events.append({
            'event_id': f'evt_{1000 + i}',
            'timestamp': ts,
            'event_type': random.choice(event_types),
            'details': random.choice(details_pool)
        })

    collection.insert_many(events)
    collection.create_index('event_id', unique=True)
    client.close()


def generate_support_tickets(**context):
    from datetime import datetime, timedelta
    import random

    client = get_mongo_client()
    db = client['etl_source']
    collection = db['SupportTickets']
    collection.drop()

    statuses = [
        'open',
        'closed',
        'in_progress',
        'pending'
    ]
    issue_types = [
        'payment',
        'delivery',
        'account',
        'product',
        'refund'
    ]

    tickets = []
    for i in range(1, 151):
        created = datetime(2024, 1, 1) + timedelta(
            days=random.randint(0, 89),
            hours=random.randint(0, 23)
        )
        updated = created + timedelta(hours=random.randint(1, 72))
        status = random.choice(statuses)
        issue = random.choice(issue_types)

        messages = [
            {
                'sender': 'user',
                'message': f'Проблема с {issue}',
                'timestamp': created.isoformat()
            },
            {
                'sender': 'support',
                'message': 'Рассматриваем ваш запрос.',
                'timestamp': (created + timedelta(hours=1)).isoformat()
            }
        ]
        if status == 'closed':
            messages.append({
                'sender': 'support',
                'message': 'Проблема решена.',
                'timestamp': updated.isoformat()
            })

        tickets.append({
            'ticket_id': f'ticket_{700 + i}',
            'user_id': f'user_{random.randint(1, 50):03d}',
            'status': status,
            'issue_type': issue,
            'messages': messages,
            'created_at': created,
            'updated_at': updated
        })

    collection.insert_many(tickets)
    collection.create_index('ticket_id', unique=True)
    client.close()


def generate_recommendations(**context):
    from datetime import datetime, timedelta
    import random

    client = get_mongo_client()
    db = client['etl_source']
    collection = db['UserRecommendations']
    collection.drop()

    products = [f'prod_{100 + i}' for i in range(50)]

    recs = []
    for i in range(1, 51):
        recs.append({
            'user_id': f'user_{i:03d}',
            'recommended_products': random.sample(products, k=random.randint(2, 6)),
            'last_updated': datetime(2024, 1, 1) + timedelta(days=random.randint(0, 89))
        })

    collection.insert_many(recs)
    collection.create_index('user_id', unique=True)
    client.close()


def generate_moderation_queue(**context):
    from datetime import datetime, timedelta
    import random

    client = get_mongo_client()
    db = client['etl_source']
    collection = db['ModerationQueue']
    collection.drop()

    mod_statuses = [
        'pending',
        'approved',
        'rejected'
    ]
    flag_pool = [
        'contains_images',
        'spam', 'offensive',
        'verified_purchase',
        'long_review'
    ]
    products = [f'prod_{100 + i}' for i in range(50)]
    review_texts = [
        'Отличный товар, рекомендую!',
        'Плохое качество, не советую.',
        'Нормальный продукт за свою цену.',
        'Доставили быстро, всё работает.',
        'Не соответствует описанию.',
        'Буду покупать ещё раз!'
    ]

    reviews = []
    for i in range(1, 101):
        submitted = datetime(2024, 1, 1) + timedelta(
            days=random.randint(0, 89),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        reviews.append({
            'review_id': f'rev_{500 + i}',
            'user_id': f'user_{random.randint(1, 50):03d}',
            'product_id': random.choice(products),
            'review_text': random.choice(review_texts),
            'rating': random.randint(1, 5),
            'moderation_status': random.choice(mod_statuses),
            'flags': random.sample(flag_pool, k=random.randint(0, 2)),
            'submitted_at': submitted
        })

    collection.insert_many(reviews)
    collection.create_index('review_id', unique=True)
    client.close()
    print(f"ModerationQueue: вставлено {len(reviews)} документов")


with DAG(
        dag_id='generate_mongo_data',
        default_args=default_args,
        description='Генерация тестовых данных в MongoDB',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['etl', 'mongodb', 'generate'],
) as dag:
    t1 = PythonOperator(
        task_id='generate_user_sessions',
        python_callable=generate_user_sessions,
    )
    t2 = PythonOperator(
        task_id='generate_event_logs',
        python_callable=generate_event_logs,
    )
    t3 = PythonOperator(
        task_id='generate_support_tickets',
        python_callable=generate_support_tickets,
    )
    t4 = PythonOperator(
        task_id='generate_recommendations',
        python_callable=generate_recommendations,
    )
    t5 = PythonOperator(
        task_id='generate_moderation_queue',
        python_callable=generate_moderation_queue,
    )

    t1 >> t2 >> t3 >> t4 >> t5
