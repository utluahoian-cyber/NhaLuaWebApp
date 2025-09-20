# your_project/celery.py
import os
from celery import Celery
from celery.schedules import crontab

# Set Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'NhaLuaWebApp.settings')

app = Celery('NhaLuaWebApp')
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks
app.autodiscover_tasks(['api_integration'])
app.conf.beat_schedule = {
    # Sync shops - mỗi ngày 1 lần (3:00 AM)
    'sync-shops-daily': {
        'task': 'api_integration.tasks.sync_shops_task',
        'schedule': crontab(hour=3, minute=0), 
    },
    
    # Sync categories - mỗi ngày 1 lần (3:30 AM)
    'sync-categories-daily': {
        'task': 'api_integration.tasks.sync_categories_task', 
        'schedule': crontab(hour=3, minute=30),  
    },
    
    # Sync products - mỗi ngày 1 lần (4:00 AM)
    'sync-products-daily': {
        'task': 'api_integration.tasks.sync_all_products',
        'schedule': crontab(hour=4, minute=0),  
    },
    
    # Sync customers (30 days) - mỗi 1 giờ 30 phút
    'sync-customers-30-days': {
        'task': 'api_integration.tasks.sync_all_customers_30_days',
        'schedule': crontab(minute='*/90'),  # Mỗi 90 phút
    },
    
    # Sync orders - mỗi 30 phút
    'sync-orders-every-30-minutes': {
        'task': 'api_integration.tasks.sync_orders_daily',
        'schedule': crontab(minute='*/30'),
    },
    
    # Sync all data - mỗi ngày (2:00 AM)
    'sync-all-data-daily': {
        'task': 'api_integration.tasks.sync_all_data_task',
        'schedule': crontab(hour=2, minute=0),
    },
    
    # Cleanup - mỗi 10 phút
    'cleanup-customer-sync-histories': {
        'task': 'api_integration.tasks.cleanup_old_customer_sync_histories',
        'schedule': crontab(minute='*/10'),
    },
}

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')