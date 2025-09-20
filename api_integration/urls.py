from django.urls import path
from . import views

app_name = 'api_integration'

urlpatterns = [
    path('', views.sync_shops, name='sync_shops'),
    path('sync/', views.sync_shops, name='sync'),
    path('sync-categories/', views.sync_categories, name='sync_categories'),
    path('sync/products/', views.sync_products, name='sync_products'),
    path('sync/customers/', views.sync_customers, name='sync_customers'),
    path('sync/orders/', views.sync_orders, name='sync_orders')
]