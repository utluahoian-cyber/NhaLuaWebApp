from celery import shared_task
from django.utils import timezone
from django.conf import settings
from django.core.mail import send_mail
from django.template.loader import render_to_string
import requests
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from shops.models import *
import logging
import requests
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import pytz
from django.db import transaction, connection
import time

VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
logger = logging.getLogger(__name__)


@dataclass
class ShopSyncResult:
    shops_created: int = 0
    shops_updated: int = 0
    pages_created: int = 0
    pages_updated: int = 0
    tags_created: int = 0
    tags_updated: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

@dataclass
class CategorySyncResult:
    categories_created: int = 0
    categories_updated: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


def _get_vietnam_time(dt=None):
    """Get current time in Vietnam timezone or convert datetime to Vietnam timezone"""
    if dt is None:
        return timezone.now().astimezone(VIETNAM_TZ)
    
    if dt.tzinfo is None:
        # Assume UTC if no timezone info
        dt = pytz.UTC.localize(dt)
    
    return dt.astimezone(VIETNAM_TZ)

def _send_sync_notification(task_name: str, result: dict, is_success: bool = True):
    """Send email notification about sync result"""
    try:
        if not hasattr(settings, 'SYNC_NOTIFICATION_EMAILS') or not settings.SYNC_NOTIFICATION_EMAILS:
            return
        
        subject = f"[{'SUCCESS' if is_success else 'ERROR'}] {task_name} - {_get_vietnam_time().strftime('%Y-%m-%d %H:%M:%S')}"
        
        context = {
            'task_name': task_name,
            'result': result,
            'is_success': is_success,
            'sync_time': _get_vietnam_time(),
        }
        
        # Render email template (tạo template sau)
        message = render_to_string('emails/sync_notification.html', context)
        
        send_mail(
            subject=subject,
            message=message,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=settings.SYNC_NOTIFICATION_EMAILS,
            html_message=message,
            fail_silently=True
        )
        
    except Exception as e:
        logger.error(f"Failed to send sync notification: {e}")

# ===== SHOP SYNC FUNCTIONS =====
def _fetch_shops_data() -> Dict:
    """Fetch shops data from Pancake API"""
    api_url = f"{settings.PANCAKE_API_BASE_URL}/shops"
    params = {'api_key': settings.PANCAKE_API_KEY}
    
    logger.info("Fetching shops data from Pancake API")
    
    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        shops_data = data.get('shops', [])
        logger.info(f"API response: {len(shops_data)} shops received")
        
        return shops_data
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise

def _sync_single_shop(shop_data: Dict) -> Tuple[Shop, bool, int, int]:
    """Sync a single shop with its pages and tags"""
    vietnam_now = _get_vietnam_time()
    
    # Sync Shop
    shop, shop_created = Shop.objects.update_or_create(
        pancake_id=shop_data.get('id'),
        defaults={
            'name': shop_data.get('name', ''),
            'currency': shop_data.get('currency', 'VND'),
            'avatar_url': shop_data.get('avatar_url'),
            'link_post_marketer': shop_data.get('link_post_marketer', []),
            'last_sync': vietnam_now,
        }
    )
    
    # Sync Pages
    pages_synced = 0
    tags_synced = 0
    existing_page_ids = []
    
    for page_data in shop_data.get('pages', []):
        page, page_created = Page.objects.update_or_create(
            shop=shop,
            pancake_id=page_data.get('id'),
            defaults={
                'name': page_data.get('name', ''),
                'platform': page_data.get('platform', ''),
                'username': page_data.get('username'),
                'is_onboard_xendit': page_data.get('is_onboard_xendit'),
                'progressive_catalog_error': page_data.get('progressive_catalog_error'),
                'settings': page_data.get('settings', {}),
            }
        )
        existing_page_ids.append(page.pancake_id)
        pages_synced += 1
        
        # Sync Tags
        existing_tag_ids = []
        for tag_data in page_data.get('tags', []):
            tag, tag_created = Tag.objects.update_or_create(
                page=page,
                pancake_id=tag_data.get('id'),
                defaults={
                    'text': tag_data.get('text', ''),
                    'color': tag_data.get('color', ''),
                    'lighten_color': tag_data.get('lighten_color', ''),
                    'description': tag_data.get('description', ''),
                    'is_lead_event': tag_data.get('is_lead_event', False),
                }
            )
            existing_tag_ids.append(tag.pancake_id)
            tags_synced += 1
        
        # Xóa tags không còn tồn tại
        page.tags.exclude(pancake_id__in=existing_tag_ids).delete()
    
    # Xóa pages không còn tồn tại
    shop.pages.exclude(pancake_id__in=existing_page_ids).delete()
    
    return shop, shop_created, pages_synced, tags_synced

def _sync_all_shops() -> ShopSyncResult:
    """Sync all shops from Pancake API"""
    result = ShopSyncResult()
    vietnam_start = _get_vietnam_time()
    
    try:
        shops_data = _fetch_shops_data()
        logger.info(f"Starting sync for {len(shops_data)} shops at {vietnam_start}")
        
        for i, shop_data in enumerate(shops_data):
            try:
                # Sync từng shop riêng biệt để tránh rollback toàn bộ
                with transaction.atomic():
                    shop, shop_created, pages_synced, tags_synced = _sync_single_shop(shop_data)
                    
                    if shop_created:
                        result.shops_created += 1
                    else:
                        result.shops_updated += 1
                    
                    result.pages_created += pages_synced
                    result.tags_created += tags_synced
                    
                    logger.info(f"Synced shop {shop.name}: {pages_synced} pages, {tags_synced} tags")
                    
            except Exception as e:
                error_msg = f"Error syncing shop {i+1}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                result.errors.append(error_msg)
                continue
        
        vietnam_end = _get_vietnam_time()
        duration = (vietnam_end - vietnam_start).total_seconds()
        logger.info(f"Shop sync completed in {duration:.2f}s: "
                   f"{result.shops_created + result.shops_updated} shops, "
                   f"{result.pages_created} pages, {result.tags_created} tags")
        
    except Exception as e:
        error_msg = f"Critical error in shop sync: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.errors.append(error_msg)
    
    return result

# ===== CATEGORY SYNC FUNCTIONS =====
def _fetch_categories_for_shop(shop: Shop) -> List[Dict]:
    """Fetch categories for a single shop"""
    api_url = f"{settings.PANCAKE_API_BASE_URL}/shops/{shop.pancake_id}/categories"
    params = {'api_key': settings.PANCAKE_API_KEY}
    
    logger.info(f"Fetching categories for shop {shop.name}")
    
    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        response_data = response.json()
        
        if not response_data.get('success', False):
            raise ValueError(f"API returned success=false for shop {shop.name}")
        
        categories_data = response_data.get('data', [])
        logger.info(f"Received {len(categories_data)} categories for shop {shop.name}")
        
        return categories_data
        
    except requests.RequestException as e:
        logger.error(f"Network error fetching categories for shop {shop.name}: {e}")
        raise
    except ValueError as e:
        logger.error(f"API error for shop {shop.name}: {e}")
        raise

def _sync_categories_for_shop(shop: Shop) -> Tuple[int, int]:
    """Sync categories for a single shop"""
    try:
        categories_data = _fetch_categories_for_shop(shop)
        
        if not categories_data:
            return 0, 0
        
        existing_category_ids = []
        category_map = {}
        created_count = 0
        updated_count = 0
        
        # Lần 1: Tạo/update tất cả parent categories
        for category_data in categories_data:
            pancake_id = category_data.get('id')
            if not pancake_id:
                continue
                
            category, created = Category.objects.update_or_create(
                shop=shop,
                pancake_id=pancake_id,
                defaults={
                    'name': category_data.get('text', ''),
                    'description': '',
                    'sort_order': 0,
                    'is_active': not category_data.get('is_admin_category', False),
                    'parent': None
                }
            )
            existing_category_ids.append(category.pancake_id)
            category_map[pancake_id] = category
            
            if created:
                created_count += 1
            else:
                updated_count += 1
        
        # Lần 2: Xử lý nested categories (nodes)
        for category_data in categories_data:
            parent_id = category_data.get('id')
            nodes = category_data.get('nodes', [])
            
            if parent_id in category_map and nodes:
                parent_category = category_map[parent_id]
                
                for node_data in nodes:
                    node_id = node_data.get('id')
                    if not node_id:
                        continue
                    
                    child_category, created = Category.objects.update_or_create(
                        shop=shop,
                        pancake_id=node_id,
                        defaults={
                            'name': node_data.get('text', ''),
                            'description': '',
                            'sort_order': 0,
                            'is_active': not node_data.get('is_admin_category', False),
                            'parent': parent_category
                        }
                    )
                    existing_category_ids.append(child_category.pancake_id)
                    
                    if created:
                        created_count += 1
                    else:
                        updated_count += 1
        
        # Xóa categories không còn tồn tại
        deleted_count = shop.categories.exclude(pancake_id__in=existing_category_ids).delete()[0]
        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} categories for shop {shop.name}")
        
        logger.info(f"Shop {shop.name}: {created_count} created, {updated_count} updated categories")
        return created_count, updated_count
        
    except Exception as e:
        logger.error(f"Error syncing categories for shop {shop.name}: {e}", exc_info=True)
        raise

def _sync_all_categories() -> CategorySyncResult:
    """Sync categories for all shops"""
    result = CategorySyncResult()
    vietnam_start = _get_vietnam_time()
    
    try:
        shops = Shop.objects.all()
        logger.info(f"Starting category sync for {shops.count()} shops at {vietnam_start}")
        
        for shop in shops:
            try:
                # Sync từng shop riêng biệt
                with transaction.atomic():
                    created, updated = _sync_categories_for_shop(shop)
                    result.categories_created += created
                    result.categories_updated += updated
                    
            except Exception as e:
                error_msg = f"Shop {shop.name}: {str(e)}"
                logger.error(error_msg)
                result.errors.append(error_msg)
                continue
        
        vietnam_end = _get_vietnam_time()
        duration = (vietnam_end - vietnam_start).total_seconds()
        logger.info(f"Category sync completed in {duration:.2f}s: "
                   f"{result.categories_created} created, {result.categories_updated} updated")
        
    except Exception as e:
        error_msg = f"Critical error in category sync: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.errors.append(error_msg)
    
    return result


# ===== CELERY TASKS =====
@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def sync_shops_task(self):
    """
    Celery task để đồng bộ shops từ Pancake API
    Tự động retry 3 lần với delay 5 phút
    """
    task_name = "Sync Shops"
    vietnam_start = _get_vietnam_time()
    
    try:
        logger.info(f"Starting {task_name} task at {vietnam_start}")
        
        # Thực hiện đồng bộ
        result = _sync_all_shops()
        vietnam_end = _get_vietnam_time()
        duration = (vietnam_end - vietnam_start).total_seconds()
        
        # Tạo summary
        summary = {
            'task_id': self.request.id,
            'start_time': vietnam_start.isoformat(),
            'end_time': vietnam_end.isoformat(),
            'duration_seconds': duration,
            'shops_created': result.shops_created,
            'shops_updated': result.shops_updated,
            'pages_synced': result.pages_created,
            'tags_synced': result.tags_created,
            'total_errors': len(result.errors),
            'error_details': result.errors[:10],  # Chỉ lấy 10 lỗi đầu
            'success': len(result.errors) == 0
        }
        
        # Log kết quả
        if summary['success']:
            logger.info(f"{task_name} completed successfully: {summary}")
        else:
            logger.warning(f"{task_name} completed with errors: {summary}")
        
        
        return summary
        
    except Exception as exc:
        vietnam_error = _get_vietnam_time()
        error_summary = {
            'task_id': self.request.id,
            'error_time': vietnam_error.isoformat(),
            'error_message': str(exc),
            'retry_count': self.request.retries,
            'max_retries': self.max_retries
        }
        
        logger.error(f"Error in {task_name} task: {error_summary}", exc_info=True)
        
        # Retry nếu chưa đạt max retries
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying {task_name} task in 5 minutes...")
            raise self.retry(exc=exc, countdown=300)
        

        
        raise exc

@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def sync_categories_task(self):
    """
    Celery task để đồng bộ categories từ Pancake API
    Tự động retry 3 lần với delay 5 phút
    """
    task_name = "Sync Categories"
    vietnam_start = _get_vietnam_time()
    
    try:
        logger.info(f"Starting {task_name} task at {vietnam_start}")
        
        # Thực hiện đồng bộ
        result = _sync_all_categories()
        vietnam_end = _get_vietnam_time()
        duration = (vietnam_end - vietnam_start).total_seconds()
        
        # Tạo summary
        summary = {
            'task_id': self.request.id,
            'start_time': vietnam_start.isoformat(),
            'end_time': vietnam_end.isoformat(),
            'duration_seconds': duration,
            'categories_created': result.categories_created,
            'categories_updated': result.categories_updated,
            'total_errors': len(result.errors),
            'error_details': result.errors[:10],
            'success': len(result.errors) == 0
        }
        
        # Log kết quả
        if summary['success']:
            logger.info(f"{task_name} completed successfully: {summary}")
        else:
            logger.warning(f"{task_name} completed with errors: {summary}")
        

        
        return summary
        
    except Exception as exc:
        vietnam_error = _get_vietnam_time()
        error_summary = {
            'task_id': self.request.id,
            'error_time': vietnam_error.isoformat(),
            'error_message': str(exc),
            'retry_count': self.request.retries,
            'max_retries': self.max_retries
        }
        
        logger.error(f"Error in {task_name} task: {error_summary}", exc_info=True)
        
        # Retry nếu chưa đạt max retries
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying {task_name} task in 5 minutes...")
            raise self.retry(exc=exc, countdown=300)
        

        
        raise exc

@shared_task
def sync_all_data_task():
    """
    Task tổng hợp để sync cả shops và categories theo thứ tự
    """
    vietnam_start = _get_vietnam_time()
    logger.info(f"Starting full data sync at {vietnam_start}")
    
    results = {
        'start_time': vietnam_start.isoformat(),
        'shops_result': None,
        'categories_result': None,
        'overall_success': True
    }
    
    try:
        
        logger.info("Step 1: Syncing shops...")
        shops_result = sync_shops_task.apply()
        results['shops_result'] = shops_result.result
        
        if not results['shops_result']['success']:
            results['overall_success'] = False
            logger.warning("Shop sync failed, but continuing with category sync...")
        
        
        logger.info("Step 2: Syncing categories...")
        categories_result = sync_categories_task.apply()
        results['categories_result'] = categories_result.result
        
        if not results['categories_result']['success']:
            results['overall_success'] = False
        
    except Exception as e:
        logger.error(f"Error in full data sync: {e}", exc_info=True)
        results['overall_success'] = False
        results['error'] = str(e)
    
    vietnam_end = _get_vietnam_time()
    results['end_time'] = vietnam_end.isoformat()
    results['total_duration'] = (vietnam_end - vietnam_start).total_seconds()
    
    logger.info(f"Full data sync completed: {results}")
    return results

@dataclass
class ProductSyncResult:
    products_created: int = 0
    products_updated: int = 0
    variations_created: int = 0
    variations_updated: int = 0
    fields_created: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

# ===== UTILITY FUNCTIONS =====
def _get_vietnam_time(dt=None):
    """Get current time in Vietnam timezone"""
    if dt is None:
        return timezone.now().astimezone(VIETNAM_TZ)
    
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    
    return dt.astimezone(VIETNAM_TZ)

def _parse_datetime(datetime_str: Optional[str]) -> timezone.datetime:
    """Parse datetime string from API with proper timezone handling"""
    if not datetime_str:
        return timezone.now()
    
    try:
        if datetime_str.endswith('Z'):
            dt = timezone.datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        elif '+' in datetime_str or '-' in datetime_str[-6:]:
            dt = timezone.datetime.fromisoformat(datetime_str)
        else:
            dt = timezone.datetime.fromisoformat(datetime_str + '+00:00')
        
        return dt if dt else timezone.now()
        
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse datetime '{datetime_str}': {e}")
        return timezone.now()

def _reset_database_connection():
    """Reset database connection"""
    try:
        connection.close()
        connection.ensure_connection()
        logger.info("Database connection reset successfully")
    except Exception as e:
        logger.error(f"Failed to reset database connection: {e}")

# ===== API FUNCTIONS =====
def _fetch_product_variations_page(shop_id: int, page: int = 1, page_size: int = 30) -> Dict:
    """Fetch single page of product variations from Pancake API"""
    api_url = f"{settings.PANCAKE_API_BASE_URL}/shops/{shop_id}/products/variations"
    params = {
        'api_key': settings.PANCAKE_API_KEY,
        'page': page,
        'page_size': page_size,
    }
    
    logger.info(f"Fetching shop {shop_id}, page {page} with page_size {page_size}")
    
    response = requests.get(api_url, params=params, timeout=540)
    response.raise_for_status()
    
    data = response.json()
    logger.info(f"API response: success={data.get('success')}, page={data.get('page_number')}, "
                f"total_pages={data.get('total_pages')}, data_count={len(data.get('data', []))}")
    
    return data

# ===== DATA EXTRACTION FUNCTIONS =====
def _extract_products_data(variations_data: List[Dict], shop: Shop) -> List[Dict]:
    """Extract unique products data from variations response"""
    products_dict = {}
    
    logger.info(f"Extracting products from {len(variations_data)} variations for shop {shop.name}")
    
    for i, variation_data in enumerate(variations_data):
        try:
            product_id = variation_data.get('product_id')
            if not product_id:
                logger.warning(f"Variation {i} missing product_id")
                continue
                
            if product_id in products_dict:
                continue
                
            product_info = variation_data.get('product', {})
            if not product_info:
                logger.warning(f"Variation {i} missing product info")
                continue
            
            products_dict[product_id] = {
                'shop': shop,
                'pancake_id': product_id,
                'display_id': product_info.get('display_id', ''),
                'name': product_info.get('name', ''),
                'image_url': product_info.get('image'),
                'note_product': product_info.get('note_product', ''),
                'is_published': product_info.get('is_published'),
                'tags': product_info.get('tags', []),
                'manipulation_warehouses': product_info.get('manipulation_warehouses', []),
                'inserted_at': _parse_datetime(product_info.get('inserted_at')),
                'last_sync': timezone.now(),
                'category_ids': [cat.get('id') if isinstance(cat, dict) else cat for cat in product_info.get('categories', [])]
            }
        except Exception as e:
            logger.error(f"Error extracting product from variation {i}: {e}")
            continue
    
    result = list(products_dict.values())
    logger.info(f"Extracted {len(result)} unique products")
    return result

def _extract_variations_data(variations_data: List[Dict], products_map: Dict) -> List[Dict]:
    """Extract variations data from API response"""
    variations = []
    
    for i, variation_data in enumerate(variations_data):
        try:
            product_id = variation_data.get('product_id')
            variation_id = variation_data.get('id')
            
            if not product_id or not variation_id:
                continue
                
            if product_id not in products_map:
                continue
            
            variations.append({
                'product': products_map[product_id],
                'pancake_id': variation_id,
                'display_id': variation_data.get('display_id', ''),
                'barcode': variation_data.get('barcode'),
                'retail_price': variation_data.get('retail_price', 0),
                'retail_price_after_discount': variation_data.get('retail_price_after_discount', 0),
                'price_at_counter': variation_data.get('price_at_counter', 0),
                'total_purchase_price': variation_data.get('total_purchase_price', 0),
                'last_imported_price': variation_data.get('last_imported_price', 0),
                'wholesale_price': variation_data.get('wholesale_price', []),
                'remain_quantity': variation_data.get('remain_quantity', 0),
                'weight': variation_data.get('weight', 0),
                'is_composite': variation_data.get('is_composite', False),
                'is_hidden': variation_data.get('is_hidden', False),
                'is_locked': variation_data.get('is_locked', False),
                'is_removed': variation_data.get('is_removed'),
                'is_sell_negative_variation': variation_data.get('is_sell_negative_variation', False),
                'images': variation_data.get('images', []),
                'videos': variation_data.get('videos'),
                'composite_products': variation_data.get('composite_products', []),
                'bonus_variations': variation_data.get('bonus_variations', []),
                'variations_warehouses': variation_data.get('variations_warehouses', []),
                'inserted_at': _parse_datetime(variation_data.get('inserted_at')),
                'last_sync': timezone.now(),
                'fields_data': variation_data.get('fields', [])
            })
        except Exception as e:
            logger.error(f"Error extracting variation {i}: {e}")
            continue
    
    logger.info(f"Extracted {len(variations)} variations")
    return variations

def _extract_fields_data(variations_data: List[Dict]) -> List[Dict]:
    """Extract all variation fields data"""
    fields_dict = {}
    
    for variation_data in variations_data:
        for field_data in variation_data.get('fields', []):
            field_id = field_data.get('id')
            if not field_id or field_id in fields_dict:
                continue
                
            fields_dict[field_id] = {
                'pancake_id': field_id,
                'name': field_data.get('name', ''),
                'key_value': field_data.get('keyValue', ''),
                'value': field_data.get('value', ''),
            }
    
    logger.info(f"Extracted {len(fields_dict)} unique fields")
    return list(fields_dict.values())

# ===== BULK DATABASE OPERATIONS =====
def _bulk_upsert_products(products_data: List[Dict]) -> Tuple[int, int]:
    """Bulk create/update products"""
    if not products_data:
        return 0, 0
    
    shop = products_data[0]['shop']
    pancake_ids = [p['pancake_id'] for p in products_data]
    
    existing_products = {
        p.pancake_id: p for p in Product.objects.filter(
            pancake_id__in=pancake_ids, shop=shop
        )
    }
    
    products_to_create = []
    products_to_update = []
    m2m_data = []
    
    for product_data in products_data:
        pancake_id = product_data['pancake_id']
        category_ids = product_data.pop('category_ids', [])
        
        if pancake_id in existing_products:
            product = existing_products[pancake_id]
            for field, value in product_data.items():
                if field != 'shop':
                    setattr(product, field, value)
            products_to_update.append(product)
            m2m_data.append((product, category_ids))
        else:
            product = Product(**product_data)
            products_to_create.append(product)
            m2m_data.append((product, category_ids))
    
    created_count = 0
    if products_to_create:
        try:
            Product.objects.bulk_create(products_to_create, batch_size=30, ignore_conflicts=True)
            created_count = len(products_to_create)
            logger.info(f"Bulk created {created_count} products")
        except Exception as e:
            logger.error(f"Error bulk creating products: {e}")
    
    updated_count = 0
    if products_to_update:
        try:
            Product.objects.bulk_update(
                products_to_update,
                ['display_id', 'name', 'image_url', 'note_product', 'is_published', 
                 'tags', 'manipulation_warehouses', 'inserted_at', 'last_sync'],
                batch_size=30
            )
            updated_count = len(products_to_update)
            logger.info(f"Bulk updated {updated_count} products")
        except Exception as e:
            logger.error(f"Error bulk updating products: {e}")
    
    _handle_product_categories_m2m(m2m_data, shop)
    
    return created_count, updated_count

def _handle_product_categories_m2m(m2m_data: List[Tuple], shop: Shop):
    """Handle product-category M2M relationships"""
    for product, category_ids in m2m_data:
        if category_ids:
            try:
                if not product.pk:
                    try:
                        product = Product.objects.get(
                            shop=shop, 
                            pancake_id=product.pancake_id
                        )
                    except Product.DoesNotExist:
                        logger.warning(f"Product {product.pancake_id} not found in database")
                        continue
                
                categories = Category.objects.filter(shop=shop, pancake_id__in=category_ids)
                product.categories.set(categories)
                
            except Exception as e:
                logger.error(f"Error setting categories for product {product.pancake_id}: {e}")

def _bulk_upsert_variations(variations_data: List[Dict]) -> Tuple[int, int]:
    """Bulk create/update variations"""
    if not variations_data:
        return 0, 0
    
    pancake_ids = [v['pancake_id'] for v in variations_data]
    
    existing_variations = {
        v.pancake_id: v for v in ProductVariation.objects.filter(
            pancake_id__in=pancake_ids
        ).select_related('product')
    }
    
    variations_to_create = []
    variations_to_update = []
    
    for variation_data in variations_data:
        pancake_id = variation_data['pancake_id']
        variation_data.pop('fields_data', [])
        
        # Set default values
        variation_data.setdefault('is_composite', False)
        variation_data.setdefault('is_hidden', False)
        variation_data.setdefault('is_locked', False)
        variation_data.setdefault('is_sell_negative_variation', False)
        variation_data.setdefault('retail_price', 0)
        variation_data.setdefault('retail_price_after_discount', 0)
        variation_data.setdefault('remain_quantity', 0)
        variation_data.setdefault('weight', 0)
        
        if pancake_id in existing_variations:
            variation = existing_variations[pancake_id]
            for field, value in variation_data.items():
                if field != 'product':
                    setattr(variation, field, value)
            variations_to_update.append(variation)
        else:
            variations_to_create.append(ProductVariation(**variation_data))
    
    created_count = 0
    if variations_to_create:
        try:
            created_variations = ProductVariation.objects.bulk_create(
                variations_to_create, batch_size=30, ignore_conflicts=True
            )
            created_count = len(created_variations)
            logger.info(f"Bulk created {created_count} variations")
        except Exception as e:
            logger.error(f"Bulk create failed: {e}")
            # Fallback to individual creation
            created_count = 0
            for variation in variations_to_create:
                try:
                    ProductVariation.objects.create(**{
                        field.name: getattr(variation, field.name)
                        for field in variation._meta.fields
                        if hasattr(variation, field.name) and 
                           (getattr(variation, field.name) is not None or not field.has_default())
                    })
                    created_count += 1
                except Exception as individual_error:
                    logger.error(f"Failed to create variation {variation.pancake_id}: {individual_error}")
    
    updated_count = 0
    if variations_to_update:
        try:
            fields_to_update = [
                'display_id', 'barcode', 'retail_price', 'retail_price_after_discount',
                'price_at_counter', 'total_purchase_price', 'last_imported_price',
                'wholesale_price', 'remain_quantity', 'weight', 'is_composite',
                'is_hidden', 'is_locked', 'is_removed', 'is_sell_negative_variation',
                'images', 'videos', 'composite_products', 'bonus_variations',
                'variations_warehouses', 'inserted_at', 'last_sync'
            ]
            ProductVariation.objects.bulk_update(variations_to_update, fields_to_update, batch_size=30)
            updated_count = len(variations_to_update)
            logger.info(f"Bulk updated {updated_count} variations")
        except Exception as e:
            logger.error(f"Error bulk updating variations: {e}")
    
    return created_count, updated_count

def _bulk_upsert_fields(fields_data: List[Dict]) -> int:
    """Bulk create/update variation fields"""
    if not fields_data:
        return 0
    
    pancake_ids = [f['pancake_id'] for f in fields_data]
    
    existing_fields = {
        f.pancake_id: f for f in ProductVariationField.objects.filter(
            pancake_id__in=pancake_ids
        )
    }
    
    fields_to_create = []
    fields_to_update = []
    
    for field_data in fields_data:
        pancake_id = field_data['pancake_id']
        
        if pancake_id in existing_fields:
            field = existing_fields[pancake_id]
            for attr, value in field_data.items():
                setattr(field, attr, value)
            fields_to_update.append(field)
        else:
            fields_to_create.append(ProductVariationField(**field_data))
    
    created_count = 0
    if fields_to_create:
        try:
            ProductVariationField.objects.bulk_create(fields_to_create, batch_size=30, ignore_conflicts=True)
            created_count = len(fields_to_create)
            logger.info(f"Bulk created {created_count} fields")
        except Exception as e:
            logger.error(f"Error bulk creating fields: {e}")
    
    if fields_to_update:
        try:
            ProductVariationField.objects.bulk_update(
                fields_to_update, ['name', 'key_value', 'value'], batch_size=30
            )
            logger.info(f"Bulk updated {len(fields_to_update)} fields")
        except Exception as e:
            logger.error(f"Error bulk updating fields: {e}")
    
    return created_count

def _handle_variation_fields_m2m(variations_data: List[Dict]):
    """Handle variation-fields M2M relationships"""
    logger.info(f"Processing M2M for {len(variations_data)} variations")
    
    for variation_data in variations_data:
        variation_id = variation_data['pancake_id']
        fields_data = variation_data.get('fields_data', [])
        
        if not fields_data:
            continue
            
        try:
            variation = ProductVariation.objects.get(pancake_id=variation_id)
            field_ids = [f.get('id') for f in fields_data if f.get('id')]
            fields = ProductVariationField.objects.filter(pancake_id__in=field_ids)
            variation.fields.set(fields)
        except ProductVariation.DoesNotExist:
            logger.warning(f"Variation {variation_id} not found for M2M setup")
            continue
        except Exception as e:
            logger.error(f"Error setting M2M for variation {variation_id}: {e}")

# ===== SYNC SHOP FUNCTION =====
def _sync_shop_products(shop: Shop) -> ProductSyncResult:
    """Sync all products for a single shop"""
    result = ProductSyncResult()
    
    try:
        page = 1
        total_pages = 1
        processed_pages = 0
        
        logger.info(f"Starting sync for shop: {shop.name} (ID: {shop.pancake_id})")
        
        while page <= total_pages:
            try:
                # Fetch data
                api_response = _fetch_product_variations_page(shop.pancake_id, page, 30)
                
                if not api_response.get('success', False):
                    error_msg = f"API returned success=false for shop {shop.name} page {page}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)
                    break
                
                total_pages = api_response.get('total_pages', 1)
                variations_data = api_response.get('data', [])
                
                logger.info(f"Shop {shop.name} - Page {page}/{total_pages}: {len(variations_data)} variations")
                
                if not variations_data:
                    logger.warning(f"No data for shop {shop.name} page {page}")
                    page += 1
                    continue
                
                # Extract and transform data
                products_data = _extract_products_data(variations_data, shop)
                fields_data = _extract_fields_data(variations_data)
                
                # Bulk upsert operations
                products_created, products_updated = _bulk_upsert_products(products_data)
                fields_created = _bulk_upsert_fields(fields_data)
                
                # Create products map for variations
                product_ids = [pd['pancake_id'] for pd in products_data if pd.get('pancake_id')]
                products_map = {
                    p.pancake_id: p for p in Product.objects.filter(
                        shop=shop, pancake_id__in=product_ids
                    )
                }
                
                # Extract and upsert variations
                variations_data_processed = _extract_variations_data(variations_data, products_map)
                variations_created, variations_updated = _bulk_upsert_variations(variations_data_processed)
                
                # Handle M2M relationships
                _handle_variation_fields_m2m(variations_data_processed)
                
                # Aggregate results
                result.products_created += products_created
                result.products_updated += products_updated
                result.variations_created += variations_created
                result.variations_updated += variations_updated
                result.fields_created += fields_created
                
                processed_pages += 1
                logger.info(f"Completed page {page}/{total_pages} for shop {shop.name}")
                
                # Small delay between pages
                time.sleep(0.5)
                
            except Exception as page_error:
                error_msg = f"Error processing page {page} for shop {shop.name}: {str(page_error)}"
                logger.error(error_msg, exc_info=True)
                result.errors.append(error_msg)
            
            page += 1
        
        logger.info(f"Completed sync for shop {shop.name}: {processed_pages}/{total_pages} pages processed")
            
    except requests.RequestException as e:
        error_msg = f"Shop {shop.name}: Network error - {str(e)}"
        logger.error(error_msg)
        result.errors.append(error_msg)
    except Exception as e:
        error_msg = f"Shop {shop.name}: Unexpected error - {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.errors.append(error_msg)
    
    return result

# ===== CELERY TASKS =====

@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def sync_single_shop_products(self, shop_id: int):
    """Sync products for a single shop - can be used independently"""
    try:
        shop = Shop.objects.get(id=shop_id)
        logger.info(f"Starting product sync for shop: {shop.name}")
        
        result = _sync_shop_products(shop)
        
        return {
            'success': len(result.errors) == 0,
            'shop_id': shop_id,
            'shop_name': shop.name,
            'products_created': result.products_created,
            'products_updated': result.products_updated,
            'variations_created': result.variations_created,
            'variations_updated': result.variations_updated,
            'fields_created': result.fields_created,
            'errors': result.errors,
            'completed_at': _get_vietnam_time().isoformat()
        }
        
    except Shop.DoesNotExist:
        logger.error(f"Shop with id {shop_id} not found")
        return {
            'success': False,
            'error': f'Shop with id {shop_id} not found',
            'shop_id': shop_id
        }
    except Exception as e:
        logger.error(f"Error syncing products for shop {shop_id}: {e}", exc_info=True)
        
        # Retry logic
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying sync for shop {shop_id} (attempt {self.request.retries + 1})")
            raise self.retry(exc=e)
        
        return {
            'success': False,
            'error': str(e),
            'shop_id': shop_id,
            'retries_exhausted': True
        }

@shared_task(bind=True)
def sync_all_products(self):
    """
    Main task to sync products for all shops
    Scheduled to run every 4 hours via Celery Beat
    """
    vietnam_start = _get_vietnam_time()
    logger.info(f"Starting scheduled product sync at {vietnam_start}")
    
    try:
        # Create sync history record
        sync_history = SyncHistory.objects.create(
            sync_type='products_scheduled',
            status='running',
            started_at=vietnam_start,
            total_records=0
        )
        
        shops = Shop.objects.all()
        total_result = ProductSyncResult()
        shop_results = []
        
        logger.info(f"Found {shops.count()} shops to sync")
        
        for shop in shops:
            try:
                logger.info(f"Processing products for shop: {shop.name}")
                shop_result = _sync_shop_products(shop)
                
                # Aggregate results
                total_result.products_created += shop_result.products_created
                total_result.products_updated += shop_result.products_updated
                total_result.variations_created += shop_result.variations_created
                total_result.variations_updated += shop_result.variations_updated
                total_result.fields_created += shop_result.fields_created
                total_result.errors.extend(shop_result.errors)
                
                shop_results.append({
                    'shop_id': shop.id,
                    'shop_name': shop.name,
                    'products_created': shop_result.products_created,
                    'products_updated': shop_result.products_updated,
                    'variations_created': shop_result.variations_created,
                    'variations_updated': shop_result.variations_updated,
                    'fields_created': shop_result.fields_created,
                    'errors': shop_result.errors
                })
                
                logger.info(f"Shop {shop.name} completed: "
                           f"{shop_result.products_created + shop_result.products_updated} products, "
                           f"{shop_result.variations_created + shop_result.variations_updated} variations")
                
            except Exception as e:
                error_msg = f"Error processing shop {shop.name}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                total_result.errors.append(error_msg)
                shop_results.append({
                    'shop_id': shop.id,
                    'shop_name': shop.name,
                    'error': error_msg
                })
        
        vietnam_end = _get_vietnam_time()
        duration = (vietnam_end - vietnam_start).total_seconds()
        
        # Update sync history
        sync_history.status = 'completed' if not total_result.errors else 'failed'
        sync_history.created_records = total_result.products_created
        sync_history.updated_records = total_result.products_updated
        sync_history.failed_records = len(total_result.errors)
        sync_history.finished_at = vietnam_end
        sync_history.total_records = total_result.products_created + total_result.products_updated
        sync_history.error_message = '; '.join(total_result.errors[:3]) if total_result.errors else None
        sync_history.error_details = {
            'total_errors': len(total_result.errors),
            'errors': total_result.errors[:10],
            'shop_results': shop_results
        }
        sync_history.save()
        
        logger.info(f"Product sync completed in {duration:.2f}s: "
                   f"{total_result.products_created} products created, "
                   f"{total_result.products_updated} products updated, "
                   f"{total_result.variations_created} variations created, "
                   f"{total_result.variations_updated} variations updated")
        
        return {
            'success': len(total_result.errors) == 0,
            'duration_seconds': duration,
            'products_created': total_result.products_created,
            'products_updated': total_result.products_updated,
            'variations_created': total_result.variations_created,
            'variations_updated': total_result.variations_updated,
            'fields_created': total_result.fields_created,
            'shops_processed': len(shop_results),
            'errors_count': len(total_result.errors),
            'errors': total_result.errors[:10],  # Only return first 10 errors
            'shop_results': shop_results,
            'completed_at': vietnam_end.isoformat(),
            'sync_history_id': sync_history.id
        }
        
    except Exception as e:
        vietnam_error = _get_vietnam_time()
        logger.error(f"Critical error in scheduled product sync: {e}", exc_info=True)
        
        # Update sync history for critical failure
        try:
            sync_history.status = 'failed'
            sync_history.error_message = f"Critical error: {str(e)}"
            sync_history.finished_at = vietnam_error
            sync_history.save()
        except:
            pass
        
        return {
            'success': False,
            'error': f'Critical error in sync: {str(e)}',
            'completed_at': vietnam_error.isoformat()
        }

@shared_task
def cleanup_old_sync_histories():
    """Clean up old sync history records (keep last 100 records)"""
    try:
        # Keep only the latest 100 records for each sync type
        sync_types = ['products_scheduled', 'products_manual']
        
        for sync_type in sync_types:
            histories = SyncHistory.objects.filter(
                sync_type=sync_type
            ).order_by('-started_at')
            
            if histories.count() > 100:
                old_histories = histories[100:]
                old_ids = [h.id for h in old_histories]
                deleted_count = SyncHistory.objects.filter(id__in=old_ids).delete()[0]
                logger.info(f"Cleaned up {deleted_count} old {sync_type} sync histories")
        
        return {'success': True, 'message': 'Cleanup completed'}
        
    except Exception as e:
        logger.error(f"Error in cleanup task: {e}")
        return {'success': False, 'error': str(e)}
    

from celery import shared_task
from django.utils import timezone
from django.conf import settings
from django.db import transaction
import requests
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from shops.models import *
import logging
from decimal import Decimal
from datetime import datetime, timedelta
import pytz
import time

VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
logger = logging.getLogger(__name__)
@dataclass
class CustomerSyncResult:
    users_created: int = 0
    users_updated: int = 0
    customers_created: int = 0
    customers_updated: int = 0
    addresses_created: int = 0
    addresses_updated: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

# ===== UTILITY FUNCTIONS =====
def _get_vietnam_time(dt=None):
    """Get current time in Vietnam timezone"""
    if dt is None:
        return timezone.now().astimezone(VIETNAM_TZ)
    
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    
    return dt.astimezone(VIETNAM_TZ)

def _parse_datetime(datetime_str: Optional[str]) -> Optional[timezone.datetime]:
    """Parse datetime string from API with proper timezone handling"""
    if not datetime_str:
        return None
    
    try:
        if datetime_str.endswith('Z'):
            dt = timezone.datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        elif '+' in datetime_str or '-' in datetime_str[-6:]:
            dt = timezone.datetime.fromisoformat(datetime_str)
        else:
            dt = timezone.datetime.fromisoformat(datetime_str + '+00:00')
        
        return dt if dt else None
        
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse datetime '{datetime_str}': {e}")
        return None

def _parse_date(date_str: Optional[str]) -> Optional[timezone.datetime.date]:
    """Parse date string from API"""
    if not date_str:
        return None
    try:
        return timezone.datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
    except (ValueError, TypeError):
        return None

def _send_sync_notification(task_name: str, result: dict, is_success: bool = True):
    """Send email notification about sync result"""
    try:
        if not hasattr(settings, 'SYNC_NOTIFICATION_EMAILS') or not settings.SYNC_NOTIFICATION_EMAILS:
            return
        
        from django.core.mail import send_mail
        from django.template.loader import render_to_string
        
        subject = f"[{'SUCCESS' if is_success else 'ERROR'}] {task_name} - {_get_vietnam_time().strftime('%Y-%m-%d %H:%M:%S')}"
        
        context = {
            'task_name': task_name,
            'result': result,
            'is_success': is_success,
            'sync_time': _get_vietnam_time(),
        }
        
        message = render_to_string('emails/sync_notification.html', context)
        
        send_mail(
            subject=subject,
            message=message,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=settings.SYNC_NOTIFICATION_EMAILS,
            html_message=message,
            fail_silently=True
        )
        
    except Exception as e:
        logger.error(f"Failed to send sync notification: {e}")

# ===== API FUNCTIONS =====
def _fetch_customers_page(shop_id: int, page: int = 1, page_size: int = 50, 
                         start_time_updated_at: Optional[datetime] = None,
                         end_time_updated_at: Optional[datetime] = None) -> Dict:
    """
    Fetch customers page with updated date range filtering
    
    Args:
        shop_id: Shop ID
        page: Page number (default 1)
        page_size: Page size (default 50)
        start_time_updated_at: Start time for updated_at filter (optional)
        end_time_updated_at: End time for updated_at filter (optional)
    """
    api_url = f"{settings.PANCAKE_API_BASE_URL}/shops/{shop_id}/customers"
    params = {
        'api_key': settings.PANCAKE_API_KEY,
        'page_number': page,  # Updated parameter name
        'page_size': page_size,
    }
    
    # Add date range filters if provided
    if start_time_updated_at:
        # Convert to unix timestamp
        start_unix = int(start_time_updated_at.timestamp())
        params['start_time_updated_at'] = start_unix
        logger.info(f"Setting start_time_updated_at: {start_unix} ({start_time_updated_at})")
    
    if end_time_updated_at:
        # Convert to unix timestamp  
        end_unix = int(end_time_updated_at.timestamp())
        params['end_time_updated_at'] = end_unix
        logger.info(f"Setting end_time_updated_at: {end_unix} ({end_time_updated_at})")
    
    logger.info(f"Fetching customers for shop {shop_id}, page {page} with page_size {page_size}")
    if start_time_updated_at or end_time_updated_at:
        logger.info(f"Date range filter: {start_time_updated_at} to {end_time_updated_at}")
    
    try:
        response = requests.get(api_url, params=params, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"API response: success={data.get('success')}, page={data.get('page_number')}, "
                    f"total_pages={data.get('total_pages')}, data_count={len(data.get('data', []))}")
        
        return data
    except requests.RequestException as e:
        logger.error(f"API request failed for shop {shop_id} page {page}: {e}")
        raise

# ===== DATA EXTRACTION FUNCTIONS =====
def _extract_users_data(customers_data: List[Dict]) -> List[Dict]:
    """Extract unique users (creators/assigned users) from customers response"""
    users_dict = {}
    
    for customer_data in customers_data:
        # Extract creator
        creator_data = customer_data.get('creator')
        if creator_data and creator_data.get('id'):
            user_id = creator_data['id']
            if user_id not in users_dict:
                users_dict[user_id] = {
                    'pancake_id': user_id,
                    'name': creator_data.get('name', ''),
                    'avatar_url': creator_data.get('avatar_url'),
                    'fb_id': creator_data.get('fb_id'),
                    'phone_number': creator_data.get('phone_number'),
                    'last_sync': _get_vietnam_time(),
                }
        
        # Extract assigned user
        assigned_user_id = customer_data.get('assigned_user_id')
        if assigned_user_id and assigned_user_id not in users_dict:
            users_dict[assigned_user_id] = {
                'pancake_id': assigned_user_id,
                'name': f'User {assigned_user_id}',
                'avatar_url': None,
                'fb_id': None,
                'phone_number': None,
                'last_sync': _get_vietnam_time(),
            }
    
    logger.info(f"Extracted {len(users_dict)} unique users")
    return list(users_dict.values())

def _extract_customers_data(customers_data: List[Dict], shop, users_map: Dict) -> List[Dict]:
    """Extract customers data from API response"""
    customers = []
    vietnam_now = _get_vietnam_time()
    
    logger.info(f"Processing {len(customers_data)} customers for shop {shop.name}")
    
    for i, customer_data in enumerate(customers_data):
        try:
            customer_id = customer_data.get('id')
            if not customer_id:
                logger.warning(f"Customer {i} missing id")
                continue
            
            # Get creator and assigned user
            creator = None
            assigned_user = None
            
            creator_data = customer_data.get('creator')
            if creator_data and creator_data.get('id'):
                creator = users_map.get(creator_data['id'])
            
            assigned_user_id = customer_data.get('assigned_user_id')
            if assigned_user_id:
                assigned_user = users_map.get(assigned_user_id)
            
            customers.append({
                'shop': shop,
                'pancake_id': customer_id,
                'customer_id': customer_data.get('customer_id', ''),
                'name': customer_data.get('name', ''),
                'username': customer_data.get('username'),
                'gender': customer_data.get('gender'),
                'date_of_birth': _parse_date(customer_data.get('date_of_birth')),
                'phone_numbers': customer_data.get('phone_numbers', []),
                'emails': customer_data.get('emails', []),
                'fb_id': customer_data.get('fb_id'),
                'current_debts': customer_data.get('current_debts', 0),
                'purchased_amount': customer_data.get('purchased_amount', 0),
                'total_amount_referred': customer_data.get('total_amount_referred'),
                'reward_point': customer_data.get('reward_point', 0),
                'used_reward_point': customer_data.get('used_reward_point'),
                'order_count': customer_data.get('order_count', 0),
                'succeed_order_count': customer_data.get('succeed_order_count', 0),
                'returned_order_count': customer_data.get('returned_order_count', 0),
                'last_order_at': _parse_datetime(customer_data.get('last_order_at')),
                'referral_code': customer_data.get('referral_code'),
                'count_referrals': customer_data.get('count_referrals', 0),
                'is_block': customer_data.get('is_block', False),
                'is_discount_by_level': customer_data.get('is_discount_by_level', True),
                'is_adjust_debts': customer_data.get('is_adjust_debts'),
                'active_levera_pay': customer_data.get('active_levera_pay', False),
                'creator': creator,
                'assigned_user': assigned_user,
                'level': customer_data.get('level'),
                'currency': customer_data.get('currency'),
                'user_block_id': customer_data.get('user_block_id'),
                'conversation_tags': customer_data.get('conversation_tags'),
                'order_sources': customer_data.get('order_sources', []),
                'tags': customer_data.get('tags', []),
                'list_voucher': customer_data.get('list_voucher', []),
                'notes': customer_data.get('notes', []),
                'inserted_at': _parse_datetime(customer_data.get('inserted_at')),
                'updated_at_api': _parse_datetime(customer_data.get('updated_at')),
                'last_sync': vietnam_now,
                'addresses_data': customer_data.get('shop_customer_addresses', [])
            })
        except Exception as e:
            logger.error(f"Error extracting customer {i}: {e}")
            continue
    
    logger.info(f"Extracted {len(customers)} customers")
    return customers

def _extract_addresses_data(customers_data: List[Dict]) -> List[Dict]:
    """Extract all customer addresses"""
    addresses = []
    vietnam_now = _get_vietnam_time()
    
    for customer_data in customers_data:
        customer_id = customer_data.get('id')
        if not customer_id:
            continue
            
        for address_data in customer_data.get('shop_customer_addresses', []):
            address_id = address_data.get('id')
            if not address_id:
                continue
                
            addresses.append({
                'customer_pancake_id': customer_id,
                'pancake_id': address_id,
                'full_name': address_data.get('full_name', ''),
                'phone_number': address_data.get('phone_number', ''),
                'address': address_data.get('address', ''),
                'full_address': address_data.get('full_address', ''),
                'post_code': address_data.get('post_code'),
                'country_code': address_data.get('country_code', 84),
                'province_id': address_data.get('province_id', ''),
                'district_id': address_data.get('district_id', ''),
                'commune_id': address_data.get('commune_id', ''),
                'last_sync': vietnam_now,
            })
    
    logger.info(f"Extracted {len(addresses)} addresses")
    return addresses

# ===== BULK DATABASE OPERATIONS =====
def _bulk_upsert_users(users_data: List[Dict]) -> Tuple[int, int]:
    """Bulk create/update users"""
    if not users_data:
        return 0, 0
    
    from shops.models import User  # Import your User model
    
    pancake_ids = [u['pancake_id'] for u in users_data]
    
    existing_users = {
        u.pancake_id: u for u in User.objects.filter(pancake_id__in=pancake_ids)
    }
    
    users_to_create = []
    users_to_update = []
    
    for user_data in users_data:
        pancake_id = user_data['pancake_id']
        
        if pancake_id in existing_users:
            user = existing_users[pancake_id]
            for field, value in user_data.items():
                setattr(user, field, value)
            users_to_update.append(user)
        else:
            users_to_create.append(User(**user_data))
    
    created_count = 0
    if users_to_create:
        try:
            User.objects.bulk_create(users_to_create, batch_size=50, ignore_conflicts=True)
            created_count = len(users_to_create)
            logger.info(f"Bulk created {created_count} users")
        except Exception as e:
            logger.error(f"Error bulk creating users: {e}")
    
    updated_count = 0
    if users_to_update:
        try:
            User.objects.bulk_update(
                users_to_update,
                ['name', 'avatar_url', 'fb_id', 'phone_number', 'last_sync'],
                batch_size=50
            )
            updated_count = len(users_to_update)
            logger.info(f"Bulk updated {updated_count} users")
        except Exception as e:
            logger.error(f"Error bulk updating users: {e}")
    
    return created_count, updated_count

def _bulk_upsert_customers(customers_data: List[Dict]) -> Tuple[int, int]:
    """Bulk create/update customers"""
    if not customers_data:
        return 0, 0
    
    from shops.models import Customer  # Import your Customer model
    
    shop = customers_data[0]['shop']
    pancake_ids = [c['pancake_id'] for c in customers_data]
    
    existing_customers = {
        c.pancake_id: c for c in Customer.objects.filter(
            pancake_id__in=pancake_ids, shop=shop
        )
    }
    
    customers_to_create = []
    customers_to_update = []
    
    for customer_data in customers_data:
        pancake_id = customer_data['pancake_id']
        customer_data.pop('addresses_data', [])
        
        if pancake_id in existing_customers:
            customer = existing_customers[pancake_id]
            for field, value in customer_data.items():
                if field != 'shop':
                    setattr(customer, field, value)
            customers_to_update.append(customer)
        else:
            customers_to_create.append(Customer(**customer_data))
    
    created_count = 0
    if customers_to_create:
        try:
            Customer.objects.bulk_create(customers_to_create, batch_size=50, ignore_conflicts=True)
            created_count = len(customers_to_create)
            logger.info(f"Bulk created {created_count} customers")
        except Exception as e:
            logger.error(f"Error bulk creating customers: {e}")
    
    updated_count = 0
    if customers_to_update:
        try:
            fields_to_update = [
                'customer_id', 'name', 'username', 'gender', 'date_of_birth',
                'phone_numbers', 'emails', 'fb_id', 'current_debts', 'purchased_amount',
                'total_amount_referred', 'reward_point', 'used_reward_point',
                'order_count', 'succeed_order_count', 'returned_order_count', 'last_order_at',
                'referral_code', 'count_referrals', 'is_block', 'is_discount_by_level',
                'is_adjust_debts', 'active_levera_pay', 'creator', 'assigned_user',
                'level', 'currency', 'user_block_id', 'conversation_tags',
                'order_sources', 'tags', 'list_voucher', 'notes',
                'inserted_at', 'updated_at_api', 'last_sync'
            ]
            Customer.objects.bulk_update(customers_to_update, fields_to_update, batch_size=50)
            updated_count = len(customers_to_update)
            logger.info(f"Bulk updated {updated_count} customers")
        except Exception as e:
            logger.error(f"Error bulk updating customers: {e}")
    
    return created_count, updated_count

def _bulk_upsert_addresses(addresses_data: List[Dict], customers_map: Dict) -> Tuple[int, int]:
    """Bulk create/update customer addresses"""
    if not addresses_data:
        return 0, 0
    
    from shops.models import CustomerAddress  # Import your CustomerAddress model
    
    valid_addresses = []
    for address_data in addresses_data:
        customer_id = address_data['customer_pancake_id']
        if customer_id in customers_map:
            address_data['customer'] = customers_map[customer_id]
            address_data.pop('customer_pancake_id')
            valid_addresses.append(address_data)
    
    if not valid_addresses:
        return 0, 0
    
    pancake_ids = [a['pancake_id'] for a in valid_addresses]
    
    existing_addresses = {
        a.pancake_id: a for a in CustomerAddress.objects.filter(
            pancake_id__in=pancake_ids
        ).select_related('customer')
    }
    
    addresses_to_create = []
    addresses_to_update = []
    
    for address_data in valid_addresses:
        pancake_id = address_data['pancake_id']
        
        if pancake_id in existing_addresses:
            address = existing_addresses[pancake_id]
            for field, value in address_data.items():
                if field != 'customer':
                    setattr(address, field, value)
            addresses_to_update.append(address)
        else:
            addresses_to_create.append(CustomerAddress(**address_data))
    
    created_count = 0
    if addresses_to_create:
        try:
            CustomerAddress.objects.bulk_create(addresses_to_create, batch_size=50, ignore_conflicts=True)
            created_count = len(addresses_to_create)
            logger.info(f"Bulk created {created_count} addresses")
        except Exception as e:
            logger.error(f"Error bulk creating addresses: {e}")
    
    updated_count = 0
    if addresses_to_update:
        try:
            fields_to_update = [
                'full_name', 'phone_number', 'address', 'full_address', 'post_code',
                'country_code', 'province_id', 'district_id', 'commune_id', 'last_sync'
            ]
            CustomerAddress.objects.bulk_update(addresses_to_update, fields_to_update, batch_size=50)
            updated_count = len(addresses_to_update)
            logger.info(f"Bulk updated {updated_count} addresses")
        except Exception as e:
            logger.error(f"Error bulk updating addresses: {e}")
    
    return created_count, updated_count

# ===== SYNC FUNCTIONS =====
def _sync_shop_customers(shop, start_time_updated_at: Optional[datetime] = None,
                        end_time_updated_at: Optional[datetime] = None) -> CustomerSyncResult:
    """
    Sync customers for a single shop with date range filtering
    
    Args:
        shop: Shop instance
        start_time_updated_at: Start time for updated_at filter (optional)
        end_time_updated_at: End time for updated_at filter (optional)
    """
    result = CustomerSyncResult()
    
    try:
        page = 1
        total_pages = 1
        processed_pages = 0
        
        # Determine sync type for logging
        if start_time_updated_at or end_time_updated_at:
            sync_type = "date_range"
            logger.info(f"Starting date range customer sync for shop: {shop.name} (ID: {shop.pancake_id})")
            logger.info(f"Date range: {start_time_updated_at} to {end_time_updated_at}")
        else:
            sync_type = "full"
            logger.info(f"Starting full customer sync for shop: {shop.name} (ID: {shop.pancake_id})")
        
        while page <= total_pages:
            try:
                # Fetch data with date range filter
                api_response = _fetch_customers_page(
                    shop.pancake_id, 
                    page, 
                    50,
                    start_time_updated_at=start_time_updated_at,
                    end_time_updated_at=end_time_updated_at
                )
                
                if not api_response.get('success', False):
                    error_msg = f"API returned success=false for shop {shop.name} page {page}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)
                    break
                
                total_pages = api_response.get('total_pages', 1)
                customers_data = api_response.get('data', [])
                
                logger.info(f"Shop {shop.name} - Page {page}/{total_pages}: {len(customers_data)} customers")
                
                if not customers_data:
                    logger.info(f"No data for shop {shop.name} page {page}")
                    page += 1
                    continue
                
                # Extract and transform data
                users_data = _extract_users_data(customers_data)
                
                # Bulk upsert users first
                users_created, users_updated = _bulk_upsert_users(users_data)
                
                # Create users map for customers
                from shops.models import User
                user_ids = [ud['pancake_id'] for ud in users_data]
                users_map = {
                    u.pancake_id: u for u in User.objects.filter(pancake_id__in=user_ids)
                }
                
                # Extract and upsert customers
                customers_data_processed = _extract_customers_data(customers_data, shop, users_map)
                customers_created, customers_updated = _bulk_upsert_customers(customers_data_processed)
                
                # Create customers map for addresses
                from shops.models import Customer
                customer_ids = [cd['pancake_id'] for cd in customers_data_processed]
                customers_map = {
                    c.pancake_id: c for c in Customer.objects.filter(
                        shop=shop, pancake_id__in=customer_ids
                    )
                }
                
                # Extract and upsert addresses
                addresses_data = _extract_addresses_data(customers_data)
                addresses_created, addresses_updated = _bulk_upsert_addresses(addresses_data, customers_map)
                
                # Aggregate results
                result.users_created += users_created
                result.users_updated += users_updated
                result.customers_created += customers_created
                result.customers_updated += customers_updated
                result.addresses_created += addresses_created
                result.addresses_updated += addresses_updated
                                
                processed_pages += 1
                logger.info(f"Completed page {page}/{total_pages} for shop {shop.name}")
                
                # Small delay between pages
                time.sleep(0.5)
                
            except Exception as page_error:
                error_msg = f"Error processing page {page} for shop {shop.name}: {str(page_error)}"
                logger.error(error_msg, exc_info=True)
                result.errors.append(error_msg)
            
            page += 1
        
        logger.info(f"Completed {sync_type} customer sync for shop {shop.name}: {processed_pages}/{total_pages} pages processed")
            
    except requests.RequestException as e:
        error_msg = f"Shop {shop.name}: Network error - {str(e)}"
        logger.error(error_msg)
        result.errors.append(error_msg)
    except Exception as e:
        error_msg = f"Shop {shop.name}: Unexpected error - {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.errors.append(error_msg)
    
    return result

# ===== CELERY TASKS =====

@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def sync_single_shop_customers_30_days(self, shop_id: int):
    """
    Sync customers for a single shop for the last 30 days
    
    Args:
        shop_id: Shop ID to sync
    """
    try:
        from shops.models import Shop
        shop = Shop.objects.get(id=shop_id)
        vietnam_now = _get_vietnam_time()
        
        # Calculate date range: today to 30 days ago
        end_time_updated_at = vietnam_now
        start_time_updated_at = vietnam_now - timedelta(days=30)
        
        logger.info(f"30-day sync for shop {shop.name} - customers updated from {start_time_updated_at} to {end_time_updated_at}")
        
        result = _sync_shop_customers(
            shop, 
            start_time_updated_at=start_time_updated_at,
            end_time_updated_at=end_time_updated_at
        )
        
        vietnam_end = _get_vietnam_time()
        
        return {
            'success': len(result.errors) == 0,
            'shop_id': shop_id,
            'shop_name': shop.name,
            'sync_type': '30_days',
            'date_range': {
                'start': start_time_updated_at.isoformat(),
                'end': end_time_updated_at.isoformat()
            },
            'users_created': result.users_created,
            'users_updated': result.users_updated,
            'customers_created': result.customers_created,
            'customers_updated': result.customers_updated,
            'addresses_created': result.addresses_created,
            'addresses_updated': result.addresses_updated,
            'errors': result.errors,
            'started_at': vietnam_now.isoformat(),
            'completed_at': vietnam_end.isoformat(),
            'duration_seconds': (vietnam_end - vietnam_now).total_seconds()
        }
        
    except Exception as e:
        logger.error(f"Error syncing customers for shop {shop_id}: {e}", exc_info=True)
        
        # Retry logic
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying customer sync for shop {shop_id} (attempt {self.request.retries + 1})")
            raise self.retry(exc=e)
        
        return {
            'success': False,
            'error': str(e),
            'shop_id': shop_id,
            'retries_exhausted': True
        }

@shared_task(bind=True)
def sync_all_customers_30_days(self):
    """
    Sync customers for all shops for the last 30 days
    """
    task_name = "30-Day Customer Sync"
    vietnam_start = _get_vietnam_time()
    
    try:
        from shops.models import Shop, SyncHistory
        
        logger.info(f"Starting {task_name} at {vietnam_start}")
        
        # Create sync history record
        sync_history = SyncHistory.objects.create(
            sync_type='customers_30_days',
            status='running',
            started_at=vietnam_start,
            total_records=0
        )
        
        shops = Shop.objects.all()
        total_result = CustomerSyncResult()
        shop_results = []
        
        # Calculate date range: today to 30 days ago
        end_time_updated_at = vietnam_start
        start_time_updated_at = vietnam_start - timedelta(days=30)
        
        logger.info(f"Found {shops.count()} shops to sync customers (30 days: {start_time_updated_at} to {end_time_updated_at})")
        
        for shop in shops:
            try:
                logger.info(f"Processing 30-day customers for shop: {shop.name}")
                shop_result = _sync_shop_customers(
                    shop, 
                    start_time_updated_at=start_time_updated_at,
                    end_time_updated_at=end_time_updated_at
                )
                
                # Aggregate results
                total_result.users_created += shop_result.users_created
                total_result.users_updated += shop_result.users_updated
                total_result.customers_created += shop_result.customers_created
                total_result.customers_updated += shop_result.customers_updated
                total_result.addresses_created += shop_result.addresses_created
                total_result.addresses_updated += shop_result.addresses_updated
                total_result.errors.extend(shop_result.errors)
                
                shop_results.append({
                    'shop_id': shop.id,
                    'shop_name': shop.name,
                    'users_created': shop_result.users_created,
                    'users_updated': shop_result.users_updated,
                    'customers_created': shop_result.customers_created,
                    'customers_updated': shop_result.customers_updated,
                    'addresses_created': shop_result.addresses_created,
                    'addresses_updated': shop_result.addresses_updated,
                    'errors': shop_result.errors
                })
                
                logger.info(f"Shop {shop.name} completed: "
                           f"{shop_result.customers_created + shop_result.customers_updated} customers, "
                           f"{shop_result.addresses_created + shop_result.addresses_updated} addresses")
                
            except Exception as e:
                error_msg = f"Error processing shop {shop.name}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                total_result.errors.append(error_msg)
                shop_results.append({
                    'shop_id': shop.id,
                    'shop_name': shop.name,
                    'error': error_msg
                })
        
        vietnam_end = _get_vietnam_time()
        duration = (vietnam_end - vietnam_start).total_seconds()
        
        # Update sync history
        sync_history.status = 'completed' if not total_result.errors else 'failed'
        sync_history.created_records = total_result.customers_created
        sync_history.updated_records = total_result.customers_updated
        sync_history.failed_records = len(total_result.errors)
        sync_history.finished_at = vietnam_end
        sync_history.total_records = total_result.customers_created + total_result.customers_updated
        sync_history.error_message = '; '.join(total_result.errors[:3]) if total_result.errors else None
        sync_history.error_details = {
            'total_errors': len(total_result.errors),
            'errors': total_result.errors[:10],
            'shop_results': shop_results,
            'sync_type': '30_days',
            'date_range': {
                'start': start_time_updated_at.isoformat(),
                'end': end_time_updated_at.isoformat()
            }
        }
        sync_history.save()
        
        # Create summary
        summary = {
            'task_id': self.request.id,
            'start_time': vietnam_start.isoformat(),
            'end_time': vietnam_end.isoformat(),
            'duration_seconds': duration,
            'sync_type': '30_days',
            'date_range': {
                'start': start_time_updated_at.isoformat(),
                'end': end_time_updated_at.isoformat()
            },
            'shops_processed': len(shop_results),
            'users_created': total_result.users_created,
            'users_updated': total_result.users_updated,
            'customers_created': total_result.customers_created,
            'customers_updated': total_result.customers_updated,
            'addresses_created': total_result.addresses_created,
            'addresses_updated': total_result.addresses_updated,
            'total_errors': len(total_result.errors),
            'error_details': total_result.errors[:10],
            'success': len(total_result.errors) == 0,
            'sync_history_id': sync_history.id
        }
        
        # Log results
        if summary['success']:
            logger.info(f"{task_name} completed successfully: {summary}")
        else:
            logger.warning(f"{task_name} completed with errors: {summary}")
        
        # Send notification

        
        return summary
        
    except Exception as exc:
        vietnam_error = _get_vietnam_time()
        error_summary = {
            'task_id': self.request.id,
            'error_time': vietnam_error.isoformat(),
            'error_message': str(exc),
            'sync_type': '30_days',
            'retry_count': self.request.retries,
            'max_retries': self.max_retries
        }
        
        logger.error(f"Error in {task_name} task: {error_summary}", exc_info=True)
        
        # Update sync history for critical failure
        try:
            sync_history.status = 'failed'
            sync_history.error_message = f"Critical error: {str(exc)}"
            sync_history.finished_at = vietnam_error
            sync_history.save()
        except:
            pass
        
        # Retry if not exhausted
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying {task_name} task in 5 minutes...")
            raise self.retry(exc=exc, countdown=300)
        
        # Send error notification

        
        raise exc

@shared_task(bind=True)
def sync_all_customers_full(self):
    """
    Full customer sync for all shops (no date filter)
    Should be run weekly or when needed
    """
    task_name = "Full Customer Sync"
    vietnam_start = _get_vietnam_time()
    
    try:
        from shops.models import Shop, SyncHistory
        
        logger.info(f"Starting {task_name} at {vietnam_start}")
        
        # Create sync history record
        sync_history = SyncHistory.objects.create(
            sync_type='customers_full',
            status='running',
            started_at=vietnam_start,
            total_records=0
        )
        
        shops = Shop.objects.all()
        total_result = CustomerSyncResult()
        shop_results = []
        
        logger.info(f"Found {shops.count()} shops for full customer sync")
        
        for shop in shops:
            try:
                logger.info(f"Processing all customers for shop: {shop.name}")
                # No date filter for full sync
                shop_result = _sync_shop_customers(shop)
                
                # Aggregate results
                total_result.users_created += shop_result.users_created
                total_result.users_updated += shop_result.users_updated
                total_result.customers_created += shop_result.customers_created
                total_result.customers_updated += shop_result.customers_updated
                total_result.addresses_created += shop_result.addresses_created
                total_result.addresses_updated += shop_result.addresses_updated
                total_result.errors.extend(shop_result.errors)
                
                shop_results.append({
                    'shop_id': shop.id,
                    'shop_name': shop.name,
                    'users_created': shop_result.users_created,
                    'users_updated': shop_result.users_updated,
                    'customers_created': shop_result.customers_created,
                    'customers_updated': shop_result.customers_updated,
                    'addresses_created': shop_result.addresses_created,
                    'addresses_updated': shop_result.addresses_updated,
                    'errors': shop_result.errors
                })
                
                logger.info(f"Shop {shop.name} completed: "
                           f"{shop_result.customers_created + shop_result.customers_updated} customers, "
                           f"{shop_result.addresses_created + shop_result.addresses_updated} addresses")
                
            except Exception as e:
                error_msg = f"Error processing shop {shop.name}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                total_result.errors.append(error_msg)
                shop_results.append({
                    'shop_id': shop.id,
                    'shop_name': shop.name,
                    'error': error_msg
                })
        
        vietnam_end = _get_vietnam_time()
        duration = (vietnam_end - vietnam_start).total_seconds()
        
        # Update sync history
        sync_history.status = 'completed' if not total_result.errors else 'failed'
        sync_history.created_records = total_result.customers_created
        sync_history.updated_records = total_result.customers_updated
        sync_history.failed_records = len(total_result.errors)
        sync_history.finished_at = vietnam_end
        sync_history.total_records = total_result.customers_created + total_result.customers_updated
        sync_history.error_message = '; '.join(total_result.errors[:3]) if total_result.errors else None
        sync_history.error_details = {
            'total_errors': len(total_result.errors),
            'errors': total_result.errors[:10],
            'shop_results': shop_results,
            'sync_type': 'full'
        }
        sync_history.save()
        
        # Create summary
        summary = {
            'task_id': self.request.id,
            'start_time': vietnam_start.isoformat(),
            'end_time': vietnam_end.isoformat(),
            'duration_seconds': duration,
            'sync_type': 'full',
            'shops_processed': len(shop_results),
            'users_created': total_result.users_created,
            'users_updated': total_result.users_updated,
            'customers_created': total_result.customers_created,
            'customers_updated': total_result.customers_updated,
            'addresses_created': total_result.addresses_created,
            'addresses_updated': total_result.addresses_updated,
            'total_errors': len(total_result.errors),
            'error_details': total_result.errors[:10],
            'success': len(total_result.errors) == 0,
            'sync_history_id': sync_history.id
        }
        
        # Log results
        if summary['success']:
            logger.info(f"{task_name} completed successfully: {summary}")
        else:
            logger.warning(f"{task_name} completed with errors: {summary}")
        
        # Send notification

        
        return summary
        
    except Exception as exc:
        vietnam_error = _get_vietnam_time()
        error_summary = {
            'task_id': self.request.id,
            'error_time': vietnam_error.isoformat(),
            'error_message': str(exc),
            'sync_type': 'full',
        }
        
        logger.error(f"Error in {task_name} task: {error_summary}", exc_info=True)
        
        # Update sync history for critical failure
        try:
            sync_history.status = 'failed'
            sync_history.error_message = f"Critical error: {str(exc)}"
            sync_history.finished_at = vietnam_error
            sync_history.save()
        except:
            pass
        
        # Send error notification

        
        raise exc

@shared_task
def sync_customer_pipeline():
    """
    Orchestrate customer sync pipeline with proper dependencies
    Run this for scheduled customer syncs
    """
    vietnam_start = _get_vietnam_time()
    logger.info(f"Starting customer sync pipeline at {vietnam_start}")
    
    results = {
        'start_time': vietnam_start.isoformat(),
        '30_days_result': None,
        'overall_success': True
    }
    
    try:
        # Run 30-day sync
        logger.info("Running 30-day customer sync...")
        sync_30_days_result = sync_all_customers_30_days.apply()
        results['30_days_result'] = sync_30_days_result.result
        
        if not results['30_days_result']['success']:
            results['overall_success'] = False
            logger.warning("30-day customer sync had errors")
        
    except Exception as e:
        logger.error(f"Error in customer sync pipeline: {e}", exc_info=True)
        results['overall_success'] = False
        results['error'] = str(e)
    
    vietnam_end = _get_vietnam_time()
    results['end_time'] = vietnam_end.isoformat()
    results['total_duration'] = (vietnam_end - vietnam_start).total_seconds()
    
    logger.info(f"Customer sync pipeline completed: {results}")
    return results

@shared_task
def cleanup_old_customer_sync_histories():
    """Clean up old customer sync history records (keep last 50 records per type)"""
    try:
        from shops.models import SyncHistory
        
        sync_types = ['customers_30_days', 'customers_full']
        
        for sync_type in sync_types:
            histories = SyncHistory.objects.filter(
                sync_type=sync_type
            ).order_by('-started_at')
            
            if histories.count() > 50:
                old_histories = histories[50:]
                old_ids = [h.id for h in old_histories]
                deleted_count = SyncHistory.objects.filter(id__in=old_ids).delete()[0]
                logger.info(f"Cleaned up {deleted_count} old {sync_type} sync histories")
        
        return {'success': True, 'message': 'Customer sync history cleanup completed'}
        
    except Exception as e:
        logger.error(f"Error in customer sync history cleanup: {e}")
        return {'success': False, 'error': str(e)}

# ===== UTILITY FUNCTIONS FOR MONITORING =====

def get_customer_sync_status():
    """Get current customer sync status and statistics"""
    from shops.models import SyncHistory, Customer, Shop
    
    vietnam_now = _get_vietnam_time()
    
    # Get latest syncs
    latest_30_days = SyncHistory.objects.filter(
        sync_type='customers_30_days'
    ).order_by('-started_at').first()
    
    latest_full = SyncHistory.objects.filter(
        sync_type='customers_full'
    ).order_by('-started_at').first()
    
    # Get running syncs
    running_syncs = SyncHistory.objects.filter(
        sync_type__in=['customers_30_days', 'customers_full'],
        status='running'
    ).count()
    
    # Get today's stats
    today_start = vietnam_now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_syncs = SyncHistory.objects.filter(
        sync_type__in=['customers_30_days', 'customers_full'],
        started_at__gte=today_start
    )
    
    # Recent customer activity
    recent_customers = Customer.objects.filter(
        last_sync__gte=vietnam_now - timedelta(hours=24)
    ).count()
    
    return {
        'latest_30_days': latest_30_days,
        'latest_full': latest_full,
        'running_syncs': running_syncs,
        'today_syncs_count': today_syncs.count(),
        'today_completed': today_syncs.filter(status='completed').count(),
        'today_failed': today_syncs.filter(status='failed').count(),
        'recent_customers_24h': recent_customers,
        'current_time': vietnam_now,
        'timezone': 'GMT+7'
    }

def get_customer_stats_by_shop():
    """Get customer statistics by shop"""
    from django.db.models import Count, Q
    from shops.models import Shop
    
    stats = Shop.objects.annotate(
        total_customers=Count('customers'),
        recent_customers=Count(
            'customers',
            filter=Q(customers__last_sync__gte=timezone.now() - timedelta(days=30))
        ),
        customers_with_orders=Count(
            'customers',
            filter=Q(customers__order_count__gt=0)
        )
    ).values(
        'id', 'name', 'total_customers', 'recent_customers', 'customers_with_orders'
    )
    
    return list(stats)
                    
# ===== ORDERS =====
logger = logging.getLogger(__name__)

# Vietnam timezone
VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

@dataclass
class OrderSyncResult:
    orders_created: int = 0
    orders_updated: int = 0
    items_created: int = 0
    addresses_created: int = 0
    partners_created: int = 0
    warehouses_created: int = 0
    histories_created: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

# ===== UTILITY FUNCTIONS =====
def _get_vietnam_time(dt=None):
    """Get current time in Vietnam timezone or convert datetime to Vietnam timezone"""
    if dt is None:
        return timezone.now().astimezone(VIETNAM_TZ)
    
    if dt.tzinfo is None:
        # Assume UTC if no timezone info
        dt = pytz.UTC.localize(dt)
    
    return dt.astimezone(VIETNAM_TZ)

def _get_or_create_anonymous_customer(shop: Shop) -> Customer:
    """Get or create anonymous customer for orders without customer data"""
    try:
        anonymous_customer, created = Customer.objects.get_or_create(
            shop=shop,
            pancake_id='anonymous',
            defaults={
                'customer_id': 'anonymous',
                'name': 'Khách hàng ẩn danh',
                'inserted_at': _get_vietnam_time(),
                'updated_at_api': _get_vietnam_time(),
            }
        )
        if created:
            logger.info(f"Created anonymous customer for shop {shop.name}")
        return anonymous_customer
    except Exception as e:
        logger.error(f"Error creating anonymous customer for shop {shop.name}: {e}")
        raise

def _parse_datetime(datetime_str: Optional[str]) -> Optional[timezone.datetime]:
    """Parse datetime string from API and convert to Vietnam timezone"""
    if not datetime_str:
        return None
    try:
        # Remove timezone info and parse as UTC first
        clean_str = datetime_str.replace('Z', '+00:00')
        dt = timezone.datetime.fromisoformat(clean_str)
        
        # Convert to Vietnam timezone
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        
        return dt.astimezone(VIETNAM_TZ)
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse datetime '{datetime_str}': {e}")
        return None

def _parse_decimal(value) -> Decimal:
    """Parse decimal value safely"""
    if value is None:
        return Decimal('0')
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return Decimal('0')

def _reset_database_connection():
    """Reset database connection to handle broken transactions"""
    try:
        connection.close()
        # Force a new connection
        connection.ensure_connection()
        logger.info("Database connection reset successfully")
    except Exception as e:
        logger.error(f"Failed to reset database connection: {e}")

def _get_or_create_choice_value(model_class, field_name: str, value, display_name: str = None):
    """
    Tự động tạo choice value mới nếu chưa có
    Cập nhật choices trong model dynamically
    """
    if value is None:
        return None
        
    field = model_class._meta.get_field(field_name)
    current_choices = dict(field.choices) if field.choices else {}
    
    # Nếu value chưa có trong choices, thêm vào
    if value not in current_choices:
        if display_name is None:
            display_name = str(value)
            
        # Cập nhật choices
        new_choices = list(field.choices) if field.choices else []
        new_choices.append((value, display_name))
        field.choices = new_choices
        
        logger.info(f"Added new choice for {model_class.__name__}.{field_name}: {value} -> {display_name}")
    
    return value

def _get_date_range_timestamps():
    """Get timestamp range for last 30 days"""
    vietnam_now = _get_vietnam_time()
    
    # End date: today at 23:59:59
    end_date = vietnam_now.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Start date: 30 days ago at 00:00:00
    start_date = (vietnam_now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Convert to Unix timestamp
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    return start_timestamp, end_timestamp, start_date, end_date

# ===== API FUNCTIONS =====
def _fetch_orders_page_with_date_range(shop_id: int, start_timestamp: int, end_timestamp: int, page: int = 1, page_size: int = 100) -> Dict:
    """Fetch single page of orders from Pancake API with date range"""
    api_url = f"{settings.PANCAKE_API_BASE_URL}/shops/{shop_id}/orders"
    params = {
        'api_key': settings.PANCAKE_API_KEY,
        'updateStatus': 'updated_at',
        'startDateTime': start_timestamp,
        'endDateTime': end_timestamp,
        'page': page,
        'page_size': page_size,
    }
    
    logger.info(f"Fetching orders for shop {shop_id}, page {page} with date range {start_timestamp}-{end_timestamp}")
    
    try:
        response = requests.get(api_url, params=params, timeout=300)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"API response: success={data.get('success')}, page={data.get('page_number')}, "
                    f"total_pages={data.get('total_pages')}, data_count={len(data.get('data', []))}")
        
        return data
    except requests.RequestException as e:
        logger.error(f"API request failed for shop {shop_id} page {page}: {e}")
        raise

# ===== DATA EXTRACTION FUNCTIONS =====
def _extract_orders_data(orders_data: List[Dict], shop: Shop, users_map: Dict, customers_map: Dict, pages_map: Dict) -> List[Dict]:
    """Extract orders data from API response"""
    orders = []
    vietnam_now = _get_vietnam_time()
    
    logger.info(f"Processing {len(orders_data)} orders for shop {shop.name}")
    
    for i, order_data in enumerate(orders_data):
        try:
            order_id = order_data.get('id')
            if not order_id:
                logger.warning(f"Order {i} missing id")
                continue
            
            system_id = order_data.get('system_id')
            if not system_id:
                logger.warning(f"Order {i} missing system_id")
                continue
            
            # Get related objects
            creator = None
            assigning_seller = None
            assigning_care = None
            marketer = None
            last_editor = None
            customer = None
            page = None
            
            # Users
            creator_data = order_data.get('creator')
            if creator_data and creator_data.get('id'):
                creator = users_map.get(creator_data['id'])
                
            assigning_seller_data = order_data.get('assigning_seller')
            if assigning_seller_data and assigning_seller_data.get('id'):
                assigning_seller = users_map.get(assigning_seller_data['id'])
                
            assigning_care_data = order_data.get('assigning_care')
            if assigning_care_data and assigning_care_data.get('id'):
                assigning_care = users_map.get(assigning_care_data['id'])
                
            marketer_data = order_data.get('marketer')
            if marketer_data and marketer_data.get('id'):
                marketer = users_map.get(marketer_data['id'])
                
            last_editor_data = order_data.get('last_editor')
            if last_editor_data and last_editor_data.get('id'):
                last_editor = users_map.get(last_editor_data['id'])
            
            # Customer - handle missing customer with anonymous customer and track for later reassignment
            customer_data = order_data.get('customer')
            customer_pancake_id_from_order = None
            
            if customer_data and customer_data.get('id'):
                customer_pancake_id_from_order = customer_data['id']
                customer = customers_map.get(customer_pancake_id_from_order)
                
                if not customer:
                    logger.warning(f"Customer {customer_pancake_id_from_order} not found for order {order_id}, using anonymous customer")
                    customer = _get_or_create_anonymous_customer(shop)
                    # Lưu customer_id từ API vào order note để có thể reassign sau
                    original_note = order_data.get('note', '')
                    order_data['note'] = f"{original_note}\n[MISSING_CUSTOMER_ID:{customer_pancake_id_from_order}]".strip()
            else:
                logger.warning(f"Order {order_id} has no customer data, using anonymous customer")
                customer = _get_or_create_anonymous_customer(shop)
            
            # Page
            page_data = order_data.get('page')
            if page_data and page_data.get('id'):
                page = pages_map.get(page_data['id'])
            
            # Handle order sources - tự động tạo choice nếu chưa có
            order_sources_raw = order_data.get('order_sources')
            order_sources_name = order_data.get('order_sources_name', '')
            
            # Tự động tạo choice cho order_sources nếu cần
            if order_sources_raw is not None:
                order_sources = _get_or_create_choice_value(
                    Order, 'order_sources', 
                    order_sources_raw, 
                    order_sources_name or str(order_sources_raw)
                )
            else:
                order_sources = None
            
            # Handle status - tự động tạo choice nếu cần
            status_raw = order_data.get('status')
            status_name = order_data.get('status_name', '')
            if status_raw is not None:
                status = _get_or_create_choice_value(
                    Order, 'status',
                    status_raw,
                    status_name or f"Trạng thái {status_raw}"
                )
            else:
                status = 0
            
            orders.append({
                'shop': shop,
                'pancake_id': str(order_id),
                'system_id': system_id,
                'status': status,
                'sub_status': order_data.get('sub_status'),
                'order_sources': order_sources,
                'order_sources_name': order_sources_name,
                
                # Users
                'creator': creator,
                'assigning_seller': assigning_seller,
                'assigning_care': assigning_care,
                'marketer': marketer,
                'last_editor': last_editor,
                'customer': customer,
                'page': page,
                
                # Pricing
                'total_price': _parse_decimal(order_data.get('total_price')),
                'total_discount': _parse_decimal(order_data.get('total_discount')),
                'total_price_after_sub_discount': _parse_decimal(order_data.get('total_price_after_sub_discount')),
                'shipping_fee': _parse_decimal(order_data.get('shipping_fee')),
                'partner_fee': _parse_decimal(order_data.get('partner_fee')),
                'tax': _parse_decimal(order_data.get('tax')),
                'cod': _parse_decimal(order_data.get('cod')),
                'prepaid': _parse_decimal(order_data.get('prepaid')),
                'transfer_money': _parse_decimal(order_data.get('transfer_money')),
                'money_to_collect': _parse_decimal(order_data.get('money_to_collect')),
                
                # Payment info
                'charged_by_card': _parse_decimal(order_data.get('charged_by_card')),
                'charged_by_momo': _parse_decimal(order_data.get('charged_by_momo')),
                'charged_by_qrpay': _parse_decimal(order_data.get('charged_by_qrpay')),
                'cash': _parse_decimal(order_data.get('cash')),
                'exchange_payment': _parse_decimal(order_data.get('exchange_payment')),
                'exchange_value': _parse_decimal(order_data.get('exchange_value')),
                'surcharge': _parse_decimal(order_data.get('surcharge')),
                'levera_point': order_data.get('levera_point', 0),
                
                # JSON fields
                'bank_payments': order_data.get('bank_payments', {}),
                'prepaid_by_point': order_data.get('prepaid_by_point', {}),
                'advanced_platform_fee': order_data.get('advanced_platform_fee', {}),
                
                # Billing info
                'bill_full_name': order_data.get('bill_full_name', ''),
                'bill_phone_number': order_data.get('bill_phone_number', ''),
                'bill_email': order_data.get('bill_email'),
                
                # Flags
                'is_free_shipping': order_data.get('is_free_shipping', False),
                'is_livestream': order_data.get('is_livestream', False),
                'is_live_shopping': order_data.get('is_live_shopping', False),
                'is_exchange_order': order_data.get('is_exchange_order', False),
                'is_smc': order_data.get('is_smc', False),
                'customer_pay_fee': order_data.get('customer_pay_fee', False),
                'received_at_shop': order_data.get('received_at_shop', False),
                'return_fee': order_data.get('return_fee', False),
                
                # Other fields
                'warehouse_id': order_data.get('warehouse_id'),
                'note': order_data.get('note', ''),
                'note_print': order_data.get('note_print'),
                'note_image': order_data.get('note_image'),
                'link': order_data.get('link'),
                'link_confirm_order': order_data.get('link_confirm_order'),
                'order_link': order_data.get('order_link'),
                
                # Social media
                'account': order_data.get('account'),
                'account_name': order_data.get('account_name'),
                'page_external_id': order_data.get('page_id'),  # Fix field name
                'conversation_id': order_data.get('conversation_id'),
                'post_id': order_data.get('post_id'),
                'ad_id': order_data.get('ad_id'),
                'ads_source': order_data.get('ads_source'),
                
                # UTM
                'p_utm_source': order_data.get('p_utm_source'),
                'p_utm_medium': order_data.get('p_utm_medium'),
                'p_utm_campaign': order_data.get('p_utm_campaign'),
                'p_utm_content': order_data.get('p_utm_content'),
                'p_utm_term': order_data.get('p_utm_term'),
                'p_utm_id': order_data.get('p_utm_id'),
                
                # Referral
                'customer_referral_code': order_data.get('customer_referral_code'),
                'pke_mkter': order_data.get('pke_mkter'),
                
                # Marketplace
                'marketplace_id': order_data.get('marketplace_id'),
                'fee_marketplace': _parse_decimal(order_data.get('fee_marketplace')),
                
                # Arrays
                'tags': order_data.get('tags', []),
                'customer_needs': order_data.get('customer_needs', []),
                'activated_combo_products': order_data.get('activated_combo_products', []),
                'activated_promotion_advances': order_data.get('activated_promotion_advances', []),
                'payment_purchase_histories': order_data.get('payment_purchase_histories', []),
                
                # Quantities
                'total_quantity': order_data.get('total_quantity', 0),
                'items_length': order_data.get('items_length', 0),
                
                # Return info
                'returned_reason': order_data.get('returned_reason'),
                'returned_reason_name': order_data.get('returned_reason_name'),
                
                # Dates - converted to Vietnam timezone
                'time_assign_seller': _parse_datetime(order_data.get('time_assign_seller')),
                'time_assign_care': _parse_datetime(order_data.get('time_assign_care')),
                'time_send_partner': _parse_datetime(order_data.get('time_send_partner')),
                'estimate_delivery_date': _parse_datetime(order_data.get('estimate_delivery_date')),
                'buyer_total_amount': _parse_decimal(order_data.get('buyer_total_amount')) if order_data.get('buyer_total_amount') else None,
                
                # API timestamps - converted to Vietnam timezone
                'inserted_at': _parse_datetime(order_data.get('inserted_at')) or vietnam_now,
                'updated_at_api': _parse_datetime(order_data.get('updated_at')) or vietnam_now,
                
                # Currency
                'order_currency': order_data.get('order_currency', 'VND'),
                
                # Metadata
                'last_sync': vietnam_now,
                
                # Raw data for related objects
                'shipping_address_data': order_data.get('shipping_address'),
                'warehouse_info_data': order_data.get('warehouse_info'),
                'partner_data': order_data.get('partner'),
                'items_data': order_data.get('items', []),
                'status_history_data': order_data.get('status_history', []),
                'histories_data': order_data.get('histories', []),
            })
            
        except Exception as e:
            logger.error(f"Error extracting order {i}: {e}", exc_info=True)
            continue
    
    logger.info(f"Extracted {len(orders)} orders")
    return orders

def _extract_shipping_addresses_data(orders_data: List[Dict]) -> List[Dict]:
    """Extract shipping addresses data"""
    addresses = []
    
    for order_data in orders_data:
        order_id = order_data.get('pancake_id')
        if not order_id:
            continue
            
        shipping_data = order_data.get('shipping_address_data')
        if not shipping_data:
            continue
            
        addresses.append({
            'order_pancake_id': order_id,
            'full_name': shipping_data.get('full_name', ''),
            'phone_number': shipping_data.get('phone_number', ''),
            'address': shipping_data.get('address', ''),
            'full_address': shipping_data.get('full_address', ''),
            'country_code': shipping_data.get('country_code', '84'),
            'province_id': shipping_data.get('province_id', ''),
            'province_name': shipping_data.get('province_name', ''),
            'district_id': shipping_data.get('district_id', ''),
            'district_name': shipping_data.get('district_name', ''),
            'commune_id': shipping_data.get('commune_id', ''),
            'commune_name': shipping_data.get('commune_name', ''),
            'commnue_name': shipping_data.get('commnue_name'),  # Typo in API
            'new_province_id': shipping_data.get('new_province_id'),
            'new_commune_id': shipping_data.get('new_commune_id'),
            'new_full_address': shipping_data.get('new_full_address'),
            'post_code': shipping_data.get('post_code'),
            'marketplace_address': shipping_data.get('marketplace_address'),
            'render_type': shipping_data.get('render_type', 'old'),
            'commune_code_sicepat': shipping_data.get('commune_code_sicepat'),
        })
    
    return addresses

def _extract_items_data(orders_data: List[Dict], products_map: Dict, variations_map: Dict) -> List[Dict]:
    """Extract order items data"""
    items = []
    
    for order_data in orders_data:
        order_pancake_id = order_data.get('pancake_id')
        if not order_pancake_id:
            continue
            
        items_data = order_data.get('items_data', [])
        for item_data in items_data:
            item_id = item_data.get('id')
            if not item_id:
                continue
                
            # Get product and variation
            product = None
            variation = None
            
            product_id = item_data.get('product_id')
            if product_id:
                product = products_map.get(product_id)
                
            variation_id = item_data.get('variation_id')
            if variation_id:
                variation = variations_map.get(variation_id)
            
            items.append({
                'order_pancake_id': order_pancake_id,
                'item_id': item_id,
                'product': product,
                'variation': variation,
                'quantity': item_data.get('quantity', 1),
                'added_to_cart_quantity': item_data.get('added_to_cart_quantity', 0),
                'retail_price': _parse_decimal(item_data.get('retail_price', 0)),
                'discount_each_product': _parse_decimal(item_data.get('discount_each_product', 0)),
                'same_price_discount': _parse_decimal(item_data.get('same_price_discount', 0)),
                'total_discount': _parse_decimal(item_data.get('total_discount', 0)),
                'is_bonus_product': item_data.get('is_bonus_product', False),
                'is_composite': item_data.get('is_composite'),
                'is_discount_percent': item_data.get('is_discount_percent', False),
                'is_wholesale': item_data.get('is_wholesale', False),
                'one_time_product': item_data.get('one_time_product', False),
                'return_quantity': item_data.get('return_quantity', 0),
                'returned_count': item_data.get('returned_count', 0),
                'returning_quantity': item_data.get('returning_quantity', 0),
                'exchange_count': item_data.get('exchange_count', 0),
                'composite_item_id': item_data.get('composite_item_id'),
                'measure_group_id': item_data.get('measure_group_id'),
                'note': item_data.get('note'),
                'note_product': item_data.get('note_product'),
                'components': item_data.get('components'),
                'variation_info': item_data.get('variation_info', {}),
            })
    
    return items

# ===== BULK UPSERT FUNCTIONS =====
def _safe_bulk_upsert_orders(orders_data: List[Dict]) -> Tuple[int, int]:
    """Safely bulk create/update orders with transaction management"""
    if not orders_data:
        return 0, 0
    
    shop = orders_data[0]['shop']
    pancake_ids = [o['pancake_id'] for o in orders_data]
    
    created_count = 0
    updated_count = 0
    
    try:
        # Get existing orders
        existing_orders = {
            o.pancake_id: o for o in Order.objects.filter(
                pancake_id__in=pancake_ids, shop=shop
            )
        }
        
        orders_to_create = []
        orders_to_update = []
        
        for order_data in orders_data:
            pancake_id = order_data['pancake_id']
            
            # Remove related data before creating model instance
            related_data_fields = [
                'shipping_address_data', 'warehouse_info_data', 'partner_data',
                'items_data', 'status_history_data', 'histories_data'
            ]
            clean_order_data = {k: v for k, v in order_data.items() if k not in related_data_fields}
            
            if pancake_id in existing_orders:
                # Update existing
                order = existing_orders[pancake_id]
                for field, value in clean_order_data.items():
                    if field != 'shop':
                        setattr(order, field, value)
                orders_to_update.append(order)
            else:
                # Create new
                orders_to_create.append(Order(**clean_order_data))
        
        # Use separate transactions for create and update
        if orders_to_create:
            with transaction.atomic():
                Order.objects.bulk_create(orders_to_create, batch_size=100, ignore_conflicts=True)
                created_count = len(orders_to_create)
                logger.info(f"Bulk created {created_count} orders")
        
        if orders_to_update:
            with transaction.atomic():
                # Define fields to update
                fields_to_update = [
                    'status', 'sub_status', 'order_sources', 'order_sources_name',
                    'total_price', 'total_discount', 'total_price_after_sub_discount',
                    'shipping_fee', 'partner_fee', 'tax', 'cod', 'prepaid',
                    'transfer_money', 'money_to_collect', 'charged_by_card',
                    'charged_by_momo', 'charged_by_qrpay', 'cash', 'exchange_payment',
                    'exchange_value', 'surcharge', 'levera_point', 'bank_payments',
                    'prepaid_by_point', 'advanced_platform_fee', 'bill_full_name',
                    'bill_phone_number', 'bill_email', 'is_free_shipping',
                    'is_livestream', 'is_live_shopping', 'is_exchange_order',
                    'is_smc', 'customer_pay_fee', 'received_at_shop', 'return_fee',
                    'warehouse_id', 'note', 'note_print', 'note_image', 'link',
                    'link_confirm_order', 'order_link', 'account', 'account_name',
                    'page_external_id', 'conversation_id', 'post_id', 'ad_id', 'ads_source',
                    'p_utm_source', 'p_utm_medium', 'p_utm_campaign', 'p_utm_content',
                    'p_utm_term', 'p_utm_id', 'customer_referral_code', 'pke_mkter',
                    'marketplace_id', 'fee_marketplace', 'tags', 'customer_needs',
                    'activated_combo_products', 'activated_promotion_advances',
                    'payment_purchase_histories', 'total_quantity', 'items_length',
                    'returned_reason', 'returned_reason_name', 'time_assign_seller',
                    'time_assign_care', 'time_send_partner', 'estimate_delivery_date',
                    'buyer_total_amount', 'updated_at_api', 'order_currency',
                    'last_sync', 'creator', 'assigning_seller', 'assigning_care',
                    'marketer', 'last_editor', 'customer', 'page'
                ]
                
                Order.objects.bulk_update(orders_to_update, fields_to_update, batch_size=100)
                updated_count = len(orders_to_update)
                logger.info(f"Bulk updated {updated_count} orders")
        
    except Exception as e:
        logger.error(f"Error in bulk upsert orders: {e}", exc_info=True)
        # Reset connection if transaction is broken
        _reset_database_connection()
        raise
    
    return created_count, updated_count

def _safe_bulk_upsert_shipping_addresses(addresses_data: List[Dict], orders_map: Dict) -> Tuple[int, int]:
    """Safely bulk create/update shipping addresses"""
    if not addresses_data:
        return 0, 0
    
    # Filter valid addresses
    valid_addresses = []
    for address_data in addresses_data:
        order_pancake_id = address_data['order_pancake_id']
        if order_pancake_id in orders_map:
            address_data['order'] = orders_map[order_pancake_id]
            address_data.pop('order_pancake_id')
            valid_addresses.append(address_data)
    
    if not valid_addresses:
        return 0, 0
    
    created_count = 0
    updated_count = 0
    
    try:
        order_ids = [a['order'].id for a in valid_addresses]
        existing_addresses = {
            a.order_id: a for a in OrderShippingAddress.objects.filter(order_id__in=order_ids)
        }
        
        addresses_to_create = []
        addresses_to_update = []
        
        for address_data in valid_addresses:
            order_id = address_data['order'].id
            
            if order_id in existing_addresses:
                # Update existing
                address = existing_addresses[order_id]
                for field, value in address_data.items():
                    if field != 'order':
                        setattr(address, field, value)
                addresses_to_update.append(address)
            else:
                # Create new
                addresses_to_create.append(OrderShippingAddress(**address_data))
        
        if addresses_to_create:
            with transaction.atomic():
                OrderShippingAddress.objects.bulk_create(addresses_to_create, batch_size=100, ignore_conflicts=True)
                created_count = len(addresses_to_create)
                logger.info(f"Bulk created {created_count} shipping addresses")
        
        if addresses_to_update:
            with transaction.atomic():
                fields_to_update = [
                    'full_name', 'phone_number', 'address', 'full_address',
                    'country_code', 'province_id', 'province_name', 'district_id',
                    'district_name', 'commune_id', 'commune_name', 'commnue_name',
                    'new_province_id', 'new_commune_id', 'new_full_address',
                    'post_code', 'marketplace_address', 'render_type', 'commune_code_sicepat'
                ]
                OrderShippingAddress.objects.bulk_update(addresses_to_update, fields_to_update, batch_size=100)
                updated_count = len(addresses_to_update)
                logger.info(f"Bulk updated {updated_count} shipping addresses")
                
    except Exception as e:
        logger.error(f"Error in bulk upsert shipping addresses: {e}", exc_info=True)
        _reset_database_connection()
        raise
    
    return created_count, updated_count

def _safe_bulk_upsert_order_items(items_data: List[Dict], orders_map: Dict) -> Tuple[int, int]:
    """Safely bulk create/update order items"""
    if not items_data:
        return 0, 0
    
    # Filter valid items
    valid_items = []
    for item_data in items_data:
        order_pancake_id = item_data['order_pancake_id']
        if order_pancake_id in orders_map:
            item_data['order'] = orders_map[order_pancake_id]
            item_data.pop('order_pancake_id')
            valid_items.append(item_data)
    
    if not valid_items:
        return 0, 0
    
    created_count = 0
    updated_count = 0
    
    try:
        # Get existing items
        order_item_pairs = [(item['order'].id, item['item_id']) for item in valid_items]
        existing_items = {}
        for item in OrderItem.objects.filter(order_id__in=[pair[0] for pair in order_item_pairs]):
            existing_items[(item.order_id, item.item_id)] = item
        
        items_to_create = []
        items_to_update = []
        
        for item_data in valid_items:
            order_id = item_data['order'].id
            item_id = item_data['item_id']
            key = (order_id, item_id)
            
            if key in existing_items:
                # Update existing
                item = existing_items[key]
                for field, value in item_data.items():
                    if field not in ['order', 'item_id']:
                        setattr(item, field, value)
                items_to_update.append(item)
            else:
                # Create new
                items_to_create.append(OrderItem(**item_data))
        
        if items_to_create:
            with transaction.atomic():
                OrderItem.objects.bulk_create(items_to_create, batch_size=100, ignore_conflicts=True)
                created_count = len(items_to_create)
                logger.info(f"Bulk created {created_count} order items")
        
        if items_to_update:
            with transaction.atomic():
                fields_to_update = [
                    'product', 'variation', 'quantity', 'added_to_cart_quantity',
                    'retail_price', 'discount_each_product', 'same_price_discount',
                    'total_discount', 'is_bonus_product', 'is_composite',
                    'is_discount_percent', 'is_wholesale', 'one_time_product',
                    'return_quantity', 'returned_count', 'returning_quantity',
                    'exchange_count', 'composite_item_id', 'measure_group_id',
                    'note', 'note_product', 'components', 'variation_info'
                ]
                OrderItem.objects.bulk_update(items_to_update, fields_to_update, batch_size=100)
                updated_count = len(items_to_update)
                logger.info(f"Bulk updated {updated_count} order items")
                
    except Exception as e:
        logger.error(f"Error in bulk upsert order items: {e}", exc_info=True)
        _reset_database_connection()
        raise
    
    return created_count, updated_count

# ===== WAREHOUSE, PARTNER AND HISTORY FUNCTIONS =====
def _bulk_upsert_warehouses(orders_data: List[Dict], orders_map: Dict) -> int:
    """Bulk create/update order warehouses"""
    warehouses_data = []
    
    for order_data in orders_data:
        order_pancake_id = order_data.get('pancake_id')
        warehouse_info = order_data.get('warehouse_info_data')
        
        if not warehouse_info or order_pancake_id not in orders_map:
            continue
        
        warehouses_data.append({
            'order': orders_map[order_pancake_id],
            'name': warehouse_info.get('name', ''),
            'address': warehouse_info.get('address', ''),
            'full_address': warehouse_info.get('full_address', ''),
            'phone_number': warehouse_info.get('phone_number', ''),
            'province_id': warehouse_info.get('province_id', ''),
            'district_id': warehouse_info.get('district_id', ''),
            'commune_id': warehouse_info.get('commune_id', ''),
            'postcode': warehouse_info.get('postcode'),
            'settings': warehouse_info.get('settings'),
            'has_snappy_service': warehouse_info.get('has_snappy_service', False),
            'custom_id': warehouse_info.get('custom_id'),
            'affiliate_id': warehouse_info.get('affiliate_id'),
            'ffm_id': warehouse_info.get('ffm_id'),
        })
    
    if not warehouses_data:
        return 0
    
    created_count = 0
    
    try:
        # Get existing warehouses
        order_ids = [w['order'].id for w in warehouses_data]
        existing_warehouses = {
            w.order_id: w for w in OrderWarehouse.objects.filter(order_id__in=order_ids)
        }
        
        warehouses_to_create = []
        warehouses_to_update = []
        
        for warehouse_data in warehouses_data:
            order_id = warehouse_data['order'].id
            
            if order_id in existing_warehouses:
                # Update existing
                warehouse = existing_warehouses[order_id]
                for field, value in warehouse_data.items():
                    if field != 'order':
                        setattr(warehouse, field, value)
                warehouses_to_update.append(warehouse)
            else:
                # Create new
                warehouses_to_create.append(OrderWarehouse(**warehouse_data))
        
        if warehouses_to_create:
            with transaction.atomic():
                OrderWarehouse.objects.bulk_create(warehouses_to_create, batch_size=100, ignore_conflicts=True)
                created_count = len(warehouses_to_create)
                logger.info(f"Bulk created {created_count} warehouses")
        
        if warehouses_to_update:
            with transaction.atomic():
                fields_to_update = [
                    'name', 'address', 'full_address', 'phone_number', 'province_id',
                    'district_id', 'commune_id', 'postcode', 'settings',
                    'has_snappy_service', 'custom_id', 'affiliate_id', 'ffm_id'
                ]
                OrderWarehouse.objects.bulk_update(warehouses_to_update, fields_to_update, batch_size=100)
                logger.info(f"Bulk updated {len(warehouses_to_update)} warehouses")
                
    except Exception as e:
        logger.error(f"Error bulk processing warehouses: {e}", exc_info=True)
        _reset_database_connection()
    
    return created_count

def _bulk_upsert_partners(orders_data: List[Dict], orders_map: Dict) -> int:
    """Bulk create/update order partners"""
    partners_data = []
    
    for order_data in orders_data:
        order_pancake_id = order_data.get('pancake_id')
        partner_info = order_data.get('partner_data')
        
        if not partner_info or order_pancake_id not in orders_map:
            continue
        
        partners_data.append({
            'order': orders_map[order_pancake_id],
            'partner_id': partner_info.get('partner_id', 0),
            'partner_name': partner_info.get('partner_name', ''),
            'partner_status': partner_info.get('partner_status', ''),
            'extend_code': partner_info.get('extend_code', ''),
            'order_number_vtp': partner_info.get('order_number_vtp'),
            'sort_code': partner_info.get('sort_code'),
            'custom_partner_id': partner_info.get('custom_partner_id'),
            'cod': _parse_decimal(partner_info.get('cod')),
            'total_fee': _parse_decimal(partner_info.get('total_fee')),
            'delivery_name': partner_info.get('delivery_name'),
            'delivery_tel': partner_info.get('delivery_tel'),
            'count_of_delivery': partner_info.get('count_of_delivery'),
            'system_created': partner_info.get('system_created', True),
            'is_returned': partner_info.get('is_returned'),
            'is_ghn_v2': partner_info.get('is_ghn_v2'),
            'printed_form': partner_info.get('printed_form'),
            'order_id_ghn': partner_info.get('order_id_ghn'),
            'first_delivery_at': _parse_datetime(partner_info.get('first_delivery_at')),
            'picked_up_at': _parse_datetime(partner_info.get('picked_up_at')),
            'paid_at': _parse_datetime(partner_info.get('paid_at')),
            'updated_at_partner': _parse_datetime(partner_info.get('updated_at')),
            'service_partner': partner_info.get('service_partner', {}),
            'extend_update': partner_info.get('extend_update', []),
        })
    
    if not partners_data:
        return 0
    
    created_count = 0
    
    try:
        # Get existing partners
        order_ids = [p['order'].id for p in partners_data]
        existing_partners = {
            p.order_id: p for p in OrderPartner.objects.filter(order_id__in=order_ids)
        }
        
        partners_to_create = []
        partners_to_update = []
        
        for partner_data in partners_data:
            order_id = partner_data['order'].id
            
            if order_id in existing_partners:
                # Update existing
                partner = existing_partners[order_id]
                for field, value in partner_data.items():
                    if field != 'order':
                        setattr(partner, field, value)
                partners_to_update.append(partner)
            else:
                # Create new
                partners_to_create.append(OrderPartner(**partner_data))
        
        if partners_to_create:
            with transaction.atomic():
                OrderPartner.objects.bulk_create(partners_to_create, batch_size=100, ignore_conflicts=True)
                created_count = len(partners_to_create)
                logger.info(f"Bulk created {created_count} partners")
        
        if partners_to_update:
            with transaction.atomic():
                fields_to_update = [
                    'partner_id', 'partner_name', 'partner_status', 'extend_code',
                    'order_number_vtp', 'sort_code', 'custom_partner_id', 'cod',
                    'total_fee', 'delivery_name', 'delivery_tel', 'count_of_delivery',
                    'system_created', 'is_returned', 'is_ghn_v2', 'printed_form',
                    'order_id_ghn', 'first_delivery_at', 'picked_up_at', 'paid_at',
                    'updated_at_partner', 'service_partner', 'extend_update'
                ]
                OrderPartner.objects.bulk_update(partners_to_update, fields_to_update, batch_size=100)
                logger.info(f"Bulk updated {len(partners_to_update)} partners")
                
    except Exception as e:
        logger.error(f"Error bulk processing partners: {e}", exc_info=True)
        _reset_database_connection()
    
    return created_count

def _bulk_upsert_histories(orders_data: List[Dict], orders_map: Dict, users_map: Dict) -> int:
    """Bulk create order status histories"""
    status_histories_data = []
    order_histories_data = []
    vietnam_now = _get_vietnam_time()
    
    for order_data in orders_data:
        order_pancake_id = order_data.get('pancake_id')
        if order_pancake_id not in orders_map:
            continue
            
        order = orders_map[order_pancake_id]
        
        # Status histories
        status_history_list = order_data.get('status_history_data', [])
        for status_data in status_history_list:
            editor = None
            editor_id = status_data.get('editor_id')
            if editor_id:
                editor = users_map.get(editor_id)
            
            status_histories_data.append({
                'order': order,
                'editor': editor,
                'editor_fb': status_data.get('editor_fb'),
                'name': status_data.get('name'),
                'avatar_url': status_data.get('avatar_url'),
                'old_status': status_data.get('old_status'),
                'status': status_data.get('status', 0),
                'updated_at': _parse_datetime(status_data.get('updated_at')) or vietnam_now,
            })
        
        # Order histories
        histories_list = order_data.get('histories_data', [])
        for history_data in histories_list:
            editor = None
            editor_id = history_data.get('editor_id')
            if editor_id:
                editor = users_map.get(editor_id)
            
            # Remove editor_id from changes as it's handled separately
            changes = dict(history_data)
            changes.pop('editor_id', None)
            changes.pop('updated_at', None)
            
            order_histories_data.append({
                'order': order,
                'editor': editor,
                'changes': changes,
                'updated_at': _parse_datetime(history_data.get('updated_at')) or vietnam_now,
            })
    
    created_count = 0
    
    try:
        # Create status histories
        if status_histories_data:
            with transaction.atomic():
                # Delete existing status histories for these orders to avoid duplicates
                order_ids = list(set(h['order'].id for h in status_histories_data))
                OrderStatusHistory.objects.filter(order_id__in=order_ids).delete()
                
                OrderStatusHistory.objects.bulk_create(
                    [OrderStatusHistory(**data) for data in status_histories_data],
                    batch_size=100
                )
                created_count += len(status_histories_data)
                logger.info(f"Bulk created {len(status_histories_data)} status histories")
        
        # Create order histories
        if order_histories_data:
            with transaction.atomic():
                # Delete existing order histories for these orders to avoid duplicates
                order_ids = list(set(h['order'].id for h in order_histories_data))
                OrderHistory.objects.filter(order_id__in=order_ids).delete()
                
                OrderHistory.objects.bulk_create(
                    [OrderHistory(**data) for data in order_histories_data],
                    batch_size=100
                )
                created_count += len(order_histories_data)
                logger.info(f"Bulk created {len(order_histories_data)} order histories")
                
    except Exception as e:
        logger.error(f"Error bulk creating histories: {e}", exc_info=True)
        _reset_database_connection()
    
    return created_count

# ===== MAIN SYNC FUNCTION FOR SINGLE SHOP =====
def _sync_shop_orders_with_date_range(shop: Shop, start_timestamp: int, end_timestamp: int, start_date, end_date) -> OrderSyncResult:
    """Sync orders for a single shop with date range"""
    result = OrderSyncResult()
    
    try:
        page = 1
        total_pages = None  
        processed_pages = 0
        vietnam_now = _get_vietnam_time()
        
        logger.info(f"Starting orders sync for shop: {shop.name} (ID: {shop.pancake_id}) "
                   f"from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Continue until we've processed all pages
        while total_pages is None or page <= total_pages:
            page_start_time = _get_vietnam_time()
            
            # Show progress if we know total pages
            if total_pages:
                logger.info(f"Processing page {page}/{total_pages} for shop {shop.name}")
            else:
                logger.info(f"Processing page {page} for shop {shop.name}")
            
            try:
                # Fetch data with retry mechanism
                max_retries = 3
                api_response = None
                
                for retry in range(max_retries):
                    try:
                        api_response = _fetch_orders_page_with_date_range(
                            shop.pancake_id, start_timestamp, end_timestamp, page, 100
                        )
                        break
                    except requests.RequestException as e:
                        if retry == max_retries - 1:
                            raise
                        logger.warning(f"API request failed (retry {retry + 1}/{max_retries}): {e}")
                        import time
                        time.sleep(2)  # Wait before retry
                
                if not api_response.get('success', False):
                    error_msg = f"API returned success=false for shop {shop.name} page {page}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)
                    break
                
                # Set total_pages from first response
                if total_pages is None:
                    total_pages = api_response.get('total_pages', 1)
                    logger.info(f"Shop {shop.name}: Total pages to process = {total_pages}")
                
                orders_data = api_response.get('data', [])
                
                logger.info(f"Shop {shop.name} - Page {page}/{total_pages}: {len(orders_data)} orders")
                
                if not orders_data:
                    logger.warning(f"No data for shop {shop.name} page {page}")
                    page += 1
                    continue
                
                # Prepare mapping data with optimized queries
                try:
                    # Get all IDs mentioned in orders
                    user_ids = set()
                    customer_ids = set()
                    page_ids = set()
                    product_ids = set()
                    variation_ids = set()
                    
                    for order_data in orders_data:
                        # Users
                        for user_field in ['creator', 'assigning_seller', 'assigning_care', 'marketer', 'last_editor']:
                            user_data = order_data.get(user_field)
                            if user_data and user_data.get('id'):
                                user_ids.add(user_data['id'])
                        
                        # Customer
                        customer_data = order_data.get('customer')
                        if customer_data and customer_data.get('id'):
                            customer_ids.add(customer_data['id'])
                        
                        # Page
                        page_data = order_data.get('page')
                        if page_data and page_data.get('id'):
                            page_ids.add(page_data['id'])
                        
                        # Products and variations from items
                        for item_data in order_data.get('items', []):
                            product_id = item_data.get('product_id')
                            if product_id:
                                product_ids.add(product_id)
                            
                            variation_id = item_data.get('variation_id')
                            if variation_id:
                                variation_ids.add(variation_id)
                    
                    # Create mapping dictionaries with error handling
                    users_map = {}
                    customers_map = {}
                    pages_map = {}
                    products_map = {}
                    variations_map = {}
                    
                    if user_ids:
                        try:
                            users_map = {
                                u.pancake_id: u for u in User.objects.filter(pancake_id__in=user_ids)
                            }
                        except Exception as e:
                            logger.warning(f"Error loading users map: {e}")
                    
                    if customer_ids:
                        try:
                            customers_map = {
                                c.pancake_id: c for c in Customer.objects.filter(
                                    shop=shop, pancake_id__in=customer_ids
                                )
                            }
                        except Exception as e:
                            logger.warning(f"Error loading customers map: {e}")
                    
                    if page_ids:
                        try:
                            pages_map = {
                                p.pancake_id: p for p in Page.objects.filter(
                                    shop=shop, pancake_id__in=page_ids
                                )
                            }
                        except Exception as e:
                            logger.warning(f"Error loading pages map: {e}")
                    
                    if product_ids:
                        try:
                            products_map = {
                                p.pancake_id: p for p in Product.objects.filter(
                                    shop=shop, pancake_id__in=product_ids
                                )
                            }
                        except Exception as e:
                            logger.warning(f"Error loading products map: {e}")
                    
                    if variation_ids:
                        try:
                            variations_map = {
                                v.pancake_id: v for v in ProductVariation.objects.filter(
                                    product__shop=shop, pancake_id__in=variation_ids
                                )
                            }
                        except Exception as e:
                            logger.warning(f"Error loading variations map: {e}")
                    
                    # Extract and transform data
                    orders_processed = _extract_orders_data(orders_data, shop, users_map, customers_map, pages_map)
                    
                    if not orders_processed:
                        logger.warning(f"No orders processed for shop {shop.name} page {page}")
                        page += 1
                        continue
                    
                    # Process data with separate error handling for each operation
                    try:
                        # Bulk upsert orders
                        orders_created, orders_updated = _safe_bulk_upsert_orders(orders_processed)
                        result.orders_created += orders_created
                        result.orders_updated += orders_updated
                        
                        # Create orders map for related objects
                        order_pancake_ids = [o['pancake_id'] for o in orders_processed]
                        orders_map = {
                            o.pancake_id: o for o in Order.objects.filter(
                                shop=shop, pancake_id__in=order_pancake_ids
                            )
                        }
                        
                        # Extract and upsert shipping addresses
                        try:
                            addresses_data = _extract_shipping_addresses_data(orders_processed)
                            addresses_created, addresses_updated = _safe_bulk_upsert_shipping_addresses(addresses_data, orders_map)
                            result.addresses_created += addresses_created
                        except Exception as e:
                            logger.error(f"Error processing shipping addresses for page {page}: {e}")
                            result.errors.append(f"Shipping addresses error page {page}: {str(e)}")
                        
                        # Extract and upsert order items
                        try:
                            items_data = _extract_items_data(orders_processed, products_map, variations_map)
                            items_created, items_updated = _safe_bulk_upsert_order_items(items_data, orders_map)
                            result.items_created += items_created
                        except Exception as e:
                            logger.error(f"Error processing order items for page {page}: {e}")
                            result.errors.append(f"Order items error page {page}: {str(e)}")
                        
                        # Handle warehouses and partners if needed
                        try:
                            warehouses_created = _bulk_upsert_warehouses(orders_processed, orders_map)
                            result.warehouses_created += warehouses_created
                        except Exception as e:
                            logger.error(f"Error processing warehouses for page {page}: {e}")
                            result.errors.append(f"Warehouses error page {page}: {str(e)}")
                        
                        try:
                            partners_created = _bulk_upsert_partners(orders_processed, orders_map)
                            result.partners_created += partners_created
                        except Exception as e:
                            logger.error(f"Error processing partners for page {page}: {e}")
                            result.errors.append(f"Partners error page {page}: {str(e)}")
                        
                        try:
                            histories_created = _bulk_upsert_histories(orders_processed, orders_map, users_map)
                            result.histories_created += histories_created
                        except Exception as e:
                            logger.error(f"Error processing histories for page {page}: {e}")
                            result.errors.append(f"Histories error page {page}: {str(e)}")
                        
                        page_end_time = _get_vietnam_time()
                        page_duration = (page_end_time - page_start_time).total_seconds()
                        logger.info(f"Completed page {page}/{total_pages} for shop {shop.name} in {page_duration:.2f}s - "
                                   f"Orders: +{orders_created}/~{orders_updated}, Items: +{items_created}")
                        
                    except Exception as process_error:
                        error_msg = f"Processing error for shop {shop.name} page {page}: {str(process_error)}"
                        logger.error(error_msg, exc_info=True)
                        result.errors.append(error_msg)
                        
                        # Reset database connection and continue
                        _reset_database_connection()
                        
                except Exception as mapping_error:
                    error_msg = f"Mapping error for shop {shop.name} page {page}: {str(mapping_error)}"
                    logger.error(error_msg, exc_info=True)
                    result.errors.append(error_msg)
                    
            except Exception as page_error:
                error_msg = f"Page error {page} for shop {shop.name}: {str(page_error)}"
                logger.error(error_msg, exc_info=True)
                result.errors.append(error_msg)
                
                # For critical errors, we might want to stop processing
                if "timeout" in str(page_error).lower() or "connection" in str(page_error).lower():
                    logger.error(f"Connection error for shop {shop.name}, will retry in 30 seconds")
                    import time
                    time.sleep(30)
            
            processed_pages += 1
            page += 1
            
            # Add small delay between pages to avoid overwhelming the API
            import time
            time.sleep(0.1)
            
            # Progress update every 10 pages
            if processed_pages % 10 == 0:
                logger.info(f"Progress update - Shop {shop.name}: {processed_pages} pages completed, "
                           f"{result.orders_created + result.orders_updated} total orders processed")
        
        completion_time = _get_vietnam_time()
        logger.info(f"COMPLETED sync for shop {shop.name}: {processed_pages}/{total_pages} pages processed")
        logger.info(f"Results - Orders: +{result.orders_created}/~{result.orders_updated}, "
                   f"Items: +{result.items_created}, Addresses: +{result.addresses_created}, "
                   f"Errors: {len(result.errors)}")
            
    except requests.RequestException as e:
        error_msg = f"Shop {shop.name}: Network error - {str(e)}"
        logger.error(error_msg)
        result.errors.append(error_msg)
    except Exception as e:
        error_msg = f"Shop {shop.name}: Unexpected error - {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.errors.append(error_msg)
    
    return result

# ===== MAIN CELERY TASK =====
@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def sync_orders_task(self, shop_ids=None):
    """
    Celery task để đồng bộ đơn hàng từ Pancake API với date range (30 ngày gần nhất)
    
    Args:
        shop_ids (list, optional): Danh sách shop IDs để sync. Nếu None thì sync tất cả shops.
    
    Returns:
        dict: Kết quả sync với thống kê chi tiết
    """
    vietnam_start_time = _get_vietnam_time()
    
    try:
        # Get date range for last 30 days
        start_timestamp, end_timestamp, start_date, end_date = _get_date_range_timestamps()
        
        logger.info(f"[TASK] Starting orders sync task with date range: {start_date.strftime('%Y-%m-%d %H:%M:%S')} to {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"[TASK] Timestamp range: {start_timestamp} to {end_timestamp}")
        
        # Get shops to sync
        if shop_ids:
            shops = Shop.objects.filter(id__in=shop_ids)
            logger.info(f"[TASK] Syncing specific shops: {shop_ids}")
        else:
            shops = Shop.objects.all()
            logger.info(f"[TASK] Syncing all {shops.count()} shops")
        
        if not shops.exists():
            error_msg = "No shops found to sync"
            logger.error(f"[TASK] {error_msg}")
            return {
                'success': False,
                'message': error_msg,
                'data': {},
                'timestamp': vietnam_start_time.isoformat()
            }
        
        total_result = OrderSyncResult()
        
        # Create sync history record
        sync_history = SyncHistory.objects.create(
            sync_type='orders',
            status='running',
            total_records=0,
            started_at=vietnam_start_time,
            error_details={
                'date_range': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'start_timestamp': start_timestamp,
                    'end_timestamp': end_timestamp
                },
                'shop_count': shops.count(),
                'shop_ids': list(shops.values_list('id', flat=True)) if shop_ids else 'all'
            }
        )
        
        logger.info(f"[TASK] Created sync history record: {sync_history.id}")
        
        try:
            # Process each shop
            for shop_index, shop in enumerate(shops, 1):
                shop_start_time = _get_vietnam_time()
                logger.info(f"[TASK] Processing shop {shop_index}/{shops.count()}: {shop.name} (ID: {shop.pancake_id})")
                
                # Update task progress if bound
                if hasattr(self, 'update_state'):
                    self.update_state(
                        state='PROGRESS',
                        meta={
                            'current_shop': shop_index,
                            'total_shops': shops.count(),
                            'shop_name': shop.name,
                            'orders_processed': total_result.orders_created + total_result.orders_updated,
                            'start_time': vietnam_start_time.isoformat()
                        }
                    )
                
                try:
                    # Sync single shop with date range
                    shop_result = _sync_shop_orders_with_date_range(
                        shop, start_timestamp, end_timestamp, start_date, end_date
                    )
                    
                    # Aggregate results
                    total_result.orders_created += shop_result.orders_created
                    total_result.orders_updated += shop_result.orders_updated
                    total_result.items_created += shop_result.items_created
                    total_result.addresses_created += shop_result.addresses_created
                    total_result.partners_created += shop_result.partners_created
                    total_result.warehouses_created += shop_result.warehouses_created
                    total_result.histories_created += shop_result.histories_created
                    total_result.errors.extend(shop_result.errors)
                    
                    shop_end_time = _get_vietnam_time()
                    shop_duration = (shop_end_time - shop_start_time).total_seconds()
                    
                    logger.info(f"[TASK] Shop {shop_index}/{shops.count()} - {shop.name} completed in {shop_duration:.2f}s: "
                               f"Orders: +{shop_result.orders_created}/~{shop_result.orders_updated}, "
                               f"Items: +{shop_result.items_created}, "
                               f"Addresses: +{shop_result.addresses_created}, "
                               f"Errors: {len(shop_result.errors)}")
                    
                except Exception as shop_error:
                    error_msg = f"Error processing shop {shop.name}: {str(shop_error)}"
                    logger.error(f"[TASK] {error_msg}", exc_info=True)
                    total_result.errors.append(error_msg)
                    
                    # Continue with next shop instead of failing entire task
                    continue
                
                # Update sync progress periodically
                if shop_index % 5 == 0 or shop_index == shops.count():
                    sync_history.total_records = total_result.orders_created + total_result.orders_updated
                    sync_history.created_records = total_result.orders_created
                    sync_history.updated_records = total_result.orders_updated
                    sync_history.failed_records = len(total_result.errors)
                    sync_history.save()
                    
                    logger.info(f"[TASK] Progress update: {shop_index}/{shops.count()} shops completed, "
                               f"{total_result.orders_created + total_result.orders_updated} orders processed")
            
            # Complete sync history
            vietnam_end_time = _get_vietnam_time()
            total_duration = (vietnam_end_time - vietnam_start_time).total_seconds()
            
            sync_history.status = 'completed' if not total_result.errors else 'completed_with_errors'
            sync_history.created_records = total_result.orders_created
            sync_history.updated_records = total_result.orders_updated
            sync_history.failed_records = len(total_result.errors)
            sync_history.total_records = total_result.orders_created + total_result.orders_updated
            sync_history.finished_at = vietnam_end_time
            sync_history.error_message = '; '.join(total_result.errors[:5]) if total_result.errors else None
            
            # Update error details with final stats
            sync_history.error_details.update({
                'final_stats': {
                    'orders_created': total_result.orders_created,
                    'orders_updated': total_result.orders_updated,
                    'items_created': total_result.items_created,
                    'addresses_created': total_result.addresses_created,
                    'partners_created': total_result.partners_created,
                    'warehouses_created': total_result.warehouses_created,
                    'histories_created': total_result.histories_created,
                    'total_errors': len(total_result.errors),
                    'duration_seconds': total_duration,
                    'duration_minutes': total_duration / 60
                },
                'errors': total_result.errors[:20] if total_result.errors else []  # Store first 20 errors
            })
            sync_history.save()
            
            logger.info(f"[TASK] COMPLETED in {total_duration:.2f}s ({total_duration/60:.1f} minutes)")
            logger.info(f"[TASK] FINAL RESULTS: Orders: +{total_result.orders_created}/~{total_result.orders_updated}, "
                       f"Items: +{total_result.items_created}, Addresses: +{total_result.addresses_created}, "
                       f"Partners: +{total_result.partners_created}, Warehouses: +{total_result.warehouses_created}, "
                       f"Histories: +{total_result.histories_created}, Total Errors: {len(total_result.errors)}")
            
            # Prepare success response
            success_message = (
                f'Đồng bộ hoàn tất: {total_result.orders_created} đơn hàng mới, '
                f'{total_result.orders_updated} đơn hàng cập nhật, '
                f'{total_result.items_created} sản phẩm trong {shops.count()} shop'
            )
            
            if total_result.errors:
                success_message += f', {len(total_result.errors)} lỗi'
            
            return {
                'success': len(total_result.errors) == 0,
                'message': success_message,
                'data': {
                    'orders_created': total_result.orders_created,
                    'orders_updated': total_result.orders_updated,
                    'items_created': total_result.items_created,
                    'addresses_created': total_result.addresses_created,
                    'partners_created': total_result.partners_created,
                    'warehouses_created': total_result.warehouses_created,
                    'histories_created': total_result.histories_created,
                    'total_shops_processed': shops.count(),
                    'errors_count': len(total_result.errors),
                    'error_details': total_result.errors[:10],  # First 10 errors
                    'duration_seconds': total_duration,
                    'duration_minutes': total_duration / 60,
                    'date_range': {
                        'start_date': start_date.strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'days_covered': 30
                    },
                    'sync_history_id': sync_history.id
                },
                'timestamp': vietnam_end_time.isoformat()
            }
            
        except Exception as e:
            # Handle task-level errors
            vietnam_error_time = _get_vietnam_time()
            error_duration = (vietnam_error_time - vietnam_start_time).total_seconds()
            
            sync_history.status = 'failed'
            sync_history.error_message = str(e)
            sync_history.finished_at = vietnam_error_time
            sync_history.error_details.update({
                'task_error': str(e),
                'partial_results': {
                    'orders_created': total_result.orders_created,
                    'orders_updated': total_result.orders_updated,
                    'items_created': total_result.items_created,
                    'addresses_created': total_result.addresses_created,
                },
                'duration_seconds': error_duration
            })
            sync_history.save()
            
            logger.error(f"[TASK] FAILED after {error_duration:.2f}s: {str(e)}", exc_info=True)
            
            # Retry logic for certain errors
            if self.request.retries < self.max_retries:
                if "timeout" in str(e).lower() or "connection" in str(e).lower():
                    logger.info(f"[TASK] Retrying due to connection error (attempt {self.request.retries + 1}/{self.max_retries})")
                    raise self.retry(countdown=300, exc=e)  # Retry after 5 minutes
            
            return {
                'success': False,
                'message': f'Lỗi đồng bộ đơn hàng: {str(e)}',
                'data': {
                    'partial_results': {
                        'orders_created': total_result.orders_created,
                        'orders_updated': total_result.orders_updated,
                        'items_created': total_result.items_created,
                        'addresses_created': total_result.addresses_created,
                    },
                    'error': str(e),
                    'retry_count': self.request.retries,
                    'max_retries': self.max_retries,
                    'sync_history_id': sync_history.id,
                    'duration_seconds': error_duration
                },
                'timestamp': vietnam_error_time.isoformat()
            }
            
    except Exception as critical_error:
        # Handle critical errors that prevent task initialization
        vietnam_critical_time = _get_vietnam_time()
        critical_duration = (vietnam_critical_time - vietnam_start_time).total_seconds()
        
        logger.error(f"[TASK] CRITICAL ERROR after {critical_duration:.2f}s: {str(critical_error)}", exc_info=True)
        
        return {
            'success': False,
            'message': f'Lỗi nghiêm trọng: {str(critical_error)}',
            'data': {
                'error': str(critical_error),
                'error_type': 'critical',
                'duration_seconds': critical_duration
            },
            'timestamp': vietnam_critical_time.isoformat()
        }

# ===== CONVENIENCE TASKS FOR SPECIFIC USE CASES =====

@shared_task(bind=True)
def sync_single_shop_orders(self, shop_id):
    """Task để đồng bộ đơn hàng cho 1 shop cụ thể"""
    return sync_orders_task.apply_async(args=[[shop_id]], task_id=f"sync_shop_{shop_id}")

@shared_task(bind=True)
def sync_orders_daily(self):
    """Task đồng bộ hàng ngày - có thể schedule với celery beat"""
    logger.info("[DAILY_SYNC] Starting daily orders sync")
    return sync_orders_task.apply_async(task_id="daily_orders_sync")

@shared_task
def sync_orders_status_check(task_id):
    """Check status của sync task"""
    try:
        from celery.result import AsyncResult
        result = AsyncResult(task_id)
        
        return {
            'task_id': task_id,
            'status': result.status,
            'result': result.result if result.ready() else None,
            'info': result.info if hasattr(result, 'info') else None,
            'timestamp': _get_vietnam_time().isoformat()
        }
    except Exception as e:
        return {
            'task_id': task_id,
            'status': 'ERROR',
            'error': str(e),
            'timestamp': _get_vietnam_time().isoformat()
        }

# ===== UTILITY FUNCTIONS FOR TASK MANAGEMENT =====

def get_running_sync_tasks():
    """Get list of currently running sync tasks"""
    try:
        from celery import current_app
        inspect = current_app.control.inspect()
        
        active_tasks = inspect.active()
        sync_tasks = []
        
        if active_tasks:
            for worker, tasks in active_tasks.items():
                for task in tasks:
                    if 'sync_orders' in task.get('name', ''):
                        sync_tasks.append({
                            'worker': worker,
                            'task_id': task.get('id'),
                            'name': task.get('name'),
                            'args': task.get('args'),
                            'started': task.get('time_start'),
                            'hostname': task.get('hostname')
                        })
        
        return sync_tasks
    except Exception as e:
        logger.error(f"Error getting running sync tasks: {e}")
        return []

def cancel_sync_task(task_id):
    """Cancel a running sync task"""
    try:
        from celery import current_app
        current_app.control.revoke(task_id, terminate=True)
        
        logger.info(f"Cancelled sync task: {task_id}")
        return {'success': True, 'message': f'Task {task_id} cancelled'}
    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {e}")
        return {'success': False, 'error': str(e)}

def get_sync_task_stats():
    """Get statistics about sync tasks"""
    try:
        # Get recent sync histories
        recent_syncs = SyncHistory.objects.filter(
            sync_type='orders'
        ).order_by('-started_at')[:20]
        
        stats = {
            'total_syncs': recent_syncs.count(),
            'running_syncs': recent_syncs.filter(status='running').count(),
            'completed_syncs': recent_syncs.filter(status='completed').count(),
            'failed_syncs': recent_syncs.filter(status='failed').count(),
            'recent_syncs': []
        }
        
        for sync in recent_syncs[:10]:
            sync_data = {
                'id': sync.id,
                'status': sync.status,
                'started_at': sync.started_at.strftime('%Y-%m-%d %H:%M:%S') if sync.started_at else None,
                'finished_at': sync.finished_at.strftime('%Y-%m-%d %H:%M:%S') if sync.finished_at else None,
                'total_records': sync.total_records,
                'created_records': sync.created_records,
                'updated_records': sync.updated_records,
                'failed_records': sync.failed_records,
            }
            
            if sync.started_at and sync.finished_at:
                duration = (sync.finished_at - sync.started_at).total_seconds()
                sync_data['duration_minutes'] = round(duration / 60, 2)
            
            stats['recent_syncs'].append(sync_data)
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting sync task stats: {e}")
        return {'error': str(e)}

# ===== MONITORING AND HEALTH CHECK =====

@shared_task
def sync_orders_health_check():
    """Health check task để kiểm tra khả năng đồng bộ"""
    try:
        vietnam_now = _get_vietnam_time()
        
        # Check database connectivity
        shops_count = Shop.objects.count()
        orders_count = Order.objects.count()
        
        # Check API connectivity with a simple request
        if shops_count > 0:
            first_shop = Shop.objects.first()
            try:
                start_ts, end_ts, _, _ = _get_date_range_timestamps()
                test_response = _fetch_orders_page_with_date_range(first_shop.pancake_id, start_ts, end_ts, 1, 1)
                api_status = 'OK' if test_response.get('success') else 'ERROR'
            except Exception as api_error:
                api_status = f'ERROR: {str(api_error)}'
        else:
            api_status = 'NO_SHOPS'
        
        return {
            'status': 'OK',
            'timestamp': vietnam_now.isoformat(),
            'database': {
                'shops_count': shops_count,
                'orders_count': orders_count,
                'connection': 'OK'
            },
            'api': {
                'status': api_status,
                'base_url': settings.PANCAKE_API_BASE_URL
            },
            'timezone': 'GMT+7 (Vietnam)',
            'date_range_info': {
                'last_30_days': f"{vietnam_now - timedelta(days=30)} to {vietnam_now}"
            }
        }
        
    except Exception as e:
        return {
            'status': 'ERROR',
            'error': str(e),
            'timestamp': _get_vietnam_time().isoformat()
        }