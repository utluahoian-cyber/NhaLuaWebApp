from django.db import models
from django.utils import timezone

class Shop(models.Model):
    # ID từ Pancake API
    pancake_id = models.IntegerField(unique=True)
    
    # Thông tin cơ bản
    name = models.CharField(max_length=255)
    currency = models.CharField(max_length=10, default='VND')
    avatar_url = models.URLField(blank=True, null=True)
    
    # Link post marketer (JSON field)
    link_post_marketer = models.JSONField(default=list, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_sync = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'shops'
        verbose_name = 'Cửa hàng'
        verbose_name_plural = 'Cửa hàng'
    
    def __str__(self):
        return self.name

class Page(models.Model):
    PLATFORM_CHOICES = [
        ('facebook', 'Facebook'),
        ('instagram_official', 'Instagram Official'),
    ]
    
    # Quan hệ với Shop
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name='pages')
    
    # ID từ Pancake API
    pancake_id = models.CharField(max_length=100)
    name = models.CharField(max_length=255)
    platform = models.CharField(max_length=50, choices=PLATFORM_CHOICES)
    username = models.CharField(max_length=255, blank=True, null=True)
    
    # Cài đặt
    is_onboard_xendit = models.BooleanField(null=True, blank=True)
    progressive_catalog_error = models.CharField(max_length=255, blank=True, null=True)
    settings = models.JSONField(default=dict, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'pages'
        verbose_name = 'Trang'
        verbose_name_plural = 'Trang'
        unique_together = ['shop', 'pancake_id']
    
    def __str__(self):
        return f"{self.name} ({self.platform})"

class Tag(models.Model):
    # Quan hệ với Page
    page = models.ForeignKey(Page, on_delete=models.CASCADE, related_name='tags')
    
    # Thông tin tag
    pancake_id = models.IntegerField()
    text = models.CharField(max_length=255)
    color = models.CharField(max_length=32)  # Hex color code
    lighten_color = models.CharField(max_length=32)  # Có thể là hex hoặc rgba
    description = models.TextField(blank=True, null=True)
    is_lead_event = models.BooleanField(default=False)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'tags'
        verbose_name = 'Tag'
        verbose_name_plural = 'Tags'
        unique_together = ['page', 'pancake_id']
    
    def __str__(self):
        return f"{self.text} ({self.page.name})"

class Category(models.Model):
    # Quan hệ với Shop
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name='categories')
    
    # ID từ Pancake API
    pancake_id = models.IntegerField()
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    
    # Parent category cho nested categories
    parent = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True, related_name='children')
    
    # Thứ tự hiển thị
    sort_order = models.IntegerField(default=0)
    
    # Trạng thái
    is_active = models.BooleanField(default=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'categories'
        verbose_name = 'Danh mục'
        verbose_name_plural = 'Danh mục'
        unique_together = ['shop', 'pancake_id']
        ordering = ['sort_order', 'name']
    
    def __str__(self):
        return f"{self.name} ({self.shop.name})"
    
class Product(models.Model):
    # Quan hệ với Shop
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name='products')
    
    # ID từ Pancake API
    pancake_id = models.CharField(max_length=100)
    display_id = models.CharField(max_length=100)
    name = models.CharField(max_length=255)
    
    # Thông tin sản phẩm
    image_url = models.URLField(blank=True, null=True)
    note_product = models.TextField(blank=True, null=True)
    
    # Trạng thái
    is_published = models.BooleanField(null=True, blank=True)
    
    # Quan hệ many-to-many với categories
    categories = models.ManyToManyField(Category, blank=True, related_name='products')
    
    # Tags (JSON field để lưu danh sách tags)
    tags = models.JSONField(default=list, blank=True)
    
    # Warehouses
    manipulation_warehouses = models.JSONField(default=list, blank=True)
    
    # Metadata
    inserted_at = models.DateTimeField()  # Từ API
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_sync = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'products'
        verbose_name = 'Sản phẩm'
        verbose_name_plural = 'Sản phẩm'
        unique_together = ['shop', 'pancake_id']
    
    def __str__(self):
        return f"{self.name} ({self.display_id})"

class ProductVariationField(models.Model):
    """Lưu thông tin các trường variation như màu sắc, size"""
    # ID từ Pancake API
    pancake_id = models.CharField(max_length=100)
    name = models.CharField(max_length=100)  
    key_value = models.CharField(max_length=50)  
    value = models.CharField(max_length=100)  
    
    class Meta:
        db_table = 'product_variation_fields'
        verbose_name = 'Trường variation'
        verbose_name_plural = 'Trường variation'
    
    def __str__(self):
        return f"{self.name}: {self.value}"

class ProductVariation(models.Model):
    # Quan hệ với Product
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name='variations')
    
    # ID từ Pancake API
    pancake_id = models.CharField(max_length=100)
    display_id = models.CharField(max_length=100)
    barcode = models.CharField(max_length=100, blank=True, null=True)
    
    # Thông tin giá
    retail_price = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    retail_price_after_discount = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    price_at_counter = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    total_purchase_price = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    last_imported_price = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    
    # Wholesale prices (JSON field)
    wholesale_price = models.JSONField(default=list, blank=True)
    
    # Thông tin kho
    remain_quantity = models.IntegerField(default=0)
    weight = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    
    # Trạng thái
    is_composite = models.BooleanField(default=False,null=True, blank=True,) 
    is_hidden = models.BooleanField(default=False)
    is_locked = models.BooleanField(default=False)
    is_removed = models.BooleanField(null=True, blank=True, default=None)
    is_sell_negative_variation = models.BooleanField(default=False)
    
    # Media
    images = models.JSONField(default=list, blank=True)
    videos = models.JSONField(null=True, blank=True)
    
    # Relationships
    fields = models.ManyToManyField(ProductVariationField, blank=True, related_name='variations')
    
    # Composite và bonus (JSON fields)
    composite_products = models.JSONField(default=list, blank=True)
    bonus_variations = models.JSONField(default=list, blank=True)
    
    # Warehouses
    variations_warehouses = models.JSONField(default=list, blank=True)
    
    # Metadata
    inserted_at = models.DateTimeField()  # Từ API
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_sync = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'product_variations'
        verbose_name = 'Biến thể sản phẩm'
        verbose_name_plural = 'Biến thể sản phẩm'
        unique_together = ['product', 'pancake_id']
    
    def __str__(self):
        return f"{self.display_id} - {self.product.name}"
    
    def get_field_display(self):
        """Trả về string hiển thị các trường variation"""
        field_values = []
        for field in self.fields.all():
            field_values.append(f"{field.name}: {field.value}")
        return " | ".join(field_values)

# Bảng trung gian để lưu lịch sử sync
class SyncHistory(models.Model):
    SYNC_TYPES = [
        ('shops', 'Shops'),
        ('pages', 'Pages'),
        ('tags', 'Tags'),
        ('categories', 'Categories'),
        ('products', 'Products'),
        ('variations', 'Product Variations'),
        ('customers', 'Customers'),  # Thêm customers
        ('users', 'Users'),  # Thêm users
         ('orders', 'Orders'),
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    sync_type = models.CharField(max_length=50, choices=SYNC_TYPES)
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name='sync_histories', null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
    # Thống kê
    total_records = models.IntegerField(default=0)
    processed_records = models.IntegerField(default=0)
    created_records = models.IntegerField(default=0)
    updated_records = models.IntegerField(default=0)
    failed_records = models.IntegerField(default=0)
    
    # Thông tin lỗi
    error_message = models.TextField(blank=True, null=True)
    error_details = models.JSONField(default=dict, blank=True)
    
    # Thời gian
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        db_table = 'sync_histories'
        verbose_name = 'Lịch sử đồng bộ'
        verbose_name_plural = 'Lịch sử đồng bộ'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"Sync {self.sync_type} - {self.status} ({self.started_at})"
    

class User(models.Model):
    """Model cho creator/assigned user từ Pancake API"""
    # ID từ Pancake API
    pancake_id = models.CharField(max_length=100, unique=True)
    
    # Thông tin cơ bản
    name = models.CharField(max_length=255)
    avatar_url = models.URLField(blank=True, null=True)
    fb_id = models.CharField(max_length=100, blank=True, null=True)
    phone_number = models.CharField(max_length=20, blank=True, null=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_sync = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'users'
        verbose_name = 'Người dùng'
        verbose_name_plural = 'Người dùng'
    
    def __str__(self):
        return self.name

class Customer(models.Model):
    GENDER_CHOICES = [
        ('male', 'Nam'),
        ('female', 'Nữ'),
        ('other', 'Khác'),
    ]
    
    # Quan hệ với Shop
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name='customers')
    
    # ID từ Pancake API
    pancake_id = models.CharField(max_length=100)  # id field từ API
    customer_id = models.CharField(max_length=100)  # customer_id field từ API
    
    # Thông tin cơ bản
    name = models.CharField(max_length=255, blank=True, null=True)
    username = models.CharField(max_length=255, blank=True, null=True)
    gender = models.CharField(max_length=10, choices=GENDER_CHOICES, blank=True, null=True)
    date_of_birth = models.DateField(blank=True, null=True)
    
    # Thông tin liên hệ
    phone_numbers = models.JSONField(default=list, blank=True)  # Array of phone numbers
    emails = models.JSONField(default=list, blank=True)  # Array of emails
    
    # Social media
    fb_id = models.CharField(max_length=255, blank=True, null=True)
    
    # Thông tin tài chính
    current_debts = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    purchased_amount = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    total_amount_referred = models.DecimalField(max_digits=15, decimal_places=2, blank=True, null=True)
    
    # Điểm thưởng
    reward_point = models.IntegerField(default=0)
    used_reward_point = models.IntegerField(blank=True, null=True)
    
    # Đơn hàng
    order_count = models.IntegerField(default=0)
    succeed_order_count = models.IntegerField(default=0)
    returned_order_count = models.IntegerField(default=0)
    last_order_at = models.DateTimeField(blank=True, null=True)
    
    # Giới thiệu
    referral_code = models.CharField(max_length=50, blank=True, null=True)
    count_referrals = models.IntegerField(default=0)
    
    # Trạng thái
    is_block = models.BooleanField(default=False)
    is_discount_by_level = models.BooleanField(default=True, null=True, blank=True)
    is_adjust_debts = models.BooleanField(blank=True, null=True)
    active_levera_pay = models.BooleanField(default=False)
    
    # Quan hệ với User
    creator = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='created_customers')
    assigned_user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='assigned_customers')
    
    # Khác
    level = models.CharField(max_length=100, blank=True, null=True)
    currency = models.CharField(max_length=10, blank=True, null=True)
    user_block_id = models.CharField(max_length=100, blank=True, null=True)
    conversation_tags = models.JSONField(blank=True, null=True)
    
    # Arrays từ API
    order_sources = models.JSONField(default=list, blank=True, null=True)
    tags = models.JSONField(default=list, blank=True, null=True)  # Customer tags
    list_voucher = models.JSONField(default=list, blank=True, null=True)
    notes = models.JSONField(default=list, blank=True, null=True)
    
    # Metadata
    inserted_at = models.DateTimeField()  # Từ API
    updated_at_api = models.DateTimeField()  # updated_at từ API
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_sync = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'customers'
        verbose_name = 'Khách hàng'
        verbose_name_plural = 'Khách hàng'
        unique_together = ['shop', 'pancake_id']    
        indexes = [
            models.Index(fields=['referral_code']),
            models.Index(fields=['shop', 'customer_id']),  
            models.Index(fields=['fb_id']),
        ]
    
    def __str__(self):
        return f"{self.name} - {self.shop.name}"
    
    @property
    def primary_phone(self):
        """Trả về số điện thoại chính"""
        return self.phone_numbers[0] if self.phone_numbers else None
    
    @property
    def primary_email(self):
        """Trả về email chính"""
        return self.emails[0] if self.emails else None

class CustomerAddress(models.Model):
    """Địa chỉ khách hàng"""
    # Quan hệ với Customer
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name='addresses')
    
    # ID từ Pancake API
    pancake_id = models.CharField(max_length=100)
    
    # Thông tin người nhận
    full_name = models.CharField(max_length=255,null=True, blank=True)
    phone_number = models.CharField(max_length=20, blank=True, null=True)
    
    # Địa chỉ
    address = models.TextField(null=True, blank=True)  
    full_address = models.TextField(null=True, blank=True) 
    post_code = models.CharField(max_length=10, blank=True, null=True)
    
    # Mã vùng
    country_code = models.IntegerField(default=84, null=True, blank=True)
    province_id = models.CharField(max_length=10, blank=True, null=True)
    district_id = models.CharField(max_length=10, blank=True, null=True)
    commune_id = models.CharField(max_length=20, blank=True, null=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_sync = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'customer_addresses'
        verbose_name = 'Địa chỉ khách hàng'
        verbose_name_plural = 'Địa chỉ khách hàng'
        unique_together = ['customer', 'pancake_id']
    
    def __str__(self):
        return f"{self.full_name} - {self.address}"
    
class Order(models.Model):
    STATUS_CHOICES = [
        (0, 'Mới'),
        (17, 'Chờ xác nhận'),
        (11, 'Chờ hàng'),
        (12, 'Chờ in'),
        (13, 'Đã in'),
        (20, 'Đã đặt hàng'),
        (1, 'Đã xác nhận'),
        (8, 'Đang đóng hàng'),
        (9, 'Chờ chuyển hàng'),
        (2, 'Đã gửi hàng'),
        (3, 'Đã nhận'),
        (16, 'Đã thu tiền'),
        (4, 'Đang hoàn'),
        (15, 'Hoàn một phần'),
        (5, 'Đã hoàn'),
        (6, 'Đã hủy'),
        (7, 'Đã xóa'),
    ]
    
    ORDER_SOURCE_CHOICES = [
        (-1, 'Facebook'),
        (23532, 'Telesale'),
        (1228024055, 'Tự ship'),
        (0, 'Hệ thống'),  # Thêm option hệ thống
    ]
    ORDER_SOURCE_NAME_CHOICES = [
        ('Facebook', 'Facebook'),
        ('TELESALE', 'Telesale'),
        ('TỰ SHIP', 'Tự ship'),
        ('Hệ thống', 'Hệ thống'),
    ]
    
    # Quan hệ
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, related_name='orders')
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name='orders')
    page = models.ForeignKey(Page, on_delete=models.SET_NULL, null=True, blank=True, related_name='orders')
    
    # Users
    creator = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='created_orders')
    assigning_seller = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='assigned_seller_orders')
    assigning_care = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='assigned_care_orders')
    marketer = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='marketer_orders')
    last_editor = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='edited_orders')
    
    # ID fields
    pancake_id = models.CharField(max_length=100, unique=True)  # id từ API
    system_id = models.IntegerField(unique=True)  # system_id từ API
    
    # Basic info
    status = models.IntegerField(choices=STATUS_CHOICES, default=0)
    sub_status = models.CharField(max_length=50, blank=True, null=True)
    order_sources = models.CharField(max_length=50, choices=ORDER_SOURCE_CHOICES,null=True, blank=True)
    order_sources_name = models.CharField(max_length=50, choices=ORDER_SOURCE_NAME_CHOICES)
    
    # Pricing
    total_price = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    total_discount = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    total_price_after_sub_discount = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    shipping_fee = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    partner_fee = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    tax = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    cod = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    prepaid = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    transfer_money = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    money_to_collect = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    
    # Payment info
    charged_by_card = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    charged_by_momo = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    charged_by_qrpay = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    cash = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    exchange_payment = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    exchange_value = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    surcharge = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    levera_point = models.IntegerField(default=0)
    
    # Payment structures (JSON)
    bank_payments = models.JSONField(default=dict,null=True, blank=True)
    prepaid_by_point = models.JSONField(default=dict, null=True, blank=True)
    advanced_platform_fee = models.JSONField(default=dict, null=True, blank=True)
    
    # Billing info
    bill_full_name = models.CharField(max_length=255, blank=True, null=True)
    bill_phone_number = models.CharField(max_length=20, blank=True, null=True)
    bill_email = models.EmailField(blank=True, null=True)
    
    # Flags
    is_free_shipping = models.BooleanField(default=False, null=True, blank=True)
    is_livestream = models.BooleanField(default=False, null=True, blank=True)
    is_live_shopping = models.BooleanField(default=False, null=True, blank=True)
    is_exchange_order = models.BooleanField(default=False, null=True, blank=True)
    is_smc = models.BooleanField(default=False, null=True, blank=True)
    customer_pay_fee = models.BooleanField(default=False, null=True, blank=True)
    received_at_shop = models.BooleanField(default=False, null=True, blank=True)
    return_fee = models.BooleanField(default=False, null=True, blank=True)
    
    # Warehouse
    warehouse_id = models.CharField(max_length=100, blank=True, null=True)
    
    # Notes and links
    note = models.TextField(blank=True, null=True)
    note_print = models.TextField(blank=True, null=True)
    note_image = models.URLField(blank=True, null=True)
    link = models.URLField(blank=True, null=True)
    link_confirm_order = models.URLField(blank=True, null=True)
    order_link = models.URLField(blank=True, null=True)
    
    # Social media
    account = models.CharField(max_length=100, blank=True, null=True)
    account_name = models.CharField(max_length=255, blank=True, null=True)
    page_external_id = models.CharField(max_length=100, blank=True, null=True)
    conversation_id = models.CharField(max_length=255, blank=True, null=True)
    post_id = models.CharField(max_length=255, blank=True, null=True)
    ad_id = models.CharField(max_length=255, blank=True, null=True)
    ads_source = models.CharField(max_length=100, blank=True, null=True)
    
    # UTM tracking
    p_utm_source = models.CharField(max_length=255, blank=True, null=True)
    p_utm_medium = models.CharField(max_length=255, blank=True, null=True)
    p_utm_campaign = models.CharField(max_length=255, blank=True, null=True)
    p_utm_content = models.CharField(max_length=255, blank=True, null=True)
    p_utm_term = models.CharField(max_length=255, blank=True, null=True)
    p_utm_id = models.CharField(max_length=255, blank=True, null=True)
    
    # Referral
    customer_referral_code = models.CharField(max_length=50, blank=True, null=True)
    pke_mkter = models.CharField(max_length=100, blank=True, null=True)
    
    # Marketplace
    marketplace_id = models.CharField(max_length=100, blank=True, null=True)
    fee_marketplace = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    
    # Arrays (JSON)
    tags = models.JSONField(default=list, blank=True)
    customer_needs = models.JSONField(default=list, blank=True)
    activated_combo_products = models.JSONField(default=list, blank=True)
    activated_promotion_advances = models.JSONField(default=list, blank=True)
    payment_purchase_histories = models.JSONField(default=list, blank=True)
    
    # Quantities
    total_quantity = models.IntegerField(default=0)
    items_length = models.IntegerField(default=0)
    
    # Returned info
    returned_reason = models.TextField(blank=True, null=True)
    returned_reason_name = models.CharField(max_length=255, blank=True, null=True)
    
    # Dates and times
    time_assign_seller = models.DateTimeField(blank=True, null=True)
    time_assign_care = models.DateTimeField(blank=True, null=True)
    time_send_partner = models.DateTimeField(blank=True, null=True)
    estimate_delivery_date = models.DateTimeField(blank=True, null=True)
    buyer_total_amount = models.DecimalField(max_digits=15, decimal_places=2, blank=True, null=True)
    
    # API timestamps
    inserted_at = models.DateTimeField()
    updated_at_api = models.DateTimeField()  # updated_at từ API
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_sync = models.DateTimeField(default=timezone.now)
    
    # Currency
    order_currency = models.CharField(max_length=10, default='VND')
    
    class Meta:
        db_table = 'orders'
        verbose_name = 'Đơn hàng'
        verbose_name_plural = 'Đơn hàng'
        unique_together = ['shop', 'pancake_id']
        indexes = [
            models.Index(fields=['shop', 'status']),
            models.Index(fields=['customer']),
            models.Index(fields=['system_id']),
            models.Index(fields=['order_sources']),
            models.Index(fields=['inserted_at']),
        ]
    
    def __str__(self):
        return f"Order #{self.system_id} - {self.bill_full_name}"

class OrderShippingAddress(models.Model):
    """Địa chỉ giao hàng của đơn hàng"""
    order = models.OneToOneField(Order, on_delete=models.CASCADE, related_name='shipping_address')
    
    # Address info
    full_name = models.CharField(max_length=255, null=True, blank=True)
    phone_number = models.CharField(max_length=20, null=True, blank=True)
    address = models.TextField(null=True, blank=True)
    full_address = models.TextField(null=True, blank=True)
    
    # Location codes
    country_code = models.CharField(max_length=10, default='84', null=True, blank=True)
    province_id = models.CharField(max_length=10, null=True, blank=True)
    province_name = models.CharField(max_length=100, null=True, blank=True)
    district_id = models.CharField(max_length=10, null=True, blank=True)
    district_name = models.CharField(max_length=100, null=True, blank=True)
    commune_id = models.CharField(max_length=20, null=True, blank=True)
    commune_name = models.CharField(max_length=100, null=True, blank=True)
    commnue_name = models.CharField(max_length=100, blank=True, null=True)
    
    # New format
    new_province_id = models.CharField(max_length=20, blank=True, null=True)
    new_commune_id = models.CharField(max_length=20, blank=True, null=True)
    new_full_address = models.TextField(blank=True, null=True)
    
    # Post code and marketplace
    post_code = models.CharField(max_length=10, blank=True, null=True)
    marketplace_address = models.TextField(blank=True, null=True)
    render_type = models.CharField(max_length=20, default='old', null=True, blank=True)
    
    # Sicepat integration
    commune_code_sicepat = models.CharField(max_length=20, blank=True, null=True)
    
    class Meta:
        db_table = 'order_shipping_addresses'
        verbose_name = 'Địa chỉ giao hàng'
        verbose_name_plural = 'Địa chỉ giao hàng'

class OrderWarehouse(models.Model):
    """Thông tin kho hàng"""
    order = models.OneToOneField(Order, on_delete=models.CASCADE, related_name='warehouse')
    
    # Warehouse info
    name = models.CharField(max_length=255)
    address = models.TextField(null=True, blank=True)
    full_address = models.TextField(null=True, blank=True)
    phone_number = models.CharField(max_length=20, null=True, blank=True)
    
    # Location
    province_id = models.CharField(max_length=10, null=True, blank=True)
    district_id = models.CharField(max_length=10, null=True, blank=True)
    commune_id = models.CharField(max_length=20, null=True, blank=True)
    postcode = models.CharField(max_length=10, blank=True, null=True)
    
    # Settings
    settings = models.JSONField(blank=True, null=True)
    has_snappy_service = models.BooleanField(default=False)
    
    # IDs
    custom_id = models.CharField(max_length=100, blank=True, null=True)
    affiliate_id = models.CharField(max_length=100, blank=True, null=True)
    ffm_id = models.CharField(max_length=100, blank=True, null=True)
    
    class Meta:
        db_table = 'order_warehouses'
        verbose_name = 'Kho hàng đơn hàng'
        verbose_name_plural = 'Kho hàng đơn hàng'

class OrderPartner(models.Model):
    """Thông tin đối tác vận chuyển"""
    order = models.OneToOneField(Order, on_delete=models.CASCADE, related_name='partner')
    
    # Partner info
    partner_id = models.IntegerField()
    partner_name = models.CharField(max_length=100, blank=True, null=True )
    partner_status = models.CharField(max_length=50, blank=True, null=True)
    
    # Codes
    extend_code = models.CharField(max_length=100,blank=True, null=True)
    order_number_vtp = models.CharField(max_length=100, blank=True, null=True)
    sort_code = models.CharField(max_length=100, blank=True, null=True)
    custom_partner_id = models.CharField(max_length=100, blank=True, null=True)
    
    # Financial
    cod = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    total_fee = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    
    # Delivery info
    delivery_name = models.CharField(max_length=255, blank=True, null=True)
    delivery_tel = models.CharField(max_length=20, blank=True, null=True)
    count_of_delivery = models.IntegerField(blank=True, null=True)
    
    # Status flags
    system_created = models.BooleanField(default=True,null=True, blank=True,)
    is_returned = models.BooleanField(blank=True, null=True)
    is_ghn_v2 = models.BooleanField(blank=True, null=True)
    
    # Links
    printed_form = models.URLField(blank=True, null=True)
    
    # GHN specific
    order_id_ghn = models.CharField(max_length=100, blank=True, null=True)
    
    # Dates
    first_delivery_at = models.DateTimeField(blank=True, null=True)
    picked_up_at = models.DateTimeField(blank=True, null=True)
    paid_at = models.DateTimeField(blank=True, null=True)
    updated_at_partner = models.DateTimeField(blank=True, null=True)
    
    # Service details (JSON)
    service_partner = models.JSONField(default=dict, blank=True,null=True)
    extend_update = models.JSONField(default=list, blank=True)
    
    class Meta:
        db_table = 'order_partners'
        verbose_name = 'Đối tác vận chuyển'
        verbose_name_plural = 'Đối tác vận chuyển'

class OrderItem(models.Model):
    """Chi tiết sản phẩm trong đơn hàng"""
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='items')
    
    # IDs từ API
    item_id = models.BigIntegerField()  # id từ API
    product = models.ForeignKey(Product, on_delete=models.SET_NULL, null=True, blank=True)
    variation = models.ForeignKey(ProductVariation, on_delete=models.SET_NULL, null=True, blank=True)
    
    # Basic info
    quantity = models.IntegerField(default=1)
    added_to_cart_quantity = models.IntegerField(default=0)
    
    # Pricing
    retail_price = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    discount_each_product = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    same_price_discount = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    total_discount = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    
    # Flags
    is_bonus_product = models.BooleanField(default=False)
    is_composite = models.BooleanField(blank=True, null=True)
    is_discount_percent = models.BooleanField(default=False)
    is_wholesale = models.BooleanField(default=False)
    one_time_product = models.BooleanField(default=False)
    
    # Return info
    return_quantity = models.IntegerField(default=0)
    returned_count = models.IntegerField(default=0)
    returning_quantity = models.IntegerField(default=0)
    exchange_count = models.IntegerField(default=0)
    
    # Composite and measure
    composite_item_id = models.CharField(max_length=100, blank=True, null=True)
    measure_group_id = models.CharField(max_length=100, blank=True, null=True)
    
    # Notes
    note = models.TextField(blank=True, null=True)
    note_product = models.TextField(blank=True, null=True)
    
    # Components (JSON)
    components = models.JSONField(blank=True, null=True)
    
    # Variation info snapshot (JSON) - để lưu trữ thông tin tại thời điểm đặt hàng
    variation_info = models.JSONField(default=dict, blank=True)
    
    class Meta:
        db_table = 'order_items'
        verbose_name = 'Sản phẩm đơn hàng'
        verbose_name_plural = 'Sản phẩm đơn hàng'
        unique_together = ['order', 'item_id']
    
    def __str__(self):
        product_name = self.variation_info.get('name', 'Unknown Product') if self.variation_info else 'Unknown Product'
        return f"{product_name} x{self.quantity}"

class OrderStatusHistory(models.Model):
    """Lịch sử thay đổi trạng thái đơn hàng"""
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='status_history')
    
    # Editor info
    editor = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    editor_fb = models.CharField(max_length=100, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    avatar_url = models.URLField(blank=True, null=True)
    
    # Status change
    old_status = models.IntegerField(blank=True, null=True)
    status = models.IntegerField()
    
    # Timestamp
    updated_at = models.DateTimeField()
    
    class Meta:
        db_table = 'order_status_histories'
        verbose_name = 'Lịch sử trạng thái đơn hàng'
        verbose_name_plural = 'Lịch sử trạng thái đơn hàng'
        ordering = ['-updated_at']

class OrderHistory(models.Model):
    """Lịch sử thay đổi đơn hàng"""
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='histories')
    
    # Editor
    editor = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    
    # Changes (JSON field để lưu tất cả các thay đổi)
    changes = models.JSONField(default=dict)
    
    # Timestamp
    updated_at = models.DateTimeField()
    
    class Meta:
        db_table = 'order_histories'
        verbose_name = 'Lịch sử đơn hàng'
        verbose_name_plural = 'Lịch sử đơn hàng'
        ordering = ['-updated_at']