from django.contrib import admin
from django.utils.html import format_html
from .models import *
from django.db.models import Count

class PageInline(admin.TabularInline):
    model = Page
    extra = 0
    readonly_fields = ['pancake_id', 'platform', 'username']
    fields = ['name', 'platform', 'username', 'pancake_id']

class CategoryInline(admin.TabularInline):
    model = Category
    extra = 0
    readonly_fields = ['pancake_id']
    fields = ['name', 'parent', 'sort_order', 'is_active', 'pancake_id']

class TagInline(admin.TabularInline):
    model = Tag
    extra = 0
    readonly_fields = ['pancake_id', 'color_preview']
    fields = ['text', 'color_preview', 'is_lead_event', 'pancake_id']
    
    def color_preview(self, obj):
        if obj.color:
            return format_html(
                '<span style="background-color: {}; width: 20px; height: 20px; display: inline-block; border: 1px solid #ccc;"></span> {}',
                obj.color,
                obj.color
            )
        return '-'
    color_preview.short_description = 'Màu'

@admin.register(Shop)
class ShopAdmin(admin.ModelAdmin):
    list_display = ['name', 'pancake_id', 'currency', 'pages_count', 'categories_count', 'last_sync']
    list_filter = ['currency', 'last_sync']
    search_fields = ['name', 'pancake_id']
    readonly_fields = ['pancake_id', 'created_at', 'updated_at', 'last_sync', 'pages_count', 'categories_count']
    inlines = [PageInline, CategoryInline]
    
    fieldsets = (
        ('Thông tin cơ bản', {
            'fields': ('pancake_id', 'name', 'currency', 'avatar_url')
        }),
        ('Thống kê', {
            'fields': ('pages_count', 'categories_count')
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at', 'last_sync'),
            'classes': ('collapse',)
        }),
    )
    
    def pages_count(self, obj):
        return obj.pages.count()
    pages_count.short_description = 'Số trang'
    
    def categories_count(self, obj):
        return obj.categories.count()
    categories_count.short_description = 'Số danh mục'

@admin.register(Page)
class PageAdmin(admin.ModelAdmin):
    list_display = ['name', 'platform', 'username', 'shop', 'tags_count', 'is_onboard_xendit']
    list_filter = ['platform', 'is_onboard_xendit', 'shop']
    search_fields = ['name', 'username', 'pancake_id']
    readonly_fields = ['pancake_id', 'created_at', 'updated_at', 'tags_count']
    inlines = [TagInline]
    
    fieldsets = (
        ('Thông tin cơ bản', {
            'fields': ('shop', 'pancake_id', 'name', 'platform', 'username')
        }),
        ('Cài đặt', {
            'fields': ('is_onboard_xendit', 'progressive_catalog_error'),
        }),
        ('Thống kê', {
            'fields': ('tags_count',)
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def tags_count(self, obj):
        return obj.tags.count()
    tags_count.short_description = 'Số tags'

@admin.register(Tag)
class TagAdmin(admin.ModelAdmin):
    list_display = ['text', 'page', 'color_preview', 'is_lead_event', 'pancake_id']
    list_filter = ['is_lead_event', 'page__platform', 'page__shop']
    search_fields = ['text', 'description', 'page__name']
    readonly_fields = ['pancake_id', 'created_at', 'updated_at', 'color_preview']
    
    fieldsets = (
        ('Thông tin cơ bản', {
            'fields': ('page', 'pancake_id', 'text', 'description')
        }),
        ('Hiển thị', {
            'fields': ('color', 'lighten_color', 'color_preview', 'is_lead_event')
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def color_preview(self, obj):
        if obj.color:
            return format_html(
                '<div style="display: flex; align-items: center; gap: 10px;">'
                '<span style="background-color: {}; width: 30px; height: 30px; display: inline-block; border: 1px solid #ccc; border-radius: 4px;"></span>'
                '<span>{}</span>'
                '</div>',
                obj.color,
                obj.color
            )
        return '-'
    color_preview.short_description = 'Preview màu'

@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ['name', 'shop', 'parent', 'sort_order', 'is_active', 'pancake_id']
    list_filter = ['is_active', 'shop', 'parent']
    search_fields = ['name', 'description', 'shop__name']
    readonly_fields = ['pancake_id', 'created_at', 'updated_at']
    list_editable = ['sort_order', 'is_active']
    
    fieldsets = (
        ('Thông tin cơ bản', {
            'fields': ('shop', 'pancake_id', 'name', 'description')
        }),
        ('Phân cấp', {
            'fields': ('parent', 'sort_order', 'is_active')
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('shop', 'parent')
    

from django.contrib import admin
from django.utils.html import format_html, format_html_join
from .models import (
    Shop, Page, Tag, Category,
    Product, ProductVariationField, ProductVariation, SyncHistory
)

# ---------- Inlines ----------
class ProductVariationInline(admin.TabularInline):
    model = ProductVariation
    extra = 0
    fields = (
        'display_id', 'barcode',
        'retail_price', 'retail_price_after_discount',
        'remain_quantity', 'is_hidden', 'is_locked', 'updated_at'
    )
    readonly_fields = ('updated_at',)
    show_change_link = True


# ---------- Product ----------
@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = (
        'name', 'display_id', 'shop', 'is_published',
        'categories_list', 'variations_count', 'last_sync'
    )
    list_filter = ('shop', 'is_published', 'categories', 'last_sync')
    search_fields = ('name', 'display_id', 'pancake_id')
    readonly_fields = (
        'pancake_id', 'inserted_at', 'created_at', 'updated_at',
        'last_sync', 'variations_count', 'categories_list'
    )
    filter_horizontal = ('categories',)
    inlines = [ProductVariationInline]

    fieldsets = (
        ('Thông tin cơ bản', {
            'fields': ('shop', 'pancake_id', 'display_id', 'name', 'is_published')
        }),
        ('Nội dung', {
            'fields': ('image_url', 'note_product', 'tags', 'manipulation_warehouses')
        }),
        ('Phân loại', {
            'fields': ('categories', 'categories_list')
        }),
        ('Metadata', {
            'fields': ('inserted_at', 'created_at', 'updated_at', 'last_sync', 'variations_count'),
            'classes': ('collapse',)
        }),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('shop').prefetch_related('categories', 'variations')

    def categories_list(self, obj):
        names = [c.name for c in obj.categories.all()]
        return ', '.join(names) if names else '-'
    categories_list.short_description = 'Danh mục'

    def variations_count(self, obj):
        return obj.variations.count()
    variations_count.short_description = 'Số biến thể'


# ---------- ProductVariationField ----------
@admin.register(ProductVariationField)
class ProductVariationFieldAdmin(admin.ModelAdmin):
    list_display = ('name', 'key_value', 'value', 'pancake_id')
    search_fields = ('name', 'key_value', 'value', 'pancake_id')


# ---------- ProductVariation ----------
@admin.register(ProductVariation)
class ProductVariationAdmin(admin.ModelAdmin):
    list_display = (
        'display_id', 'product_link', 'remain_quantity',
        'retail_price', 'retail_price_after_discount',
        'is_hidden', 'is_locked', 'updated_at'
    )
    list_filter = ('is_hidden', 'is_locked', 'product__shop')
    search_fields = ('display_id', 'barcode', 'pancake_id', 'product__name')
    readonly_fields = ('inserted_at', 'created_at', 'updated_at', 'last_sync')
    filter_horizontal = ('fields',)

    fieldsets = (
        ('Thông tin cơ bản', {
            'fields': ('product', 'pancake_id', 'display_id', 'barcode')
        }),
        ('Giá', {
            'fields': (
                'retail_price', 'retail_price_after_discount',
                'price_at_counter', 'total_purchase_price', 'last_imported_price',
                'wholesale_price'
            )
        }),
        ('Kho & trạng thái', {
            'fields': (
                'remain_quantity', 'weight',
                'is_composite', 'is_hidden', 'is_locked', 'is_removed', 'is_sell_negative_variation'
            )
        }),
        ('Media & thuộc tính', {
            'fields': ('images', 'videos', 'fields', 'composite_products', 'bonus_variations', 'variations_warehouses')
        }),
        ('Metadata', {
            'fields': ('inserted_at', 'created_at', 'updated_at', 'last_sync'),
            'classes': ('collapse',)
        }),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('product', 'product__shop').prefetch_related('fields')

    def product_link(self, obj):
        if obj.product_id:
            return format_html(
                '<a href="/admin/{}/{}/{}/change/">{}</a>',
                obj._meta.app_label,
                obj.product._meta.model_name,
                obj.product_id,
                obj.product.name
            )
        return '-'
    product_link.short_description = 'Sản phẩm'


# ---------- SyncHistory ----------
@admin.register(SyncHistory)
class SyncHistoryAdmin(admin.ModelAdmin):
    list_display = (
        'sync_type', 'shop', 'status',
        'total_records', 'processed_records',
        'created_records', 'updated_records', 'failed_records',
        'started_at', 'finished_at',
    )
    list_filter = ('sync_type', 'status', 'shop', 'started_at')
    search_fields = ('error_message', 'shop__name')
    readonly_fields = (
        'sync_type', 'shop', 'status',
        'total_records', 'processed_records', 'created_records',
        'updated_records', 'failed_records',
        'error_message', 'error_details',
        'started_at', 'finished_at'
    )
    date_hierarchy = 'started_at'

    fieldsets = (
        ('Thông tin đồng bộ', {
            'fields': ('sync_type', 'shop', 'status')
        }),
        ('Thống kê', {
            'fields': (
                'total_records', 'processed_records',
                'created_records', 'updated_records', 'failed_records'
            )
        }),
        ('Lỗi', {
            'fields': ('error_message', 'error_details')
        }),
        ('Thời gian', {
            'fields': ('started_at', 'finished_at')
        }),
    )
class CustomerAddressInline(admin.TabularInline):
    model = CustomerAddress
    extra = 0
    fields = (
        'full_name', 'phone_number', 'address', 'full_address', 'post_code',
        'country_code', 'province_id', 'district_id', 'commune_id',
        'pancake_id', 'created_at', 'updated_at', 'last_sync'
    )
    readonly_fields = ('pancake_id', 'created_at', 'updated_at', 'last_sync')
    show_change_link = True


# ---- User ----
@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ('name', 'phone_number', 'fb_id', 'pancake_id', 'last_sync', 'created_at')
    search_fields = ('name', 'pancake_id', 'fb_id', 'phone_number')
    readonly_fields = ('created_at', 'updated_at', 'last_sync')
    fieldsets = (
        ('Thông tin', {
            'fields': ('pancake_id', 'name', 'avatar_url', 'fb_id', 'phone_number')
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at', 'last_sync'),
            'classes': ('collapse',)
        }),
    )
    date_hierarchy = 'created_at'


# ---- Customer ----
@admin.register(Customer)
class CustomerAdmin(admin.ModelAdmin):
    list_display = (
        'name', 'shop', 'gender', 'primary_phone_display',
        'order_count', 'succeed_order_count',
        'current_debts', 'purchased_amount',
        'reward_point', 'last_order_at'
    )
    list_filter = ('shop', 'gender', 'is_block', 'active_levera_pay', 'is_discount_by_level', 'level')
    search_fields = ('name', 'username', 'customer_id', 'pancake_id', 'fb_id')
    readonly_fields = (
        'inserted_at', 'updated_at_api', 'created_at', 'updated_at', 'last_sync',
        'primary_phone_display', 'primary_email_display'
    )
    inlines = [CustomerAddressInline]
    date_hierarchy = 'last_order_at'
    fieldsets = (
        ('Liên kết', {
            'fields': ('shop', 'pancake_id', 'customer_id')
        }),
        ('Thông tin cơ bản', {
            'fields': ('name', 'username', 'gender', 'date_of_birth', 'fb_id', 'level', 'currency')
        }),
        ('Liên hệ', {
            'fields': ('phone_numbers', 'emails', 'primary_phone_display', 'primary_email_display')
        }),
        ('Tài chính & Điểm', {
            'fields': ('current_debts', 'purchased_amount', 'total_amount_referred', 'reward_point', 'used_reward_point')
        }),
        ('Đơn hàng', {
            'fields': ('order_count', 'succeed_order_count', 'returned_order_count', 'last_order_at')
        }),
        ('Trạng thái & Khác', {
            'fields': (
                'is_block', 'is_discount_by_level', 'is_adjust_debts', 'active_levera_pay',
                'creator', 'assigned_user', 'referral_code', 'count_referrals', 'user_block_id',
                'conversation_tags', 'order_sources', 'tags', 'list_voucher', 'notes'
            )
        }),
        ('Metadata', {
            'fields': ('inserted_at', 'updated_at_api', 'created_at', 'updated_at', 'last_sync'),
            'classes': ('collapse',)
        }),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('shop', 'creator', 'assigned_user')

    def primary_phone_display(self, obj):
        return obj.primary_phone
    primary_phone_display.short_description = 'SĐT chính'

    def primary_email_display(self, obj):
        return obj.primary_email
    primary_email_display.short_description = 'Email chính'


# ---- CustomerAddress (tùy thích: vừa có inline, vừa có trang riêng) ----
@admin.register(CustomerAddress)
class CustomerAddressAdmin(admin.ModelAdmin):
    list_display = (
        'full_name', 'phone_number', 'customer',
        'province_id', 'district_id', 'commune_id',
        'post_code', 'pancake_id', 'updated_at'
    )
    list_filter = ('country_code', 'province_id', 'district_id', 'commune_id')
    search_fields = ('full_name', 'phone_number', 'address', 'full_address', 'pancake_id', 'customer__name')
    readonly_fields = ('created_at', 'updated_at', 'last_sync')
    fieldsets = (
        ('Khách hàng', {'fields': ('customer', 'pancake_id')}),
        ('Người nhận', {'fields': ('full_name', 'phone_number')}),
        ('Địa chỉ', {'fields': ('address', 'full_address', 'post_code')}),
        ('Khu vực', {'fields': ('country_code', 'province_id', 'district_id', 'commune_id')}),
        ('Metadata', {'fields': ('created_at', 'updated_at', 'last_sync'), 'classes': ('collapse',)}),
    )

# Thêm vào cuối file admin.py

# ---------- Order Related Inlines ----------
class OrderItemInline(admin.TabularInline):
    model = OrderItem
    extra = 0
    fields = (
        'item_id', 'product', 'variation', 'quantity', 'retail_price',
        'discount_each_product', 'total_discount', 'is_bonus_product',
        'return_quantity', 'note'
    )
    readonly_fields = ('item_id',)
    show_change_link = True


class OrderStatusHistoryInline(admin.TabularInline):
    model = OrderStatusHistory
    extra = 0
    fields = ('editor', 'old_status', 'status', 'updated_at')
    readonly_fields = ('updated_at',)
    ordering = ('-updated_at',)


class OrderHistoryInline(admin.TabularInline):
    model = OrderHistory
    extra = 0
    fields = ('editor', 'changes', 'updated_at')
    readonly_fields = ('updated_at',)
    ordering = ('-updated_at',)


# ---------- Order ----------
@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = (
        'system_id', 'bill_full_name', 'status_display', 'shop',
        'total_price', 'total_quantity', 'order_sources_name',
        'creator', 'items_count_display', 'inserted_at'
    )
    list_filter = (
        'status', 'order_sources', 'shop', 'is_livestream',
        'is_free_shipping', 'customer_pay_fee', 'inserted_at'
    )
    search_fields = (
        'system_id', 'pancake_id', 'bill_full_name', 'bill_phone_number',
        'customer__name', 'note'
    )
    readonly_fields = (
        'pancake_id', 'system_id', 'inserted_at', 'updated_at_api',
        'created_at', 'updated_at', 'last_sync', 'items_count_display'
    )
    
    # Thêm raw_id_fields để tăng tốc
    raw_id_fields = (
        'customer', 'creator', 'assigning_seller', 'assigning_care', 
        'marketer', 'last_editor', 'page'
    )
    
    # Giảm số lượng inline
    inlines = [OrderItemInline]  # Bỏ history inlines tạm thời
    date_hierarchy = 'inserted_at'
    list_per_page = 20  # Giảm pagination

    fieldsets = (
        ('Thông tin cơ bản', {
            'fields': (
                'shop', 'customer', 'page', 'pancake_id', 'system_id',
                'status', 'sub_status', 'order_sources', 'order_sources_name'
            )
        }),
        ('Thông tin thanh toán', {
            'fields': (
                'total_price', 'total_discount', 'total_price_after_sub_discount',
                'shipping_fee', 'cod', 'prepaid'
            )
        }),
        ('Thông tin người nhận', {
            'fields': (
                'bill_full_name', 'bill_phone_number', 'bill_email'
            )
        }),
        ('Người liên quan', {
            'fields': (
                'creator', 'assigning_seller', 'assigning_care',
                'marketer', 'last_editor'
            ),
            'classes': ('collapse',)
        }),
        ('Các hình thức thanh toán', {
            'fields': (
                'charged_by_card', 'charged_by_momo', 'charged_by_qrpay',
                'cash', 'exchange_payment', 'exchange_value', 'surcharge',
                'levera_point', 'bank_payments', 'prepaid_by_point'
            ),
            'classes': ('collapse',)
        }),
        ('Cài đặt & Flags', {
            'fields': (
                'is_free_shipping', 'is_livestream', 'is_live_shopping',
                'is_exchange_order', 'is_smc', 'customer_pay_fee',
                'received_at_shop', 'return_fee'
            ),
            'classes': ('collapse',)
        }),
        ('Social Media & Marketing', {
            'fields': (
                'account', 'account_name', 'page_external_id',
                'conversation_id', 'post_id', 'ad_id', 'ads_source',
                'p_utm_source', 'p_utm_medium', 'p_utm_campaign',
                'p_utm_content', 'p_utm_term', 'p_utm_id'
            ),
            'classes': ('collapse',)
        }),
        ('Ghi chú & Links', {
            'fields': (
                'note', 'note_print', 'note_image', 'link',
                'link_confirm_order', 'order_link'
            ),
            'classes': ('collapse',)
        }),
        ('Khác', {
            'fields': (
                'warehouse_id', 'marketplace_id', 'fee_marketplace',
                'customer_referral_code', 'pke_mkter', 'tags',
                'customer_needs', 'total_quantity', 'items_count_display',
                'returned_reason', 'returned_reason_name'
            ),
            'classes': ('collapse',)
        }),
        ('Thời gian', {
            'fields': (
                'time_assign_seller', 'time_assign_care', 'time_send_partner',
                'estimate_delivery_date', 'inserted_at', 'updated_at_api'
            ),
            'classes': ('collapse',)
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at', 'last_sync'),
            'classes': ('collapse',)
        }),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related(
            'shop', 'customer', 'page', 'creator', 'assigning_seller',
            'assigning_care', 'marketer', 'last_editor'
        ).prefetch_related(
            'items__product',
            'items__variation'
        ).annotate(
            items_count_annotated=Count('items')
        )

    def status_display(self, obj):
        status_dict = dict(Order.STATUS_CHOICES)
        status_name = status_dict.get(obj.status, f'Unknown ({obj.status})')
        
        color_map = {
            0: '#6c757d', 1: '#007bff', 2: '#28a745', 3: '#28a745',
            4: '#dc3545', 5: '#fd7e14', 6: '#6f42c1', 7: '#28a745',
            11: '#ffc107', 15: '#17a2b8', 16: '#dc3545', 18: '#fd7e14',
            19: '#dc3545',
        }
        
        color = color_map.get(obj.status, '#6c757d')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color, status_name
        )
    status_display.short_description = 'Trạng thái'

    def items_count_display(self, obj):
        # Sử dụng annotation thay vì query
        return getattr(obj, 'items_count_annotated', 0)
    items_count_display.short_description = 'Số sản phẩm'


# Tối ưu OrderItemInline
class OrderItemInline(admin.TabularInline):
    model = OrderItem
    extra = 0
    max_num = 15  # Giới hạn số item
    fields = (
        'item_id', 'product', 'variation', 'quantity', 'retail_price',
        'total_discount', 'is_bonus_product'
    )
    readonly_fields = ('item_id',)
    raw_id_fields = ('product', 'variation')
    
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('product', 'variation')

# ---------- OrderShippingAddress ----------
@admin.register(OrderShippingAddress)
class OrderShippingAddressAdmin(admin.ModelAdmin):
    list_display = (
        'order_link', 'full_name', 'phone_number',
        'province_name', 'district_name', 'commune_name'
    )
    search_fields = (
        'full_name', 'phone_number', 'address', 'full_address',
        'order__system_id', 'order__bill_full_name'
    )
    readonly_fields = ('order_link',)

    fieldsets = (
        ('Đơn hàng', {
            'fields': ('order', 'order_link')
        }),
        ('Người nhận', {
            'fields': ('full_name', 'phone_number')
        }),
        ('Địa chỉ', {
            'fields': ('address', 'full_address', 'post_code', 'marketplace_address')
        }),
        ('Mã vùng (cũ)', {
            'fields': ('country_code', 'province_id', 'province_name', 
                      'district_id', 'district_name', 'commune_id', 'commune_name')
        }),
        ('Mã vùng (mới)', {
            'fields': ('new_province_id', 'new_commune_id', 'new_full_address', 'render_type')
        }),
        ('Khác', {
            'fields': ('commune_code_sicepat',)
        }),
    )

    def order_link(self, obj):
        if obj.order:
            return format_html(
                '<a href="/admin/shop/order/{}/change/">Order #{}</a>',
                obj.order.pk, obj.order.system_id
            )
        return '-'
    order_link.short_description = 'Đơn hàng'


# ---------- OrderWarehouse ----------
@admin.register(OrderWarehouse)
class OrderWarehouseAdmin(admin.ModelAdmin):
    list_display = (
        'order_link', 'name', 'phone_number',
        'province_id', 'district_id', 'has_snappy_service'
    )
    search_fields = (
        'name', 'address', 'phone_number',
        'order__system_id', 'custom_id', 'affiliate_id'
    )
    readonly_fields = ('order_link',)

    fieldsets = (
        ('Đơn hàng', {
            'fields': ('order', 'order_link')
        }),
        ('Thông tin kho', {
            'fields': ('name', 'address', 'full_address', 'phone_number')
        }),
        ('Vị trí', {
            'fields': ('province_id', 'district_id', 'commune_id', 'postcode')
        }),
        ('Cài đặt & IDs', {
            'fields': ('settings', 'has_snappy_service', 'custom_id', 'affiliate_id', 'ffm_id')
        }),
    )

    def order_link(self, obj):
        if obj.order:
            return format_html(
                '<a href="/admin/shop/order/{}/change/">Order #{}</a>',
                obj.order.pk, obj.order.system_id
            )
        return '-'
    order_link.short_description = 'Đơn hàng'


# ---------- OrderPartner ----------
@admin.register(OrderPartner)
class OrderPartnerAdmin(admin.ModelAdmin):
    list_display = (
        'order_link', 'partner_name', 'partner_status',
        'extend_code', 'cod', 'total_fee', 'is_returned'
    )
    list_filter = ('partner_name', 'partner_status', 'is_returned', 'system_created')
    search_fields = (
        'partner_name', 'extend_code', 'order_number_vtp',
        'sort_code', 'order__system_id'
    )
    readonly_fields = ('order_link',)

    fieldsets = (
        ('Đơn hàng', {
            'fields': ('order', 'order_link')
        }),
        ('Thông tin đối tác', {
            'fields': ('partner_id', 'partner_name', 'partner_status')
        }),
        ('Mã & ID', {
            'fields': ('extend_code', 'order_number_vtp', 'sort_code', 
                      'custom_partner_id', 'order_id_ghn')
        }),
        ('Tài chính', {
            'fields': ('cod', 'total_fee')
        }),
        ('Giao hàng', {
            'fields': ('delivery_name', 'delivery_tel', 'count_of_delivery')
        }),
        ('Trạng thái & Cài đặt', {
            'fields': ('system_created', 'is_returned', 'is_ghn_v2')
        }),
        ('Links & Dịch vụ', {
            'fields': ('printed_form', 'service_partner')
        }),
        ('Thời gian', {
            'fields': ('first_delivery_at', 'picked_up_at', 'paid_at', 'updated_at_partner')
        }),
        ('Cập nhật mở rộng', {
            'fields': ('extend_update',),
            'classes': ('collapse',)
        }),
    )

    def order_link(self, obj):
        if obj.order:
            return format_html(
                '<a href="/admin/shop/order/{}/change/">Order #{}</a>',
                obj.order.pk, obj.order.system_id
            )
        return '-'
    order_link.short_description = 'Đơn hàng'


# ---------- OrderItem ----------
@admin.register(OrderItem)
class OrderItemAdmin(admin.ModelAdmin):
    list_display = (
        'order_link', 'product_name', 'variation_display',
        'quantity', 'retail_price', 'total_discount',
        'is_bonus_product', 'return_quantity'
    )
    list_filter = ('is_bonus_product', 'is_composite', 'is_wholesale', 'one_time_product')
    search_fields = (
        'order__system_id', 'product__name', 'variation__display_id',
        'note', 'note_product'
    )
    readonly_fields = ('item_id', 'order_link', 'product_name', 'variation_display')

    fieldsets = (
        ('Đơn hàng & Sản phẩm', {
            'fields': ('order', 'order_link', 'item_id', 'product', 'variation')
        }),
        ('Số lượng', {
            'fields': ('quantity', 'added_to_cart_quantity', 'return_quantity', 
                      'returned_count', 'returning_quantity', 'exchange_count')
        }),
        ('Giá & Giảm giá', {
            'fields': ('retail_price', 'discount_each_product', 'same_price_discount', 'total_discount')
        }),
        ('Cài đặt', {
            'fields': ('is_bonus_product', 'is_composite', 'is_discount_percent', 
                      'is_wholesale', 'one_time_product')
        }),
        ('Ghi chú', {
            'fields': ('note', 'note_product')
        }),
        ('Liên kết & Nhóm', {
            'fields': ('composite_item_id', 'measure_group_id', 'components')
        }),
        ('Thông tin biến thể', {
            'fields': ('variation_info',),
            'classes': ('collapse',)
        }),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('order', 'product', 'variation')

    def order_link(self, obj):
        if obj.order:
            return format_html(
                '<a href="/admin/shop/order/{}/change/">Order #{}</a>',
                obj.order.pk, obj.order.system_id
            )
        return '-'
    order_link.short_description = 'Đơn hàng'

    def product_name(self, obj):
        if obj.product:
            return obj.product.name
        elif obj.variation_info:
            return obj.variation_info.get('name', 'Unknown Product')
        return 'Unknown Product'
    product_name.short_description = 'Tên sản phẩm'

    def variation_display(self, obj):
        if obj.variation:
            return obj.variation.display_id
        return '-'
    variation_display.short_description = 'Mã biến thể'


# ---------- OrderStatusHistory ----------
@admin.register(OrderStatusHistory)
class OrderStatusHistoryAdmin(admin.ModelAdmin):
    list_display = (
        'order_link', 'name', 'old_status_display', 'status_display', 'updated_at'
    )
    list_filter = ('status', 'old_status', 'updated_at')
    search_fields = ('order__system_id', 'name', 'editor__name')
    readonly_fields = ('order_link', 'old_status_display', 'status_display')
    date_hierarchy = 'updated_at'

    fieldsets = (
        ('Đơn hàng', {
            'fields': ('order', 'order_link')
        }),
        ('Người thay đổi', {
            'fields': ('editor', 'editor_fb', 'name', 'avatar_url')
        }),
        ('Thay đổi trạng thái', {
            'fields': ('old_status', 'old_status_display', 'status', 'status_display')
        }),
        ('Thời gian', {
            'fields': ('updated_at',)
        }),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('order', 'editor')

    def order_link(self, obj):
        if obj.order:
            return format_html(
                '<a href="/admin/shop/order/{}/change/">Order #{}</a>',
                obj.order.pk, obj.order.system_id
            )
        return '-'
    order_link.short_description = 'Đơn hàng'

    def old_status_display(self, obj):
        if obj.old_status is not None:
            status_dict = dict(Order.STATUS_CHOICES)
            return status_dict.get(obj.old_status, f'Unknown ({obj.old_status})')
        return '-'
    old_status_display.short_description = 'Trạng thái cũ'

    def status_display(self, obj):
        status_dict = dict(Order.STATUS_CHOICES)
        return status_dict.get(obj.status, f'Unknown ({obj.status})')
    status_display.short_description = 'Trạng thái mới'


# ---------- OrderHistory ----------
@admin.register(OrderHistory)
class OrderHistoryAdmin(admin.ModelAdmin):
    list_display = ('order_link', 'editor', 'changes_summary', 'updated_at')
    search_fields = ('order__system_id', 'editor__name')
    readonly_fields = ('order_link', 'changes_summary')
    date_hierarchy = 'updated_at'

    fieldsets = (
        ('Đơn hàng', {
            'fields': ('order', 'order_link')
        }),
        ('Người thay đổi', {
            'fields': ('editor',)
        }),
        ('Chi tiết thay đổi', {
            'fields': ('changes', 'changes_summary')
        }),
        ('Thời gian', {
            'fields': ('updated_at',)
        }),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('order', 'editor')

    def order_link(self, obj):
        if obj.order:
            return format_html(
                '<a href="/admin/shop/order/{}/change/">Order #{}</a>',
                obj.order.pk, obj.order.system_id
            )
        return '-'
    order_link.short_description = 'Đơn hàng'

    def changes_summary(self, obj):
        if obj.changes:
            fields_changed = list(obj.changes.keys())
            if len(fields_changed) > 3:
                return f"{', '.join(fields_changed[:3])}... ({len(fields_changed)} fields)"
            return ', '.join(fields_changed)
        return '-'
    changes_summary.short_description = 'Tóm tắt thay đổi'