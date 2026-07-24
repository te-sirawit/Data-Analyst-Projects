import re
import os
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# ==========================================
# 1. ตั้งค่าการเชื่อมต่อ Database
# ==========================================
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'your_password') 
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopee_analytics_db')

engine_real = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
engine_portfolio = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}_portfolio')

# ==========================================
# 2. ฟังก์ชันโหลดข้อมูล (ETL)
# ==========================================



COLUMN_MAPPING = {
    # ------------------- Order_All -------------------
    'หมายเลขคำสั่งซื้อ': 'order_id',
    'สถานะการสั่งซื้อ': 'order_status',
    'เหตุผลในการยกเลิกคำสั่งซื้อ': 'cancel_reason',
    'สถานะการคืนเงินหรือคืนสินค้า': 'return_refund_status',
    'ชื่อผู้ใช้ (ผู้ซื้อ)': 'buyer_username',
    'วันที่ทำการสั่งซื้อ': 'order_date',
    'เวลาการชำระสินค้า': 'payment_time',
    'ช่องทางการชำระเงิน': 'payment_channel',
    'ตัวเลือกการจัดส่ง': 'shipping_option',
    'วิธีการจัดส่ง': 'shipping_method',
    '*หมายเลขติดตามพัสดุ': 'tracking_number',
    'วันที่คาดว่าจะทำการจัดส่งสินค้า': 'expected_shipping_date',
    'เวลาส่งสินค้า': 'shipping_time',
    'เลขอ้างอิง Parent SKU': 'parent_sku_reference',
    'ชื่อสินค้า': 'product_name',
    'เลขอ้างอิง SKU (SKU Reference No.)': 'sku_reference_no',
    'ชื่อตัวเลือก': 'variant_name',
    'ราคาตั้งต้น': 'original_price',
    'ราคาขาย': 'selling_price',
    'จำนวน': 'quantity',
    'ราคาขายสุทธิ': 'net_selling_price',
    'ส่วนลดจาก Shopee': 'shopee_discount',
    'โค้ดส่วนลดชำระโดยผู้ขาย': 'seller_voucher_discount',
    'โค้ด Coins Cashback': 'coins_cashback',
    'โค้ดส่วนลดชำระโดย Shopee': 'shopee_voucher_discount',
    'โค้ดส่วนลดชำระโดย Shopee (เช่น โค้ดจากโปรแกรม ร้านโค้ดคุ้ม, โค้ดส่วนลด Shopee, โค้ดส่วนลด Shopee Mall)': 'shopee_voucher_discount',
    'โค้ดส่วนลด': 'voucher_code',
    'เข้าร่วมแคมเปญ bundle deal หรือไม่': 'joined_bundle_deal',
    'ส่วนลด bundle deal ชำระโดยผู้ขาย': 'seller_bundle_discount',
    'ส่วนลด bundle deal ชำระโดย Shopee': 'shopee_bundle_discount',
    'ส่วนลดจากการใช้เหรียญ': 'coins_discount',
    'ส่วนลดทั้งหมดจากบัตรเครดิต': 'credit_card_discount',
    'ค่าคอมมิชชั่น': 'commission_fee',
    'Transaction Fee': 'transaction_fee',
    'ต้นทุนขายหักคูปองและcoin': 'cogs_after_discount',
    'ค่าจัดส่งที่ชำระโดยผู้ซื้อ': 'buyer_paid_shipping',
    'ค่าจัดส่งที่ Shopee ออกให้โดยประมาณ': 'shopee_shipping_subsidy',
    'ค่าจัดส่งสินค้าคืน': 'return_shipping_fee',
    'ค่าบริการ': 'service_fee',
    'จำนวนเงินทั้งหมด': 'total_amount',
    'ค่าจัดส่งโดยประมาณ': 'estimated_shipping_fee',
    'ชื่อผู้รับ': 'recipient_name',
    'หมายเลขโทรศัพท์': 'phone_number',
    'หมายเหตุจากผู้ซื้อ': 'buyer_note',
    'ที่อยู่ในการจัดส่ง': 'delivery_address',
    'ประเทศ': 'country',
    'จังหวัด': 'province',
    'เขต/อำเภอ': 'district',
    'รหัสไปรษณีย์': 'postal_code',
    'ประเภทคำสั่งซื้อ': 'order_type',
    'ประเภทคำสั่งซื้อ.1': 'order_type_1',
    'เวลาที่ทำการสั่งซื้อสำเร็จ': 'order_success_time',
    'บันทึก': 'note',
    'ช่องทางการชำระเงิน (รายละเอียด)_1': 'payment_channel_detail_1',
    'ช่องทางการชำระเงิน (รายละเอียด)_2': 'payment_channel_detail_2',
    'ช่องทางการชำระเงิน (รายละเอียด)': 'payment_channel_detail',
    'ค่าธรรมเนียม (%)': 'fee_percentage',
    'Hot Listing': 'hot_listing',
    'แผนการผ่อนชำระ': 'installment_plan',
    'จำนวนที่ส่งคืน': 'returned_quantity',
    'โค้ด Coins Cashback ชำระโดยผู้ขาย': 'seller_coins_cashback',
    'โปรโมชั่นช่องทางชำระเงินทั้งหมด': 'all_payment_promotions',
    'ส่วนลดเครื่องเก่าแลกใหม่': 'trade_in_discount',
    'โบนัสส่วนลดเครื่องเก่าแลกใหม่': 'trade_in_bonus',
    'โบนัสส่วนลดเครื่องเก่าแลกใหม่จากผู้ขาย': 'seller_trade_in_bonus',
    'ราคาสินค้าที่ชำระโดยผู้ซื้อ (THB)': 'buyer_paid_amount',

    # ------------------- Business_Insights -------------------
    'วันที่': 'date',
    'ยอดขายทั้งหมด (THB)': 'total_sales_thb',
    'คำสั่งซื้อทั้งหมด': 'total_orders',
    'ยอดขายเฉลี่ยต่อคำสั่งซื้อ': 'avg_sales_per_order',
    'ยอดเข้าชม': 'page_views',
    'จำนวนผู้เยี่ยมชม': 'unique_visitors',
    'อัตราการซื้อสินค้า (จากคำสั่งซื้อที่ชำระเงิน)': 'cvr_paid_orders',
    'คำสั่งซื้อที่ยกเลิก': 'cancelled_orders',
    'ยอดขายที่ยกเลิก': 'cancelled_sales',
    'คำสั่งซื้อที่คืนเงิน/คืนสินค้า': 'refunded_orders',
    'ยอดขายที่คืนเงิน/คืนสินค้า': 'refunded_sales',
    '# ของผู้ซื้อ': 'buyer_count',
    '# ของผู้ซื้อใหม่': 'new_buyer_count',
    '# ของผู้ซื้อเดิม': 'existing_buyer_count',
    '# ผู้ที่อาจจะซื้อ': 'potential_buyers',
    'อัตราการกลับมาซื้อซ้ำ': 'repeat_purchase_rate',
    'จำนวนคลิก': 'clicks',
    'อัตราการซื้อสินค้า': 'cvr',
    'ยอดขายที่ไม่รวมส่วนลดจาก Shopee': 'sales_excluding_shopee_discount',

    # ------------------- Ads_Performance & Ads_Keywords -------------------
    'ลำดับ': 'rank',
    'ชื่อโฆษณา': 'ad_name',
    'สถานะ': 'status',
    'ประเภทโฆษณา': 'ad_type',
    'รหัสสินค้า': 'item_id',
    'ปรับแต่ง': 'adjust',
    'การตั้งราคาประมูล': 'bid_price',
    'ตำแหน่ง': 'placement',
    'Keywordsตำแหน่ง': 'keyword_placement',
    'Match Type': 'match_type',
    'วันที่เริ่มต้น': 'start_date',
    'วันที่สิ้นสุด': 'end_date',
    'การมองเห็น': 'impressions',
    'การมองเห็นสินค้า': 'product_impressions',
    'จำนวนคลิก': 'clicks',
    'จำนวนคลิกสินค้า': 'product_clicks',
    'อัตราการคลิก (CTR)': 'ctr',
    'อัตราการคลิกสินค้า (CTR)': 'product_ctr',
    'การสั่งซื้อ': 'orders',
    'การสั่งซื้อโดยตรง': 'direct_orders',
    'อัตราการสั่งซื้อ': 'cvr',
    'อัตราการสั่งซื้อโดยตรง': 'direct_cvr',
    'ราคาต่อการสั่งซื้อ': 'cpa',
    'ราคาต่อการสั่งซื้อโดยตรง': 'cpa_direct',
    'สินค้าที่ขายแล้ว': 'items_sold',
    'สินค้าที่ขายแล้วโดยตรง': 'items_sold_direct',
    'ยอดขาย': 'sales',
    'ยอดขายโดยตรง': 'direct_sales',
    'ค่าโฆษณา': 'expense',
    'ยอดขาย/รายจ่าย (ROAS)': 'roas',
    'ผลตอบแทนจากการลงทุนโดยตรง (Direct ROAS)': 'direct_roas',
    'อัตราส่วนค่าใช้จ่ายต่อรายได้ (ACOS)': 'acos',
    'อัตราส่วนค่าใช้จ่ายต่อรายได้โดยตรง (Direct ACOS)': 'direct_acos',
    'จำนวนการเข้าชมที่สร้างขึ้นจากการบรอดแคสต์': 'broadcast_views',
    'ยอดขายที่สร้างขึ้นจากการบรอดแคสต์': 'broadcast_sales',
    'Add to Cart': 'add_to_cart',
    'Add to Cart Rate': 'add_to_cart_rate',
    'Voucher Amount': 'voucher_amount',
    'Vouchered Sales': 'vouchered_sales',
    
    # --- Legacy Columns (2021-2022) ---
    'ตำแหน่งที่แสดง/Keyword': 'keyword_placement',
    'Keywords/ตำแหน่ง': 'keyword_placement',
    'ยอดขาย/รายจ่าย (ROI)': 'roas',
    'ผลตอบแทนจากการลงทุนโดยตรง (Direct ROI)': 'direct_roas',
    'CIR': 'acos',
    'ACOS': 'acos',
    'อัตราส่วนค่าใช้จ่ายต่อรายได้โดยตรง (Direct CIR)': 'direct_acos'
}

def clean_column_name(col):
    col = str(col).strip()
    if col in COLUMN_MAPPING:
        return COLUMN_MAPPING[col]
    # Remove special chars but keep spaces/underscores
    cleaned = re.sub(r'[^a-zA-Z0-9ก-๙\s_]', '', col).strip()
    cleaned = cleaned.replace(' ', '_')
    # If it's still too long in bytes (PostgreSQL limit is 63 bytes), truncate it
    while len(cleaned.encode('utf-8')) > 60:
        cleaned = cleaned[:-1]
    return cleaned


def process_shopee_data(base_dir):
    years = ['2021', '2022', '2023', '2024', '2025', '2026']
    categories = ['Order_All', 'Ads_Data', 'Business_Insights']
    
    # Store dataframes for each category
    all_data = {
        'Order_All': [], 
        'Business_Insights': [],
        'Ads_Performance': [],  # ข้อมูล Shopee Ads
        'Ads_Keywords': []      # รายงาน Keywords
    }
    
    for year in years:
        year_path = os.path.join(base_dir, year)
        if not os.path.exists(year_path):
            continue
            
        print(f"กำลังประมวลผลข้อมูลปี {year}...")
        
        for root, dirs, files in os.walk(year_path):
            for file in files:
                if (file.endswith('.xlsx') or file.endswith('.csv')) and not file.startswith('~'):
                    file_path = os.path.join(root, file)
                    
                    category = None
                    if 'Order.all' in file:
                        category = 'Order_All'
                    elif 'shopee-shop-stats' in file:
                        category = 'Business_Insights'
                    elif 'ข้อมูล-Shopee-Ads' in file or 'Shopee-ads-Overall' in file:
                        category = 'Ads_Performance'
                    elif 'รายงาน-Keyword' in file or 'Shopee-ads-Keyword' in file:
                        category = 'Ads_Keywords'
                        
                    if not category:
                        continue
                        
                    print(f"  -> อ่านไฟล์: {file} [{category}]")
                    
                    try:
                        # --- EXTRACT ---
                        if file.endswith('.csv'):
                            if category in ['Ads_Performance', 'Ads_Keywords']:
                                try:
                                    df = pd.read_csv(file_path, encoding='utf-8', skiprows=6)
                                except Exception:
                                    df = pd.read_csv(file_path, encoding='cp874', skiprows=6)
                            else:
                                try:
                                    df = pd.read_csv(file_path, encoding='utf-8')
                                except Exception:
                                    try:
                                        df = pd.read_csv(file_path, encoding='utf-8', skiprows=6)
                                    except Exception:
                                        df = pd.read_csv(file_path, encoding='cp874', skiprows=6)
                        else:
                            df = pd.read_excel(file_path)
                        
                        # ลบ Header ที่ซ้ำ (ถ้ามี)
                        header_val = str(df.columns[0]).strip()
                        matches = df[df.iloc[:, 0].astype(str).str.strip() == header_val]
                        if not matches.empty:
                            idx = matches.index[0]
                            df = df.iloc[idx + 1:].reset_index(drop=True)
                        # Clean columns for EACH dataframe BEFORE appending
                        new_cols = []
                        seen = set()
                        for c in df.columns:
                            cleaned = clean_column_name(c)
                            base_name = cleaned
                            counter = 1
                            while cleaned in seen:
                                cleaned = f"{base_name}_{counter}"
                                counter += 1
                            seen.add(cleaned)
                            new_cols.append(cleaned)
                        df.columns = new_cols
                        
                        df = df.dropna(how='all')
                        all_data[category].append(df)
                    except Exception as e:
                        print(f"เกิดข้อผิดพลาดในการอ่านไฟล์ {file}: {e}")
                        
    # --- TRANSFORM & LOAD ---
    target_tables = ['Order_All', 'Business_Insights', 'Ads_Performance', 'Ads_Keywords']
    for category in target_tables:
        if len(all_data[category]) == 0:
            continue
            
        print(f"กำลังบันทึกข้อมูลหมวดหมู่ {category} ลง Database...")
        
        # Concat all dataframes for this category. Since columns are already aligned, they will merge perfectly.
        combined_df = pd.concat(all_data[category], ignore_index=True)      
        exclude_numeric = [
            'order_id', 'order_status', 'cancel_reason', 'return_refund_status', 'buyer_username', 
            'order_date', 'payment_time', 'payment_channel', 'shipping_option', 'shipping_method', 
            'tracking_number', 'expected_shipping_date', 'shipping_time', 'parent_sku_reference', 
            'product_name', 'sku_reference_no', 'variant_name', 'recipient_name', 'phone_number', 
            'buyer_note', 'delivery_address', 'country', 'province', 'district', 'postal_code', 
            'order_type', 'order_success_time', 'note', 'payment_channel_detail_1', 
            'payment_channel_detail_2', 'payment_channel_detail', 'installment_plan', 
            'all_payment_promotions', 'order_type_1', 'date', 'ad_name', 'status', 'ad_type', 
            'item_id', 'adjust', 'placement', 'keyword_placement', 'match_type', 'start_date', 'end_date'
        ]
        for col in combined_df.columns:
            if col not in exclude_numeric:
                try:
                    cleaned = combined_df[col].astype(str).str.replace(',', '').str.replace('%', '').str.replace('฿', '').str.strip()
                    cleaned = cleaned.replace(['nan', 'None', '-', ''], np.nan)
                    combined_df[col] = pd.to_numeric(cleaned, errors='coerce')
                except:
                    pass
                        
        for col in combined_df.columns:
            if pd.api.types.is_numeric_dtype(combined_df[col]):
                combined_df[col] = combined_df[col].astype(float)
        
        table_name = category.lower()
        
        # Load Real DB
        combined_df.to_sql(f'{table_name}_real', engine_real, if_exists='replace', index=False)
        
        # Create Portfolio DB
        df_mock = combined_df.copy()
        multiplier = np.random.uniform(0.4, 0.8)
        for col in df_mock.columns:
            if any(keyword in col for keyword in ['price', 'total', 'ยอด', 'ราคา', 'sales', 'revenue']):
                df_mock[col] = df_mock[col] * multiplier
            if any(keyword in col for keyword in ['name', 'username', 'ชื่อ']):
                df_mock[col] = 'Customer_' + pd.Series(np.random.randint(1000, 9999, size=len(df_mock))).astype(str)
                
        df_mock.to_sql(f'{table_name}_portfolio', engine_portfolio, if_exists='replace', index=False)
        
        # Export to CSV with Star Schema naming convention
        csv_filename = f'{table_name}.csv'
        if category == 'Order_All':
            csv_filename = 'fact_order.csv'
        elif category == 'Ads_Performance':
            csv_filename = 'fact_ad.csv'
        elif category == 'Business_Insights':
            csv_filename = 'dim_product.csv'
            
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, csv_filename)
        df_mock.to_csv(csv_path, index=False)
        
        print(f"✅ บันทึกข้อมูล {category} ลง Database และสร้างไฟล์ {csv_filename} สำเร็จ!")

if __name__ == "__main__":
    # REPLACE WITH YOUR OWN DIRECTORY PATH CONTAINING THE SHOPEE EXPORT FILES
    base_directory = os.getenv('SHOPEE_DATA_DIR', './raw_data/shopee_exports')
    print("เริ่มการรัน ETL Pipeline...")
    process_shopee_data(base_directory)
    print("✅ โหลดข้อมูลเสร็จสมบูรณ์!")
