import os
import pandas as pd
from vnstock import Finance
from sqlalchemy import create_engine

# --- 1. Cấu hình ---

# Lấy đường dẫn kết nối DB từ biến môi trường của Railway
# Railway tự động cung cấp biến này khi bạn liên kết service với DB
DB_CONNECTION_STRING = os.environ.get('DATABASE_URL')

# Đây là đường dẫn "mount path" mà bạn đã cài đặt cho Volume
# Giả sử bạn đặt là /data
VOLUME_PATH = "/data"
FILE_NAME = "acb_ratios.csv"
FULL_FILE_PATH = os.path.join(VOLUME_PATH, FILE_NAME)

# Tên bảng bạn muốn lưu trong Database
TABLE_NAME = "financial_ratios_acb"

# Kiểm tra xem có DB_CONNECTION_STRING không
if not DB_CONNECTION_STRING:
    raise ValueError("Biến môi trường DATABASE_URL chưa được Cài đặt.")

# --- 2. Crawl Dữ liệu ---

print("Bắt đầu crawl dữ liệu cho 'ACB'...")
try:
    finance = Finance(symbol='ACB', source='VCI')
    # Dữ liệu trả về từ vnstock là một DataFrame của Pandas
    data = finance.ratio(period='year', lang='vi', dropna=True)

    if data.empty:
        print("Không tìm thấy dữ liệu.")
    else:
        print(f"Crawl thành công. Tìm thấy {len(data)} dòng dữ liệu.")
        print(data.head())  # In ra 5 dòng đầu

        # --- 3. Lưu file xuống Volume ---

        print(f"Đang lưu dữ liệu ra file tại: {FULL_FILE_PATH}...")
        # Lưu file CSV vào đường dẫn /data/acb_ratios.csv
        # File này sẽ tồn tại vĩnh viễn trên Volume
        data.to_csv(FULL_FILE_PATH, index=False, encoding='utf-8-sig')
        print("Lưu file xuống Volume thành công.")

        # --- 4. Load dữ liệu vào Database ---

        print("Kết nối tới Database...")
        # Tạo kết nối đến DB bằng SQLAlchemy
        engine = create_engine(DB_CONNECTION_STRING)

        print(f"Đang load dữ liệu vào bảng '{TABLE_NAME}'...")
        # Load DataFrame vào thẳng DB
        # if_exists='replace': Xóa bảng cũ (nếu có) và tạo bảng mới
        # if_exists='append': Nối dữ liệu vào bảng đã có
        data.to_sql(
            TABLE_NAME,
            con=engine,
            if_exists='replace',  # Bạn có thể đổi thành 'append' nếu muốn
            index=False
        )
        print("Load dữ liệu vào Database thành công!")

except Exception as e:
    print(f"Đã xảy ra lỗi: {e}")

print("Hoàn tất.")