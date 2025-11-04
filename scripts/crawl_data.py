import os
import pandas as pd
import argparse
import datetime
from pathlib import Path  # Thư viện mới để xử lý đường dẫn file
from vnstock import Finance, Vnstock, Listing, Quote
from sqlalchemy import create_engine
from dotenv import load_dotenv


def connect_to_db():
    """Tải cấu hình và kết nối tới database."""
    load_dotenv()
    db_user = os.getenv("MYSQLUSER")
    db_pass = os.getenv("MYSQLPASSWORD")
    db_host = os.getenv("MYSQLHOST")
    db_port = os.getenv("MYSQLPORT")
    db_name = os.getenv("MYSQLDATABASE")

    if not all([db_user, db_pass, db_host, db_port, db_name]):
        print("Lỗi: Không tìm thấy biến môi trường database.")
        return None
    try:
        db_url = f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(db_url)
        print(f"Kết nối tới {db_host} thành công!")
        return engine
    except Exception as e:
        print(f"Lỗi kết nối database: {e}")
        return None


def fetch_data(target_date):
    """Lấy dữ liệu từ vnstock cho MỘT NGÀY (target_date)."""
    print(f"Bắt đầu lấy dữ liệu vnstock cho ngày: {target_date}...")
    try:
        company = Vnstock().stock(symbol='ACB', source='TCBS').company
        finance = Finance(symbol='ACB', source='VCI')
        listing = Listing()
        quote = Quote(symbol='ACB', source='VCI')

        # Dùng target_date cho cả start và end để lấy dữ liệu 1 ngày
        data_history = quote.history(start=target_date, end=target_date, interval='1D')

        data_to_load = {
            "company_overview": company.overview(),
            "finance_ratio": finance.ratio(period='year', lang='vi', dropna=True),
            "symbols_by_exchange": listing.symbols_by_exchange(),
            "symbols_by_industries": listing.symbols_by_industries(),
            "quote_history": data_history
        }
        print("Lấy dữ liệu thành công.")
        return data_to_load
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu vnstock: {e}")
        return None


def save_data_to_csv(data_dict):
    """
    Lưu các DataFrame thành file CSV với định dạng tên yêu cầu.
    """
    # Tạo thư mục output_data/ nếu nó chưa tồn tại
    output_dir = Path("output_data")
    output_dir.mkdir(exist_ok=True)

    # Lấy ngày-giờ hiện tại để làm timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    print(f"Bắt đầu lưu file CSV vào thư mục {output_dir}...")

    for table_name, df in data_dict.items():
        if df is not None and not df.empty:
            # Định dạng: [chuc_nang_file]_[ngay-gio].csv
            file_name = f"{table_name}_{timestamp}.csv"
            file_path = output_dir / file_name

            try:
                df.to_csv(file_path, index=False, encoding='utf-8-sig')
                print(f"-> Đã lưu: {file_path}")
            except Exception as e:
                print(f"Lỗi khi lưu file {file_name}: {e}")
        else:
            print(f"-> Bỏ qua {table_name} (không có dữ liệu)")


def load_data_to_db(engine, data_dict):
    """Tải (load) từng DataFrame lên database MySQL."""
    print("Bắt đầu đẩy dữ liệu lên database...")
    try:
        for table_name, df in data_dict.items():
            if df is not None and not df.empty:
                df.to_sql(
                    table_name,
                    con=engine,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                print(f"-> Đẩy thành công bảng: {table_name}")
            else:
                print(f"-> Bỏ qua bảng {table_name} (không có dữ liệu).")
        print("\n--- HOÀN TẤT ĐẨY DỮ LIỆU LÊN DB ---")
    except Exception as e:
        print(f"Lỗi khi đẩy dữ liệu lên SQL: {e}")


def main():
    """
    Hàm chính điều phối ETL, nhận 1 tham số --date.
    """
    parser = argparse.ArgumentParser(description="Chạy ETL cho dữ liệu vnstock.")

    # Tính ngày mặc định (hôm nay)
    today_str = datetime.date.today().isoformat()

    # Thêm tham số --date, mặc định là ngày hôm nay
    parser.add_argument(
        '--date',
        help="Ngày chạy ETL (định dạng YYYY-MM-DD)",
        required=False,
        default=today_str
    )
    args = parser.parse_args()

    # --- Bắt đầu quy trình ---
    engine = connect_to_db()
    if not engine:
        return  # Dừng nếu không kết nối được DB

    data_dict = fetch_data(target_date=args.date)

    if data_dict:
        # Bước 1: Lưu file CSV (theo yêu cầu mới của bạn)
        save_data_to_csv(data_dict)

        # Bước 2: Tải dữ liệu lên Database
        load_data_to_db(engine, data_dict)


if __name__ == "__main__":
    main()