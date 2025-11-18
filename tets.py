import mysql.connector
from mysql.connector import Error
import pandas as pd
import os
from datetime import datetime
from vnstock import Finance, Vnstock, Listing, Quote
from dotenv import load_dotenv

load_dotenv()
db_user = os.getenv("DB_USER_CONTROLLER")
db_pass = os.getenv("DB_PASS_CONTROLLER")
db_host = os.getenv("DB_HOST_CONTROLLER")
db_port = os.getenv("DB_PORT_CONTROLLER")
db_name = os.getenv("DB_NAME_CONTROLLER")

DB_CONFIG = {
    'host': db_host,
    'user': db_user,
    'password': db_pass,
    'database': db_name,
    'port': db_port
}

RAW_DATA_PATH = "/raw-data"

def fetch_data(target_date: str, symbol: str) -> dict or None:

    print(f"\t[API] Gọi Vnstock cho mã {symbol} ngày: {target_date}...")
    try:
        vnstock_instance = Vnstock()

        company = vnstock_instance.stock(symbol=symbol, source='TCBS').company
        data1 = company.overview()

        finance = Finance(symbol=symbol, source='VCI')
        data2 = finance.ratio(period='year', lang='vi', dropna=True).head()

        listing = Listing()
        data3 = listing.symbols_by_exchange().head()

        data4 = listing.symbols_by_industries().head()

        quote = Quote(symbol=symbol, source='VCI')
        data5 = quote.history(start=target_date, end=target_date, interval='1D')

        data_to_load = {
            "company_overview": data1,
            "finance_ratio": data2,
            "symbols_by_exchange": data3,
            "symbols_by_industries": data4,
            "quote_history": data5
        }

        print("\t[API] Lấy dữ liệu thành công.")
        return data_to_load

    except Exception as e:
        print(f"\t[LỖI API] Lỗi khi lấy dữ liệu Vnstock: {e}")
        return None


def save_data_to_single_csv(data_dict: dict, file_path: str):

    df_list = list(data_dict.values())
    if not df_list or all(df.empty for df in df_list if isinstance(df, pd.DataFrame)):
        raise ValueError("Dữ liệu trả về rỗng hoặc không chứa DataFrame hợp lệ để lưu.")

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w", encoding="utf-8-sig", newline='') as f:
        for i, df in enumerate(df_list, start=1):
            if isinstance(df, pd.DataFrame) and not df.empty:
                f.write(f"--- DATA {i} - {list(data_dict.keys())[i - 1].upper()} ---\n")
                df.to_csv(f, index=False)
                f.write("\n\n")
            elif isinstance(df, pd.DataFrame) and df.empty:
                f.write(f"--- DATA {i} - {list(data_dict.keys())[i - 1].upper()} ---\n")
                f.write("No data returned for this segment.\n\n")

def connect_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"❌ Lỗi khi kết nối tới DB Controller: {e}")
        return None


def get_configs_to_run(conn) -> list:
    query = "SELECT id, data_date, ticker_symbol, directory_file, filename FROM Config WHERE flag = 1"
    try:
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    except Error as e:
        print(f"❌ Lỗi khi lấy config: {e}")
        return []


def update_config_status(conn, config_id, status, is_processing, flag=None):
    """Cập nhật trạng thái Config."""
    query = "UPDATE Config SET status_config = %s, is_processing = %s, update_at = %s"
    params = [status, is_processing, datetime.now()]
    if flag is not None:
        query += ", flag = %s"
        params.append(flag)
    query += " WHERE id = %s"
    params.append(config_id)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, tuple(params))
        conn.commit()
    except Error as e:
        print(f"❌ Lỗi khi cập nhật config ID {config_id}: {e}")


def log_event(conn, config_id, status, description):
    """Insert vào bảng Log."""
    query = "INSERT INTO Log (id_config, status, description, created_at) VALUES (%s, %s, %s, %s)"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (config_id, status, description, datetime.now()))
        conn.commit()
    except Error as e:
        print(f"❌ Lỗi khi ghi log cho config ID {config_id}: {e}")

def run_extract_process():
    """Thực hiện luồng Extract dữ liệu cổ phiếu đã thống nhất."""
    conn = connect_db()
    if not conn:
        print("Không thể kết nối DB, dừng chương trình.")
        return

    configs_to_run = get_configs_to_run(conn)
    if not configs_to_run:
        print("⏸️ Không tìm thấy config nào có flag=1. Kết thúc.")
        conn.close()
        return

    print(f"🔥 Tìm thấy {len(configs_to_run)} công việc cần chạy.")

    for config in configs_to_run:
        config_id = config['id']

        data_date = config.get('data_date', datetime.now().strftime('%Y-%m-%d'))
        symbol = config.get('ticker_symbol', 'VCB')
        raw_filename = config.get('filename', f"stock_{symbol}_{data_date.replace('-', '')}.csv")
        raw_dir = config.get('directory_file', RAW_DATA_PATH)
        file_path = os.path.join(raw_dir, raw_filename)

        print(f"\n--- Bắt đầu xử lý Config ID: {config_id} ({symbol} - {data_date}) ---")

        # --- A: Xóa Data Cũ ---
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"\t[Dọn dẹp] Đã xóa file cũ tại {file_path}")
            except OSError as e:
                print(f"\t[CẢNH BÁO] Không thể xóa file cũ: {e}")

        update_config_status(conn, config_id, 'CRAWLING', 1)
        log_event(conn, config_id, 'CRAWLING', f"Bắt đầu trích xuất cho {symbol} ngày {data_date}")

        try:
            data_to_load = fetch_data(data_date, symbol)

            if data_to_load is None:
                raise Exception("Lỗi API/Kết nối Vnstock hoặc không có dữ liệu trả về.")

            save_data_to_single_csv(data_to_load, file_path)
            print(f"✅ Đã lưu dữ liệu thành công vào {file_path}")

            update_config_status(conn, config_id, 'CRAWLED', 0, flag=0)
            log_event(conn, config_id, 'CRAWLED', f"Hoàn thành, file đã lưu tại {file_path}")

        except Exception as e:
            error_msg = f"Lỗi trong quá trình Extract: {e}"
            print(f"🚨 {error_msg}")

            update_config_status(conn, config_id, 'ERROR', 0, flag=1)
            log_event(conn, config_id, 'ERROR', error_msg)

    conn.close()
    print("\n--- Hoàn tất quá trình Extract ---")


if __name__ == '__main__':
    run_extract_process()