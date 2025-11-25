import pandas as pd
import mysql.connector
from datetime import datetime
import os
import sys
import argparse
from dotenv import load_dotenv
from vnstock import Finance, Vnstock, Listing, Quote

load_dotenv()
# Bước 2: Load toàn bộ các biến cấu hình kết nối tới db config
DB_CONFIG = {
    "host": os.getenv("DB_HOST_CONTROLLER", "localhost"),
    "port": os.getenv("DB_PORT_CONTROLLER", "3306"),
    "user": os.getenv("DB_USER_CONTROLLER"),
    "password": os.getenv("DB_PASS_CONTROLLER"),
    "database": os.getenv("DB_NAME_CONTROLLER")
}

# Bước 14: lấy path để lưu trữ các file csv
DEFAULT_CSV_PATH = os.getenv(
    "CSV_OUTPUT_PATH",
    r"D:\Learn\Data Warehouse\DW_project\Data_Warehouse_Project\scripts\csv_output"
)
# Bước 8: lấy toàn bộ danh sách các công ty cần theo dõi
SYMBOL_FILE = os.getenv(
    "SYMBOL_FILE_PATH",
    r"D:\Learn\Data Warehouse\DW_project\Data_Warehouse_Project\scripts\symbol_company.txt"
)
# Bước 7:  lấy các ngày cần crawl dữ liệu
TODAY_DATE = datetime.now().strftime('%Y-%m-%d')
DATE_FORMAT = '%Y-%m-%d'


class CrawlJob:
    def __init__(self, db_config, manual_start=None, manual_end=None):
        self.db_config = db_config
        self.manual_start = manual_start
        self.manual_end = manual_end

        self.conn = None
        self.config_id = None
        self.job_config = None
        self.crawled_data_price = []
        self.crawled_data_overview = []
        self.crawled_data_ratio = []
        self.data_listing_exchange = None
        self.data_listing_industries = None
        self.error_count = 0
        self.success_count = 0

    def _get_db_connection(self):
        try:
            if any(v is None for v in self.db_config.values()):
                return None
            if not self.conn or not self.conn.is_connected():
                self.conn = mysql.connector.connect(**self.db_config)
            return self.conn
        except mysql.connector.Error:
            return None

    def _close_db_connection(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()
            self.conn = None

    def _insert_logging(self, status: str, description: str):
        if not self.config_id:
            print(f"[LOGGING FAILED] Status: {status}, Desc: {description}", file=sys.stderr)
            return
        conn = self._get_db_connection()
        if not conn: return
        try:
            cursor = conn.cursor()
            query = "INSERT INTO logging (id_config, status, description) VALUES (%s, %s, %s)"
            cursor.execute(query, (self.config_id, status, description))
            conn.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print(f"Error inserting log: {err}")
            conn.rollback()

    # Bước 3 thử kết nối tới db nếu không kết nối thành công thì kết thúc chương trình
    def setup_config(self):
        conn = self._get_db_connection()
        if not conn: return False
        cursor = conn.cursor(dictionary=True)

        print("\n--- 1. Setting up Config ---")
        try:
            if self.manual_start and self.manual_end:
                print(f"👉 RUNNING MANUAL MODE: {self.manual_start} to {self.manual_end}")
                start_dt = datetime.strptime(self.manual_start, DATE_FORMAT)
                end_dt = datetime.strptime(self.manual_end, DATE_FORMAT)
            else:
                print(f"👉 RUNNING DEFAULT MODE: Today ({TODAY_DATE})")
                start_dt = datetime.strptime(TODAY_DATE, DATE_FORMAT)
                end_dt = datetime.strptime(TODAY_DATE, DATE_FORMAT)

            # Bước 4 insert dòng conflig đầu để bắt đầu chu trình
            # insert với status =  READY và flag = 1
            insert_query = """
                INSERT INTO config (status, flag, is_processing, path, data_date_start, data_date_end)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, ('READY', 1, False, DEFAULT_CSV_PATH, start_dt, end_dt))
            self.config_id = cursor.lastrowid
            conn.commit()
            print(f"Job Setup Complete. Config ID: {self.config_id}")
            return True
        except mysql.connector.Error as err:
            print(f"Config setup error: {err}")
            conn.rollback()
            return False
        except ValueError as ve:
            print(f"Date format error (Use YYYY-MM-DD): {ve}")
            return False
        finally:
            cursor.close()
    # Bước 5 :Kiểm tra xem đã đã có config nào có tatus =  READY và flag = 1
    # Có thì bắt đầu thực hiện crawl dữ liệu , nếu không thì end
    def start_processing(self):
        if not self.config_id: return False
        conn = self._get_db_connection()
        if not conn: return False

        try:
            cursor = conn.cursor(dictionary=True)
            conn.start_transaction()

            # Bước 6: set statuc =  CRAWLING
            update_query = """
                UPDATE config
                SET status = 'CRAWLING', is_processing = TRUE
                WHERE id = %s AND status = 'READY' AND is_processing = FALSE
            """
            cursor.execute(update_query, (self.config_id,))

            if cursor.rowcount == 0:
                conn.rollback()
                return False

            conn.commit()
            cursor.execute("SELECT * FROM config WHERE id = %s", (self.config_id,))
            self.job_config = cursor.fetchone()
            return True

        except mysql.connector.Error:
            conn.rollback()
            return False
        finally:
            if 'cursor' in locals() and cursor: cursor.close()

    def execute_crawl(self):
        if not self.job_config: return
        self._insert_logging('CRAWLING', 'Start crawling 5 data types.')

        start_date_str = self.job_config['data_date_start'].strftime(DATE_FORMAT)
        end_date_str = self.job_config['data_date_end'].strftime(DATE_FORMAT)

        try:
            # Sử dụng đường dẫn file symbol từ cấu hình
            with open(SYMBOL_FILE, 'r', encoding='utf-8') as f:
                symbols = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            symbols = []
            self._insert_logging('ERR', f"File {SYMBOL_FILE} not found.")

        try:
            listing = Listing()
            self.data_listing_exchange = listing.symbols_by_exchange()
            self.data_listing_industries = listing.symbols_by_industries()
            self.success_count += 2
        except Exception as e:
            self._insert_logging('WARN', f"Listing data error (skipped): {e}")
        # Chạy lấy dữ liệu của toàn bo cac cong ty can theo doi
        for symbol in symbols:
            try:

                company_api = Vnstock().stock(symbol=symbol, source='TCBS').company
                finance_api = Finance(symbol=symbol, source='VCI')
                quote_api = Quote(symbol=symbol, source='VCI')

                # Bước 9 : load thông tin công ty
                df_overview = company_api.overview()
                df_overview['symbol'] = symbol
                self.crawled_data_overview.append(df_overview)
                # Bước 10: load chỉ số tài chính
                df_ratio = finance_api.ratio(period='year', lang='vi', dropna=True)
                df_ratio['symbol'] = symbol
                self.crawled_data_ratio.append(df_ratio)
                # Bước 11: Load chỉ số cổ phiếu trong ngày
                df_history = quote_api.history(start=start_date_str, end=end_date_str, interval='1D')
                df_history['symbol'] = symbol
                self.crawled_data_price.append(df_history)

                self.success_count += 3
            except Exception as e:
                # Bước 13: Ghi nhận log nếu có lỗi
                self.error_count += 1
                self._insert_logging('ERR', f"Error for {symbol}: {e}")

    def finalize_job(self):
        if not self.config_id: return
        conn = self._get_db_connection()
        if not conn: return
        # Bước 12 set status = ERR nếu có lỗi
        # Bước 16.1.1: set status = ERR  và is_processing = 0
        final_status = 'ERR'
        final_flag = 0

        try:
            cursor = conn.cursor()
            # Sử dụng path từ DB config hoặc fallback về DEFAULT
            path = self.job_config['path'] if self.job_config else DEFAULT_CSV_PATH
            os.makedirs(path, exist_ok=True)

            date_tag = self.job_config['data_date_end'].strftime(DATE_FORMAT)
            total_rows_saved = 0

            def save_to_csv(data, name_prefix, is_df=False):
                nonlocal total_rows_saved
                if is_df and data is not None:
                    df = data
                elif data and isinstance(data, list):
                    df = pd.concat(data, ignore_index=True)
                else:
                    return 0

                filename = f"{name_prefix}_{date_tag}.csv"
                full_path = os.path.join(path, filename)
                df.to_csv(full_path, index=False)
                print(f"Saved {name_prefix}: {len(df)} rows")
                total_rows_saved += len(df)
                return len(df)
            # Bước 15. Chuyển data vừa crawl được thành file csv
            save_to_csv(self.crawled_data_price, "price_history")
            save_to_csv(self.crawled_data_overview, "company_overview")
            save_to_csv(self.crawled_data_ratio, "finance_ratio")
            save_to_csv(self.data_listing_exchange, "listing_exchange", is_df=True)
            save_to_csv(self.data_listing_industries, "listing_industries", is_df=True)

            if total_rows_saved > 0:
                # Bước 16.2.1 : set status = CRAWED và isprocessing = 0
                final_status = 'CRAWLED'
                final_flag = 1
                # Bước 16.2.2 : Log SUCCESS
                self._insert_logging('SUCCESS', f"Finished. Saved {total_rows_saved} rows.")
            else:
                # Bước: 16.1.2 Log FAIL lại lỗi
                self._insert_logging('FAIL', "No data saved.")

            update_query = "UPDATE config SET status = %s, is_processing = FALSE, flag = %s WHERE id = %s"
            cursor.execute(update_query, (final_status, final_flag, self.config_id))
            conn.commit()

            print(f"Job Finalized. Status: {final_status}")
            cursor.close()

        except Exception as e:
            print(f"Finalize error: {e}")
            conn.rollback()
        finally:
            self._close_db_connection()


def main():
    parser = argparse.ArgumentParser(description="Run Crawl Job")
    parser.add_argument('--start', type=str, help='Start Date (YYYY-MM-DD)', default=None)
    parser.add_argument('--end', type=str, help='End Date (YYYY-MM-DD)', default=None)

    args = parser.parse_args()

    job = CrawlJob(DB_CONFIG, manual_start=args.start, manual_end=args.end)

    if job.setup_config():
        if job.start_processing():
            try:
                job.execute_crawl()
            finally:
                job.finalize_job()


if __name__ == "__main__":
    main()