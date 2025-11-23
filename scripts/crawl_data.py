import pandas as pd
import mysql.connector
from datetime import datetime
import os
import sys
import argparse
from dotenv import load_dotenv
from vnstock import Finance, Vnstock, Listing, Quote

# --- [Step 2] Load toàn bộ variable environment ---
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST_CONTROLLER", "localhost"),
    "port": os.getenv("DB_PORT_CONTROLLER", "3306"),
    "user": os.getenv("DB_USER_CONTROLLER"),
    "password": os.getenv("DB_PASS_CONTROLLER"),
    "database": os.getenv("DB_NAME_CONTROLLER")
}

DEFAULT_CSV_PATH = os.getenv("CSV_OUTPUT_PATH", r"D:\Data_Warehouse\csv_output")
SYMBOL_FILE = os.getenv("SYMBOL_FILE_PATH", r"D:\Data_Warehouse\symbol_company.txt")
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

    def _get_db_connection(self):
        # --- [Step 3] Kết nối tới DB ---
        try:
            if any(v is None for v in self.db_config.values()): return None
            if not self.conn or not self.conn.is_connected():
                self.conn = mysql.connector.connect(**self.db_config)
            return self.conn
        except mysql.connector.Error:
            return None

    def _close_db_connection(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()

    def _insert_logging(self, status: str, description: str):
        conn = self._get_db_connection()
        if not conn or not self.config_id: return
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO logging (id_config, status, description) VALUES (%s, %s, %s)",
                           (self.config_id, status, description))
            conn.commit()
            cursor.close()
        except mysql.connector.Error:
            pass

    def setup_config(self):
        conn = self._get_db_connection()
        if not conn: return False

        # (Logic chuẩn bị data để test Step 4)
        try:
            cursor = conn.cursor()
            start_dt = datetime.strptime(self.manual_start or datetime.now().strftime(DATE_FORMAT), DATE_FORMAT)
            end_dt = datetime.strptime(self.manual_end or datetime.now().strftime(DATE_FORMAT), DATE_FORMAT)

            cursor.execute("""
                           INSERT INTO config (status, flag, is_processing, path, data_date_start, data_date_end)
                           VALUES (%s, %s, %s, %s, %s, %s)
                           """, ('READY', 1, False, DEFAULT_CSV_PATH, start_dt, end_dt))

            self.config_id = cursor.lastrowid
            conn.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"Setup Error: {e}")
            return False

    def start_processing(self):
        if not self.config_id: return False
        conn = self._get_db_connection()
        if not conn: return False

        try:
            cursor = conn.cursor(dictionary=True)
            conn.start_transaction()

            # --- [Step 4, 5, 6] Check Flag=1/Ready -> Set Status=CRAWLING & is_processing=1 ---
            cursor.execute("""
                           UPDATE config
                           SET status        = 'CRAWLING',
                               is_processing = TRUE
                           WHERE id = %s
                             AND status = 'READY'
                             AND is_processing = FALSE
                           """, (self.config_id,))

            if cursor.rowcount == 0:
                conn.rollback()
                return False

            conn.commit()
            cursor.execute("SELECT * FROM config WHERE id = %s", (self.config_id,))
            self.job_config = cursor.fetchone()
            cursor.close()
            return True
        except mysql.connector.Error:
            conn.rollback()
            return False

    def execute_crawl(self):
        if not self.job_config: return

        # --- [Step 7] Lấy danh sách ngày cần crawl từ config ---
        start_date = self.job_config['data_date_start'].strftime(DATE_FORMAT)
        end_date = self.job_config['data_date_end'].strftime(DATE_FORMAT)

        # --- [Step 8] Lấy danh sách công ty từ file ---
        try:
            with open(SYMBOL_FILE, 'r', encoding='utf-8') as f:
                symbols = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            symbols = []

        # (Pre-crawl listing data)
        try:
            listing = Listing()
            self.data_listing_exchange = listing.symbols_by_exchange()
            self.data_listing_industries = listing.symbols_by_industries()
        except:
            pass

        # --- [Loop] Crawl cho từng công ty ---
        for symbol in symbols:
            try:
                # --- [Step 9] Load thông tin công ty ---
                company_api = Vnstock().stock(symbol=symbol, source='TCBS').company
                df_overview = company_api.overview()
                df_overview['symbol'] = symbol
                self.crawled_data_overview.append(df_overview)

                # --- [Step 10] Load chỉ số tài chính ---
                finance_api = Finance(symbol=symbol, source='VCI')
                df_ratio = finance_api.ratio(period='year', lang='vi', dropna=True)
                df_ratio['symbol'] = symbol
                self.crawled_data_ratio.append(df_ratio)

                # --- [Step 11] Load chỉ số cổ phiếu ---
                quote_api = Quote(symbol=symbol, source='VCI')
                df_history = quote_api.history(start=start_date, end=end_date, interval='1D')
                df_history['symbol'] = symbol
                self.crawled_data_price.append(df_history)

            except Exception as e:
                # --- [Step 12] Set status=ERR & processing=0 (xử lý nội bộ) ---
                # --- [Step 13] Ghi Log ERR ---
                self._insert_logging('ERR', f"Crawl error {symbol}: {e}")

    def finalize_job(self):
        if not self.config_id: return
        conn = self._get_db_connection()
        if not conn: return
        cursor = conn.cursor()

        # --- [Step 14] Lấy path lưu file ---
        path = self.job_config['path'] if self.job_config else DEFAULT_CSV_PATH
        os.makedirs(path, exist_ok=True)
        date_tag = self.job_config['data_date_end'].strftime(DATE_FORMAT)

        saved_rows = 0

        # --- [Step 15] Chuyển data thành file CSV ---
        def save(data, prefix, is_df=False):
            nonlocal saved_rows
            if not is_df and not data: return
            df = data if is_df else pd.concat(data, ignore_index=True)
            df.to_csv(os.path.join(path, f"{prefix}_{date_tag}.csv"), index=False)
            saved_rows += len(df)

        try:
            save(self.crawled_data_price, "price_history")
            save(self.crawled_data_overview, "company_overview")
            save(self.crawled_data_ratio, "finance_ratio")
            save(self.data_listing_exchange, "listing_exchange", is_df=True)

            # --- [Step 16] Check lưu thành công? ---
            if saved_rows > 0:
                # --- [Step 16.2.1 & 16.2.2] Success -> Update Status CRAWLED & Log ---
                status, flag, log_msg = 'CRAWLED', 1, f"Success: {saved_rows} rows"
                self._insert_logging('SUCCESS', log_msg)
            else:
                # --- [Step 16.1.1 & 16.1.2] Fail -> Update Status ERR & Log ---
                status, flag, log_msg = 'ERR', 0, "Failed: No data saved"
                self._insert_logging('FAIL', log_msg)

            cursor.execute("""
                           UPDATE config
                           SET status        = %s,
                               is_processing = FALSE,
                               flag          = %s
                           WHERE id = %s
                           """, (status, flag, self.config_id))
            conn.commit()

        except Exception as e:
            self._insert_logging('FAIL', f"System Error: {e}")
        finally:
            self._close_db_connection()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str)
    parser.add_argument('--end', type=str)
    args = parser.parse_args()

    job = CrawlJob(DB_CONFIG, args.start, args.end)

    if job.setup_config():  # Init
        if job.start_processing():  # Step 2-6
            job.execute_crawl()  # Step 7-13
            job.finalize_job()  # Step 14-16


if __name__ == "__main__":
    main()