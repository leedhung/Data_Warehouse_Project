import mysql.connector
import os
import json
import sys
from dotenv import load_dotenv

# Tải biến môi trường
load_dotenv()

# --- 1. CẤU HÌNH KẾT NỐI DATABASE ---

# DB Controller: Quản lý Job/Config/Logging
CONTROLLER_DB_CONFIG = {
    "host": os.getenv("DB_HOST_CONTROLLER"),
    "port": os.getenv("DB_PORT_CONTROLLER"),
    "user": os.getenv("DB_USER_CONTROLLER"),
    "password": os.getenv("DB_PASS_CONTROLLER"),
    "database": os.getenv("DB_NAME_CONTROLLER")
}

# DB ODS Buffer: Nơi chứa Procedure Parse_JSON_To_ODS
# (Thường chung Host với Controller nhưng khác Schema 'ods_buffer')
ODS_DB_CONFIG = {
    "host": os.getenv("DB_HOST_ST"),
    "port": os.getenv("DB_PORT_ST"),
    "user": os.getenv("DB_USER_ST"),
    "password": os.getenv("DB_PASS_ST"),
    "database": "ods_buffer"
}

# DB DWH Production: Nơi chứa Procedure Sync_ODS_To_DWH
# (Thường chung Host với Controller nhưng khác Schema 'dwh_production')
DWH_DB_CONFIG = {
    "host": os.getenv("DB_HOST_ST"),
    "port": os.getenv("DB_PORT_ST"),
    "user": os.getenv("DB_USER_ST"),
    "password": os.getenv("DB_PASS_ST"),
    "database": "dwh_production"
}

# Đường dẫn file Symbol để lọc
SYMBOL_FILE = os.getenv(
    "SYMBOL_FILE_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "symbol_company.txt")
)


class TransformJob:
    def __init__(self):
        self.config_id = None
        self.symbol_json_list = "[]"

    def _get_conn(self, config):
        try:
            return mysql.connector.connect(**config)
        except mysql.connector.Error as err:
            print(f"❌ Connection Error ({config.get('database')}): {err}")
            return None

    # --- BƯỚC 1: TÌM & LOCK JOB ---
    def get_job_to_transform(self):
        conn = self._get_conn(CONTROLLER_DB_CONFIG)
        if not conn: return False
        try:
            cursor = conn.cursor(dictionary=True)
            # Tìm Job đã Load xong (ST_LOADED) và chưa xử lý
            query = """
                    SELECT * \
                    FROM config
                    WHERE status = 'ST_LOADED' \
                      AND flag = 1 \
                      AND is_processing = FALSE
                    ORDER BY id ASC LIMIT 1 \
                    """
            cursor.execute(query)
            job = cursor.fetchone()

            if not job:
                print("💤 Không tìm thấy Job nào cần Transform (ST_LOADED).")
                return False

            self.config_id = job['id']

            cursor.execute("UPDATE config SET status = 'TRANSFORMING', is_processing = TRUE WHERE id = %s",
                           (self.config_id,))
            conn.commit()
            print(f"🔒 Đã Lock Job ID: {self.config_id}. Trạng thái: TRANSFORMING")
            return True
        finally:
            conn.close()

    # --- BƯỚC 2: CHUẨN BỊ LIST LỌC ---
    def prepare_filter_list(self):
        try:
            with open(SYMBOL_FILE, 'r', encoding='utf-8') as f:
                symbols = [line.strip() for line in f if line.strip()]

            self.symbol_json_list = json.dumps(symbols)
            print(f"📋 Đã tải danh sách lọc: {len(symbols)} mã cổ phiếu.")
            return True
        except FileNotFoundError:
            msg = f"Không tìm thấy file symbol tại: {SYMBOL_FILE}"
            print(f"❌ {msg}")
            self.report_error(msg)
            return False

    # --- BƯỚC 3: GỌI SQL PARSE JSON -> ODS ---
    def call_ods_procedure(self):
        conn = self._get_conn(ODS_DB_CONFIG)
        if not conn:
            self.report_error("Không thể kết nối ODS Database")
            return False

        try:
            cursor = conn.cursor()
            print("⏳ Đang chạy Procedure: Parse_JSON_To_ODS...")

            # Gọi thủ tục với tham số là JSON List các mã cổ phiếu
            cursor.callproc('Parse_JSON_To_ODS', [self.symbol_json_list])
            conn.commit()

            print("✅ Thành công: JSON đã được chuyển sang ODS Buffer.")
            return True

        except mysql.connector.Error as err:
            print(f"❌ Lỗi SQL (ODS): {err}")
            self.report_error(f"SQL Error (ODS): {err}")
            return False
        finally:
            conn.close()

    # --- BƯỚC 4: GỌI SQL SYNC ODS -> DWH ---
    def call_dwh_procedure(self):
        conn = self._get_conn(DWH_DB_CONFIG)
        if not conn:
            self.report_error("Không thể kết nối DWH Database")
            return False

        try:
            cursor = conn.cursor()
            print("⏳ Đang chạy Procedure: Sync_ODS_To_DWH...")

            # Gọi thủ tục đồng bộ sang Dim/Fact
            cursor.callproc('Sync_ODS_To_DWH')
            conn.commit()

            print("✅ Thành công: Dữ liệu đã vào kho DWH Production.")
            return True

        except mysql.connector.Error as err:
            print(f"❌ Lỗi SQL (DWH): {err}")
            self.report_error(f"SQL Error (DWH): {err}")
            return False
        finally:
            conn.close()

    # --- BƯỚC 5: HOÀN TẤT ---
    def finalize_job(self):
        conn = self._get_conn(CONTROLLER_DB_CONFIG)
        if not conn: return
        try:
            cursor = conn.cursor()

            query = "UPDATE config SET status = 'TRANSFORMED', is_processing = FALSE, flag = 1 WHERE id = %s"
            cursor.execute(query, (self.config_id,))

            # Ghi Log thành công
            log_query = "INSERT INTO logging (id_config, status, description) VALUES (%s, 'SUCCESS', 'Transform & Load Complete')"
            cursor.execute(log_query, (self.config_id,))

            conn.commit()
            print("🏁 Job Transform Hoàn tất: TRANSFORMED ")
        finally:
            conn.close()

    # --- HỖ TRỢ: BÁO LỖI ---
    def report_error(self, msg):
        conn = self._get_conn(CONTROLLER_DB_CONFIG)
        if not conn: return
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE config SET status = 'ERR_TRANSFORM', is_processing = FALSE WHERE id = %s",
                           (self.config_id,))
            cursor.execute("INSERT INTO logging (id_config, status, description) VALUES (%s, 'ERR', %s)",
                           (self.config_id, msg))
            conn.commit()
        finally:
            conn.close()


def main():
    job = TransformJob()

    # 1. Tìm Job
    if job.get_job_to_transform():
        # 2. Chuẩn bị Filter
        if job.prepare_filter_list():
            # 3. Chạy Staging -> ODS
            if job.call_ods_procedure():
                # 4. Chạy ODS -> DWH
                if job.call_dwh_procedure():
                    # 5. Kết thúc
                    job.finalize_job()


if __name__ == "__main__":
    main()