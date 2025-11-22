import pandas as pd
import mysql.connector
import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- Cấu hình ---
CONTROLLER_DB_CONFIG = {
    "host": os.getenv("DB_HOST_CONTROLLER"),
    "port": os.getenv("DB_PORT_CONTROLLER"),
    "user": os.getenv("DB_USER_CONTROLLER"),
    "password": os.getenv("DB_PASS_CONTROLLER"),
    "database": os.getenv("DB_NAME_CONTROLLER")
}

STAGING_DB_CONFIG = {
    "host": os.getenv("DB_HOST_ST"),
    "port": os.getenv("DB_PORT_ST"),
    "user": os.getenv("DB_USER_ST"),
    "password": os.getenv("DB_PASS_ST"),
    "database": os.getenv("DB_NAME_ST")
}

DATE_FORMAT = '%Y-%m-%d'


class StagingLoadJob:
    def __init__(self):
        self.job_config = None
        self.config_id = None
        self.file_mapping = {}
        self.data_payload = {}

    def _get_conn(self, config):
        try:
            return mysql.connector.connect(**config)
        except mysql.connector.Error as err:
            print(f"Connection Error: {err}")
            return None

    # --- BƯỚC 1: Lấy thông tin Job (Chưa Lock) ---
    def get_candidate_job(self):
        """Tìm job đang chờ (CRAWLED) để lấy ngày và đường dẫn kiểm tra"""
        conn = self._get_conn(CONTROLLER_DB_CONFIG)
        if not conn: return False

        try:
            cursor = conn.cursor(dictionary=True)
            # Chỉ lấy thông tin, KHÔNG dùng FOR UPDATE để tránh lock lâu
            query = """
                    SELECT * \
                    FROM config
                    WHERE status = 'CRAWLED' \
                      AND flag = 1 \
                      AND is_processing = FALSE
                    ORDER BY id ASC LIMIT 1 \
                    """
            cursor.execute(query)
            self.job_config = cursor.fetchone()

            if not self.job_config:
                print("💤 Không có Job nào trạng thái CRAWLED để xử lý.")
                return False

            self.config_id = self.job_config['id']
            print(f"🔍 Tìm thấy Job ID: {self.config_id}. Chuẩn bị kiểm tra file...")
            return True
        finally:
            conn.close()

    # --- BƯỚC 2: Kiểm tra File (Strict Mode) ---
    def check_files_exist(self):
        """Kiểm tra sự tồn tại của 5 file. Thiếu 1 file -> Trả về False ngay"""
        if not self.job_config: return False

        date_tag = self.job_config['data_date_end'].strftime(DATE_FORMAT)
        path = self.job_config['path']

        # Định nghĩa tên file cần check
        self.file_mapping = {
            "company_overview_data": f"company_overview_{date_tag}.csv",
            "finance_ratio_data": f"finance_ratio_{date_tag}.csv",
            "listing_exchange_data": f"listing_exchange_{date_tag}.csv",
            "listing_industries_data": f"listing_industries_{date_tag}.csv",
            "price_history_data": f"price_history_{date_tag}.csv"
        }

        missing_files = []
        for col, filename in self.file_mapping.items():
            full_path = os.path.join(path, filename)
            if not os.path.exists(full_path):
                missing_files.append(filename)

        if missing_files:
            print(f"❌ LỖI NGHIÊM TRỌNG: Thiếu các file sau: {missing_files}")
            # Gọi hàm báo lỗi lên DB để người dùng biết
            self.report_error(f"Missing files: {str(missing_files)}")
            return False  # <-- Dừng quy trình tại đây

        print("✅ Đã tìm thấy đầy đủ 5 file CSV.")
        return True

    # --- BƯỚC 3: Lock Job & Đọc File ---
    def lock_and_read_files(self):
        """Khóa Job và đọc nội dung file vào bộ nhớ"""
        conn = self._get_conn(CONTROLLER_DB_CONFIG)
        if not conn: return False

        try:
            cursor = conn.cursor()
            conn.start_transaction()

            # Lock Job
            update_query = "UPDATE config SET status = 'ST_LOADING', is_processing = TRUE WHERE id = %s"
            cursor.execute(update_query, (self.config_id,))
            conn.commit()

            # Đọc file (Lúc này đã chắc chắn file tồn tại nhờ Bước 2)
            path = self.job_config['path']
            for col, filename in self.file_mapping.items():
                full_path = os.path.join(path, filename)

                if "finance_ratio" in filename:
                    df = pd.read_csv(full_path, header=1)
                else:
                    df = pd.read_csv(full_path)
                # Chuyển thành JSON
                self.data_payload[col] = df.to_json(orient='records', force_ascii=False)

            print("✅ Đã đọc và chuyển đổi dữ liệu sang JSON.")
            return True
        except Exception as e:
            print(f"Lỗi khi đọc/lock: {e}")
            return False
        finally:
            conn.close()

    # --- BƯỚC 4: Load vào Staging ---
        # ... (Các phần khác giữ nguyên)

    def load_to_staging(self):
            conn = self._get_conn(STAGING_DB_CONFIG)
            if not conn:
                print("❌ Không thể kết nối Staging DB.")
                self.report_error("Connection failed to Staging DB")
                return False

            try:
                cursor = conn.cursor()

                # ===> THÊM ĐOẠN NÀY: Xóa sạch dữ liệu cũ trước khi load mới <===
                print("🧹 Đang dọn dẹp bảng staging_raw_data...")
                cursor.execute("TRUNCATE TABLE staging_raw_data")
                # ==============================================================

                insert_query = """
                               INSERT INTO staging_raw_data
                               (company_overview_data, finance_ratio_data, listing_exchange_data,
                                listing_industries_data, price_history_data)
                               VALUES (%s, %s, %s, %s, %s) \
                               """

                values = (
                    self.data_payload["company_overview_data"],
                    self.data_payload["finance_ratio_data"],
                    self.data_payload["listing_exchange_data"],
                    self.data_payload["listing_industries_data"],
                    self.data_payload["price_history_data"]
                )

                cursor.execute(insert_query, values)
                conn.commit()
                print("✅ Đã Insert vào Staging DB thành công.")
                return True
            except Exception as e:
                print(f"❌ Lỗi Insert Staging: {e}")
                self.report_error(f"Staging Insert Error: {str(e)}")
                return False
            finally:
                conn.close()

    # --- BƯỚC 5: Hoàn tất ---
    def finalize_success(self):
        conn = self._get_conn(CONTROLLER_DB_CONFIG)
        if not conn: return
        try:
            cursor = conn.cursor()
            # Thành công: flag=0 để kết thúc chuỗi ETL này
            query = "UPDATE config SET status = 'ST_LOADED', is_processing = FALSE, flag = 1 WHERE id = %s"
            cursor.execute(query, (self.config_id,))

            # Ghi Log
            log_query = "INSERT INTO logging (id_config, status, description) VALUES (%s, 'SUCCESS', 'Loaded to Staging')"
            cursor.execute(log_query, (self.config_id,))

            conn.commit()
            print("🎉 Job hoàn tất thành công (ST_LOADED).")
        finally:
            conn.close()

    # --- Hỗ trợ: Báo lỗi ---
    def report_error(self, message):
        """Cập nhật trạng thái lỗi vào DB để không bị kẹt Job"""
        conn = self._get_conn(CONTROLLER_DB_CONFIG)
        if not conn: return
        try:
            cursor = conn.cursor()
            # Set flag=0 để không chạy lại tự động, hoặc flag=1 nếu muốn retry (tùy bạn)
            # Ở đây tôi để flag=0 và status=ERR_FILE để bạn kiểm tra thủ công
            query = "UPDATE config SET status = 'ERR_STAGING', is_processing = FALSE, flag = 0 WHERE id = %s"
            cursor.execute(query, (self.config_id,))

            log_query = "INSERT INTO logging (id_config, status, description) VALUES (%s, 'ERR', %s)"
            cursor.execute(log_query, (self.config_id, message))
            conn.commit()
            print(f"⚠️ Đã cập nhật trạng thái lỗi cho Job {self.config_id}")
        finally:
            conn.close()


def main():
    job = StagingLoadJob()

    # 1. Lấy thông tin (Chưa Lock)
    if not job.get_candidate_job():
        return

    # 2. KIỂM TRA FILE (Nếu thiếu -> Báo lỗi DB & Thoát ngay)
    if not job.check_files_exist():
        return

    # 3. Lock Job & Đọc file
    if not job.lock_and_read_files():
        return

    # 4. Load vào Staging
    if job.load_to_staging():
        job.finalize_success()


if __name__ == "__main__":
    main()