import mysql.connector
import os
import sys
from dotenv import load_dotenv

load_dotenv()

CONTROLLER_CONFIG = {
    "host": os.getenv("DB_HOST_CONTROLLER"),
    "port": os.getenv("DB_PORT_CONTROLLER"),
    "user": os.getenv("DB_USER_CONTROLLER"),
    "password": os.getenv("DB_PASS_CONTROLLER"),
    "database": os.getenv("DB_NAME_CONTROLLER")
}

# --- SỬA Ở ĐÂY: Kết nối vào Data Mart ---
DATA_MART_CONFIG = {
    "host": os.getenv("DB_HOST_DW"),
    "port": os.getenv("DB_PORT_DW"),
    "user": os.getenv("DB_USER_DW"),
    "password": os.getenv("DB_PASS_DW"),
    "database": "data_mart"
}


class AggregateJob:
    def __init__(self):
        self.config_id = None

    def _get_conn(self, config):
        try:
            return mysql.connector.connect(**config)
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

    def get_job(self):
        conn = self._get_conn(CONTROLLER_CONFIG)
        if not conn: return False
        try:
            cursor = conn.cursor(dictionary=True)
            # Tìm Job đã Load xong DWH
            query = """
                    SELECT * \
                    FROM config
                    WHERE status = 'DW_LOADED' \
                      AND is_processing = FALSE
                    ORDER BY id ASC LIMIT 1 \
                    """
            cursor.execute(query)
            job = cursor.fetchone()

            if not job:
                print("💤 Không có Job cần Aggregate.")
                return False

            self.config_id = job['id']

            cursor.execute("UPDATE config SET status = 'AGGREGATING', is_processing = TRUE WHERE id = %s",
                           (self.config_id,))
            conn.commit()
            print(f"🔒 Job ID {self.config_id} Locked. Status: AGGREGATING")
            return True
        finally:
            conn.close()

    def execute_aggregation(self):
        conn = self._get_conn(DATA_MART_CONFIG)  # Kết nối vào Data Mart
        if not conn: return False
        try:
            cursor = conn.cursor()
            print("⏳ Đang chạy Procedure: Refresh_Data_Mart...")

            # Gọi Procedure mới
            cursor.callproc('Refresh_Data_Mart')
            conn.commit()

            print("✅ Data Mart đã được làm mới.")
            return True
        except Exception as e:
            print(f"❌ Lỗi SQL: {e}")
            self.report_error(f"Agg Error: {e}")
            return False
        finally:
            conn.close()

    def finalize(self):
        conn = self._get_conn(CONTROLLER_CONFIG)
        if not conn: return
        try:
            cursor = conn.cursor()

            # Cập nhật config
            query = "UPDATE config SET status = 'AGGREGATED', is_processing = FALSE, flag = 0 WHERE id = %s"
            cursor.execute(query, (self.config_id,))

            cursor.execute(
                "INSERT INTO logging (id_config, status, description) VALUES (%s, 'SUCCESS', 'Data Mart Refresh Complete')",
                (self.config_id,)
            )
            # --------------------

            conn.commit()
            print("🏁 Job Hoàn tất: AGGREGATED")
        finally:
            conn.close()

    def report_error(self, msg):
        conn = self._get_conn(CONTROLLER_CONFIG)
        if not conn: return
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE config SET status = 'ERR_AGG', is_processing = FALSE WHERE id = %s",
                           (self.config_id,))
            cursor.execute("INSERT INTO logging (id_config, status, description) VALUES (%s, 'ERR', %s)",
                           (self.config_id, msg))
            conn.commit()
        finally:
            conn.close()


if __name__ == "__main__":
    job = AggregateJob()
    if job.get_job():
        if job.execute_aggregation():
            job.finalize()