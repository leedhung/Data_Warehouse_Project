import mysql.connector
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# --- 1. CẤU HÌNH KẾT NỐI 3 SERVER ---

# A. CONTROLLER (Quản lý trạng thái Job)
CONTROLLER_CONFIG = {
    "host": os.getenv("DB_HOST_CONTROLLER"),
    "port": os.getenv("DB_PORT_CONTROLLER"),
    "user": os.getenv("DB_USER_CONTROLLER"),
    "password": os.getenv("DB_PASS_CONTROLLER"),
    "database": os.getenv("DB_NAME_CONTROLLER")
}

# B. STAGING SERVER (Nguồn dữ liệu - Mirror DWH)
# Lưu ý: Kết nối vào schema dwh_production ở server Staging
STAGING_CONFIG = {
    "host": os.getenv("DB_HOST_ST"),
    "port": os.getenv("DB_PORT_ST"),
    "user": os.getenv("DB_USER_ST"),
    "password": os.getenv("DB_PASS_ST"),
    "database": "dwh_production"
}

# C. REAL DATA WAREHOUSE (Đích đến - Server lưu trữ cuối cùng)
DWH_CONFIG = {
    "host": os.getenv("DB_HOST_DW"),
    "port": os.getenv("DB_PORT_DW"),
    "user": os.getenv("DB_USER_DW"),
    "password": os.getenv("DB_PASS_DW"),
    "database": "dwh_production"
}


class LoadDwhJob:
    def __init__(self):
        self.config_id = None
        self.data_companies = []
        self.data_prices = []
        self.data_financials = []

    def _get_conn(self, config):
        try:
            conn = mysql.connector.connect(**config)
            return conn
        except mysql.connector.Error as err:
            # Ẩn password khi in log lỗi
            safe_config = config.copy()
            if 'password' in safe_config: safe_config['password'] = '******'
            print(f"❌ Connection Error to {safe_config.get('host')}: {err}")
            return None

    # --- BƯỚC 1: TÌM JOB CẦN LOAD ---
    def get_job_to_load(self):
        conn = self._get_conn(CONTROLLER_CONFIG)
        if not conn: return False
        try:
            cursor = conn.cursor(dictionary=True)
            # Tìm Job đã Transform xong (TRANSFORMED)
            query = """
                    SELECT * \
                    FROM config
                    WHERE status = 'TRANSFORMED' \
                      AND flag = 1 \
                      AND is_processing = FALSE
                    ORDER BY id ASC LIMIT 1 \
                    """
            cursor.execute(query)
            job = cursor.fetchone()

            if not job:
                print("💤 Không có Job nào cần Load DWH (Trạng thái TRANSFORMED).")
                return False

            self.config_id = job['id']

            # Lock Job
            cursor.execute("UPDATE config SET status = 'LOADING_DWH', is_processing = TRUE WHERE id = %s",
                           (self.config_id,))
            conn.commit()
            print(f"🔒 Đã Lock Job ID: {self.config_id}. Trạng thái: LOADING_DWH")
            return True
        finally:
            conn.close()

    # --- BƯỚC 2: EXTRACT TỪ STAGING (MIRROR DWH) ---
    def extract_from_staging(self):
        print("🚀 Đang lấy dữ liệu từ Server Staging (Mirror DWH)...")
        conn = self._get_conn(STAGING_CONFIG)
        if not conn: return False

        try:
            cursor = conn.cursor()

            # 2.1 Lấy Dimensions (Company)
            # Lấy trực tiếp từ bảng dim_company ở Staging
            cursor.execute("""
                           SELECT symbol, company_name, exchange, industry, company_type
                           FROM dim_company
                           """)
            self.data_companies = cursor.fetchall()
            print(f"   -> Đã lấy {len(self.data_companies)} công ty.")

            # 2.2 Lấy Fact Price
            # QUAN TRỌNG: Phải JOIN về dim_company để lấy SYMBOL.
            # Lý do: ID ở Staging (ví dụ 1) khác ID ở Real DWH (ví dụ 105).
            # Ta dùng Symbol làm cầu nối.
            sql_price = """
                        SELECT dc.symbol, f.date_id, f.open_price, f.high_price, f.low_price, f.close_price, f.volume
                        FROM fact_price_history f
                                 JOIN dim_company dc ON f.company_id = dc.id \
                        """
            cursor.execute(sql_price)
            self.data_prices = cursor.fetchall()
            print(f"   -> Đã lấy {len(self.data_prices)} dòng giá.")

            # 2.3 Lấy Fact Financial
            sql_fin = """
                      SELECT dc.symbol, f.year, f.period, f.roe, f.roa, f.eps, f.pe
                      FROM fact_financial_ratio f
                               JOIN dim_company dc ON f.company_id = dc.id \
                      """
            cursor.execute(sql_fin)
            self.data_financials = cursor.fetchall()
            print(f"   -> Đã lấy {len(self.data_financials)} dòng tài chính.")

            return True
        except Exception as e:
            print(f"❌ Lỗi Extract Staging: {e}")
            self.report_error(f"Extract Error: {e}")
            return False
        finally:
            conn.close()

    # --- BƯỚC 3: LOAD VÀO REAL DATA WAREHOUSE ---
    def load_to_real_dwh(self):
        print("💾 Đang đẩy dữ liệu sang Server DWH Thật...")
        conn = self._get_conn(DWH_CONFIG)
        if not conn:
            self.report_error("Connection Failed to Real DWH")
            return False

        try:
            cursor = conn.cursor()
            conn.start_transaction()

            # 3.1 Load Dimensions (Upsert)
            if self.data_companies:
                sql_dim = """
                          INSERT INTO dim_company (symbol, company_name, exchange, industry, company_type)
                          VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY \
                          UPDATE \
                              company_name = \
                          VALUES (company_name), exchange = \
                          VALUES (exchange), industry = \
                          VALUES (industry), updated_at = NOW() \
                          """
                cursor.executemany(sql_dim, self.data_companies)
                print(f"   ✅ Upserted Dim_Company: {cursor.rowcount} dòng.")

            # 3.2 Load Fact Price
            # Logic: Dùng Subquery (SELECT id FROM dim_company WHERE symbol = %s)
            # để tìm ID đúng trên server đích.
            if self.data_prices:
                sql_price = """
                            INSERT \
                            IGNORE INTO fact_price_history 
                    (company_id, date_id, open_price, high_price, low_price, close_price, volume)
                    VALUES (
                        (SELECT id FROM dim_company WHERE symbol = \
                            %s \
                            LIMIT \
                            1 \
                            ),
                            %s, \
                            %s, \
                            %s, \
                            %s, \
                            %s, \
                            %s
                            ) \
                            """
                cursor.executemany(sql_price, self.data_prices)
                print(f"   ✅ Inserted Fact_Price: {cursor.rowcount} dòng.")

            # 3.3 Load Fact Financial
            if self.data_financials:
                sql_fin = """
                          INSERT \
                          IGNORE INTO fact_financial_ratio
                    (company_id, year, period, roe, roa, eps, pe)
                    VALUES (
                        (SELECT id FROM dim_company WHERE symbol = \
                          %s \
                          LIMIT \
                          1 \
                          ),
                          %s, \
                          %s, \
                          %s, \
                          %s, \
                          %s, \
                          %s
                          ) \
                          """
                cursor.executemany(sql_fin, self.data_financials)
                print(f"   ✅ Inserted Fact_Financial: {cursor.rowcount} dòng.")

            conn.commit()
            return True

        except Exception as e:
            print(f"❌ Lỗi Load DWH: {e}")
            self.report_error(f"Load DWH Error: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    # --- BƯỚC 4: HOÀN TẤT ---
    def finalize_job(self):
        conn = self._get_conn(CONTROLLER_CONFIG)
        if not conn: return
        try:
            cursor = conn.cursor()
            # Kết thúc chu trình: Flag = 0, Status = DW_LOADED
            query = "UPDATE config SET status = 'DW_LOADED', is_processing = FALSE, flag = 1 WHERE id = %s"
            cursor.execute(query, (self.config_id,))
            # MỚI (Đúng): Thêm (self.config_id,) vào cuối
            cursor.execute(
                "INSERT INTO logging (id_config, status, description) VALUES (%s, 'SUCCESS', 'Final Load to Real DWH Complete')",
                (self.config_id,)
            )
            # ------------------------

            conn.commit()
            print("🏁 Job Hoàn tất: DW_LOADED")
        finally:
            conn.close()

    # --- HỖ TRỢ: BÁO LỖI ---
    def report_error(self, msg):
        conn = self._get_conn(CONTROLLER_CONFIG)
        if not conn: return
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE config SET status = 'ERR_DWH', is_processing = FALSE WHERE id = %s",
                           (self.config_id,))
            cursor.execute("INSERT INTO logging (id_config, status, description) VALUES (%s, 'ERR', %s)",
                           (self.config_id, msg))
            conn.commit()
        finally:
            conn.close()


def main():
    job = LoadDwhJob()

    # 1. Tìm Job (TRANSFORMED)
    if job.get_job_to_load():
        # 2. Lấy dữ liệu từ Staging (Đã được validate cấu trúc)
        if job.extract_from_staging():
            # 3. Đẩy sang DWH Thật
            if job.load_to_real_dwh():
                # 4. Hoàn tất
                job.finalize_job()


if __name__ == "__main__":
    main()