import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv


load_dotenv()

# --- CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Stock Data Warehouse Dashboard",
    page_icon="📊",
    layout="wide"
)


# --- KẾT NỐI DATABASE (DATA MART) ---
# Sử dụng st.cache_resource để không phải kết nối lại mỗi khi reload
@st.cache_resource
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST_DW"),
            port=os.getenv("DB_PORT_DW"),
            user=os.getenv("DB_USER_DW"),
            password=os.getenv("DB_PASS_DW"),
            database="data_mart"
        )
        return conn
    except Exception as e:
        st.error(f"❌ Lỗi kết nối Database: {e}")
        return None


# --- HÀM LẤY DỮ LIỆU ---
@st.cache_data(ttl=600)  # Cache dữ liệu trong 10 phút
def load_industry_data():
    conn = get_db_connection()
    if not conn: return pd.DataFrame()
    query = """
            SELECT date_id, industry_name, avg_price_change, total_volume, leading_stock
            FROM agg_industry_daily
            ORDER BY date_id DESC \
            """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=600)
def load_stock_list():
    conn = get_db_connection()
    if not conn: return []
    query = "SELECT DISTINCT symbol FROM agg_stock_monthly ORDER BY symbol"
    df = pd.read_sql(query, conn)
    return df['symbol'].tolist()


@st.cache_data(ttl=600)
def load_stock_history(symbol):
    conn = get_db_connection()
    if not conn: return pd.DataFrame()
    # Lấy dữ liệu tháng và sắp xếp theo thời gian
    query = f"""
        SELECT month_id, open_price, close_price, high_price, low_price, total_volume, price_change_pct
        FROM agg_stock_monthly
        WHERE symbol = '{symbol}'
        ORDER BY month_id ASC
    """
    return pd.read_sql(query, conn)


# --- GIAO DIỆN CHÍNH ---
st.title("📊 Dashboard Phân Tích Chứng Khoán (Data Mart)")
st.markdown("---")

# Tạo Tabs
tab1, tab2 = st.tabs(["🏢 Tổng Quan Ngành", "📈 Phân Tích Cổ Phiếu"])

# === TAB 1: TỔNG QUAN NGÀNH ===
with tab1:
    st.header("Hiệu suất các Ngành (Daily)")
    df_ind = load_industry_data()

    if not df_ind.empty:
        # Lấy ngày mới nhất
        latest_date = df_ind['date_id'].max()
        st.info(f"Dữ liệu cập nhật ngày: {latest_date}")

        df_latest = df_ind[df_ind['date_id'] == latest_date].copy()

        # Biểu đồ 1: Top Ngành Tăng Trưởng (Bar Chart)
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🔥 Top Ngành Tăng Trưởng Mạnh Nhất")
            fig_growth = px.bar(
                df_latest.sort_values('avg_price_change', ascending=True).tail(10),
                x='avg_price_change', y='industry_name',
                orientation='h',
                title=f"Biến động giá trung bình ngày {latest_date} (%)",
                text_auto='.2f',
                color='avg_price_change',
                color_continuous_scale=['red', 'yellow', 'green']
            )
            st.plotly_chart(fig_growth, use_container_width=True)

        with col2:
            st.subheader("💰 Dòng Tiền Theo Ngành (Volume)")
            fig_vol = px.pie(
                df_latest,
                values='total_volume',
                names='industry_name',
                title=f"Tỷ trọng khối lượng giao dịch ngày {latest_date}"
            )
            st.plotly_chart(fig_vol, use_container_width=True)

        # Bảng chi tiết
        st.subheader("Chi tiết từng ngành")
        st.dataframe(df_latest, use_container_width=True)
    else:
        st.warning("Chưa có dữ liệu ngành trong Data Mart.")

# === TAB 2: PHÂN TÍCH CỔ PHIẾU ===
with tab2:
    st.header("Biểu đồ Kỹ thuật Theo Tháng")

    # Sidebar chọn cổ phiếu
    stock_list = load_stock_list()
    if stock_list:
        selected_symbol = st.selectbox("Chọn Mã Cổ Phiếu:", stock_list)

        if selected_symbol:
            df_stock = load_stock_history(selected_symbol)

            if not df_stock.empty:
                # Chuyển đổi month_id (202411) sang datetime để vẽ cho đẹp
                df_stock['date'] = pd.to_datetime(df_stock['month_id'].astype(str), format='%Y%m')

                # Vẽ biểu đồ Nến (Candlestick) kết hợp Volume
                fig = go.Figure()

                # Nến
                fig.add_trace(go.Candlestick(
                    x=df_stock['date'],
                    open=df_stock['open_price'],
                    high=df_stock['high_price'],
                    low=df_stock['low_price'],
                    close=df_stock['close_price'],
                    name='Price'
                ))

                # Layout
                fig.update_layout(
                    title=f"Biểu đồ giá tháng của {selected_symbol}",
                    yaxis_title="Giá (VND)",
                    xaxis_title="Tháng",
                    height=600
                )

                st.plotly_chart(fig, use_container_width=True)

                # Metric thống kê
                last_row = df_stock.iloc[-1]
                col1, col2, col3 = st.columns(3)
                col1.metric("Giá Đóng Cửa (Tháng này)", f"{last_row['close_price']:,.0f}",
                            f"{last_row['price_change_pct']}%")
                col2.metric("Khối lượng GD", f"{last_row['total_volume']:,.0f}")
                col3.metric("Cao Nhất / Thấp Nhất", f"{last_row['high_price']:,.0f} / {last_row['low_price']:,.0f}")

                with st.expander("Xem dữ liệu chi tiết"):
                    st.dataframe(df_stock)
            else:
                st.warning(f"Không tìm thấy dữ liệu lịch sử cho mã {selected_symbol}")
    else:
        st.warning("Chưa có danh sách cổ phiếu trong Data Mart.")

# --- FOOTER ---
st.markdown("---")
st.caption("Data Warehouse Project - Built with Streamlit & MySQL")