import streamlit as st
import pandas as pd
import pyodbc
from xml.dom import minidom
from datetime import datetime, timedelta

# ==============================================
# Config general
# ==============================================
st.set_page_config(page_title='Publicación GS1 → EDI', layout='wide')

# Título simplificado y CENTRADO
st.markdown(
    """
    <div style="display:flex;align-items:center;justify-content:center;margin:4px 0 12px 0;">
      <span style="font-size:45px;font-weight:700;">Publicación GS1 → EDI</span>
    </div>
    """,
    unsafe_allow_html=True
)

# ===== CSS helpers =====
HIDE_SIDEBAR_CSS = """
<style>
/* Oculta completamente la barra lateral y el botón de colapsado */
[data-testid="stSidebar"] { display: none !important; }
[data-testid="collapsedControl"] { display: none !important; }
</style>
"""
HIDE_PASSWORD_TOGGLE_CSS = """
<style>
/* Oculta el botón ojo de los inputs de password */
button[aria-label="Show password text"],
button[aria-label="Hide password text"] {
  display: none !important;
}
</style>
"""

# ==============================================
# Utils
# ==============================================
def prettify_xml(xml_text: str) -> str:
    try:
        return minidom.parseString(xml_text.encode('utf-8')).toprettyxml(indent='  ')
    except Exception:
        return xml_text

@st.cache_resource(show_spinner=False)
def get_conn(server: str, database: str, user: str, password: str, encrypt: bool, trust_cert: bool):
    """Crea una conexión pyodbc. Cacheada por Streamlit."""
    last_err = None
    for driver in ('{ODBC Driver 18 for SQL Server}', '{ODBC Driver 17 for SQL Server}'):
        try:
            conn_str = (
                f'DRIVER={driver};'
                f'SERVER={server},1433;'
                f'DATABASE={database};'
                f'UID={user};PWD={password};'
                f'Encrypt={"yes" if encrypt else "no"};'
                f'TrustServerCertificate={"yes" if trust_cert else "no"};'
                'Connection Timeout=15;'
            )
            return pyodbc.connect(conn_str)
        except Exception as e:
            last_err = e
    raise last_err

# ==============================================
# Sidebar: Login
# ==============================================
if 'conn' not in st.session_state:
    st.session_state.conn = None
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

with st.sidebar:
    st.header('Login SQL Server')

    # Oculta el botón "ojo" (show/hide password)
    st.markdown(HIDE_PASSWORD_TOGGLE_CSS, unsafe_allow_html=True)

    with st.form('login_form', clear_on_submit=False):
        server = st.text_input('Servidor', value='ec2-18-210-23-246.compute-1.amazonaws.com')
        database = st.text_input('Base de datos', value='PortalIntegradoGS1BD')
        user = st.text_input('Usuario', value='')
        password = st.text_input('Password', type='password', value='')
        encrypt = st.checkbox('Encrypt=yes', value=True)
        trust = st.checkbox('TrustServerCertificate=yes (requerido por tu servidor)', value=True)
        submitted = st.form_submit_button('Conectar')

if submitted:
    try:
        st.session_state.conn = get_conn(server, database, user, password, encrypt, trust)
        st.session_state.authenticated = True
        st.success('Conectado correctamente.')
    except Exception as e:
        st.session_state.conn = None
        st.session_state.authenticated = False
        st.error(f'Error de conexión: {e}')

# Si no hay conexión, quedamos en login
if not st.session_state.conn:
    st.info('Conéctate en la barra lateral para comenzar.')
    st.stop()

# Si ya está autenticado, ocultamos por completo la sidebar
if st.session_state.get('authenticated'):
    st.markdown(HIDE_SIDEBAR_CSS, unsafe_allow_html=True)

conn = st.session_state.conn

# ==============================================
# Parámetros y estrategia de rendimiento
# ==============================================
if 'page' not in st.session_state:
    st.session_state.page = 1

ctrl_left, ctrl_mid, ctrl_right = st.columns([1,1,2])
with ctrl_left:
    # Preseleccionado en 500
    page_size = st.selectbox('Filas por página', [50,100,200,500], index=3)
with ctrl_mid:
    plataformas = ['(todas)','EDI','AltaEmpresa','BajaEmpresa','AltaUsuario']
    plat_default = plataformas.index('EDI') if 'EDI' in plataformas else 0
    plataforma_sel = st.selectbox('Plataforma (server-side)', plataformas, index=plat_default)
with ctrl_right:
    st.caption('Filtrando por defecto últimos 30 días (server-side).')

end_date = datetime.now().date() + timedelta(days=1)  # exclusivo
start_date = (datetime.now().date() - timedelta(days=30))

SQL_COUNT = """
    SELECT COUNT(*) AS total
    FROM [PortalIntegradoGS1BD].[dbo].[LegacyJobs] j
    LEFT JOIN [PortalIntegradoGS1BD].[dbo].[Empresas] e ON e.IdEmpresa = j.IdEmpresa
    WHERE j.FechaAlta >= ? AND j.FechaAlta < ?
      AND ( ? IS NULL OR j.Plataforma = ? )
"""

SQL_PAGE = """
    SELECT j.Id, j.FechaAlta, j.Plataforma, j.Metodo,
           j.MotivoRechazo, j.IdEmpresa,
           e.CodEmpre, e.RazonSocial, e.CUIT
    FROM [PortalIntegradoGS1BD].[dbo].[LegacyJobs] j
    LEFT JOIN [PortalIntegradoGS1BD].[dbo].[Empresas] e ON e.IdEmpresa = j.IdEmpresa
    WHERE j.FechaAlta >= ? AND j.FechaAlta < ?
      AND ( ? IS NULL OR j.Plataforma = ? )
    ORDER BY j.FechaAlta DESC
    OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
"""

plat_param = None if plataforma_sel == '(todas)' else plataforma_sel
page = st.session_state.page
offset = max((page-1)*page_size, 0)

with st.spinner('Calculando total…'):
    cur = conn.cursor()
    cur.execute(SQL_COUNT, (start_date, end_date, plat_param, plat_param))
    total = cur.fetchone()[0] if cur else 0
    cur.close()

with st.spinner('Cargando página de datos…'):
    cur = conn.cursor()
    cur.execute(SQL_PAGE, (start_date, end_date, plat_param, plat_param, offset, page_size))
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    df = pd.DataFrame.from_records(rows, columns=cols)
    cur.close()

m1, m2, m3, m4 = st.columns(4)
with m1: st.metric('Total últimos 30 días', total)
with m2: st.metric('Página', page)
with m3: st.metric('Filas en página', len(df))
with m4: st.metric('Plataforma', plataforma_sel)

# ================== Semáforo (ROJO y VERDE, enfatizado) ==================
critical_pattern = r'(Error al dar de alta la empresa|No existe la empresa, no se creo el usuario)'
crit_mask = df['MotivoRechazo'].astype(str).str.contains(critical_pattern, case=False, na=False)
crit_count = int(crit_mask.sum())
ok_count = int(len(df) - crit_count)

c1, c2 = st.columns(2)
with c1:
    st.markdown(
        f"""
        <div style="
            padding:16px;border-radius:14px;background:#ff4d4f;
            color:white;font-weight:800;font-size:22px;text-align:center;
            box-shadow:0 6px 18px rgba(255,77,79,0.35);">
            🔴 ERROR: {crit_count}
        </div>
        """,
        unsafe_allow_html=True
    )
with c2:
    st.markdown(
        f"""
        <div style="
            padding:16px;border-radius:14px;background:#06c1671a;
            color:#0e7a3f;font-weight:900;font-size:22px;text-align:center;border:2px solid #23c16b;
            box-shadow:0 6px 18px rgba(35,193,107,0.25);">
            🟢 OK: {ok_count}
        </div>
        """,
        unsafe_allow_html=True
    )

# Checkboxes de filtro (solo rojos / solo verdes)
filter_col1, filter_col2 = st.columns(2)
with filter_col1:
    show_only_crit = st.checkbox("Ver SOLO errores críticos (rojo)", value=False)
with filter_col2:
    show_only_ok = st.checkbox("Ver SOLO OK (verde)", value=False)

if show_only_crit and show_only_ok:
    display_df = df  # si marcan ambos, mostramos todo
elif show_only_crit:
    display_df = df[crit_mask]
elif show_only_ok:
    display_df = df[~crit_mask]
else:
    display_df = df

# ================== Navegación ==================
nav1, nav2, nav3, nav4 = st.columns([1,1,3,3])
with nav1:
    if st.button('⬅️ Anterior', disabled=(page<=1)):
        st.session_state.page = max(page-1, 1)
        st.rerun()
with nav2:
    if st.button('Siguiente ➡️', disabled=(offset+page_size>=total)):
        st.session_state.page = page+1
        st.rerun()
with nav3:
    goto = st.number_input('Ir a página', min_value=1, max_value=max((total+page_size-1)//page_size,1), value=page, step=1)
with nav4:
    if st.button('Ir'):
        st.session_state.page = int(goto)
        st.rerun()

# ================== Tabla con colores ==================
if not display_df.empty:
    # Renombrar "MotivoRechazo" → "Respuestas" para la UI
    display_df = display_df.rename(columns={"MotivoRechazo": "Respuestas"})
    display_df['FechaAlta'] = pd.to_datetime(display_df['FechaAlta'], errors='coerce')

    def highlight_respuesta(val):
        # Regla original para resaltar esa respuesta puntual
        if isinstance(val, str) and 'No existe el usuario, no se creo el usuario' in val:
            return 'background-color: red; color: white;'
        elif isinstance(val, str) and val.strip():
            return 'background-color: #eaffea; color: black;'
        else:
            return ''

    styled_df = display_df[['Id','FechaAlta','Plataforma','CodEmpre','RazonSocial','CUIT','Respuestas']].style.applymap(
        highlight_respuesta, subset=['Respuestas']
    )

    st.dataframe(styled_df, use_container_width=True, hide_index=True)
else:
    st.warning('No hay resultados para los filtros actuales.')

st.markdown('---')

# ================== Detalle: XML por selección ==================
if not display_df.empty:
    left, right = st.columns([1,2])
    with left:
        job_id = st.selectbox('Selecciona un Job Id para ver su XML de Parametros', options=list(display_df['Id']))

    SQL_XML = """
        SELECT CAST(Parametros AS NVARCHAR(MAX)) AS ParametrosXml
        FROM [PortalIntegradoGS1BD].[dbo].[LegacyJobs]
        WHERE Id = ?;
    """
    xml_text = ''
    try:
        cur = conn.cursor()
        cur.execute(SQL_XML, (int(job_id),))
        row = cur.fetchone()
        if row and row[0]:
            xml_text = str(row[0])
        cur.close()
    except Exception as e:
        st.error(f'No se pudo obtener el XML: {e}')

    with right:
        st.subheader('XML de Parametros')
        if xml_text:
            st.code(prettify_xml(xml_text), language='xml')
        else:
            st.info('Selecciona un Job con Parametros disponibles para visualizar el XML.')

# ================== Notas de performance ==================
with st.expander('Sugerencias de performance (DB)'):
    st.markdown(
        """
- **Evitar traer el XML en la grilla**: ahora solo se consulta al seleccionar un registro.
- **Filtros server-side**: últimos 30 días y plataforma se aplican en SQL.
- **Paginación**: OFFSET/FETCH evita cargar todo en memoria.
- **Índices recomendados** (en SQL Server):
  - `CREATE INDEX IX_LegacyJobs_Fecha_Plataforma ON dbo.LegacyJobs(FechaAlta DESC, Plataforma) INCLUDE (MotivoRechazo, IdEmpresa, Metodo);`
  - Asegurar PK/Índice en `Empresas(IdEmpresa)` y si buscan por `CodEmpre`, un índice en `Empresas(CodEmpre)`.
- **Reducción de ancho de fila**: sacar columnas no usadas del SELECT.
        """
    )
