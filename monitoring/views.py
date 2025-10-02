import pyodbc
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render

# Включать ли фолбэк-ссылки для LOVATI без UID
LOVATI_FALLBACK_TO_PARAM = True


# ───────── helpers ─────────
def _dsn(dct) -> str:
    return ';'.join(f"{k}={v}" for k, v in dct.items())


def _to_float(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, str):
            s = x.replace(',', '.').strip()
            if s == '':
                return default
            return float(s)
        return float(x)
    except Exception:
        return default


def _roundf(x, nd=2):
    try:
        return round(_to_float(x), nd)
    except Exception:
        return 0.0


def _looks_like_lovati_uid(val) -> bool:
    """Алфанумерик с хотя бы одной буквой, длина >= 6."""
    if not val:
        return False
    s = str(val).strip()
    return len(s) >= 6 and any(ch.isalpha() for ch in s)


def _collect_lovati_uids(conn) -> dict[str, str]:
    """
    Карта {PTC -> UID} из возможных колонок: можно задать
    settings.LOVATI_UID_COLUMN / LOVATI_UID_COLUMNS_PRIORITY.
    """
    forced = getattr(settings, "LOVATI_UID_COLUMN", None)
    prio = list(getattr(settings, "LOVATI_UID_COLUMNS_PRIORITY", []))

    defaults = [
        'id_lovati', 'idlovati',
        'device_uid', 'deviceuid',
        'device_id', 'deviceid',
        'uid', 'devuid',
        'deveui', 'dev_eui',
        'imei', 'serial',
        'loggerid', 'logger_id', 'idlogger',
    ]

    cols: list[str] = []
    if forced:
        cols.append(forced)
    cols += [c for c in prio if c not in cols]
    cols += [c for c in defaults if c not in cols]

    id_map: dict[str, str] = {}
    cur = conn.cursor()
    for col in cols:
        try:
            cur.execute(f"""
                SELECT RTRIM(PTI) AS PTC, RTRIM({col}) AS UID
                FROM pti
                WHERE {col} IS NOT NULL AND LTRIM(RTRIM({col})) <> ''
            """)
            for r in cur.fetchall():
                ptc = str(r.PTC).strip()
                if ptc in id_map:
                    continue
                uid = str(r.UID).strip()
                if _looks_like_lovati_uid(uid):
                    id_map[ptc] = uid
        except Exception:
            continue
    return id_map


# ───────── core fetchers ─────────
def _fetch_termocom_rows():
    """
    TERMOCOM5: UNITS.UNIT_NAME вида PT_####/#####, возвращаем нормализованные dict.
    """
    pompa_map = {
        "2009": [2], "2055": [2, 3], "2056": [2], "2057": [2], "2201": [2], "2202": [2, 3], "2209": [1],
        "2216": [2], "3012": [2],
        "3125": [1, 2, 3], "4009": [2], "4012": [2], "4014": [2], "4016": [2], "4019": [2], "4021": [2],
        "4025": [2], "4027": [2], "4037": [2], "4040": [2], "4041": [2], "4050": [2], "4054": [2], "4058": [2],
        "4063": [2], "4065": [2], "4066": [2], "4068": [2], "4077": [2], "5002": [2], "5003": [2], "5008": [2],
        "5009": [2], "5014": [2], "5019": [2], "5047": [2], "5057": [2], "5058": [2], "5075": [2],
    }

    dsn_lovati = _dsn(settings.LOVATI_SERVER)
    with pyodbc.connect(dsn_lovati) as conn_l:
        cur_l = conn_l.cursor()
        cur_l.execute("""
            SELECT PTI, AVG(PAR_VALUE) AS AVG_PAR
            FROM GacmPredictPTC
            GROUP BY PTI
        """)
        gacm_p_map = {int(r.PTI): float(r.AVG_PAR) for r in cur_l.fetchall()}

        cur_l.execute("SELECT PTC, adresa FROM PTC_adrese")
        address_map = {str(r.PTC).strip(): r.adresa for r in cur_l.fetchall()}

    dsn_termo = _dsn(settings.SQL_SERVER)
    with pyodbc.connect(dsn_termo) as conn_t:
        cur_t = conn_t.cursor()
        cur_t.execute("""
            SELECT
                u.UNIT_ID, u.UNIT_NAME,
                mc.MC_T1_VALUE_INSTANT, mc.MC_T2_VALUE_INSTANT,
                mc.MC_G1_VALUE_INSTANT, mc.MC_G2_VALUE_INSTANT,
                mc.MC_POWER1_VALUE_INSTANT, mc.MC_CINAVH_VALUE_INSTANT,
                mc.MC_DTIME_VALUE_INSTANT,
                mc.MC_DT_VALUE,
                dcx.DCX_TR03_VALUE_INSTANT,
                dcx.DCX_AI08_VALUE,
                dcx.DCX_AI01_VALUE,
                dcx.DCX_AI02_VALUE,
                dcx.DCX_AI03_VALUE,
                dcx.DCX_DTIME_VALUE_INSTANT,
                dcx.DCX_CNT3_VALUE_INSTANT, dcx.DCX_CNT4_VALUE_INSTANT,
                comp.PT_MC_GINB_VALUE_INSTANT,
                dcx.DCX_TR01_VALUE AS T31,
                dcx.DCX_TR02_VALUE AS T32,
                dcx.DCX_TR07_VALUE AS T41,
                dcx.DCX_TR05_VALUE AS T42,
                dcx.DCX_TR04_VALUE AS T43,
                dcx.DCX_TR02_VALUE AS T44,
                t3u.UNIT_LCS_VALUE
            FROM UNITS u
            LEFT JOIN MULTICAL_CURRENT_DATA mc ON u.UNIT_ID = mc.UNIT_ID
            LEFT JOIN DCX7600_CURRENT_DATA dcx ON u.UNIT_ID = dcx.UNIT_ID
            LEFT JOIN PT_MC_COMPUTED_DATA comp ON u.UNIT_ID = comp.UNIT_ID
            LEFT JOIN TERMOCOM3_UNIT t3u ON u.UNIT_ID = t3u.UNIT_ID
            WHERE u.UNIT_ENABLED = 1
              AND u.UNIT_NAME LIKE 'PT_%'
              AND (LEN(REPLACE(RTRIM(UNIT_NAME), 'PT_', '')) IN (4,5))
            ORDER BY mc.MC_DTIME_VALUE_INSTANT DESC
        """)

        out = []
        for row in cur_t.fetchall():
            ptc = row.UNIT_NAME.replace('PT_', '').strip()
            unit_id = int(row.UNIT_ID) if row.UNIT_ID is not None else None

            g1 = row.MC_G1_VALUE_INSTANT or 0
            g2 = row.MC_G2_VALUE_INSTANT or 0
            dg = g1 - g2
            dg_pct = (dg / g1 * 100) if g1 else 0

            v220_raw = row.DCX_AI08_VALUE or 0
            v220_on = v220_raw >= 12.5

            pompa_vals = None
            if ptc in pompa_map:
                pompa_vals = []
                for num in pompa_map[ptc]:
                    if num == 1:
                        pompa_vals.append(row.DCX_AI01_VALUE or 0)
                    elif num == 2:
                        pompa_vals.append(row.DCX_AI02_VALUE or 0)
                    elif num == 3:
                        pompa_vals.append(row.DCX_AI03_VALUE or 0)

            out.append({
                'src': 'termocom',
                'ptc': ptc,
                'address': address_map.get(ptc, ''),
                't1': round(row.MC_T1_VALUE_INSTANT or 0, 1),
                'id_t1': f"http://10.1.1.248:1111/?param_rokura=t1&obiect=PT_{ptc}",
                't2': round(row.MC_T2_VALUE_INSTANT or 0, 1),
                'id_t2': f"http://10.1.1.248:1111/?param_rokura=t2&obiect=PT_{ptc}",
                't3': round(row.DCX_CNT3_VALUE_INSTANT or 0),
                't4': round(row.DCX_CNT4_VALUE_INSTANT or 0),
                't31': round(row.T31 or 0, 1),
                'id_t31': f"http://10.1.1.248:1111/?param_rokura=t31&obiect=PT_{ptc}",
                't32': round(row.T32 or 0, 1),
                'id_t32': f"http://10.1.1.248:1111/?param_rokura=t32&обiect=PT_{ptc}".replace('об', 'ob'),
                't41': round(row.T41 or 0, 1),
                'id_t41': f"http://10.1.1.248:1111/?param_rokura=t41&obiect=PT_{ptc}",
                't42': round(row.T42 or 0, 1),
                'id_t42': f"http://10.1.1.248:1111/?param_rokura=t42&obiect=PT_{ptc}",
                't43': round(row.T43 or 0, 1),
                'id_t43': f"http://10.1.1.248:1111/?param_rokura=t43&obiect=PT_{ptc}",
                't44': round(row.T44 or 0, 1),
                'id_t44': f"http://10.1.1.248:1111/?param_rokura=t44&obiect=PT_{ptc}",
                'g1': round(g1, 2),
                'id_g1': f"http://10.1.1.248:1111/?param_rokura=g1&obiect=PT_{ptc}",
                'g2': round(g2, 2),
                'id_g2': f"http://10.1.1.248:1111/?param_rokura=g2&obiect=PT_{ptc}",
                'q1': round(row.MC_POWER1_VALUE_INSTANT or 0, 2),
                'id_q1': f"http://10.1.1.248:1111/?param_rokura=q&obiect=PT_{ptc}",
                'dg': round(dg, 2),
                'id_dg': f"http://10.1.1.248:1111/?param_rokura=dg&obiect=PT_{ptc}",
                'dt': round(row.MC_DT_VALUE or 0, 2),
                'id_dt': f"http://10.1.1.248:1111/?param_rokura=dt&obiect=PT_{ptc}",
                'dg_pct': round(dg_pct, 1),
                'id_dg_pct': f"http://10.1.1.248:1111/?param_rokura=dg_pct&obiect=PT_{ptc}",
                'gacm': round(row.MC_CINAVH_VALUE_INSTANT or 0, 2),
                'id_gacm': f"http://10.1.1.248:1111/?param_rokura=gacm&obiect=PT_{ptc}",
                'gacm_p': round(gacm_p_map.get(unit_id, 0), 2),
                'tacm': round(row.DCX_TR03_VALUE_INSTANT or 0, 1),
                'id_tacm': f"http://10.1.1.248:1111/?param_rokura=tacm&obiect=PT_{ptc}",
                'g_adaos': round(row.PT_MC_GINB_VALUE_INSTANT or 0, 2),
                'id_g_adaos': f"http://10.1.1.248:1111/?param_rokura=gadaos&obiect=PT_{ptc}",
                'sursa': v220_on,
                'id_sursa': f"http://10.1.1.248:1111/?param_rokura=sursa&obiect=PT_{ptc}",
                'pompa': pompa_vals,
                'pompa_nums': pompa_map.get(ptc, []),
                'id_pompa1': f"http://10.1.1.248:1111/?param_rokura=pompa&obiect=PT_{ptc}",
                'id_pompa2': f"http://10.1.1.248:1111/?param_rokura=pompa2&obiect=PT_{ptc}",
                'id_pompa3': f"http://10.1.1.248:1111/?param_rokura=pompa3&obiect=PT_{ptc}",
                'lcs': round((row.UNIT_LCS_VALUE or 0) * 100, 2),
                'time': row.MC_DTIME_VALUE_INSTANT.strftime('%Y-%m-%d %H:%M') if row.MC_DTIME_VALUE_INSTANT else '-',
            })
        return out


def _fetch_lovati_rows():
    """
    LOVATI: ссылки через UID; при его отсутствии — опциональный fallback.
    """
    dsn_lovati = _dsn(settings.LOVATI_SERVER)
    with pyodbc.connect(dsn_lovati) as conn:
        cur = conn.cursor()
        cur.execute(r"""
            SELECT RTRIM(p.pti) AS PTC,
                   RTRIM(p.adres) AS Adresa,
                   ROUND(CAST(RTRIM(p.q1) AS float), 2) AS q1,
                   ROUND(CAST(RTRIM(p.g1) AS float), 2) AS G1,
                   ROUND(CAST(RTRIM(p.g2) AS float), 2) AS G2,
                   ROUND(CAST(RTRIM(p.t1) AS float), 2) AS T1,
                   ROUND(CAST(RTRIM(p.t2) AS float), 2) AS T2,
                   ROUND(CAST(RTRIM(p.tacm) AS float), 2) AS Tacm,
                   ROUND(CAST(RTRIM(p.gacm) AS float), 2) AS Gacm,
                   ROUND(CAST(RTRIM(p.gadaos) AS float), 2) AS Gadaos,
                   ROUND(CAST(RTRIM(p.sursa) AS float), 2) AS V220,
                   ROUND(CAST(RTRIM(p.pompa) AS float), 2) AS Pompa
            FROM pti p
            WHERE p.typeObj = 0
              AND LEN(RTRIM(p.pti)) = 4
              AND (LEFT(RTRIM(p.pti), 1) IN ('1','2','3','4','5'))
            ORDER BY p.pti
        """)
        rows = cur.fetchall()

        cur.execute("SELECT PTC, adresa FROM PTC_adrese")
        address_map = {str(r.PTC).strip(): r.adresa for r in cur.fetchall()}

        id_map = _collect_lovati_uids(conn)

    out = []
    for r in rows:
        ptc = str(r.PTC).strip()
        address = address_map.get(ptc, str(r.Adresa or '').strip())

        g1 = _to_float(r.G1)
        g2 = _to_float(r.G2)
        dg = round(g1 - g2, 2)
        dg_pct = round((dg / g1 * 100.0), 1) if g1 else 100.0

        uid = (id_map.get(ptc) or '').strip()
        if uid:
            base_1111 = f"http://10.1.1.248:1111/?id_lovati={uid}"
        elif LOVATI_FALLBACK_TO_PARAM:
            base_1111 = f"http://10.1.1.248:1111/?param_rokura=q&obiect=PT_{ptc}"
        else:
            base_1111 = None

        out.append({
            'src': 'lovati',
            'ptc': ptc,
            'address': address,
            't1': _roundf(r.T1, 1), 'id_t1': base_1111,
            't2': _roundf(r.T2, 1), 'id_t2': base_1111,
            't3': 0, 't4': 0,
            't31': 0.0, 'id_t31': base_1111,
            't32': 0.0, 'id_t32': base_1111,
            't41': 0.0, 'id_t41': base_1111,
            't42': 0.0, 'id_t42': base_1111,
            't43': 0.0, 'id_t43': base_1111,
            't44': 0.0, 'id_t44': base_1111,
            'g1': _roundf(g1, 2), 'id_g1': base_1111,
            'g2': _roundf(g2, 2), 'id_g2': base_1111,
            'q1': _roundf(r.q1, 2), 'id_q1': base_1111,
            'dg': dg, 'id_dg': base_1111,
            'dt': _roundf(_to_float(r.T1) - _to_float(r.T2), 2), 'id_dt': base_1111,
            'dg_pct': dg_pct, 'id_dg_pct': base_1111,
            'gacm': _roundf(r.Gacm, 2), 'id_gacm': base_1111,
            'gacm_p': 0.0,
            'tacm': _roundf(r.Tacm, 1), 'id_tacm': base_1111,
            'g_adaos': _roundf(r.Gadaos, 2), 'id_g_adaos': base_1111,
            'sursa': _to_float(r.V220) >= 12.5, 'id_sursa': base_1111,
            'pompa': None, 'pompa_nums': [],
            'id_pompa1': base_1111, 'id_pompa2': base_1111, 'id_pompa3': base_1111,
            'lcs': 0.0, 'time': '-',
        })

    return out


def fetch_ptc_data():
    """TERMOCOM5 + LOVATI с de-dup по PTC. Приоритет у TERMOCOM5."""
    termo_rows = _fetch_termocom_rows()
    termo_ptc = {row['ptc'] for row in termo_rows}

    lovati_rows = _fetch_lovati_rows()
    lovati_rows = [r for r in lovati_rows if r['ptc'] not in termo_ptc]

    return termo_rows + lovati_rows


# ───────── Django views ─────────
def ptc_table(request):
    return render(request, 'monitoring/ptc_table.html')


def api_ptc_data(request):
    season = (request.GET.get('season') or 'Iarna').strip()
    season = season if season in ('Iarna', 'Vara', 'Toate') else 'Iarna'

    def _flag(name):
        val = (request.GET.get(name) or '').strip().lower()
        return val in ('1', 'true', 'on', 'yes')

    # --- флаги ---
    t1_en = _flag('t1min_enabled')  # 1. T1 min
    t4_en = _flag('t4min_enabled')  # 2. T4 min
    dt_en = _flag('dtmin_enabled')  # 3. ΔT min
    tacm_en = _flag('tacm_enabled')  # 4. Tacm
    gacm_max_en = _flag('gacm_max_enabled')  # 5. Gacm max
    dgacm_en = _flag('dgacm_enabled')  # 6. ΔGacm max
    g1_min_en = _flag('g1_min_enabled')  # 7. G1 min
    dgp_en = _flag('dgp_enabled')  # 8. ΔG% max
    dg_flow_en = _flag('dg_flow_enabled')  # 9. ΔG max
    gadaos_en = _flag('gadaos_enabled')  # 10. Gadaos max
    dataora_en = _flag('dataora_enabled')  # 11. Ore fără date

    # --- пороги ---
    t1_thr = _to_float(request.GET.get('t1min_t1'), 50.0)
    g1_thr = _to_float(request.GET.get('t1min_g1'), 0.1)

    t4_thr = _to_float(request.GET.get('t4min_t4'), 30.0)

    dt_thr = _to_float(request.GET.get('dtmin_dt'), 5.0)
    t1_over = _to_float(request.GET.get('dtmin_t1_over'), 50.0)

    tacm_min = _to_float(request.GET.get('tacm_min'), 50.0)
    tacm_max = _to_float(request.GET.get('tacm_max'), 60.0)

    gacm_max_limit = _to_float(request.GET.get('gacm_max'), 10.0)

    dgacm_split = _to_float(request.GET.get('dgacm_split'), 5.0)
    dgacm_abs = 1.0  # ΔG ≥ 1.0
    dgacm_pct = 20.0  # ΔG% ≥ 20

    g1_min_limit = _to_float(request.GET.get('g1_min'), 0.5)
    dgp_limit = _to_float(request.GET.get('dgp_limit'), 2.5)
    dg_flow_limit = _to_float(request.GET.get('dg_flow_limit'), 1.0)
    gadaos_limit = _to_float(request.GET.get('gadaos_limit'), 0.1)
    dataora_limit = int(request.GET.get('dataora_limit') or 1)

    search = (request.GET.get('search') or '').strip().lower()

    data = fetch_ptc_data()

    # --- вычисляем триггеры/подсветку ---
    from datetime import datetime
    for r in data:
        # 1. T1 min
        r['t1_trigger'] = (_to_float(r.get('t1')) <= t1_thr) and (_to_float(r.get('g1')) > g1_thr)

        # 2. T4 min
        if t4_en:
            r['t41_red'] = _to_float(r.get('t41')) <= t4_thr
            r['t42_red'] = _to_float(r.get('t42')) <= t4_thr
            r['t43_red'] = _to_float(r.get('t43')) <= t4_thr
            r['t44_red'] = _to_float(r.get('t44')) <= t4_thr
        else:
            r['t41_red'] = r['t42_red'] = r['t43_red'] = r['t44_red'] = False

        # 3. ΔT min
        if dt_en:
            r['t2_red'] = (_to_float(r.get('dt')) < dt_thr) and (_to_float(r.get('t1')) > t1_over)
        else:
            r['t2_red'] = False

        # 4. Tacm
        if tacm_en:
            r['tacm_red'] = (_to_float(r.get('tacm')) <= tacm_min) or (_to_float(r.get('tacm')) >= tacm_max)
        else:
            r['tacm_red'] = False

        # 5. Gacm max
        if gacm_max_en:
            r['gacm_red'] = _to_float(r.get('gacm')) >= gacm_max_limit
        else:
            r['gacm_red'] = False

        # 6. ΔGacm max (ACM) → красит Gacm-P
        if dgacm_en:
            v_gacm = _to_float(r.get('gacm'))
            v_gacm_p = _to_float(r.get('gacm_p'))
            d_gacm = v_gacm - v_gacm_p

            # если Gacm-P > 0, считаем %, иначе ΔG% = 0 (игнорируем)
            d_pct = (d_gacm / v_gacm_p) * 100.0 if v_gacm_p > 0 else 0.0

            # --- ЛОГИКА Streamlit (но с управляемыми лимитами) ---
            dgacm_abs = _to_float(request.GET.get('dgacm_abs'), 1.0)  # ΔG ≥
            dgacm_pct = _to_float(request.GET.get('dgacm_pct'), 20.0)  # ΔG% ≥

            if v_gacm >= dgacm_split:  # dgacm_split остаётся фиксированным (5.0)
                r['dgacm_red'] = d_gacm >= dgacm_abs
            else:
                r['dgacm_red'] = d_pct >= dgacm_pct
        else:
            r['dgacm_red'] = False

        # 7. G1 min
        if g1_min_en:
            r['g1_red'] = _to_float(r.get('g1')) <= g1_min_limit
        else:
            r['g1_red'] = False

        # 8. ΔG% max
        if dgp_en:
            g1 = _to_float(r.get('g1'))
            g2 = _to_float(r.get('g2'))
            dgp = ((g1 - g2) / g1 * 100.0) if g1 else 0
            r['dgp_red'] = dgp > dgp_limit
        else:
            r['dgp_red'] = False

        # 9. ΔG max
        if dg_flow_en:
            g1 = _to_float(r.get('g1'))
            g2 = _to_float(r.get('g2'))
            r['dg_flow_red'] = (g1 - g2) > dg_flow_limit
        else:
            r['dg_flow_red'] = False

        # 10. Gadaos max
        if gadaos_en:
            r['gadaos_red'] = _to_float(r.get('gadaos')) > gadaos_limit
        else:
            r['gadaos_red'] = False

        # 11. Ore fără date (работаем с полем 'time')
        if dataora_en:
            try:
                ts = datetime.strptime(str(r.get('time')), "%Y-%m-%d %H:%M")
                delta_h = (datetime.now() - ts).total_seconds() / 3600
                r['dataora_red'] = delta_h > dataora_limit
            except Exception:
                # если нет даты или формат не распознан → НЕ считаем устаревшим
                r['dataora_red'] = False
        else:
            r['dataora_red'] = False

    # --- фильтрация по включённым правилам ---
    if (t1_en or t4_en or dt_en or tacm_en or gacm_max_en or dgacm_en or
            g1_min_en or dgp_en or dg_flow_en or gadaos_en or dataora_en):
        filtered = []
        for r in data:
            if (t1_en and r['t1_trigger']) \
                    or (t4_en and (r['t41_red'] or r['t42_red'] or r['t43_red'] or r['t44_red'])) \
                    or (dt_en and r['t2_red']) \
                    or (tacm_en and r['tacm_red']) \
                    or (gacm_max_en and r['gacm_red']) \
                    or (dgacm_en and r['dgacm_red']) \
                    or (g1_min_en and r['g1_red']) \
                    or (dgp_en and r['dgp_red']) \
                    or (dg_flow_en and r['dg_flow_red']) \
                    or (gadaos_en and r['gadaos_red']) \
                    or (dataora_en and r['dataora_red']):
                filtered.append(r)
        data = filtered

    # --- поиск ---
    if search:
        data = [
            r for r in data
            if (search in str(r.get('ptc', '')).lower()
                or search in str(r.get('address', '')).lower())
        ]

    return JsonResponse(data, safe=False)
