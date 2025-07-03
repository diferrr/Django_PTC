from django.shortcuts import render
from django.http import JsonResponse
from django.conf import settings
from .mssql import get_units
import pyodbc


def fetch_ptc_data():


    dsn_termocom = ';'.join(f"{k}={v}" for k, v in settings.SQL_SERVER.items())
    conn_termocom = pyodbc.connect(dsn_termocom)
    cursor_termocom = conn_termocom.cursor()

    dsn_lovati = ';'.join(f"{k}={v}" for k, v in settings.LOVATI_SERVER.items())
    conn_lovati = pyodbc.connect(dsn_lovati)
    cursor_lovati = conn_lovati.cursor()

    # Чтение адресов
    cursor_lovati.execute("SELECT PTC, adresa FROM PTC_adrese")
    address_map = {}
    for row in cursor_lovati.fetchall():
        try:
            ptc_id = int(row.PTC.strip())
            address_map[ptc_id] = row.adresa
        except ValueError:
            continue

    # Чтение id_lovati по PTI (PTC)
    cursor_lovati.execute("SELECT PTI, T1, t2, G1, G2, Gacm FROM IDS")
    id_map = {}
    for row in cursor_lovati.fetchall():
        ptc = int(row.PTI)
        id_map[ptc] = {
            'id_lovati_t1': row.T1.strip() if row.T1 else None,
            'id_lovati_t2': row.t2.strip() if row.t2 else None,
            'id_lovati_g1': row.G1.strip() if row.G1 else None,
            'id_lovati_g2': row.G2.strip() if row.G2 else None,
            'id_lovati_q1': row.G2.strip() if row.G2 else None,
            'id_lovati_dg': row.G2.strip() if row.G2 else None,
            'id_lovati_dg_pct': row.G2.strip() if row.G2 else None,
            'id_lovati_gacm': row.Gacm.strip() if row.Gacm else None
        }

    cursor_termocom.execute("""
        SELECT TOP 1000
            u.UNIT_ID, u.UNIT_NAME, u.UNIT_DESC,
            mc.MC_T1_VALUE_INSTANT, mc.MC_T2_VALUE_INSTANT,
            mc.MC_G1_VALUE_INSTANT, mc.MC_G2_VALUE_INSTANT,
            mc.MC_POWER1_VALUE_INSTANT, mc.MC_CINAVH_VALUE_INSTANT,
            mc.MC_DTIME_VALUE_INSTANT,
            dcx.DCX_TR03_VALUE_INSTANT, dcx.DCX_AI08_VALUE_INSTANT,
            dcx.DCX_AI02_VALUE_INSTANT, dcx.DCX_DTIME_VALUE_INSTANT,
            dcx.DCX_CNT3_VALUE_INSTANT, dcx.DCX_CNT4_VALUE_INSTANT,
            comp.PT_MC_GINB_VALUE_INSTANT
        FROM UNITS u
        LEFT JOIN MULTICAL_CURRENT_DATA mc ON u.UNIT_ID = mc.UNIT_ID
        LEFT JOIN DCX7600_CURRENT_DATA dcx ON u.UNIT_ID = dcx.UNIT_ID
        LEFT JOIN PT_MC_COMPUTED_DATA comp ON u.UNIT_ID = comp.UNIT_ID
        WHERE u.UNIT_NAME LIKE 'PT_%'
        ORDER BY mc.MC_DTIME_VALUE_INSTANT DESC
    """)

    data = []
    for row in cursor_termocom.fetchall():
        ptc_str = row.UNIT_NAME.replace('PT_', '')
        try:
            ptc = int(ptc_str)
        except ValueError:
            ptc = None

        g1 = row.MC_G1_VALUE_INSTANT or 0
        g2 = row.MC_G2_VALUE_INSTANT or 0
        dg = g1 - g2
        dg_pct = (dg / g1 * 100) if g1 else 0

        lovati_ids = id_map.get(ptc, {})

        # Формирование ссылок
        id_t1 = (
            f"http://10.1.1.248:1111/?id_lovati={lovati_ids['id_lovati_t1']}"
            if lovati_ids.get('id_lovati_t1')
            else f"http://10.1.1.248:1111/?param_rokura=t1&obiect=PT_{ptc}"
        )

        id_t2 = (
            f"http://10.1.1.248:1111/?id_lovati={lovati_ids['id_lovati_t2']}"
            if lovati_ids.get('id_lovati_t2')
            else f"http://10.1.1.248:1111/?param_rokura=t2&obiect=PT_{ptc}"
        )

        id_g1 = (
            f"http://10.1.1.248:1111/?id_lovati={lovati_ids['id_lovati_g1']}"
            if lovati_ids.get('id_lovati_g1')
            else f"http://10.1.1.248:1111/?param_rokura=g1&obiect=PT_{ptc}"
        )

        id_g2 = (
            f"http://10.1.1.248:1111/?id_lovati={lovati_ids['id_lovati_g2']}"
            if lovati_ids.get('id_lovati_g2')
            else f"http://10.1.1.248:1111/?param_rokura=g2&obiect=PT_{ptc}"
        )

        id_q1 = f"http://10.1.1.248:1111/?param_rokura=q&obiect=PT_{ptc}"

        # Формируем корректные ссылки для ΔG и Δ%
        id_dg = f"http://10.1.1.248:1111/?param_rokura=dg&obiect=PT_{ptc}"
        id_dg_pct = f"http://10.1.1.248:1111/?param_rokura=dg_pct&obiect=PT_{ptc}"


        # Gacm с проверкой наличия id_lovati, иначе fallback на param_rokura=gacm
        id_gacm = (
            f"http://10.1.1.248:1111/?id_lovati={lovati_ids['id_lovati_gacm']}"
            if lovati_ids.get('id_lovati_gacm')
            else f"http://10.1.1.248:1111/?param_rokura=gacm&obiect=PT_{ptc}"
        )

        # Tacm и G_adaos напрямую из Rokura
        id_tacm = f"http://10.1.1.248:1111/?param_rokura=tacm&obiect=PT_{ptc}"
        id_g_adaos = f"http://10.1.1.248:1111/?param_rokura=gadaos&obiect=PT_{ptc}"

        data.append({
            'ptc': ptc_str,
            'address': address_map.get(ptc, ''),
            't1': round(row.MC_T1_VALUE_INSTANT or 0, 1),
            'id_t1': id_t1,
            't2': round(row.MC_T2_VALUE_INSTANT or 0, 1),
            'id_t2': id_t2,
            't3': round(row.DCX_CNT3_VALUE_INSTANT or 0),
            't4': round(row.DCX_CNT4_VALUE_INSTANT or 0),
            'g1': round(g1, 2),
            'id_g1': id_g1,
            'g2': round(g2, 2),
            'id_g2': id_g2,
            'q1': round(row.MC_POWER1_VALUE_INSTANT or 0, 2),
            'id_q1': id_q1,
            'dg': round(dg, 2),
            'id_dg': id_dg,
            'dg_pct': round(dg_pct, 1),
            'id_dg_pct': id_dg_pct,
            'gacm': round(row.MC_CINAVH_VALUE_INSTANT or 0, 2),
            'id_gacm': id_gacm,
            'tacm': round(row.DCX_TR03_VALUE_INSTANT or 0, 1),
            'id_tacm': f"http://10.1.1.248:1111/?param_rokura=tacm&obiect=PT_{ptc}",
            'v220': '✓' if row.DCX_AI08_VALUE_INSTANT else '✗',
            'pump': '✓' if row.DCX_AI02_VALUE_INSTANT else '✗',
            'g_adaos': round(row.PT_MC_GINB_VALUE_INSTANT or 0, 2),
            'id_g_adaos': f"http://10.1.1.248:1111/?param_rokura=gadaos&obiect=PT_{ptc}",

            'time': row.MC_DTIME_VALUE_INSTANT.strftime('%Y-%m-%d %H:%M') if row.MC_DTIME_VALUE_INSTANT else '-',
        })

    conn_termocom.close()
    conn_lovati.close()

    return data


def ptc_table(request):
    return render(request, 'monitoring/ptc_table.html')


def api_ptc_data(request):
    return JsonResponse(fetch_ptc_data(), safe=False)
