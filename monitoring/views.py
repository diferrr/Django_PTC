from django.shortcuts import render
from django.http import JsonResponse
from django.conf import settings
import pyodbc

def fetch_ptc_data():
    pompa_map = {
        "2009": [2],
        "2055": [2, 3],
        "2056": [2],
        "2057": [2],
        "2201": [2],
        "2202": [2, 3],
        "2209": [1],
        "2216": [2],
        "3012": [2],
        "3125": [1, 2, 3],
        "4009": [2],
        "4012": [2],
        "4014": [2],
        "4016": [2],
        "4019": [2],
        "4021": [2],
        "4025": [2],
        "4027": [2],
        "4037": [2],
        "4040": [2],
        "4041": [2],
        "4050": [2],
        "4054": [2],
        "4058": [2],
        "4063": [2],
        "4065": [2],
        "4066": [2],
        "4068": [2],
        "4077": [2],
        "5002": [2],
        "5003": [2],
        "5008": [2],
        "5009": [2],
        "5014": [2],
        "5019": [2],
        "5047": [2],
        "5057": [2],
        "5058": [2],
        "5075": [2],
    }

    dsn_termocom = ';'.join(f"{k}={v}" for k, v in settings.SQL_SERVER.items())
    conn_termocom = pyodbc.connect(dsn_termocom)
    cursor_termocom = conn_termocom.cursor()

    dsn_lovati = ';'.join(f"{k}={v}" for k, v in settings.LOVATI_SERVER.items())
    conn_lovati = pyodbc.connect(dsn_lovati)
    cursor_lovati = conn_lovati.cursor()
    cursor_lovati.execute("SELECT PTC, adresa FROM PTC_adrese")
    address_map = {str(row.PTC).strip(): row.adresa for row in cursor_lovati.fetchall()}
    conn_lovati.close()

    cursor_termocom.execute("""
        SELECT TOP 1000
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
            t3u.UNIT_LCS_VALUE  -- ← вот тут мы добавили поле LCS
        FROM UNITS u
        LEFT JOIN MULTICAL_CURRENT_DATA mc ON u.UNIT_ID = mc.UNIT_ID
        LEFT JOIN DCX7600_CURRENT_DATA dcx ON u.UNIT_ID = dcx.UNIT_ID
        LEFT JOIN PT_MC_COMPUTED_DATA comp ON u.UNIT_ID = comp.UNIT_ID
        LEFT JOIN TERMOCOM3_UNIT t3u ON u.UNIT_ID = t3u.UNIT_ID  -- ← вот это было пропущено!
        WHERE u.UNIT_ENABLED = 1
            AND u.UNIT_NAME LIKE 'PT_%'
        ORDER BY mc.MC_DTIME_VALUE_INSTANT DESC
    """)

    data = []
    for row in cursor_termocom.fetchall():
        ptc_str = row.UNIT_NAME.replace('PT_', '').strip()
        ptc = ptc_str

        g1 = row.MC_G1_VALUE_INSTANT or 0
        g2 = row.MC_G2_VALUE_INSTANT or 0
        dg = g1 - g2
        dg_pct = (dg / g1 * 100) if g1 else 0

        v220_raw = row.DCX_AI08_VALUE or 0
        v220_on = v220_raw >= 12.5

        # Только нужные помпы и только для нужных объектов
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

        data.append({
            'ptc': ptc_str,
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
            'id_t32': f"http://10.1.1.248:1111/?param_rokura=t32&obiect=PT_{ptc}",
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
            'dt': round(row.MC_DT_VALUE or 0, 2),  # ← вот это!
            'id_dt': f"http://10.1.1.248:1111/?param_rokura=dt&obiect=PT_{ptc}",
            'dg_pct': round(dg_pct, 1),
            'id_dg_pct': f"http://10.1.1.248:1111/?param_rokura=dg_pct&obiect=PT_{ptc}",
            'gacm': round(row.MC_CINAVH_VALUE_INSTANT or 0, 2),
            'id_gacm': f"http://10.1.1.248:1111/?param_rokura=gacm&obiect=PT_{ptc}",
            'tacm': round(row.DCX_TR03_VALUE_INSTANT or 0, 1),
            'id_tacm': f"http://10.1.1.248:1111/?param_rokura=tacm&obiect=PT_{ptc}",
            'g_adaos': round(row.PT_MC_GINB_VALUE_INSTANT or 0, 2),
            'id_g_adaos': f"http://10.1.1.248:1111/?param_rokura=gadaos&obiect=PT_{ptc}",
            'sursa': v220_on,
            'id_sursa': f"http://10.1.1.248:1111/?param_rokura=sursa&obiect=PT_{ptc}",
            'pompa': pompa_vals,
            'pompa_nums': pompa_map.get(ptc, []),  # <- Чтобы на фронте знать номера помп
            'id_pompa1': f"http://10.1.1.248:1111/?param_rokura=pompa&obiect=PT_{ptc}",
            'id_pompa2': f"http://10.1.1.248:1111/?param_rokura=pompa2&obiect=PT_{ptc}",
            'id_pompa3': f"http://10.1.1.248:1111/?param_rokura=pompa3&obiect=PT_{ptc}",
            'lcs': round((row.UNIT_LCS_VALUE or 0) * 100, 2),
            'time': row.MC_DTIME_VALUE_INSTANT.strftime('%Y-%m-%d %H:%M') if row.MC_DTIME_VALUE_INSTANT else '-',
        })

    conn_termocom.close()
    return data

def ptc_table(request):
    return render(request, 'monitoring/ptc_table.html')

def api_ptc_data(request):
    return JsonResponse(fetch_ptc_data(), safe=False)
