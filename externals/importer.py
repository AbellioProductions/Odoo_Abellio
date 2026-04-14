import pyodbc
import requests
import time
import hashlib
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

START_DATE_STR = '2026-04-14 11:53:00'
END_DATE_STR = '2026-04-14 11:56:00'
LOAD_EVENTS = True
LOAD_COUNTS = True
LOAD_PROCESS = True
AUTO_LOAD = True
BATCH_SIZE = 3000

MS_SQL_CONN_STR = 'DRIVER={SQL Server};SERVER=AB-AS03;DATABASE=Connect;UID=Report;PWD=Report_1t'
ODOO_URL = 'https://localhost:8443'

ODOO_DB = 'Abellio_Odoo' 
ODOO_USER = 'admin'
ODOO_PASS = 'admin'

EXCL_MACS = {'M8'}

class DataNorm:
    @staticmethod
    def norm_ts(dt_val) -> str:
        if isinstance(dt_val, str):
            dt_val = datetime.strptime(dt_val[:19], '%Y-%m-%d %H:%M:%S')
        return dt_val.strftime('%Y-%m-%d %H:%M:%S.%f')

    @staticmethod
    def norm_val(val, t_type: str) -> str:
        if t_type == 'process':
            return str(float(val))
        return str(int(float(val)))

class CryptoHash:
    @staticmethod
    def gen_evt_id(ts: str, mac: str, tag: str, val: str) -> str:
        return hashlib.sha256(f"{ts}|{mac}|{tag}|{val}".encode()).hexdigest()

def ensure_ms_conn(conn_str, ext_conn=None):
    if ext_conn:
        try:
            cur = ext_conn.cursor()
            cur.execute("SELECT 1")
            return ext_conn, cur
        except Exception:
            try:
                ext_conn.close()
            except Exception:
                pass

    while True:
        try:
            new_conn = pyodbc.connect(conn_str)
            return new_conn, new_conn.cursor()
        except pyodbc.Error:
            time.sleep(5)

def send_rpc_req(http_sess, url, payload, exp_rx_key, exp_rx_cnt, max_retries=5, base_delay=2):
    for attempt in range(1, max_retries + 1):
        try:
            res = http_sess.post(url, json=payload, timeout=30, verify=False)

            if 'text/html' in res.headers.get('Content-Type', ''):
                html_snippet = res.text[:800].replace('\n', ' ')
                raise RuntimeError(f"HTML instead of JSON! Status: {res.status_code}")
            
            res.raise_for_status()
            res_data = res.json()
            
            if 'error' in res_data:
                raise RuntimeError(f"RPC Error: {res_data['error']}")
                
            rpc_res = res_data.get('result', {})
            if rpc_res.get('status') == 'error':
                raise RuntimeError(f"Odoo Import Error: {rpc_res.get('message')}")
                
            rx_cnt = rpc_res.get(exp_rx_key, 0)
            if rx_cnt != exp_rx_cnt:
                raise RuntimeError(f"ACK Mismatch: Sent {exp_rx_cnt}, RX {rx_cnt}")

            return rpc_res
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries:
                raise RuntimeError(f"Network Fault: {e}")
            time.sleep(base_delay * (2 ** (attempt - 1)))
        except RuntimeError as e:
            if attempt == max_retries:
                raise e
            time.sleep(base_delay * (2 ** (attempt - 1)))

def proc_events(ms_cur, http_sess, start_dt_str, end_dt_str):
    if not LOAD_EVENTS:
        return
        
    print(f"-> Extracting Events: {start_dt_str} to {end_dt_str}...")
    ms_cur.execute("""
        SELECT e.StartTime, e.ArrivedTime, a.Code, e.Value
        FROM dbo.tblDATRawEventAuto e
        JOIN dbo.tblCFGAsset a ON e.AssetID = a.AssetID
        WHERE e.StartTime >= ? AND e.StartTime <= ?
        ORDER BY e.StartTime ASC
    """, (start_dt_str, end_dt_str))
    
    total_sent = 0
    fetch_batches = iter(lambda: ms_cur.fetchmany(BATCH_SIZE), [])
    for i, rows in enumerate(fetch_batches, start=1):
        batch = []
        for r in rows:
            mac_name = r.Code.split(' - ')[0].strip() if r.Code else "UNKNOWN"
            if mac_name in EXCL_MACS:
                continue
            raw_val = int(r.Value)
            tag = 'OEE.nStopRootReason' if raw_val < 10000 else 'OEE.nMachineState'
            final_val = raw_val if raw_val < 10000 else raw_val - 10000
            ts_norm = DataNorm.norm_ts(r.StartTime)
            val_norm = DataNorm.norm_val(final_val, 'event')
            evt_id = CryptoHash.gen_evt_id(ts_norm, mac_name, tag, val_norm)
            arr_norm = DataNorm.norm_ts(r.ArrivedTime) if r.ArrivedTime else ts_norm
            batch.append([ts_norm, arr_norm, mac_name, tag, final_val, evt_id])
            
        if batch:
            print(f"   Sending batch {i} ({len(batch)} events)...")
            payload = {"jsonrpc": "2.0", "method": "call", "params": {"events": batch}}
            send_rpc_req(http_sess, f"{ODOO_URL}/mes/api/import_historical", payload, 'events_rx', len(batch))
            total_sent += len(batch)
            
    print(f"-> Events completed. Total sent: {total_sent}")

def proc_counts(ms_cur, http_sess, start_dt_str, end_dt_str):
    if not LOAD_COUNTS:
        return
        
    print(f"-> Extracting Counts: {start_dt_str} to {end_dt_str}...")
    ms_cur.execute("""
        SELECT c.RecordTime, c.ArrivedTime, a.Code, s.PLCAddress, c.Value
        FROM dbo.tblDATRawCount c
        JOIN dbo.tblCFGAsset a ON c.AssetID = a.AssetID
        JOIN dbo.tblCFGSignal s ON c.SignalID = s.SignalID
        WHERE c.RecordTime >= ? AND c.RecordTime <= ?
        ORDER BY c.RecordTime ASC
    """, (start_dt_str, end_dt_str))
    
    total_sent = 0
    fetch_batches = iter(lambda: ms_cur.fetchmany(BATCH_SIZE), [])
    for i, rows in enumerate(fetch_batches, start=1):
        batch = []
        for r in rows:
            mac_name = r.Code.split(' - ')[0].strip() if r.Code else "UNKNOWN"
            if mac_name in EXCL_MACS:
                continue
            tag = str(r.PLCAddress)
            val_int = int(r.Value)
            ts_norm = DataNorm.norm_ts(r.RecordTime)
            val_norm = DataNorm.norm_val(val_int, 'count')
            evt_id = CryptoHash.gen_evt_id(ts_norm, mac_name, tag, val_norm)
            arr_norm = DataNorm.norm_ts(r.ArrivedTime) if r.ArrivedTime else ts_norm
            batch.append([ts_norm, arr_norm, mac_name, tag, val_int, evt_id])
            
        if batch:
            print(f"   Sending batch {i} ({len(batch)} counts)...")
            payload = {"jsonrpc": "2.0", "method": "call", "params": {"counts": batch}}
            send_rpc_req(http_sess, f"{ODOO_URL}/mes/api/import_historical", payload, 'counts_rx', len(batch))
            total_sent += len(batch)
            
    print(f"-> Counts completed. Total sent: {total_sent}")

def proc_processes(ms_cur, http_sess, start_dt_str, end_dt_str):
    if not LOAD_PROCESS:
        return
        
    print(f"-> Extracting Processes: {start_dt_str} to {end_dt_str}...")
    ms_cur.execute("""
        SELECT p.RecordTime, e2.Code, s.PLCAddress, p.Value
        FROM dbo.tblDATAutoProcess p
        JOIN dbo.tblCFGSignal s ON p.SignalID = s.SignalID
        JOIN dbo.tblCFGEntity e1 ON s.EntityID = e1.EntityID
        JOIN dbo.tblCFGEntity e2 ON e1.ParentID = e2.EntityID
        WHERE p.RecordTime >= ? AND p.RecordTime <= ?
        ORDER BY p.RecordTime ASC
    """, (start_dt_str, end_dt_str))
    
    total_sent = 0
    fetch_batches = iter(lambda: ms_cur.fetchmany(BATCH_SIZE), [])
    for i, rows in enumerate(fetch_batches, start=1):
        batch = []
        for r in rows:
            mac_name = r.Code.split(' - ')[0].strip() if r.Code else "UNKNOWN"
            if mac_name in EXCL_MACS:
                continue
            tag = str(r.PLCAddress)
            val_flt = float(r.Value)
            ts_norm = DataNorm.norm_ts(r.RecordTime)
            val_norm = DataNorm.norm_val(val_flt, 'process')
            evt_id = CryptoHash.gen_evt_id(ts_norm, mac_name, tag, val_norm)
            batch.append([ts_norm, ts_norm, mac_name, tag, val_flt, evt_id])
            
        if batch:
            print(f"   Sending batch {i} ({len(batch)} processes)...")
            payload = {"jsonrpc": "2.0", "method": "call", "params": {"processes": batch}}
            send_rpc_req(http_sess, f"{ODOO_URL}/mes/api/import_historical", payload, 'processes_rx', len(batch))
            total_sent += len(batch)
            
    print(f"-> Processes completed. Total sent: {total_sent}")

def exec_etl_win(ms_cur, http_sess, start_dt, end_dt):
    print(f"\n=== Starting ETL Window: {start_dt} to {end_dt} ===")
    start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    proc_events(ms_cur, http_sess, start_str, end_str)
    proc_counts(ms_cur, http_sess, start_str, end_str)
    proc_processes(ms_cur, http_sess, start_str, end_str)

def main():
    http_sess = requests.Session()
    http_sess.headers.update({
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Connection': 'close'
    })

    auth_url = f"{ODOO_URL.rstrip('/')}/web/session/authenticate"
    auth_payload = {"jsonrpc": "2.0", "method": "call", "params": {"db": ODOO_DB, "login": ODOO_USER, "password": ODOO_PASS}}

    try:
        resp = http_sess.post(auth_url, json=auth_payload, timeout=15, verify=False)
        if resp.status_code != 200 or "error" in resp.json():
            print(f"Auth failed: {resp.text}")
            return
        print("Auth OK")

        dt_fmt = '%Y-%m-%d %H:%M:%S'
        cur_start_dt = datetime.strptime(START_DATE_STR, dt_fmt)
        cur_end_dt = datetime.strptime(END_DATE_STR, dt_fmt)

        ms_conn, ms_cur = ensure_ms_conn(MS_SQL_CONN_STR)
        try:
            exec_etl_win(ms_cur, http_sess, cur_start_dt, cur_end_dt)
        except Exception as e:
            print(f"Init load err: {e}")

        if AUTO_LOAD:
            print("\n=== Entering Auto-Load Mode (polling every 60s) ===")
            while True:
                time.sleep(60)
                now_dt = datetime.now()
                nxt_start_dt = cur_end_dt
                nxt_end_dt = now_dt.replace(second=0, microsecond=0)
                if nxt_start_dt >= nxt_end_dt: 
                    continue
                try:
                    ms_conn, ms_cur = ensure_ms_conn(MS_SQL_CONN_STR, ms_conn)
                    exec_etl_win(ms_cur, http_sess, nxt_start_dt, nxt_end_dt)
                    cur_end_dt = nxt_end_dt
                except Exception as e:
                    print(f"Loop err: {e}")

    finally:
        if 'ms_conn' in locals() and ms_conn:
            ms_conn.close()

if __name__ == "__main__":
    main()
