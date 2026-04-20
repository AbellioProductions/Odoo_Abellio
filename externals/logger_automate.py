import time
import sqlite3
import pyads
import requests
import queue
import signal
import threading
import logging
import json
import os
import ctypes
import hashlib
import urllib3
from dataclasses import dataclass
from typing import List, Dict, Tuple, Any, Optional
from datetime import datetime, timezone, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app_dir = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    filename=os.path.join(app_dir, 'edge_node.log'),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s'
)
log = logging.getLogger('EdgeNode')

@dataclass
class SysCfg:
    mac_name: str
    plc_ip: str
    plc_port: int
    db_path: str
    retn_days: int
    cache_path: str
    api_url: str
    api_db: str
    api_usr: str
    api_pwd: str
    cfg_poll_min_sec: float = 5.0
    cfg_poll_max_sec: float = 300.0

@dataclass
class TagCfg:
    tag_name: str
    type: str
    mode: str
    interval_sec: float = 1.0
    is_cumul: bool = False

@dataclass
class TxRec:
    ts: str
    mac_name: str
    tag_type: str
    tag_name: str
    val: str
    evt_id: str

@dataclass
class RawEvent:
    ts: float
    tag_name: str
    val: Any

class CryptoHash:
    @staticmethod
    def gen_evt_id(ts: str, mac: str, tag: str, val: str) -> str:
        return hashlib.sha256(f"{ts}|{mac}|{tag}|{val}".encode()).hexdigest()

    @staticmethod
    def hash_cfg(data: List[Dict[str, Any]]) -> str:
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

class TxRepo:
    def __init__(self, db_path: str, retn_days: int):
        self.retn_days = retn_days
        self.conn = sqlite3.connect(db_path, timeout=60.0, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.mut = threading.Lock()
        self._init_schema()

    def _init_schema(self):
        with self.mut:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tx_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    mac_name TEXT,
                    tag_type TEXT,
                    tag_name TEXT,
                    val TEXT,
                    evt_id TEXT UNIQUE,
                    sync_status INTEGER DEFAULT 0
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_ts ON tx_log(sync_status, ts);")

    def insert_batch(self, recs: List[TxRec]):
        if not recs: return
        with self.mut:
            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                self.conn.executemany("""
                    INSERT OR IGNORE INTO tx_log (ts, mac_name, tag_type, tag_name, val, evt_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [(r.ts, r.mac_name, r.tag_type, r.tag_name, r.val, r.evt_id) for r in recs])
                self.conn.execute("COMMIT;")
            except Exception as e:
                self.conn.execute("ROLLBACK;")
                log.error(f"DB_ERR_INSERT: {e}")

    def get_pending(self, limit: int = 500) -> List[Tuple[int, TxRec]]:
        with self.mut:
            rows = self.conn.execute("""
                SELECT id, ts, mac_name, tag_type, tag_name, val, evt_id
                FROM tx_log WHERE sync_status = 0 ORDER BY ts ASC LIMIT ?
            """, (limit,)).fetchall()
            return [(r[0], TxRec(*r[1:])) for r in rows]

    def mark_synced(self, ids: List[int]):
        if not ids: return
        pl = ",".join("?" * len(ids))
        with self.mut:
            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                self.conn.execute(f"UPDATE tx_log SET sync_status = 1 WHERE id IN ({pl})", ids)
                self.conn.execute("COMMIT;")
            except Exception as e:
                self.conn.execute("ROLLBACK;")
                log.error(f"DB_ERR_MARK_SYNCED: {e}")

    def purge_stale(self):
        cutoff = (datetime.now() - timedelta(days=self.retn_days)).strftime('%Y-%m-%d %H:%M:%S')
        with self.mut:
            self.conn.execute("BEGIN IMMEDIATE;")
            try:
                self.conn.execute("DELETE FROM tx_log WHERE ts <= ?", (cutoff,))
                self.conn.execute("DELETE FROM tx_log WHERE sync_status = 1")
                self.conn.execute("COMMIT;")
            except Exception as e:
                self.conn.execute("ROLLBACK;")
                log.error(f"DB_ERR_PURGE: {e}")

class MesGw:
    def __init__(self, cfg: SysCfg):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.http = requests.Session()
        self.http.headers.update({
            'User-Agent': 'MES-Edge-Node/3.6',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Connection': 'close'
        })
        self._authenticate()

    def _authenticate(self) -> bool:
        try:
            self.http.get(self.cfg.api_url, timeout=10, verify=False)
            auth_url = f"{self.cfg.api_url.rstrip('/')}/web/session/authenticate"
            payload = {
                "jsonrpc": "2.0",
                "method": "call",
                "params": {"db": self.cfg.api_db, "login": self.cfg.api_usr, "password": self.cfg.api_pwd}
            }
            res = self.http.post(auth_url, json=payload, timeout=15, verify=False)
            res.raise_for_status()
            log.info("AUTH_OK")
            return True
        except Exception as e:
            log.error(f"AUTH_FAIL: {e}")
            return False

    def _exec_rpc(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        res = self.http.post(url, json=payload, timeout=25, verify=False)
        res.raise_for_status()
        if 'text/html' in res.headers.get('Content-Type', ''):
            raise ValueError("RESP_FORMAT_ERR_HTML")
        data = res.json()
        if "error" in data:
            raise ValueError(f"RPC_ERR: {data['error']}")
        return data

    def invoke(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.cfg.api_url}{endpoint}"
        payload = {"jsonrpc": "2.0", "method": "call", "params": params}

        with self.lock:
            try:
                data = self._exec_rpc(url, payload)
            except Exception as e:
                log.warning(f"INVOKE_RETRY_TRIG: {e}")
                if not self._authenticate():
                    raise RuntimeError("AUTH_RECV_FAIL")
                data = self._exec_rpc(url, payload)

        return data.get("result", {})

    def transmit(self, records: List[Tuple[int, TxRec]]):
        payload = {"events": [], "counts": [], "processes": []}
        check_map = {'event': 'events', 'count': 'counts', 'process': 'processes'}
        rx_map = {'event': 'events_rx', 'count': 'counts_rx', 'process': 'processes_rx'}
        counters = {'event': 0, 'count': 0, 'process': 0}
        
        for _, r in records:
            try: val = float(r.val) if r.tag_type == 'process' else int(float(r.val))
            except: val = 0
            row = [r.ts, r.ts, r.mac_name, r.tag_name, val, r.evt_id]
            payload[check_map[r.tag_type]].append(row)
            counters[r.tag_type] += 1
            
        if any(payload.values()):
            res = self.invoke("/mes/api/import_historical", payload)
            for t_type, p_key in rx_map.items():
                if counters[t_type] > 0 and res.get(p_key, 0) != counters[t_type]:
                    raise RuntimeError(f"ACK_ERR_{t_type}")

class CfgMgr:
    def __init__(self, cfg: SysCfg, gw: MesGw):
        self.cfg = cfg
        self.gw = gw
        self.hash = ""
        self.tags: List[TagCfg] = []
        self._load_local()

    def _load_local(self):
        if not os.path.exists(self.cfg.cache_path): return
        try:
            with open(self.cfg.cache_path, "r") as f:
                raw = json.load(f)
                self.tags = [TagCfg(**t) for t in raw]
                self.hash = CryptoHash.hash_cfg(raw)
        except Exception as e:
            log.error(f"CFG_LOAD_ERR: {e}")

    def refresh(self) -> bool:
        try:
            raw = self.gw.invoke("/mes/api/get_machine_config", {"mac_name": self.cfg.mac_name}).get("tags", [])
            new_hash = CryptoHash.hash_cfg(raw)
            if new_hash != self.hash:
                with open(self.cfg.cache_path, "w") as f: json.dump(raw, f)
                self.tags = [TagCfg(**t) for t in raw]
                self.hash = new_hash
                return True
        except Exception as e:
            log.error(f"CFG_REFRESH_ERR: {e}")
        return False

class PlcNode:
    def __init__(self, cfg: SysCfg, raw_q: queue.Queue):
        self.cfg = cfg
        self.raw_q = raw_q
        self.plc = pyads.Connection(cfg.plc_ip, cfg.plc_port)
        self.cfg_tags: List[TagCfg] = []
        self.sym_map: Dict[str, Any] = {}
        self.notif_map: Dict[str, Tuple[tuple, Any]] = {}
        self.task_map: Dict[str, Tuple[TagCfg, float]] = {}
        self.is_connected = False
        self.needs_rebind = False

    def apply_cfg(self, tags: List[TagCfg]):
        self.cfg_tags = tags
        self.needs_rebind = True

    def _resolve_type(self, sym: Any) -> Tuple[int, Any]:
        pt = str(sym.plc_type).upper().strip()
        type_map = {
            'BOOL': (1, ctypes.c_bool), 'BYTE': (1, ctypes.c_uint8),
            'WORD': (2, ctypes.c_uint16), 'DWORD': (4, ctypes.c_uint32),
            'LWORD': (8, ctypes.c_uint64), 'SINT': (1, ctypes.c_int8),
            'INT': (2, ctypes.c_int16), 'DINT': (4, ctypes.c_int32),
            'LINT': (8, ctypes.c_int64), 'USINT': (1, ctypes.c_uint8),
            'UINT': (2, ctypes.c_uint16), 'UDINT': (4, ctypes.c_uint32),
            'ULINT': (8, ctypes.c_uint64), 'REAL': (4, ctypes.c_float),
            'LREAL': (8, ctypes.c_double),
        }
        if pt in type_map: return type_map[pt]
        sz = getattr(sym, 'size', getattr(sym, 'index_length', 4))
        if 'REAL' in pt or 'FLOAT' in pt: return sz, ctypes.c_float if sz == 4 else ctypes.c_double
        if 'BOOL' in pt: return sz, ctypes.c_bool
        if sz == 1: return 1, ctypes.c_uint8
        if sz == 2: return 2, ctypes.c_uint16
        if sz == 8: return 8, ctypes.c_uint64
        return 4, ctypes.c_uint32

    def _build_cb(self, tag_name: str, c_type: Any):
        def _cb(notif, _):
            try:
                _, _, val = self.plc.parse_notification(notif, c_type)
                self.raw_q.put_nowait(RawEvent(time.time(), tag_name, val))
            except Exception as e:
                log.error(f"PLC_CB_ERR [{tag_name}]: {e}")
        return _cb

    def _purge_binds(self):
        for h_notif, _ in self.notif_map.values():
            try: self.plc.del_device_notification(*h_notif)
            except: pass
        self.notif_map.clear()
        for sym in self.sym_map.values():
            try: sym.release_handle()
            except: pass
        self.sym_map.clear()
        self.task_map.clear()

    def cycle(self):
        if not self.is_connected:
            try:
                self.plc.open()
                self.is_connected = True
                self.needs_rebind = True
                log.info("PLC_LINK_UP")
            except Exception as e:
                log.error(f"PLC_OPEN_FAIL: {e}")
                time.sleep(5)
                return

        if self.needs_rebind:
            self._purge_binds()
            self.needs_rebind = False
            bind_success = 0
            
            for t in self.cfg_tags:
                try:
                    sym = self.plc.get_symbol(t.tag_name)
                    self.sym_map[t.tag_name] = sym
                    if t.mode == 'on_change':
                        sz, c_type = self._resolve_type(sym)
                        attr = pyads.NotificationAttrib(sz)
                        hndl = self.plc.add_device_notification(t.tag_name, attr, self._build_cb(t.tag_name, c_type))
                        self.notif_map[t.tag_name] = (hndl, None)
                    elif t.mode == 'cyclic':
                        self.task_map[t.tag_name] = (t, 0.0)
                    bind_success += 1
                except Exception as e:
                    log.error(f"TAG_BIND_FAIL [{t.tag_name}]: {e}")
            
            # Самовосстановление при пуске машины (защита от загрузки ОС)
            if self.cfg_tags and bind_success == 0:
                log.warning("PLC_NOT_READY: Zero tags bound. Retrying in 10s...")
                self.is_connected = False
                self.plc.close()
                time.sleep(10)
                return
            else:
                log.info(f"PLC_BOUND: {bind_success}/{len(self.cfg_tags)} tags.")

        now = time.time()
        for t_name, (t_cfg, last) in self.task_map.items():
            if now - last >= t_cfg.interval_sec:
                if self.raw_q.full(): continue
                try:
                    val = self.sym_map[t_name].read()
                    self.raw_q.put_nowait(RawEvent(now, t_name, val))
                    self.task_map[t_name] = (t_cfg, now)
                except pyads.ADSError as e:
                    log.error(f"PLC_READ_ERR (ADSError): {e}. Dropping link.")
                    self.is_connected = False
                    self._purge_binds()
                    self.plc.close()
                    break
                except Exception as e:
                    log.error(f"PLC_READ_ERR [{t_name}]: {e}")

class RuntimeManager:
    def __init__(self, cfg: SysCfg):
        self.cfg = cfg
        self.term_evt = threading.Event()
        self.raw_q = queue.Queue(maxsize=100000)
        self.repo = TxRepo(cfg.db_path, cfg.retn_days)
        self.gw = MesGw(cfg)
        self.cfg_mgr = CfgMgr(cfg, self.gw)
        self.plc = PlcNode(cfg, self.raw_q)
        self.tag_cache: Dict[str, TagCfg] = {}
        self.prev_vals: Dict[str, str] = {}
        self._update_tag_cache(self.cfg_mgr.tags)
        signal.signal(signal.SIGINT, lambda s, f: self.term_evt.set())
        signal.signal(signal.SIGTERM, lambda s, f: self.term_evt.set())

    def _update_tag_cache(self, tags):
        self.tag_cache = {t.tag_name: t for t in tags}

    def _clean_val(self, val: Any) -> str:
        try:
            f_val = float(val)
            if f_val.is_integer(): return str(int(f_val))
            return f"{f_val:.4f}".rstrip('0').rstrip('.')
        except: return str(val)

    def _eval_drift(self, t_name: str, val_str: str) -> bool:
        if self.prev_vals.get(t_name) == val_str: return False
        self.prev_vals[t_name] = val_str
        return True

    def loop_plc(self):
        self.plc.apply_cfg(self.cfg_mgr.tags)
        while not self.term_evt.is_set():
            try: 
                self.plc.cycle()
            except Exception as e:
                log.error(f"LOOP_PLC_CRASH: {e}")
            time.sleep(0.01)

    def loop_pipeline(self):
        batch = []
        last_flush = time.time()
        while not self.term_evt.is_set() or not self.raw_q.empty():
            try:
                raw = self.raw_q.get(timeout=0.5)
                t_cfg = self.tag_cache.get(raw.tag_name)
                if not t_cfg: continue
                val_s = self._clean_val(raw.val)
                if raw.tag_name == 'OEE.nMachineState':
                    try:
                        if float(val_s) > 9: continue
                    except: pass
                if self._eval_drift(raw.tag_name, val_s):
                    dt = datetime.fromtimestamp(raw.ts)
                    ts_s = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                    eid = CryptoHash.gen_evt_id(ts_s, self.cfg.mac_name, raw.tag_name, val_s)
                    batch.append(TxRec(ts_s, self.cfg.mac_name, t_cfg.type, raw.tag_name, val_s, eid))
            except queue.Empty: pass
            except Exception as e:
                log.error(f"PIPE_ERR: {e}")
                
            if len(batch) >= 500 or (batch and time.time() - last_flush >= 5.0):
                self.repo.insert_batch(batch)
                batch.clear()
                last_flush = time.time()

    def loop_tx(self):
        last_purge = time.time()
        while not self.term_evt.is_set():
            if time.time() - last_purge > 3600:
                self.repo.purge_stale()
                last_purge = time.time()
                
            batch = self.repo.get_pending(500)
            if not batch: 
                self.term_evt.wait(2.0)
                continue
                
            try:
                self.gw.transmit(batch)
                self.repo.mark_synced([r[0] for r in batch])
                log.info(f"TX_OK: {len(batch)} records")
            except Exception as e:
                log.error(f"TX_FAIL: {e}")
                self.term_evt.wait(5.0)

    def loop_cfg(self):
        curr_poll = self.cfg.cfg_poll_min_sec
        while not self.term_evt.is_set():
            if self.cfg_mgr.refresh():
                self._update_tag_cache(self.cfg_mgr.tags)
                self.plc.apply_cfg(self.cfg_mgr.tags)
                curr_poll = self.cfg.cfg_poll_min_sec
            else: 
                curr_poll = min(self.cfg.cfg_poll_max_sec, curr_poll * 1.5)
            self.term_evt.wait(curr_poll)

    def run(self):
        threads = [
            threading.Thread(target=self.loop_plc, name="PLC_IO"),
            threading.Thread(target=self.loop_pipeline, name="PIPE"),
            threading.Thread(target=self.loop_tx, name="TX"),
            threading.Thread(target=self.loop_cfg, name="CFG")
        ]
        for t in threads: t.start()
        for t in threads: t.join()

def main():
    app_dir = os.path.dirname(os.path.abspath(__file__))
    cfg = SysCfg(
        mac_name=os.getenv('MAC_NAME', 'M8'),
        plc_ip=os.getenv('PLC_IP', '192.168.2.1.1.1'),
        plc_port=851,
        db_path=os.path.join(app_dir, "tx.sqlite"),
        retn_days=30,
        cache_path=os.path.join(app_dir, "cfg.json"),
        api_url=os.getenv('API_URL', 'https://10.0.0.8:8443'),#https://86.47.88.185:8443
        api_db=os.getenv('API_DB', 'Abellio_Odoo'),
        api_usr=os.getenv('API_USR', 'admin'),
        api_pwd=os.getenv('API_PWD', 'admin')
    )
    RuntimeManager(cfg).run()

if __name__ == "__main__":
    main()
