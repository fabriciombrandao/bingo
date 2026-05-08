"""
=============================================================
  BINGO WHATSAPP - v2.0
  Backend completo com SQLite (sem Google Sheets)
=============================================================
"""

import os, re, json, hashlib, secrets, threading, random, time, unicodedata
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, jsonify, request, session, redirect, url_for

import sqlite3

try:
    from twilio.rest import Client as TwilioClient
    TWILIO_OK = True
except ImportError:
    TWILIO_OK = False

try:
    import qrcode
    QRCODE_OK = True
except ImportError:
    QRCODE_OK = False

# ─────────────────────────────────────────────
APP_DIR       = os.path.dirname(os.path.abspath(__file__))
DB_PATH       = os.path.join(APP_DIR, "bingo.db")
CONFIG_PATH   = os.path.join(APP_DIR, "config.json")
INBOX_PATH    = os.path.join(APP_DIR, "inbox.json")
USUARIOS_PATH = os.path.join(APP_DIR, "usuarios.json")
PASTA_RELAT   = os.path.join(APP_DIR, "relatorios")
PASTA_QRCODES = os.path.join(APP_DIR, "static", "qrcodes")
LOG_PATH      = os.path.join(APP_DIR, "log_atividades.json")
# ─────────────────────────────────────────────

app = Flask(__name__)
_KEY_FILE = os.path.join(APP_DIR, ".flask_secret")
if os.path.exists(_KEY_FILE):
    with open(_KEY_FILE) as f: app.secret_key = f.read().strip()
else:
    _key = secrets.token_hex(32)
    with open(_KEY_FILE, "w") as f: f.write(_key)
    app.secret_key = _key

# Sessão expira em 8 horas de inatividade
from datetime import timedelta
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)
app.config["SESSION_REFRESH_EACH_REQUEST"] = True  # renova a cada request

estado = {"enviando":False,"progresso":0,"total":0,"enviados":0,"ignorados":0,"erros":0,"previsao_envio":0,"log":[]}
_log_lock   = threading.Lock()
_inbox_lock = threading.Lock()
_db_lock    = threading.RLock()  # RLock permite reentrada na mesma thread

# ══════════════════════════════════════════════════════════
#  BANCO DE DADOS
# ══════════════════════════════════════════════════════════

from contextlib import contextmanager

@contextmanager
def get_db():
    acquired = _db_lock.acquire(timeout=20)
    if not acquired:
        raise RuntimeError("Banco de dados ocupado — tente novamente em instantes.")
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    finally:
        _db_lock.release()

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # Força checkpoint do WAL na inicialização — garante que dados recentes
    # que estavam no .wal sejam incorporados ao .db principal
    try:
        conn.execute("PRAGMA wal_checkpoint(FULL)")
    except: pass
    # Verifica e reconstrói índices corrompidos automaticamente
    global _init_db_status
    try:
        resultado = conn.execute("PRAGMA integrity_check").fetchone()
        if resultado and resultado[0] != "ok":
            conn.execute("REINDEX")
            _init_db_status = f"⚠️ Índices corrompidos reconstruídos — {resultado[0]}"
            print(f"[INIT] {_init_db_status}")
        else:
            _init_db_status = "✅ Banco íntegro"
            print(f"[INIT] {_init_db_status}")
    except Exception as e:
        _init_db_status = f"❌ Banco corrompido (malformed): {e}"
        print(f"[INIT] {_init_db_status}")
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contatos (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            lote          TEXT DEFAULT '',
            intervalo     TEXT DEFAULT '',
            vendedor      TEXT DEFAULT '',
            nome          TEXT DEFAULT '',
            telefone      TEXT DEFAULT '',
            whatsapp      TEXT DEFAULT '',
            valor         TEXT DEFAULT '',
            status        TEXT DEFAULT 'Disponivel',
            observacoes   TEXT DEFAULT '',
            ultimo_wa     TEXT DEFAULT '',
            criado_em     TEXT DEFAULT (datetime('now','localtime')),
            atualizado_em TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(lote, intervalo)
        );
        CREATE TABLE IF NOT EXISTS log_envios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contato_id INTEGER, nome TEXT, telefone TEXT,
            status TEXT, mensagem TEXT,
            criado_em TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS auditoria (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            acao       TEXT NOT NULL,
            contato_id INTEGER,
            usuario    TEXT DEFAULT \'\',
            lote       TEXT DEFAULT \'\',
            intervalo  TEXT DEFAULT \'\',
            nome       TEXT DEFAULT \'\',
            telefone   TEXT DEFAULT \'\',
            status_de  TEXT DEFAULT \'\',
            status_para TEXT DEFAULT \'\',
            detalhes   TEXT DEFAULT \'\',
            criado_em  TEXT DEFAULT (datetime(\'now\',\'localtime\'))
        );
        CREATE INDEX IF NOT EXISTS idx_audit_contato ON auditoria(contato_id);
        CREATE INDEX IF NOT EXISTS idx_audit_acao    ON auditoria(acao);
        CREATE INDEX IF NOT EXISTS idx_audit_data    ON auditoria(criado_em);
        CREATE INDEX IF NOT EXISTS idx_tel    ON contatos(telefone);
        CREATE INDEX IF NOT EXISTS idx_status ON contatos(status);
        CREATE INDEX IF NOT EXISTS idx_lote   ON contatos(lote);
        CREATE TABLE IF NOT EXISTS usuarios (
            username    TEXT PRIMARY KEY,
            nome        TEXT DEFAULT \'\',
            senha       TEXT DEFAULT \'\',
            perfil      TEXT DEFAULT \'vendedor\',
            permissoes  TEXT DEFAULT \'\',
            trocar_senha INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS config (
            chave TEXT PRIMARY KEY,
            valor TEXT DEFAULT \'\'
        );
        CREATE TABLE IF NOT EXISTS log_atividades (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            hora      TEXT,
            msg       TEXT,
            tipo      TEXT DEFAULT \'info\'
        );
        CREATE TABLE IF NOT EXISTS premios (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nome        TEXT NOT NULL,
            tipo_batida TEXT DEFAULT \'cartela_cheia\',
            descricao   TEXT DEFAULT \'\',
            foto_base64 TEXT DEFAULT \'\',
            tem_foto    INTEGER DEFAULT 0,
            criado_em   TEXT DEFAULT (datetime(\'now\',\'localtime\'))
        );
        CREATE TABLE IF NOT EXISTS sorteios (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            nome       TEXT NOT NULL,
            data       TEXT DEFAULT \'\',
            status     TEXT DEFAULT \'ativo\',
            criado_em  TEXT DEFAULT (datetime(\'now\',\'localtime\'))
        );
        CREATE TABLE IF NOT EXISTS sorteio_premios (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sorteio_id  INTEGER NOT NULL,
            ordem       INTEGER NOT NULL,
            nome        TEXT NOT NULL,
            tipo_batida TEXT DEFAULT \'cartela_cheia\',
            status      TEXT DEFAULT \'aguardando\',
            premio_ref_id INTEGER DEFAULT NULL,
            cartela_id  INTEGER DEFAULT NULL,
            cartela_intervalo TEXT DEFAULT \'\',
            foto_base64 TEXT DEFAULT \'\',
            tem_foto    INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS sorteio_numeros (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sorteio_id   INTEGER NOT NULL,
            numero       INTEGER NOT NULL,
            ordem        INTEGER NOT NULL,
            criado_em    TEXT DEFAULT (datetime(\'now\',\'localtime\'))
        );
        CREATE TABLE IF NOT EXISTS sorteio_ganhadores (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sorteio_id      INTEGER NOT NULL,
            premio_id       INTEGER NOT NULL,
            contato_id      INTEGER,
            numero_cartela  TEXT,
            nome_ganhador   TEXT,
            verificado_em   TEXT DEFAULT (datetime(\'now\',\'localtime\'))
        );
        CREATE TABLE IF NOT EXISTS camisetas_pedidos (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            cpf               TEXT NOT NULL UNIQUE,
            nome              TEXT NOT NULL,
            telefone          TEXT DEFAULT \'\',
            tamanho           TEXT DEFAULT \'\',
            status_pagamento  TEXT DEFAULT \'Pendente\',
            criado_em         TEXT DEFAULT (datetime(\'now\',\'localtime\')),
            atualizado_em     TEXT DEFAULT (datetime(\'now\',\'localtime\'))
        );
        CREATE TABLE IF NOT EXISTS camisetas_adicionais (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id  INTEGER NOT NULL,
            nome       TEXT NOT NULL,
            telefone   TEXT DEFAULT \'\',
            tamanho    TEXT DEFAULT \'\'
        );
        """)
        # Migrações — ALTER TABLE falha silenciosamente se coluna já existe
        for sql in [
            "ALTER TABLE usuarios ADD COLUMN permissoes TEXT DEFAULT \'\'",
            "ALTER TABLE usuarios ADD COLUMN trocar_senha INTEGER DEFAULT 0",
            "ALTER TABLE usuarios ADD COLUMN ativo INTEGER DEFAULT 1",
            "ALTER TABLE usuarios ADD COLUMN telefone TEXT DEFAULT ''",
            "ALTER TABLE contatos ADD COLUMN observacoes TEXT DEFAULT \'\'",
            "ALTER TABLE contatos ADD COLUMN previsao_pagamento TEXT DEFAULT \'\'",
            "ALTER TABLE contatos ADD COLUMN data_pagamento TEXT DEFAULT \'\'",
            "ALTER TABLE contatos ADD COLUMN forma_pagamento TEXT DEFAULT \'\'",
            "ALTER TABLE contatos ADD COLUMN origem_id INTEGER DEFAULT NULL",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_lote_intervalo ON contatos(lote, intervalo)",
            "ALTER TABLE sorteio_premios ADD COLUMN premio_ref_id INTEGER DEFAULT NULL",
            "ALTER TABLE premios ADD COLUMN descricao TEXT DEFAULT ''",
            "ALTER TABLE premios ADD COLUMN foto_base64 TEXT DEFAULT ''",
            "ALTER TABLE premios ADD COLUMN tem_foto INTEGER DEFAULT 0",
            "ALTER TABLE sorteios ADD COLUMN logo_base64 TEXT DEFAULT ''",
            "ALTER TABLE sorteios ADD COLUMN pausado INTEGER DEFAULT 0",
            "ALTER TABLE sorteio_premios ADD COLUMN cartela_id INTEGER DEFAULT NULL",
            "ALTER TABLE sorteio_premios ADD COLUMN cartela_intervalo TEXT DEFAULT ''",
            "ALTER TABLE sorteio_ganhadores ADD COLUMN desclassificado INTEGER DEFAULT 0",
            "ALTER TABLE sorteio_premios ADD COLUMN foto_base64 TEXT DEFAULT ''",
            "ALTER TABLE sorteio_premios ADD COLUMN tem_foto INTEGER DEFAULT 0",
            "ALTER TABLE camisetas_pedidos ADD COLUMN forma_pagamento TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN data_pagamento TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN entregue INTEGER DEFAULT 0",
            "ALTER TABLE camisetas_pedidos ADD COLUMN data_entrega TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN numero_pedido TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN data_nascimento TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN obs_cadastro TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN obs_pagamento TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN obs_entrega TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN equipe TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN comprovante_base64 TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN valor_pago REAL DEFAULT 0",
            """CREATE TABLE IF NOT EXISTS camisetas_pagamentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pedido_id INTEGER NOT NULL,
                data_pagamento TEXT DEFAULT '',
                valor REAL DEFAULT 0,
                forma_pagamento TEXT DEFAULT '',
                obs TEXT DEFAULT '',
                criado_em TEXT,
                usuario TEXT DEFAULT ''
            )""",
            "ALTER TABLE camisetas_pedidos ADD COLUMN comprovante_tipo TEXT DEFAULT ''",
            "ALTER TABLE camisetas_pedidos ADD COLUMN comprovante_em TEXT DEFAULT ''",
            "ALTER TABLE contatos ADD COLUMN valor_pago REAL DEFAULT 0",
            "ALTER TABLE contatos ADD COLUMN saldo_devedor REAL DEFAULT NULL",
            """CREATE TABLE IF NOT EXISTS pagamentos_parciais (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                contato_id       INTEGER NOT NULL,
                valor            REAL NOT NULL,
                data_pagamento   TEXT DEFAULT '',
                forma_pagamento  TEXT DEFAULT '',
                recebido_por     TEXT DEFAULT '',
                criado_em        TEXT DEFAULT (datetime('now','localtime')),
                usuario          TEXT DEFAULT ''
            )""",
            "CREATE INDEX IF NOT EXISTS idx_pag_parcial_contato ON pagamentos_parciais(contato_id)",

        ]:
            try: conn.execute(sql)
            except: pass

        # Remover UNIQUE do CPF em camisetas_pedidos se ainda existir
        try:
            info = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='camisetas_pedidos'").fetchone()
            if info and 'UNIQUE' in (info['sql'] or '').upper() and 'cpf' in (info['sql'] or '').lower():
                conn.execute("""CREATE TABLE camisetas_pedidos_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cpf TEXT NOT NULL,
                    numero_pedido TEXT DEFAULT '',
                    nome TEXT, telefone TEXT, tamanho TEXT,
                    data_nascimento TEXT DEFAULT '', equipe TEXT DEFAULT '',
                    status_pagamento TEXT DEFAULT 'Pendente',
                    forma_pagamento TEXT DEFAULT '', data_pagamento TEXT DEFAULT '',
                    obs_cadastro TEXT DEFAULT '', obs_pagamento TEXT DEFAULT '',
                    entregue INTEGER DEFAULT 0, data_entrega TEXT DEFAULT '',
                    obs_entrega TEXT DEFAULT '',
                    criado_em TEXT, atualizado_em TEXT
                )""")
                conn.execute("INSERT INTO camisetas_pedidos_new SELECT id,cpf,numero_pedido,nome,telefone,tamanho,data_nascimento,equipe,status_pagamento,forma_pagamento,data_pagamento,obs_cadastro,obs_pagamento,entregue,data_entrega,obs_entrega,criado_em,atualizado_em FROM camisetas_pedidos")
                conn.execute("ALTER TABLE camisetas_pedidos RENAME TO camisetas_pedidos_old")
                conn.execute("ALTER TABLE camisetas_pedidos_new RENAME TO camisetas_pedidos")
                conn.execute("DROP TABLE IF EXISTS camisetas_pedidos_old")
                log('Migração: UNIQUE removido de camisetas_pedidos.cpf', 'success')
        except Exception as em:
            log(f'Migração UNIQUE cpf: {em}', 'warning')

        # Migrar log_envios: converter criado_em de dd/mm/YYYY para YYYY-MM-DD
        try:
            rows = conn.execute("SELECT id, criado_em FROM log_envios WHERE criado_em LIKE '__/__/____%%'").fetchall()
            for row in rows:
                s = row['criado_em'] or ''
                if len(s) >= 10 and s[2] == '/':
                    try:
                        from datetime import datetime as _dtm
                        dt = _dtm.strptime(s[:16], "%d/%m/%Y %H:%M") if len(s) >= 16 else _dtm.strptime(s[:10], "%d/%m/%Y")
                        iso = dt.strftime("%Y-%m-%d %H:%M")
                        conn.execute("UPDATE log_envios SET criado_em=? WHERE id=?", (iso, row['id']))
                    except: pass
            if rows:
                log(f'Migração log_envios: {len(rows)} registros convertidos para ISO', 'info')
        except Exception as em:
            log(f'Migração log_envios: {em}', 'warning')

        # Garante admin — NUNCA reimporta usuarios.json se já existem usuários no banco
        count = conn.execute("SELECT COUNT(*) FROM usuarios").fetchone()[0]
        if count == 0:
            import json as _json, hashlib as _hl
            importado = False
            try:
                with open(os.path.join(APP_DIR, "usuarios.json")) as f:
                    udata = _json.load(f)
                if udata:
                    for uname, ud in udata.items():
                        conn.execute("INSERT OR IGNORE INTO usuarios (username,nome,senha,perfil) VALUES (?,?,?,?)",
                            (uname, ud.get("nome",""), ud.get("senha",""), ud.get("perfil","vendedor")))
                    conn.commit()
                    importado = True
                    print(f"[INIT] Importados {len(udata)} usuarios do usuarios.json")
            except: pass
            if not importado:
                admin_senha = _hl.sha256(b"admin123").hexdigest()
                conn.execute("INSERT OR IGNORE INTO usuarios (username,nome,senha,perfil) VALUES (?,?,?,?)",
                    ("admin","Administrador", admin_senha, "admin"))
                conn.commit()
                print("[INIT] Admin padrao criado (banco estava vazio)")

        # Importar config.json → SQLite (só se tabela vazia)
        count = conn.execute("SELECT COUNT(*) FROM config").fetchone()[0]
        if count == 0:
            import json as _json
            try:
                with open(os.path.join(APP_DIR, "config.json")) as f:
                    cdata = _json.load(f)
                for k, v in cdata.items():
                    conn.execute("INSERT OR REPLACE INTO config (chave,valor) VALUES (?,?)", (k, str(v) if not isinstance(v, str) else v))
                conn.commit()
            except: pass
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════
#  AUDITORIA
# ══════════════════════════════════════════════════════════

def auditar(acao, contato_id=None, lote="", intervalo="", nome="", telefone="",
            status_de="", status_para="", detalhes="", usuario=None):
    """Registra uma ação de auditoria no banco."""
    try:
        if usuario is None:
            try:
                from flask import session as _s
                usuario = _s.get("usuario", "sistema")
            except: usuario = "sistema"
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        with get_db() as conn:
            conn.execute(
                "INSERT INTO auditoria (acao,contato_id,usuario,lote,intervalo,nome,telefone,status_de,status_para,detalhes,criado_em) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (acao, contato_id, usuario, lote, intervalo, nome, telefone, status_de, status_para, detalhes, now)
            )
    except Exception as e:
        print(f"[AUDITORIA ERRO] {e}")

# ══════════════════════════════════════════════════════════
#  LOG
# ══════════════════════════════════════════════════════════

def log(msg, tipo="info"):
    entrada = {"hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "msg": msg, "tipo": tipo}
    print(f"[{entrada['hora']}] {msg}")
    with _log_lock:
        estado["log"].insert(0, entrada)
        estado["log"] = estado["log"][:200]
    try:
        with get_db() as conn:
            conn.execute("INSERT INTO log_atividades (hora,msg,tipo) VALUES (?,?,?)",
                (entrada["hora"], msg, tipo))
            # manter apenas últimos 500
            conn.execute("DELETE FROM log_atividades WHERE id NOT IN (SELECT id FROM log_atividades ORDER BY id DESC LIMIT 500)")
    except: pass

# ══════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════

TWILIO_SID = TWILIO_TOKEN = TWILIO_NUMERO = ""
NOME_EVENTO = NOME_ORGANIZADOR = CHAVE_PIX = DATA_SORTEIO = ""
MODO_ENVIO = "manual"; HORARIO_ENVIO = "09:00"; DIA_SEMANA = "monday"
DIA_MES = INTERVALO_MIN = INTERVALO_MAX = LIMITE_SESSAO = 0
USAR_TEMPLATE = False; TEMPLATE_SID = URL_PUBLICA = ""
AGENDAMENTO_ATIVO = False

def carregar_config():
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT chave, valor FROM config").fetchall()
            cfg = {}
            for r in rows:
                v = r[1]
                # Tentar deserializar listas/dicts salvos como JSON
                if v and v.startswith('[') or (v and v.startswith('{')):
                    try: v = json.loads(v)
                    except: pass
                cfg[r[0]] = v
        # fallback para arquivo se db vazio
        if not cfg:
            try:
                with open(CONFIG_PATH) as f: cfg = json.load(f)
            except: pass
        return cfg
    except:
        try:
            with open(CONFIG_PATH) as f: return json.load(f)
        except: return {}

def salvar_config(cfg):
    try:
        with get_db() as conn:
            for k, v in cfg.items():
                if isinstance(v, (list, dict)):
                    val = json.dumps(v, ensure_ascii=False)
                else:
                    val = str(v) if not isinstance(v, str) else v
                conn.execute("INSERT OR REPLACE INTO config (chave,valor) VALUES (?,?)", (k, val))
    except: pass
    # mantém arquivo como backup
    try:
        with open(CONFIG_PATH,"w") as f: json.dump(cfg, f, indent=2, ensure_ascii=False)
    except: pass

def aplicar_config():
    global TWILIO_SID, TWILIO_TOKEN, TWILIO_NUMERO
    global NOME_EVENTO, NOME_ORGANIZADOR, CHAVE_PIX, DATA_SORTEIO
    global MODO_ENVIO, HORARIO_ENVIO, DIA_SEMANA, DIA_MES
    global INTERVALO_MIN, INTERVALO_MAX, LIMITE_SESSAO
    global USAR_TEMPLATE, TEMPLATE_SID, URL_PUBLICA
    cfg = carregar_config()
    TWILIO_SID      = cfg.get("twilio_sid","")
    TWILIO_TOKEN    = cfg.get("twilio_token","")
    TWILIO_NUMERO   = cfg.get("twilio_numero","")
    NOME_EVENTO     = cfg.get("nome_evento","Bingo Beneficente")
    NOME_ORGANIZADOR= cfg.get("nome_organizador","Organizador")
    CHAVE_PIX       = cfg.get("chave_pix","")
    DATA_SORTEIO    = cfg.get("data_sorteio","")
    MODO_ENVIO      = cfg.get("modo_envio","manual")
    HORARIO_ENVIO   = cfg.get("horario_envio","09:00")
    DIA_SEMANA      = cfg.get("dia_semana","monday")
    DIA_MES         = int(cfg.get("dia_mes",1) or 1)
    INTERVALO_MIN   = int(cfg.get("intervalo_min",20) or 20)
    INTERVALO_MAX   = int(cfg.get("intervalo_max",45) or 45)
    LIMITE_SESSAO   = int(cfg.get("limite_sessao",0) or 0)
    USAR_TEMPLATE   = bool(cfg.get("usar_content_template", False))
    TEMPLATE_SID    = cfg.get("content_template_sid","")
    URL_PUBLICA     = cfg.get("url_publica","")
    global AGENDAMENTO_ATIVO
    AGENDAMENTO_ATIVO = bool(cfg.get("agendamento_ativo", False))
    # Aplica timeout de sessão dinamicamente
    horas = int(cfg.get("session_timeout_horas", 8) or 8)
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=horas)

aplicar_config()

# ══════════════════════════════════════════════════════════
#  USUARIOS / AUTH
# ══════════════════════════════════════════════════════════

def carregar_usuarios():
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT username,nome,senha,perfil,permissoes,trocar_senha,ativo,telefone FROM usuarios").fetchall()
            if rows:
                return {r[0]: {"nome": r[1], "senha": r[2], "perfil": r[3], "permissoes": json.loads(r[4] or "[]"), "trocar_senha": bool(r[5]), "ativo": r[6] != 0, "telefone": r[7] or ""} for r in rows}
    except Exception as e:
        print(f"[USUARIOS ERRO carregar] {e}")
    # fallback
    try:
        admin_senha = hashlib.sha256(b"admin123").hexdigest()
        with get_db() as conn:
            conn.execute("INSERT OR IGNORE INTO usuarios (username,nome,senha,perfil,permissoes,trocar_senha,ativo) VALUES (?,?,?,?,?,?,?)",
                ("admin","Administrador", admin_senha, "admin", "[]", 0, 1))
        return {"admin": {"nome": "Administrador", "senha": admin_senha, "perfil": "admin", "permissoes": [], "trocar_senha": False, "ativo": True, "telefone": ""}}
    except Exception as e:
        print(f"[USUARIOS ERRO fallback] {e}")
        admin_senha = hashlib.sha256(b"admin123").hexdigest()
        return {"admin": {"nome": "Administrador", "senha": admin_senha, "perfil": "admin", "permissoes": [], "trocar_senha": False, "ativo": True, "telefone": ""}}

def salvar_usuarios(usuarios):
    try:
        with get_db() as conn:
            usernames = list(usuarios.keys())
            if not usernames:
                print("[USUARIOS AVISO] salvar_usuarios chamado com dict vazio — ignorado")
                return
            placeholders = ",".join("?" * len(usernames))
            conn.execute(f"DELETE FROM usuarios WHERE username NOT IN ({placeholders})", usernames)
            for uname, ud in usuarios.items():
                conn.execute("""INSERT OR REPLACE INTO usuarios (username,nome,senha,perfil,permissoes,trocar_senha,ativo,telefone)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (uname, ud.get("nome",""), ud.get("senha",""), ud.get("perfil","vendedor"),
                     json.dumps(ud.get("permissoes",[])),
                     1 if ud.get("trocar_senha") else 0,
                     0 if ud.get("ativo") == False else 1,
                     sanitizar_telefone(ud.get("telefone","") or "")))
            conn.commit()
    except Exception as e:
        print(f"[USUARIOS ERRO salvar] {e}")

def _hash(s): return hashlib.sha256(s.encode()).hexdigest()

def requer_login(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logado"):
            if request.path.startswith("/api/") or request.path.startswith("/webhook"):
                return jsonify({"ok":False,"erro":"Não autenticado"}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

# ══════════════════════════════════════════════════════════
#  INBOX
# ══════════════════════════════════════════════════════════

def carregar_inbox():
    try:
        with open(INBOX_PATH) as f: return json.load(f)
    except: return {}

def salvar_inbox(inbox):
    with _inbox_lock:
        with open(INBOX_PATH,"w") as f: json.dump(inbox, f, indent=2, ensure_ascii=False)

def numero_limpo(numero):
    n = re.sub(r'\D','', str(numero))
    if n.startswith("55") and len(n) > 11: n = n[2:]
    return n

def sanitizar_telefone(tel):
    """Remove não-dígitos, DDI 55 e zero inicial. Limita a 11 dígitos (DDD + número)."""
    n = re.sub(r'\D', '', str(tel or ""))
    # Remove DDI 55 se presente e resultado ficaria > 11
    if n.startswith("55") and len(n) > 11:
        n = n[2:]
    # Remove zero inicial (ex: 063...)
    if n.startswith("0"):
        n = n[1:]
    return n[:11]

def numeros_equivalentes(n1, n2):
    a, b = numero_limpo(n1), numero_limpo(n2)
    if a == b: return True
    def var(n):
        vs = {n}
        if len(n)==10 and n[2]!='9': vs.add(n[:2]+'9'+n[2:])
        if len(n)==11 and n[2]=='9': vs.add(n[:2]+n[3:])
        return vs
    return bool(var(a) & var(b))

# ══════════════════════════════════════════════════════════
#  PIX
# ══════════════════════════════════════════════════════════

def _tlv(tag, value): return "{}{:02d}{}".format(tag, len(value), value)
def _ascii(texto):
    nfkd = unicodedata.normalize('NFKD', str(texto))
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).upper()

def gerar_payload_pix(chave, nome, cidade, valor=None):
    nome_l=_ascii(nome)[:25]; cidade_l=_ascii(cidade)[:15]
    mai = _tlv("26", _tlv("00","BR.GOV.BCB.PIX")+_tlv("01",chave.strip()))
    valor_str=""
    if valor:
        try: valor_str="{:.2f}".format(float(str(valor).replace("R$","").replace(",",".").strip()))
        except: pass
    payload = (_tlv("00","01")+mai+_tlv("52","0000")+_tlv("53","986")+
               (_tlv("54",valor_str) if valor_str else "")+
               _tlv("58","BR")+_tlv("59",nome_l)+_tlv("60",cidade_l)+_tlv("62",_tlv("05","***")))
    crc_data = payload+"6304"; crc=0xFFFF
    for byte in crc_data.encode('utf-8'):
        crc ^= byte<<8
        for _ in range(8): crc = (crc<<1)^0x1021 if crc&0x8000 else crc<<1
    return payload+_tlv("63", format(crc&0xFFFF,"04X"))

# ══════════════════════════════════════════════════════════
#  TWILIO / ENVIO
# ══════════════════════════════════════════════════════════

# Status que disparam envio
STATUS_DISPARA = ["pendente"]
STATUS_PAGOS      = ["pago", "quitado", "confirmado", "ok", "sim", "s"]
STATUS_BLOQUEADOS = ["pago", "quitado", "confirmado", "ok", "sim", "s", "desmembrado", "pgto. parcial"]

def deve_disparar(status, previsao_pagamento=""):
    """Dispara se status=Pendente E (previsão vazia OU previsão <= hoje)."""
    if str(status).strip().lower() not in STATUS_DISPARA:
        return False
    prev = str(previsao_pagamento or "").strip()
    if not prev:
        return True  # Sem previsão → dispara
    try:
        data_prev = datetime.strptime(prev, "%Y-%m-%d").date()
        return data_prev < datetime.now().date()  # Só vencida (antes de hoje) → dispara
    except:
        return True  # Data inválida → dispara por segurança

def deve_disparar_v2(c, ultimo_disparo_data=None, hoje=None):
    """
    Regra de disparo:
    1. Status = Pendente (obrigatório)
    2. Se tem previsão de pagamento e ainda não venceu → NÃO dispara
    3. Se tem previsão e venceu → só dispara no dia seguinte ao vencimento (dias_prev >= 1)
       Após esse primeiro disparo, conta N dias a partir dele
    4. Se não tem previsão → conta N dias a partir do cadastro ou último disparo
    5. Se N=0 → dispara todos os pendentes sem restrição de dias
    Retorna (bool, motivo)
    """
    from datetime import datetime as _dt

    if hoje is None:
        hoje = _dt.now().date()

    status = (c.get("status") or "").strip().lower()
    if status != "pendente":
        return False, f"Status: {c.get('status','')}"

    cfg = carregar_config()
    n   = int(cfg.get("dias_disparo", 0) or 0)

    def _parse(s):
        s = (s or "").strip()
        if not s: return None
        try:
            if len(s) >= 10 and s[2] == '/':
                return _dt.strptime(s[:10], "%d/%m/%Y").date()
            return _dt.strptime(s[:10], "%Y-%m-%d").date()
        except:
            try: return _dt.strptime(s[:16], "%Y-%m-%d %H:%M").date()
            except: return None

    d_prev   = _parse(c.get("previsao_pagamento"))
    d_ultimo = _parse(ultimo_disparo_data)

    # Barreira: previsão existe e ainda não venceu (inclui o próprio dia)
    if d_prev:
        dias_prev = (hoje - d_prev).days
        if dias_prev <= 0:
            return False, f"Prev. pgto não venceu ({'hoje' if dias_prev == 0 else f'vence em {abs(dias_prev)}d'})"

        # Previsão venceu — verificar se já houve disparo APÓS o vencimento
        if d_ultimo and d_ultimo >= d_prev:
            # Já disparou após o vencimento — aplicar regra de N dias a partir do último disparo
            if n == 0:
                return True, "N=0 — sem restrição de dias"
            dias_ultimo = (hoje - d_ultimo).days
            if dias_ultimo > n:
                return True, f"Último disparo há {dias_ultimo}d > N={n}"
            else:
                return False, f"Último disparo há {dias_ultimo}d ≤ N={n}"
        else:
            # Ainda não disparou após o vencimento → dispara agora (dia seguinte ao vencimento)
            return True, f"Primeiro disparo após vencimento da prev. pgto ({d_prev.strftime('%d/%m/%Y')})"

    # Sem previsão de pagamento
    if n == 0:
        return True, "N=0 — sem restrição de dias"

    d_cadastro = _parse(c.get("criado_em"))

    # Referência: último disparo se existir, senão cadastro
    if d_ultimo:
        dias_ref = (hoje - d_ultimo).days
        ref_campo = "último disparo"
    elif d_cadastro:
        dias_ref = (hoje - d_cadastro).days
        ref_campo = "cadastro"
    else:
        return False, "Sem datas válidas"

    if dias_ref > n:
        return True, f"Ref: {ref_campo} ({dias_ref}d) > N={n}"
    else:
        return False, f"Ref: {ref_campo} ({dias_ref}d) ≤ N={n}"

def ids_enviados_hoje():
    """Retorna set de contato_ids que já receberam envio BEM SUCEDIDO hoje."""
    hoje_br  = datetime.now().strftime("%d/%m/%Y")   # "14/03/2026"
    hoje_iso = datetime.now().strftime("%Y-%m-%d")   # "2026-03-14"
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT DISTINCT contato_id FROM log_envios "
                "WHERE status='ENVIADO' AND (criado_em LIKE ? OR criado_em LIKE ?)",
                (hoje_br + "%", hoje_iso + "%")
            ).fetchall()
        return {r[0] for r in rows if r[0]}
    except:
        return set()

def formatar_telefone(tel):
    n = re.sub(r'\D','',str(tel))
    if n.startswith("0"): n = n[1:]
    if not n.startswith("55"): n = "55" + n
    return n

def formatar_valor(valor):
    try:
        v = float(str(valor).replace("R$","").replace(".","").replace(",",".").strip())
        return "R$ {:,.2f}".format(v).replace(",","X").replace(".",",").replace("X",".")
    except:
        return str(valor)

def saudacao_por_horario():
    h = datetime.now().hour
    return "Bom dia" if h < 12 else ("Boa tarde" if h < 18 else "Boa noite")

def get_base_url():
    """URL pública configurada ou host_url como fallback."""
    if URL_PUBLICA:
        return URL_PUBLICA.rstrip("/") + "/"
    try:
        return request.host_url
    except:
        return ""

def get_midias_template(nome_template):
    """Retorna lista de URLs relativas: [banner, rodape/qrcode]."""
    tpl = carregar_template(nome_template)
    midias = []
    banner = tpl.get("imagem","")
    if banner and os.path.exists(os.path.join(PASTA_IMAGENS, banner)):
        midias.append("static/imagens/" + banner)
    rodape = tpl.get("rodape","") or tpl.get("qrcode","")
    if rodape:
        if os.path.exists(os.path.join(PASTA_IMAGENS, rodape)):
            midias.append("static/imagens/" + rodape)
        elif os.path.exists(os.path.join(PASTA_QRCODES, rodape)):
            midias.append("static/qrcodes/" + rodape)
    return midias

def montar_mensagem(c, cfg):
    """Monta mensagem do template ativo com variáveis do contato."""
    tpl = carregar_template(TEMPLATE_ATIVO)
    texto = tpl.get("texto", "Olá {nome}! Lote {lote} | Cartelas {intervalo} | {valor}")
    try:
        return texto.format(
            nome         = c.get("nome",""),
            lote         = c.get("lote",""),
            intervalo    = c.get("intervalo",""),
            vendedor     = c.get("vendedor",""),
            valor        = formatar_valor(c.get("valor","")),
            chave_pix    = cfg.get("chave_pix",""),
            beneficiario = cfg.get("nome_organizador",""),
            data_sorteio = cfg.get("data_sorteio",""),
            evento       = cfg.get("nome_evento",""),
            saudacao     = saudacao_por_horario(),
        )
    except KeyError as e:
        log(f"Variável desconhecida no template: {e}","warning")
        return texto
    except Exception as e:
        log(f"Erro ao montar mensagem: {e}","warning")
        return texto

def salvar_relatorio(resultados, total, enviados, ignorados, erros, modo_teste=False):
    os.makedirs(PASTA_RELAT, exist_ok=True)
    nome = os.path.join(PASTA_RELAT, "relatorio_"+datetime.now().strftime("%Y%m%d_%H%M%S")+".json")
    # Estatísticas extras
    enviados_list  = [r for r in resultados if r.get("status") == "ENVIADO"]
    erros_list     = [r for r in resultados if r.get("status") == "ERRO"]
    ignorados_list = [r for r in resultados if str(r.get("status","")).startswith("IGNORADO")]
    teste_list     = [r for r in resultados if r.get("status") == "TESTE"]
    dados = {
        "data":      datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "evento":    NOME_EVENTO,
        "modo":      "TESTE" if modo_teste else "REAL",
        "resumo":    {
            "total":     total,
            "enviados":  enviados,
            "ignorados": ignorados,
            "erros":     erros,
        },
        "enviados":  enviados_list,
        "erros":     erros_list,
        "ignorados": ignorados_list,
        "teste":     teste_list,
        "detalhes":  resultados,
    }
    with open(nome,"w",encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    log("Relatório salvo: "+os.path.basename(nome),"success")

def _enviar_twilio(client, num_from, c, cfg):
    """Envia mensagem via Twilio para um contato. Retorna (ok, sid_ou_erro)."""
    tel = formatar_telefone(c.get("telefone","") or c.get("whatsapp",""))
    if len(tel) < 12:
        return False, "Telefone inválido: "+tel

    usar_tpl = cfg.get("usar_content_template", False)
    tpl_sid  = cfg.get("content_template_sid","")
    params   = {"from_": num_from, "to": "whatsapp:+"+tel}

    if usar_tpl and tpl_sid:
        params["content_sid"] = tpl_sid
        params["content_variables"] = json.dumps({
            "1": c.get("nome",""),
            "2": str(c.get("lote","")),
            "3": str(c.get("intervalo","")),
            "4": formatar_valor(c.get("valor","")),
            "5": cfg.get("chave_pix",""),
            "6": cfg.get("nome_organizador",""),
            "7": cfg.get("data_sorteio",""),
            "8": str(c.get("vendedor","")),
            "9": cfg.get("nome_evento",""),
        })
    else:
        params["body"] = montar_mensagem(c, cfg)
        midias = get_midias_template(TEMPLATE_ATIVO)
        if midias:
            base = get_base_url()
            params["media_url"] = [base + m for m in midias]

    msg = client.messages.create(**params)
    return True, msg.sid

def executar_envio_thread(ids=None, limite=0, modo_teste=False, data_base=None, usuario_disparo="sistema"):
    if estado["enviando"]:
        log("Já existe um envio em andamento!","error")
        return

    estado.update({"enviando":True,"progresso":0,"enviados":0,"ignorados":0,"erros":0,"total":0,"previsao_envio":0})
    cfg = carregar_config()
    lim = limite or int(cfg.get("limite_sessao",0) or 0)
    resultados = []

    # Data base para cálculo dos dias (padrão: hoje)
    try:
        hoje = datetime.strptime(data_base, "%Y-%m-%d").date() if data_base else datetime.now().date()
    except:
        hoje = datetime.now().date()
    if data_base and hoje != datetime.now().date():
        log(f"Data base: {hoje.strftime('%d/%m/%Y')} (simulação)","info")

    try:
        if modo_teste:
            log("🔬 ══════════════════════════════════════","info")
            log("🔬 MODO TESTE — nenhuma mensagem será enviada","warning")
            log("🔬 ══════════════════════════════════════","info")

        # Carrega contatos a processar
        with get_db() as conn:
            if ids:
                ph = ','.join('?'*len(ids))
                rows = conn.execute(f"SELECT * FROM contatos WHERE id IN ({ph})", ids).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM contatos WHERE LOWER(TRIM(status)) = 'pendente'"
                    " ORDER BY CAST(lote AS INTEGER), id"
                ).fetchall()
        contatos = [dict(r) for r in rows]
        estado["total"] = len(contatos)
        log(f"{len(contatos)} contato(s) carregado(s).","info")

        # Carrega IDs já enviados hoje (uma única query antes do loop)
        ja_enviados_hoje = ids_enviados_hoje()
        if ja_enviados_hoje:
            log(f"{len(ja_enviados_hoje)} contato(s) já enviado(s) hoje — serão ignorados.","info")

        # Busca data do último disparo por contato
        with get_db() as conn:
            envios = conn.execute(
                "SELECT contato_id, criado_em as ultimo FROM log_envios "
                "WHERE status='ENVIADO' AND id IN ("
                "  SELECT MAX(id) FROM log_envios WHERE status='ENVIADO' GROUP BY contato_id"
                ")"
            ).fetchall()
        ultimo_envio_data = {r["contato_id"]: r["ultimo"] for r in envios}


        # Calcula quantos serão disparados de fato (denominador do progresso)
        _previsao_envio = sum(
            1 for c in contatos
            if c.get("nome") and c.get("telefone")
            and deve_disparar_v2(c, ultimo_envio_data.get(c["id"]), hoje)[0]
            and c.get("id") not in ja_enviados_hoje
        )
        if lim > 0:
            _previsao_envio = min(_previsao_envio, lim)
        estado["previsao_envio"] = _previsao_envio
        log(f"{_previsao_envio} contato(s) a disparar nesta sessão.","info")

        # Conecta Twilio UMA vez antes do loop (igual v1)
        twilio_client = None
        if not modo_teste:
            sid   = cfg.get("twilio_sid","")
            token = cfg.get("twilio_token","")
            num   = cfg.get("twilio_numero","")
            if not all([sid,token,num]):
                log("Twilio não configurado! Verifique as configurações.","error")
                return
            if not TWILIO_OK:
                log("Biblioteca Twilio não instalada!","error")
                return
            log("Conectando ao Twilio...","info")
            twilio_client = TwilioClient(sid, token)
            twilio_client.api.accounts(sid).fetch()  # valida credenciais
            log("Twilio conectado!","success")

        for i, c in enumerate(contatos):
            if not estado["enviando"]: break
            if lim > 0 and estado["enviados"] >= lim:
                log(f"Limite de {lim} atingido.","warning"); break

            # Progresso baseado em enviados sobre previsão real de envio
            _den = max(estado.get("previsao_envio", 1), 1)
            estado["progresso"] = min(int((estado["enviados"] / _den) * 100), 100)

            if not c.get("nome") or not c.get("telefone"): continue

            # Nova regra de disparo v2
            ok_disparar, motivo_v2 = deve_disparar_v2(c, ultimo_envio_data.get(c["id"]), hoje)
            if not ok_disparar:
                estado["ignorados"] += 1
                resultados.append({"nome":c["nome"],"lote":c.get("lote",""),"intervalo":c.get("intervalo",""),"telefone":c.get("telefone",""),"status":"IGNORADO","motivo":motivo_v2})
                continue

            # Bloqueia se já recebeu envio bem sucedido hoje
            if c.get("id") in ja_enviados_hoje:
                estado["ignorados"] += 1
                resultados.append({"nome":c["nome"],"lote":c.get("lote",""),"intervalo":c.get("intervalo",""),"telefone":c.get("telefone",""),"status":"IGNORADO","motivo":"Já enviado hoje"})
                continue

            if modo_teste:
                tel = formatar_telefone(c.get("telefone",""))
                log(f"[TESTE] {c['nome']} | +{tel} | lote {c.get('lote','')}","info")
                resultados.append({"nome":c["nome"],"lote":c.get("lote",""),"intervalo":c.get("intervalo",""),"telefone":c.get("telefone",""),"status":"TESTE"})
                estado["enviados"] += 1
                time.sleep(0.1)  # pequeno delay para o polling conseguir capturar o log
                continue

            # Envio real
            try:
                now = datetime.now().strftime("%d/%m/%Y %H:%M")
                ok, sid_ou_erro = _enviar_twilio(twilio_client, cfg.get("twilio_numero",""), c, cfg)
                if ok:
                    with get_db() as conn:
                        conn.execute("UPDATE contatos SET ultimo_wa=?,atualizado_em=? WHERE id=?", (now,now,c["id"]))
                        conn.execute(
                            "INSERT INTO log_envios (contato_id,nome,telefone,status,mensagem,criado_em) VALUES (?,?,?,?,?,?)",
                            (c["id"],c["nome"],c.get("telefone",""),"ENVIADO",sid_ou_erro,now)
                        )
                    log(f"✅ Enviado: {c['nome']} | SID: {sid_ou_erro}","success")
                    resultados.append({"nome":c["nome"],"lote":c.get("lote",""),"intervalo":c.get("intervalo",""),"telefone":c.get("telefone",""),"status":"ENVIADO","sid":sid_ou_erro,"hora":now})
                    estado["enviados"] += 1
                    espera = random.randint(INTERVALO_MIN or 20, INTERVALO_MAX or 45)
                    time.sleep(espera)
                else:
                    with get_db() as conn:
                        conn.execute(
                            "INSERT INTO log_envios (contato_id,nome,telefone,status,mensagem,criado_em) VALUES (?,?,?,?,?,?)",
                            (c["id"],c["nome"],c.get("telefone",""),"ERRO",sid_ou_erro,now)
                        )
                    log(f"❌ Erro {c['nome']}: {sid_ou_erro}","error")
                    resultados.append({"nome":c["nome"],"lote":c.get("lote",""),"intervalo":c.get("intervalo",""),"telefone":c.get("telefone",""),"status":"ERRO","erro":sid_ou_erro,"hora":now})
                    estado["erros"] += 1
            except Exception as e:
                log(f"❌ ERRO {c['nome']}: {e}","error")
                resultados.append({"nome":c["nome"],"lote":c.get("lote",""),"intervalo":c.get("intervalo",""),"telefone":c.get("telefone",""),"status":"ERRO","erro":str(e),"hora":now})
                estado["erros"] += 1

        estado["progresso"] = 100
        if estado["enviados"] == 0 and estado["ignorados"] == 0 and estado["erros"] == 0:
            log("⚠️ Nenhum registro a enviar no momento. Verifique os filtros e a configuração de Dias Para Disparo.","warning")
        log(f"Envio concluído! ✅ Enviados: {estado['enviados']} | ⏭ Ignorados: {estado['ignorados']} | ❌ Erros: {estado['erros']}","success")
        if modo_teste:
            log("🔬 ══════════════════════════════════════","info")
            log("🔬 FIM DO TESTE — nenhuma mensagem foi enviada","warning")
            log("🔬 ══════════════════════════════════════","info")
        salvar_relatorio(resultados, estado["total"], estado["enviados"], estado["ignorados"], estado["erros"], modo_teste=modo_teste)

        # Registra resultado na auditoria
        tipo_disparo = "TESTE" if modo_teste else "REAL"
        eh_agendado  = not ids  # sem ids = agendamento automático
        acao_audit   = "AGENDAMENTO_RESULTADO" if eh_agendado else "DISPARO_MANUAL_RESULTADO"
        auditar(acao_audit, usuario="sistema" if eh_agendado else usuario_disparo,
                detalhes=f"Tipo: {tipo_disparo} | ✅ Enviados: {estado['enviados']} | ⏭ Ignorados: {estado['ignorados']} | ❌ Erros: {estado['erros']} | Total processado: {estado['total']}")

        # Salva resultado do último disparo no config para exibir na tela de agendamento
        try:
            now_result   = datetime.now().strftime("%d/%m/%Y %H:%M")
            resultado_str = f"✅ {estado['enviados']} enviados | ⏭ {estado['ignorados']} ignorados | ❌ {estado['erros']} erros"
            with get_db() as conn:
                # Sempre salva o último disparo (seja manual ou agendado)
                conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_disparo_resultado", resultado_str))
                conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_disparo_hora", now_result))
                conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_disparo_tipo", tipo_disparo))
                conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_disparo_origem", "agendado" if eh_agendado else "manual"))
                # Salva separado por origem para mostrar os dois no histórico
                if eh_agendado:
                    conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_agendado_hora", now_result))
                    conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_agendado_resultado", resultado_str))
                else:
                    conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_manual_hora", now_result))
                    conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_manual_resultado", resultado_str))
                    conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_manual_usuario", usuario_disparo))
            log(f"📋 Histórico atualizado ({'agendado' if eh_agendado else 'manual'}): {resultado_str}", "info")
        except Exception as e:
            log(f"⚠️ Erro ao salvar histórico de disparo: {e}", "warning")

        # Notifica todos os administradores com telefone cadastrado
        try:
            admins = [u for u in carregar_usuarios().values()
                      if u.get("perfil") == "admin"
                      and u.get("ativo", True)
                      and sanitizar_telefone(u.get("telefone",""))]
            log(f"📱 Admins para notificar: {len(admins)} — {[u.get('nome','?')+'/'+u.get('telefone','sem tel') for u in admins]}","info")
            if admins:
                # Cria client Twilio se não existir (ex: modo teste não conecta antes)
                notif_client = twilio_client
                if not notif_client and TWILIO_OK:
                    sid   = cfg.get("twilio_sid","")
                    token = cfg.get("twilio_token","")
                    log(f"📱 Criando client Twilio para notificação — sid={'OK' if sid else 'VAZIO'} token={'OK' if token else 'VAZIO'}","info")
                    if sid and token:
                        notif_client = TwilioClient(sid, token)
                log(f"📱 notif_client: {'OK' if notif_client else 'NULO — não será enviado'}","info")
                if notif_client:
                    data_hora = datetime.now().strftime("%d/%m/%Y %H:%M")
                    if modo_teste:
                        msg_admin = (
                            f"🔬 *Teste de disparo — {data_hora}*\n\n"
                            f"Nenhuma mensagem foi enviada.\n"
                            f"📋 Registros processados: {estado['enviados']}\n"
                            f"⏭ Ignorados: {estado['ignorados']}\n\n"
                            f"📋 Evento: {cfg.get('nome_evento','')}"
                        )
                    elif estado["enviados"] > 0:
                        msg_admin = (
                            f"✅ *Disparo concluído — {data_hora}*\n\n"
                            f"📨 Enviados: {estado['enviados']}\n"
                            f"⏭ Ignorados: {estado['ignorados']}\n"
                            f"❌ Erros: {estado['erros']}\n\n"
                            f"📋 Evento: {cfg.get('nome_evento','')}"
                        )
                    else:
                        msg_admin = (
                            f"⚠️ *Disparo sem envios — {data_hora}*\n\n"
                            f"Nenhuma mensagem foi enviada.\n"
                            f"⏭ Ignorados: {estado['ignorados']}\n"
                            f"❌ Erros: {estado['erros']}\n\n"
                            f"📋 Evento: {cfg.get('nome_evento','')}"
                        )
                    for admin in admins:
                        try:
                            tel_fmt = formatar_telefone(sanitizar_telefone(admin["telefone"]))
                            num_from = cfg.get("twilio_numero","")
                            # Mesmo padrão do _enviar_twilio — from_ sem prefixo whatsapp:
                            notif_client.messages.create(
                                from_=num_from,
                                to=f"whatsapp:+{tel_fmt}",
                                body=msg_admin
                            )
                            log(f"📱 Admin notificado: {admin.get('nome','')} (+{tel_fmt}).","info")
                        except Exception as e:
                            log(f"⚠️ Falha ao notificar {admin.get('nome','')}: {e}","warning")
        except Exception as e:
            log(f"⚠️ Erro ao notificar administradores: {e}","warning")

    except Exception as e:
        log(f"ERRO CRÍTICO no disparo: {e}","error")
    finally:
        estado["enviando"] = False

# ══════════════════════════════════════════════════════════
#  SCHEDULER
# ══════════════════════════════════════════════════════════

def run_scheduler():
    DIAS={"monday":0,"tuesday":1,"wednesday":2,"thursday":3,"friday":4,"saturday":5,"sunday":6}
    while True:
        try:
            if AGENDAMENTO_ATIVO and MODO_ENVIO!="manual" and not estado["enviando"]:
                agora=datetime.now(); h,m=map(int,(HORARIO_ENVIO or "09:00").split(":"))
                no_horario=agora.hour==h and agora.minute==m and agora.second<30
                disparar=False
                if no_horario:
                    if MODO_ENVIO=="diario": disparar=True
                    elif MODO_ENVIO=="semanal": disparar=agora.weekday()==DIAS.get(DIA_SEMANA,0)
                    elif MODO_ENVIO=="quinzenal": disparar=agora.day in(1,16)
                    elif MODO_ENVIO=="mensal": disparar=agora.day==DIA_MES
                if disparar:
                    now_str = agora.strftime("%d/%m/%Y %H:%M")
                    log(f"⏰ Agendamento automático disparado — {now_str}","info")
                    # Salva último disparo no config
                    try:
                        cfg = carregar_config()
                        cfg["ultimo_agendamento"] = now_str
                        cfg["ultimo_agendamento_modo"] = MODO_ENVIO
                        with get_db() as conn:
                            conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_agendamento", now_str))
                            conn.execute("INSERT OR REPLACE INTO config(chave,valor) VALUES(?,?)", ("ultimo_agendamento_modo", MODO_ENVIO))
                    except Exception as e:
                        log(f"Aviso: não foi possível salvar último agendamento: {e}", "warning")
                    # Registra na auditoria
                    auditar("AGENDAMENTO_DISPARO", usuario="sistema",
                            detalhes=f"Disparo automático iniciado — Modo: {MODO_ENVIO} | Horário: {HORARIO_ENVIO} | Data: {now_str}")
                    threading.Thread(target=executar_envio_thread, kwargs={"usuario_disparo":"sistema"}, daemon=True).start()
                    time.sleep(60)
        except Exception as e: log(f"Erro scheduler: {e}","error")
        time.sleep(15)

# ══════════════════════════════════════════════════════════
#  ROTAS AUTH
# ══════════════════════════════════════════════════════════

@app.route("/login", methods=["GET","POST"])
def login():
    erro=""
    if request.method=="POST":
        u=request.form.get("usuario","").strip().lower(); s=request.form.get("senha","")
        usuarios=carregar_usuarios(); usr=usuarios.get(u)
        if usr and usr["senha"]==_hash(s):
            if not usr.get("ativo", True):
                erro="Usuário desabilitado. Contate o administrador."
            else:
                session.permanent = True  # aplica o timeout configurado
                session["logado"]=True; session["usuario"]=u
                session["perfil"]=usr.get("perfil","admin"); session["nome"]=usr.get("nome",u)
                session["permissoes"]=usr.get("permissoes",[])
                session["trocar_senha"]=bool(usr.get("trocar_senha"))
                try:
                    auditar("LOGIN", usuario=u, detalhes=f"Nome: {usr.get('nome', u)}")
                except Exception as e:
                    print(f"[LOGIN AUDITORIA ERRO] {e}")
                return redirect("/")
        erro="Usuário ou senha incorretos."
    return render_template("login.html", erro=erro)

@app.route("/logout")
@app.route("/sair")
def logout():
    try:
        usuario = session.get("usuario", "")
        nome    = session.get("nome", usuario)
        if usuario:
            auditar("LOGOUT", usuario=usuario, detalhes=f"Nome: {nome}")
    except Exception as e:
        print(f"[LOGOUT AUDITORIA ERRO] {e}")
    session.clear()
    return redirect("/login")

# ══════════════════════════════════════════════════════════
#  ROTAS PÁGINAS
# ══════════════════════════════════════════════════════════

@app.route("/")
@requer_login
def index(): return render_template("index.html")

@app.route("/cadastro")
@requer_login
def cadastro(): return render_template("cadastro.html")

# ══════════════════════════════════════════════════════════
#  API CONTATOS CRUD
# ══════════════════════════════════════════════════════════

@app.route("/api/contatos")
@requer_login
def api_contatos_listar():
    busca=request.args.get("q","").strip(); status=request.args.get("status",""); lote=request.args.get("lote","")
    page=int(request.args.get("page",1)); per=int(request.args.get("per",50))
    sql="SELECT * FROM contatos WHERE 1=1"; params=[]
    if busca: sql+=" AND (nome LIKE ? OR telefone LIKE ? OR lote LIKE ? OR vendedor LIKE ?)"; b=f"%{busca}%"; params+=[b,b,b,b]
    if status: sql+=" AND status=?"; params.append(status)
    if lote:   sql+=" AND lote=?";   params.append(lote)
    with get_db() as conn:
        total=conn.execute("SELECT COUNT(*) FROM ("+sql+")", params).fetchone()[0]
        rows=conn.execute(sql+f" ORDER BY CAST(lote AS INTEGER),id LIMIT {per} OFFSET {(page-1)*per}", params).fetchall()
    return jsonify({"ok":True,"total":total,"page":page,"per":per,"contatos":[dict(r) for r in rows]})

@app.route("/api/contatos/disponiveis")
@requer_login
def api_contatos_disponiveis():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, lote, intervalo, valor, observacoes FROM contatos WHERE status='Disponivel' ORDER BY lote, intervalo"
        ).fetchall()
    return jsonify({"ok": True, "contatos": [dict(r) for r in rows]})

@app.route("/api/contatos/<int:cid>")
@requer_login
def api_contato_get(cid):
    with get_db() as conn: row=conn.execute("SELECT * FROM contatos WHERE id=?", (cid,)).fetchone()
    if not row: return jsonify({"ok":False,"erro":"Não encontrado"}), 404
    return jsonify({"ok":True,"contato":dict(row)})

@app.route("/api/contatos", methods=["POST"])
@requer_login
def api_contato_criar():
    d=request.get_json() or {}
    lote=(d.get("lote","") or "").strip(); intervalo=(d.get("intervalo","") or "").strip()
    if not lote or not intervalo: return jsonify({"ok":False,"erro":"Lote e intervalo de cartelas são obrigatórios"})
    nome=(d.get("nome","") or "").strip().upper(); tel=sanitizar_telefone(d.get("telefone",""))
    status="Disponivel" if not nome else "Pendente"
    obs=(d.get("observacoes","") or "").strip()
    prev=(d.get("previsao_pagamento","") or "").strip()
    data_pgto=(d.get("data_pagamento","") or "").strip()
    forma_pgto=(d.get("forma_pagamento","") or "").strip()
    now=datetime.now().strftime("%d/%m/%Y %H:%M")
    try:
        with get_db() as conn:
            cur=conn.execute("INSERT INTO contatos (lote,intervalo,vendedor,nome,telefone,whatsapp,valor,status,observacoes,previsao_pagamento,data_pagamento,forma_pagamento,criado_em,atualizado_em) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (lote,intervalo,(d.get("vendedor","") or "").strip().upper(),nome,tel,tel,d.get("valor",""),status,obs,prev,data_pgto,forma_pgto,now,now))
        novo_id = cur.lastrowid
    except Exception as e:
        return jsonify({"ok":False,"erro":f"Lote {lote} / Cartela {intervalo} já existe"})
    auditar("CRIACAO", contato_id=novo_id, lote=lote, intervalo=intervalo,
            nome=nome, telefone=tel, status_para=status,
            detalhes=f"Vendedor: {d.get('vendedor','')} | Valor: {d.get('valor','')} | Obs: {obs}")
    log(f"Novo contato: {nome or '(sem nome)'} | Lote {lote} | {intervalo}", "success")
    return jsonify({"ok":True,"id":novo_id,"msg":"Contato criado!"})

@app.route("/api/contatos/<int:cid>", methods=["PUT"])
@requer_login
def api_contato_atualizar(cid):
    d=request.get_json() or {}
    lote=(d.get("lote","") or "").strip(); intervalo=(d.get("intervalo","") or "").strip()
    if not lote or not intervalo: return jsonify({"ok":False,"erro":"Lote e intervalo de cartelas são obrigatórios"})
    nome=(d.get("nome","") or "").strip().upper(); tel=sanitizar_telefone(d.get("telefone",""))
    status=d.get("status","") or ("Disponivel" if not nome else "Pendente")
    obs=(d.get("observacoes","") or "").strip()
    prev=(d.get("previsao_pagamento","") or "").strip()
    data_pgto=(d.get("data_pagamento","") or "").strip()
    forma_pgto=(d.get("forma_pagamento","") or "").strip()
    now=datetime.now().strftime("%d/%m/%Y %H:%M")
    try:
        with get_db() as conn:
            # Captura estado anterior para auditoria
            anterior = conn.execute("SELECT * FROM contatos WHERE id=?", (cid,)).fetchone()
            status_de = dict(anterior).get("status","") if anterior else ""
            nome_de   = dict(anterior).get("nome","")   if anterior else ""
            conn.execute("UPDATE contatos SET lote=?,intervalo=?,vendedor=?,nome=?,telefone=?,whatsapp=?,valor=?,status=?,observacoes=?,previsao_pagamento=?,data_pagamento=?,forma_pagamento=?,atualizado_em=? WHERE id=?",
                (lote,intervalo,(d.get("vendedor","") or "").strip().upper(),nome,tel,d.get("whatsapp",""),d.get("valor",""),status,obs,prev,data_pgto,forma_pgto,now,cid))
    except Exception as e:
        return jsonify({"ok":False,"erro":f"Lote {lote} / Cartela {intervalo} já existe em outro registro"})
    # Detecta o que mudou
    mudancas = []
    if anterior:
        ant = dict(anterior)
        if ant.get("nome","") != nome:      mudancas.append(f"nome: '{ant.get('nome','')}' → '{nome}'")
        if ant.get("telefone","") != tel:   mudancas.append(f"tel: '{ant.get('telefone','')}' → '{tel}'")
        if ant.get("status","") != status:  mudancas.append(f"status: '{ant.get('status','')}' → '{status}'")
        if ant.get("valor","") != d.get("valor",""): mudancas.append(f"valor: '{ant.get('valor','')}' → '{d.get('valor','')}'")
        if ant.get("vendedor","") != d.get("vendedor",""): mudancas.append(f"vendedor: '{ant.get('vendedor','')}' → '{d.get('vendedor','')}'")
    auditar("EDICAO", contato_id=cid, lote=lote, intervalo=intervalo,
            nome=nome, telefone=tel, status_de=status_de, status_para=status,
            detalhes=" | ".join(mudancas) if mudancas else "Sem alterações de campos monitorados")
    log(f"Contato editado: {nome or nome_de} | Lote {lote}", "info")
    return jsonify({"ok":True,"msg":"Contato atualizado!"})

@app.route("/api/contatos/<int:cid>", methods=["DELETE"])
@requer_login
def api_contato_deletar(cid):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM contatos WHERE id=?", (cid,)).fetchone()
        c = dict(row) if row else {}
        conn.execute("DELETE FROM contatos WHERE id=?", (cid,))
    auditar("EXCLUSAO", contato_id=cid,
            lote=c.get("lote",""), intervalo=c.get("intervalo",""),
            nome=c.get("nome",""), telefone=c.get("telefone",""),
            status_de=c.get("status",""),
            detalhes=f"Vendedor: {c.get('vendedor','')} | Valor: {c.get('valor','')} | Último WA: {c.get('ultimo_wa','')}")
    log(f"Contato excluído: {c.get('nome','?')} | Lote {c.get('lote','?')}", "warning")
    return jsonify({"ok":True,"msg":"Contato removido!"})

@app.route("/api/contatos/<int:cid>/desmembrar", methods=["POST"])
@requer_login
def api_desmembrar(cid):
    """Desmembra um lote em cartelas individuais."""
    # Verifica permissão — admin tem acesso total, outros verificam no banco
    perfil = session.get("perfil","")
    if perfil != "admin":
        usuarios = carregar_usuarios()
        u = usuarios.get(session.get("usuario",""), {})
        botoes = u.get("permissoes", [])
        if "btn-desmembrar" not in botoes:
            return jsonify({"ok":False,"msg":"Sem permissão para desmembrar lotes"}), 403

    with get_db() as conn:
        row = conn.execute("SELECT * FROM contatos WHERE id=?", (cid,)).fetchone()
        if not row:
            return jsonify({"ok":False,"msg":"Registro não encontrado"})
        c = dict(row)

    # Validações
    status = (c.get("status") or "").strip().lower()
    if status != "disponivel":
        return jsonify({"ok":False,"msg":f"Só é possível desmembrar lotes com status Disponível. Status atual: {c.get('status')}"})
    if c.get("origem_id"):
        return jsonify({"ok":False,"msg":"Este registro já é uma cartela desmembrada — não pode ser desmembrado novamente"})

    # Parseia intervalo — espera formato "00100 a 00109"
    intervalo = (c.get("intervalo") or "").strip()
    partes = intervalo.split(" a ")
    if len(partes) != 2:
        return jsonify({"ok":False,"msg":f"Formato de intervalo inválido: '{intervalo}'. Esperado: '00100 a 00109'"})

    try:
        inicio = int(partes[0].strip())
        fim    = int(partes[1].strip())
        zeros  = len(partes[0].strip())  # preserva zeros à esquerda
    except:
        return jsonify({"ok":False,"msg":f"Não foi possível parsear o intervalo: '{intervalo}'"})

    qtd = fim - inicio + 1
    if qtd <= 1:
        return jsonify({"ok":False,"msg":"O intervalo deve ter mais de uma cartela para desmembrar"})

    # Calcula valor individual
    try:
        valor_orig = float(re.sub(r'[^\d,.]', '', c.get("valor","0")).replace(',','.'))
        valor_unit = valor_orig / qtd
        # Formata valor: R$ XX,XX
        valor_fmt  = f"R$ {valor_unit:,.2f}".replace(',','X').replace('.',',').replace('X','.')
    except:
        valor_fmt = c.get("valor","")

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    ids_gerados = []

    with get_db() as conn:
        # 1. Bloqueia o lote original → status Desmembrado
        conn.execute(
            "UPDATE contatos SET status='Desmembrado', atualizado_em=? WHERE id=?",
            (now, cid)
        )

        # 2. Gera cartelas individuais
        for num in range(inicio, fim + 1):
            cart = str(num).zfill(zeros)
            try:
                cur = conn.execute(
                    """INSERT INTO contatos
                       (lote, intervalo, vendedor, nome, telefone, whatsapp, valor,
                        status, observacoes, criado_em, atualizado_em, origem_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (c["lote"], cart, c.get("vendedor",""), "", "", "", valor_fmt,
                     "Disponivel", f"Desmembrada do lote {c['lote']} / {intervalo}", now, now, cid)
                )
                ids_gerados.append(cur.lastrowid)
            except Exception as e:
                return jsonify({"ok":False,"msg":f"Erro ao criar cartela {cart}: {e}"})

    # 3. Audita
    auditar("DESMEMBRAMENTO", contato_id=cid,
            lote=c["lote"], intervalo=intervalo,
            nome=c.get("nome",""), telefone=c.get("telefone",""),
            status_de="Disponivel", status_para="Desmembrado",
            detalhes=f"Geradas {qtd} cartelas individuais ({partes[0].strip()} a {partes[1].strip()}) | Valor unit: {valor_fmt} | IDs: {ids_gerados}")

    log(f"Lote {c['lote']} desmembrado em {qtd} cartelas por {session.get('usuario','?')}", "success")
    return jsonify({
        "ok": True,
        "msg": f"Lote desmembrado com sucesso em {qtd} cartelas!",
        "qtd": qtd,
        "ids": ids_gerados,
        "valor_unit": valor_fmt
    })


@app.route("/api/contatos/<int:cid>/cancelar-desmembramento", methods=["POST"])
@requer_login
def api_cancelar_desmembramento(cid):
    """Cancela o desmembramento de um lote, restaurando-o para Disponível.
    Só é permitido se todas as cartelas filhas estiverem com status Disponível."""
    # Verifica permissão
    perfil = session.get("perfil", "")
    if perfil != "admin":
        usuarios = carregar_usuarios()
        u = usuarios.get(session.get("usuario", ""), {})
        botoes = u.get("permissoes", [])
        if "btn-desmembrar" not in botoes:
            return jsonify({"ok": False, "msg": "Sem permissão para cancelar desmembramento"}), 403

    with get_db() as conn:
        # Busca o lote original
        row = conn.execute("SELECT * FROM contatos WHERE id=?", (cid,)).fetchone()
        if not row:
            return jsonify({"ok": False, "msg": "Registro não encontrado"})
        c = dict(row)

        # Valida que é um lote desmembrado
        status = (c.get("status") or "").strip().lower()
        if status != "desmembrado":
            return jsonify({"ok": False, "msg": f"Este registro não está com status Desmembrado. Status atual: {c.get('status')}"}),

        # Busca todas as cartelas filhas
        filhas = conn.execute(
            "SELECT * FROM contatos WHERE origem_id=?", (cid,)
        ).fetchall()
        filhas = [dict(f) for f in filhas]

        if not filhas:
            return jsonify({"ok": False, "msg": "Nenhuma cartela filha encontrada para este lote."})

        # Verifica se todas estão Disponível
        nao_disponiveis = [
            f for f in filhas
            if (f.get("status") or "").strip().lower() != "disponivel"
        ]

        if nao_disponiveis:
            detalhes = ", ".join([
                f"Cartela {f.get('intervalo','?')} ({f.get('status','?')})"
                for f in nao_disponiveis
            ])
            return jsonify({
                "ok": False,
                "msg": f"Não é possível cancelar o desmembramento. {len(nao_disponiveis)} cartela(s) não estão disponíveis: {detalhes}"
            })

        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        qtd = len(filhas)

        # Remove as cartelas filhas
        conn.execute("DELETE FROM contatos WHERE origem_id=?", (cid,))

        # Restaura o lote original para Disponível
        conn.execute(
            "UPDATE contatos SET status='Disponivel', atualizado_em=? WHERE id=?",
            (now, cid)
        )

    # Audita
    auditar("CANCELAMENTO_DESMEMBRAMENTO", contato_id=cid,
            lote=c["lote"], intervalo=c.get("intervalo", ""),
            nome=c.get("nome", ""), telefone=c.get("telefone", ""),
            status_de="Desmembrado", status_para="Disponivel",
            detalhes=f"{qtd} cartelas removidas — lote restaurado para Disponível")

    log(f"Desmembramento do lote {c['lote']} cancelado por {session.get('usuario', '?')}", "success")
    return jsonify({
        "ok": True,
        "msg": f"Desmembramento cancelado! Lote {c['lote']} restaurado para Disponível.",
        "qtd_removidas": qtd
    })


@app.route("/api/contatos/<int:cid>/pagamento-parcial", methods=["POST"])
@requer_login
def api_pagamento_parcial(cid):
    """Registra um pagamento parcial. Se quitar, vira Pago. Senão, vira Pgto. Parcial."""
    d = request.get_json() or {}
    valor_str    = str(d.get("valor", "0") or "0")
    data_pgto    = (d.get("data_pagamento") or datetime.now().strftime("%d/%m/%Y")).strip()
    forma_pgto   = (d.get("forma_pagamento") or "Pix").strip()
    recebido_por = (d.get("recebido_por") or "").strip()

    try:
        valor_num = float(re.sub(r"[^\d,.]", "", valor_str).replace(",", "."))
    except:
        return jsonify({"ok": False, "msg": "Valor inválido"})

    if valor_num <= 0:
        return jsonify({"ok": False, "msg": "O valor deve ser maior que zero"})

    with get_db() as conn:
        row = conn.execute("SELECT * FROM contatos WHERE id=?", (cid,)).fetchone()
        if not row:
            return jsonify({"ok": False, "msg": "Registro não encontrado"})
        c = dict(row)

        status_atual = (c.get("status") or "").strip().lower()
        if status_atual in ["desmembrado", "pago"]:
            return jsonify({"ok": False, "msg": f"Não é possível registrar pagamento para status '{c.get('status')}'"})

        try:
            valor_total = float(re.sub(r"[^\d,.]", "", str(c.get("valor") or "0")).replace(",", "."))
        except:
            valor_total = 0

        valor_pago_ant = float(c.get("valor_pago") or 0)
        saldo_ant      = float(c.get("saldo_devedor") or valor_total)

        # Validação: não pode ser maior que o saldo
        if saldo_ant > 0 and valor_num > saldo_ant:
            return jsonify({"ok": False, "msg": f"Valor não pode ser maior que o saldo devedor (R$ {saldo_ant:.2f})"})

        novo_valor_pago = valor_pago_ant + valor_num
        novo_saldo      = max(0, valor_total - novo_valor_pago)
        novo_status     = "Pago" if (valor_total > 0 and novo_valor_pago >= valor_total) else "Pgto. Parcial"

        now = datetime.now().strftime("%d/%m/%Y %H:%M")

        conn.execute(
            """INSERT INTO pagamentos_parciais
               (contato_id, valor, data_pagamento, forma_pagamento, recebido_por, criado_em, usuario)
               VALUES (?,?,?,?,?,?,?)""",
            (cid, valor_num, data_pgto, forma_pgto, recebido_por, now, session.get("usuario", ""))
        )
        conn.execute(
            """UPDATE contatos SET status=?, valor_pago=?, saldo_devedor=?,
               data_pagamento=?, forma_pagamento=?, atualizado_em=? WHERE id=?""",
            (novo_status, novo_valor_pago, novo_saldo, data_pgto, forma_pgto, now, cid)
        )

    auditar("PAGAMENTO_PARCIAL", contato_id=cid,
            lote=c["lote"], intervalo=c.get("intervalo",""),
            nome=c.get("nome",""), telefone=c.get("telefone",""),
            status_de=c.get("status",""), status_para=novo_status,
            detalhes=f"Valor: R$ {valor_num:.2f} | Forma: {forma_pgto} | Por: {recebido_por} | Total pago: R$ {novo_valor_pago:.2f} | Saldo: R$ {novo_saldo:.2f}")

    valor_fmt = f"R$ {valor_num:_.2f}".replace("_", ".").replace(".",",",1) if False else f"R$ {valor_num:.2f}".replace(".",",")
    return jsonify({
        "ok": True,
        "msg": f"Pagamento de R$ {valor_num:.2f} registrado!".replace(".",","),
        "novo_status": novo_status,
        "valor_pago": novo_valor_pago,
        "saldo_devedor": novo_saldo,
        "quitado": novo_status == "Pago"
    })



@app.route("/api/contatos/<int:cid>/pagamentos")
@requer_login
def api_pagamentos_contato(cid):
    """Retorna histórico de pagamentos parciais de uma cartela."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM contatos WHERE id=?", (cid,)).fetchone()
        if not row:
            return jsonify({"ok": False, "msg": "Registro não encontrado"})
        c = dict(row)
        pagamentos = conn.execute(
            "SELECT * FROM pagamentos_parciais WHERE contato_id=? ORDER BY id ASC", (cid,)
        ).fetchall()
        pagamentos = [dict(p) for p in pagamentos]
    try:
        valor_total = float(re.sub(r"[^\d,.]", "", str(c.get("valor") or "0")).replace(",", "."))
    except:
        valor_total = 0
    valor_pago = float(c.get("valor_pago") or 0)
    saldo = max(0, valor_total - valor_pago)
    return jsonify({
        "ok": True,
        "contato": {
            "id": c["id"], "lote": c.get("lote",""), "intervalo": c.get("intervalo",""),
            "nome": c.get("nome",""), "valor": c.get("valor",""),
            "valor_total": valor_total, "valor_pago": valor_pago,
            "saldo_devedor": saldo, "status": c.get("status","")
        },
        "pagamentos": pagamentos
    })


@app.route("/api/pagamentos-parciais/<int:pid>/estornar", methods=["POST"])
@requer_login
def api_estornar_pagamento(pid):
    """Estorna um pagamento parcial. Recalcula saldo e status do contato."""
    with get_db() as conn:
        pag = conn.execute("SELECT * FROM pagamentos_parciais WHERE id=?", (pid,)).fetchone()
        if not pag:
            return jsonify({"ok": False, "msg": "Pagamento não encontrado"})
        pag = dict(pag)
        cid = pag["contato_id"]

        row = conn.execute("SELECT * FROM contatos WHERE id=?", (cid,)).fetchone()
        if not row:
            return jsonify({"ok": False, "msg": "Contato não encontrado"})
        c = dict(row)

        # Remove o pagamento
        conn.execute("DELETE FROM pagamentos_parciais WHERE id=?", (pid,))

        # Recalcula valor_pago somando os restantes
        restantes = conn.execute(
            "SELECT COALESCE(SUM(valor),0) as total FROM pagamentos_parciais WHERE contato_id=?", (cid,)
        ).fetchone()
        novo_valor_pago = float(restantes["total"] or 0)

        try:
            valor_total = float(re.sub(r"[^\d,.]", "", str(c.get("valor") or "0")).replace(",", "."))
        except:
            valor_total = 0

        novo_saldo = max(0, valor_total - novo_valor_pago)

        # Define novo status
        if novo_valor_pago <= 0:
            novo_status = "Pendente"
        elif novo_valor_pago >= valor_total:
            novo_status = "Pago"
        else:
            novo_status = "Pgto. Parcial"

        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        conn.execute(
            "UPDATE contatos SET status=?, valor_pago=?, saldo_devedor=?, atualizado_em=? WHERE id=?",
            (novo_status, novo_valor_pago, novo_saldo, now, cid)
        )

    auditar("ESTORNO_PAGAMENTO", contato_id=cid,
            lote=c["lote"], intervalo=c.get("intervalo",""),
            nome=c.get("nome",""), telefone=c.get("telefone",""),
            status_de=c.get("status",""), status_para=novo_status,
            detalhes=f"Estorno do pagamento #{pid} de R$ {pag['valor']:.2f} | Novo saldo: R$ {novo_saldo:.2f}")

    return jsonify({
        "ok": True,
        "msg": f"Pagamento estornado! Status voltou para {novo_status}.",
        "novo_status": novo_status,
        "valor_pago": novo_valor_pago,
        "saldo_devedor": novo_saldo
    })



@app.route("/api/contatos/lotes")
@requer_login
def api_lotes():
    with get_db() as conn:
        rows=conn.execute("SELECT DISTINCT lote FROM contatos WHERE lote!='' ORDER BY CAST(lote AS INTEGER)").fetchall()
    return jsonify({"ok":True,"lotes":[r[0] for r in rows]})

@app.route("/api/contatos/proximo-lote")
@requer_login
def api_proximo_lote():
    with get_db() as conn:
        # Maior lote atual
        row = conn.execute("SELECT MAX(CAST(lote AS INTEGER)) FROM contatos WHERE lote!=''").fetchone()
        max_lote = (row[0] or 0)
        proximo_lote = max_lote + 1
        # Maior cartela final do intervalo atual
        rows = conn.execute("SELECT intervalo FROM contatos WHERE intervalo!=''").fetchall()
        max_cartela = 0
        for r in rows:
            iv = (r[0] or "").strip()
            # Extrai último número do intervalo (ex: "00001 a 00010" -> 10)
            partes = iv.replace(" a ", " ").replace(" A ", " ").split()
            for p in partes:
                try:
                    v = int(p.strip())
                    if v > max_cartela: max_cartela = v
                except: pass
        inicio = max_cartela + 1
        fim = inicio + 9
        intervalo = f"{inicio:05d} a {fim:05d}"
    return jsonify({"ok":True,"lote":str(proximo_lote),"intervalo":intervalo})

@app.route("/api/contatos/marcar-pago", methods=["POST"])
@requer_login
def api_marcar_pago():
    d=request.get_json() or {}; ids=d.get("ids",[])
    if not ids: return jsonify({"ok":False,"erro":"Nenhum contato"})
    now=datetime.now().strftime("%d/%m/%Y %H:%M")
    data_pgto  = (d.get("data_pagamento") or now[:10]).strip()
    forma_pgto = (d.get("forma_pagamento") or "").strip()
    with get_db() as conn:
        rows = conn.execute(f"SELECT id,nome,lote,intervalo,telefone,status FROM contatos WHERE id IN ({','.join('?'*len(ids))})", ids).fetchall()
        conn.execute(f"UPDATE contatos SET status='Pago',data_pagamento=?,forma_pagamento=?,atualizado_em=? WHERE id IN ({','.join('?'*len(ids))})", [data_pgto,forma_pgto,now]+ids)
    for r in rows:
        c = dict(r)
        auditar("MARCAR_PAGO", contato_id=c["id"], lote=c.get("lote",""),
                intervalo=c.get("intervalo",""), nome=c.get("nome",""),
                telefone=c.get("telefone",""), status_de=c.get("status",""), status_para="Pago",
                detalhes=f"Forma: {forma_pgto} | Data: {data_pgto}")
    log(f"Marcado como Pago: {len(ids)} contato(s)","success")
    return jsonify({"ok":True,"msg":f"{len(ids)} contato(s) marcado(s) como Pago!"})

@app.route("/api/contatos/importar", methods=["POST"])
@requer_login
def api_importar():
    d=request.get_json() or {}; contatos=d.get("contatos",[]); now=datetime.now().strftime("%d/%m/%Y %H:%M"); inseridos=0
    with get_db() as conn:
        for c in contatos:
            nome=(c.get("nome","") or "").strip(); tel=(c.get("telefone","") or "").strip()
            if not nome or not tel: continue
            conn.execute("INSERT INTO contatos (lote,intervalo,vendedor,nome,telefone,whatsapp,valor,status,ultimo_wa,criado_em,atualizado_em) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (c.get("lote",""),c.get("intervalo",""),c.get("vendedor",""),nome,tel,c.get("whatsapp",""),c.get("valor",""),c.get("status","Disponivel"),c.get("ultimo_wa",""),now,now))
            inseridos+=1
    log(f"Importados: {inseridos} contatos","success")
    return jsonify({"ok":True,"msg":f"{inseridos} contato(s) importado(s)!","inseridos":inseridos})

# ══════════════════════════════════════════════════════════
#  API IMPORTAÇÃO (Sheets / Arquivo)
# ══════════════════════════════════════════════════════════

@app.route("/api/importar/listar-planilhas", methods=["POST"])
@requer_login
def api_listar_planilhas():
    """Lista todas as planilhas disponíveis na conta Google (via service account)."""
    cred_path = os.path.join(APP_DIR, "credenciais_google.json")
    if not os.path.exists(cred_path):
        return jsonify({"ok":False,"erro":"credenciais_google.json não encontrado no servidor"})
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds  = Credentials.from_service_account_file(cred_path, scopes=scopes)
        gc     = gspread.authorize(creds)
        planilhas = [{"nome": s.title, "id": s.id} for s in gc.openall()]
        if not planilhas:
            return jsonify({"ok":True,"planilhas":[],"msg":"Nenhuma planilha compartilhada com esta conta de serviço."})
        return jsonify({"ok":True,"planilhas":planilhas,"msg":f"{len(planilhas)} planilha(s) encontrada(s)"})
    except Exception as e:
        return jsonify({"ok":False,"erro":str(e)})


@app.route("/api/importar/testar-sheets", methods=["POST"])
@requer_login
def api_testar_sheets():
    """Conecta em uma planilha específica e retorna as abas disponíveis."""
    d = request.get_json() or {}
    nome_planilha = d.get("nome_planilha","").strip()
    sheet_id      = d.get("sheet_id","").strip()
    cred_path = os.path.join(APP_DIR, "credenciais_google.json")
    if not os.path.exists(cred_path):
        return jsonify({"ok":False,"erro":"credenciais_google.json não encontrado no servidor"})
    if not nome_planilha and not sheet_id:
        return jsonify({"ok":False,"erro":"Informe o nome ou selecione uma planilha"})
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds  = Credentials.from_service_account_file(cred_path, scopes=scopes)
        gc     = gspread.authorize(creds)
        sh     = gc.open_by_key(sheet_id) if sheet_id else gc.open(nome_planilha)
        abas   = [ws.title for ws in sh.worksheets()]
        return jsonify({"ok":True,"abas":abas,"nome":sh.title,"msg":f"Conectado! {len(abas)} aba(s)"})
    except Exception as e:
        return jsonify({"ok":False,"erro":str(e)})


@app.route("/api/importar/sheets", methods=["POST"])
@requer_login
def api_importar_sheets():
    d = request.get_json() or {}
    nome_planilha = d.get("nome_planilha","").strip()
    sheet_id      = d.get("sheet_id","").strip()
    aba           = d.get("aba","").strip()
    limpar_antes  = d.get("limpar_antes", True)
    if not nome_planilha and not sheet_id:
        return jsonify({"ok":False,"erro":"Informe o nome ou selecione uma planilha"})
    cred_path = os.path.join(APP_DIR, "credenciais_google.json")
    if not os.path.exists(cred_path):
        return jsonify({"ok":False,"erro":"credenciais_google.json não encontrado no servidor"})
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds  = Credentials.from_service_account_file(cred_path, scopes=scopes)
        gc     = gspread.authorize(creds)
        if sheet_id:
            sh = gc.open_by_key(sheet_id)
            ws = sh.worksheet(aba) if aba else sh.sheet1
        elif aba:
            ws = gc.open(nome_planilha).worksheet(aba)
        else:
            ws = gc.open(nome_planilha).sheet1
        rows   = ws.get_all_values()
    except Exception as e:
        return jsonify({"ok":False,"erro":f"Erro ao conectar: {e}"})
    if len(rows) < 2: return jsonify({"ok":False,"erro":"Planilha vazia ou sem dados"})
    # Colunas v1: A=lote B=intervalo C=vendedor D=nome E=telefone F=whatsapp G=valor H=status I=ultimo_wa
    def col(row, i): return row[i-1].strip() if len(row) >= i else ""
    contatos = []
    sem_lote = 0
    for row in rows[1:]:
        lote      = col(row, 1)
        intervalo = col(row, 2)
        if not lote or not intervalo: sem_lote += 1; continue  # só ignora se lote OU intervalo vazio
        contatos.append({
            "lote":      lote,
            "intervalo": intervalo,
            "vendedor":  col(row, 3).upper(),
            "nome":      col(row, 4).upper(),
            "telefone":  col(row, 5),
            "whatsapp":  col(row, 6),
            "valor":     col(row, 7),
            "status":    col(row, 8) if col(row, 8) else "Disponivel",
            "ultimo_wa": col(row, 9)
        })
    if not contatos: return jsonify({"ok":False,"erro":"Nenhum contato válido encontrado"})
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    inseridos = substituidos = 0
    ign_sem_nome = ign_sem_tel = ign_duplicado = 0
    with get_db() as conn:
        if limpar_antes:
            conn.execute("DELETE FROM contatos")
            conn.execute("DELETE FROM sqlite_sequence WHERE name='contatos'")
            log("Base zerada antes da importação","warning")
        for c in contatos:
            # Nome e telefone podem ser vazios (lote ainda não vendido = Disponivel)
            nome_db    = c["nome"]    or ""
            telefone_db = c["telefone"] or ""
            # Se status veio da planilha como Pago/Pendente explícito, mantém; senão recalcula pelo nome
            status_raw = c["status"] if c["status"] not in ("Disponivel", "", None) else ""
            status_db  = status_raw if status_raw else ("Disponivel" if not nome_db else "Pendente")
            existe = conn.execute(
                "SELECT id FROM contatos WHERE lote=? AND intervalo=?",
                (c["lote"], c["intervalo"])
            ).fetchone()
            if existe and not limpar_antes:
                conn.execute("""UPDATE contatos SET vendedor=?,nome=?,telefone=?,whatsapp=?,valor=?,
                    status=?,ultimo_wa=?,atualizado_em=? WHERE lote=? AND intervalo=?""",
                    (c["vendedor"],nome_db,telefone_db,c["whatsapp"],c["valor"],
                     status_db,c["ultimo_wa"],now, c["lote"],c["intervalo"]))
                substituidos += 1
            else:
                try:
                    conn.execute("""INSERT INTO contatos
                        (lote,intervalo,vendedor,nome,telefone,whatsapp,valor,status,ultimo_wa,criado_em,atualizado_em)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (c["lote"],c["intervalo"],c["vendedor"],nome_db,telefone_db,
                         c["whatsapp"],c["valor"],status_db,c["ultimo_wa"],now,now))
                    inseridos += 1
                except Exception:
                    ign_duplicado += 1
    ignorados = ign_sem_nome + ign_sem_tel + ign_duplicado
    total = inseridos + substituidos
    detalhe = []
    if sem_lote:    detalhe.append(f"{sem_lote} sem lote")
    if ign_sem_nome: detalhe.append(f"{ign_sem_nome} sem nome")
    if ign_sem_tel:  detalhe.append(f"{ign_sem_tel} sem telefone")
    if ign_duplicado: detalhe.append(f"{ign_duplicado} duplicados")
    auditar("IMPORTACAO_SHEETS", detalhes=f"Inseridos: {inseridos} | Atualizados: {substituidos} | Ignorados: {ignorados} | Sem lote: {sem_lote}")
    log(f"Importados do Sheets: {inseridos} novos, {substituidos} atualizados, {ignorados} ignorados ({', '.join(detalhe) if detalhe else 'nenhum'})","success")
    return jsonify({"ok":True,"inseridos":inseridos,"substituidos":substituidos,"ignorados":ignorados,
        "sem_lote":sem_lote,"ign_sem_nome":ign_sem_nome,"ign_sem_tel":ign_sem_tel,"ign_duplicado":ign_duplicado,
        "total":total,"msg":f"{total} contato(s) importado(s) do Google Sheets!"})


@app.route("/api/importar/arquivo", methods=["POST"])
@requer_login
def api_importar_arquivo():
    if "arquivo" not in request.files: return jsonify({"ok":False,"erro":"Nenhum arquivo enviado"})
    arq  = request.files["arquivo"]
    nome = arq.filename.lower()
    try:
        if nome.endswith(".xlsx") or nome.endswith(".xls"):
            try:
                import openpyxl
                from io import BytesIO
                wb   = openpyxl.load_workbook(BytesIO(arq.read()), data_only=True)
                ws   = wb.active
                rows = [[str(cell.value or "").strip() for cell in row] for row in ws.iter_rows()]
            except ImportError:
                return jsonify({"ok":False,"erro":"openpyxl não instalado. Execute: pip install openpyxl"})
        elif nome.endswith(".csv"):
            import csv
            from io import StringIO
            content = arq.read().decode("utf-8-sig")
            rows = [[c.strip() for c in row] for row in csv.reader(StringIO(content))]
        else:
            return jsonify({"ok":False,"erro":"Formato não suportado. Use .xlsx ou .csv"})
    except Exception as e:
        return jsonify({"ok":False,"erro":f"Erro ao ler arquivo: {e}"})
    if len(rows) < 2: return jsonify({"ok":False,"erro":"Arquivo vazio ou sem dados"})

    # Detecta colunas pelo cabeçalho
    header = [h.lower() for h in rows[0]]
    def ci(names):
        for n in names:
            for i,h in enumerate(header):
                if n in h: return i
        return -1
    idx_lote=ci(["lote"]); idx_int=ci(["intervalo","cartela"]); idx_vend=ci(["vendedor","responsavel"])
    idx_nome=ci(["nome"]); idx_tel=ci(["telefone","tel","fone","celular"])
    idx_wa=ci(["whatsapp","wpp","zap"]); idx_val=ci(["valor","preco"]); idx_st=ci(["status","situac"])
    # fallback posicional (layout v1)
    if idx_nome<0 and idx_tel<0:
        idx_lote=0;idx_int=1;idx_vend=2;idx_nome=3;idx_tel=4;idx_wa=5;idx_val=6;idx_st=7

    def get(row,i): return row[i].strip() if i>=0 and i<len(row) else ""
    data_rows = rows[1:] if ci(["nome","lote"])>=0 else rows
    contatos = []
    sem_lote = 0
    for row in data_rows:
        lote      = get(row, idx_lote)
        intervalo = get(row, idx_int)
        if not lote or not intervalo: sem_lote += 1; continue  # só ignora se lote OU intervalo vazio
        contatos.append({
            "lote":      lote,
            "intervalo": intervalo,
            "vendedor":  get(row, idx_vend).upper(),
            "nome":      get(row, idx_nome).upper(),
            "telefone":  get(row, idx_tel),
            "whatsapp":  get(row, idx_wa),
            "valor":     get(row, idx_val),
            "status":    get(row, idx_st) or ""
        })
    if not contatos: return jsonify({"ok":False,"erro":"Nenhum contato válido encontrado"})
    limpar_antes = request.form.get("limpar_antes","1") != "0"
    now=datetime.now().strftime("%d/%m/%Y %H:%M")
    inseridos = substituidos = ign_duplicado = 0
    with get_db() as conn:
        if limpar_antes:
            conn.execute("DELETE FROM contatos")
            conn.execute("DELETE FROM sqlite_sequence WHERE name='contatos'")
            log("Base zerada antes da importação","warning")
        for c in contatos:
            nome_db     = c["nome"]     or ""
            telefone_db = c["telefone"] or ""
            status_raw  = c["status"] if c["status"] not in ("Disponivel", "", None) else ""
            status_db   = status_raw if status_raw else ("Disponivel" if not nome_db else "Pendente")
            existe = conn.execute(
                "SELECT id FROM contatos WHERE lote=? AND intervalo=?",
                (c["lote"], c["intervalo"])
            ).fetchone()
            if existe and not limpar_antes:
                conn.execute("""UPDATE contatos SET vendedor=?,nome=?,telefone=?,whatsapp=?,valor=?,
                    status=?,atualizado_em=? WHERE lote=? AND intervalo=?""",
                    (c["vendedor"],nome_db,telefone_db,c["whatsapp"],c["valor"],status_db,now,
                     c["lote"],c["intervalo"]))
                substituidos += 1
            else:
                try:
                    conn.execute("""INSERT INTO contatos
                        (lote,intervalo,vendedor,nome,telefone,whatsapp,valor,status,criado_em,atualizado_em)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (c["lote"],c["intervalo"],c["vendedor"],nome_db,telefone_db,
                         c["whatsapp"],c["valor"],status_db,now,now))
                    inseridos += 1
                except Exception:
                    ign_duplicado += 1
    ignorados = ign_duplicado
    total = inseridos + substituidos
    detalhe = []
    if sem_lote:      detalhe.append(f"{sem_lote} sem lote/intervalo")
    if ign_duplicado: detalhe.append(f"{ign_duplicado} duplicados")
    auditar("IMPORTACAO_ARQUIVO", detalhes=f"Inseridos: {inseridos} | Atualizados: {substituidos} | Ignorados: {ignorados} | Sem lote: {sem_lote}")
    log(f"Importados do arquivo: {inseridos} novos, {substituidos} atualizados, {ignorados} ignorados","success")
    return jsonify({"ok":True,"inseridos":inseridos,"substituidos":substituidos,"ignorados":ignorados,
        "sem_lote":sem_lote,"ign_sem_nome":0,"ign_sem_tel":0,"ign_duplicado":ign_duplicado,
        "total":total,"msg":f"{total} contato(s) importado(s) do arquivo!"})


# ══════════════════════════════════════════════════════════
#  API GRID (compatível v1)
# ══════════════════════════════════════════════════════════

@app.route("/api/grid/dados")
@requer_login
def api_grid_dados():
    with get_db() as conn:
        rows=conn.execute("SELECT * FROM contatos ORDER BY CAST(lote AS INTEGER),id").fetchall()
    dados=[dict(r) for r in rows]
    for d in dados: d["linha_sheet"]=d["id"]
    return jsonify({"ok":True,"dados":dados,"total":len(dados)})

@app.route("/api/grid/marcar-pago", methods=["POST"])
@requer_login
def api_grid_marcar_pago():
    d=request.get_json() or {}; linhas=d.get("linhas",[])
    if not linhas: return jsonify({"ok":False,"msg":"Nenhuma linha!"})
    now=datetime.now().strftime("%d/%m/%Y %H:%M")
    data_pgto  = (d.get("data_pagamento") or now[:10]).strip()
    forma_pgto = (d.get("forma_pagamento") or "").strip()
    ids = [int(lid) for lid in linhas]
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT id,nome,lote,intervalo,telefone,status FROM contatos WHERE id IN ({','.join('?'*len(ids))})", ids
        ).fetchall()
        for lid in ids:
            conn.execute("UPDATE contatos SET status='Pago',data_pagamento=?,forma_pagamento=?,atualizado_em=? WHERE id=?", (data_pgto,forma_pgto,now,lid))
    for r in rows:
        c = dict(r)
        auditar("MARCAR_PAGO", contato_id=c["id"], lote=c.get("lote",""),
                intervalo=c.get("intervalo",""), nome=c.get("nome",""),
                telefone=c.get("telefone",""), status_de=c.get("status",""), status_para="Pago",
                detalhes=f"Forma: {forma_pgto} | Data: {data_pgto}")
    log(f"Marcado como Pago: {len(ids)} registro(s)","success")
    return jsonify({"ok":True,"msg":f"{len(ids)} registro(s) marcado(s) como Pago!"})

@app.route("/api/grid/check-enviado-hoje", methods=["POST"])
@requer_login
def api_check_enviado_hoje():
    """Recebe lista de IDs e retorna quais já foram enviados hoje."""
    d = request.get_json() or {}
    linhas = d.get("linhas", [])
    if not linhas:
        return jsonify({"ok": True, "ja_enviados": []})
    ja_hoje = ids_enviados_hoje()
    ja_enviados = [lid for lid in linhas if int(lid) in ja_hoje]
    return jsonify({"ok": True, "ja_enviados": ja_enviados})

@app.route("/api/grid/enviar-cobranca", methods=["POST"])
@requer_login
def api_grid_enviar_cobranca():
    d = request.get_json() or {}; linhas = d.get("linhas",[]); cfg = carregar_config()
    if not linhas: return jsonify({"ok":False,"msg":"Nenhuma linha selecionada!"})
    sid   = cfg.get("twilio_sid",""); token = cfg.get("twilio_token",""); num = cfg.get("twilio_numero","")
    if not all([sid,token,num]): return jsonify({"ok":False,"msg":"Twilio não configurado!"})
    if not TWILIO_OK: return jsonify({"ok":False,"msg":"Biblioteca Twilio não instalada!"})
    try:
        client = TwilioClient(sid, token)
    except Exception as e:
        return jsonify({"ok":False,"msg":"Erro ao conectar Twilio: "+str(e)})

    # Busca contatos ANTES de qualquer envio — fora do lock Twilio
    with get_db() as conn:
        contatos = []
        for lid in linhas:
            row = conn.execute("SELECT * FROM contatos WHERE id=?", (int(lid),)).fetchone()
            if row:
                contatos.append(dict(row))

    ja_hoje = ids_enviados_hoje()
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    enviados = erros = 0

    for c in contatos:
        if c.get("id") in ja_hoje:
            log(f"⏭ {c['nome']} já recebeu mensagem hoje — ignorado.","warning")
            continue
        try:
            ok, sid_ou_erro = _enviar_twilio(client, num, c, cfg)
            # Cada update do banco é uma operação curta e independente
            with get_db() as conn:
                if ok:
                    conn.execute("UPDATE contatos SET ultimo_wa=?,atualizado_em=? WHERE id=?", (now,now,c["id"]))
                    conn.execute(
                        "INSERT INTO log_envios (contato_id,nome,telefone,status,mensagem,criado_em) VALUES (?,?,?,?,?,?)",
                        (c["id"],c["nome"],c.get("telefone",""),"ENVIADO",sid_ou_erro,now)
                    )
                    log(f"✅ Cobrança enviada: {c['nome']} | SID: {sid_ou_erro}","success")
                    enviados += 1
                else:
                    conn.execute(
                        "INSERT INTO log_envios (contato_id,nome,telefone,status,mensagem,criado_em) VALUES (?,?,?,?,?,?)",
                        (c["id"],c["nome"],c.get("telefone",""),"ERRO",sid_ou_erro,now)
                    )
                    log(f"❌ Erro cobrança {c['nome']}: {sid_ou_erro}","error")
                    erros += 1
        except Exception as e:
            log(f"❌ Erro cobrança {c['nome']}: {e}","error")
            erros += 1

    return jsonify({"ok":True,"msg":f"{enviados} enviada(s)"+(f", {erros} erro(s)" if erros else "")+"!"})

# ══════════════════════════════════════════════════════════
#  API DISPARO
# ══════════════════════════════════════════════════════════

@app.route("/api/estado")
@requer_login
def api_estado():
    cfg=carregar_config()
    return jsonify({"ok":True,**estado,"config":{"evento":cfg.get("nome_evento",""),"planilha":"SQLite local",
        "agendamento":(cfg.get("modo_envio","manual")+" "+cfg.get("horario_envio","")).strip(),"proximo":"—"}})

@app.route("/api/disparar", methods=["POST"])
@requer_login
def api_disparar():
    if estado["enviando"]: return jsonify({"ok":False,"msg":"Já existe envio em andamento!"})
    d = request.get_json() or {}
    modo_teste = d.get("modo_teste", False)
    limite     = int(d.get("limite", 0) or 0)
    data_base  = d.get("data_base", "").strip() or None
    threading.Thread(
        target=executar_envio_thread,
        kwargs={"limite":limite, "modo_teste":modo_teste, "data_base":data_base, "usuario_disparo":session.get("usuario","?")},
        daemon=True
    ).start()
    log("Disparo iniciado — Modo: "+("TESTE" if modo_teste else "REAL")+(f" | Data base: {data_base}" if data_base else ""),"info")
    return jsonify({"ok":True,"msg":"Disparo iniciado! Acompanhe o log."})

@app.route("/api/disparar/parar", methods=["POST"])
@requer_login
def api_parar_disparo():
    estado["enviando"] = False
    log("Disparo interrompido pelo usuário.","warning")
    return jsonify({"ok":True,"msg":"Disparo interrompido!"})

@app.route("/api/disparar/resumo")
@requer_login
def api_disparar_resumo():
    hoje    = datetime.now().date()
    ja_hoje = ids_enviados_hoje()
    with get_db() as conn:
        rows   = conn.execute("SELECT * FROM contatos").fetchall()
        envios = conn.execute(
            "SELECT contato_id, criado_em as ultimo FROM log_envios "
            "WHERE status='ENVIADO' AND id IN ("
            "  SELECT MAX(id) FROM log_envios WHERE status='ENVIADO' GROUP BY contato_id"
            ")"
        ).fetchall()
    ultimo_envio_data = {r["contato_id"]: r["ultimo"] for r in envios}
    disparar = sem_contato = pagos = ja_enviado = ignorado = 0
    for r in rows:
        c      = dict(r)
        status = (c.get("status") or "").strip().lower()
        nome   = (c.get("nome")   or "").strip()
        tel    = (c.get("telefone") or "").strip()
        if status in ("pago","quitado","confirmado","ok","sim","s"):
            pagos += 1; continue
        if status != "pendente":
            continue
        if not nome or not tel:
            sem_contato += 1; continue
        if c["id"] in ja_hoje:
            ja_enviado += 1; continue
        ok_v2, _ = deve_disparar_v2(c, ultimo_envio_data.get(c["id"]), hoje)
        if ok_v2:
            disparar += 1
        else:
            ignorado += 1
    return jsonify({"ok":True,"disparar":disparar,"sem_contato":sem_contato,"pagos":pagos,"ja_enviado":ja_enviado,"ignorado":ignorado})

@app.route("/api/disparar/progresso")
@requer_login
def api_progresso():
    return jsonify({"ok":True,"enviando":estado["enviando"],"progresso":estado["progresso"],
        "total":estado["total"],"enviados":estado["enviados"],"ignorados":estado["ignorados"],"erros":estado["erros"]})

@app.route("/api/teste/unico", methods=["POST"])
@app.route("/api/teste-unico", methods=["POST"])
@requer_login
def api_teste_unico():
    d = request.get_json() or {}
    cfg = carregar_config()
    sid   = cfg.get("twilio_sid","")
    token = cfg.get("twilio_token","")
    num   = cfg.get("twilio_numero","")
    if not all([sid,token,num]): return jsonify({"ok":False,"msg":"Twilio não configurado!"})
    if not TWILIO_OK: return jsonify({"ok":False,"msg":"Biblioteca Twilio não instalada!"})
    c = {
        "nome":      d.get("nome","Teste"),
        "telefone":  d.get("numero",""),
        "whatsapp":  d.get("numero",""),
        "lote":      d.get("lote","1"),
        "intervalo": d.get("intervalo","00001 a 00010"),
        "valor":     d.get("valor","R$ 0,00"),
        "vendedor":  d.get("vendedor","Teste"),
    }
    try:
        client = TwilioClient(sid, token)
        ok, sid_ou_erro = _enviar_twilio(client, num, c, cfg)
        if ok:
            log(f"✅ Teste enviado para {d.get('numero','')} | SID: {sid_ou_erro}","success")
            return jsonify({"ok":True,"msg":"Mensagem de teste enviada!\nSID: "+sid_ou_erro})
        else:
            log(f"❌ Erro no teste: {sid_ou_erro}","error")
            return jsonify({"ok":False,"msg":"ERRO: "+sid_ou_erro})
    except Exception as e:
        log(f"❌ Erro no teste: {e}","error")
        return jsonify({"ok":False,"msg":"ERRO: "+str(e)})

@app.route("/api/disparar-lote-teste", methods=["POST"])
@requer_login
def api_disparar_lote_teste():
    d = request.get_json() or {}
    limite = int(d.get("limite", 5) or 5)
    if estado["enviando"]: return jsonify({"ok":False,"msg":"Já existe envio em andamento!"})
    threading.Thread(
        target=executar_envio_thread,
        kwargs={"limite":limite, "modo_teste":True, "usuario_disparo":session.get("usuario","?")},
        daemon=True
    ).start()
    log(f"Disparo de teste em lote iniciado — limite: {limite}","info")
    return jsonify({"ok":True,"msg":f"Disparo de teste iniciado para os primeiros {limite} contatos! Acompanhe o log."})

@app.route("/api/agendar", methods=["POST"])
@requer_login
def api_agendar():
    d=request.get_json() or {}; cfg=carregar_config()
    for k in ["modo_envio","horario_envio","dia_semana","dia_mes"]:
        if k in d: cfg[k]=d[k]
    salvar_config(cfg); aplicar_config()
    log(f"Agendamento configurado: {cfg.get('modo_envio','')} às {cfg.get('horario_envio','')}","success")
    return jsonify({"ok":True,"msg":"Agendamento salvo!"})

# ══════════════════════════════════════════════════════════
#  API LOG
# ══════════════════════════════════════════════════════════

@app.route("/api/log")
@requer_login
def api_log():
    try:
        with open(LOG_PATH) as f: logs=json.load(f)
    except: logs=estado["log"]
    return jsonify({"ok":True,"log":logs[:200]})

@app.route("/api/log/limpar", methods=["POST"])
@requer_login
def api_log_limpar():
    estado["log"]=[]
    try:
        with open(LOG_PATH,"w") as f: json.dump([], f)
    except: pass
    return jsonify({"ok":True,"msg":"Log limpo!"})

# ══════════════════════════════════════════════════════════
#  API DASHBOARD
# ══════════════════════════════════════════════════════════

@app.route("/api/dashboard")
@requer_login
def api_dashboard():
    with get_db() as conn:
        total      = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status) != 'desmembrado'").fetchone()[0]
        pagos      = conn.execute("SELECT COUNT(*) FROM contatos WHERE status='Pago'").fetchone()[0]
        pendentes  = conn.execute("SELECT COUNT(*) FROM contatos WHERE status='Pendente'").fetchone()[0]
        disponiveis= conn.execute("SELECT COUNT(*) FROM contatos WHERE status='Disponivel'").fetchone()[0]
        desmembrados= conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status)='desmembrado'").fetchone()[0]
    return jsonify({"ok":True,"total":total,"pagos":pagos,"pendentes":pendentes,"disponiveis":disponiveis,"desmembrados":desmembrados})

# ══════════════════════════════════════════════════════════
#  API CONFIG
# ══════════════════════════════════════════════════════════

@app.route("/api/config/carregar")
@requer_login
def api_carregar_config():
    cfg=carregar_config()
    safe={k:v for k,v in cfg.items() if k!="senha"}
    safe["twilio_token"]=(cfg.get("twilio_token","")[:6]+"****") if cfg.get("twilio_token") else ""
    return jsonify(safe)

@app.route("/api/config/salvar", methods=["POST"])
@requer_login
def api_salvar_config():
    d=request.get_json() or {}; cfg=carregar_config()
    campos=["twilio_sid","twilio_numero","nome_evento","nome_organizador","chave_pix","telefone_contato",
            "data_sorteio","modo_envio","horario_envio","dia_semana","dia_mes","intervalo_min","intervalo_max",
            "limite_sessao","dias_disparo","usar_content_template","content_template_sid","url_publica",
            "session_timeout_horas"]
    for campo in campos:
        if campo in d: cfg[campo]=d[campo]
    if "twilio_token" in d and d["twilio_token"] and "****" not in str(d["twilio_token"]):
        cfg["twilio_token"]=d["twilio_token"]
    salvar_config(cfg); aplicar_config()
    log("Configurações salvas!","success")
    return jsonify({"ok":True,"msg":"Configurações salvas!"})

@app.route("/api/config/alterar-senha", methods=["POST"])
@requer_login
def api_alterar_senha():
    d=request.get_json() or {}; cfg=carregar_config()
    if d.get("senha_atual","")!=cfg.get("senha","admin123"): return jsonify({"ok":False,"msg":"Senha atual incorreta!"})
    nova=d.get("senha_nova","")
    if len(nova)<4: return jsonify({"ok":False,"msg":"Senha muito curta!"})
    if nova!=d.get("senha_conf",""): return jsonify({"ok":False,"msg":"Senhas não coincidem!"})
    cfg["senha"]=nova; salvar_config(cfg)
    return jsonify({"ok":True,"msg":"Senha alterada!"})

@app.route("/api/testar-twilio")
@requer_login
def api_testar_twilio():
    cfg=carregar_config()
    try:
        c=TwilioClient(cfg.get("twilio_sid",""),cfg.get("twilio_token",""))
        c.api.accounts(cfg.get("twilio_sid","")).fetch()
        return jsonify({"ok":True,"msg":f"SUCESSO!\nNúmero: {cfg.get('twilio_numero','')}"})
    except Exception as e: return jsonify({"ok":False,"msg":f"ERRO: {e}"})

@app.route("/api/qrcode/status")
@requer_login
def api_qrcode_status():
    return jsonify({"disponivel":QRCODE_OK})

@app.route("/api/qrcode/gerar", methods=["POST"])
@requer_login
def api_gerar_qrcode():
    if not QRCODE_OK: return jsonify({"ok":False,"msg":"qrcode não instalado"})
    d=request.get_json() or {}; cfg=carregar_config()
    chave=d.get("chave_pix",cfg.get("chave_pix","")); nome=d.get("beneficiario",cfg.get("nome_organizador",""))
    cidade=d.get("cidade","Brasil"); valor=d.get("valor","")
    try:
        payload=gerar_payload_pix(chave, nome, cidade, valor)
        os.makedirs(PASTA_QRCODES, exist_ok=True)
        fname=f"pix_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
        fpath=os.path.join(PASTA_QRCODES, fname)
        qrcode.make(payload).save(fpath)
        return jsonify({"ok":True,"arquivo":fname,"payload":payload,"url":f"/static/qrcodes/{fname}"})
    except Exception as e: return jsonify({"ok":False,"msg":str(e)})

# ══════════════════════════════════════════════════════════
#  API INBOX
# ══════════════════════════════════════════════════════════

@app.route("/api/inbox/resumo")
@requer_login
def api_inbox_resumo():
    inbox=carregar_inbox(); resumo={}; historico={}; total=0
    for numero,dados in inbox.items():
        msgs=dados.get("msgs",[])
        if msgs: historico[numero]=len(msgs)
        nao_lidas=sum(1 for m in msgs if not m.get("lida",False))
        if nao_lidas>0: resumo[numero]=nao_lidas; total+=nao_lidas
    return jsonify({"ok":True,"resumo":resumo,"historico":historico,"total":total})

@app.route("/api/inbox/relatorio")
@requer_login
def api_inbox_relatorio():
    """Retorna todas as conversas com mensagens completas para relatório por data."""
    inbox = carregar_inbox()
    # Carregar contatos para enriquecer com nome e intervalo
    contatos_map = {}
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT id, nome, telefone, whatsapp, lote, intervalo, status FROM contatos").fetchall()
            for r in rows:
                for tel in [r["telefone"] or "", r["whatsapp"] or ""]:
                    tel_limpo = numero_limpo(tel)
                    if tel_limpo:
                        contatos_map[tel_limpo] = {"nome": r["nome"] or "", "lote": r["lote"] or "", "intervalo": r["intervalo"] or "", "status": r["status"] or ""}
    except: pass

    resultado = {}
    for numero, dados in inbox.items():
        msgs = dados.get("msgs", [])
        if msgs:
            # Buscar contato pelo número (com variações)
            contato = contatos_map.get(numero, {})
            if not contato:
                for k, v in contatos_map.items():
                    if numeros_equivalentes(k, numero):
                        contato = v
                        break
            nome_exib = dados.get("nome", "") or contato.get("nome", "") or ""
            resultado[numero] = {
                "nome": nome_exib,
                "lote": contato.get("lote", ""),
                "intervalo": contato.get("intervalo", ""),
                "status": contato.get("status", ""),
                "msgs": msgs,
                "total": len(msgs),
                "nao_lidas": sum(1 for m in msgs if not m.get("lida", False))
            }
    return jsonify({"ok": True, "inbox": resultado})


@app.route("/api/inbox/conversa/<numero>")
@requer_login
def api_inbox_conversa(numero):
    inbox=carregar_inbox(); num_norm=numero_limpo(numero)
    # Buscar no inbox com variações do número (com/sem 9 extra)
    chave_inbox = num_norm
    if num_norm not in inbox:
        for k in inbox:
            if numeros_equivalentes(k, num_norm):
                chave_inbox = k
                break
    dados=dict(inbox.get(chave_inbox,{"msgs":[],"nome":""}))
    contato_id = request.args.get("id","").strip()
    try:
        with get_db() as conn:
            row = None
            if contato_id:
                row = conn.execute("SELECT id,nome,lote,intervalo,telefone,whatsapp,status FROM contatos WHERE id=?", (int(contato_id),)).fetchone()
            if not row:
                # fallback: busca por telefone, prioriza Pendente
                rows = conn.execute("SELECT id,nome,lote,intervalo,telefone,whatsapp,status FROM contatos").fetchall()
                matches = [dict(r) for r in rows if numeros_equivalentes(r["telefone"] or "",num_norm) or numeros_equivalentes(r["whatsapp"] or "",num_norm)]
                if matches:
                    matches.sort(key=lambda r: 0 if (r.get("status","") or "").lower()=="pendente" else 2 if (r.get("status","") or "").lower() in ("pago","disponivel") else 1)
                    row = matches[0]
            if row:
                r = dict(row)
                dados["nome"]=r["nome"]; dados["contato_id"]=r["id"]
                dados["linha_sheet"]=r["id"]; dados["status"]=r["status"]
                dados["outros_lotes"] = []
    except: pass
    for m in inbox.get(chave_inbox,{}).get("msgs",[]): m["lida"]=True
    if chave_inbox in inbox: salvar_inbox(inbox)
    return jsonify({"ok":True,"numero":num_norm,"dados":dados})

@app.route("/api/inbox/responder", methods=["POST"])
@requer_login
def api_inbox_responder():
    d=request.get_json() or {}; numero=(d.get("numero","") or "").strip(); mensagem=(d.get("mensagem","") or "").strip()
    if not numero or not mensagem: return jsonify({"ok":False,"erro":"Número e mensagem obrigatórios"})
    cfg=carregar_config(); sid=cfg.get("twilio_sid",""); token=cfg.get("twilio_token",""); from_num=cfg.get("twilio_numero","")
    if not all([sid,token,from_num]): return jsonify({"ok":False,"erro":"Twilio não configurado"})
    try:
        TwilioClient(sid,token).messages.create(body=mensagem, from_=from_num, to="whatsapp:+55"+numero_limpo(numero))
        inbox=carregar_inbox(); num_norm=numero_limpo(numero)
        if num_norm not in inbox: inbox[num_norm]={"msgs":[],"nome":""}
        inbox[num_norm]["msgs"].append({"de":"sistema","texto":mensagem,"midias":[],
            "hora":datetime.now().strftime("%d/%m/%Y %H:%M:%S"),"lida":True})
        salvar_inbox(inbox); return jsonify({"ok":True})
    except Exception as e: return jsonify({"ok":False,"erro":str(e)})

@app.route("/api/inbox/limpar/<numero>", methods=["POST"])
@requer_login
def api_inbox_limpar(numero):
    inbox=carregar_inbox(); num_norm=numero_limpo(numero)
    if num_norm in inbox: del inbox[num_norm]; salvar_inbox(inbox)
    return jsonify({"ok":True})

@app.route("/api/inbox/midia")
@requer_login
def api_inbox_midia():
    import requests as req
    from flask import Response, stream_with_context
    url = request.args.get("url", "")
    if not url or "twilio.com" not in url:
        return "URL inválida", 400
    cfg = carregar_config()
    try:
        r = req.get(
            url,
            auth=(cfg.get("twilio_sid",""), cfg.get("twilio_token","")),
            timeout=10,
            stream=True
        )
        content_type = r.headers.get("Content-Type", "application/octet-stream")
        # Para imagens e PDFs, retorna com streaming para não bloquear
        def generate():
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    yield chunk
        headers = {
            "Content-Type": content_type,
            "Cache-Control": "private, max-age=3600",
        }
        if "Content-Length" in r.headers:
            headers["Content-Length"] = r.headers["Content-Length"]
        return Response(stream_with_context(generate()), status=r.status_code, headers=headers)
    except req.exceptions.Timeout:
        return "Timeout ao carregar mídia", 504
    except Exception as e:
        return str(e), 500

# ══════════════════════════════════════════════════════════
#  API USUÁRIOS
# ══════════════════════════════════════════════════════════

# Lista completa de botões da sidebar com IDs e labels
TODOS_BOTOES = [
    {"id": "btn-novo-cadastro",     "label": "👤 Novo Cadastro"},
    {"id": "btn-manutencao-lotes",  "label": "🔧 Manutenção Lotes"},
    {"id": "btn-relatorios",        "label": "📁 Relatórios"},
    {"id": "btn-desmembrar",        "label": "🔀 Desmembrar Lotes"},
    {"id": "btn-preview",           "label": "👁 Preview da Mensagem"},
    {"id": "btn-teste-unico",       "label": "🧪 Teste Unitário"},
    {"id": "btn-disparo-teste",     "label": "🔬 Teste em Lote"},
    {"id": "btn-disparo-real",      "label": "🚀 Disparo REAL"},
    {"id": "btn-sidebar-agendamento","label": "⏰ Agendamento"},
    {"id": "btn-templates",         "label": "✏️ Templates de Mensagem"},
    {"id": "btn-qrcode",            "label": "📸 Gerar QR Code PIX"},
    {"id": "btn-twilio",            "label": "📱 Verificar Twilio"},
    {"id": "btn-configuracoes",     "label": "⚙️ Configurações do Sistema"},
    {"id": "btn-usuarios",          "label": "👥 Gerenciar Usuários"},
    {"id": "btn-manutencao-dados",  "label": "🗄️ Manutenção de Dados"},
    {"id": "btn-sorteio",           "label": "🎱 Sorteio"},
    {"id": "btn-conciliacao",       "label": "🏦 Conciliação Bancária"},
    {"id": "btn-camisetas",         "label": "👕 Camisetas"},
    {"id": "btn-encerrar",          "label": "⏻ Encerrar Sistema"},
]
TODOS_BOTOES_IDS = [b["id"] for b in TODOS_BOTOES]

PERFIS = {
    "admin":        {"label":"Administrador","cor":"#f59e0b"},
    "operador":     {"label":"Operador",      "cor":"#3b82f6"},
    "visualizador": {"label":"Visualizador",  "cor":"#10b981"},
}
PERMISSOES = {
    "admin":        ["dashboard","relatorio","templates","configuracoes","disparar","agendar","usuarios"],
    "operador":     ["dashboard","relatorio","templates","disparar","agendar"],
    "visualizador": ["dashboard","relatorio"],
}

def usuario_tem_permissao(permissao):
    return permissao in PERMISSOES.get(session.get("perfil",""), [])

@app.route("/api/auth/eu")
@requer_login
def api_auth_eu():
    perfil = session.get("perfil","")
    if perfil == "admin":
        botoes = TODOS_BOTOES_IDS
    else:
        botoes = session.get("permissoes") or []
        if not botoes:
            usuarios = carregar_usuarios()
            u = usuarios.get(session.get("usuario",""), {})
            botoes = u.get("permissoes", [])
    return jsonify({
        "usuario":      session.get("usuario"),
        "nome":         session.get("nome"),
        "perfil":       perfil,
        "permissoes":   PERMISSOES.get(perfil,[]),
        "botoes":       botoes,
        "todos_botoes": TODOS_BOTOES,
        "perfil_label": PERFIS.get(perfil,{}).get("label",""),
        "perfil_cor":   PERFIS.get(perfil,{}).get("cor","#64748b"),
        "trocar_senha": bool(session.get("trocar_senha")),
    })

@app.route("/api/usuarios/listar")
@requer_login
def api_listar_usuarios():
    if not usuario_tem_permissao("usuarios"):
        return jsonify({"ok":False,"erro":"Sem permissão"}), 403
    usuarios = carregar_usuarios()
    lista = [{"usuario":u,"nome":d.get("nome",u),"perfil":d.get("perfil","visualizador"),
              "permissoes": d.get("permissoes",[]),
              "ativo": d.get("ativo", True),
              "telefone": d.get("telefone",""),
              "perfil_label":PERFIS.get(d.get("perfil",""),{}).get("label",""),
              "perfil_cor":PERFIS.get(d.get("perfil",""),{}).get("cor","#64748b")}
             for u,d in usuarios.items()]
    return jsonify({"ok":True,"usuarios":lista,"todos_botoes":TODOS_BOTOES})

@app.route("/api/usuarios/salvar", methods=["POST"])
@requer_login
def api_salvar_usuario():
    if not usuario_tem_permissao("usuarios"):
        return jsonify({"ok":False,"msg":"Sem permissão"}), 403
    d = request.get_json() or {}
    usuario = d.get("usuario","").strip().lower()
    nome    = d.get("nome","").strip()
    perfil  = d.get("perfil","visualizador")
    senha   = d.get("senha","").strip()
    if not usuario or not nome: return jsonify({"ok":False,"msg":"Usuário e nome são obrigatórios!"})
    if perfil not in PERFIS:    return jsonify({"ok":False,"msg":"Perfil inválido!"})
    usuarios = carregar_usuarios()
    novo = usuario not in usuarios
    if novo and not senha: return jsonify({"ok":False,"msg":"Senha obrigatória para novo usuário!"})
    if novo: usuarios[usuario] = {}
    permissoes = d.get("permissoes", None)
    usuarios[usuario]["nome"]   = nome
    usuarios[usuario]["perfil"] = perfil
    if senha: usuarios[usuario]["senha"] = _hash(senha)
    if permissoes is not None: usuarios[usuario]["permissoes"] = permissoes
    usuarios[usuario]["telefone"] = sanitizar_telefone(d.get("telefone","") or "")
    if novo: usuarios[usuario]["trocar_senha"] = True  # força troca no primeiro login
    salvar_usuarios(usuarios)
    log(("Usuário criado" if novo else "Usuário atualizado")+": "+usuario,"success")
    return jsonify({"ok":True,"msg":("Usuário criado" if novo else "Usuário atualizado")+"!"})

@app.route("/api/usuarios/reabilitar", methods=["POST"])
@requer_login
def api_reabilitar_usuario():
    if not usuario_tem_permissao("usuarios"):
        return jsonify({"ok":False,"msg":"Sem permissão"}), 403
    d = request.get_json() or {}
    usuario = d.get("usuario","").strip().lower()
    usuarios = carregar_usuarios()
    if usuario not in usuarios:
        return jsonify({"ok":False,"msg":"Usuário não encontrado!"})
    usuarios[usuario]["ativo"] = True
    salvar_usuarios(usuarios)
    log(f"Usuário reabilitado: {usuario}", "success")
    return jsonify({"ok":True})

@app.route("/api/usuarios/remover", methods=["POST"])
@requer_login
def api_remover_usuario():
    if not usuario_tem_permissao("usuarios"):
        return jsonify({"ok":False,"msg":"Sem permissão"}), 403
    d = request.get_json() or {}
    usuario = d.get("usuario","").strip().lower()
    if usuario == session.get("usuario"):
        return jsonify({"ok":False,"msg":"Não é possível remover o próprio usuário!"})
    usuarios = carregar_usuarios()
    if usuario not in usuarios:
        return jsonify({"ok":False,"msg":"Usuário não encontrado!"})
    admins = [u for u,dd in usuarios.items() if dd.get("perfil")=="admin" and u!=usuario and dd.get("ativo",True)]
    if not admins and usuarios.get(usuario,{}).get("perfil")=="admin":
        return jsonify({"ok":False,"msg":"Não é possível remover o único administrador ativo!"})
    # Verifica se há registros de auditoria deste usuário
    try:
        with get_db() as conn:
            tem_log = conn.execute("SELECT 1 FROM auditoria WHERE usuario=? LIMIT 1", (usuario,)).fetchone()
    except:
        tem_log = None
    if tem_log:
        # Tem histórico — desabilita em vez de excluir
        usuarios[usuario]["ativo"] = False
        salvar_usuarios(usuarios)
        log(f"Usuário desabilitado: {usuario}", "warning")
        return jsonify({"ok":True, "desabilitado":True, "msg":f"Usuário \"{usuarios[usuario]['nome']}\" desabilitado (possui histórico de atividades)."})
    else:
        # Sem histórico — pode excluir
        del usuarios[usuario]
        salvar_usuarios(usuarios)
        log("Usuário removido: "+usuario, "warning")
        return jsonify({"ok":True, "desabilitado":False})

@app.route("/api/usuarios/alterar-senha", methods=["POST"])
@requer_login
def api_alterar_minha_senha():
    d = request.get_json() or {}
    senha_atual = d.get("senha_atual",""); senha_nova = d.get("senha_nova","")
    if len(senha_nova) < 6: return jsonify({"ok":False,"msg":"A nova senha deve ter pelo menos 6 caracteres!"})
    usuarios = carregar_usuarios(); u = session.get("usuario")
    if usuarios.get(u,{}).get("senha") != _hash(senha_atual):
        return jsonify({"ok":False,"msg":"Senha atual incorreta!"})
    usuarios[u]["senha"] = _hash(senha_nova)
    usuarios[u]["trocar_senha"] = False  # limpa flag de primeiro login
    salvar_usuarios(usuarios)
    session["trocar_senha"] = False
    return jsonify({"ok":True,"msg":"Senha alterada com sucesso!"})

# ══════════════════════════════════════════════════════════
#  API TEMPLATES
# ══════════════════════════════════════════════════════════

PASTA_TEMPLATES = os.path.join(APP_DIR, "templates_msg")
PASTA_IMAGENS   = os.path.join(APP_DIR, "static", "imagens")
def _carregar_template_ativo():
    try:
        cfg = carregar_config()
        return cfg.get("template_ativo", "padrao") or "padrao"
    except: return "padrao"

TEMPLATE_ATIVO  = _carregar_template_ativo()
TEMPLATE_PADRAO = {
    "nome":"padrao","titulo":"Template Padrão",
    "texto":("Olá {nome}! 🎱\n\nVocê está participando do *{evento}*!\n\n"
             "🎟️ Lote: *{lote}*\n📋 Cartelas: *{intervalo}*\n💰 Valor: *{valor}*\n\n"
             "💳 *Pagamento via PIX:*\n`{chave_pix}`\nBeneficiário: {beneficiario}\n\n"
             "📅 Sorteio: *{data_sorteio}*\n\nDúvidas? Fale com {vendedor}. Boa sorte! 🍀"),
    "imagem":"","rodape":"","qrcode":""
}

def carregar_template(nome):
    os.makedirs(PASTA_TEMPLATES, exist_ok=True)
    path = os.path.join(PASTA_TEMPLATES, nome+".json")
    try:
        with open(path) as f: return json.load(f)
    except: return dict(TEMPLATE_PADRAO)

def salvar_template_arquivo(data):
    os.makedirs(PASTA_TEMPLATES, exist_ok=True)
    nome = re.sub(r"[^a-z0-9_]","_", data.get("nome","padrao").lower())
    data["nome"] = nome
    with open(os.path.join(PASTA_TEMPLATES, nome+".json"),"w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return nome

def listar_templates():
    os.makedirs(PASTA_TEMPLATES, exist_ok=True)
    lista = []
    # Garante que padrão existe
    padrao_path = os.path.join(PASTA_TEMPLATES,"padrao.json")
    if not os.path.exists(padrao_path):
        with open(padrao_path,"w") as f: json.dump(TEMPLATE_PADRAO, f, indent=2, ensure_ascii=False)
    for f in sorted(os.listdir(PASTA_TEMPLATES)):
        if f.endswith(".json"):
            try:
                with open(os.path.join(PASTA_TEMPLATES,f)) as fp:
                    t = json.load(fp)
                    lista.append({"nome":t.get("nome",f[:-5]),"titulo":t.get("titulo",f[:-5]),
                                  "imagem":t.get("imagem",""),"rodape":t.get("rodape","")})
            except: pass
    return lista

@app.route("/api/templates")
@requer_login
def api_listar_templates():
    return jsonify({"ok":True,"templates":listar_templates(),"ativo":TEMPLATE_ATIVO})

@app.route("/api/templates/ativar", methods=["POST"])
@requer_login
def api_ativar_template():
    global TEMPLATE_ATIVO
    d = request.get_json() or {}
    TEMPLATE_ATIVO = d.get("nome","padrao")
    cfg = carregar_config(); cfg["template_ativo"] = TEMPLATE_ATIVO; salvar_config(cfg)
    log("Template ativo: "+TEMPLATE_ATIVO,"success")
    return jsonify({"ok":True,"ativo":TEMPLATE_ATIVO})

@app.route("/api/templates/salvar", methods=["POST"])
@requer_login
def api_salvar_template():
    d = request.get_json() or {}
    if not d.get("nome") or not d.get("texto"):
        return jsonify({"ok":False,"msg":"Nome e texto são obrigatórios!"})
    nome_arq = re.sub(r"[^a-z0-9_]","_",d.get("nome","").lower())
    existente = carregar_template(nome_arq)
    if "imagem" not in d: d["imagem"] = existente.get("imagem","")
    if "qrcode" not in d: d["qrcode"] = existente.get("qrcode","")
    if "rodape" not in d: d["rodape"] = existente.get("rodape","")
    nome = salvar_template_arquivo(d)
    log("Template salvo: "+nome,"success")
    return jsonify({"ok":True,"nome":nome})

@app.route("/api/templates/deletar", methods=["POST"])
@requer_login
def api_deletar_template():
    global TEMPLATE_ATIVO
    d = request.get_json() or {}
    nome = d.get("nome","")
    if nome == "padrao": return jsonify({"ok":False,"msg":"Não é possível deletar o template padrão!"})
    path = os.path.join(PASTA_TEMPLATES, nome+".json")
    if os.path.exists(path):
        os.remove(path)
        if TEMPLATE_ATIVO == nome: TEMPLATE_ATIVO = "padrao"
        log("Template deletado: "+nome,"warning")
        return jsonify({"ok":True})
    return jsonify({"ok":False,"msg":"Template não encontrado!"})

@app.route("/api/templates/upload-imagem", methods=["POST"])
@requer_login
def api_upload_imagem():
    nome_template = request.form.get("template","padrao")
    tipo          = request.form.get("tipo","banner")
    if "imagem" not in request.files: return jsonify({"ok":False,"msg":"Nenhuma imagem enviada!"})
    arquivo = request.files["imagem"]
    ext = arquivo.filename.rsplit(".",1)[-1].lower()
    if ext not in ["jpg","jpeg","png"]: return jsonify({"ok":False,"msg":"Apenas JPG e PNG!"})
    os.makedirs(PASTA_IMAGENS, exist_ok=True)
    nome_arq = nome_template+"_"+tipo+"."+ext
    arquivo.save(os.path.join(PASTA_IMAGENS, nome_arq))
    t = carregar_template(nome_template)
    t["rodape" if tipo=="rodape" else "imagem"] = nome_arq
    salvar_template_arquivo(t)
    log("Imagem ("+tipo+") salva: "+nome_arq,"success")
    return jsonify({"ok":True,"imagem":nome_arq,"tipo":tipo})

@app.route("/api/templates/definir-rodape", methods=["POST"])
@requer_login
def api_definir_rodape():
    d = request.get_json() or {}
    tpl = carregar_template(d.get("template", TEMPLATE_ATIVO))
    tpl["rodape"] = d.get("rodape",""); tpl["qrcode"] = d.get("rodape","")
    salvar_template_arquivo(tpl)
    log("Rodapé vinculado ao template","success")
    return jsonify({"ok":True})

@app.route("/api/templates/preview", methods=["POST"])
@requer_login
def api_templates_preview():
    d = request.get_json() or {}; texto = d.get("texto","")
    cfg = carregar_config()
    try:
        preview = texto.format(nome="Maria da Silva",lote="001",intervalo="001-010",
            vendedor="João Vendedor",valor="R$ 50,00",chave_pix=cfg.get("chave_pix",""),
            beneficiario=cfg.get("nome_organizador",""),data_sorteio=cfg.get("data_sorteio",""),
            evento=cfg.get("nome_evento",""))
        return jsonify({"ok":True,"preview":preview})
    except KeyError as e: return jsonify({"ok":False,"msg":"Variável desconhecida: "+str(e)})
    except Exception as e: return jsonify({"ok":False,"msg":str(e)})

@app.route("/api/templates/preview-midias")
@requer_login
def api_preview_midias():
    tpl = carregar_template(TEMPLATE_ATIVO)
    banner = tpl.get("imagem",""); rodape = tpl.get("rodape","") or tpl.get("qrcode","")
    rodape_url = "/static/imagens/"+rodape if rodape and os.path.exists(os.path.join(PASTA_IMAGENS,rodape)) else \
                 "/static/qrcodes/"+rodape if rodape and os.path.exists(os.path.join(PASTA_QRCODES,rodape)) else ""
    return jsonify({"banner":"/static/imagens/"+banner if banner and os.path.exists(os.path.join(PASTA_IMAGENS,banner)) else "",
                    "qrcode":rodape_url,"tem_banner":bool(banner),"tem_rodape":bool(rodape_url)})

@app.route("/api/templates/converter-meta")
@requer_login
def api_converter_para_meta():
    tpl = carregar_template(TEMPLATE_ATIVO)
    texto = tpl.get("texto", TEMPLATE_PADRAO["texto"])
    mapa = [("{nome}","{{1}}"),("{lote}","{{2}}"),("{intervalo}","{{3}}"),("{valor}","{{4}}"),
            ("{chave_pix}","{{5}}"),("{beneficiario}","{{6}}"),("{data_sorteio}","{{7}}"),
            ("{vendedor}","{{8}}"),("{evento}","{{9}}")]
    texto_meta = texto
    for var,num in mapa: texto_meta = texto_meta.replace(var, num)
    return jsonify({"ok":True,"texto_meta":texto_meta,"template":tpl.get("titulo",TEMPLATE_ATIVO)})

# ══════════════════════════════════════════════════════════
#  API RESUMO / DRILLDOWN / RELATÓRIOS
# ══════════════════════════════════════════════════════════

def fmt_brl(v):
    return "R$ {:,.2f}".format(v).replace(",","X").replace(".",",").replace("X",".")

@app.route("/api/admin/corrigir-status", methods=["POST"])
@requer_login
def api_corrigir_status():
    """Corrige registros com nome preenchido mas status Disponivel — devem ser Pendente"""
    with get_db() as conn:
        resultado = conn.execute(
            "SELECT COUNT(*) FROM contatos WHERE TRIM(nome) != '' AND LOWER(TRIM(status)) = 'disponivel'"
        ).fetchone()[0]
        conn.execute(
            "UPDATE contatos SET status='Pendente' WHERE TRIM(nome) != '' AND LOWER(TRIM(status)) = 'disponivel'"
        )
        log(f"Correção de status: {resultado} registros alterados de Disponivel para Pendente", "warning")
    return jsonify({"ok": True, "corrigidos": resultado})

@app.route("/api/dados/editar", methods=["POST"])
@requer_login
def api_dados_editar():
    d = request.get_json() or {}
    id_reg   = d.get("id")
    nome     = (d.get("nome") or "").strip().upper()
    telefone = sanitizar_telefone(d.get("telefone"))
    vendedor = (d.get("vendedor") or "").strip().upper()
    valor    = (d.get("valor") or "").strip()
    status   = (d.get("status") or "").strip()
    obs      = (d.get("observacoes") or "").strip()
    prev     = (d.get("previsao_pagamento") or "").strip()
    data_pgto  = (d.get("data_pagamento") or "").strip()
    forma_pgto = (d.get("forma_pagamento") or "").strip()
    if not id_reg:
        return jsonify({"ok": False, "msg": "ID não informado"})
    # Regra: sem nome → Disponivel; com nome → nunca pode ser Disponivel
    if not nome:
        status = "Disponivel"
    elif status == "Disponivel":
        status = "Pendente"
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    with get_db() as conn:
        existe = conn.execute("SELECT id, status, lote, intervalo, nome FROM contatos WHERE id=?", (id_reg,)).fetchone()
        if not existe:
            return jsonify({"ok": False, "msg": "Registro não encontrado"})
        status_anterior = existe["status"]
        lote_reg     = existe["lote"] or ""
        intervalo_reg= existe["intervalo"] or ""
        conn.execute("""UPDATE contatos SET nome=?, telefone=?, whatsapp=?, vendedor=?, valor=?,
            status=?, observacoes=?, previsao_pagamento=?, data_pagamento=?, forma_pagamento=?, atualizado_em=? WHERE id=?""",
            (nome, telefone, telefone, vendedor, valor, status, obs, prev, data_pgto, forma_pgto, now, id_reg))
    # Audita fora do with para evitar conflito de conexão SQLite
    detalhes = f"nome={nome}, tel={telefone}, vendedor={vendedor}, valor={valor}"
    auditar("EDICAO", contato_id=id_reg, lote=lote_reg, intervalo=intervalo_reg,
            nome=nome, telefone=telefone,
            status_de=status_anterior, status_para=status,
            detalhes=detalhes)
    log(f"Registro {id_reg} editado por {session.get('usuario','?')}: {status_anterior}→{status}", "info")
    return jsonify({"ok": True})

@app.route("/api/dados/previsao", methods=["POST"])
@requer_login
def api_dados_previsao():
    d = request.get_json() or {}
    id_reg = d.get("id")
    prev   = (d.get("previsao_pagamento") or "").strip()
    obs    = (d.get("observacoes") or "").strip()
    if not id_reg:
        return jsonify({"ok": False, "msg": "ID não informado"})
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    with get_db() as conn:
        conn.execute("UPDATE contatos SET previsao_pagamento=?, observacoes=?, atualizado_em=? WHERE id=?",
                     (prev, obs, now, id_reg))
        conn.execute("""INSERT INTO auditoria (acao, contato_id, usuario, detalhes, criado_em)
            VALUES (?,?,?,?,?)""",
            ("PREVISAO", id_reg, session.get("usuario","?"),
             f"Previsão: {prev} | Obs: {obs}", now))
    return jsonify({"ok": True})

@app.route("/api/dados/detalhes/<int:id_reg>")
@requer_login
def api_dados_detalhes(id_reg):
    with get_db() as conn:
        contato = conn.execute("SELECT * FROM contatos WHERE id=?", (id_reg,)).fetchone()
        if not contato:
            return jsonify({"ok": False, "msg": "Não encontrado"})
        envios = conn.execute(
            "SELECT * FROM log_envios WHERE contato_id=? ORDER BY criado_em DESC LIMIT 20",
            (id_reg,)).fetchall()
        auditoria = conn.execute(
            "SELECT * FROM auditoria WHERE contato_id=? ORDER BY id DESC LIMIT 30",
            (id_reg,)).fetchall()
    return jsonify({
        "ok": True,
        "contato":   dict(contato),
        "envios":    [dict(e) for e in envios],
        "auditoria": [dict(a) for a in auditoria],
    })

@app.route("/api/resumo")
@requer_login
def api_resumo():
    def parse_valor(v):
        if not v: return 0.0
        try:
            v = str(v).replace("R$","").replace(".","").replace(",",".").strip()
            return float(v)
        except: return 0.0
    with get_db() as conn:
        # Exclui desmembrados das contagens
        # Lotes normais = sem origem_id e não desmembrado
        # Cartelas unitárias = com origem_id (desmembradas)
        total_lotes    = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status) != 'desmembrado' AND origem_id IS NULL").fetchone()[0]
        total_cartelas_unit = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status) != 'desmembrado' AND origem_id IS NOT NULL").fetchone()[0]
        pagos       = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status)='pago' AND origem_id IS NULL").fetchone()[0]
        pagos_unit  = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status)='pago' AND origem_id IS NOT NULL").fetchone()[0]
        pendentes   = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status)='pendente' AND origem_id IS NULL").fetchone()[0]
        pendentes_unit = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status)='pendente' AND origem_id IS NOT NULL").fetchone()[0]
        disponiveis = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status)='disponivel' AND origem_id IS NULL").fetchone()[0]
        disponiveis_unit = conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status)='disponivel' AND origem_id IS NOT NULL").fetchone()[0]
        desmembrados= conn.execute("SELECT COUNT(*) FROM contatos WHERE LOWER(status)='desmembrado'").fetchone()[0]
        outros      = total_lotes - pagos - pendentes - disponiveis
        rows_pago   = conn.execute("SELECT valor FROM contatos WHERE LOWER(status)='pago'").fetchall()
        rows_pend   = conn.execute("SELECT valor FROM contatos WHERE LOWER(status)='pendente'").fetchall()
        rows_disp   = conn.execute("SELECT valor FROM contatos WHERE LOWER(status)='disponivel'").fetchall()
        rows_all    = conn.execute("SELECT valor FROM contatos WHERE LOWER(status) != 'desmembrado'").fetchall()
    cfg = carregar_config()
    cartelas_por_lote = int(cfg.get("cartelas_por_lote", 10) or 10)
    # Total de cartelas = lotes normais * cartelas_por_lote + cartelas unitárias
    total = total_lotes  # para compatibilidade dos cards de lotes
    pct = round(pagos/total*100,1) if total>0 else 0
    val_pago       = sum(parse_valor(r[0]) for r in rows_pago)
    val_pendente   = sum(parse_valor(r[0]) for r in rows_pend)
    val_disponivel = sum(parse_valor(r[0]) for r in rows_disp)
    val_total      = sum(parse_valor(r[0]) for r in rows_all)
    return jsonify({
        "total":total_lotes, "pagos":pagos, "pendentes":pendentes, "disponiveis":disponiveis,
        "outros":outros, "invalidos":0, "disparar":pendentes, "pct_arrecadado":pct,
        "desmembrados": desmembrados,
        # Contagens de cartelas unitárias (desmembradas)
        "cartelas_unit_total":     total_cartelas_unit,
        "cartelas_unit_pagos":     pagos_unit,
        "cartelas_unit_pendentes": pendentes_unit,
        "cartelas_unit_disponiveis": disponiveis_unit,
        "evento":cfg.get("nome_evento",""), "data_sorteio":cfg.get("data_sorteio",""),
        "gerado_em":datetime.now().strftime("%d/%m/%Y %H:%M"),
        "val_lote":      cfg.get("valor_lote",""),
        "val_pago":      fmt_brl(val_pago),
        "val_pendente":  fmt_brl(val_pendente),
        "val_disponivel":fmt_brl(val_disponivel),
        "val_total":     fmt_brl(val_total),
        "ok": True,
    })

@app.route("/api/drilldown")
@requer_login
def api_drilldown():
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM contatos WHERE LOWER(status) != 'desmembrado' ORDER BY CAST(lote AS INTEGER),id"
        ).fetchall()]
    vendedores = {}; lotes_dict = {}
    for c in rows:
        v = (c.get("vendedor") or "Sem vendedor").strip()
        if v not in vendedores: vendedores[v] = {"vendedor":v,"total":0,"pagos":0,"pendentes":0,"val_pago":0,"compradores":[]}
        vendedores[v]["total"] += 1
        st = (c.get("status") or "").lower()
        if st == "pago":     vendedores[v]["pagos"] += 1
        elif st == "pendente": vendedores[v]["pendentes"] += 1
        vendedores[v]["compradores"].append({"nome":c["nome"],"lote":c.get("lote",""),"intervalo":c.get("intervalo",""),"status":c.get("status","")})
        l = (c.get("lote") or "Sem lote").strip()
        if l not in lotes_dict: lotes_dict[l] = {"lote":l,"total":0,"pagos":0,"pendentes":0,"compradores":[]}
        lotes_dict[l]["total"] += 1
        if st == "pago": lotes_dict[l]["pagos"] += 1
        elif st == "pendente": lotes_dict[l]["pendentes"] += 1
        lotes_dict[l]["compradores"].append({"nome":c["nome"],"vendedor":c.get("vendedor",""),"intervalo":c.get("intervalo",""),"status":c.get("status","")})
    return jsonify({"ok":True,"por_status":rows,
        "por_vendedor":sorted(vendedores.values(),key=lambda x:x["pagos"],reverse=True),
        "por_lote":sorted(lotes_dict.values(),key=lambda x:x["lote"]),
        "gerado_em":datetime.now().strftime("%d/%m/%Y %H:%M"),"evento":carregar_config().get("nome_evento","")})

@app.route("/api/disparar/simulacao")
@requer_login
def api_disparar_simulacao():
    """Simula quem seria disparado na data informada com base na regra de Dias Para Disparo."""
    from datetime import datetime as _dt
    cfg  = carregar_config()
    n    = int(cfg.get("dias_disparo", 0) or 0)
    data_param = request.args.get("data", "").strip()
    try:
        hoje = _dt.strptime(data_param, "%Y-%m-%d").date() if data_param else _dt.now().date()
    except:
        hoje = _dt.now().date()

    def parse_data(s):
        s = (s or "").strip()
        if not s: return None
        try:
            if len(s) >= 10 and s[2] == "/":
                return _dt.strptime(s[:10], "%d/%m/%Y").date()
            return _dt.strptime(s[:10], "%Y-%m-%d").date()
        except: return None

    def dias_atras(d):
        if not d: return None
        return (hoje - d).days

    with get_db() as conn:
        rows = conn.execute("SELECT * FROM contatos ORDER BY CAST(lote AS INTEGER), id").fetchall()
        envios = conn.execute(
            "SELECT contato_id, criado_em as ultimo FROM log_envios "
            "WHERE status='ENVIADO' AND id IN ("
            "  SELECT MAX(id) FROM log_envios WHERE status='ENVIADO' GROUP BY contato_id"
            ")"
        ).fetchall()

    ultimo_envio = {r["contato_id"]: r["ultimo"] for r in envios}
    disparar  = []
    ignorados = []

    for row in rows:
        c = dict(row)
        status = (c.get("status") or "").strip().lower()

        if status != "pendente":
            continue

        d_cadastro = parse_data(c.get("criado_em"))
        d_ultimo   = parse_data(ultimo_envio.get(c["id"]))
        d_prev     = parse_data(c.get("previsao_pagamento"))

        dias_cad  = dias_atras(d_cadastro)
        dias_ult  = dias_atras(d_ultimo)
        dias_prev = dias_atras(d_prev)

        # Datas formatadas para exibição
        str_cad  = d_cadastro.strftime("%d/%m/%Y") if d_cadastro else None
        str_ult  = d_ultimo.strftime("%d/%m/%Y")   if d_ultimo   else None
        str_prev = d_prev.strftime("%d/%m/%Y")     if d_prev     else None
        def _append(lista, motivo, ref_campo=None, ref_dias=None):
            lista.append({**c,
                "motivo": motivo,
                "dias_cadastro": dias_cad, "dias_ultimo_disparo": dias_ult,
                "dias_prev_pgto": dias_prev,
                "data_cadastro_fmt": str_cad, "data_ultimo_fmt": str_ult, "data_prev_fmt": str_prev,
                "data_referencia": ref_campo or "",
                "data_referencia_dias": ref_dias or 0})

        # BARREIRA: previsão não venceu
        if d_prev and dias_prev <= 0:
            _append(ignorados,
                f"Prev. pgto nao venceu ({'vence hoje' if dias_prev == 0 else f'vence em {abs(dias_prev)} dia(s)'})",
                "prev. pagamento (barreira)", dias_prev)
            continue

        # Previsão existe e venceu
        if d_prev:
            if d_ultimo and d_ultimo >= d_prev:
                # Já disparou após vencimento — aplicar N dias
                if n == 0:
                    _append(disparar, "N=0 — sem restricao de dias", "ultimo disparo", dias_ult)
                elif dias_ult > n:
                    _append(disparar, f"Ultimo disparo ha {dias_ult}d > N={n}", "ultimo disparo", dias_ult)
                else:
                    _append(ignorados, f"Ultimo disparo ha {dias_ult}d <= N={n} — aguarda", "ultimo disparo", dias_ult)
            else:
                # Primeiro disparo após vencimento
                _append(disparar, f"Primeiro disparo apos vencimento da prev. pgto ({str_prev})", "prev. pagamento", dias_prev)
            continue

        # Sem previsão
        if n == 0:
            _append(disparar, "N=0 — sem restricao de dias")
            continue

        if dias_ult is not None:
            ref_campo, ref_dias = "ultimo disparo", dias_ult
        elif dias_cad is not None:
            ref_campo, ref_dias = "cadastro", dias_cad
        else:
            _append(ignorados, "Sem datas validas para comparacao")
            continue

        if ref_dias > n:
            _append(disparar, f"Ref: {ref_campo} ({ref_dias}d) > N={n}", ref_campo, ref_dias)
        else:
            _append(ignorados, f"Ref: {ref_campo} ({ref_dias}d) <= N={n} — aguarda", ref_campo, ref_dias)

    return jsonify({
        "ok": True,
        "n": n,
        "hoje": hoje.strftime("%d/%m/%Y"),
        "total_pendentes": len(disparar) + len(ignorados),
        "total_disparar": len(disparar),
        "total_ignorar": len(ignorados),
        "disparar": disparar,
        "ignorados": ignorados
    })

# ══════════════════════════════════════════════════════════
#  SORTEIO
# ══════════════════════════════════════════════════════════

@app.route("/api/premios", methods=["GET"])
@requer_login
def api_premios_listar():
    with get_db() as conn:
        rows = conn.execute("SELECT id, nome, tipo_batida, descricao, tem_foto FROM premios ORDER BY id DESC").fetchall()
    return jsonify({"ok": True, "premios": [dict(r) for r in rows]})

@app.route("/api/premios", methods=["POST"])
@requer_login
def api_premios_salvar():
    d = request.get_json() or {}
    nome      = (d.get("nome") or "").strip()
    tipo      = (d.get("tipo_batida") or "cartela_cheia").strip()
    descricao = (d.get("descricao") or "").strip()
    foto_b64  = (d.get("foto_base64") or "").strip()
    pid       = d.get("id")
    if not nome:
        return jsonify({"ok": False, "msg": "Nome do prêmio é obrigatório"})
    tem_foto  = 1 if foto_b64 else 0
    with get_db() as conn:
        if pid:
            # Edição
            if foto_b64:
                conn.execute("UPDATE premios SET nome=?,tipo_batida=?,descricao=?,foto_base64=?,tem_foto=1 WHERE id=?",
                             (nome, tipo, descricao, foto_b64, pid))
            else:
                conn.execute("UPDATE premios SET nome=?,tipo_batida=?,descricao=? WHERE id=?",
                             (nome, tipo, descricao, pid))
            msg = "Prêmio atualizado!"
        else:
            cur = conn.execute("INSERT INTO premios (nome, tipo_batida, descricao, foto_base64, tem_foto) VALUES (?,?,?,?,?)",
                               (nome, tipo, descricao, foto_b64, tem_foto))
            pid = cur.lastrowid
            msg = "Prêmio cadastrado!"
    return jsonify({"ok": True, "id": pid, "msg": msg})

@app.route("/api/premios/<int:pid>", methods=["DELETE"])
@requer_login
def api_premios_deletar(pid):
    with get_db() as conn:
        conn.execute("DELETE FROM premios WHERE id=?", (pid,))
    return jsonify({"ok": True, "msg": "Prêmio removido!"})

@app.route("/api/premios/<int:pid>/foto")
def api_premios_foto(pid):
    """Retorna a foto do prêmio como imagem — sem login para uso no telão."""
    with get_db() as conn:
        row = conn.execute("SELECT foto_base64 FROM premios WHERE id=?", (pid,)).fetchone()
    if not row or not row["foto_base64"]:
        return "", 404
    import base64 as _b64
    try:
        b64 = row["foto_base64"]
        if "," in b64:
            header, data = b64.split(",", 1)
            mime = header.split(":")[1].split(";")[0] if ":" in header else "image/jpeg"
        else:
            data = b64
            mime = "image/jpeg"
        img_bytes = _b64.b64decode(data)
        from flask import Response
        return Response(img_bytes, mimetype=mime)
    except:
        return "", 400

@app.route("/api/sorteio", methods=["GET"])
@requer_login
def api_sorteio_listar():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM sorteios ORDER BY id DESC").fetchall()
    return jsonify({"ok": True, "sorteios": [dict(r) for r in rows]})

@app.route("/api/sorteio", methods=["POST"])
@requer_login
def api_sorteio_criar():
    d = request.get_json() or {}
    nome     = (d.get("nome") or "").strip()
    data     = (d.get("data") or "").strip()
    logo_b64 = (d.get("logo_base64") or "").strip()
    premios  = d.get("premios", [])
    if not nome:
        return jsonify({"ok": False, "msg": "Nome do sorteio é obrigatório"})
    if not premios:
        return jsonify({"ok": False, "msg": "Cadastre ao menos um prêmio"})
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO sorteios (nome, data, status, logo_base64) VALUES (?,?,?,?)",
            (nome, data, "ativo", logo_b64)
        )
        sorteio_id = cur.lastrowid
        for i, p in enumerate(premios):
            foto = p.get("foto_base64") or ""
            conn.execute(
                "INSERT INTO sorteio_premios (sorteio_id, ordem, nome, tipo_batida, status, cartela_intervalo, foto_base64, tem_foto) VALUES (?,?,?,?,?,?,?,?)",
                (sorteio_id, i+1, p.get("nome",""), p.get("tipo_batida","cartela_cheia"),
                 "aguardando", p.get("cartela_intervalo",""), foto, 1 if foto else 0)
            )
    log(f"Sorteio '{nome}' criado por {session.get('usuario','?')}", "success")
    return jsonify({"ok": True, "id": sorteio_id, "msg": "Sorteio criado!"})

@app.route("/api/sorteio/<int:sid>", methods=["GET"])
@requer_login
def api_sorteio_get(sid):
    with get_db() as conn:
        s = conn.execute("SELECT * FROM sorteios WHERE id=?", (sid,)).fetchone()
        if not s:
            return jsonify({"ok": False, "msg": "Sorteio não encontrado"})
        premios  = conn.execute("SELECT * FROM sorteio_premios WHERE sorteio_id=? ORDER BY ordem", (sid,)).fetchall()
        numeros  = conn.execute("SELECT * FROM sorteio_numeros WHERE sorteio_id=? ORDER BY ordem", (sid,)).fetchall()
        ganhadores = conn.execute(
            "SELECT sg.*, sp.nome as premio_nome, sp.tipo_batida FROM sorteio_ganhadores sg "
            "JOIN sorteio_premios sp ON sg.premio_id=sp.id WHERE sg.sorteio_id=? ORDER BY sg.id", (sid,)
        ).fetchall()
    return jsonify({
        "ok": True,
        "sorteio": dict(s),
        "premios": [dict(p) for p in premios],
        "numeros": [dict(n) for n in numeros],
        "ganhadores": [dict(g) for g in ganhadores]
    })

@app.route("/api/sorteio/<int:sid>/estado")
def api_sorteio_estado(sid):
    """Estado público do sorteio — sem login (usado pelo telão)."""
    with get_db() as conn:
        s = conn.execute("SELECT * FROM sorteios WHERE id=?", (sid,)).fetchone()
        if not s:
            return jsonify({"ok": False, "msg": "Sorteio não encontrado"})
        premios  = conn.execute("SELECT * FROM sorteio_premios WHERE sorteio_id=? ORDER BY ordem", (sid,)).fetchall()
        numeros  = conn.execute("SELECT numero, ordem FROM sorteio_numeros WHERE sorteio_id=? ORDER BY ordem", (sid,)).fetchall()
        ganhadores = conn.execute(
            "SELECT sg.nome_ganhador, sg.numero_cartela, sg.desclassificado, "
            "sp.nome as premio_nome, sp.tipo_batida, sp.ordem as premio_ordem "
            "FROM sorteio_ganhadores sg JOIN sorteio_premios sp ON sg.premio_id=sp.id "
            "WHERE sg.sorteio_id=? ORDER BY sg.id", (sid,)
        ).fetchall()
    nums = [n["numero"] for n in numeros]
    ultimo = nums[-1] if nums else None
    sorteio_dict = dict(s)
    # Remove logo do estado público para não trafegar base64 enorme no polling
    logo = sorteio_dict.pop("logo_base64", "")
    sorteio_dict["tem_logo"] = bool(logo)
    # Premio atual em disputa
    premio_atual = next((dict(p) for p in premios if p["status"] == "em_andamento"), None)
    if not premio_atual:
        premio_atual = next((dict(p) for p in premios if p["status"] == "aguardando"), None)
    if premio_atual and premio_atual.get("tem_foto"):
        premio_atual["foto_url"] = f"/api/sorteio/{sid}/premio/{premio_atual['id']}/foto"
    elif premio_atual and premio_atual.get("premio_ref_id"):
        premio_atual["foto_url"] = f"/api/premios/{premio_atual['premio_ref_id']}/foto"
    # Ganhadores válidos (não desclassificados)
    ganhadores_validos = [dict(g) for g in ganhadores if not g["desclassificado"]]
    return jsonify({
        "ok": True,
        "sorteio": sorteio_dict,
        "pausado": bool(s["pausado"]),
        "numeros": nums,
        "ultimo_numero": ultimo,
        "premio_atual": premio_atual,
        "premios": [dict(p) for p in premios],
        "ganhadores": ganhadores_validos
    })

@app.route("/api/sorteio/<int:sid>/numero", methods=["POST"])
@requer_login
def api_sorteio_numero(sid):
    d = request.get_json() or {}
    numero = int(d.get("numero", 0))
    if numero < 1 or numero > 75:
        return jsonify({"ok": False, "msg": "Número inválido (1-75)"})
    with get_db() as conn:
        # Verifica se número já foi chamado
        existe = conn.execute(
            "SELECT id FROM sorteio_numeros WHERE sorteio_id=? AND numero=?", (sid, numero)
        ).fetchone()
        if existe:
            return jsonify({"ok": False, "msg": f"Número {numero} já foi chamado!"})
        ordem = conn.execute(
            "SELECT COUNT(*)+1 FROM sorteio_numeros WHERE sorteio_id=?", (sid,)
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO sorteio_numeros (sorteio_id, numero, ordem) VALUES (?,?,?)",
            (sid, numero, ordem)
        )
    return jsonify({"ok": True, "numero": numero, "ordem": ordem})

@app.route("/api/sorteio/<int:sid>/numero", methods=["DELETE"])
@requer_login
def api_sorteio_numero_desfazer(sid):
    """Remove o último número chamado."""
    with get_db() as conn:
        ultimo = conn.execute(
            "SELECT * FROM sorteio_numeros WHERE sorteio_id=? ORDER BY ordem DESC LIMIT 1", (sid,)
        ).fetchone()
        if not ultimo:
            return jsonify({"ok": False, "msg": "Nenhum número para desfazer"})
        conn.execute("DELETE FROM sorteio_numeros WHERE id=?", (ultimo["id"],))
    return jsonify({"ok": True, "numero": ultimo["numero"], "msg": f"Número {ultimo['numero']} removido"})

@app.route("/api/sorteio/<int:sid>/premio/<int:pid>/iniciar", methods=["POST"])
@requer_login
def api_sorteio_premio_iniciar(sid, pid):
    with get_db() as conn:
        # Encerra qualquer prêmio em andamento
        conn.execute("UPDATE sorteio_premios SET status='encerrado' WHERE sorteio_id=? AND status='em_andamento'", (sid,))
        conn.execute("UPDATE sorteio_premios SET status='em_andamento' WHERE id=? AND sorteio_id=?", (pid, sid))
        conn.execute("UPDATE sorteios SET pausado=0 WHERE id=?", (sid,))
    return jsonify({"ok": True})

@app.route("/api/sorteio/<int:sid>/pausar", methods=["POST"])
@requer_login
def api_sorteio_pausar(sid):
    with get_db() as conn:
        conn.execute("UPDATE sorteios SET pausado=1 WHERE id=?", (sid,))
    return jsonify({"ok": True, "pausado": True})

@app.route("/api/sorteio/<int:sid>/retomar", methods=["POST"])
@requer_login
def api_sorteio_retomar(sid):
    with get_db() as conn:
        conn.execute("UPDATE sorteios SET pausado=0 WHERE id=?", (sid,))
    return jsonify({"ok": True, "pausado": False})

@app.route("/api/sorteio/<int:sid>/verificar", methods=["POST"])
@requer_login
def api_sorteio_verificar(sid):
    """Verifica se uma cartela ganhou o prêmio atual."""
    d = request.get_json() or {}
    numero_cartela = (d.get("numero_cartela") or "").strip()
    premio_id = int(d.get("premio_id", 0))
    if not numero_cartela or not premio_id:
        return jsonify({"ok": False, "msg": "Informe o número da cartela e o prêmio"})
    with get_db() as conn:
        premio = conn.execute("SELECT * FROM sorteio_premios WHERE id=? AND sorteio_id=?", (premio_id, sid)).fetchone()
        if not premio:
            return jsonify({"ok": False, "msg": "Prêmio não encontrado"})
        # Busca a cartela — intervalo no formato "00001 a 00010"
        # O operador digita o número da cartela (ex: 5 ou 00005)
        contato = None
        try:
            num = int(numero_cartela)
            rows = conn.execute("SELECT * FROM contatos").fetchall()
            for row in rows:
                intervalo = (row["intervalo"] or "")
                if " a " in intervalo:
                    partes = intervalo.split(" a ")
                    inicio = int(partes[0].strip())
                    fim    = int(partes[1].strip())
                    if inicio <= num <= fim:
                        contato = row
                        break
                else:
                    if str(num).zfill(5) == intervalo.strip() or str(num) == intervalo.strip():
                        contato = row
                        break
        except ValueError:
            pass
        # Fallback: busca pelo texto exato
        if not contato:
            contato = conn.execute(
                "SELECT * FROM contatos WHERE TRIM(intervalo)=?",
                (numero_cartela,)
            ).fetchone()
        numeros_chamados = [r["numero"] for r in conn.execute(
            "SELECT numero FROM sorteio_numeros WHERE sorteio_id=?", (sid,)
        ).fetchall()]
    if not contato:
        return jsonify({"ok": False, "msg": f"Cartela '{numero_cartela}' não encontrada. Digite o número da cartela (ex: 5 para a cartela 00005)"})
    c = dict(contato)
    if (c.get("status") or "").lower() != "pago":
        return jsonify({"ok": False, "paga": False, "msg": f"Cartela não está paga! Status: {c.get('status')}"})
    return jsonify({
        "ok": True,
        "paga": True,
        "contato": {"id": c.get("id"), "nome": c.get("nome"), "lote": c.get("lote"), "intervalo": c.get("intervalo"), "status": c.get("status")},
        "numeros_chamados": numeros_chamados,
        "tipo_batida": dict(premio)["tipo_batida"],
        "msg": f"Cartela válida — {c.get('nome')} | Lote {c.get('lote')} | {c.get('intervalo')}"
    })


@app.route("/api/sorteio/<int:sid>/ganhador", methods=["POST"])
@requer_login
def api_sorteio_registrar_ganhador(sid):
    d = request.get_json() or {}
    premio_id      = int(d.get("premio_id", 0))
    contato_id     = d.get("contato_id")
    numero_cartela = (d.get("numero_cartela") or "").strip()
    nome_ganhador  = (d.get("nome_ganhador") or "").strip()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sorteio_ganhadores (sorteio_id, premio_id, contato_id, numero_cartela, nome_ganhador, desclassificado) VALUES (?,?,?,?,?,0)",
            (sid, premio_id, contato_id, numero_cartela, nome_ganhador)
        )
        conn.execute("UPDATE sorteio_premios SET status='encerrado' WHERE id=?", (premio_id,))
        conn.execute("UPDATE sorteios SET pausado=0 WHERE id=?", (sid,))
        # Verifica se todos prêmios encerrados
        restantes = conn.execute(
            "SELECT COUNT(*) FROM sorteio_premios WHERE sorteio_id=? AND status != 'encerrado'", (sid,)
        ).fetchone()[0]
        if restantes == 0:
            conn.execute("UPDATE sorteios SET status='encerrado' WHERE id=?", (sid,))
    log(f"Ganhador registrado: {nome_ganhador} | Cartela {numero_cartela}", "success")
    return jsonify({"ok": True, "msg": f"Ganhador {nome_ganhador} registrado!", "todos_encerrados": restantes == 0})

@app.route("/api/sorteio/<int:sid>/desclassificar", methods=["POST"])
@requer_login
def api_sorteio_desclassificar(sid):
    """Desclassifica uma tentativa de ganhador — registra e continua."""
    d = request.get_json() or {}
    premio_id      = int(d.get("premio_id", 0))
    numero_cartela = (d.get("numero_cartela") or "").strip()
    motivo         = (d.get("motivo") or "Desclassificado").strip()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sorteio_ganhadores (sorteio_id, premio_id, numero_cartela, nome_ganhador, desclassificado) VALUES (?,?,?,?,1)",
            (sid, premio_id, numero_cartela, motivo)
        )
        conn.execute("UPDATE sorteios SET pausado=0 WHERE id=?", (sid,))
    return jsonify({"ok": True, "msg": "Desclassificado. Continuando o sorteio."})



@app.route("/api/sorteio/<int:sid>/encerrar", methods=["POST"])
@requer_login
def api_sorteio_encerrar(sid):
    with get_db() as conn:
        conn.execute("UPDATE sorteios SET status='encerrado' WHERE id=?", (sid,))
    log(f"Sorteio {sid} encerrado", "info")
    return jsonify({"ok": True, "msg": "Sorteio encerrado!"})

@app.route("/api/sorteio/<int:sid>", methods=["DELETE"])
@requer_login
def api_sorteio_deletar(sid):
    with get_db() as conn:
        conn.execute("DELETE FROM sorteio_numeros WHERE sorteio_id=?", (sid,))
        conn.execute("DELETE FROM sorteio_ganhadores WHERE sorteio_id=?", (sid,))
        conn.execute("DELETE FROM sorteio_premios WHERE sorteio_id=?", (sid,))
        conn.execute("DELETE FROM sorteios WHERE id=?", (sid,))
    log(f"Sorteio {sid} removido", "info")
    return jsonify({"ok": True, "msg": "Sorteio removido!"})

@app.route("/sorteio/<int:sid>/telao")
def sorteio_telao(sid):
    """Tela pública do telão — sem login."""
    return render_template("telao.html", sorteio_id=sid)

@app.route("/api/sorteio/<int:sid>/premio/<int:pid>/foto")
def api_sorteio_premio_foto(sid, pid):
    """Retorna foto do prêmio do sorteio — sem login."""
    with get_db() as conn:
        row = conn.execute("SELECT foto_base64 FROM sorteio_premios WHERE id=? AND sorteio_id=?", (pid, sid)).fetchone()
    if not row or not row["foto_base64"]:
        return "", 404
    try:
        import base64 as _b64
        b64 = row["foto_base64"]
        if "," in b64:
            header, data = b64.split(",", 1)
            mime = header.split(":")[1].split(";")[0] if ":" in header else "image/jpeg"
        else:
            data, mime = b64, "image/jpeg"
        from flask import Response
        return Response(_b64.b64decode(data), mimetype=mime)
    except:
        return "", 400

@app.route("/api/sorteio/<int:sid>/logo")
def api_sorteio_logo(sid):
    """Retorna logo do sorteio — sem login para uso no telão."""
    with get_db() as conn:
        row = conn.execute("SELECT logo_base64 FROM sorteios WHERE id=?", (sid,)).fetchone()
    if not row or not row["logo_base64"]:
        return "", 404
    try:
        import base64 as _b64
        b64 = row["logo_base64"]
        if "," in b64:
            header, data = b64.split(",", 1)
            mime = header.split(":")[1].split(";")[0] if ":" in header else "image/jpeg"
        else:
            data, mime = b64, "image/jpeg"
        from flask import Response
        return Response(_b64.b64decode(data), mimetype=mime)
    except:
        return "", 400


@requer_login
def api_relatorios_desmembramentos():
    """Retorna todos os lotes desmembrados com suas cartelas filhas."""
    with get_db() as conn:
        # Busca todos os lotes com status Desmembrado
        pais = conn.execute(
            "SELECT * FROM contatos WHERE LOWER(status)='desmembrado' ORDER BY CAST(lote AS INTEGER), id"
        ).fetchall()
        # Busca todas as cartelas filhas (com origem_id)
        filhas = conn.execute(
            "SELECT * FROM contatos WHERE origem_id IS NOT NULL ORDER BY CAST(lote AS INTEGER), id"
        ).fetchall()
        # Busca auditoria de desmembramentos
        audits = conn.execute(
            "SELECT * FROM auditoria WHERE acao='DESMEMBRAMENTO' ORDER BY criado_em DESC"
        ).fetchall()

    filhas_por_pai = {}
    for f in filhas:
        d = dict(f)
        pid = d.get("origem_id")
        if pid not in filhas_por_pai:
            filhas_por_pai[pid] = []
        filhas_por_pai[pid].append(d)

    resultado = []
    for p in pais:
        d = dict(p)
        d["cartelas"] = filhas_por_pai.get(d["id"], [])
        resultado.append(d)

    return jsonify({
        "ok": True,
        "total_lotes": len(resultado),
        "total_cartelas": sum(len(r["cartelas"]) for r in resultado),
        "lotes": resultado,
        "auditoria": [dict(a) for a in audits]
    })

@app.route("/api/relatorios")
@requer_login
def api_relatorios():
    os.makedirs(PASTA_RELAT, exist_ok=True)
    arquivos = sorted(
        [f for f in os.listdir(PASTA_RELAT) if f.endswith(".json")],
        reverse=True
    )[:20]
    return jsonify({"ok":True,"arquivos":arquivos})

@app.route("/api/relatorios/log_envios")
@requer_login
def api_relatorios_log_envios():
    """Retorna histórico de envios agrupado por data, direto do banco."""
    data_de  = request.args.get("data_de","").strip()
    data_ate = request.args.get("data_ate","").strip()

    with get_db() as conn:
        # Monta query com filtros opcionais de data
        where = []
        params = []
        if data_de:
            where.append("le.criado_em >= ?")
            params.append(data_de)
        if data_ate:
            where.append("le.criado_em <= ?")
            params.append(data_ate + " 23:59")
        sql = (
            "SELECT le.*, c.lote, c.intervalo, c.previsao_pagamento "
            "FROM log_envios le "
            "LEFT JOIN contatos c ON le.contato_id = c.id "
            + ("WHERE " + " AND ".join(where) if where else "") +
            " ORDER BY le.id DESC LIMIT 2000"
        )
        rows = conn.execute(sql, params).fetchall()
    registros = [dict(r) for r in rows]
    # Agrupa por data — suporta dois formatos: "dd/mm/yyyy HH:MM" e "yyyy-mm-dd HH:MM:SS"
    grupos = {}
    for r in registros:
        raw = (r.get("criado_em") or "").strip()
        # Detecta formato e normaliza para "dd/mm/yyyy"
        if len(raw) >= 10 and raw[4] == '-':
            # formato ISO: yyyy-mm-dd ...
            partes = raw[:10].split('-')
            data = partes[2] + '/' + partes[1] + '/' + partes[0]
        else:
            data = raw[:10]  # dd/mm/yyyy
        if not data or len(data) < 8:
            data = "sem data"
        if data not in grupos:
            grupos[data] = {"data": data, "enviados": 0, "erros": 0, "itens": []}
        if r.get("status") == "ENVIADO":
            grupos[data]["enviados"] += 1
        else:
            grupos[data]["erros"] += 1
        grupos[data]["itens"].append(r)
    # Ordena grupos mais recentes primeiro
    def sort_key(g):
        parts = g["data"].split("/")
        return parts[2]+parts[1]+parts[0] if len(parts)==3 else g["data"]
    lista = sorted(grupos.values(), key=sort_key, reverse=True)
    return jsonify({"ok": True, "grupos": lista, "total": len(registros)})

@app.route("/api/relatorios/<nome>")
@requer_login
def api_relatorio_detalhe(nome):
    nome = os.path.basename(nome)
    caminho = os.path.join(PASTA_RELAT, nome)
    if not os.path.exists(caminho):
        return jsonify({"ok":False,"msg":"Relatorio nao encontrado"})
    with open(caminho, encoding="utf-8") as f:
        dados = json.load(f)
    return jsonify({"ok":True,"dados":dados})

# ══════════════════════════════════════════════════════════
#  API COMPATIBILIDADE V1 (aliases de rotas)
# ══════════════════════════════════════════════════════════

@app.route("/api/parar", methods=["POST"])
@requer_login
def api_parar():
    estado["enviando"] = False
    log("Envio interrompido.","warning")
    return jsonify({"ok":True})

@app.route("/api/limpar-log", methods=["POST"])
@requer_login
def api_limpar_log_v1():
    estado["log"] = []
    try:
        with open(LOG_PATH,"w") as f: json.dump([], f)
    except: pass
    return jsonify({"ok":True})

@app.route("/api/agendamento", methods=["POST"])
@requer_login
def api_agendamento_v1():
    d = request.get_json() or {}; cfg = carregar_config()
    cfg["modo_envio"]    = d.get("modo", cfg.get("modo_envio","manual"))
    cfg["horario_envio"] = d.get("horario", cfg.get("horario_envio","09:00"))
    cfg["dia_semana"]    = d.get("dia_semana", cfg.get("dia_semana","monday"))
    salvar_config(cfg); aplicar_config()
    log(f"Agendamento: {cfg['modo_envio']} às {cfg['horario_envio']}","success")
    return jsonify({"ok":True,"msg":f"Agendamento salvo! {cfg['modo_envio'].upper()} às {cfg['horario_envio']}"})

@app.route("/api/agendamento/toggle", methods=["POST"])
@requer_login
def api_agendamento_toggle():
    global AGENDAMENTO_ATIVO
    cfg = carregar_config()
    AGENDAMENTO_ATIVO = not bool(cfg.get("agendamento_ativo", False))
    cfg["agendamento_ativo"] = AGENDAMENTO_ATIVO
    salvar_config(cfg)
    status = "ATIVADO" if AGENDAMENTO_ATIVO else "DESATIVADO"
    log(f"Agendamento automático {status} por {session.get('usuario','?')}", "success" if AGENDAMENTO_ATIVO else "warning")
    return jsonify({"ok":True,"ativo":AGENDAMENTO_ATIVO,"msg":f"Agendamento {status}!"})

@app.route("/api/agendamento/status")
@requer_login
def api_agendamento_status():
    cfg = carregar_config()
    return jsonify({
        "ok": True,
        "ativo": bool(cfg.get("agendamento_ativo", False)),
        "modo": cfg.get("modo_envio","manual"),
        "horario": cfg.get("horario_envio","09:00"),
        "dia_semana": cfg.get("dia_semana","monday"),
        # Último disparo (qualquer origem)
        "ultimo_disparo_hora":     cfg.get("ultimo_disparo_hora",""),
        "ultimo_disparo_resultado":cfg.get("ultimo_disparo_resultado",""),
        "ultimo_disparo_tipo":     cfg.get("ultimo_disparo_tipo",""),
        "ultimo_disparo_origem":   cfg.get("ultimo_disparo_origem",""),
        # Último agendamento automático
        "ultimo_agendamento":      cfg.get("ultimo_agendamento", cfg.get("ultimo_agendado_hora","")),
        "ultimo_agendamento_modo": cfg.get("ultimo_agendamento_modo",""),
        "ultimo_agendado_hora":    cfg.get("ultimo_agendado_hora",""),
        "ultimo_agendado_resultado":cfg.get("ultimo_agendado_resultado",""),
        # Último disparo manual
        "ultimo_manual_hora":      cfg.get("ultimo_manual_hora",""),
        "ultimo_manual_resultado": cfg.get("ultimo_manual_resultado",""),
        "ultimo_manual_usuario":   cfg.get("ultimo_manual_usuario",""),
    })

@app.route("/api/config/verificar-senha", methods=["POST"])
@requer_login
def api_verificar_senha():
    d = request.get_json() or {}; cfg = carregar_config()
    if d.get("senha","") == cfg.get("senha","admin123"):
        return jsonify({"ok":True})
    return jsonify({"ok":False,"msg":"Senha incorreta!"})

@app.route("/api/teste-preview", methods=["POST"])
@requer_login
def api_teste_preview():
    d = request.get_json() or {}
    numero_destino = d.get("numero",""); enviar = d.get("enviar", False)
    cfg = carregar_config()
    with get_db() as conn:
        row = conn.execute("SELECT * FROM contatos WHERE LOWER(status)='pendente' ORDER BY CAST(lote AS INTEGER),id LIMIT 1").fetchone()
    if not row: return jsonify({"ok":False,"msg":"Nenhum contato pendente encontrado!"})
    c = dict(row)
    tpl = carregar_template(TEMPLATE_ATIVO)
    try:
        mensagem = tpl.get("texto",TEMPLATE_PADRAO["texto"]).format(
            nome=c["nome"],lote=c.get("lote",""),intervalo=c.get("intervalo",""),
            vendedor=c.get("vendedor",""),valor=c.get("valor",""),
            chave_pix=cfg.get("chave_pix",""),beneficiario=cfg.get("nome_organizador",""),
            data_sorteio=cfg.get("data_sorteio",""),evento=cfg.get("nome_evento",""))
    except: mensagem = str(c)
    resultado = {"ok":True,"preview":mensagem,"nome":c["nome"],"lote":c.get("lote",""),"numero_original":c.get("telefone","")}
    if enviar and numero_destino:
        sid   = cfg.get("twilio_sid",""); token = cfg.get("twilio_token",""); num = cfg.get("twilio_numero","")
        if not all([sid,token,num]): return jsonify({"ok":False,"msg":"Twilio não configurado!"})
        try:
            client = TwilioClient(sid, token)
            c_dest = {"nome":c["nome"],"telefone":numero_destino,"whatsapp":numero_destino,
                "lote":c.get("lote",""),"intervalo":c.get("intervalo",""),"valor":c.get("valor",""),"vendedor":c.get("vendedor","")}
            ok, sid_ou_erro = _enviar_twilio(client, num, c_dest, cfg)
            resultado["enviado"] = ok
            if not ok: return jsonify({"ok":False,"msg":sid_ou_erro})
            log(f"Preview enviado para {numero_destino} | SID: {sid_ou_erro}","success")
        except Exception as e:
            return jsonify({"ok":False,"msg":str(e)})
    else: resultado["enviado"] = False
    return jsonify(resultado)

@app.route("/api/qrcode/listar")
@requer_login
def api_listar_qrcodes():
    os.makedirs(PASTA_QRCODES, exist_ok=True)
    arquivos = sorted([f for f in os.listdir(PASTA_QRCODES) if f.endswith(".png")])
    return jsonify({"ok":True,"arquivos":arquivos})

@app.route("/api/qrcode/download/<nome_arquivo>")
@requer_login
def api_download_qrcode(nome_arquivo):
    from flask import send_file as sf
    caminho = os.path.join(PASTA_QRCODES, nome_arquivo)
    if os.path.exists(caminho): return sf(caminho, as_attachment=True, download_name=nome_arquivo)
    return jsonify({"erro":"Arquivo não encontrado"}), 404

@app.route("/api/sair", methods=["POST"])
@requer_login
def api_sair():
    log("Sistema encerrado pelo usuário.","warning")
    def shutdown():
        time.sleep(1)
        try:
            # Checkpoint WAL antes de encerrar
            with get_db() as conn: conn.execute("PRAGMA wal_checkpoint(FULL)")
        except: pass
        os._exit(0)
    threading.Thread(target=shutdown, daemon=True).start()
    return jsonify({"ok":True,"msg":"Encerrando..."})


# ══════════════════════════════════════════════════════════
#  API AUDITORIA
# ══════════════════════════════════════════════════════════

ACAO_LABELS = {
    "CRIACAO":                  ("➕ Criação",           "#3b82f6"),
    "EDICAO":                   ("✏️ Edição",             "#f59e0b"),
    "EXCLUSAO":                 ("🗑 Exclusão",           "#ef4444"),
    "MARCAR_PAGO":              ("✅ Pago",               "#10b981"),
    "IMPORTACAO_SHEETS":        ("📥 Import. Sheets",     "#8b5cf6"),
    "IMPORTACAO_ARQUIVO":       ("📥 Import. Arquivo",    "#8b5cf6"),
    "DESMEMBRAMENTO":           ("🔀 Desmembramento",     "#8b5cf6"),
    "AGENDAMENTO_RESULTADO":    ("⏰ Disparo Agendado",   "#22c55e"),
    "DISPARO_MANUAL_RESULTADO": ("🚀 Disparo Manual",     "#f59e0b"),
    "AGENDAMENTO_DISPARO":      ("⏰ Ag. Iniciado",       "#3b82f6"),
    "AGENDAMENTO_ATIVADO":      ("⏰ Ag. Ativado",        "#22c55e"),
    "AGENDAMENTO_DESATIVADO":   ("⏸ Ag. Desativado",     "#64748b"),
    "SISTEMA_INIT":             ("🔧 Início do Sistema",  "#06b6d4"),
    "LOGIN":                    ("🔑 Login",              "#06b6d4"),
    "LOGOUT":                   ("🚪 Logout",             "#64748b"),
}

@app.route("/api/auditoria")
@requer_login
def api_auditoria():
    page    = int(request.args.get("page", 1))
    per     = int(request.args.get("per", 50))
    acao    = request.args.get("acao", "")
    usuario = request.args.get("usuario", "").strip()
    busca   = request.args.get("q", "").strip()
    data_de = request.args.get("data_de", "").strip()
    data_ate= request.args.get("data_ate", "").strip()
    sql    = "SELECT * FROM auditoria WHERE 1=1"
    params = []
    if acao:    sql += " AND acao=?";    params.append(acao)
    if usuario: sql += " AND usuario=?"; params.append(usuario)
    if data_de:
        # criado_em é "dd/mm/yyyy HH:MM:SS" — usa substr p/ converter para yyyy-mm-dd na query
        sql += " AND (substr(criado_em,7,4)||'-'||substr(criado_em,4,2)||'-'||substr(criado_em,1,2)) >= ?"
        params.append(data_de)
    if data_ate:
        sql += " AND (substr(criado_em,7,4)||'-'||substr(criado_em,4,2)||'-'||substr(criado_em,1,2)) <= ?"
        params.append(data_ate)
    if busca:
        sql += " AND (nome LIKE ? OR lote LIKE ? OR telefone LIKE ? OR usuario LIKE ? OR detalhes LIKE ?)"
        b = f"%{busca}%"; params += [b,b,b,b,b]
    with get_db() as conn:
        total    = conn.execute(f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
        rows     = conn.execute(sql + f" ORDER BY id DESC LIMIT {per} OFFSET {(page-1)*per}", params).fetchall()
        usuarios = [r[0] for r in conn.execute("SELECT DISTINCT usuario FROM auditoria WHERE usuario IS NOT NULL AND usuario != '' ORDER BY usuario").fetchall()]
    registros = []
    for r in rows:
        reg = dict(r)
        label, cor = ACAO_LABELS.get(reg.get("acao",""), (reg.get("acao",""), "#64748b"))
        reg["acao_label"] = label
        reg["acao_cor"]   = cor
        registros.append(reg)
    return jsonify({"ok":True,"total":total,"page":page,"per":per,"registros":registros,"usuarios":usuarios})

@app.route("/api/auditoria/acoes")
@requer_login
def api_auditoria_acoes():
    with get_db() as conn:
        rows = conn.execute("SELECT DISTINCT acao FROM auditoria ORDER BY acao").fetchall()
    acoes = [{"acao": r[0], "label": ACAO_LABELS.get(r[0],(r[0],""))[0]} for r in rows]
    return jsonify({"ok":True,"acoes":acoes})

@app.route("/api/auditoria/contato/<int:cid>")
@requer_login
def api_auditoria_contato(cid):
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM auditoria WHERE contato_id=? ORDER BY id DESC", (cid,)).fetchall()
    registros = []
    for r in rows:
        reg = dict(r)
        label, cor = ACAO_LABELS.get(reg.get("acao",""), (reg.get("acao",""), "#64748b"))
        reg["acao_label"] = label; reg["acao_cor"] = cor
        registros.append(reg)
    return jsonify({"ok":True,"registros":registros})

# ══════════════════════════════════════════════════════════
#  CONCILIAÇÃO BANCÁRIA
# ══════════════════════════════════════════════════════════

try:
    import pdfplumber
    PDFPLUMBER_OK = True
except ImportError:
    PDFPLUMBER_OK = False

# Cache em memória: { session_key: { "bingo_id": int, "pix": [...] } }
# Estrutura de cada PIX:
#   { "data": "dd/mm/yyyy", "valor": float, "remetente": str, "documento": str, "idx": int }
_conciliacao_cache = {}
_conciliacao_lock  = threading.Lock()


def _extrair_pix_sicredi(pdf_bytes: bytes) -> list[dict]:
    """
    Extrai transações PIX recebidas de um extrato PDF do Sicredi.
    Tenta leitura como tabela estruturada; cai para texto corrido se necessário.
    Retorna lista de dicts com: data, valor, remetente, documento.
    """
    if not PDFPLUMBER_OK:
        raise RuntimeError("pdfplumber não instalado no servidor. Contate o administrador.")

    import io as _io, re as _re
    from decimal import Decimal, InvalidOperation

    re_data  = _re.compile(r'\b(\d{2}/\d{2}/\d{4})\b')
    re_valor = _re.compile(r'\b(\d{1,3}(?:\.\d{3})*,\d{2})\b')

    def to_float(s):
        try:
            return float(Decimal(s.replace('.', '').replace(',', '.')))
        except InvalidOperation:
            return None

    # Palavras-chave que identificam crédito Pix no extrato Sicredi
    KW_PIX = ['PIX RECEBIDO', 'RECEBIMENTO PIX', 'TRANSF PIX REC',
               'PIX CREDIT', 'PIX - RECEBIDO', 'RECEB PIX']
    KW_PIX_UPPER = [k.upper() for k in KW_PIX]

    linhas_brutas = []
    with pdfplumber.open(_io.BytesIO(pdf_bytes)) as pdf:
        for pagina in pdf.pages:
            tabelas = pagina.extract_tables()
            if tabelas:
                for tabela in tabelas:
                    for linha in tabela:
                        if linha:
                            linhas_brutas.append(' | '.join(str(c or '').strip() for c in linha))
            else:
                texto = pagina.extract_text(layout=True) or ''
                linhas_brutas.extend(texto.split('\n'))

    pix = []
    linhas = [l.strip() for l in linhas_brutas]

    for i, linha in enumerate(linhas):
        lu = linha.upper()
        if not any(kw in lu for kw in KW_PIX_UPPER):
            continue

        # Extrai data — tenta na mesma linha ou até 2 linhas antes
        data_str = None
        for off in [0, -1, -2, 1]:
            idx = i + off
            if 0 <= idx < len(linhas):
                m = re_data.search(linhas[idx])
                if m:
                    data_str = m.group(1)
                    break

        # Extrai valor positivo — tenta na mesma linha ou até 2 linhas seguintes
        valor = None
        for off in [0, 1, 2, -1]:
            idx = i + off
            if 0 <= idx < len(linhas):
                vals = [to_float(v) for v in re_valor.findall(linhas[idx])]
                vals = [v for v in vals if v and v > 0]
                # Ignora valores que parecem ser datas (ex: 2025,00 improvável)
                vals = [v for v in vals if v < 500_000]
                if vals:
                    valor = max(vals)
                    break

        if not data_str or not valor:
            continue

        # Remetente: linha seguinte com "De:" / "ORIGEM:" ou linha em MAIÚSCULO curta
        remetente = 'Não identificado'
        for off in [1, 2]:
            idx = i + off
            if 0 <= idx < len(linhas):
                prox = linhas[idx]
                pu = prox.upper()
                if pu.startswith('DE:') or pu.startswith('ORIGEM:'):
                    remetente = _re.sub(r'^(DE:|ORIGEM:)\s*', '', prox, flags=_re.I).strip()
                    break
                # Linha de nome: só maiúsculas, sem dígitos de valor
                if (5 < len(prox) < 60
                        and not re_valor.search(prox)
                        and not re_data.search(prox)
                        and prox == prox.upper()
                        and _re.search(r'[A-Z]', prox)):
                    remetente = prox
                    break

        # Documento (código do Pix / número de sequência)
        documento = ''
        m_doc = _re.search(r'\b(PIX\w*|E\d{10,})\b', lu)
        if m_doc:
            documento = m_doc.group(0)

        pix.append({
            'data':      data_str,
            'valor':     valor,
            'remetente': remetente,
            'documento': documento,
        })

    return pix


@app.route('/api/conciliacao/upload', methods=['POST'])
@requer_login
def api_conciliacao_upload():
    """
    Recebe PDF do extrato Sicredi + bingo_id selecionado.
    Extrai PIX, cruza com cartelas Pendentes do bingo, devolve resultado para o frontend.
    """
    if not PDFPLUMBER_OK:
        return jsonify({'ok': False, 'erro': 'pdfplumber não instalado no servidor. Execute: pip install pdfplumber'})

    if 'arquivo' not in request.files:
        return jsonify({'ok': False, 'erro': 'Nenhum arquivo enviado'})

    arquivo   = request.files['arquivo']
    bingo_id  = request.form.get('bingo_id', '').strip()

    if not arquivo.filename.lower().endswith('.pdf'):
        return jsonify({'ok': False, 'erro': 'Envie um arquivo PDF'})

    try:
        pdf_bytes = arquivo.read()
    except Exception as e:
        return jsonify({'ok': False, 'erro': f'Erro ao ler arquivo: {e}'})

    # Extrai PIX
    try:
        pix_list = _extrair_pix_sicredi(pdf_bytes)
    except Exception as e:
        return jsonify({'ok': False, 'erro': f'Erro ao processar PDF: {e}'})

    if not pix_list:
        return jsonify({
            'ok': False,
            'erro': (
                'Nenhum PIX recebido encontrado no PDF. '
                'Verifique se o arquivo é o extrato correto do Sicredi '
                'e se contém transações Pix recebidas no período.'
            )
        })

    # Adiciona índice para identificação no frontend
    for idx, p in enumerate(pix_list):
        p['idx'] = idx

    # Busca cartelas Pendentes do bingo selecionado
    # Se bingo_id vazio, busca todas as pendentes
    with get_db() as conn:
        if bingo_id:
            # Tenta filtrar por lote se bingo_id for numérico
            pendentes_rows = conn.execute(
                "SELECT id, nome, telefone, lote, intervalo, valor "
                "FROM contatos WHERE status='Pendente' AND lote=? "
                "ORDER BY CAST(lote AS INTEGER), id",
                (bingo_id,)
            ).fetchall()
        else:
            pendentes_rows = conn.execute(
                "SELECT id, nome, telefone, lote, intervalo, valor "
                "FROM contatos WHERE status='Pendente' "
                "ORDER BY CAST(lote AS INTEGER), id"
            ).fetchall()

        # Busca todos os lotes para o seletor do frontend
        lotes_rows = conn.execute(
            "SELECT DISTINCT lote FROM contatos WHERE lote != '' ORDER BY CAST(lote AS INTEGER)"
        ).fetchall()

    pendentes = [dict(r) for r in pendentes_rows]
    lotes     = [r[0] for r in lotes_rows]

    # Agrupa PIX por valor (arredondado em centavos)
    from collections import defaultdict
    pix_por_valor = defaultdict(list)
    for p in pix_list:
        chave = round(p['valor'] * 100)  # centavos como chave inteira
        pix_por_valor[chave].append(p)

    # Para cada valor de PIX, lista as cartelas Pendentes com valor compatível
    grupos = []
    for centavos, lista_pix in sorted(pix_por_valor.items()):
        valor_float = centavos / 100
        valor_fmt   = f"R$ {valor_float:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

        # Cartelas cujo valor bate com este PIX
        def parse_valor_cartela(v):
            try:
                return float(str(v or '0').replace('R$', '').replace('.', '').replace(',', '.').strip())
            except:
                return 0.0

        cartelas_match = [
            p for p in pendentes
            if abs(parse_valor_cartela(p.get('valor')) - valor_float) < 0.02
        ]

        grupos.append({
            'valor_centavos': centavos,
            'valor_fmt':      valor_fmt,
            'pix':            lista_pix,
            'qtd_pix':        len(lista_pix),
            'cartelas':       cartelas_match,
        })

    # Salva cache na sessão (chave = usuário logado)
    cache_key = session.get('usuario', 'anon')
    with _conciliacao_lock:
        _conciliacao_cache[cache_key] = {
            'bingo_id': bingo_id,
            'pix':      pix_list,
        }

    log(f"Conciliação: {len(pix_list)} PIX extraídos do extrato — {session.get('usuario','?')}", 'info')

    return jsonify({
        'ok':         True,
        'total_pix':  len(pix_list),
        'pendentes':  len(pendentes),
        'grupos':     grupos,
        'lotes':      lotes,
        'bingo_id':   bingo_id,
    })


@app.route('/api/conciliacao/confirmar', methods=['POST'])
@requer_login
def api_conciliacao_confirmar():
    """
    Confirma pagamento de uma ou mais cartelas Pendentes.
    Recebe: { ids: [int], forma_pagamento: str, data_pagamento: str }
    """
    d           = request.get_json() or {}
    ids         = [int(i) for i in d.get('ids', [])]
    forma_pgto  = (d.get('forma_pagamento') or 'PIX').strip()
    data_pgto   = (d.get('data_pagamento') or datetime.now().strftime('%Y-%m-%d')).strip()

    if not ids:
        return jsonify({'ok': False, 'erro': 'Nenhuma cartela selecionada'})

    now = datetime.now().strftime('%d/%m/%Y %H:%M')

    with get_db() as conn:
        rows = conn.execute(
            f"SELECT id, nome, lote, intervalo, telefone, status FROM contatos "
            f"WHERE id IN ({','.join('?' * len(ids))})",
            ids
        ).fetchall()

        conn.execute(
            f"UPDATE contatos SET status='Pago', data_pagamento=?, forma_pagamento=?, atualizado_em=? "
            f"WHERE id IN ({','.join('?' * len(ids))})",
            [data_pgto, forma_pgto, now] + ids
        )

    for r in rows:
        c = dict(r)
        auditar(
            'MARCAR_PAGO',
            contato_id=c['id'],
            lote=c.get('lote', ''),
            intervalo=c.get('intervalo', ''),
            nome=c.get('nome', ''),
            telefone=c.get('telefone', ''),
            status_de=c.get('status', ''),
            status_para='Pago',
            detalhes=f'Conciliação bancária | Forma: {forma_pgto} | Data: {data_pgto}'
        )

    log(f"Conciliação: {len(ids)} cartela(s) marcadas como Pago — {session.get('usuario','?')}", 'success')
    return jsonify({'ok': True, 'confirmados': len(ids), 'msg': f'{len(ids)} cartela(s) confirmada(s) como Pago!'})



# ══════════════════════════════════════════════════════════
#  CAMISETAS
# ══════════════════════════════════════════════════════════

TAMANHOS_CAMISETA = ['N0', 'N2', 'N4', 'N6', 'N8', 'PP', 'P', 'M', 'G', 'GG', 'EXG', 'EXGG']

EQUIPES_CAMISETA = [
    'Bingo', 'Equipe de Arrecadação', 'Equipe de Estrutura', 'Equipe de Caixa',
    'Bar', 'Espetinho', 'Batata Frita', 'Pastel', 'Cachorro-Quente', 'Caldo',
    'Maria Izabel', 'Doces', 'Espaço Criança', 'Leilão', 'Apresentação Cultural',
    'Montagem', 'Ornamentação Cultural', 'Limpeza', 'Segurança / Estacionamento',
    'Camisetas', 'Vendas', 'Pascom e Comunicação Visual', 'Coordenação Geral',
]

def _normalizar_texto(texto: str) -> str:
    """Remove acentos e converte para maiúsculas."""
    import unicodedata
    nfkd = unicodedata.normalize('NFKD', str(texto))
    sem_acento = ''.join(ch for ch in nfkd if not unicodedata.combining(ch))
    return sem_acento.upper()

def _validar_cpf(cpf: str) -> bool:
    """Valida CPF com dígito verificador."""
    cpf = re.sub(r'\D', '', cpf)
    if len(cpf) != 11 or len(set(cpf)) == 1:
        return False
    for i in range(2):
        soma = sum(int(cpf[j]) * (10 + i - j) for j in range(9 + i))
        dig = (soma * 10 % 11) % 10
        if dig != int(cpf[9 + i]):
            return False
    return True

def _cfg_camisetas():
    """Retorna configurações do módulo camisetas."""
    cfg = carregar_config()
    return {
        'ativo':         cfg.get('camisetas_ativo', '1') == '1',  # default habilitado
        'data_inicio':   cfg.get('camisetas_data_inicio', ''),
        'data_fim':      cfg.get('camisetas_data_fim', ''),
        'chave_pix':     cfg.get('camisetas_chave_pix', cfg.get('chave_pix', '')),
        'beneficiario':  cfg.get('camisetas_beneficiario', cfg.get('nome_organizador', '')),
        'descricao':     cfg.get('camisetas_descricao', ''),
        'valores':       {t: cfg.get(f'camisetas_valor_{t}', '') for t in TAMANHOS_CAMISETA},
    }

def _periodo_aberto(cfg_cam):
    """Verifica se o período de pedidos está aberto."""
    if not cfg_cam['ativo']:
        return False, 'Pedidos de camisetas estão encerrados no momento.'
    hoje = datetime.now().date()
    di = cfg_cam['data_inicio']
    df = cfg_cam['data_fim']
    try:
        if di and hoje < datetime.strptime(di, '%Y-%m-%d').date():
            return False, f'As inscrições ainda não iniciaram. Volte a partir de {di.split("-")[::-1][0]}/{di.split("-")[1]}/{di.split("-")[0][:4]}.'
        if df and hoje > datetime.strptime(df, '%Y-%m-%d').date():
            return False, 'O prazo para pedidos de camisetas foi encerrado.'
    except:
        pass
    return True, ''

# ── Página pública ──────────────────────────────────────

@app.route('/camisetas')
def camisetas_publico():
    return render_template('camisetas.html')

@app.route('/api/camisetas/config-publica')
def api_camisetas_config_publica():
    """Config pública sem login — período, valores, fotos."""
    cfg = _cfg_camisetas()
    aberto, msg = _periodo_aberto(cfg)
    log(f'[DEBUG] config-publica: ativo={cfg["ativo"]} aberto={aberto} msg={msg}', 'info')
    # Fotos: retorna URLs se existirem
    foto_frente = '/api/camisetas/foto/frente' if carregar_config().get('camisetas_foto_frente') else ''
    foto_verso  = '/api/camisetas/foto/verso'  if carregar_config().get('camisetas_foto_verso')  else ''
    return jsonify({
        'ok': True,
        'aberto': aberto,
        'msg_fechado': msg,
        'valores': cfg['valores'],
        'chave_pix': cfg['chave_pix'],
        'beneficiario': cfg['beneficiario'],
        'descricao': cfg['descricao'],
        'tamanhos': TAMANHOS_CAMISETA,
        'equipes': EQUIPES_CAMISETA,
        'foto_frente': foto_frente,
        'foto_verso': foto_verso,
        'celular_comprovante': cfg.get('celular_comprovante') or carregar_config().get('camisetas_celular_comprovante', ''),
    })

@app.route('/api/camisetas/foto/<lado>')
def api_camisetas_foto(lado):
    """Serve foto da camiseta — sem login."""
    if lado not in ('frente', 'verso'):
        return '', 404
    cfg = carregar_config()
    b64 = cfg.get(f'camisetas_foto_{lado}', '')
    if not b64:
        return '', 404
    try:
        import base64 as _b64
        if ',' in b64:
            header, data = b64.split(',', 1)
            mime = header.split(':')[1].split(';')[0] if ':' in header else 'image/jpeg'
        else:
            data, mime = b64, 'image/jpeg'
        from flask import Response
        return Response(_b64.b64decode(data), mimetype=mime)
    except:
        return '', 400

def _proximo_numero_pedido(conn):
    """Gera próximo número CAM-XXX."""
    row = conn.execute(
        "SELECT numero_pedido FROM camisetas_pedidos WHERE numero_pedido != '' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row and row['numero_pedido']:
        try:
            n = int(row['numero_pedido'].split('-')[-1]) + 1
        except:
            n = 1
    else:
        n = 1
    return f'CAM-{n:06d}'

@app.route('/api/camisetas/buscar-cpf', methods=['POST'])
def api_camisetas_buscar_cpf():
    """Busca pedido pelo CPF — sem login."""
    d   = request.get_json() or {}
    cpf = re.sub(r'\D', '', d.get('cpf', ''))
    if not _validar_cpf(cpf):
        return jsonify({'ok': False, 'erro': 'CPF inválido.'})
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM camisetas_pedidos WHERE cpf=? AND status_pagamento != ? ORDER BY id DESC LIMIT 1',
            (cpf, 'Cancelado')
        ).fetchone()
        if not row:
            return jsonify({'ok': True, 'existe': False})
        adicionais = conn.execute(
            'SELECT * FROM camisetas_adicionais WHERE pedido_id=?', (row['id'],)
        ).fetchall()
    return jsonify({
        'ok': True, 'existe': True,
        'pedido': dict(row),
        'adicionais': [dict(a) for a in adicionais],
    })

@app.route('/api/camisetas/salvar', methods=['POST'])
def api_camisetas_salvar():
    """Cria ou atualiza pedido — sem login."""
    try:
        cfg = _cfg_camisetas()
        aberto, msg = _periodo_aberto(cfg)
        if not aberto:
            return jsonify({'ok': False, 'erro': msg})

        d             = request.get_json() or {}
        cpf           = re.sub(r'\D', '', d.get('cpf', ''))
        nome          = _normalizar_texto((d.get('nome') or '').strip())
        tel           = sanitizar_telefone(d.get('telefone', ''))
        tam           = (d.get('tamanho') or '').strip().upper()
        dt_nasc       = (d.get('data_nascimento') or '').strip()
        equipe        = (d.get('equipe') or '').strip()
        obs_cadastro  = _normalizar_texto((d.get('obs_cadastro') or '').strip())
        adicionais    = d.get('adicionais', [])

        if not _validar_cpf(cpf):
            return jsonify({'ok': False, 'erro': 'CPF inválido.'})
        if not nome:
            return jsonify({'ok': False, 'erro': 'Nome é obrigatório.'})
        if tam not in TAMANHOS_CAMISETA:
            return jsonify({'ok': False, 'erro': 'Tamanho inválido.'})
        if equipe not in EQUIPES_CAMISETA:
            return jsonify({'ok': False, 'erro': 'Selecione uma equipe válida.'})

        now = datetime.now().strftime('%d/%m/%Y %H:%M')
        with get_db() as conn:
            row = conn.execute(
                'SELECT * FROM camisetas_pedidos WHERE cpf=? AND status_pagamento != ?',
                (cpf, 'Cancelado')
            ).fetchone()
            if row:
                dnasc_cadastro = (dict(row).get('data_nascimento') or '').strip()
                if dnasc_cadastro and dt_nasc != dnasc_cadastro:
                    return jsonify({'ok': False, 'erro': 'Data de nascimento não confere com o cadastro.'})
                pid = row['id']
                conn.execute(
                    'UPDATE camisetas_pedidos SET nome=?,telefone=?,tamanho=?,data_nascimento=?,equipe=?,obs_cadastro=?,atualizado_em=? WHERE id=?',
                    (nome, tel, tam, dt_nasc, equipe, obs_cadastro, now, pid)
                )
                conn.execute('DELETE FROM camisetas_adicionais WHERE pedido_id=?', (pid,))
                acao_audit = 'CAM_EDICAO_PUBLICA'
            else:
                num_pedido = _proximo_numero_pedido(conn)
                cur = conn.execute(
                    'INSERT INTO camisetas_pedidos (cpf,nome,telefone,tamanho,data_nascimento,equipe,obs_cadastro,numero_pedido,status_pagamento,criado_em,atualizado_em) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    (cpf, nome, tel, tam, dt_nasc, equipe, obs_cadastro, num_pedido, 'Realizado', now, now)
                )
                pid = cur.lastrowid
                acao_audit = 'CAM_CADASTRO'

            for a in adicionais:
                an  = _normalizar_texto((a.get('nome') or '').strip())
                at  = sanitizar_telefone(a.get('telefone', ''))
                atz = (a.get('tamanho') or '').strip().upper()
                if an and atz in TAMANHOS_CAMISETA:
                    conn.execute(
                        'INSERT INTO camisetas_adicionais (pedido_id,nome,telefone,tamanho) VALUES (?,?,?,?)',
                        (pid, an, at, atz)
                    )
            num_pedido_final = conn.execute(
                'SELECT numero_pedido FROM camisetas_pedidos WHERE id=?', (pid,)
            ).fetchone()['numero_pedido']

        auditar(acao_audit, usuario='publico',
                nome=nome, telefone=tel,
                detalhes=f'Pedido: {num_pedido_final} | CPF: {cpf} | Tamanho: {tam} | Equipe: {equipe} | Adicionais: {len(adicionais)} | Obs: {obs_cadastro}')

        def val(t):
            try: return float((cfg['valores'].get(t) or '0').replace(',', '.'))
            except: return 0.0

        total = val(tam) + sum(
            val((a.get('tamanho') or '').upper()) for a in adicionais
            if (a.get('nome') or '').strip() and (a.get('tamanho') or '').upper() in TAMANHOS_CAMISETA
        )
        total_fmt = 'R$ {:,.2f}'.format(total).replace(',','X').replace('.',',').replace('X','.')

        qr_url = ''
        try:
            import qrcode as _qr, base64 as _b64
            from io import BytesIO as _BytesIO
            payload = gerar_payload_pix(cfg['chave_pix'], cfg['beneficiario'], 'Palmas', total)
            img = _qr.make(payload)
            buf = _BytesIO()
            img.save(buf, format='PNG')
            qr_url = 'data:image/png;base64,' + _b64.b64encode(buf.getvalue()).decode()
        except Exception as eq:
            log(f'QR Code camiseta: {eq}', 'warning')

        return jsonify({
            'ok': True,
            'pid': pid,
            'numero_pedido': num_pedido_final,
            'total': total_fmt,
            'total_num': total,
            'chave_pix': cfg['chave_pix'],
            'beneficiario': cfg['beneficiario'],
            'qr_base64': qr_url,
        })

    except Exception as e:
        import traceback
        log(f'ERRO salvar camiseta: {e}\n{traceback.format_exc()}', 'error')
        return jsonify({'ok': False, 'erro': f'Erro interno: {str(e)}'})


# ── Retaguarda (requer login) ───────────────────────────

@app.route('/api/camisetas/lista')
@requer_login
def api_camisetas_lista():
    with get_db() as conn:
        pedidos = conn.execute(
            '''SELECT id,cpf,numero_pedido,nome,telefone,tamanho,data_nascimento,equipe,
               status_pagamento,forma_pagamento,data_pagamento,obs_cadastro,obs_pagamento,
               entregue,data_entrega,obs_entrega,criado_em,atualizado_em,valor_pago,
               comprovante_tipo,comprovante_em,
               CASE WHEN comprovante_base64 IS NOT NULL AND comprovante_base64 != '' THEN 1 ELSE 0 END as tem_comprovante
               FROM camisetas_pedidos ORDER BY criado_em DESC'''
        ).fetchall()
        adicionais = conn.execute('SELECT * FROM camisetas_adicionais').fetchall()
    ads_por_pedido = {}
    for a in adicionais:
        ads_por_pedido.setdefault(a['pedido_id'], []).append(dict(a))
    resultado = []
    for p in pedidos:
        pd = dict(p)
        pd['adicionais'] = ads_por_pedido.get(p['id'], [])
        pd['total_camisetas'] = 1 + len(pd['adicionais'])
        resultado.append(pd)
    return jsonify({'ok': True, 'pedidos': resultado})

@app.route('/api/camisetas/marcar-pago', methods=['POST'])
@requer_login
def api_camisetas_marcar_pago():
    try:
        return _api_camisetas_marcar_pago_impl()
    except Exception as _e:
        import traceback
        log(f'ERRO marcar-pago: {_e}\n{traceback.format_exc()}', 'error')
        return jsonify({'ok': False, 'erro': str(_e)}), 500

def _api_camisetas_marcar_pago_impl():
    d          = request.get_json() or {}
    ids        = [int(i) for i in d.get('ids', [])]
    forma_pgto = (d.get('forma_pagamento') or '').strip()
    data_pgto  = (d.get('data_pagamento')  or datetime.now().strftime('%Y-%m-%d')).strip()
    obs_pgto   = (d.get('obs_pagamento') or '').strip()
    valor_pago = float(d.get('valor_pago') or 0)
    if not ids:
        return jsonify({'ok': False, 'erro': 'Nenhum pedido selecionado'})
    now = datetime.now().strftime('%d/%m/%Y %H:%M')
    usuario = session.get('usuario', '?')
    cfg = _cfg_camisetas()
    def _val(tam):
        try: return float(cfg['valores'].get(tam) or 0)
        except: return 0.0
    auditorias = []
    with get_db() as conn:
        for pid in ids:
            row = conn.execute('SELECT id,nome,numero_pedido,tamanho FROM camisetas_pedidos WHERE id=?', (pid,)).fetchone()
            if not row: continue
            adds = conn.execute('SELECT tamanho FROM camisetas_adicionais WHERE pedido_id=?', (pid,)).fetchall()
            total = _val(row['tamanho']) + sum(_val(a['tamanho']) for a in adds)
            soma_ant = conn.execute('SELECT COALESCE(SUM(valor),0) FROM camisetas_pagamentos WHERE pedido_id=?', (pid,)).fetchone()[0] or 0
            vp = valor_pago if valor_pago > 0 else (total - soma_ant)
            if vp <= 0:
                continue
            conn.execute(
                'INSERT INTO camisetas_pagamentos (pedido_id,data_pagamento,valor,forma_pagamento,obs,criado_em,usuario) VALUES (?,?,?,?,?,?,?)',
                (pid, data_pgto, vp, forma_pgto, obs_pgto, now, usuario)
            )
            total_pago = soma_ant + vp
            if total_pago >= total:
                novo_status = 'Pago'
                total_pago = total
            else:
                novo_status = 'Parcial'
            conn.execute(
                'UPDATE camisetas_pedidos SET status_pagamento=?, forma_pagamento=?, data_pagamento=?, obs_pagamento=?, valor_pago=?, atualizado_em=? WHERE id=?',
                [novo_status, forma_pgto, data_pgto, obs_pgto, total_pago, now, pid]
            )
            auditorias.append((row['nome'], f"Pedido: {row['numero_pedido']} | Status: {novo_status} | Pagamento: R$ {vp:.2f} | Total pago: R$ {total_pago:.2f} / R$ {total:.2f} | Forma: {forma_pgto}"))
    # Auditar FORA do with get_db() para evitar deadlock
    for nome_audit, det in auditorias:
        auditar('CAM_PAGAMENTO', usuario=usuario, nome=nome_audit, detalhes=det)
    log(f'Camisetas: {len(ids)} pedido(s) pagamento registrado — {usuario}', 'success')
    return jsonify({'ok': True, 'msg': f'{len(ids)} pedido(s) atualizado(s)!'})
@app.route('/api/camisetas/comprovante/<int:pid>')
@requer_login
def api_camisetas_comprovante(pid):
    with get_db() as conn:
        row = conn.execute(
            'SELECT comprovante_base64, comprovante_tipo, comprovante_em FROM camisetas_pedidos WHERE id=?', (pid,)
        ).fetchone()
    if not row or not row['comprovante_base64']:
        return jsonify({'ok': False, 'erro': 'Sem comprovante'})
    return jsonify({'ok': True, 'base64': row['comprovante_base64'], 'tipo': row['comprovante_tipo'], 'em': row['comprovante_em']})


@app.route('/api/camisetas/pagamentos/<int:pid>')
@requer_login
def api_camisetas_pagamentos(pid):
    with get_db() as conn:
        rows = conn.execute(
            'SELECT * FROM camisetas_pagamentos WHERE pedido_id=? ORDER BY id',
            (pid,)
        ).fetchall()
    return jsonify({'ok': True, 'pagamentos': [dict(r) for r in rows]})


@app.route('/api/camisetas/estornar', methods=['POST'])
@requer_login
def api_camisetas_estornar():
    d         = request.get_json() or {}
    pgid      = int(d.get('pagamento_id') or 0)
    pedid     = int(d.get('pedido_id') or 0)
    motivo    = (d.get('motivo') or '').strip()
    if not pgid or not pedid or not motivo:
        return jsonify({'ok': False, 'erro': 'Dados incompletos'})
    now = datetime.now().strftime('%d/%m/%Y %H:%M')
    usuario = session.get('usuario', '?')
    try:
        cfg = _cfg_camisetas()
        def _val(tam):
            try: return float(cfg['valores'].get(tam) or 0)
            except: return 0.0
        with get_db() as conn:
            # Buscar pagamento original
            pg = conn.execute('SELECT * FROM camisetas_pagamentos WHERE id=? AND pedido_id=?', (pgid, pedid)).fetchone()
            if not pg:
                return jsonify({'ok': False, 'erro': 'Pagamento não encontrado'})
            if pg['valor'] <= 0:
                return jsonify({'ok': False, 'erro': 'Este lançamento já é um estorno'})
            # Verificar se já existe estorno para este pagamento
            ja_estornado = conn.execute(
                "SELECT COUNT(*) FROM camisetas_pagamentos WHERE pedido_id=? AND valor=? AND obs LIKE 'ESTORNO:%' AND id > ?",
                (pedid, -pg['valor'], pgid)
            ).fetchone()[0]
            if ja_estornado:
                return jsonify({'ok': False, 'erro': 'Este pagamento já foi estornado.'})
            # Inserir lançamento negativo
            conn.execute(
                'INSERT INTO camisetas_pagamentos (pedido_id,data_pagamento,valor,forma_pagamento,obs,criado_em,usuario) VALUES (?,?,?,?,?,?,?)',
                (pedid, datetime.now().strftime('%Y-%m-%d'), -pg['valor'], pg['forma_pagamento'], f'ESTORNO: {motivo}', now, usuario)
            )
            # Recalcular total pago
            total_pago = conn.execute('SELECT COALESCE(SUM(valor),0) FROM camisetas_pagamentos WHERE pedido_id=?', (pedid,)).fetchone()[0] or 0
            # Calcular total do pedido
            row = conn.execute('SELECT tamanho FROM camisetas_pedidos WHERE id=?', (pedid,)).fetchone()
            adds = conn.execute('SELECT tamanho FROM camisetas_adicionais WHERE pedido_id=?', (pedid,)).fetchall()
            total = _val(row['tamanho']) + sum(_val(a['tamanho']) for a in adds)
            # Determinar novo status
            if total_pago <= 0:
                novo_status = 'Realizado'
                total_pago = 0
            elif total_pago >= total:
                novo_status = 'Pago'
            else:
                novo_status = 'Parcial'
            conn.execute(
                'UPDATE camisetas_pedidos SET status_pagamento=?, valor_pago=?, atualizado_em=? WHERE id=?',
                [novo_status, total_pago, now, pedid]
            )
        auditar('CAM_ESTORNO', usuario=usuario,
                detalhes=f'Pedido {pedid} | Estorno de R$ {pg["valor"]:.2f} | Motivo: {motivo} | Novo status: {novo_status}')
        return jsonify({'ok': True, 'msg': 'Estorno registrado com sucesso!'})
    except Exception as e:
        import traceback
        log(f'ERRO estornar: {e}\n{traceback.format_exc()}', 'error')
        return jsonify({'ok': False, 'erro': str(e)}), 500


@app.route('/api/camisetas/marcar-entregue', methods=['POST'])
@requer_login
def api_camisetas_marcar_entregue():
    d            = request.get_json() or {}
    pid          = int(d.get('id', 0))
    data_entrega = (d.get('data_entrega') or datetime.now().strftime('%Y-%m-%d')).strip()
    obs_entrega  = (d.get('obs_entrega') or '').strip()
    if not pid:
        return jsonify({'ok': False, 'erro': 'ID não informado'})
    now = datetime.now().strftime('%d/%m/%Y %H:%M')
    with get_db() as conn:
        row = conn.execute('SELECT nome, numero_pedido, status_pagamento FROM camisetas_pedidos WHERE id=?', (pid,)).fetchone()
        if not row:
            return jsonify({'ok': False, 'erro': 'Pedido não encontrado'})
        if row['status_pagamento'] != 'Pago':
            return jsonify({'ok': False, 'erro': 'Só é possível marcar entregue pedidos pagos'})
        conn.execute(
            'UPDATE camisetas_pedidos SET entregue=1, data_entrega=?, obs_entrega=?, atualizado_em=? WHERE id=?',
            (data_entrega, obs_entrega, now, pid)
        )
    auditar('CAM_ENTREGA', usuario=session.get('usuario','?'),
            nome=row['nome'],
            detalhes=f"Pedido: {row['numero_pedido']} | Data entrega: {data_entrega} | Obs: {obs_entrega}")
    log(f"Camiseta pedido {pid} marcado como entregue — {session.get('usuario','?')}", 'success')
    return jsonify({'ok': True, 'msg': 'Camiseta marcada como entregue!'})

@app.route('/api/camisetas/editar', methods=['POST'])
@requer_login
def api_camisetas_editar():
    d              = request.get_json() or {}
    pid            = d.get('id')
    nome           = (d.get('nome') or '').strip().upper()
    telefone       = sanitizar_telefone(d.get('telefone', ''))
    tamanho        = (d.get('tamanho') or '').strip().upper()
    status_pgto    = (d.get('status_pagamento') or 'Realizado').strip()
    forma_pgto     = (d.get('forma_pagamento') or '').strip()
    data_pgto      = (d.get('data_pagamento') or '').strip()
    entregue       = 1 if d.get('entregue') else 0
    data_entrega   = (d.get('data_entrega') or '').strip()
    obs_cadastro   = (d.get('obs_cadastro') or '').strip()
    obs_pagamento  = (d.get('obs_pagamento') or '').strip()
    obs_entrega    = (d.get('obs_entrega') or '').strip()
    dt_nasc        = (d.get('data_nascimento') or '').strip()
    equipe         = (d.get('equipe') or '').strip()
    if not pid:
        return jsonify({'ok': False, 'erro': 'ID não informado'})
    # Bloquear edição se já tem pagamento registrado
    with get_db() as conn:
        tem_pagamento = conn.execute(
            'SELECT COUNT(*) FROM camisetas_pagamentos WHERE pedido_id=? AND valor > 0', (pid,)
        ).fetchone()[0]
    if tem_pagamento:
        return jsonify({'ok': False, 'erro': 'Edição bloqueada: pedido com pagamento registrado. Faça um estorno antes de editar.'})
    if not nome:
        return jsonify({'ok': False, 'erro': 'Nome é obrigatório'})
    if tamanho not in TAMANHOS_CAMISETA:
        return jsonify({'ok': False, 'erro': 'Tamanho inválido'})
    if status_pgto not in ('Pendente', 'Pago'):
        return jsonify({'ok': False, 'erro': 'Status inválido'})
    if entregue and status_pgto != 'Pago':
        entregue = 0; data_entrega = ''
    now = datetime.now().strftime('%d/%m/%Y %H:%M')
    with get_db() as conn:
        ant = conn.execute('SELECT * FROM camisetas_pedidos WHERE id=?', (pid,)).fetchone()
        if not ant:
            return jsonify({'ok': False, 'erro': 'Pedido não encontrado'})
        conn.execute(
            'UPDATE camisetas_pedidos SET nome=?,telefone=?,tamanho=?,status_pagamento=?,forma_pagamento=?,data_pagamento=?,entregue=?,data_entrega=?,obs_cadastro=?,obs_pagamento=?,obs_entrega=?,data_nascimento=?,equipe=?,atualizado_em=? WHERE id=?',
            (nome, telefone, tamanho, status_pgto, forma_pgto, data_pgto, entregue, data_entrega, obs_cadastro, obs_pagamento, obs_entrega, dt_nasc, equipe, now, pid)
        )
        num_pedido = dict(ant).get('numero_pedido','')
    # Detecta mudanças para auditoria
    ant_d = dict(ant)
    mudancas = []
    if ant_d.get('nome') != nome: mudancas.append(f"nome: {ant_d.get('nome')}→{nome}")
    if ant_d.get('tamanho') != tamanho: mudancas.append(f"tam: {ant_d.get('tamanho')}→{tamanho}")
    if ant_d.get('status_pagamento') != status_pgto: mudancas.append(f"status: {ant_d.get('status_pagamento')}→{status_pgto}")
    if str(ant_d.get('entregue',0)) != str(entregue): mudancas.append(f"entregue: {ant_d.get('entregue')}→{entregue}")
    auditar('CAM_EDICAO', usuario=session.get('usuario','?'),
            nome=nome,
            detalhes=f"Pedido: {num_pedido} | " + (" | ".join(mudancas) if mudancas else 'sem alterações') + f" | Obs pgto: {obs_pagamento} | Obs entrega: {obs_entrega}")
    log(f'Camiseta pedido {pid} editado — {session.get("usuario","?")}', 'info')
    return jsonify({'ok': True, 'msg': 'Pedido atualizado!'})

@app.route('/api/camisetas/upload-comprovante', methods=['POST'])
def api_camisetas_upload_comprovante():
    """Upload de comprovante de pagamento — público via CPF."""
    import base64 as _b64
    cpf  = re.sub(r'\D', '', request.form.get('cpf', ''))
    pid  = request.form.get('pid', '')
    arq  = request.files.get('comprovante')
    if not cpf or not pid or not arq:
        return jsonify({'ok': False, 'erro': 'Dados incompletos.'})
    if arq.content_length and arq.content_length > 5 * 1024 * 1024:
        return jsonify({'ok': False, 'erro': 'Arquivo muito grande. Máximo 5MB.'})
    tipo = arq.content_type or 'image/jpeg'
    if tipo not in ('image/jpeg','image/png','image/jpg','application/pdf'):
        return jsonify({'ok': False, 'erro': 'Formato inválido. Use JPG, PNG ou PDF.'})
    dados = _b64.b64encode(arq.read()).decode()
    now   = datetime.now().strftime('%d/%m/%Y %H:%M')
    with get_db() as conn:
        row = conn.execute('SELECT id,cpf FROM camisetas_pedidos WHERE id=?', (pid,)).fetchone()
        if not row or row['cpf'] != cpf:
            return jsonify({'ok': False, 'erro': 'Pedido não encontrado.'})
        conn.execute(
            'UPDATE camisetas_pedidos SET comprovante_base64=?,comprovante_tipo=?,comprovante_em=?,atualizado_em=? WHERE id=?',
            (dados, tipo, now, now, pid)
        )
    log(f'Comprovante enviado para pedido {pid}', 'success')
    return jsonify({'ok': True, 'msg': 'Comprovante enviado com sucesso!'})


@app.route('/api/camisetas/cancelar', methods=['POST'])
def api_camisetas_cancelar():
    """Cancela pedido — disponível público (via CPF) e admin (via login)."""
    d   = request.get_json() or {}
    pid = d.get('id')
    cpf = re.sub(r'\D', '', d.get('cpf', ''))

    if not pid:
        return jsonify({'ok': False, 'erro': 'ID não informado'})

    now = datetime.now().strftime('%d/%m/%Y %H:%M')
    with get_db() as conn:
        row = conn.execute('SELECT * FROM camisetas_pedidos WHERE id=?', (pid,)).fetchone()
        if not row:
            return jsonify({'ok': False, 'erro': 'Pedido não encontrado'})
        p = dict(row)

        # Acesso público: valida CPF
        eh_admin = session.get('logado', False)
        if not eh_admin:
            if not cpf or cpf != p.get('cpf',''):
                return jsonify({'ok': False, 'erro': 'CPF não confere com o pedido'})
            if p.get('status_pagamento') not in ('Pendente', 'Realizado'):
                return jsonify({'ok': False, 'erro': 'Só é possível cancelar pedidos com status Realizado ou Pendente.'})
        else:
            # Admin também não pode cancelar Pago ou Entregue
            if p.get('status_pagamento') == 'Pago':
                return jsonify({'ok': False, 'erro': 'Pedido pago não pode ser cancelado.'})

        conn.execute(
            "UPDATE camisetas_pedidos SET status_pagamento='Cancelado', atualizado_em=? WHERE id=?",
            (now, pid)
        )

    usuario = session.get('usuario', 'publico') if session.get('logado') else 'publico'
    auditar('CAM_CANCELAMENTO', usuario=usuario,
            nome=p.get('nome',''),
            detalhes=f"Pedido: {p.get('numero_pedido','')} | CPF: {p.get('cpf','')} | Status anterior: {p.get('status_pagamento','')}")
    log(f"Pedido {p.get('numero_pedido','')} cancelado por {usuario}", 'warning')
    return jsonify({'ok': True, 'msg': 'Pedido cancelado!'})


@app.route('/api/camisetas/resumo-tamanhos')
@requer_login
def api_camisetas_resumo_tamanhos():
    """Totais por tamanho (principal + adicionais)."""
    with get_db() as conn:
        peds = conn.execute('SELECT tamanho, status_pagamento FROM camisetas_pedidos').fetchall()
        adds = conn.execute(
            'SELECT ca.tamanho, cp.status_pagamento FROM camisetas_adicionais ca '
            'JOIN camisetas_pedidos cp ON ca.pedido_id=cp.id'
        ).fetchall()
    contagem = {t: {'total': 0, 'pago': 0, 'pendente': 0} for t in TAMANHOS_CAMISETA}
    for r in list(peds) + list(adds):
        if r['status_pagamento'] == 'Cancelado':
            continue  # cancelados não entram nos totais por tamanho
        t = (r['tamanho'] or '').upper()
        if t in contagem:
            contagem[t]['total'] += 1
            if r['status_pagamento'] == 'Pago':
                contagem[t]['pago'] += 1
            else:
                contagem[t]['pendente'] += 1
    cfg = _cfg_camisetas()
    return jsonify({'ok': True, 'resumo': contagem, 'tamanhos': TAMANHOS_CAMISETA, 'valores': cfg['valores']})

@app.route('/api/camisetas/config-admin', methods=['GET'])
@requer_login
def api_camisetas_config_admin_get():
    cfg = carregar_config()
    return jsonify({
        'ok': True,
        'ativo':          cfg.get('camisetas_ativo', '0'),
        'celular_comprovante': cfg.get('camisetas_celular_comprovante', ''),
        'data_inicio':    cfg.get('camisetas_data_inicio', ''),
        'data_fim':       cfg.get('camisetas_data_fim', ''),
        'chave_pix':      cfg.get('camisetas_chave_pix', ''),
        'beneficiario':   cfg.get('camisetas_beneficiario', ''),
        'descricao':      cfg.get('camisetas_descricao', ''),
        'valores':        {t: cfg.get(f'camisetas_valor_{t}', '') for t in TAMANHOS_CAMISETA},
        'tem_foto_frente': bool(cfg.get('camisetas_foto_frente')),
        'tem_foto_verso':  bool(cfg.get('camisetas_foto_verso')),
    })

@app.route('/api/camisetas/config-admin', methods=['POST'])
@requer_login
def api_camisetas_config_admin_post():
    d   = request.get_json() or {}
    cfg = carregar_config()
    campos = ['camisetas_ativo','camisetas_data_inicio','camisetas_data_fim',
              'camisetas_chave_pix','camisetas_beneficiario','camisetas_descricao',
              'camisetas_celular_comprovante']
    for c in campos:
        if c in d:
            cfg[c] = str(d[c])
    for t in TAMANHOS_CAMISETA:
        k = f'camisetas_valor_{t}'
        if k in d:
            cfg[k] = str(d[k])
    salvar_config(cfg)
    log('Configurações de camisetas salvas', 'success')
    return jsonify({'ok': True, 'msg': 'Configurações salvas!'})

@app.route('/api/camisetas/upload-foto', methods=['POST'])
@requer_login
def api_camisetas_upload_foto():
    lado = request.form.get('lado', '')
    if lado not in ('frente', 'verso'):
        return jsonify({'ok': False, 'erro': 'Lado inválido (frente ou verso)'})
    if 'foto' not in request.files:
        return jsonify({'ok': False, 'erro': 'Nenhuma foto enviada'})
    arq = request.files['foto']
    ext = arq.filename.rsplit('.', 1)[-1].lower()
    if ext not in ('jpg', 'jpeg', 'png', 'webp'):
        return jsonify({'ok': False, 'erro': 'Formato inválido. Use JPG, PNG ou WEBP'})
    import base64 as _b64
    dados = arq.read()
    mime  = 'image/jpeg' if ext in ('jpg','jpeg') else f'image/{ext}'
    b64   = f'data:{mime};base64,' + _b64.b64encode(dados).decode()
    cfg   = carregar_config()
    cfg[f'camisetas_foto_{lado}'] = b64
    salvar_config(cfg)
    log(f'Foto {lado} da camiseta atualizada', 'success')
    return jsonify({'ok': True, 'msg': f'Foto {lado} salva!'})


# ══════════════════════════════════════════════════════════
#  WEBHOOK
# ══════════════════════════════════════════════════════════

@app.route("/api/respostas-rapidas", methods=["GET"])
@requer_login
def api_respostas_rapidas_get():
    cfg = carregar_config()
    return jsonify({"ok": True, "respostas": cfg.get("respostas_rapidas", [])})

@app.route("/api/respostas-rapidas", methods=["POST"])
@requer_login
def api_respostas_rapidas_post():
    d = request.get_json() or {}
    cfg = carregar_config()
    cfg["respostas_rapidas"] = d.get("respostas", [])
    salvar_config(cfg)
    return jsonify({"ok": True})


@app.route("/webhook/receber", methods=["POST"])
def webhook_receber():
    try:
        from_raw=request.form.get("From",""); body=request.form.get("Body","").strip()
        num_media=int(request.form.get("NumMedia",0)); numero=numero_limpo(from_raw)
        midias=[{"url":request.form.get(f"MediaUrl{i}",""),"tipo":request.form.get(f"MediaContentType{i}","")}
                for i in range(num_media) if request.form.get(f"MediaUrl{i}","")]
        inbox=carregar_inbox()
        if numero not in inbox: inbox[numero]={"msgs":[],"nome":""}
        inbox[numero]["msgs"].append({"de":numero,"texto":body,"midias":midias,
            "hora":datetime.now().strftime("%d/%m/%Y %H:%M:%S"),"lida":False})
        inbox[numero]["msgs"]=inbox[numero]["msgs"][-50:]
        salvar_inbox(inbox)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📩 Msg de +{numero}: {'[mídia]' if midias else body[:60]}")
    except Exception as e: print(f"Erro webhook: {e}")
    return '<?xml version="1.0" encoding="UTF-8"?><Response></Response>',200,{"Content-Type":"text/xml"}

# ══════════════════════════════════════════════════════════
#  INIT
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    _init_db_status = ""
    init_db()
    # Registra status do banco na auditoria
    try:
        auditar("SISTEMA_INIT", usuario="sistema", detalhes=f"Banco de dados: {_init_db_status}")
    except: pass
    # Carregar log do SQLite na memória
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT hora,msg,tipo FROM log_atividades ORDER BY id DESC LIMIT 200").fetchall()
            estado["log"] = [{"hora": r[0], "msg": r[1], "tipo": r[2]} for r in rows]
    except: pass
    threading.Thread(target=run_scheduler, daemon=True).start()
    log("Bingo v2.6.0 iniciado!","success")
    app.run(host="0.0.0.0", port=60080, debug=False, threaded=True)
else:
    _init_db_status = ""
    init_db()
    try:
        auditar("SISTEMA_INIT", usuario="sistema", detalhes=f"Banco de dados: {_init_db_status}")
    except: pass
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT hora,msg,tipo FROM log_atividades ORDER BY id DESC LIMIT 200").fetchall()
            estado["log"] = [{"hora": r[0], "msg": r[1], "tipo": r[2]} for r in rows]
    except: pass
