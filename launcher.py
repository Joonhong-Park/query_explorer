"""
Query Explorer Launcher
SSH 터널 연결 + 브라우저 자동 오픈 (Windows .exe용)

빌드:
    pip install paramiko pyinstaller
    pyinstaller --onefile --noconsole --name QueryExplorer launcher.py
    → dist/QueryExplorer.exe 생성
"""

import json
import os
import select
import socket
import threading
import webbrowser
import time
import tkinter as tk
from tkinter import messagebox
from pathlib import Path

import paramiko

# ── 서버 설정 (실제 값으로 변경 후 빌드) ─────────────────────────────────────
TUNNEL_HOST = "tunnel_server"
TUNNEL_PORT = 22
TUNNEL_USER = "tunnel_user"

NODE_HOST   = "node1"
NODE_PORT   = 22
NODE_USER   = "node_user"

LOCAL_PORT  = 9090
REMOTE_PORT = 9090
APP_URL     = f"http://localhost:{LOCAL_PORT}"
# ─────────────────────────────────────────────────────────────────────────────

# 비밀번호 저장 경로: %APPDATA%\QueryExplorer\credentials.json
CRED_PATH = Path(os.environ.get("APPDATA", "~")) / "QueryExplorer" / "credentials.json"


def load_credentials() -> dict:
    try:
        return json.loads(CRED_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_credentials(tunnel_pw: str, node_pw: str):
    CRED_PATH.parent.mkdir(parents=True, exist_ok=True)
    CRED_PATH.write_text(
        json.dumps({"tunnel_pw": tunnel_pw, "node_pw": node_pw}, ensure_ascii=False),
        encoding="utf-8",
    )


def clear_credentials():
    try:
        CRED_PATH.unlink()
    except Exception:
        pass


# ── SSH 터널 ──────────────────────────────────────────────────────────────────

def _forward_handler(local_sock, transport):
    try:
        chan = transport.open_channel(
            "direct-tcpip",
            ("localhost", REMOTE_PORT),
            local_sock.getpeername(),
        )
    except Exception:
        local_sock.close()
        return

    while True:
        r, _, _ = select.select([local_sock, chan], [], [], 1.0)
        if local_sock in r:
            data = local_sock.recv(4096)
            if not data:
                break
            chan.sendall(data)
        if chan in r:
            data = chan.recv(4096)
            if not data:
                break
            local_sock.sendall(data)

    chan.close()
    local_sock.close()


class TunnelManager:
    def __init__(self):
        self.tunnel_client = None
        self.node_client   = None
        self._stop         = threading.Event()

    def connect(self, tunnel_pw, node_pw):
        self.tunnel_client = paramiko.SSHClient()
        self.tunnel_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.tunnel_client.connect(
            TUNNEL_HOST, port=TUNNEL_PORT,
            username=TUNNEL_USER, password=tunnel_pw,
            timeout=15,
        )

        ch = self.tunnel_client.get_transport().open_channel(
            "direct-tcpip", (NODE_HOST, NODE_PORT), ("127.0.0.1", 0)
        )

        self.node_client = paramiko.SSHClient()
        self.node_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.node_client.connect(
            NODE_HOST, username=NODE_USER, password=node_pw,
            sock=ch, timeout=15,
        )

        self._start_forward(self.node_client.get_transport())

    def _start_forward(self, transport):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        srv.bind(("localhost", LOCAL_PORT))
        srv.listen(10)
        srv.settimeout(1.0)

        def loop():
            while not self._stop.is_set():
                try:
                    conn, _ = srv.accept()
                    threading.Thread(
                        target=_forward_handler,
                        args=(conn, transport),
                        daemon=True,
                    ).start()
                except socket.timeout:
                    continue
            srv.close()

        threading.Thread(target=loop, daemon=True).start()

    def disconnect(self):
        self._stop.set()
        for client in (self.node_client, self.tunnel_client):
            if client:
                try:
                    client.close()
                except Exception:
                    pass


# ── GUI ───────────────────────────────────────────────────────────────────────

BG       = "#1a1d2e"
FG       = "#e0e0e0"
ENTRY_BG = "#0f1117"
ACCENT   = "#7eb8f7"
BTN_BG   = "#3d5afe"


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Query Explorer")
        self.configure(bg=BG)
        self.resizable(False, False)
        self.tunnel    = TunnelManager()
        self._connected = False
        self._build_ui()
        self._load_saved()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        tk.Label(self, text="⚡ Query Explorer", bg=BG, fg=ACCENT,
                 font=("Segoe UI", 15, "bold")).pack(pady=(24, 2))
        tk.Label(self, text="SSH 터널 연결", bg=BG, fg="#666",
                 font=("Segoe UI", 9)).pack(pady=(0, 18))

        frame = tk.Frame(self, bg=BG)
        frame.pack(padx=36)

        self.e_tunnel_pw = self._entry_row(
            frame, f"터널링 서버 비밀번호  ({TUNNEL_USER}@{TUNNEL_HOST})"
        )
        self.e_node_pw = self._entry_row(
            frame, f"node1 비밀번호  ({NODE_USER}@{NODE_HOST})"
        )

        # 비밀번호 저장 체크박스
        self._save_var = tk.BooleanVar(value=False)
        chk_frame = tk.Frame(self, bg=BG)
        chk_frame.pack(padx=36, fill="x")
        self._chk = tk.Checkbutton(
            chk_frame, text="비밀번호 저장", variable=self._save_var,
            bg=BG, fg="#aaa", selectcolor=ENTRY_BG,
            activebackground=BG, activeforeground=FG,
            font=("Segoe UI", 9), command=self._on_save_toggle,
        )
        self._chk.pack(side="left")
        self._btn_clear = tk.Button(
            chk_frame, text="저장 삭제", bg="#2a2d3e", fg="#888",
            relief="flat", font=("Segoe UI", 8), cursor="hand2",
            command=self._clear_saved,
        )
        # 저장된 값 있을 때만 표시
        self._btn_clear_visible = False

        self.btn = tk.Button(
            self, text="연결 및 브라우저 열기",
            bg=BTN_BG, fg="white", activebackground="#536dfe",
            font=("Segoe UI", 10, "bold"), relief="flat",
            cursor="hand2", pady=8, command=self._connect,
        )
        self.btn.pack(pady=(12, 6), padx=36, fill="x")

        self.lbl_status = tk.Label(self, text="", bg=BG, fg="#888",
                                   font=("Segoe UI", 9), wraplength=280)
        self.lbl_status.pack(pady=(0, 24))

        self.bind("<Return>", lambda _: self._connect())

    def _entry_row(self, parent, label):
        tk.Label(parent, text=label, bg=BG, fg="#aaa",
                 font=("Segoe UI", 9), justify="left", anchor="w").pack(fill="x")
        e = tk.Entry(parent, bg=ENTRY_BG, fg=FG, insertbackground=FG,
                     font=("Segoe UI", 10), relief="flat",
                     highlightthickness=1, highlightbackground="#2e3148",
                     width=32, show="*")
        e.pack(pady=(3, 12), ipady=6)
        return e

    def _load_saved(self):
        """저장된 비밀번호가 있으면 자동으로 채움"""
        creds = load_credentials()
        if creds.get("tunnel_pw") and creds.get("node_pw"):
            self.e_tunnel_pw.insert(0, creds["tunnel_pw"])
            self.e_node_pw.insert(0, creds["node_pw"])
            self._save_var.set(True)
            self._btn_clear.pack(side="right", padx=(8, 0))
            self._btn_clear_visible = True
            self._set_status("저장된 비밀번호를 불러왔습니다", "#888")

    def _on_save_toggle(self):
        """체크 해제 시 저장 파일 삭제"""
        if not self._save_var.get():
            clear_credentials()
            self._btn_clear.pack_forget()
            self._btn_clear_visible = False

    def _clear_saved(self):
        clear_credentials()
        self._save_var.set(False)
        self._btn_clear.pack_forget()
        self._btn_clear_visible = False
        self.e_tunnel_pw.delete(0, "end")
        self.e_node_pw.delete(0, "end")
        self._set_status("저장된 비밀번호를 삭제했습니다", "#888")

    def _connect(self):
        if self._connected:
            webbrowser.open(APP_URL)
            return

        tunnel_pw = self.e_tunnel_pw.get()
        node_pw   = self.e_node_pw.get()

        if not tunnel_pw or not node_pw:
            messagebox.showwarning("입력 오류", "비밀번호를 모두 입력해주세요.")
            return

        # 저장 체크 시 연결 전에 저장
        if self._save_var.get():
            save_credentials(tunnel_pw, node_pw)
            if not self._btn_clear_visible:
                self._btn_clear.pack(side="right", padx=(8, 0))
                self._btn_clear_visible = True

        self.btn.config(state="disabled", text="연결 중...")
        self._set_status("터널링 서버에 연결 중...", "#ffa726")

        def work():
            try:
                self.tunnel.connect(tunnel_pw, node_pw)
                time.sleep(0.5)
                self.after(0, self._on_connected)
            except Exception as ex:
                self.after(0, lambda: self._on_error(str(ex)))

        threading.Thread(target=work, daemon=True).start()

    def _on_connected(self):
        self._connected = True
        self._set_status(f"연결됨  {APP_URL}", "#66bb6a")
        self.btn.config(state="normal", text="브라우저 다시 열기")
        webbrowser.open(APP_URL)

    def _on_error(self, msg):
        self._connected = False
        self._set_status(f"오류: {msg}", "#ef5350")
        self.btn.config(state="normal", text="연결 및 브라우저 열기")

    def _set_status(self, msg, color):
        self.lbl_status.config(text=msg, fg=color)

    def _on_close(self):
        self.tunnel.disconnect()
        self.destroy()


if __name__ == "__main__":
    App().mainloop()
