"""
painel.py  -  Painel de Controle  |  CPFL Macro (ValidaPN GMP)
==============================================================
Botão liga/desliga para o orquestrador contínuo da macro CPFL.

Uso:
    python painel.py
    (ou: PAINEL.bat)
"""
import tkinter as tk
from tkinter import font as tkfont
import subprocess
import threading
import queue
import os
import sys
import shutil
import time
import re
from pathlib import Path


# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------
HERE      = Path(__file__).resolve().parent
SCRIPT    = HERE / "executar_automatico.py"

DADOS_DIR     = HERE.parent / "dados_cpfl"
RESULTADO_CSV = DADOS_DIR / "resultado_lote.csv"
PROJETO_DIR   = HERE.parents[1]
ETL_RETORNO   = PROJETO_DIR / "etl" / "load" / "macro_cpfl" / "04_processar_retorno_cpfl.py"

_sys_python = shutil.which("python") or shutil.which("python3") or sys.executable


# ---------------------------------------------------------------------------
# Paleta de cores (idêntica ao painel neoenergia)
# ---------------------------------------------------------------------------
COR_BG      = "#1e1e2e"
COR_PAINEL  = "#2a2a3e"
COR_VERDE   = "#40c057"
COR_CINZA   = "#495057"
COR_HOVER_V = "#2f9e44"
COR_HOVER_C = "#343a40"
COR_TEXTO   = "#ced4da"
COR_LOG_BG  = "#12121f"
COR_LOG_FG  = "#d0d0d0"
COR_TITULO  = "#74c0fc"
COR_AVISO   = "#ffd43b"
COR_ERRO    = "#ff6b6b"
COR_OK      = "#69db7c"
COR_PARANDO = "#e67700"
COR_INPUT_BG= "#2c2c42"
COR_INPUT_FG= "#e0e0e0"


# ---------------------------------------------------------------------------
# Classe principal
# ---------------------------------------------------------------------------
class PainelCPFL:
    def __init__(self, root: tk.Tk):
        self.root     = root
        self.processo = None
        self.rodando  = False
        self.parando  = False
        self._q       = queue.Queue()
        self._thread  = None

        self.ciclos = 0
        self.ok     = 0
        self.erros  = 0
        self.inicio = None

        self._construir_ui()
        self._poll_queue()
        self._atualizar_timer()

        root.protocol("WM_DELETE_WINDOW", self._fechar)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _construir_ui(self):
        self.root.title("CPFL  -  Orquestrador Macro (ValidaPN GMP)")
        self.root.configure(bg=COR_BG)
        self.root.resizable(True, True)
        self.root.minsize(640, 560)

        f_titulo = tkfont.Font(family="Segoe UI", size=13, weight="bold")
        f_btn    = tkfont.Font(family="Segoe UI", size=28, weight="bold")
        f_status = tkfont.Font(family="Segoe UI", size=11, weight="bold")
        f_label  = tkfont.Font(family="Segoe UI", size=9)
        f_mono   = tkfont.Font(family="Consolas",  size=9)

        # Título
        tk.Label(
            self.root, text="CPFL  -  Orquestrador Macro (ValidaPN GMP)",
            bg=COR_BG, fg=COR_TITULO, font=f_titulo, pady=10
        ).pack(fill=tk.X)

        # Botão toggle
        frm_btn = tk.Frame(self.root, bg=COR_BG, pady=6)
        frm_btn.pack()

        self.btn = tk.Button(
            frm_btn,
            text="DESLIGADO",
            font=f_btn,
            bg=COR_CINZA, fg="white",
            activebackground=COR_HOVER_C, activeforeground="white",
            relief="flat", bd=0,
            padx=50, pady=18,
            cursor="hand2",
            command=self._toggle
        )
        self.btn.pack()
        self.btn.bind("<Enter>", self._btn_hover)
        self.btn.bind("<Leave>", self._btn_leave)

        # Status
        self.var_status = tk.StringVar(value="Aguardando...")
        tk.Label(
            self.root, textvariable=self.var_status,
            bg=COR_BG, fg=COR_TEXTO, font=f_status, pady=2
        ).pack()

        # Configurações
        frm_cfg = tk.Frame(self.root, bg=COR_PAINEL, padx=16, pady=10)
        frm_cfg.pack(fill=tk.X, padx=20, pady=(8, 4))

        self._label_entry(frm_cfg, "Lote (registros):", "500",  0, "var_tam")
        self._label_entry(frm_cfg, "Pausa (segundos):", "60",   1, "var_pausa")
        self._label_entry(frm_cfg, "Max erros seguidos:", "3",  2, "var_erros")

        # Contadores
        frm_stats = tk.Frame(self.root, bg=COR_BG)
        frm_stats.pack(fill=tk.X, padx=20, pady=4)

        self.var_ciclos = tk.StringVar(value="Ciclos: 0")
        self.var_ok     = tk.StringVar(value="OK: 0")
        self.var_err    = tk.StringVar(value="Erros: 0")
        self.var_tempo  = tk.StringVar(value="Tempo: --:--:--")

        for var, cor in [
            (self.var_ciclos, COR_TEXTO),
            (self.var_ok,     COR_OK),
            (self.var_err,    COR_ERRO),
            (self.var_tempo,  COR_AVISO),
        ]:
            tk.Label(
                frm_stats, textvariable=var, bg=COR_BG, fg=cor,
                font=tkfont.Font(family="Segoe UI", size=10, weight="bold"),
                padx=14
            ).pack(side=tk.LEFT)

        # Log
        frm_log = tk.Frame(self.root, bg=COR_BG)
        frm_log.pack(fill=tk.BOTH, expand=True, padx=20, pady=(4, 14))

        tk.Label(
            frm_log, text="Log de saída", bg=COR_BG, fg=COR_TEXTO,
            font=f_label, anchor="w"
        ).pack(fill=tk.X)

        self.log = tk.Text(
            frm_log,
            bg=COR_LOG_BG, fg=COR_LOG_FG,
            font=f_mono,
            relief="flat", bd=0,
            state=tk.DISABLED,
            wrap=tk.WORD,
        )
        sb = tk.Scrollbar(frm_log, command=self.log.yview)
        self.log.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.log.pack(fill=tk.BOTH, expand=True)

        self.log.tag_configure("ok",     foreground=COR_OK)
        self.log.tag_configure("erro",   foreground=COR_ERRO)
        self.log.tag_configure("aviso",  foreground=COR_AVISO)
        self.log.tag_configure("titulo", foreground=COR_TITULO)
        self.log.tag_configure("normal", foreground=COR_LOG_FG)

    def _label_entry(self, parent, label, default, col, attr):
        f = tkfont.Font(family="Segoe UI", size=9)
        tk.Label(
            parent, text=label, bg=COR_PAINEL, fg=COR_TEXTO, font=f
        ).grid(row=0, column=col * 2, sticky="e", padx=(12, 4))
        var = tk.StringVar(value=default)
        setattr(self, attr, var)
        tk.Entry(
            parent, textvariable=var, width=6,
            bg=COR_INPUT_BG, fg=COR_INPUT_FG,
            insertbackground=COR_INPUT_FG,
            relief="flat", font=f
        ).grid(row=0, column=col * 2 + 1, padx=(0, 8))

    # ------------------------------------------------------------------
    # Toggle ON / OFF
    # ------------------------------------------------------------------
    def _toggle(self):
        if self.parando:
            return
        if self.rodando:
            self._parar()
        else:
            self._iniciar()

    def _iniciar(self):
        try:
            tamanho = int(self.var_tam.get())
            pausa   = int(self.var_pausa.get())
            max_err = int(self.var_erros.get())
        except ValueError:
            self._log_append("Valores de configuração inválidos.\n", "erro")
            return

        if not SCRIPT.exists():
            self._log_append(f"Script não encontrado: {SCRIPT}\n", "erro")
            return

        cmd = [
            _sys_python, "-u", str(SCRIPT),
            "--continuar",
            "--tamanho",   str(tamanho),
            "--pausa",     str(pausa),
            "--max-erros", str(max_err),
        ]

        self._log_append(f">>> Iniciando: {' '.join(cmd[2:])}\n", "titulo")

        self.ciclos = 0
        self.ok     = 0
        self.erros  = 0
        self.inicio = time.time()
        self._atualizar_contadores()

        try:
            self.processo = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(HERE),
                encoding="utf-8",
                errors="replace",
                env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
            )
        except Exception as e:
            self._log_append(f"Erro ao iniciar processo: {e}\n", "erro")
            return

        self.rodando = True
        self._atualizar_btn()

        self._thread = threading.Thread(target=self._ler_stdout, daemon=True)
        self._thread.start()

    def _matar_processo_tree(self):
        if not self.processo:
            return
        pid = self.processo.pid
        try:
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(pid)],
                capture_output=True
            )
        except Exception:
            try:
                self.processo.terminate()
            except Exception:
                pass

    def _salvar_no_banco(self, motivo="parada"):
        if not RESULTADO_CSV.exists() or RESULTADO_CSV.stat().st_size < 10:
            self._log_append(f"[BANCO] Nenhum resultado para salvar ({motivo}).\n", "aviso")
            return
        if not ETL_RETORNO.exists():
            self._log_append(f"[BANCO] Script ETL não encontrado: {ETL_RETORNO}\n", "erro")
            return

        self._log_append(f"[BANCO] Salvando resultados no banco ({motivo})...\n", "aviso")
        self.var_status.set("Salvando no banco...")

        def _run():
            try:
                r = subprocess.run(
                    [_sys_python, "-u", str(ETL_RETORNO)],
                    cwd=str(PROJETO_DIR),
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=300,
                )
                if r.stdout:
                    self._q.put(f"[BANCO] Saída ETL:\n{r.stdout[-800:]}\n")
                if r.returncode == 0:
                    self._q.put("[BANCO] Resultados salvos com sucesso.\n")
                else:
                    self._q.put(f"[BANCO][ERRO] ETL encerrou com código {r.returncode}\n{r.stderr[-400:]}\n")
            except subprocess.TimeoutExpired:
                self._q.put("[BANCO][ERRO] Timeout ao salvar no banco (300s).\n")
            except Exception as e:
                self._q.put(f"[BANCO][ERRO] {e}\n")

        threading.Thread(target=_run, daemon=True).start()

    def _parar(self):
        if self.processo and self.processo.poll() is None:
            self._log_append(">>> Parando — encerrando processos...\n", "aviso")
            self._matar_processo_tree()
        self.parando = True
        self.rodando = False
        self._atualizar_btn()
        self._salvar_no_banco(motivo="parada pelo usuário")

    def _ler_stdout(self):
        try:
            for line in self.processo.stdout:
                self._q.put(line)
        except Exception:
            pass
        finally:
            self.processo.wait()
            self._q.put(None)

    # ------------------------------------------------------------------
    # Polling da fila (thread principal)
    # ------------------------------------------------------------------
    def _poll_queue(self):
        try:
            while True:
                item = self._q.get_nowait()
                if item is None:
                    self.rodando = False
                    self.parando = False
                    self._atualizar_btn()
                    self.var_status.set("Encerrado.")
                    self._log_append(">>> Orquestrador encerrado.\n", "aviso")
                    self._salvar_no_banco(motivo="fim de ciclo")
                else:
                    self._processar_linha(item)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)

    def _processar_linha(self, linha: str):
        l = linha.rstrip("\n")

        if re.search(r"CICLO #\d+", l):
            self.ciclos += 1
            self._atualizar_contadores()
            self._log_append(linha, "titulo")
            return
        if re.search(r"\[OK\] Ciclo #\d+ conclu|Ciclo completo", l, re.I):
            self.ok += 1
            self._atualizar_contadores()
            self._log_append(linha, "ok")
            return
        if re.search(r"Erros consecutivos|FALHA|erro_fatal|Limite de erros", l, re.I):
            self.erros += 1
            self._atualizar_contadores()
            self._log_append(linha, "erro")
            return

        if re.search(r"\[PROG\]|\[INFO\] Total|Lote obtido", l):
            self._log_append(linha, "aviso")
            return
        if re.search(r"PASSO \d+/3", l):
            self._log_append(linha, "titulo")
            return
        if re.search(r"\[OK\]|sucesso|ativo|salvo", l, re.I):
            self._log_append(linha, "ok")
            return
        if re.search(r"\[ERRO\]|\[AVISO\]|AVISO|error|falha|excep", l, re.I):
            tag = "erro" if re.search(r"\[ERRO\]|error|falha|excep", l, re.I) else "aviso"
            self._log_append(linha, tag)
            return
        if re.search(r"\[BANCO\]", l):
            self._log_append(linha, "aviso")
            return

        self._log_append(linha, "normal")

    def _log_append(self, text: str, tag: str = "normal"):
        self.log.configure(state=tk.NORMAL)
        ts  = time.strftime("%H:%M:%S")
        lf  = text if text.endswith("\n") else text + "\n"
        self.log.insert(tk.END, f"[{ts}] {lf}", tag)
        linhas = int(self.log.index("end-1c").split(".")[0])
        if linhas > 2000:
            self.log.delete("1.0", f"{linhas - 1800}.0")
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    # ------------------------------------------------------------------
    # Helpers UI
    # ------------------------------------------------------------------
    def _atualizar_btn(self):
        if self.rodando:
            self.btn.configure(text="LIGADO",    bg=COR_VERDE,   activebackground=COR_HOVER_V, state=tk.NORMAL)
            self.var_status.set("Rodando em modo contínuo...")
        elif self.parando:
            self.btn.configure(text="PARANDO...", bg=COR_PARANDO, activebackground=COR_PARANDO, state=tk.DISABLED)
            self.var_status.set("Aguardando o lote atual terminar...")
        else:
            self.btn.configure(text="DESLIGADO", bg=COR_CINZA,   activebackground=COR_HOVER_C, state=tk.NORMAL)
            self.var_status.set("Parado." if self.inicio else "Aguardando...")

    def _atualizar_contadores(self):
        self.var_ciclos.set(f"Ciclos: {self.ciclos}")
        self.var_ok.set(f"OK: {self.ok}")
        self.var_err.set(f"Erros: {self.erros}")

    def _atualizar_timer(self):
        if self.rodando and self.inicio:
            seg = int(time.time() - self.inicio)
            h, r = divmod(seg, 3600)
            m, s = divmod(r, 60)
            self.var_tempo.set(f"Tempo: {h:02d}:{m:02d}:{s:02d}")
        self.root.after(1000, self._atualizar_timer)

    def _btn_hover(self, _):
        if self.parando:
            return
        self.btn.configure(bg=COR_HOVER_V if self.rodando else COR_HOVER_C)

    def _btn_leave(self, _):
        self._atualizar_btn()

    def _fechar(self):
        if self.rodando or self.parando:
            self._matar_processo_tree()
        deadline = time.time() + 5
        def _aguardar():
            if self.processo and self.processo.poll() is None and time.time() < deadline:
                self.root.after(300, _aguardar)
            else:
                self.root.destroy()
        self.root.after(300, _aguardar)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app  = PainelCPFL(root)
    root.mainloop()
