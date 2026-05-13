#!/usr/bin/env python3
"""
simple_runner.py - Executor Simples para Macro CPFL
===================================================
Interface minimalista para iniciar/parar o orquestrador da macro CPFL.

- Mostra status (ativo/inativo)
- Lote atual (registros em processamento)
- Processados hoje
- Projeção diária (baseada na última hora)
- Botão para iniciar/parar

Uso:
    python simple_runner.py
    # Para exe: pyinstaller --onefile simple_runner.py
"""

import tkinter as tk
from tkinter import messagebox
import subprocess
import threading
import sys
import os
import shutil
from pathlib import Path
import pymysql
from datetime import datetime, timedelta

# Caminhos
HERE = Path(__file__).resolve().parent
SCRIPT_ORQUESTRADOR = HERE / "executar_automatico.py"
PYTHON_EXE = sys.executable

# Para exe, usar python3 do sistema para subprocess
if getattr(sys, 'frozen', False):
    PYTHON_EXE = shutil.which('python3') or shutil.which('python') or 'python3'
    # Ajustar caminhos para exe
    PROJETO_DIR = HERE.parent.parent.parent
    SCRIPT_ORQUESTRADOR = PROJETO_DIR / "macro" / "macro_cpfl" / "executar_automatico.py"
    sys.path.insert(0, str(PROJETO_DIR))

class SimpleRunner:
    def __init__(self, root):
        self.root = root
        self.root.title("Macro CPFL - Executor Simples")
        self.root.geometry("350x200")
        self.root.resizable(False, False)

        # Status
        self.status_label = tk.Label(root, text="Status: Inativo", font=("Arial", 12))
        self.status_label.pack(pady=10)

        # Lote atual
        self.lote_label = tk.Label(root, text="Lote atual: Nenhum", font=("Arial", 10))
        self.lote_label.pack(pady=5)

        # Processados hoje
        self.hoje_label = tk.Label(root, text="Processados hoje: 0", font=("Arial", 10))
        self.hoje_label.pack(pady=5)

        # Projeção diária
        self.proj_label = tk.Label(root, text="Projeção diária: 0", font=("Arial", 10))
        self.proj_label.pack(pady=5)

        # Botão
        self.button = tk.Button(root, text="Iniciar", command=self.toggle, font=("Arial", 12), bg="green", fg="white")
        self.button.pack(pady=10)

        # Processo
        self.process = None
        self.running = False

        # Atualizar status periodicamente
        self.update_status()

    def toggle(self):
        if self.running:
            self.stop()
        else:
            self.start()

    def start(self):
        if self.running:
            return
        try:
            self.process = subprocess.Popen([PYTHON_EXE, str(SCRIPT_ORQUESTRADOR), "--tamanho", "300", "--max-erros", "3"],
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=HERE.parent)
            self.running = True
            self.button.config(text="Parar", bg="red")
            self.status_label.config(text="Status: Ativo")
            messagebox.showinfo("Iniciado", "Orquestrador da macro CPFL iniciado!")
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao iniciar: {e}")

    def stop(self):
        if not self.running or not self.process:
            return
        try:
            self.process.terminate()
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
        self.running = False
        self.button.config(text="Iniciar", bg="green")
        self.status_label.config(text="Status: Inativo")
        messagebox.showinfo("Parado", "Orquestrador parado!")

    def update_status(self):
        try:
            from config import db_cpfl
            conn = pymysql.connect(**db_cpfl())
            cursor = conn.cursor()

            # Lote atual: registros em processando
            cursor.execute("SELECT COUNT(*) FROM tabela_macros_cpfl WHERE status = 'processando'")
            lote_atual = cursor.fetchone()[0]
            self.lote_label.config(text=f"Lote atual: {lote_atual} registros")

            # Processados hoje
            hoje = datetime.now().date()
            cursor.execute("SELECT COUNT(*) FROM tabela_macros_cpfl WHERE DATE(data_extracao) = %s", (hoje,))
            hoje_count = cursor.fetchone()[0]
            self.hoje_label.config(text=f"Processados hoje: {hoje_count}")

            # Projeção diária: processados na última hora * 24
            uma_hora_atras = datetime.now() - timedelta(hours=1)
            cursor.execute("SELECT COUNT(*) FROM tabela_macros_cpfl WHERE data_extracao >= %s", (uma_hora_atras,))
            ult_hora = cursor.fetchone()[0]
            proj = ult_hora * 24
            self.proj_label.config(text=f"Projeção diária: {proj}")

            cursor.close()
            conn.close()
        except Exception as e:
            error_msg = f"Erro DB: {str(e)}"
            self.lote_label.config(text="Lote atual: Erro DB")
            self.hoje_label.config(text="Processados hoje: Erro DB")
            self.proj_label.config(text="Projeção diária: Erro DB")
            messagebox.showerror("Erro de Banco", error_msg)

        if self.process:
            if self.process.poll() is None:
                self.running = True
                self.status_label.config(text="Status: Ativo")
                self.button.config(text="Parar", bg="red")
            else:
                self.running = False
                self.status_label.config(text="Status: Inativo")
                self.button.config(text="Iniciar", bg="green")
        self.root.after(5000, self.update_status)  # Atualizar a cada 5s

if __name__ == "__main__":
    root = tk.Tk()
    app = SimpleRunner(root)
    root.mainloop()