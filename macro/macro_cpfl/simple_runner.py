#!/usr/bin/env python3
"""
simple_runner.py - Executor Simples para Macro CPFL
===================================================
Interface minimalista para iniciar/parar o orquestrador da macro CPFL.

- Mostra status (ativo/inativo)
- Botão para iniciar/parar
- Executa em background
- Leve e cross-platform

Uso:
    python simple_runner.py
    # Para executável: pyinstaller --onefile --windowed simple_runner.py
"""

import tkinter as tk
from tkinter import messagebox
import subprocess
import threading
import sys
import os
from pathlib import Path

# Caminhos
HERE = Path(__file__).resolve().parent
SCRIPT_ORQUESTRADOR = HERE / "executar_automatico.py"
PYTHON_EXE = sys.executable

class SimpleRunner:
    def __init__(self, root):
        self.root = root
        self.root.title("Macro CPFL - Executor Simples")
        self.root.geometry("300x150")
        self.root.resizable(False, False)

        # Status
        self.status_label = tk.Label(root, text="Status: Inativo", font=("Arial", 12))
        self.status_label.pack(pady=20)

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
        if self.process:
            if self.process.poll() is None:
                self.running = True
                self.status_label.config(text="Status: Ativo")
                self.button.config(text="Parar", bg="red")
            else:
                self.running = False
                self.status_label.config(text="Status: Inativo")
                self.button.config(text="Iniciar", bg="green")
        self.root.after(2000, self.update_status)  # Atualizar a cada 2s

if __name__ == "__main__":
    root = tk.Tk()
    app = SimpleRunner(root)
    root.mainloop()</content>
<parameter name="filePath">c:\Users\marti\Desktop\Projetos Martina\projeto_orquestracao_cpfl\macro\macro_cpfl\simple_runner.py