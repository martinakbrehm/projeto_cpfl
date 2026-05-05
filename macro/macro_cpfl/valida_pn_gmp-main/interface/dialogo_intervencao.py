import tkinter as tk
from tkinter import ttk

class JanelaIntervencao:
    def __init__(self, parent, mensagem, notificador):
        self.parent = parent # parent é a janela principal
        self.mensagem = mensagem
        self.notificador = notificador
        self.decisao = None
        self.alerta_ativo = True

        self._criar_janela()

    
    def _criar_janela(self):
        self.top = tk.Toplevel(self.parent) # Cria uma janela filha da janela principal
        self.top.title("Intervenção Necessária")
        self.top.geometry("420x240")
        self.top.resizable(False, False) # Não permite redimensionar a janela (primeiro parâmetro largura, segundo altura)

        # Mantém na frente
        self.top.transient(self.parent) # Mantém a janela filha na frente da janela principal
        self.top.grab_set() # Bloqueia a interação com a janela principal
        self.top.focus_set() # Coloca o foco na janela filha
        
        # Centralizar
        self._centralizar()

        # Layout
        frame = ttk.Frame(self.top, padding=20)
        frame.pack(fill="both", expand=True) # O frame preenche a janela filha (fill="both") e se redimensiona (expand=True)

        label = ttk.Label(frame, text=self.mensagem, wraplength=360, justify="center")
        label.pack(pady=(0, 20))

        # Sino / alerta
        self.btn_alerta = ttk.Button(frame, text="🔔 Silenciar Alerta", command=self._alternar_alerta)
        self.btn_alerta.pack(pady=(0, 15))

        # Botões de decisão
        botoes_frame = ttk.Frame(frame)
        botoes_frame.pack(pady=10)

        ttk.Button(botoes_frame, 
                   text="Continuar", 
                   command=lambda: self._finalizar("continuar"),
                   width=12
        ).grid(row=0, column=0, padx=5) # grid() é usado para organizar os widgets em uma grade

        ttk.Button(botoes_frame,
                   text="Relogar",
                   command=lambda: self._finalizar("relogar"),
                   width=12
        ).grid(row=0, column=1, padx=5)

        ttk.Button(botoes_frame,
                   text="Reiniciar",
                   command=lambda: self._finalizar("reiniciar"),
                   width=12
        ).grid(row=1, column=0, padx=5, pady=8)

        ttk.Button(botoes_frame,
                   text="Parar",
                   command=lambda: self._finalizar("parar"),
                   width=12
        ).grid(row=1, column=1, padx=5, pady=8)

        self.top.protocol("WM_DELETE_WINDOW", lambda: None) # Desativa o botão de fechar da janela filha

    def _alternar_alerta(self):
        if self.alerta_ativo:
            self.notificador.parar_alerta()
            self.btn_alerta.config(text="🔕 Ativar Alerta")
            self.alerta_ativo = False
        else:
            self.notificador.disparar_alerta()
            self.btn_alerta.config(text="🔔 Silenciar Alerta")
            self.alerta_ativo = True

    def _finalizar(self, decisao):
        self.decisao = decisao
        self.top.grab_release() # Libera a interação com a janela principal
        self.top.destroy() # Fecha a janela filha
    
    # Exibição bloqueante
    def exibir(self):
        self.parent.wait_window(self.top) # Aguarda a janela filha ser fechada
        return self.decisao
    
    def _centralizar(self):
        self.top.update_idletasks() # Atualiza a janela para que as dimensões sejam calculadas corretamente
        
        x = self.parent.winfo_x()
        y = self.parent.winfo_y()
        w = self.parent.winfo_width()
        h = self.parent.winfo_height()

        largura = 420
        altura = 240

        pos_x = x + (w // 2) - (largura // 2)
        pos_y = y + (h // 2) - (altura // 2)

        self.top.geometry(f"{largura}x{altura}+{pos_x}+{pos_y}")
