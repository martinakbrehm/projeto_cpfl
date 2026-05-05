import os
from pathlib import Path
import pygame
import threading
import time
from config import BASE_DIR

class Notificador:
    def __init__(self):
        pygame.mixer.init()
        self.thread_som = None
        self.caminho_som = self.config_caminho_som()
        self._alerta_ativo = False

    def _loop_sonoro(self, intervalo):
        while self._alerta_ativo:
            pygame.mixer.music.load(self.caminho_som)
            pygame.mixer.music.play()
            for _ in range(int(intervalo)):
                if not self._alerta_ativo:
                    break
                time.sleep(1)
    
    def disparar_alerta(self, intervalo=90):
        if not self._alerta_ativo:
            self._alerta_ativo = True
            self.thread_som = threading.Thread(target=self._loop_sonoro, args=(intervalo,))
            self.thread_som.start()

    def parar_alerta(self):
        self._alerta_ativo = False
        pygame.mixer.music.stop()
        if self.thread_som:
            self.thread_som.join()

    def config_caminho_som(self):
        audio_path = Path(BASE_DIR) / "assets" / "alerta.mp3"
    
        if not audio_path.exists():
            print(f"ERRO: Arquivo de som não encontrado em: {audio_path}")
            
        return str(audio_path)