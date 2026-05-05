import os
import random
from selenium import webdriver
from selenium.common import TimeoutException, SessionNotCreatedException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import logging
import time

class Scraping:
    """Classe para auxiliar no processo de Web Scraping."""

    @staticmethod
    def instancia_driver():
        """Instancia o driver do Selenium WebDriver."""
        try:
            service = Service(ChromeDriverManager().install())
            options = webdriver.ChromeOptions()

            prefs = {
                "safebrowsing.enabled": True,  # Habilita o modo de navegação segura
                "credentials_enable_service": False, # Desabilita o serviço de credenciais
                "profile.password_manager_enabled": False, # Desabilita o gerenciador de senhas
            }

            options.add_experimental_option("prefs", prefs)

            options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"]) # Remove a notificação de automação e logs
            options.add_experimental_option("useAutomationExtension", False) # Desabilita a extensão de automação

            options.add_argument("--no-sandbox") # Evita problemas de permissão em alguns ambientes
            options.add_argument("--disable-dev-shm-usage") # Evita problemas de memória em ambientes limitados
            options.add_argument("--disable-popup-blocking")  # Impede pop-ups indesejados
            options.add_argument("--disable-save-password-bubble") # Desabilita o pop-up de salvar senha

            driver = webdriver.Chrome(service=service, options=options)
            return driver
        except SessionNotCreatedException as e:
            if "only supports Chrome version" in str(e):
                print(f"[ERRO] Versão do Chrome incompatível com o WebDriver: atualize seu Chome")
                logging.error("Erro de SessionNotCreatedException ao iniciar o WebDriver", exc_info=True)
            else:
                print(f"[ERRO] Falha ao iniciar o WebDriver: verifique o arquivo de log para mais detalhes.")
                logging.error("Erro de SessionNotCreatedException ao iniciar o WebDriver", exc_info=True)
            return None
        except Exception as e:
            print(f"[ERRO] Falha ao iniciar o WebDriver: verifique o arquivo de log para mais detalhes.")
            logging.error("Erro ao iniciar o WebDriver", exc_info=True)
            return None

    @staticmethod
    def wait(driver, segundos=20):
        """WebDriverWait genérico."""
        return WebDriverWait(driver, segundos)

    @staticmethod
    def wait_complete(driver, timeout=20):
        """Espera até que a página esteja completamente carregada."""
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )

    @staticmethod
    def aguarda_download(pasta_download, timeout=60):
        """
        Aguarda até detectar que UM download começou na pasta:
        - aparece um .crdownload OU
        - aparece um arquivo novo (caso o download seja rápido demais).
        """
        pasta = os.path.abspath(pasta_download)

        if not os.path.isdir(pasta):
            print(f"⚠️ Pasta de download não existe: {pasta}")
            return False

        tempo_inicial = time.time()

        # Snapshot inicial dos arquivos
        arquivos_iniciais = set(os.listdir(pasta))

        while time.time() - tempo_inicial < timeout:
            arquivos_atuais = set(os.listdir(pasta))

            # 1) Download em andamento -> .crdownload detectado
            if any(nome.endswith(".crdownload") for nome in arquivos_atuais):
                return True

            # 2) Nenhum .crdownload, mas apareceu arquivo novo (download foi rápido)
            arquivos_novos = arquivos_atuais - arquivos_iniciais
            if arquivos_novos:
                return True

            time.sleep(0.5)

        print(f"⚠️ Tempo de espera por download excedido ({timeout} segundos).")
        return False

    @staticmethod
    def espera_humana(min_s=0.4, max_s=1.2):
        """
        Espera um tempo aleatório entre min_s e max_s segundos.
        Exemplo: espera_humana(0.5, 1.0)
        """
        time.sleep(random.uniform(min_s, max_s))

    @staticmethod
    def digitar_humano(elemento, texto, min_delay=0.01, max_delay=0.03):
        for char in texto:
            elemento.send_keys(char)
            time.sleep(random.uniform(min_delay, max_delay))

    @staticmethod
    def safe_find(driver, elemento, tipo=None, timeout=15, click=False, for_input=False):
        try:
            by = getattr(By, tipo) if tipo else By.XPATH  # padrão é XPATH

            if click:
                elem = WebDriverWait(driver, timeout).until(
                    EC.element_to_be_clickable((by, elemento))
                )
            elif for_input:
                elem = WebDriverWait(driver, timeout).until(
                    EC.visibility_of_element_located((by, elemento))
                )
            else:
                elem = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((by, elemento))
                )
            return elem

        except TimeoutException:
            print(f"[ERRO] Elemento não encontrado/interagível: {elemento}")
            return None

    @staticmethod
    def fechar(driver):
        """Fecha o driver do Selenium WebDriver."""
        if driver:
            driver.quit()
            print("🚪 Navegador fechado.")