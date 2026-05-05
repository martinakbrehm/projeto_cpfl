import time

from selenium.webdriver.common.by import By
from utils.scraping import Scraping as driver
from selenium.common.exceptions import NoSuchElementException

class PortalIntervencaoHumana(Exception):
    """Portal entrou em estado que exige intervenção humana."""
    pass

class ErroRecuperavelPortal(Exception):
    """Erro que pode ser tratado com retry de página."""

class TratadorErros:
    """Classe para tratar erros comuns durante a navegação no Portal GMP."""
    
    def __init__(self, portal):
        self.portal = portal
        self.erros_consecutivos = 0
    
    @property
    def driver(self):
        return self.portal.driver

    def verificar_deslogado_inesperadamente(self):
        """Verifica se o usuário está deslogado inesperadamente."""
        try:
            self.driver.find_element(By.XPATH, '//*[@id="txtLoginUser"]')
        except NoSuchElementException:
            return
           
        if not self.portal.em_processo_de_login:
            raise PortalIntervencaoHumana("Sessão expirada ou deslogado inesperadamente.")

    def verificar_erro_custom_module(self):
        """Erro: 'The custom error module does not recognize this error'"""
        if "custom error module" in self.driver.page_source.lower():
            print("Sistemas detectou erro de módulo customizado. Atualizando a página...")
            driver.espera_humana()
            self.driver.refresh()
            driver.wait_complete(self.driver)
    
    def verificar_acesso_negado(self):
        """Erro: 'Acesso Negado' (Usuário já logado)"""
        try:
            modal_titulo = self.driver.find_element(By.CLASS_NAME, "modal-title").text
            if modal_titulo and "acesso negado" in modal_titulo.lower():
                print("Sistemas detectou acesso negado. Fechando modal e atualizando a página...")
                btn_ok = driver.safe_find(self.driver, '//div[@class="bootbox-body"]/../..//button', 'XPATH', click=True)
                if btn_ok:
                    driver.espera_humana()
                    btn_ok.click()
                    time.sleep(2)
                    driver.wait_complete(self.driver)
                    return True
        except NoSuchElementException:
            return False
    
    def validar_pagina_atual(self):
        """
        Valida se a página atual é a esperada, verificando elementos específicos.
        Resolve erros mapeados.
        Se não conseguir resolver ou for erro crítico → lança exceção.
        """
        self.verificar_deslogado_inesperadamente()      
        # self.verificar_acesso_negado()
        self.verificar_erro_custom_module()
    
        
    