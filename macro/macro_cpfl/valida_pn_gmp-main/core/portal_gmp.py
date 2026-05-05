import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException
from utils.scraping import Scraping as driver
from selenium.webdriver.common.by import By
from core.tratador_erros import ErroRecuperavelPortal, TratadorErros, PortalIntervencaoHumana

class PortalGMP():
    """Classe para interagir com o Portal GMP."""
    
    def __init__(self):
        self.driver = None
        self.tratador = TratadorErros(self)
        self.em_processo_de_login = False
        self.tabela_resultado_ultima_busca = None
        self.url_login = "https://gmp.cpfl.com.br/Login.aspx"
        self.url_busca = "https://gmp.cpfl.com.br/Forms/Consulta/ConsultaUcDocumento.aspx"

    def iniciar_driver(self):
        """Inicia o driver do Selenium WebDriver."""
        self.driver = driver.instancia_driver()
        if not self.driver:
            return False
        # Maximiza a janela para garantir que todos os elementos sejam carregados corretamente
        self.driver.maximize_window()
        return True
    
    def navegar(self, url, atualizar_pagina=False):
        """Navega para a URL especificada, tratando erros comuns durante a navegação."""
        if url != self.driver.current_url or atualizar_pagina:
            self.driver.get(url)
            driver.wait_complete(self.driver)
        self.tratador.validar_pagina_atual()

    def aguardar_login(self, timeout=600):

        inicio = time.time()

        while (time.time() - inicio) < timeout:

            if self.verificar_sucesso_login():
                return "sucesso"

            if self.tratador.verificar_acesso_negado():
                return "acesso_negado"
            
            self.tratador.validar_pagina_atual()

            time.sleep(2)

        return "timeout"

    def verificar_sucesso_login(self):
        """Verifica se o login foi bem-sucedido, procurando por elementos específicos da página inicial."""
        return "Default" in self.driver.current_url
    
    def login(self, usuario, senha):
        """Realiza o login no Portal GMP."""
        self.em_processo_de_login = True
        self.navegar(self.url_login)

        tentativas_locais = 0
        max_tentativas = 3
        while tentativas_locais < max_tentativas:

            input_usuario = driver.safe_find(self.driver, '//*[@id="txtLoginUser"]', 'XPATH', for_input=True)
            input_senha = driver.safe_find(self.driver, '//*[@id="txtPassword"]', 'XPATH', for_input=True)

            if not input_usuario or not input_senha:
                print("Campos de login não encontrados. Tentando refresh...")
                self.driver.refresh()
                tentativas_locais += 1
                continue

            driver.espera_humana()
            input_usuario.clear()
            driver.digitar_humano(input_usuario, usuario)

            driver.espera_humana()
            input_senha.clear()
            driver.digitar_humano(input_senha, senha)

            # Aguarda resolver o captcha manualmente          
            print("Aguardando login ser concluído.")
            resultado = self.aguardar_login(timeout=900)

            if resultado == "sucesso":
                print("Login bem-sucedido!")
                self.em_processo_de_login = False
                return True
            if resultado == "acesso_negado":
                tentativas_locais += 1
                print(f"Acesso negado. Tentando novamente ({tentativas_locais}/{max_tentativas})")
                continue
            if resultado == "timeout":
                raise PortalIntervencaoHumana("Timeout aguardando login.")
        
        raise PortalIntervencaoHumana("Falha após múltiplas tentativas de login.")
    
    def buscar_pn(self, cpf, uc, atualizar_pagina=False):
        """
        Realiza a busca da UC/CPF e retorna os dados encontrados.
        Retorna: (pn, ativo, erro)
        """

        tentativas = 0
        max_tentativas = 2
        while tentativas <= max_tentativas:
            try:
                # 1. Garantir que estamos na página de busca ou navegar para ela
                self.navegar(self.url_busca)

                # Garantir que cpf tenha 11 dígitos
                if len(cpf) < 11:
                    cpf = cpf.zfill(11)

                # 2. Preencher o campo de busca
                input_uc = driver.safe_find(self.driver, '//*[@id="txtInstalacao"]', 'XPATH', for_input=True)
                input_cpf = driver.safe_find(self.driver, '//*[@id="txtDocumento"]', 'XPATH', for_input=True)

                driver.espera_humana()
                input_uc.clear()
                driver.digitar_humano(input_uc, uc)

                driver.espera_humana()
                input_cpf.clear()
                driver.digitar_humano(input_cpf, cpf)

                ja_existe_tabela = driver.safe_find(self.driver, '//*[@id="resultadoTable_wrapper"]', 'XPATH', timeout=2)
                # 3. Clicar no botão de busca
                btn_procurar = driver.safe_find(self.driver, '//*[@id="ContentPlaceHolderConteudo_btnVerificar"]', 'XPATH', click=True)
                btn_procurar.click()
                driver.wait_complete(self.driver)

                # 4. Verificar resultados
                self.tratador.validar_pagina_atual()
                
                # 5. Extrair dados da tabela de resultados
                if ja_existe_tabela and self.tabela_resultado_ultima_busca:
                    driver.wait(self.driver).until(EC.staleness_of(self.tabela_resultado_ultima_busca))
                                
                self.tabela_resultado_ultima_busca = driver.safe_find(self.driver, '//*[@id="resultadoTable_wrapper"]', 'XPATH')
                
                return self._extrair_resultado_da_tela()

            except (NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException, ErroRecuperavelPortal) as e:
                tentativas += 1
                if tentativas > max_tentativas:
                    raise PortalIntervencaoHumana("Falha persistente ao carregar resultado após refresh.") from e
                
                print(f"[Retry {tentativas}] Tentando recarregar página...")

                self.driver.refresh()
                driver.wait_complete(self.driver)
                self.tratador.validar_pagina_atual()
            
            except Exception as e:
                # Se for erro desconhecido precisa de intervenção humana
                raise PortalIntervencaoHumana("Erro desconhecido ao extrair resultado.") from e
                
            time.sleep(2)
            
    def _extrair_resultado_da_tela(self):
        try:
            elemento_pn = driver.safe_find(self.driver, '//*[@id="resultadoTable"]/tbody/tr/td[3]', 'XPATH')
            if not elemento_pn:
                raise NoSuchElementException("PN não encontrado.")
                        
            pn_texto = elemento_pn.text.strip()

            if pn_texto.isdigit():
                return (pn_texto, "S", "")
            
            uc_inativa = driver.safe_find(self.driver, '//*[@id="resultadoTable"]/tbody/tr/td[4]', 'XPATH', timeout=2)
            if not pn_texto and uc_inativa and "inativo" in uc_inativa.text.lower():
                return ("", "N", "Instalação inativa")
                            
            return ("", "N", pn_texto) # Erro de negócio (ex: "Outros retornos")
                    
        except (NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException) as e:
            raise ErroRecuperavelPortal(
                "Tabela de resultado não carregou corretamente."
            ) from e
        except Exception as e:
            # Se for erro desconhecido precisa de intervenção humana
            raise PortalIntervencaoHumana("Erro desconhecido ao extrair resultado.") from e
    
    def logout(self):
        """Realiza o logout do usuário."""
        try:
            btn_sair = self.driver.find_element(By.XPATH, '//*[@id="btnLogoffExterno"]')
            btn_sair.click()
            driver.wait_complete(self.driver)
        except:
            print("Não foi possível realizar logout. Provavelmente o usuário já está deslogado.")
            return

    def finalizar(self):
        """Finaliza o driver e para o alerta sonoro."""
        self.tabela_resultado_ultima_busca = None
        if self.driver:
            self.logout()
            self.driver.quit()

    

    


    
        


            

        
        
        