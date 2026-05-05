import queue
import time
from core.portal_gmp import PortalGMP
from core.gerenciador_dados import GerenciadorDados
from core.tratador_erros import PortalIntervencaoHumana


class Validador:
    def __init__(self, config_usuario, caminho_csv, callback_progresso, callback_intervencao,
                 caminho_resultado=None):
        self.dados = GerenciadorDados(caminho_csv, caminho_resultado)
        self.usuario = config_usuario["usuario"]
        self.senha = config_usuario["senha"]
        self.portal = PortalGMP()

        self.callback_progresso = callback_progresso
        self.callback_intervencao = callback_intervencao

        self.rodando = False
        self.pausado = False
        
    # ===============================
    # MÉTODO PRINCIPAL
    # ===============================

    def processamento(self):

        self.rodando = True
        erro_fatal = False
        
        self.portal.iniciar_driver()

        try:
            self._executar_fluxo()

        except Exception as e:
            erro_fatal = True
            print("Erro inesperado:", e)

        finally:
            if erro_fatal:
                print("Processo pausado por erro fatal")
                print("Driver permanecerá aberto para inspeção.")
            else:
                self.portal.finalizar()

            self.rodando = False

    # ===============================
    # FLUXO NORMAL
    # ===============================

    def _executar_fluxo(self):

        self.portal.login(self.usuario, self.senha)

        linhas_processadas, total_linhas = self.dados.obter_status()
        
        self.dados.inicializar_arquivo_resultado(continuar=True)
        for indice, linha in self.dados.leitor_csv(pular_ate=linhas_processadas):

            if not self.rodando:
                break

            while self.pausado: 
                time.sleep(1)  # Aguarda enquanto estiver pausado   

            # Cada linha é um loop
            while True:

                try:   
                    resultado_busca = self.portal.buscar_pn(linha['CPF'], linha['UC'])

                    dados_salvar = [linha['CPF'], linha['UC'], *resultado_busca]
                    self.dados.salvar_linha(dados_salvar)

                    break

                except PortalIntervencaoHumana as e:
                    decisao = self._resolver_intervencao(e.args[0])

                    if decisao == "reiniciar":
                        self.portal.finalizar()
                        self.portal.iniciar_driver()
                        self.portal.login(self.usuario, self.senha)
                        continue  # tenta a mesma linha novamente

                    elif decisao == "relogar":
                        self.portal.login(self.usuario, self.senha)
                        continue

                    elif decisao == "continuar":
                        continue  # apenas tenta a mesma linha novamente

                    elif decisao == "parar":
                        self.rodando = False
                        return

            # Callback de progresso
            atual = indice + 1
            porcentagem = (atual / total_linhas) * 100
            self.callback_progresso({
                "status": "processando",
                "progresso": round(porcentagem, 2),
                "linha_atual": atual,
                "total_linhas": total_linhas
            })

        if self.rodando: # Se terminou o loop sem ser parado pelo usuário
            self.callback_progresso({"status": "concluido"})

    # ===============================
    # INTERVENÇÃO HUMANA
    # ===============================

    def _resolver_intervencao(self, mensagem):

        print("Intervenção necessária:", mensagem)

        resposta_queue = queue.Queue()

        self.callback_intervencao({
            "mensagem": mensagem,
            "resposta_queue": resposta_queue
        })

        decisao = resposta_queue.get() # Bloqueia até que uma resposta seja dada

        return decisao
    
