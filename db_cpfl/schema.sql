-- =============================================================================
-- schema.sql  –  Banco CPFL: bd_Automacoes_time_dados_cpfl
-- =============================================================================
-- Execução: python db_cpfl/setup_database.py
-- Adaptado do schema Neoenergia para atender o fluxo da macro CPFL.
--
-- Principais diferenças em relação ao schema Neoenergia:
--   • Sem distribuidora (CPFL é mono-distribuidora, campo não necessário)
--   • tabela_macros_cpfl inclui campo `pn` (Número do Parceiro)
--   • Sem campos financeiros (qtd_faturas, valor_debito, etc.)
--   • tabela `respostas` com mapeamento de erros CPFL
--   • Sem tabela_macro_api (CPFL não usa Macro API)
-- =============================================================================

USE bd_Automacoes_time_dados_cpfl;

-- ---------------------------------------------------------------------------
-- Respostas CPFL
-- Mapeamento: (ATIVO + mensagem ERRO) → (resposta_id, status em tabela_macros_cpfl)
--
--   id | mensagem                                                     | status
--   ---+--------------------------------------------------------------+-----------
--    1 | Titularidade confirmada                                      | consolidado
--    2 | Instalação inativa                                           | reprocessar
--    3 | Informações digitadas não pertencem ao atual titular         | excluido
--    4 | CPF/CNPJ não cadastrado na instalação                        | excluido
--    5 | Aguardando processamento                                     | pendente
--    6 | ERRO (falha de comunicação / resposta desconhecida)          | reprocessar
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS respostas (
  id       TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
  mensagem TEXT,
  status   VARCHAR(50) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO respostas (id, mensagem, status) VALUES
  (1, 'Titularidade confirmada',                                          'consolidado'),
  (2, 'Instalação inativa',                                               'reprocessar'),
  (3, 'Informações digitadas não pertencem ao atual titular da instalação','excluido'),
  (4, 'CPF/CNPJ não cadastrado na instalação',                            'excluido'),
  (5, 'Aguardando processamento',                                          'pendente'),
  (6, 'ERRO',                                                              'reprocessar')
ON DUPLICATE KEY UPDATE mensagem = VALUES(mensagem), status = VALUES(status);


-- ---------------------------------------------------------------------------
-- Clientes
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clientes (
  id              INT NOT NULL AUTO_INCREMENT,
  cpf             CHAR(11) NOT NULL,
  nome            VARCHAR(255) DEFAULT NULL,
  data_nascimento DATE DEFAULT NULL,
  data_criacao    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data_update     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_clientes_cpf (cpf)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DELIMITER //
CREATE TRIGGER IF NOT EXISTS before_insert_clientes
BEFORE INSERT ON clientes
FOR EACH ROW
BEGIN
  IF LENGTH(NEW.cpf) < 11 THEN
    SET NEW.cpf = LPAD(NEW.cpf, 11, '0');
  END IF;
END //

CREATE TRIGGER IF NOT EXISTS before_update_clientes
BEFORE UPDATE ON clientes
FOR EACH ROW
BEGIN
  IF LENGTH(NEW.cpf) < 11 THEN
    SET NEW.cpf = LPAD(NEW.cpf, 11, '0');
  END IF;
END //
DELIMITER ;


-- ---------------------------------------------------------------------------
-- UC por cliente
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cliente_uc (
  id           INT NOT NULL AUTO_INCREMENT,
  cliente_id   INT NOT NULL,
  uc           CHAR(10) NOT NULL,
  ativo        TINYINT(1) DEFAULT 1,
  data_criacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_cliente_uc (cliente_id, uc),
  FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE cliente_uc
  ADD INDEX idx_cliente_uc_cliente_uc (cliente_id, uc),
  ADD INDEX idx_cliente_uc_uc         (uc);


-- ---------------------------------------------------------------------------
-- Tabela principal da macro CPFL
-- Adaptada de tabela_macros (Neoenergia):
--   + pn         VARCHAR(20)  — Número do Parceiro retornado pela macro
--   - distribuidora_id        — não necessário (banco é exclusivo CPFL)
--   - qtd_faturas / valor_debito / valor_credito / etc.
--       removidos (CPFL não retorna dados financeiros)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tabela_macros_cpfl (
  id            INT NOT NULL AUTO_INCREMENT,
  cliente_id    INT NOT NULL,
  cliente_uc_id INT DEFAULT NULL,
  resposta_id   TINYINT UNSIGNED DEFAULT 5,  -- default: pendente
  pn            VARCHAR(20) DEFAULT NULL,    -- Número do Parceiro
  status        ENUM('pendente','processando','reprocessar','consolidado','excluido')
                NOT NULL DEFAULT 'pendente',
  extraido      TINYINT(1) NOT NULL DEFAULT 0,
  data_criacao  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data_update   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data_extracao DATETIME DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_cpfl_macros_cliente    FOREIGN KEY (cliente_id)    REFERENCES clientes  (id) ON DELETE CASCADE,
  CONSTRAINT fk_cpfl_macros_resposta   FOREIGN KEY (resposta_id)   REFERENCES respostas (id) ON DELETE SET NULL,
  CONSTRAINT fk_cpfl_macros_cliente_uc FOREIGN KEY (cliente_uc_id) REFERENCES cliente_uc(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE tabela_macros_cpfl
  ADD INDEX idx_cpfl_macros_status_data     (status, data_update, cliente_id),
  ADD INDEX idx_cpfl_macros_cliente_data    (cliente_id, data_update),
  ADD INDEX idx_cpfl_macros_extraido_status (extraido, status, data_update),
  ADD INDEX idx_cpfl_macros_resposta        (resposta_id),
  ADD INDEX idx_cpfl_macros_data_extracao   (data_extracao),
  ADD INDEX idx_cpfl_macros_pn              (pn);


-- ---------------------------------------------------------------------------
-- Telefones
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telefones (
  id           INT NOT NULL AUTO_INCREMENT,
  cliente_id   INT NOT NULL,
  telefone     BIGINT UNSIGNED DEFAULT NULL,
  tipo         VARCHAR(30) DEFAULT NULL,
  data_criacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_telefones_cliente (cliente_id),
  KEY idx_telefones_numero  (telefone),
  CONSTRAINT fk_telefones_cliente FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- Endereços
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enderecos (
  id            INT NOT NULL AUTO_INCREMENT,
  cliente_id    INT DEFAULT NULL,
  cliente_uc_id INT DEFAULT NULL,
  endereco      VARCHAR(255) DEFAULT NULL,
  cidade        VARCHAR(100) DEFAULT NULL,
  uf            CHAR(2) DEFAULT NULL,
  cep           VARCHAR(20) DEFAULT NULL,
  data_criacao  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_enderecos_cliente    (cliente_id),
  KEY idx_enderecos_cliente_uc (cliente_uc_id),
  CONSTRAINT fk_enderecos_cliente    FOREIGN KEY (cliente_id)    REFERENCES clientes  (id) ON DELETE CASCADE,
  CONSTRAINT fk_enderecos_cliente_uc FOREIGN KEY (cliente_uc_id) REFERENCES cliente_uc(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- Staging — importação de arquivos CSV
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_imports (
  id                 INT NOT NULL AUTO_INCREMENT,
  filename           VARCHAR(255) NOT NULL,
  target_macro_table VARCHAR(100) DEFAULT NULL,
  total_rows       INT DEFAULT 0,
  rows_success     INT DEFAULT 0,
  rows_failed      INT DEFAULT 0,
  status           ENUM('pending','processing','completed','failed') NOT NULL DEFAULT 'pending',
  imported_by      VARCHAR(100) DEFAULT NULL,
  created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  started_at       DATETIME DEFAULT NULL,
  finished_at      DATETIME DEFAULT NULL,
  PRIMARY KEY (id),
  INDEX idx_staging_status     (status),
  INDEX idx_staging_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS staging_import_rows (
  id                 INT NOT NULL AUTO_INCREMENT,
  staging_id         INT NOT NULL,
  row_idx            INT DEFAULT NULL,
  raw_cpf            VARCHAR(50) DEFAULT NULL,
  raw_nome           VARCHAR(255) DEFAULT NULL,
  normalized_cpf     CHAR(11) DEFAULT NULL,
  normalized_uc      CHAR(10) DEFAULT NULL,
  validation_status  ENUM('new','valid','invalid','skipped') DEFAULT 'new',
  validation_message VARCHAR(255) DEFAULT NULL,
  processed_at       DATETIME DEFAULT NULL,
  PRIMARY KEY (id),
  INDEX idx_staging_rows_staging (staging_id),
  INDEX idx_staging_rows_normcpf (normalized_cpf),
  INDEX idx_staging_rows_normuc  (normalized_uc),
  CONSTRAINT fk_staging_rows_imports FOREIGN KEY (staging_id) REFERENCES staging_imports (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- Views
-- ---------------------------------------------------------------------------

-- Automação: por CPF+UC retorna o registro mais relevante (pendente ou reprocessar)
CREATE OR REPLACE VIEW view_cpfl_macros_automacao AS
SELECT vm.*
FROM (
  SELECT
    tm.*,
    c.cpf  AS __cpf,
    cu.uc  AS __uc,
    ROW_NUMBER() OVER (
      PARTITION BY c.cpf, COALESCE(cu.uc, '')
      ORDER BY
        (tm.status = 'pendente') DESC,
        tm.data_update DESC,
        tm.id DESC
    ) AS rn
  FROM tabela_macros_cpfl tm
  JOIN  clientes    c  ON c.id  = tm.cliente_id
  LEFT JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
  WHERE tm.status IN ('pendente','reprocessar')
) vm
WHERE vm.rn = 1;

-- Consolidados (resultado final bem-sucedido)
CREATE OR REPLACE VIEW view_cpfl_macros_consolidados AS
SELECT
  tm.id,
  c.cpf,
  cu.uc,
  tm.pn,
  tm.status,
  tm.data_criacao,
  tm.data_update,
  tm.data_extracao
FROM tabela_macros_cpfl tm
JOIN  clientes   c  ON c.id  = tm.cliente_id
LEFT JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
WHERE tm.status = 'consolidado';


-- ---------------------------------------------------------------------------
-- Stored procedures
-- ---------------------------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE get_cpfl_macros_batch(IN batch_size INT)
BEGIN
  IF batch_size IS NULL OR batch_size <= 0 THEN
    SET batch_size = 2000;
  END IF;
  SELECT * FROM view_cpfl_macros_automacao
  ORDER BY data_update ASC, id ASC
  LIMIT batch_size;
END$$

DELIMITER ;

-- =============================================================================
-- FIM DO SCHEMA
-- =============================================================================
