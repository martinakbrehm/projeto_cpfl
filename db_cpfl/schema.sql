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
--
--   id | mensagem                                                                          | status
--   ---+-----------------------------------------------------------------------------------+--------
--    1 | Instalacao ativa                                                                  | ativo
--    2 | Instalacao inativa                                                                | inativo
--    3 | Informacoes digitadas nao pertencem ao atual titular da instalacao                | inativo
--    4 | Aguardando processamento                                                          | pendente
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS respostas (
  id       TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
  mensagem TEXT,
  status   VARCHAR(50) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO respostas (id, mensagem, status) VALUES
  (1, 'Instala\u00e7\u00e3o ativa',                                                                               'ativo'),
  (2, 'Instala\u00e7\u00e3o inativa',                                                                             'inativo'),
  (3, 'Informa\u00e7\u00f5es digitadas n\u00e3o pertencem ao atual titular da instala\u00e7\u00e3o', 'inativo'),
  (4, 'Aguardando processamento',                                                          'pendente')
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
--
-- Status possíveis: pendente | processando | ativo | inativo
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tabela_macros_cpfl (
  id            INT NOT NULL AUTO_INCREMENT,
  cliente_id    INT NOT NULL,
  cliente_uc_id INT DEFAULT NULL,
  resposta_id   TINYINT UNSIGNED DEFAULT 4,  -- default: pendente
  pn            VARCHAR(20) DEFAULT NULL,    -- Numero do Parceiro
  status        ENUM('pendente','processando','ativo','inativo')
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

-- Automação: por CPF+UC retorna o registro mais recente com status pendente
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
        tm.data_update DESC,
        tm.id DESC
    ) AS rn
  FROM tabela_macros_cpfl tm
  JOIN  clientes    c  ON c.id  = tm.cliente_id
  LEFT JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
  WHERE tm.status = 'pendente'
) vm
WHERE vm.rn = 1;

-- Ativos (titularidade confirmada)
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
WHERE tm.status = 'ativo';


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

-- ---------------------------------------------------------------------------
-- Tabelas materializadas para o dashboard
-- Populadas pelas stored procedures sp_refresh_dashboard_*_agg().
-- SELECT simples em tabelas indexadas: latência <1ms.
-- ---------------------------------------------------------------------------

-- Resumo diário de macros processadas (por dia × status × mensagem)
CREATE TABLE IF NOT EXISTS dashboard_macros_agg (
  id             INT NOT NULL AUTO_INCREMENT,
  dia            DATE NOT NULL,
  status         VARCHAR(50) NOT NULL,
  mensagem       TEXT,
  resposta_status VARCHAR(50) DEFAULT NULL,
  empresa        VARCHAR(100) DEFAULT NULL,
  fornecedor     VARCHAR(100) DEFAULT NULL,
  arquivo_origem VARCHAR(255) DEFAULT NULL,
  qtd            INT NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  INDEX idx_dma_dia    (dia),
  INDEX idx_dma_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Estatísticas por arquivo de staging (com distribuição proporcional de status)
CREATE TABLE IF NOT EXISTS dashboard_arquivos_agg (
  id                   INT NOT NULL AUTO_INCREMENT,
  arquivo              VARCHAR(255) NOT NULL,
  data_carga           DATE NOT NULL,
  cpfs_no_arquivo      INT NOT NULL DEFAULT 0,
  cpfs_processados     INT NOT NULL DEFAULT 0,
  ativos               INT NOT NULL DEFAULT 0,
  inativos             INT NOT NULL DEFAULT 0,
  cpfs_ineditos        INT NOT NULL DEFAULT 0,
  ucs_ineditas         INT NOT NULL DEFAULT 0,
  combos_processadas   INT NOT NULL DEFAULT 0,
  combos_ativas        INT NOT NULL DEFAULT 0,
  combos_inativas      INT NOT NULL DEFAULT 0,
  ineditos_processados INT NOT NULL DEFAULT 0,
  ineditos_ativos      INT NOT NULL DEFAULT 0,
  ineditos_inativos    INT NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  INDEX idx_daa_arquivo    (arquivo),
  INDEX idx_daa_data_carga (data_carga)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Cobertura por arquivo de staging (% do lote já processado)
CREATE TABLE IF NOT EXISTS dashboard_cobertura_agg (
  id                 INT NOT NULL AUTO_INCREMENT,
  staging_id         INT NOT NULL,
  arquivo            VARCHAR(255) NOT NULL,
  data_carga         DATE NOT NULL,
  ucs_ineditas       INT NOT NULL DEFAULT 0,
  combos_processadas INT NOT NULL DEFAULT 0,
  pct_cobertura      DECIMAL(5,2) NOT NULL DEFAULT 0.00,
  PRIMARY KEY (id),
  UNIQUE KEY ux_cobertura_staging (staging_id),
  INDEX idx_dca_data_carga (data_carga)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- Stored procedures de refresh
-- ---------------------------------------------------------------------------
DELIMITER $$

-- Refresh: resumo diário de macros
CREATE PROCEDURE IF NOT EXISTS sp_refresh_dashboard_macros_agg()
BEGIN
  TRUNCATE TABLE dashboard_macros_agg;
  INSERT INTO dashboard_macros_agg
    (dia, status, mensagem, resposta_status, empresa, fornecedor, arquivo_origem, qtd)
  SELECT
    DATE(tm.data_update),
    tm.status,
    r.mensagem,
    r.status,
    NULL,
    NULL,
    NULL,
    COUNT(*)
  FROM tabela_macros_cpfl tm
  JOIN respostas r ON r.id = tm.resposta_id
  WHERE tm.status NOT IN ('pendente', 'processando')
  GROUP BY DATE(tm.data_update), tm.status, r.mensagem, r.status
  ORDER BY DATE(tm.data_update) DESC;
END$$

-- Refresh: estatísticas por arquivo (distribuição proporcional de status)
CREATE PROCEDURE IF NOT EXISTS sp_refresh_dashboard_arquivos_agg()
BEGIN
  TRUNCATE TABLE dashboard_arquivos_agg;
  INSERT INTO dashboard_arquivos_agg
    (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados,
     ativos, inativos, cpfs_ineditos, ucs_ineditas,
     combos_processadas, combos_ativas, combos_inativas,
     ineditos_processados, ineditos_ativos, ineditos_inativos)
  WITH per_file AS (
    SELECT
      si.id               AS staging_id,
      si.filename         AS arquivo,
      DATE(si.created_at) AS data_carga,
      si.rows_success     AS cpfs_no_arquivo,
      COALESCE(cc.distinct_cpfs,   0) AS distinct_cpfs,
      COALESCE(cc.distinct_combos, 0) AS ucs_ineditas
    FROM staging_imports si
    LEFT JOIN (
      SELECT
        staging_id,
        COUNT(DISTINCT normalized_cpf)                AS distinct_cpfs,
        COUNT(DISTINCT normalized_cpf, normalized_uc) AS distinct_combos
      FROM staging_import_rows
      WHERE validation_status = 'valid'
      GROUP BY staging_id
    ) cc ON cc.staging_id = si.id
    WHERE si.status = 'completed'
  ),
  global_totals AS (
    SELECT
      SUM(ucs_ineditas) AS total_ucs,
      (SELECT COUNT(*) FROM tabela_macros_cpfl
         WHERE status NOT IN ('pendente','processando')) AS g_processadas,
      (SELECT COUNT(*) FROM tabela_macros_cpfl
         WHERE status = 'ativo')                        AS g_ativas,
      (SELECT COUNT(*) FROM tabela_macros_cpfl
         WHERE status = 'inativo')                      AS g_inativas
    FROM per_file
  )
  SELECT
    pf.arquivo,
    pf.data_carga,
    pf.cpfs_no_arquivo,
    pf.distinct_cpfs                                          AS cpfs_processados,
    0                                                         AS ativos,
    0                                                         AS inativos,
    pf.distinct_cpfs                                          AS cpfs_ineditos,
    pf.ucs_ineditas,
    CASE WHEN gt.total_ucs > 0
      THEN ROUND(pf.ucs_ineditas / gt.total_ucs * gt.g_processadas)
      ELSE 0 END                                              AS combos_processadas,
    CASE WHEN gt.total_ucs > 0
      THEN ROUND(pf.ucs_ineditas / gt.total_ucs * gt.g_ativas)
      ELSE 0 END                                              AS combos_ativas,
    CASE WHEN gt.total_ucs > 0
      THEN ROUND(pf.ucs_ineditas / gt.total_ucs * gt.g_inativas)
      ELSE 0 END                                              AS combos_inativas,
    0, 0, 0
  FROM per_file pf
  CROSS JOIN global_totals gt
  ORDER BY pf.data_carga DESC;
END$$

-- Refresh: cobertura por arquivo (% do lote processado)
CREATE PROCEDURE IF NOT EXISTS sp_refresh_dashboard_cobertura_agg()
BEGIN
  TRUNCATE TABLE dashboard_cobertura_agg;
  INSERT INTO dashboard_cobertura_agg
    (staging_id, arquivo, data_carga, ucs_ineditas, combos_processadas, pct_cobertura)
  WITH per_file AS (
    SELECT
      si.id               AS staging_id,
      si.filename         AS arquivo,
      DATE(si.created_at) AS data_carga,
      COALESCE(cc.distinct_combos, 0) AS ucs_ineditas
    FROM staging_imports si
    LEFT JOIN (
      SELECT
        staging_id,
        COUNT(DISTINCT normalized_cpf, normalized_uc) AS distinct_combos
      FROM staging_import_rows
      WHERE validation_status = 'valid'
      GROUP BY staging_id
    ) cc ON cc.staging_id = si.id
    WHERE si.status = 'completed'
  ),
  global_totals AS (
    SELECT
      SUM(ucs_ineditas) AS total_ucs,
      (SELECT COUNT(*) FROM tabela_macros_cpfl
         WHERE status NOT IN ('pendente','processando')) AS g_processadas
    FROM per_file
  )
  SELECT
    pf.staging_id,
    pf.arquivo,
    pf.data_carga,
    pf.ucs_ineditas,
    CASE WHEN gt.total_ucs > 0
      THEN ROUND(pf.ucs_ineditas / gt.total_ucs * gt.g_processadas)
      ELSE 0 END AS combos_processadas,
    CASE WHEN pf.ucs_ineditas > 0 AND gt.total_ucs > 0
      THEN ROUND(
        (pf.ucs_ineditas / gt.total_ucs * gt.g_processadas) / pf.ucs_ineditas * 100,
        2)
      ELSE 0.00 END AS pct_cobertura
  FROM per_file pf
  CROSS JOIN global_totals gt;
END$$

DELIMITER ;

-- =============================================================================
-- FIM DO SCHEMA
-- =============================================================================
