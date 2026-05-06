"""
fix_respostas.py  –  Uso único
Limpa respostas antigas, insere as corretas e corrige o ENUM.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config import db_cpfl  # noqa: E402
import pymysql

NOVAS = [
    (1, "Instalação ativa",                                                                   "ativo"),
    (2, "Instalação inativa",                                                                 "inativo"),
    (3, "Informações digitadas não pertencem ao atual titular da instalação", "inativo"),
    (4, "Aguardando processamento",                                                           "pendente"),
]

ALTER_ENUM = (
    "ALTER TABLE tabela_macros_cpfl "
    "MODIFY COLUMN status ENUM('pendente','processando','ativo','inativo') "
    "NOT NULL DEFAULT 'pendente'"
)

conn = pymysql.connect(**db_cpfl(autocommit=False))
cur  = conn.cursor()

cur.execute("SET FOREIGN_KEY_CHECKS=0")
cur.execute("DELETE FROM respostas")
cur.executemany("INSERT INTO respostas (id, mensagem, status) VALUES (%s,%s,%s)", NOVAS)
cur.execute(ALTER_ENUM)
cur.execute("SET FOREIGN_KEY_CHECKS=1")
conn.commit()

cur.execute("SELECT id, mensagem, status FROM respostas ORDER BY id")
print("Respostas atuais:")
for r in cur.fetchall():
    print(f"  {r[0]:>2} | {r[1]:<70} | {r[2]}")
print("ENUM corrigido OK")
conn.close()
