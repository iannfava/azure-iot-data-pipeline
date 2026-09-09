import azure.functions as func
import logging
import json
import os
import pymssql
from decimal import Decimal

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# ============================================
# CONFIGURACAO
# ============================================
# RNF03: nenhuma credencial em texto plano -- a senha vem de uma
# variavel de ambiente (Application Settings). No Portal, o valor
# dessa configuracao pode ser uma referencia direta ao Key Vault:
# @Microsoft.KeyVault(SecretUri=https://kv-curso-azure.vault.azure.net/secrets/sql-server-password/)
#
# pymssql em vez de pyodbc -- nao depende de driver de sistema
# operacional instalado, funciona direto no Linux Consumption plan
# sem configuracao extra.

SQL_SERVER = os.environ.get("SQL_SERVER", "curso-azure-engenharia.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "free-sql-db-curso-azure")
SQL_USER = os.environ.get("SQL_USER", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")


def conectar():
    return pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=SQL_DATABASE,
        as_dict=False,
    )


def converter_valor(v):
    if isinstance(v, Decimal):
        return float(v)
    if hasattr(v, "isoformat"):
        return str(v)
    return v


def resultado_para_lista(cursor):
    colunas = [coluna[0] for coluna in cursor.description]
    return [dict(zip(colunas, [converter_valor(v) for v in linha])) for linha in cursor.fetchall()]


# ============================================
# ROTA 1 (RF06-a) -- Resumo de producao por linha
# GET /api/producao-resumo?linha=L1
# ============================================
@app.route(route="producao-resumo")
def producao_resumo(req: func.HttpRequest) -> func.HttpResponse:
    linha = req.params.get("linha")
    if not linha:
        return func.HttpResponse(
            json.dumps({"erro": "Informe o parametro 'linha', ex: ?linha=L1"}, ensure_ascii=False),
            mimetype="application/json", status_code=400
        )

    query = """
    SELECT
        l.linha_producao,
        m.id_maquina,
        d.data_producao,
        p.total_produzido,
        p.total_refugado,
        p.taxa_refugo_pct,
        p.produtividade_media
    FROM gold.fact_producao p
    JOIN gold.dim_linha l ON p.sk_linha = l.sk_linha
    JOIN gold.dim_maquina m ON p.sk_maquina = m.sk_maquina
    JOIN gold.dim_data d ON p.sk_data = d.sk_data
    WHERE l.linha_producao = %s
    ORDER BY d.data_producao DESC
    """

    conn = conectar()
    try:
        cursor = conn.cursor()
        cursor.execute(query, (linha.upper(),))
        detalhe = resultado_para_lista(cursor)
    finally:
        conn.close()

    if not detalhe:
        return func.HttpResponse(
            json.dumps({"erro": f"Nenhum dado encontrado para a linha '{linha}'"}, ensure_ascii=False),
            mimetype="application/json", status_code=404
        )

    total_produzido = sum(r["total_produzido"] for r in detalhe)
    total_refugado = sum(r["total_refugado"] for r in detalhe)

    resumo = {
        "linha_producao": linha.upper(),
        "total_produzido": total_produzido,
        "total_refugado": total_refugado,
        "taxa_refugo_media_pct": round((total_refugado / total_produzido) * 100, 2) if total_produzido else None,
        "detalhe_por_dia_maquina": detalhe,
    }

    return func.HttpResponse(json.dumps(resumo, ensure_ascii=False), mimetype="application/json", status_code=200)


# ============================================
# ROTA 2 (RF06-b) -- Condicao atual de uma maquina
# GET /api/condicao-maquina?id_maquina=M01
# ============================================
@app.route(route="condicao-maquina")
def condicao_maquina(req: func.HttpRequest) -> func.HttpResponse:
    id_maquina = req.params.get("id_maquina")
    if not id_maquina:
        return func.HttpResponse(
            json.dumps({"erro": "Informe o parametro 'id_maquina', ex: ?id_maquina=M01"}, ensure_ascii=False),
            mimetype="application/json", status_code=400
        )

    query = """
    SELECT TOP 1
        linha_producao, id_maquina, data,
        temperatura_media, vibracao_media, rpm_media
    FROM gold.condicao_maquina_diaria
    WHERE id_maquina = %s
    ORDER BY data DESC
    """

    conn = conectar()
    try:
        cursor = conn.cursor()
        cursor.execute(query, (id_maquina.upper(),))
        resultado = resultado_para_lista(cursor)
    finally:
        conn.close()

    if not resultado:
        return func.HttpResponse(
            json.dumps({"erro": f"Nenhuma leitura encontrada para a maquina '{id_maquina}'"}, ensure_ascii=False),
            mimetype="application/json", status_code=404
        )

    return func.HttpResponse(json.dumps(resultado[0], ensure_ascii=False), mimetype="application/json", status_code=200)


# ============================================
# ROTA 3 (RF06-c) -- Visao combinada: producao + condicao das maquinas da linha
# GET /api/saude-linha?linha=L1
# ============================================
@app.route(route="saude-linha")
def saude_linha(req: func.HttpRequest) -> func.HttpResponse:
    linha = req.params.get("linha")
    if not linha:
        return func.HttpResponse(
            json.dumps({"erro": "Informe o parametro 'linha', ex: ?linha=L1"}, ensure_ascii=False),
            mimetype="application/json", status_code=400
        )

    query = """
    SELECT
        l.linha_producao,
        m.id_maquina,
        d.data_producao,
        p.taxa_refugo_pct,
        p.produtividade_media,
        c.temperatura_media,
        c.vibracao_media,
        c.rpm_media
    FROM gold.fact_producao p
    JOIN gold.dim_linha l ON p.sk_linha = l.sk_linha
    JOIN gold.dim_maquina m ON p.sk_maquina = m.sk_maquina
    JOIN gold.dim_data d ON p.sk_data = d.sk_data
    LEFT JOIN gold.condicao_maquina_diaria c
        ON c.linha_producao = l.linha_producao
        AND c.id_maquina = m.id_maquina
        AND c.data = d.data_producao
    WHERE l.linha_producao = %s
    ORDER BY d.data_producao DESC, p.taxa_refugo_pct DESC
    """

    conn = conectar()
    try:
        cursor = conn.cursor()
        cursor.execute(query, (linha.upper(),))
        resultado = resultado_para_lista(cursor)
    finally:
        conn.close()

    if not resultado:
        return func.HttpResponse(
            json.dumps({"erro": f"Nenhum dado encontrado para a linha '{linha}'"}, ensure_ascii=False),
            mimetype="application/json", status_code=404
        )

    payload = {
        "linha_producao": linha.upper(),
        "registros": resultado,
    }

    return func.HttpResponse(json.dumps(payload, ensure_ascii=False), mimetype="application/json", status_code=200)