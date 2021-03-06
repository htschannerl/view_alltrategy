CREATE OR REPLACE FORCE VIEW "SAPIENS"."USU_VALLST"(
    "MES",
    "ANO",
    "DATA",
    "CODUNIDADE",
    "CODCENTRODECUSTO",
    "DESCCENTRODECUSTO",
    "CODCONTACONTABIL",
    "CODCONTAEXTENSO",
    "DESCCONTACONTABIL",
    "DOCUMENTO",
    "NATUREZA",
    "GERADOR",
    "DIMENSAO",
    "RATEIO",
    "VALOR",
    "HISTORICO",
    "CODPROJETO",
    "CAMINHO",
    "NUMLCT",
    "ORILCT",
    "CTARED",
    "CONSULTA",
    "VALORDEBCRE"
)AS
    SELECT
        "MES",
        "ANO",
        "DATA",
        "CODUNIDADE",
        "CODCENTRODECUSTO",
        "DESCCENTRODECUSTO",
        "CODCONTACONTABIL",
        "CODCONTAEXTENSO",
        "DESCCONTACONTABIL",
        "DOCUMENTO",
        "NATUREZA",
        "GERADOR",
        "DIMENSAO",
        "RATEIO",
        "VALOR",
        "HISTORICO",
        "CODPROJETO",
        "CAMINHO",
        "NUMLCT",
        "ORILCT",
        "CTARED",
        "CONSULTA",
        CASE WHEN NATUREZA = 'C' THEN VALOR * -1 ELSE VALOR END "VALORDEBCRE"
    FROM
        USU_VALLSTCPA
    UNION ALL
    SELECT
        "MES",
        "ANO",
        "DATA",
        "CODUNIDADE",
        "CODCENTRODECUSTO",
        "DESCCENTRODECUSTO",
        "CODCONTACONTABIL",
        "CODCONTAEXTENSO",
        "DESCCONTACONTABIL",
        "DOCUMENTO",
        "NATUREZA",
        "GERADOR",
        "DIMENSAO",
        "RATEIO",
        "VALOR",
        "HISTORICO",
        "CODPROJETO",
        "CAMINHO",
        "NUMLCT",
        "ORILCT",
        "CTARED",
        "CONSULTA",
        CASE WHEN NATUREZA = 'C' THEN VALOR * -1 ELSE VALOR END "VALORDEBCRE"
    FROM
        USU_VALLSTEST
    UNION ALL
    SELECT
        "MES",
        "ANO",
        "DATA",
        "CODUNIDADE",
        "CODCENTRODECUSTO",
        "DESCCENTRODECUSTO",
        "CODCONTACONTABIL",
        "CODCONTAEXTENSO",
        "DESCCONTACONTABIL",
        "DOCUMENTO",
        "NATUREZA",
        "GERADOR",
        "DIMENSAO",
        "RATEIO",
        "VALOR",
        "HISTORICO",
        "CODPROJETO",
        "CAMINHO",
        "NUMLCT",
        "ORILCT",
        "CTARED",
        "CONSULTA",
        CASE WHEN NATUREZA = 'C' THEN VALOR * -1 ELSE VALOR END "VALORDEBCRE"
    FROM
        USU_VALLSTNFE
    UNION ALL
    SELECT
        "MES",
        "ANO",
        "DATA",
        "CODUNIDADE",
        "CODCENTRODECUSTO",
        "DESCCENTRODECUSTO",
        "CODCONTACONTABIL",
        "CODCONTAEXTENSO",
        "DESCCONTACONTABIL",
        "DOCUMENTO",
        "NATUREZA",
        "GERADOR",
        "DIMENSAO",
        "RATEIO",
        "VALOR",
        "HISTORICO",
        "CODPROJETO",
        "CAMINHO",
        "NUMLCT",
        "ORILCT",
        "CTARED",
        "CONSULTA",
        CASE WHEN NATUREZA = 'C' THEN VALOR * -1 ELSE VALOR END "VALORDEBCRE"
    FROM
        USU_VALLSTNFS
    UNION ALL
    SELECT
        "MES",
        "ANO",
        "DATA",
        "CODUNIDADE",
        "CODCENTRODECUSTO",
        "DESCCENTRODECUSTO",
        "CODCONTACONTABIL",
        "CODCONTAEXTENSO",
        "DESCCONTACONTABIL",
        "DOCUMENTO",
        "NATUREZA",
        "GERADOR",
        "DIMENSAO",
        "RATEIO",
        "VALOR",
        "HISTORICO",
        "CODPROJETO",
        "CAMINHO",
        "NUMLCT",
        "ORILCT",
        "CTARED",
        "CONSULTA",
        CASE WHEN NATUREZA = 'C' THEN VALOR * -1 ELSE VALOR END "VALORDEBCRE"
    FROM
        USU_VALLSTREC
    UNION ALL
    SELECT
        "MES",
        "ANO",
        "DATA",
        "CODUNIDADE",
        "CODCENTRODECUSTO",
        "DESCCENTRODECUSTO",
        "CODCONTACONTABIL",
        "CODCONTAEXTENSO",
        "DESCCONTACONTABIL",
        "DOCUMENTO",
        "NATUREZA",
        "GERADOR",
        "DIMENSAO",
        "RATEIO",
        "VALOR",
        "HISTORICO",
        "CODPROJETO",
        "CAMINHO",
        "NUMLCT",
        "ORILCT",
        "CTARED",
        "CONSULTA",
        CASE WHEN NATUREZA = 'C' THEN VALOR * -1 ELSE VALOR END "VALORDEBCRE"
    FROM
        USU_VALLSTRES
    UNION ALL
    SELECT
        "MES",
        "ANO",
        "DATA",
        "CODUNIDADE",
        "CODCENTRODECUSTO",
        "DESCCENTRODECUSTO",
        "CODCONTACONTABIL",
        "CODCONTAEXTENSO",
        "DESCCONTACONTABIL",
        "DOCUMENTO",
        "NATUREZA",
        "GERADOR",
        "DIMENSAO",
        "RATEIO",
        "VALOR",
        "HISTORICO",
        "CODPROJETO",
        "CAMINHO",
        "NUMLCT",
        "ORILCT",
        "CTARED",
        "CONSULTA",
        CASE WHEN NATUREZA = 'C' THEN VALOR * -1 ELSE VALOR END "VALORDEBCRE"
    FROM
        USU_VALLSTTES