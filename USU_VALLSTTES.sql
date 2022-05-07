CREATE OR REPLACE FORCE VIEW "SAPIENS"."USU_VALLSTTES"(
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
    "RATEIO",
    "DIMENSAO",
    "VALOR",
    "HISTORICO",
    "CODPROJETO",
    "CAMINHO",
    "NUMLCT",
    "ORILCT",
    "CTARED",
    "CONSULTA"
)AS
    SELECT
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2)) MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))  ANO,
        E640LCT.DATLCT                    DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                 CODUNIDADE,
        E640RAT.CODCCU                    CODCENTRODECUSTO,
        E044CCU.DESCCU                    DESCCENTRODECUSTO,
        (
            CASE
                WHEN E045PLA.GRUCTA = '5' THEN
                    (DECODE(
                        E044CCU.TIPCCU,
                        1,
                        E640RAT.CTARED || 'CI',
                        2,
                        E640RAT.CTARED || 'CD',
                        E640RAT.CTARED || 'DA'
                    ))
                ELSE
                    CAST(E640RAT.CTARED AS VARCHAR(7))
            END
        )                                  CODCONTACONTABIL,
        E045PLA.MSKGCC                    CODCONTAEXTENSO,
        E045PLA.ABRCTA                    DESCCONTACONTABIL,
        E644LTE.NUMCCO                    DOCUMENTO,
        E640RAT.DEBCRE                    NATUREZA,
        E600MCC.NOMREC                    GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                    VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))       CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'TESM'                            CONSULTA
    FROM
        E640RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LTE,
        E600MCC
    WHERE
            E640RAT.CODEMP = E045PLA.CODEMP
        AND E640RAT.CTARED = E045PLA.CTARED
        AND E640LCT.CODEMP = E640RAT.CODEMP
        AND E640LCT.NUMLCT = E640RAT.NUMLCT
        AND E640RAT.CODEMP = E070EMP.CODEMP
        AND E070FIL.CODEMP = E640LCT.CODEMP
        AND E070FIL.CODFIL = E640LCT.CODFIL
        AND E640RAT.CODEMP = E044CCU.CODEMP
        AND E640RAT.CODCCU = E044CCU.CODCCU
        AND E640LCT.CODEMP IN(21,
                              24)
        AND E045PLA.GRUCTA IN(3,
                              4,
                              5,
                              6,
                              7)
        AND E640RAT.SITRAT = 2
        AND E644LTE.CODEMP = E640LCT.CODEMP
        AND E644LTE.NUMLCT = E640LCT.NUMLCT
        AND E644LTE.SEQRAT = 0
        AND E600MCC.CODEMP = E644LTE.CODEMP
        AND E600MCC.NUMCCO = E644LTE.NUMCCO
        AND E600MCC.DATMOV = E644LTE.DATMOV
        AND E600MCC.SEQMOV = E644LTE.SEQMOV
        AND NOT EXISTS(
            SELECT
                1
            FROM
                E046HPD HPD
            WHERE
                    HPD.CODHPD = E046HPD.CODHPD
                AND HPD.TIPHPD = 'Z'
        )
    GROUP BY
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2)),
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4)),
        E640LCT.DATLCT,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        )),
        E640RAT.CODCCU,
        E044CCU.DESCCU,
        (
            CASE
                WHEN E045PLA.GRUCTA = '5' THEN
                    (DECODE(
                        E044CCU.TIPCCU,
                        1,
                        E640RAT.CTARED || 'CI',
                        2,
                        E640RAT.CTARED || 'CD',
                        E640RAT.CTARED || 'DA'
                    ))
                ELSE
                    CAST(E640RAT.CTARED AS VARCHAR(7))
            END
        ),
        E045PLA.MSKGCC,
        E045PLA.ABRCTA,
        E644LTE.NUMCCO,
        E640RAT.DEBCRE,
        E600MCC.NOMREC,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'TESM'
    UNION ALL /* Tesoraria Pelo Rateio */
    SELECT
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2)) MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))  ANO,
        E640LCT.DATLCT                    DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                 CODUNIDADE,
        E640RAT.CODCCU                    CODCENTRODECUSTO,
        E044CCU.DESCCU                    DESCCENTRODECUSTO,
        (
            CASE
                WHEN E045PLA.GRUCTA = '5' THEN
                    (DECODE(
                        E044CCU.TIPCCU,
                        1,
                        E640RAT.CTARED || 'CI',
                        2,
                        E640RAT.CTARED || 'CD',
                        E640RAT.CTARED || 'DA'
                    ))
                ELSE
                    CAST(E640RAT.CTARED AS VARCHAR(7))
            END
        )                                  CODCONTACONTABIL,
        E045PLA.MSKGCC                    CODCONTAEXTENSO,
        E045PLA.ABRCTA                    DESCCONTACONTABIL,
        E644LTE.NUMCCO                    DOCUMENTO,
        E640RAT.DEBCRE                    NATUREZA,
        E600MCC.NOMREC                    GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        SUM(E600RAT.VLRRAT)                VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))       CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'TESR'                            CONSULTA
    FROM
        E640RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LTE,
        E600RAT,
        E600MCC
    WHERE
            E640RAT.CODEMP = E045PLA.CODEMP
        AND E640RAT.CTARED = E045PLA.CTARED
        AND E640LCT.CODEMP = E640RAT.CODEMP
        AND E640LCT.NUMLCT = E640RAT.NUMLCT
        AND E640RAT.CODEMP = E070EMP.CODEMP
        AND E070FIL.CODEMP = E640LCT.CODEMP
        AND E070FIL.CODFIL = E640LCT.CODFIL
        AND E640RAT.CODEMP = E044CCU.CODEMP
        AND E640RAT.CODCCU = E044CCU.CODCCU
        AND E640LCT.CODEMP IN(21,
                              24)
        AND E045PLA.GRUCTA IN(3,
                              4,
                              5,
                              6,
                              7)
        AND E640RAT.SITRAT = 2
        AND E644LTE.CODEMP = E640LCT.CODEMP
        AND E644LTE.NUMLCT = E640LCT.NUMLCT
        AND E644LTE.SEQRAT <> 0
        AND E600MCC.CODEMP = E644LTE.CODEMP
        AND E600MCC.NUMCCO = E644LTE.NUMCCO
        AND E600MCC.DATMOV = E644LTE.DATMOV
        AND E600MCC.SEQMOV = E644LTE.SEQMOV
        AND E600RAT.CODEMP = E644LTE.CODEMP
        AND E600RAT.NUMCCO = E644LTE.NUMCCO
        AND E600RAT.DATMOV = E644LTE.DATMOV
        AND E600RAT.SEQMOV = E644LTE.SEQMOV
        AND E600RAT.SEQMOV = E644LTE.SEQMOV
        AND E600RAT.SEQRAT = E644LTE.SEQRAT
        AND E600RAT.CODCCU = E640RAT.CODCCU
        AND NOT EXISTS(
            SELECT
                1
            FROM
                E046HPD HPD
            WHERE
                    HPD.CODHPD = E046HPD.CODHPD
                AND HPD.TIPHPD = 'Z'
        )
    GROUP BY
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2)),
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4)),
        E640LCT.DATLCT,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        )),
        E640RAT.CODCCU,
        E044CCU.DESCCU,
        (
            CASE
                WHEN E045PLA.GRUCTA = '5' THEN
                    (DECODE(
                        E044CCU.TIPCCU,
                        1,
                        E640RAT.CTARED || 'CI',
                        2,
                        E640RAT.CTARED || 'CD',
                        E640RAT.CTARED || 'DA'
                    ))
                ELSE
                    CAST(E640RAT.CTARED AS VARCHAR(7))
            END
        ),
        E045PLA.MSKGCC,
        E045PLA.ABRCTA,
        E644LTE.NUMCCO,
        E640RAT.DEBCRE,
        E600MCC.NOMREC,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'TESR';