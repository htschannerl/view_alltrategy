CREATE OR REPLACE FORCE VIEW "SAPIENS"."USU_VALLSTNFS"(
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
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2))   MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))    ANO,
        E640LCT.DATLCT                      DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                   CODUNIDADE,
        E640RAT.CODCCU                      CODCENTRODECUSTO,
        E044CCU.DESCCU                      DESCCENTRODECUSTO,
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
        )                                    CODCONTACONTABIL,
        E045PLA.MSKGCC                      CODCONTAEXTENSO,
        E045PLA.ABRCTA                      DESCCONTACONTABIL,
        CAST(E644LNF.NUMNFI AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))                CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSG'                              CONSULTA
    FROM
        E640RAT,
        E140RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E085CLI,
        E140NFV
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
        AND E644LNF.CODEMP = E640LCT.CODEMP
        AND E644LNF.NUMLCT = E640LCT.NUMLCT
        AND E644LNF.SEQRAT = 0
        AND E644LNF.SEQIPR = 0
        AND E644LNF.SEQISE = 0
        AND E644LNF.CODFOR = 0
        AND E085CLI.CODCLI = E140NFV.CODCLI
        AND E140RAT.CODEMP = E644LNF.CODEMP
        AND E140RAT.CODFIL = E644LNF.CODFIL
        AND E140RAT.NUMNFV = E644LNF.NUMNFI
        AND E140RAT.CODSNF = E644LNF.CODSNF
        AND E140NFV.CODEMP = E644LNF.CODEMP
        AND E140NFV.CODFIL = E644LNF.CODFIL
        AND E140NFV.NUMNFV = E644LNF.NUMNFI
        AND E140NFV.CODSNF = E644LNF.CODSNF
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSG'
    UNION ALL /* Dados com rateio da NFS */
    SELECT
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2))   MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))    ANO,
        E640LCT.DATLCT                      DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                   CODUNIDADE,
        E640RAT.CODCCU                      CODCENTRODECUSTO,
        E044CCU.DESCCU                      DESCCENTRODECUSTO,
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
        )                                    CODCONTACONTABIL,
        E045PLA.MSKGCC                      CODCONTAEXTENSO,
        E045PLA.ABRCTA                      DESCCONTACONTABIL,
        CAST(E644LNF.NUMNFI AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))                CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSR'                              CONSULTA
    FROM
        E640RAT,
        E140RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E085CLI,
        E140NFV
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
        AND E644LNF.CODEMP = E640LCT.CODEMP
        AND E644LNF.NUMLCT = E640LCT.NUMLCT
        AND E644LNF.SEQRAT <> 0
        AND E644LNF.SEQIPR = 0
        AND E644LNF.SEQISE = 0
        AND E644LNF.CODFOR = 0
        AND E085CLI.CODCLI = E140NFV.CODCLI
        AND E140RAT.CODEMP = E644LNF.CODEMP
        AND E140RAT.CODFIL = E644LNF.CODFIL
        AND E140RAT.NUMNFV = E644LNF.NUMNFI
        AND E140RAT.CODSNF = E644LNF.CODSNF
        AND E140RAT.SEQRAT = E644LNF.SEQRAT
        AND E140NFV.CODEMP = E644LNF.CODEMP
        AND E140NFV.CODFIL = E644LNF.CODFIL
        AND E140NFV.NUMNFV = E644LNF.NUMNFI
        AND E140NFV.CODSNF = E644LNF.CODSNF
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSR'
    UNION ALL /* Dados dos itens com rateio da NFS */
    SELECT
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2))   MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))    ANO,
        E640LCT.DATLCT                      DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                   CODUNIDADE,
        E640RAT.CODCCU                      CODCENTRODECUSTO,
        E044CCU.DESCCU                      DESCCENTRODECUSTO,
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
        )                                    CODCONTACONTABIL,
        E045PLA.MSKGCC                      CODCONTAEXTENSO,
        E045PLA.ABRCTA                      DESCCONTACONTABIL,
        CAST(E644LNF.NUMNFI AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))                CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSP'                              CONSULTA
    FROM
        E640RAT,
        E140RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E085CLI,
        E140NFV,
        E140IPV
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
        AND E644LNF.CODEMP = E640LCT.CODEMP
        AND E644LNF.NUMLCT = E640LCT.NUMLCT
        AND E644LNF.SEQRAT = 0
        AND E644LNF.SEQIPR <> 0
        AND E644LNF.SEQISE = 0
        AND E644LNF.CODFOR = 0
        AND E085CLI.CODCLI = E140NFV.CODCLI
        AND E140RAT.CODEMP = E644LNF.CODEMP
        AND E140RAT.CODFIL = E644LNF.CODFIL
        AND E140RAT.NUMNFV = E644LNF.NUMNFI
        AND E140RAT.CODSNF = E644LNF.CODSNF
        AND E140RAT.SEQIPV = E140IPV.SEQIPV
        AND E140NFV.CODEMP = E644LNF.CODEMP
        AND E140NFV.CODFIL = E644LNF.CODFIL
        AND E140NFV.NUMNFV = E644LNF.NUMNFI
        AND E140NFV.CODSNF = E644LNF.CODSNF
        AND E140IPV.CODEMP = E644LNF.CODEMP
        AND E140IPV.CODFIL = E644LNF.CODFIL
        AND E140IPV.NUMNFV = E644LNF.NUMNFI
        AND E140IPV.CODSNF = E644LNF.CODSNF
        AND E140IPV.SEQIPV = E644LNF.SEQIPR
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSP'
    UNION ALL /* Dados dos itens com rateio da NFS pelo geral */
    SELECT
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2))   MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))    ANO,
        E640LCT.DATLCT                      DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                   CODUNIDADE,
        E640RAT.CODCCU                      CODCENTRODECUSTO,
        E044CCU.DESCCU                      DESCCENTRODECUSTO,
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
        )                                    CODCONTACONTABIL,
        E045PLA.MSKGCC                      CODCONTAEXTENSO,
        E045PLA.ABRCTA                      DESCCONTACONTABIL,
        CAST(E644LNF.NUMNFI AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))                CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSPRG'                            CONSULTA
    FROM
        E640RAT,
        E140RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E085CLI,
        E140NFV,
        E140IPV
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
        AND E644LNF.CODEMP = E640LCT.CODEMP
        AND E644LNF.NUMLCT = E640LCT.NUMLCT
        AND E644LNF.SEQRAT = 0
        AND E644LNF.SEQIPR <> 0
        AND E644LNF.SEQISE = 0
        AND E644LNF.CODFOR = 0
        AND E085CLI.CODCLI = E140NFV.CODCLI
        AND E140RAT.CODEMP = E644LNF.CODEMP
        AND E140RAT.CODFIL = E644LNF.CODFIL
        AND E140RAT.NUMNFV = E644LNF.NUMNFI
        AND E140RAT.CODSNF = E644LNF.CODSNF
        AND E140RAT.SEQIPV = 0
        AND E140NFV.CODEMP = E644LNF.CODEMP
        AND E140NFV.CODFIL = E644LNF.CODFIL
        AND E140NFV.NUMNFV = E644LNF.NUMNFI
        AND E140NFV.CODSNF = E644LNF.CODSNF
        AND E140IPV.CODEMP = E644LNF.CODEMP
        AND E140IPV.CODFIL = E644LNF.CODFIL
        AND E140IPV.NUMNFV = E644LNF.NUMNFI
        AND E140IPV.CODSNF = E644LNF.CODSNF
        AND E140IPV.SEQIPV = E644LNF.SEQIPR
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSPRG'
    UNION ALL /* Dados dos serviços com rateio da NFS */
    SELECT
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2))   MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))    ANO,
        E640LCT.DATLCT                      DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                   CODUNIDADE,
        E640RAT.CODCCU                      CODCENTRODECUSTO,
        E044CCU.DESCCU                      DESCCENTRODECUSTO,
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
        )                                    CODCONTACONTABIL,
        E045PLA.MSKGCC                      CODCONTAEXTENSO,
        E045PLA.ABRCTA                      DESCCONTACONTABIL,
        CAST(E644LNF.NUMNFI AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))                CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSS'                              CONSULTA
    FROM
        E640RAT,
        E140RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E085CLI,
        E140NFV,
        E140ISV
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
        AND E644LNF.CODEMP = E640LCT.CODEMP
        AND E644LNF.NUMLCT = E640LCT.NUMLCT
        AND E644LNF.SEQRAT = 0
        AND E644LNF.SEQIPR = 0
        AND E644LNF.SEQISE <> 0
        AND E644LNF.CODFOR = 0
        AND E085CLI.CODCLI = E140NFV.CODCLI
        AND E140RAT.CODEMP = E644LNF.CODEMP
        AND E140RAT.CODFIL = E644LNF.CODFIL
        AND E140RAT.NUMNFV = E644LNF.NUMNFI
        AND E140RAT.CODSNF = E644LNF.CODSNF
        AND E140RAT.SEQISV = E140ISV.SEQISV
        AND E140NFV.CODEMP = E644LNF.CODEMP
        AND E140NFV.CODFIL = E644LNF.CODFIL
        AND E140NFV.NUMNFV = E644LNF.NUMNFI
        AND E140NFV.CODSNF = E644LNF.CODSNF
        AND E140ISV.CODEMP = E644LNF.CODEMP
        AND E140ISV.CODFIL = E644LNF.CODFIL
        AND E140ISV.NUMNFV = E644LNF.NUMNFI
        AND E140ISV.CODSNF = E644LNF.CODSNF
        AND E140ISV.SEQISV = E644LNF.SEQISE
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSS'
    UNION ALL /* Dados dos serviços com rateio da NFS sem sequencia do item */
    SELECT
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2))   MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))    ANO,
        E640LCT.DATLCT                      DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                   CODUNIDADE,
        E640RAT.CODCCU                      CODCENTRODECUSTO,
        E044CCU.DESCCU                      DESCCENTRODECUSTO,
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
        )                                    CODCONTACONTABIL,
        E045PLA.MSKGCC                      CODCONTAEXTENSO,
        E045PLA.ABRCTA                      DESCCONTACONTABIL,
        CAST(E644LNF.NUMNFI AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))                CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSSSR'                            CONSULTA
    FROM
        E640RAT,
        E140RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E085CLI,
        E140NFV,
        E140ISV
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
        AND E644LNF.CODEMP = E640LCT.CODEMP
        AND E644LNF.NUMLCT = E640LCT.NUMLCT
        AND E644LNF.SEQRAT = 0
        AND E644LNF.SEQIPR = 0
        AND E644LNF.SEQISE <> 0
        AND E644LNF.CODFOR = 0
        AND E085CLI.CODCLI = E140NFV.CODCLI
        AND E140RAT.CODEMP = E644LNF.CODEMP
        AND E140RAT.CODFIL = E644LNF.CODFIL
        AND E140RAT.NUMNFV = E644LNF.NUMNFI
        AND E140RAT.CODSNF = E644LNF.CODSNF
        AND E140RAT.SEQISV = 0
        AND E140NFV.CODEMP = E644LNF.CODEMP
        AND E140NFV.CODFIL = E644LNF.CODFIL
        AND E140NFV.NUMNFV = E644LNF.NUMNFI
        AND E140NFV.CODSNF = E644LNF.CODSNF
        AND E140ISV.CODEMP = E644LNF.CODEMP
        AND E140ISV.CODFIL = E644LNF.CODFIL
        AND E140ISV.NUMNFV = E644LNF.NUMNFI
        AND E140ISV.CODSNF = E644LNF.CODSNF
        AND E140ISV.SEQISV = E644LNF.SEQISE
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSSSR'
    UNION ALL /* Dados da NFS sem rateio */
    SELECT
        CAST(EXTRACT(MONTH FROM E640LCT.DATLCT) AS NUMBER(2))   MES,
        CAST(EXTRACT(YEAR FROM E640LCT.DATLCT) AS NUMBER(4))    ANO,
        E640LCT.DATLCT                      DATA,
        (DECODE(
            E640LCT.CODEMP,
            21,
            '100',
            24,
            '200'
        ))                                   CODUNIDADE,
        E640RAT.CODCCU                      CODCENTRODECUSTO,
        E044CCU.DESCCU                      DESCCENTRODECUSTO,
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
        )                                    CODCONTACONTABIL,
        E045PLA.MSKGCC                      CODCONTAEXTENSO,
        E045PLA.ABRCTA                      DESCCONTACONTABIL,
        CAST(E644LNF.NUMNFI AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500))                CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSSR'                             CONSULTA
    FROM
        E640RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E085CLI,
        E140NFV
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
        AND E644LNF.CODFOR = 0
        AND E640RAT.SITRAT = 2
        AND E644LNF.CODEMP = E640LCT.CODEMP
        AND E644LNF.NUMLCT = E640LCT.NUMLCT
        AND E085CLI.CODCLI = E140NFV.CODCLI
        AND E140NFV.CODEMP = E644LNF.CODEMP
        AND E140NFV.CODFIL = E644LNF.CODFIL
        AND E140NFV.NUMNFV = E644LNF.NUMNFI
        AND E140NFV.CODSNF = E644LNF.CODSNF
        AND NOT EXISTS(
            SELECT
                1
            FROM
                E140RAT
            WHERE
                    E140RAT.CODEMP = E644LNF.CODEMP
                AND E140RAT.CODFIL = E644LNF.CODFIL
                AND E140RAT.NUMNFV = E644LNF.NUMNFI
                AND E140RAT.CODSNF = E644LNF.CODSNF
        )
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E085CLI.CODCLI
        || ' - '
        || E085CLI.NOMCLI,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) ,
        CAST(NULL AS NUMBER(10)),
        CAST(NULL AS VARCHAR2(500)),
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFSSR';