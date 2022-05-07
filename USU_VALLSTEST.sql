CREATE OR REPLACE FORCE VIEW "SAPIENS"."USU_VALLSTEST"(
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
            E640LCT.CODEMP,21,
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
        CAST(E210MVP.NUMDOC AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E210MVP.USUREC
        || ' - '
        || R910USU.NOMCOM                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        SUM(E210RAT.VLRRAT)                  VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''',''))  HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR(500))                                  CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'EST'                               CONSULTA
    FROM
        E640RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LES,
        E210MVP,
        R910USU,
        E210RAT
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
        AND E644LES.CODEMP = E640LCT.CODEMP
        AND E644LES.NUMLCT = E640LCT.NUMLCT
        AND E210MVP.CODEMP = E644LES.CODEMP
        AND E210MVP.CODPRO = E644LES.CODPRO
        AND E210MVP.CODDER = E644LES.CODDER
        AND E210MVP.CODDEP = E644LES.CODDEP
        AND E210MVP.DATMOV = E644LES.DATMOV
        AND E210MVP.SEQMOV = E644LES.SEQMOV
        AND R910USU.CODENT = E210MVP.USUREC
        AND E210RAT.CODEMP = E210MVP.CODEMP
        AND E210RAT.CODPRO = E210MVP.CODPRO
        AND E210RAT.CODDER = E210MVP.CODDER
        AND E210RAT.CODDEP = E210MVP.CODDEP
        AND E210RAT.DATMOV = E210MVP.DATMOV
        AND E210RAT.SEQMOV = E210MVP.SEQMOV
        AND E210RAT.SEQRAT = E644LES.SEQRAT
        AND E210RAT.CODCCU = E640RAT.CODCCU
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
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        EXTRACT(MONTH FROM E640LCT.DATLCT),
        EXTRACT(YEAR FROM E640LCT.DATLCT),
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
        CAST(E210MVP.NUMDOC AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E210MVP.USUREC
        || ' - '
        || R910USU.NOMCOM,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')),
        CAST(NULL AS NUMBER(10)),
        '',
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'EST'
    UNION /* PELO MOVIMENTO */
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
        CAST(E210MVP.NUMDOC AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E210MVP.USUREC
        || ' - '
        || R910USU.NOMCOM                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        SUM(E210RAT.VLRRAT)                  VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''',''))   HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR(500))                                  CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'EST'                               CONSULTA
    FROM
        E640RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LES,
        E210MVP,
        R910USU,
        E210RAT
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
        AND E644LES.CODEMP = E640LCT.CODEMP
        AND E644LES.NUMLCT = E640LCT.NUMLCT
        AND E210MVP.CODEMP = E644LES.CODEMP
        AND E210MVP.CODPRO = E644LES.CODPRO
        AND E210MVP.CODDER = E644LES.CODDER
        AND E210MVP.CODDEP = E644LES.CODDEP
        AND E210MVP.DATMOV = E644LES.DATMOV
        AND E210MVP.SEQMOV = E644LES.SEQMOV
        AND R910USU.CODENT = E210MVP.USUREC
        AND E210RAT.CODEMP = E210MVP.CODEMP
        AND E210RAT.CODPRO = E210MVP.CODPRO
        AND E210RAT.CODDER = E210MVP.CODDER
        AND E210RAT.CODDEP = E210MVP.CODDEP
        AND E210RAT.DATMOV = E210MVP.DATMOV
        AND E210RAT.SEQMOV = E210MVP.SEQMOV
        AND E210RAT.CODCCU = E640RAT.CODCCU
        AND E644LES.SEQRAT = 0
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
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        EXTRACT(MONTH FROM E640LCT.DATLCT),
        EXTRACT(YEAR FROM E640LCT.DATLCT),
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
        CAST(E210MVP.NUMDOC AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E210MVP.USUREC
        || ' - '
        || R910USU.NOMCOM,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')),
        CAST(NULL AS NUMBER(10)),
        '',
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'EST'
    UNION /*SEM RATEIO NO MOVIMENTO DE ESTOQUE*/
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
        CAST(E210MVP.NUMDOC AS VARCHAR(255)) DOCUMENTO,
        E640RAT.DEBCRE                      NATUREZA,
        E210MVP.USUREC
        || ' - '
        || R910USU.NOMCOM                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        SUM(E640RAT.VLRRAT)                  VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR(500))                                  CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'EST'                               CONSULTA
    FROM
        E640RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LES,
        E210MVP,
        R910USU
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
        AND E644LES.CODEMP = E640LCT.CODEMP
        AND E644LES.NUMLCT = E640LCT.NUMLCT
        AND E210MVP.CODEMP = E644LES.CODEMP
        AND E210MVP.CODPRO = E644LES.CODPRO
        AND E210MVP.CODDER = E644LES.CODDER
        AND E210MVP.CODDEP = E644LES.CODDEP
        AND E210MVP.DATMOV = E644LES.DATMOV
        AND E210MVP.SEQMOV = E644LES.SEQMOV
        AND R910USU.CODENT = E210MVP.USUREC
        AND E644LES.SEQRAT = 0
        AND NOT EXISTS(
            SELECT
                1
            FROM
                E210RAT
            WHERE
                    E210RAT.CODEMP = E210MVP.CODEMP
                AND E210RAT.CODPRO = E210MVP.CODPRO
                AND E210RAT.CODDER = E210MVP.CODDER
                AND E210RAT.CODDEP = E210MVP.CODDEP
                AND E210RAT.DATMOV = E210MVP.DATMOV
                AND E210RAT.SEQMOV = E210MVP.SEQMOV
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
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        EXTRACT(MONTH FROM E640LCT.DATLCT),
        EXTRACT(YEAR FROM E640LCT.DATLCT),
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
        CAST(E210MVP.NUMDOC AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E210MVP.USUREC
        || ' - '
        || R910USU.NOMCOM,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')),
        CAST(NULL AS NUMBER(10)),
        '',
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'EST';