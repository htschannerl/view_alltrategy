CREATE OR REPLACE FORCE VIEW "SAPIENS"."USU_VALLSTNFE"(
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
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''',''))    HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        E000ANX.LOCANX                      CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFEG'                              CONSULTA
    FROM
        E640RAT,
        E440RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E095FOR,(
            E440NFC
            LEFT OUTER JOIN E000ANX ON(E000ANX.CODEMP = E440NFC.CODEMP
                                       AND E000ANX.ROTANX = 18
                                       AND E000ANX.NUMANX = E440NFC.NUMANX
                                       AND E000ANX.SEQANX = 1)
        )
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
        AND E095FOR.CODFOR = E644LNF.CODFOR
        AND E440RAT.CODEMP = E644LNF.CODEMP
        AND E440RAT.CODFIL = E644LNF.CODFIL
        AND E440RAT.CODFOR = E644LNF.CODFOR
        AND E440RAT.NUMNFC = E644LNF.NUMNFI
        AND E440RAT.CODSNF = E644LNF.CODSNF
        AND E440NFC.CODEMP = E644LNF.CODEMP
        AND E440NFC.CODFIL = E644LNF.CODFIL
        AND E440NFC.CODFOR = E644LNF.CODFOR
        AND E440NFC.NUMNFC = E644LNF.NUMNFI
        AND E440NFC.CODSNF = E644LNF.CODSNF
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')),
        CAST(NULL AS NUMBER(10)),
        E000ANX.LOCANX,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFEG'
    UNION ALL /* Dados com rateio da NFE */
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
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''',''))     HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        E000ANX.LOCANX                      CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFER'                              CONSULTA
    FROM
        E640RAT,
        E440RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E095FOR,(
            E440NFC
            LEFT OUTER JOIN E000ANX ON(E000ANX.CODEMP = E440NFC.CODEMP
                                       AND E000ANX.ROTANX = 18
                                       AND E000ANX.NUMANX = E440NFC.NUMANX
                                       AND E000ANX.SEQANX = 1)
        )
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
        AND E095FOR.CODFOR = E644LNF.CODFOR
        AND E440RAT.CODEMP = E644LNF.CODEMP
        AND E440RAT.CODFIL = E644LNF.CODFIL
        AND E440RAT.CODFOR = E644LNF.CODFOR
        AND E440RAT.NUMNFC = E644LNF.NUMNFI
        AND E440RAT.CODSNF = E644LNF.CODSNF
        AND E440RAT.SEQRAT = E644LNF.SEQRAT
        AND E440NFC.CODEMP = E644LNF.CODEMP
        AND E440NFC.CODFIL = E644LNF.CODFIL
        AND E440NFC.CODFOR = E644LNF.CODFOR
        AND E440NFC.NUMNFC = E644LNF.NUMNFI
        AND E440NFC.CODSNF = E644LNF.CODSNF
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')),
        CAST(NULL AS NUMBER(10)),
        E000ANX.LOCANX,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFER'
    UNION ALL /* Dados dos itens com rateio da NFE */
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
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''',''))  HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        E000ANX.LOCANX                      CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFEP'                              CONSULTA
    FROM
        E640RAT,
        E440IPC,
        E440RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E095FOR,(
            E440NFC
            LEFT OUTER JOIN E000ANX ON(E000ANX.CODEMP = E440NFC.CODEMP
                                       AND E000ANX.ROTANX = 18
                                       AND E000ANX.NUMANX = E440NFC.NUMANX
                                       AND E000ANX.SEQANX = 1)
        )
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
        AND E644LNF.SEQIPR <> 0
        AND E095FOR.CODFOR = E644LNF.CODFOR
        AND E440IPC.CODEMP = E644LNF.CODEMP
        AND E440IPC.CODFIL = E644LNF.CODFIL
        AND E440IPC.CODFOR = E644LNF.CODFOR
        AND E440IPC.NUMNFC = E644LNF.NUMNFI
        AND E440IPC.CODSNF = E644LNF.CODSNF
        AND E440IPC.SEQIPC = E644LNF.SEQIPR
        AND E440RAT.CODEMP = E440IPC.CODEMP
        AND E440RAT.CODFIL = E440IPC.CODFIL
        AND E440RAT.CODFOR = E440IPC.CODFOR
        AND E440RAT.NUMNFC = E440IPC.NUMNFC
        AND E440RAT.CODSNF = E440IPC.CODSNF
        AND E440RAT.SEQIPC = E440IPC.SEQIPC
        AND E440NFC.CODEMP = E644LNF.CODEMP
        AND E440NFC.CODFIL = E644LNF.CODFIL
        AND E440NFC.CODFOR = E644LNF.CODFOR
        AND E440NFC.NUMNFC = E644LNF.NUMNFI
        AND E440NFC.CODSNF = E644LNF.CODSNF
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')),
        CAST(NULL AS NUMBER(10)),
        E000ANX.LOCANX,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFEP'
    UNION ALL /* Dados dos servicos com rateio da NFE */
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
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''',''))  HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        E000ANX.LOCANX                      CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFES'                              CONSULTA
    FROM
        E640RAT,
        E440ISC,
        E440RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LNF,
        E095FOR,(
            E440NFC
            LEFT OUTER JOIN E000ANX ON(E000ANX.CODEMP = E440NFC.CODEMP
                                       AND E000ANX.ROTANX = 18
                                       AND E000ANX.NUMANX = E440NFC.NUMANX
                                       AND E000ANX.SEQANX = 1)
        )
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
        AND E644LNF.SEQISE <> 0
        AND E095FOR.CODFOR = E644LNF.CODFOR
        AND E440ISC.CODEMP = E644LNF.CODEMP
        AND E440ISC.CODFIL = E644LNF.CODFIL
        AND E440ISC.CODFOR = E644LNF.CODFOR
        AND E440ISC.NUMNFC = E644LNF.NUMNFI
        AND E440ISC.CODSNF = E644LNF.CODSNF
        AND E440ISC.SEQISC = E644LNF.SEQISE
        AND E440RAT.CODEMP = E440ISC.CODEMP
        AND E440RAT.CODFIL = E440ISC.CODFIL
        AND E440RAT.CODFOR = E440ISC.CODFOR
        AND E440RAT.NUMNFC = E440ISC.NUMNFC
        AND E440RAT.CODSNF = E440ISC.CODSNF
        AND E440RAT.SEQISC = E440ISC.SEQISC
        AND E440NFC.CODEMP = E644LNF.CODEMP
        AND E440NFC.CODFIL = E644LNF.CODFIL
        AND E440NFC.CODFOR = E644LNF.CODFOR
        AND E440NFC.NUMNFC = E644LNF.NUMNFI
        AND E440NFC.CODSNF = E644LNF.CODSNF
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')),
        CAST(NULL AS NUMBER(10)),
        E000ANX.LOCANX,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFES' /* Dados da NFE sem rateio */
    UNION ALL
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
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR                   GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                      VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''',''))   HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        E000ANX.LOCANX                      CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFESR'                             CONSULTA
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
        E095FOR,(
            E440NFC
            LEFT OUTER JOIN E000ANX ON(E000ANX.CODEMP = E440NFC.CODEMP
                                       AND E000ANX.ROTANX = 18
                                       AND E000ANX.NUMANX = E440NFC.NUMANX
                                       AND E000ANX.SEQANX = 1)
        )
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
        AND E095FOR.CODFOR = E644LNF.CODFOR
        AND E440NFC.CODEMP = E644LNF.CODEMP
        AND E440NFC.CODFIL = E644LNF.CODFIL
        AND E440NFC.CODFOR = E644LNF.CODFOR
        AND E440NFC.NUMNFC = E644LNF.NUMNFI
        AND E440NFC.CODSNF = E644LNF.CODSNF
        AND NOT EXISTS(
            SELECT
                1
            FROM
                E440RAT
            WHERE
                    E440RAT.CODEMP = E644LNF.CODEMP
                AND E440RAT.CODFIL = E644LNF.CODFIL
                AND E440RAT.CODFOR = E644LNF.CODFOR
                AND E440RAT.NUMNFC = E644LNF.NUMNFI
                AND E440RAT.CODSNF = E644LNF.CODSNF
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
        CAST(E644LNF.NUMNFI AS VARCHAR(255)),
        E640RAT.DEBCRE,
        E095FOR.CODFOR
        || ' - '
        || E095FOR.NOMFOR,
        CAST(NULL AS VARCHAR2(10)),
        CAST(NULL AS VARCHAR2(10)),
        E640RAT.VLRRAT,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')),
        CAST(NULL AS NUMBER(10)),
        E000ANX.LOCANX,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'NFESR';