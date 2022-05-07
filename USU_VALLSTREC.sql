CREATE OR REPLACE FORCE VIEW "SAPIENS"."USU_VALLSTREC"(
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
        CAST(NULL AS VARCHAR2(500))       DOCUMENTO,
        E640RAT.DEBCRE                    NATUREZA,
        CAST(NULL AS VARCHAR2(500))       GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                    VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500)) CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'RECR'                            CONSULTA
    FROM
        E640RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LTI,
        E301MCR,
        E301RAT
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
        AND E644LTI.CODEMP = E640LCT.CODEMP
        AND E644LTI.NUMLCT = E640LCT.NUMLCT
        AND E644LTI.CODFOR = 0
        AND E644LTI.SEQRAT <> 0
        AND E301MCR.CODEMP = E644LTI.CODEMP
        AND E301MCR.CODFIL = E644LTI.CODFIL
        AND E301MCR.NUMTIT = E644LTI.NUMTIT
        AND E301MCR.CODTPT = E644LTI.CODTPT
        AND E301MCR.SEQMOV = E644LTI.SEQMOV
        AND E301RAT.CODEMP = E301MCR.CODEMP
        AND E301RAT.CODFIL = E301MCR.CODFIL
        AND E301RAT.NUMTIT = E301MCR.NUMTIT
        AND E301RAT.CODTPT = E301MCR.CODTPT
        AND E301RAT.SEQMOV = E301MCR.SEQMOV
        AND E301RAT.SEQRAT = E644LTI.SEQRAT
        AND NOT EXISTS(
            SELECT
                1
            FROM
                E046HPD HPD
            WHERE
                    HPD.CODHPD = E046HPD.CODHPD
                AND HPD.TIPHPD = 'Z'
        )
    UNION ALL /* Titulos integrados pelo movimento */
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
        CAST(NULL AS VARCHAR2(500))       DOCUMENTO,
        E640RAT.DEBCRE                    NATUREZA,
        CAST(NULL AS VARCHAR2(500))       GERADOR,
        CAST(NULL AS VARCHAR2(10))        RATEIO,
        CAST(NULL AS VARCHAR2(10))        DIMENSAO,
        E640RAT.VLRRAT                    VALOR,
        HISTPADRAO(E640LCT.DATLCT, E046HPD.DESHPD, REPLACE(E640LCT.CPLLCT,'''','')) HISTORICO,
        CAST(NULL AS NUMBER(10))        CODPROJETO,
        CAST(NULL AS VARCHAR2(500)) CAMINHO,
        E640LCT.NUMLCT,
        E640LCT.ORILCT,
        E640RAT.CTARED,
        'RECM'                            CONSULTA
    FROM
        E640RAT,
        E045PLA,
        E070EMP,
        E044CCU,
        E070FIL,(
            E640LCT
            LEFT OUTER JOIN E046HPD ON(E640LCT.CODHPD = E046HPD.CODHPD)
        ),
        E644LTI,
        E301MCR,
        E301RAT
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
        AND E644LTI.CODEMP = E640LCT.CODEMP
        AND E644LTI.NUMLCT = E640LCT.NUMLCT
        AND E644LTI.CODFOR = 0
        AND E644LTI.SEQRAT = 0
        AND E301MCR.CODEMP = E644LTI.CODEMP
        AND E301MCR.CODFIL = E644LTI.CODFIL
        AND E301MCR.NUMTIT = E644LTI.NUMTIT
        AND E301MCR.CODTPT = E644LTI.CODTPT
        AND E301MCR.SEQMOV = E644LTI.SEQMOV
        AND E301RAT.CODEMP = E301MCR.CODEMP
        AND E301RAT.CODFIL = E301MCR.CODFIL
        AND E301RAT.NUMTIT = E301MCR.NUMTIT
        AND E301RAT.CODTPT = E301MCR.CODTPT
        AND E301RAT.SEQMOV = E301MCR.SEQMOV
        AND E301RAT.CODCCU = E640RAT.CODCCU
        AND NOT EXISTS(
            SELECT
                1
            FROM
                E046HPD HPD
            WHERE
                    HPD.CODHPD = E046HPD.CODHPD
                AND HPD.TIPHPD = 'Z'
        );