create or replace FUNCTION HISTPADRAO 
(
  DDATLCT IN DATE 
, ADESHPD IN VARCHAR2 
, CPLLCT IN VARCHAR2
) RETURN VARCHAR2 IS
vRetorno VARCHAR2(1000);
BEGIN
SELECT TRANSLATE(DECODE(
    NVL(
        LENGTH(
            REGEXP_REPLACE(
                ADESHPD,
                '[^*]',
                NULL
            )
        ),
        0
    ),
    0,
    ADESHPD
    || ' '
    || REPLACE(CPLLCT,'''',''),
    DECODE(
        NVL(
            LENGTH(
                REGEXP_REPLACE(
                    ADESHPD,
                    '[^*]',
                    NULL
                )
            ),
            0
        ),
        1,
        REGEXP_REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            ADESHPD,
                            '*MA',
                            EXTRACT(MONTH FROM DDATLCT)
                            || '/'
                            || EXTRACT(YEAR FROM DDATLCT)
                        ),
                        '*DMA',
                        EXTRACT(DAY FROM DDATLCT)
                        || '/'
                        || EXTRACT(MONTH FROM DDATLCT)
                        || '/'
                        || EXTRACT(YEAR FROM DDATLCT)
                    ),
                    '*ALF',
                    '*'
                ),
                '*NUM',
                '*'
            ),
            '[*]',
            (SUBSTR(
                REPLACE(CPLLCT,'''',''),
                1
            )),
            1,
            1
        ),
        DECODE(
            NVL(
                LENGTH(
                    REGEXP_REPLACE(
                        ADESHPD,
                        '[^*]',
                        NULL
                    )
                ),
                0
            ),
            2,
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    ADESHPD,
                                    '*MA',
                                    '*'
                                ),
                                '*DMA',
                                '*'
                            ),
                            '*ALF',
                            '*'
                        ),
                        '*NUM',
                        '*'
                    ),
                    '[*]',
                    (SUBSTR(
                        REPLACE(CPLLCT,'''',''),
                        1,
                        INSTR(
                                             REPLACE(CPLLCT,'''',''),
                                             ',',
                                             1,
                                             1
                                         )- 1
                    )),
                    1,
                    1
                ),
                '[*]',
                (SUBSTR(
                    REPLACE(CPLLCT,'''',''),
                    (INSTR(
                                     REPLACE(CPLLCT,'''',''),
                                     ',',
                                     1,
                                     1
                                 )+ 1)
                )),
                1,
                1
            ),
            DECODE(
                NVL(
                    LENGTH(
                        REGEXP_REPLACE(
                            ADESHPD,
                            '[^*]',
                            NULL
                        )
                    ),
                    0
                ),
                3,
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            ADESHPD,
                                            '*MA',
                                            '*'
                                        ),
                                        '*DMA',
                                        '*'
                                    ),
                                    '*ALF',
                                    '*'
                                ),
                                '*NUM',
                                '*'
                            ),
                            '[*]',
                            (SUBSTR(
                                REPLACE(CPLLCT,'''',''),
                                1,
                                INSTR(
                                                             REPLACE(CPLLCT,'''',''),
                                                             ',',
                                                             1,
                                                             1
                                                         )- 1
                            )),
                            1,
                            1
                        ),
                        '[*]',
                        (SUBSTR(
                            REPLACE(CPLLCT,'''',''),
                            (INSTR(
                                                     REPLACE(CPLLCT,'''',''),
                                                     ',',
                                                     1,
                                                     1
                                                 )+ 1),
                            (INSTR(
                                                     REPLACE(CPLLCT,'''',''),
                                                     ',',
                                                     1,
                                                     2
                                                 )-(INSTR(
                                                     REPLACE(CPLLCT,'''',''),
                                                     ',',
                                                     1,
                                                     1
                                                 )+ 1))
                        )),
                        1,
                        1
                    ),
                    '[*]',
                    (SUBSTR(
                        REPLACE(CPLLCT,'''',''),
                        (INSTR(
                                             REPLACE(CPLLCT,'''',''),
                                             ',',
                                             1,
                                             2
                                         )+ 1)
                    )),
                    1,
                    1
                ),
                ''
            )
        )
    )
),chr(10)||chr(11)||chr(13), ' ') INTO vRetorno FROM DUAL;
RETURN vRetorno;
END HISTPADRAO;