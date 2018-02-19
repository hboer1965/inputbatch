SELECT PID, '#' || NAME || '{' || VERSIONNUMBER || '}' NAME_VERSION, EXPRESSION, ARGUMENTS 
FROM IPDE_BASICEXPRESSIONS BEX
LEFT OUTER JOIN ( SELECT BASICEXPRESSION_PID, TRIM(LISTAGG(DECODE(DATA_TYPE,
                                                                 'N', 'INTEGER',
                                                                 'F', 'FLOAT',
                                                                 'D', 'DATE',
                                                                 'T', 'DATE',
                                                                 'B', 'BOOLEAN',
                                                                 'C', 'STRING'
                                                                 ) ||' ' || NAME,
                                                            ',') WITHIN GROUP (ORDER BY SEQUENCE)) ARGUMENTS
                  FROM IPDE_ARGUMENTS
                  GROUP BY BASICEXPRESSION_PID
                ) ARGS ON ARGS.BASICEXPRESSION_PID = BEX.PID
minus
SELECT PID, '#' || NAME || '{' || VERSIONNUMBER || '}' NAME_VERSION, EXPRESSION, formatted_ARGUMENTS
FROM IPDE_BASICEXPRESSIONS BEX
;
