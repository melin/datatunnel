grammar DtunnelStatement;

singleStatement
    : statement EOF
    ;

statement
    : dtunnelStatement                                                     #dtunnelCommand
    | .*?                                                                  #passThrough
    ;

dtunnelStatement
    : DATATUNNEL SOURCE '(' sourceName=STRING ')' sourceOpts=sparkOptions
        (TRANSFORM EQ transfromSql=STRING)?
        SINK '(' sinkName=STRING ')' (sinkOpts=sparkOptions)?              #dtunnelExpr
    ;

sparkOptions
    : OPTIONS '(' optionVal (',' optionVal)* ')'
    ;

optionVal
    : optionKey EQ optionValue
    ;

optionKey
    : identifier ('.' identifier)*
    | STRING
    ;

optionValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | STRING
    | '[' optionValue (',' optionValue)* ']'
    ;

booleanValue
    : TRUE | FALSE
    ;

identifier
    : IDENTIFIER    #unquotedIdentifier
    | STRING        #quotedIdentifierAlternative
    ;

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

ALL: 'ALL';
TRUE: 'TRUE';
FALSE: 'FALSE';
DATATUNNEL: 'DATATUNNEL';
SOURCE: 'SOURCE';
SINK: 'SINK';
TRANSFORM: 'TRANSFORM';
OPTIONS: 'OPTIONS';

EQ  : '=' | '==';

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D'
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-zA-Z]
    | ~[\u0000-\u007F\uD800-\uDBFF] // covers all characters above 0x7F which are not a surrogate
    | [\uD800-\uDBFF] [\uDC00-\uDFFF] // covers UTF-16 surrogate pairs encodings for U+10000 to U+10FFFF
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_EMPTY_COMMENT
    : '/**/' -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' ~[+] .*? '*/' -> channel(HIDDEN)
    ;

WS  : [ \r\n\t] + -> skip ;
