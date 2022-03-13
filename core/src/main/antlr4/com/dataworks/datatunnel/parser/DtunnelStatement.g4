grammar DtunnelStatement;

singleStatement
    : statement EOF
    ;

// If you add keywords here that should not be reserved, add them to 'nonReserved' list.
statement
    : dtunnelStatement                                                     #dtunnelCommand
    | .*?                                                                  #passThrough
    ;

dtunnelStatement
    : DTUNNEL READER '(' srcName=STRING ')' readOpts=sparkOptions
        WRITER '(' distName=STRING ')' writeOpts=sparkOptions              #dtunnelExpr
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
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

ALL: 'ALL';
TRUE: 'TRUE';
FALSE: 'FALSE';
DTUNNEL: 'DTUNNEL';
READER: 'READER';
WRITER: 'WRITER';
OPTIONS: 'OPTIONS';

EQ  : '=' | '==';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

WS  : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;