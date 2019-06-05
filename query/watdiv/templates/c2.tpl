{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v3 ?v4 ?v8
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v0 {{ e1 }} ?v2 .
    ?v2 {{ e3 }} {{{ e3_obj }}} .
    ?v2 {{ e2 }} ?v3 .
    ?v4 {{ e8 }} ?v10 .
    ?v4 {{ e9 }} ?v6 .
    ?v4 {{ e7 }} ?v7 .
    ?v7 {{ e6 }} ?v3 .
    ?v3 {{ e4 }} ?v8 .
    ?v8 {{ e5 }} ?v9 .
}