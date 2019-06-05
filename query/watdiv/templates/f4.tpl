{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6 ?v7 ?v8
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v2 {{ e1 }} ?v0 .
    ?v0 {{ e2 }} {{{ e2_obj }}} .
    ?v0 {{ e3 }} ?v4 .
    ?v0 {{ e4 }} ?v8 .
    ?v1 {{ e5 }} ?v5 .
    ?v1 {{ e6 }} ?v6 .
    ?v1 {{ e8 }} {{{ e8_obj }}} .
    ?v7 {{ e7 }} ?v1 .
}