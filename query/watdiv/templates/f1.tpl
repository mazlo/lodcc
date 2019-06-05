{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v2 ?v3 ?v4 ?v5
WHERE {
    ?v0 {{ e0 }} {{{ e0_obj }}} .
    ?v0 {{ e1 }} ?v2 .
    ?v3 {{ e3 }} ?v4 .
    ?v3 {{ e5 }} ?v5 .
    ?v3 {{ e2 }} ?v0 .
    ?v3 {{ e4 }} {{{ e4_obj }}} .
}