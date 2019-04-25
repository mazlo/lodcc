{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v2 ?v3
WHERE {
    ?v0 {{ e0 }} {{{ e0_obj }}} .
    ?v0 {{ e1 }} ?v2 .
    ?v0 {{ e2 }} ?v3 .
    ?v0 {{ e3 }} {{{ e3_obj }}} .
}