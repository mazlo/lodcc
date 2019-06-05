{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v2 ?v3
WHERE {
    ?v0 {{ e0 }} {{{ e0_obj }}} .
    ?v0 {{ e1 }} ?v2 .
    ?v3 {{ e2 }} ?v0 .
    ?v0 {{ e3 }} ?v4 .
}