{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v1 ?v3
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v0 {{ e1 }} {{{ e1_obj }}} .
    ?v0 {{ e2 }} ?v3 .
    ?v0 {{ e3 }} ?v4 .
}