{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v2
WHERE {
    ?v0 {{ e0 }} {{{ e0_obj }}} .
    ?v0 {{ e1 }} ?v2 .
}