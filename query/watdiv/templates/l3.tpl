{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v1
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v0 {{ e1 }} {{{ e1_obj }}} .
}