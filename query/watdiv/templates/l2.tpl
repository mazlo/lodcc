{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v1 ?v2
WHERE {
    {{{ e2_subj }}} {{ e2 }} ?v1 .
    ?v2 {{ e0 }} {{{ e0_obj }}} .
    ?v2 {{ e1 }} ?v1 .
}