{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v1 ?v2
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v0 {{ e1 }} ?v2 .
    {{{ e2_subj }}} {{ e2 }} ?v0 .
}