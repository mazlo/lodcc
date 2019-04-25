{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v1 ?v3 ?v4 ?v5 ?v6 ?v7 ?v8 ?v9
WHERE {
    ?v0 {{ e0 }} ?v1 .
    {{{ e1_subj }}} {{ e1 }} ?v0 .
    ?v0 {{ e2 }} ?v3 .
    ?v0 {{ e3 }} ?v4 .
    ?v0 {{ e4 }} ?v5 .
    ?v0 {{ e5 }} ?v6 .
    ?v0 {{ e6 }} ?v7 .
    ?v0 {{ e7 }} ?v8 .
    ?v0 {{ e8 }} ?v9 .
}