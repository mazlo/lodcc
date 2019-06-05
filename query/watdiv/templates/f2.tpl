{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6 ?v7
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v0 {{ e1 }} ?v2 .
    ?v0 {{ e2 }} ?v3 .
    ?v0 {{ e3 }} ?v4 .
    ?v0 {{ e4 }} ?v5 .
    ?v1 {{ e5 }} ?v6 .
    ?v1 {{ e6 }} ?v7 .
    ?v0 {{ e7 }} {{{ e7_obj }}} .
}