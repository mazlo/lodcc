{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v0 {{ e1 }} ?v2 .
    ?v0 {{ e2 }} {{{ e2_obj }}} .
    ?v4 {{ e4 }} ?v5 .
    ?v5 {{ e5 }} ?v6 .
    ?v5 {{ e3 }} ?v0 .
}