{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v4 ?v6 ?v7
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v0 {{ e1 }} ?v2 .
    ?v0 {{ e2 }} ?v3 .
    ?v0 {{ e3 }} ?v4 .
    ?v4 {{ e4 }} ?v5 .
    ?v4 {{ e5 }} ?v6 .
    ?v7 {{ e6 }} ?v6 .
    ?v7 {{ e7 }} ?v8 .
}