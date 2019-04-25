{{# prefixes }}
{{{ prefix }}}
{{/ prefixes }}

SELECT ?v0 ?v1 ?v3
WHERE {
    ?v0 {{ e0 }} ?v1 .
    ?v2 {{ e2 }} ?v3 .
    ?v0 {{ e1 }} ?v3 .
}