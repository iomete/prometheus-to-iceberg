from prometheus_to_iceberg.templating import resolve_variables, substitute


class TestResolveVariables:
    def test_none_input(self):
        assert resolve_variables(None) == {}

    def test_empty_dict(self):
        assert resolve_variables({}) == {}

    def test_scalar_string(self):
        result = resolve_variables({"cluster": "us-east-1"})
        assert result == {"cluster": "us-east-1"}

    def test_scalar_number(self):
        result = resolve_variables({"port": 9090})
        assert result == {"port": "9090"}

    def test_list_values_joined_with_pipe(self):
        result = resolve_variables({"namespace": ["kube-system", "monitoring", "default"]})
        assert result == {"namespace": "kube-system|monitoring|default"}

    def test_single_item_list(self):
        result = resolve_variables({"namespace": ["kube-system"]})
        assert result == {"namespace": "kube-system"}

    def test_mixed_types(self):
        result = resolve_variables({
            "cluster": "us-east-1",
            "namespace": ["kube-system", "monitoring"],
        })
        assert result == {
            "cluster": "us-east-1",
            "namespace": "kube-system|monitoring",
        }


class TestSubstitute:
    def test_no_variables(self):
        query = 'rate(foo[5m])'
        assert substitute(query, {}) == query

    def test_no_placeholders_in_query(self):
        query = 'rate(foo[5m])'
        assert substitute(query, {"cluster": "us-east-1"}) == query

    def test_scalar_substitution(self):
        query = 'foo{cluster="$cluster"}'
        result = substitute(query, {"cluster": "us-east-1"})
        assert result == 'foo{cluster="us-east-1"}'

    def test_list_substitution(self):
        query = 'foo{namespace=~"$namespace"}'
        result = substitute(query, {"namespace": "kube-system|monitoring"})
        assert result == 'foo{namespace=~"kube-system|monitoring"}'

    def test_multiple_variables(self):
        query = 'foo{cluster="$cluster", namespace=~"$namespace"}'
        variables = {"cluster": "us-east-1", "namespace": "kube-system|monitoring"}
        result = substitute(query, variables)
        assert result == 'foo{cluster="us-east-1", namespace=~"kube-system|monitoring"}'

    def test_longest_first_prevents_partial_match(self):
        query = 'foo{namespace=~"$namespace", ns="$ns"}'
        variables = {"ns": "default", "namespace": "kube-system|monitoring"}
        result = substitute(query, variables)
        assert result == 'foo{namespace=~"kube-system|monitoring", ns="default"}'

    def test_variable_used_multiple_times(self):
        query = 'foo{cluster="$cluster"} / bar{cluster="$cluster"}'
        result = substitute(query, {"cluster": "us-east-1"})
        assert result == 'foo{cluster="us-east-1"} / bar{cluster="us-east-1"}'
