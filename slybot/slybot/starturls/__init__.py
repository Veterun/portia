from collections import OrderedDict as ODict
from itertools import chain, product

from scrapy.utils.spider import arg_to_iter

import six
from six.moves.urllib_parse import urlparse

from .generated import GeneratedUrl
from .generator import UrlGenerator


class StartUrls():
    def __call__(self, spec):
        return spec


class StartUrlCollection(object):
    def __init__(self, start_urls, generators=None, generator_type='start_urls'):
        self.generators = generators
        self.generator_type = generator_type
        self.start_urls = map(self._url_type, start_urls)

    def __iter__(self):
        generated = (self._generate_urls(url) for url in self.start_urls)
        for url in chain(*(arg_to_iter(g) for g in generated)):
            yield url

    def uniq(self):
        return list(ODict([(s.key, s.spec) for s in self.start_urls]).values())

    @property
    def allowed_domains(self):
        domains = [start_url.allowed_domains for start_url in self.start_urls]
        return list(reduce(set.union, domains, set()))

    def _generate_urls(self, start_url):
        generator = self.generators[start_url.generator_type]
        return generator(start_url.generator_value)

    def _url_type(self, start_url):
        if self._is_legacy(start_url):
            return LegacyUrl(start_url, self.generator_type)
        return StartUrl(start_url, self.generators)

    def _is_legacy(self, start_url):
        return (isinstance(start_url, six.string_types) or
                not (start_url.get('url') and start_url.get('type')))


class StartUrl(object):
    def __init__(self, spec, generators):
        self.spec = spec
        self.generators = generators
        self.generator_type = spec['type']
        self.generator_value = self.spec if self._has_fragments else self.spec['url']

    @property
    def allowed_domains(self):
        if self._has_fragments:
            return self._find_fragment_domains()
        return set([self.spec['url']])

    def _find_fragment_domains(self):
        generator = self.generators[self.generator_type]
        fragments = generator.process_fragments(self.spec)

        while len(fragments) > 0:
            fragment = fragments.pop(0)
            if all(self._has_domain(url) for url in fragment):
                return set(fragment)
            if len(fragments) == 0:
                return set()
            fragments[0] = self._join_fragments(product(fragment, fragments[0]))
        return set()

    def _join_fragments(self, fragments):
        return map(lambda (a, b): ''.join([a, b]), fragments)

    def _has_domain(self, url):
        methods = ['path', 'params', 'query', 'fragment']
        return any(getattr(urlparse(url), method) != '' for method in methods)

    @property
    def key(self):
        fragments = self.spec.get('fragments', [])
        fragment_values = [fragment['value'] for fragment in fragments]
        return self.spec['url'] + ''.join(fragment_values)

    @property
    def _has_fragments(self):
        return self.spec.get('fragments')


class LegacyUrl(object):
    def __init__(self, spec, generator_type):
        self.key = spec
        self.spec = spec
        self.generator_value = spec
        self.generator_type = generator_type

    @property
    def allowed_domains(self):
        is_generated = self.generator_type == 'generated_urls'
        url = self.spec['template'] if is_generated else self.spec
        return set([url])
