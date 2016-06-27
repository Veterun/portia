import itertools
import six

from .generator import UrlGenerator
from .generated import GeneratedUrl
from collections import OrderedDict as ODict
from scrapy.utils.spider import arg_to_iter


class StartUrls():
    def __call__(self, spec):
        return spec

class StartUrlCollection(object):
    def __init__(self, start_urls, generators = None, generator_type = 'start_urls'):
        self.generators = generators
        self.generator_type = generator_type
        self.start_urls = map(self._url_type, start_urls)

    def __iter__(self):
        generated = (self._generate(start_url) for start_url in self.start_urls)
        for url in itertools.chain(*(arg_to_iter(g) for g in generated)):
            yield url

    def uniq(self):
        return list(ODict([(s.key, s.spec) for s in self.start_urls]).values())

    def _generate(self, start_url):
        generator = self.generators[start_url.generator_type]
        return generator(start_url.generator_value)

    def _url_type(self, start_url):
        if isinstance(start_url, six.string_types) or self._is_legacy(start_url):
            return LegacyUrl(start_url, self.generator_type)
        return StartUrl(start_url)

    def _is_legacy(self, start_url):
        return not (start_url.get('url') and start_url.get('type'))

class StartUrl(object):
    def __init__(self, spec):
        self.spec = spec
        self.generator_type = spec['type']
        self.generator_value = self.spec if self._has_fragments else self.spec['url']

    @property
    def key(self):
        fragment_values = [fragment['value'] for fragment in self.spec.get('fragments', [])]
        return self.spec['url'] + ''.join(fragment_values)

    @property
    def _has_fragments(self):
        return self.spec.get('fragments')

class LegacyUrl(object):
    def __init__(self, spec, generator_type):
        self.key = spec
        self.spec = spec
        self.generator_type = generator_type
        self.generator_value = spec
