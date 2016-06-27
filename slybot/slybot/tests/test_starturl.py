from unittest import TestCase
from slybot.starturls import StartUrlCollection, StartUrls, UrlGenerator, GeneratedUrl


class StartUrlCollectionTest(TestCase):
    def setUp(self):
        self.generators = {
            'start_urls': StartUrls(),
            'generated_urls': UrlGenerator(),
            'url': StartUrls(),
            'generated': GeneratedUrl(),
        }

    def test_mixed_start_urls_generation(self):
        start_urls = [
            'http://google.com',
            {"type": "url", "url": "http://domain.com"},
            {
                'type': 'generated',
                'url': 'https://github.com/[0-2]',
                'fragments': [
                    {'type': 'fixed', 'value': 'https://github.com/'},
                    {'type': 'range', 'value': '0-2'},
                ]
            }
        ]
        generated_start_urls = [
            'http://google.com',
            'http://domain.com',
            'https://github.com/0',
            'https://github.com/1',
            'https://github.com/2',
        ]

        generated = StartUrlCollection(start_urls, self.generators, 'start_urls')
        self.assertEqual(list(generated), generated_start_urls)

    def test_generated_type(self):
        generated_start_urls = [
            'https://github.com/scrapinghub',
            'https://github.com/scrapy',
            'https://github.com/scrapy-plugins',
        ]
        start_urls = [
            {
                "template": "https://github.com/{}",
                "paths": [{
                    "type": "options",
                    "values": ["scrapinghub", "scrapy", "scrapy-plugins"],
                }],
                "params": [],
                "params_template": {}
            },
        ]
        generated = StartUrlCollection(start_urls, self.generators, 'generated_urls')

        self.assertEqual(list(generated), generated_start_urls)

    def test_unique_legacy_urls(self):
        start_urls = [
            'http://google.com',
            'http://github.com',
            'http://github.com',
            'http://scrapinghub.com',
            'http://scrapinghub.com',
        ]
        unique_urls = [
            'http://google.com',
            'http://github.com',
            'http://scrapinghub.com',
        ]

        self.assertEqual(StartUrlCollection(start_urls).uniq(), unique_urls)

    def test_unique_list_start_urls(self):
        start_urls = [
            {"type": "url", "url": "http://domain.com"},
            {
                'type': 'generated',
                'url': 'https://github.com/[...]',
                'fragments': [
                    {'type': 'fixed', 'value': 'https://github.com/'},
                    {'type': 'list', 'value': 'scrapely portia'},
                ]
            },
            {
                'type': 'generated',
                'url': 'https://github.com/[...]',
                'fragments': [
                    {'type': 'fixed', 'value': 'https://github.com/'},
                    {'type': 'list', 'value': 'scrapely scrapinghub portia'},
                ]
            },
        ]

        self.assertEqual(StartUrlCollection(start_urls).uniq(), start_urls)
