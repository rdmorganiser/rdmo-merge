#!/usr/bin/env python3
import json
import argparse

from collections import defaultdict, Counter

import logging


def main():

    parser = argparse.ArgumentParser(description='Merge two RDMO data dumps by rewriting the secondary fixtures into an output dump.')
    parser.add_argument('primary_fixtures', help='The path to the primary data dump.')
    parser.add_argument('secondary_fixtures', help='The path to the secondary data dump.')
    parser.add_argument('output_fixtures', help='The path to the output data dump.')
    parser.add_argument('--log-level', default='WARNING', help='Log level to be used, e.g. INFO')
    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.log_level.upper())

    processor = Processor(args)
    processor.read()
    processor.process()
    processor.write()


class Processor():

    def __init__(self, args):
        self.args = args
        self.primary = defaultdict(list)
        self.secondary = defaultdict(list)
        self.map = defaultdict(dict)
        self.primary_keys = {}
        self.output = defaultdict(list)

    def read(self):
        with open(self.args.primary_fixtures) as fp:
            for item in json.load(fp):
                self.primary[item['model']].append(item)

        with open(self.args.secondary_fixtures) as fp:
            for item in json.load(fp):
                self.secondary[item['model']].append(item)

    def write(self):
        output_list = []
        for model, items in self.output.items():
            output_list += items

        with open(self.args.output_fixtures, 'w') as fp:
            json.dump(output_list, fp, indent=2)

    def process(self):
        self.init_primary_keys()

        self.match_instances('auth.user', 'email', ignore_keys=[
            'password', 'groups', 'user_permissions', 'last_login', 'date_joined'
        ])

        self.match_instances('domain.attribute', 'uri', ignore_keys=['parent', 'lft', 'rght', 'tree_id', 'level'])
        self.match_instances('conditions.condition', 'uri', ignore_keys=['source', 'target_option'])
        self.match_instances('options.optionset', 'uri')
        self.match_instances('options.option', 'uri', ignore_keys=['optionset'])
        self.match_instances('questions.catalog', 'uri', ignore_keys=['created', 'updated'])
        self.match_instances('questions.section', 'uri', ignore_keys=['catalog', 'created', 'updated'])
        self.match_instances('questions.questionset', 'uri', ignore_keys=[
            'section', 'questionset', 'attribute', 'conditions', 'created', 'updated'])
        self.match_instances('questions.question', 'uri', ignore_keys=[
            'questionset', 'attribute', 'default_option', 'conditions', 'optionsets', 'created', 'updated']
        )
        self.match_instances('tasks.task', 'uri')
        self.match_instances('views.view', 'uri')

        self.update_instances('projects.project')
        self.update_instances('projects.snapshot')
        self.update_instances('projects.value')
        self.update_instances('projects.membership')
        self.update_instances('projects.issue')

        self.fix_relations('domain.attribute', foreign_keys={
            'parent': 'domain.attribute'
        })
        self.fix_relations('conditions.condition', foreign_keys={
            'source': 'domain.attribute',
            'target_option': 'options.option'
        })
        self.fix_relations('options.optionset', many_to_many={
            'conditions': 'conditions.condition'
        })
        self.fix_relations('options.option', foreign_keys={
            'optionset': 'options.optionset'
        })
        self.fix_relations('questions.catalog')
        self.fix_relations('questions.section', foreign_keys={
            'catalog': 'questions.catalog'
        })
        self.fix_relations('questions.questionset', foreign_keys={
            'section': 'questions.section',
            # 'questionset': 'questions.questionset',
            'attribute': 'domain.attribute'
        }, many_to_many={
            'conditions': 'conditions.condition'
        })
        self.fix_relations('questions.question', foreign_keys={
            'questionset': 'questions.questionset',
            'attribute': 'domain.attribute',
            # 'default_option': 'options.optionset'
        }, many_to_many={
            'conditions': 'conditions.condition',
            'optionsets': 'options.optionset'
        })

        self.fix_relations('projects.project', foreign_keys={
            'parent': 'projects.project',
            'catalog': 'questions.catalog'
        }, many_to_many={
            # 'tasks': 'tasks.task',
            'views': 'views.view'
        })
        self.fix_relations('projects.snapshot', foreign_keys={
            'project': 'projects.project'
        })
        self.fix_relations('projects.value', foreign_keys={
            'project': 'projects.project',
            'snapshot': 'projects.snapshot',
            'attribute': 'domain.attribute',
            'option': 'options.option'
        })
        self.fix_relations('projects.membership', foreign_keys={
            'project': 'projects.project',
            'user': 'auth.user'
        })
        self.fix_relations('projects.issue', foreign_keys={
            'project': 'projects.project',
            'task': 'tasks.task'
        })

    def update_instances(self, model):
        for secondary_element in self.secondary[model]:
            old_pk = secondary_element['pk']
            new_pk = self.get_primary_key(model)
            logging.info('create %s %s -> %s', model, old_pk, new_pk)
            self.map[model][old_pk] = new_pk
            self.output[model].append({
                'model': model,
                'pk': new_pk,
                'fields': secondary_element['fields']
            })

    def match_instances(self, model, match_field, ignore_keys=[]):
        for secondary_element in self.secondary[model]:
            match_value = secondary_element['fields'][match_field]

            primary_element = self.match(model, match_field, match_value)
            if primary_element is None:
                old_pk = secondary_element['pk']
                new_pk = self.get_primary_key(model)
                logging.info('create %s %s -> %s %s', model, old_pk, new_pk, match_value)
                self.map[model][old_pk] = new_pk
                self.output[model].append({
                    'model': model,
                    'pk': new_pk,
                    'fields': secondary_element['fields']
                })
            else:
                self.check_element(primary_element, secondary_element, ignore_keys=ignore_keys)

                old_pk = secondary_element['pk']
                new_pk = primary_element['pk']
                logging.info('found %s %s -> %s %s', model, old_pk, new_pk, match_value)
                self.map[model][old_pk] = new_pk

    def fix_relations(self, model, foreign_keys={}, many_to_many={}):
        for output_element in self.output[model]:
            for fk_field, fk_model in foreign_keys.items():
                old_fk = output_element['fields'][fk_field]
                if old_fk is not None:
                    new_fk = self.map[fk_model][old_fk]
                    if old_fk != new_fk:
                        logging.info('fix %s:%s %s -> %s', model, fk_field, old_fk, new_fk)
                        output_element['fields'][fk_field] = new_fk

            for m2m_field, m2m_model in many_to_many.items():
                for i, old_m2mk in enumerate(output_element['fields'][m2m_field]):
                    new_m2mk = self.map[m2m_model][old_m2mk]
                    if old_m2mk != new_m2mk:
                        logging.info('fix %s:%s %s -> %s', model, m2m_field, old_m2mk, new_m2mk)
                        output_element['fields'][m2m_field][i] = new_m2mk

    def match(self, model, field, value):
        matches = list(filter(lambda item: item['fields'][field] == value, self.primary[model]))
        if len(matches) > 1:
            raise RuntimeError('More than one match for {}'.format(value))
        elif len(matches) == 1:
            return matches[0]
        else:
            return None

    def check_element(self, primary_element, secondary_element, ignore_keys=[]):
        model = primary_element['model']
        for key, secondary_value in secondary_element['fields'].items():
            if key not in ignore_keys:
                primary_value = primary_element['fields'][key]
                if secondary_value != primary_value:
                    logging.warning('%s:%s doesn\'t match (%s != %s)', model, key, secondary_value, primary_value)

    def filter_fields(self, element, ignore_keys=[]):
        return {key: value for key, value in element['fields'].items() if key not in ignore_keys}

    def init_primary_keys(self):
        self.primary_keys = Counter({
            model: max(self.primary[model], key=lambda element: int(element['pk']))['pk'] for model in [
                'conditions.condition', 'projects.snapshot', 'options.optionset', 'options.option',
                'projects.membership', 'domain.attribute', 'questions.questionset', 'questions.question',
                'questions.section', 'projects.value', 'auth.user', 'projects.project',
                'views.view', 'questions.catalog'
            ]
        })

    def get_primary_key(self, model):
        self.primary_keys[model] += 1
        return self.primary_keys[model]


if __name__ == "__main__":
    main()
