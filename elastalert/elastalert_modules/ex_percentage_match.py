import dateutil.parser

from elastalert.ruletypes import BaseAggregationRule

from elastalert.util import EAException

class ExPercentageMatchRule(BaseAggregationRule):
    required_options = frozenset(['total_filter', 'target_filter'])

    def __init__(self, *args):
        super(ExPercentageMatchRule, self).__init__(*args)
        self.ts_field = self.rules.get('timestamp_field', '@timestamp')
        if 'max_percentage' not in self.rules and 'min_percentage' not in self.rules:
            raise EAException("ExPercentageMatchRule must have at least one of either min_percentage or max_percentage")

        self.min_denominator = self.rules.get('min_denominator', 0)
        self.total_filter = self.rules['total_filter']
        self.target_filter = self.rules['target_filter']
        self.auto_buffer_time = self.rules.get('auto_buffer_time', False)
        self.origin_buffer_time = self.rules['buffer_time']
        self.rules['aggregation_query_element'] = self.generate_aggregation_query()

    def get_match_str(self, match):
        percentage_format_string = self.rules.get('percentage_format_string', None)
        message = 'Percentage violation, value: %s (min: %s max : %s) of %s items\n\n' % (
            percentage_format_string % (match['percentage']) if percentage_format_string else match['percentage'],
            self.rules.get('min_percentage'),
            self.rules.get('max_percentage'),
            match['denominator']
        )
        return message

    def generate_aggregation_query(self):
        return {
            'percentage_match_aggs': {
                'filters': {
                    'filters': {
                        'total_match': {
                            'bool': {
                                'must': self.total_filter
                            }
                        },
                        'target_match': {
                            'bool': {
                                'must': self.target_filter
                            }
                        }
                    }
                }
            }
        }

    def check_matches(self, timestamp, query_key, aggregation_data):
        total_match_count = aggregation_data['percentage_match_aggs']['buckets']['total_match']['doc_count']
        target_match_count = aggregation_data['percentage_match_aggs']['buckets']['target_match']['doc_count']

        if total_match_count is None or target_match_count is None:
            return
        else:
            if total_match_count == 0 or total_match_count < self.min_denominator:
                if self.auto_buffer_time:
                    self.rules['buffer_time'] = self.rules['buffer_time'] + self.origin_buffer_time
                return
            else:
                match_percentage = (target_match_count * 1.0) / (total_match_count * 1.0) * 100
                if self.percentage_violation(match_percentage):
                    match = {self.rules['timestamp_field']: timestamp, 'percentage': match_percentage, 'denominator': total_match_count, 'buffer_time': int(self.rules['buffer_time'].seconds/60)}
                    if query_key is not None:
                        match[self.rules['query_key']] = query_key
                    self.add_match(match)
                if self.auto_buffer_time:
                    self.rules['buffer_time'] = self.origin_buffer_time

    def percentage_violation(self, match_percentage):
        if 'max_percentage' in self.rules and match_percentage > self.rules['max_percentage']:
            return True
        if 'min_percentage' in self.rules and match_percentage < self.rules['min_percentage']:
            return True
        return False
