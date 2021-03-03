# Map "pilotType" (defined in harvester) to prodSourceLabel and pilotType option (defined in pilot, -i option)
# and piloturl (pilot option --piloturl) for pilot 2
def get_complicated_pilot_options(pilot_type, pilot_url=None):
    pt_psl_map = {
            'RC': {
                    'prod_source_label': 'rc_test2',
                    'pilot_type_opt': 'RC',
                    'pilot_url_str': '--piloturl http://cern.ch/atlas-panda-pilot/pilot2-dev.tar.gz',
                },
            'ALRB': {
                    'prod_source_label': 'rc_alrb',
                    'pilot_type_opt': 'ALRB',
                    'pilot_url_str': '',
                },
            'PT': {
                    'prod_source_label': 'ptest',
                    'pilot_type_opt': 'PR',
                    'pilot_url_str': '--piloturl http://cern.ch/atlas-panda-pilot/pilot2-dev2.tar.gz',
                },
        }
    pilot_opt_dict = pt_psl_map.get(pilot_type, None)
    if pilot_url and pilot_opt_dict:
        pilot_opt_dict['pilot_url_str'] = '--piloturl {0}'.format(pilot_url)
    return pilot_opt_dict


# get special flag of pilot wrapper about python version of pilot, and whether to run with python 3 if python version is "3"
# FIXME: during pilot testing phase, only prodsourcelabel ptest and rc_test2 should run python3
# This constraint will be removed when pilot is ready
def get_python_version_option(python_version, prod_source_label):
    option = ''
    if python_version.startswith('3'):
        if prod_source_label in ['rc_test2', 'ptest']:
            option = '--pythonversion 3'
    return option