from doit import create_after, get_var

DOIT_CONFIG = {
    'default_tasks': ['test']
}


def task_test():
    """Run unit tests"""
    return {
        'actions': ['pytest -q'],
        'verbosity': 2,
    }


def task_integration():
    """Run integration tests with docker-compose"""
    return {
        'actions': [
            'docker-compose up -d',
            {
                'cmd': 'pytest -q',
                'env': {'NOCO_TEST_ONLINE': '1'}
            },
            'docker-compose down'
        ],
        'verbosity': 2,
    }
