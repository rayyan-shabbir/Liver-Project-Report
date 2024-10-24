from abc import ABC, abstractmethod
from pymongo import MongoClient

class PreprocessedLabs(ABC):
    # Class variable for MongoDB connection
    client = MongoClient('mongodb://localhost:27017/')
    db = client['your_database_name']

    # Private base pipeline
    __base_pipeline = [
        {
            '$limit': 400000
        }, {
            '$match': {
                'lab_results.api_test_name': {
                    '$in': []  # This will be dynamically set in the child classes
                }
            }
        }, {
            '$set': {
                'lab_results': {
                    '$map': {
                        'input': '$lab_results', 
                        'as': 'lab', 
                        'in': {
                            '$cond': {
                                'if': {
                                    '$in': [
                                        '$$lab.api_test_name', []  # This will be dynamically set in the child classes
                                    ]
                                }, 
                                'then': {
                                    '$mergeObjects': [
                                        '$$lab', {
                                            'api_test_name': ''  # This will be dynamically set in the child classes
                                        }
                                    ]
                                }, 
                                'else': '$$lab'
                            }
                        }
                    }
                }
            }
        }, {
            '$project': {
                'PatientID': 1, 
                'Practice': 1, 
                'demographics': 1, 
                'vitals': 1, 
                'medications': 1, 
                'diagnosis': 1, 
                'Active_Meds': 1, 
                'lab_results': {
                    '$filter': {
                        'input': '$lab_results', 
                        'as': 'lab', 
                        'cond': {
                            '$eq': [
                                '$$lab.api_test_name', ''  # This will be dynamically set in the child classes
                            ]
                        }
                    }
                }
            }
        }, {
            '$project': {
                'PatientID': 1, 
                'Practice': 1, 
                'demographics': 1, 
                'vitals': 1, 
                'medications': 1, 
                'diagnosis': 1, 
                'Active_Meds': 1, 
                'lab_results': {
                    '$sortArray': {
                        'input': '$lab_results', 
                        'sortBy': {
                            'date': -1
                        }
                    }
                }
            }
        }, {
            '$addFields': {
                'valid_labs': {
                    '$reduce': {
                        'input': '$lab_results', 
                        'initialValue': {
                            'valid_labs': [], 
                            'last_lab': None
                        }, 
                        'in': {
                            '$cond': {
                                'if': {
                                    '$and': [
                                        {
                                            '$ne': [
                                                '$$value.last_lab', None
                                            ]
                                        }, {
                                            '$gte': [
                                                {
                                                    '$dateDiff': {
                                                        'startDate': '$$this.date', 
                                                        'endDate': '$$value.last_lab.date', 
                                                        'unit': 'day'
                                                    }
                                                }, 80
                                            ]
                                        }, {
                                            '$lt': [
                                                {
                                                    '$dateDiff': {
                                                        'startDate': '$$this.date', 
                                                        'endDate': '$$value.last_lab.date', 
                                                        'unit': 'day'
                                                    }
                                                }, 101
                                            ]
                                        }
                                    ]
                                }, 
                                'then': {
                                    'valid_labs': {
                                        '$concatArrays': [
                                            [
                                                '$$value.last_lab'
                                            ], [
                                                '$$this'
                                            ]
                                        ]
                                    }, 
                                    'last_lab': '$$this'
                                }, 
                                'else': {
                                    'valid_labs': '$$value.valid_labs', 
                                    'last_lab': '$$this'
                                }
                            }
                        }
                    }
                }
            }
        }, {
            '$project': {
                'PatientID': 1, 
                'Practice': 1, 
                'demographics': 1, 
                'vitals': 1, 
                'medications': 1, 
                'diagnosis': 1, 
                'Active_Meds': 1, 
                'valid_labs': '$valid_labs.valid_labs'
            }
        }, {
            '$match': {
                'valid_labs': {
                    '$size': 2
                }
            }
        }, {
            '$project': {
                'PatientID': 1, 
                'Practice': 1, 
                'lab_after': {
                    '$arrayElemAt': [
                        '$valid_labs', 0
                    ]
                }, 
                'lab_before': {
                    '$arrayElemAt': [
                        '$valid_labs', 1
                    ]
                }
            }
        }, {
            '$project': {
                'PatientID': 1, 
                'Practice': 1, 
                'lab_after': {
                    'date': '$lab_after.date', 
                    'result': '$lab_after.result', 
                    'unit': '$lab_after.unit', 
                    'range': '$lab_after.range', 
                    'api_test_name': '$lab_after.api_test_name'
                }, 
                'lab_before': {
                    'date': '$lab_before.date', 
                    'result': '$lab_before.result', 
                    'unit': '$lab_before.unit', 
                    'range': '$lab_before.range', 
                    'api_test_name': '$lab_before.api_test_name'
                }
            }
        }
    ]

    @classmethod
    def get_base_pipeline(cls, api_test_name, api_test_names, new_api_test_name):
        pipeline = cls.__base_pipeline.copy()
        pipeline[1]['$match']['lab_results.api_test_name']['$in'] = api_test_names
        pipeline[2]['$set']['lab_results']['$map']['in']['$cond']['if']['$in'][1] = api_test_names
        pipeline[2]['$set']['lab_results']['$map']['in']['$cond']['then']['$mergeObjects'][1]['api_test_name'] = new_api_test_name
        pipeline[3]['$project']['lab_results']['$filter']['cond']['$eq'][1] = new_api_test_name
        return pipeline

    @abstractmethod
    def run_aggregator_labs(self):
        pass

    @abstractmethod
    def run_aggregator_demo(self):
        pass

    @abstractmethod
    def run_aggregator_diagnosis(self):
        pass

    @abstractmethod
    def run_aggregator_vitals(self):
        pass

    @abstractmethod
    def run_aggregator_medications(self):
        pass

class ALTLab(PreprocessedLabs):
    def __init__(self):
        self.base_pipeline = self.get_base_pipeline(
            api_test_name='Alanine aminotransferase (ALT) measurement',
            api_test_names=['Alanine aminotransferase (ALT) measurement'],
            new_api_test_name='alanine_aminotransferase'
        )

    def run_aggregator_labs(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_after': {
                        'date': '$lab_after.date', 
                        'result': '$lab_after.result', 
                        'unit': '$lab_after.unit', 
                        'range': '$lab_after.range', 
                        'api_test_name': '$lab_after.api_test_name'
                    }, 
                    'lab_before': {
                        'date': '$lab_before.date', 
                        'result': '$lab_before.result', 
                        'unit': '$lab_before.unit', 
                        'range': '$lab_before.range', 
                        'api_test_name': '$lab_before.api_test_name'
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'labs')

    def run_aggregator_diagnosis(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'diagnosis': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': {
                        'lab_after': '$lab_after', 
                        'lab_before': '$lab_before'
                    }, 
                    'diagnosis': {
                        '$filter': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'cond': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$$diag.status', 'Active'
                                        ]
                                    }, {
                                        '$ne': [
                                            '$$diag.date', None
                                        ]
                                    }, {
                                        '$eq': [
                                            {
                                                '$type': '$$diag.date'
                                            }, 'date'
                                        ]
                                    }, {
                                        '$lt': [
                                            '$$diag.date', '$lab_before.date'
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    '_id': 1, 
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'diagnosis': {
                        '$filter': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'cond': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$$diag.status', 'Active'
                                        ]
                                    }, {
                                        '$ne': [
                                            '$$diag.date', None
                                        ]
                                    }, {
                                        '$eq': [
                                            {
                                                '$type': '$$diag.date'
                                            }, 'date'
                                        ]
                                    }, {
                                        '$lt': [
                                            '$$diag.date', '$valid_labs.lab_before.date'
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$diag.date', {
                                                '$dateSubtract': {
                                                    'startDate': '$valid_labs.lab_before.date', 
                                                    'unit': 'year', 
                                                    'amount': 2
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'diagnosis': {
                        '$map': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'in': {
                                'date': '$$diag.date', 
                                'icd_10': '$$diag.icd_10'
                            }
                        }
                    }
                }
            }, {
                '$addFields': {
                    'diagnosis': {
                        '$map': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'in': {
                                'icd_10': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$$diag.icd_10', '.'
                                            ]
                                        }, 0
                                    ]
                                }, 
                                '_id': '$$diag._id'
                            }
                        }
                    }
                }
            }, {
                '$set': {
                    'diagnosis': {
                        '$reduce': {
                            'input': '$diagnosis', 
                            'initialValue': [], 
                            'in': {
                                '$let': {
                                    'vars': {
                                        'existingCodes': '$$value.icd_10'
                                    }, 
                                    'in': {
                                        '$cond': {
                                            'if': {
                                                '$in': [
                                                    '$$this.icd_10', '$$existingCodes'
                                                ]
                                            }, 
                                            'then': '$$value', 
                                            'else': {
                                                '$concatArrays': [
                                                    '$$value', [
                                                        '$$this'
                                                    ]
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'diagnosis')

    def run_aggregator_vitals(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'vitals': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_before': 1, 
                    'vitals': {
                        '$filter': {
                            'input': '$vitals', 
                            'as': 'vital', 
                            'cond': {
                                '$and': [
                                    {
                                        '$lt': [
                                            '$$vital.date', '$lab_before.date'
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$vital.date', {
                                                '$dateSubtract': {
                                                    'startDate': '$lab_before.date', 
                                                    'unit': 'day', 
                                                    'amount': 350
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'vitals')

    def run_aggregator_demo(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'demographics.date_of_birth': 1, 
                    'demographics.gender': 1, 
                    'demographics.race_mapping': 1, 
                    'demographics.ethnicity_mapping': 1
                }
            }, {
                '$addFields': {
                    'age': {
                        '$floor': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        datetime.utcnow(), {
                                            '$getField': {
                                                'field': 'date_of_birth', 
                                                'input': {
                                                    '$arrayElemAt': [
                                                        '$demographics', 0
                                                    ]
                                                }
                                            }
                                        }
                                    ]
                                }, 1000 * 60 * 60 * 24 * 365
                            ]
                        }
                    }
                }
            }, {
                '$set': {
                    'date_of_birth': {
                        '$getField': {
                            'field': 'date_of_birth', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'gender': {
                        '$getField': {
                            'field': 'gender', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'race_mapping': {
                        '$getField': {
                            'field': 'race_mapping', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'ethnicity_mapping': {
                        '$getField': {
                            'field': 'ethnicity_mapping', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }
                }
            }, {
                '$unset': 'demographics'
            }
        ]
        self.run_aggregator(pipeline, 'demo')

    def run_aggregator_medications(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$addFields': {
                    'medications': {
                        '$filter': {
                            'input': '$medications', 
                            'as': 'med', 
                            'cond': {
                                '$and': [
                                    {
                                        '$ne': [
                                            '$$med.date', None
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$med.date', {
                                                '$arrayElemAt': [
                                                    '$valid_labs.date', 1
                                                ]
                                            }
                                        ]
                                    }, {
                                        '$lte': [
                                            '$$med.date', {
                                                '$arrayElemAt': [
                                                    '$valid_labs.date', 0
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'medications': {
                        '$filter': {
                            'input': '$medications', 
                            'as': 'med', 
                            'cond': {
                                '$eq': [
                                    '$$med.status', 'Active'
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'medications': {
                        '$map': {
                            'input': '$medications', 
                            'as': 'med', 
                            'in': {
                                'date': '$$med.date', 
                                'gpi': '$$med.gpi', 
                                'status': '$$med.status', 
                                'action': '$$med.action', 
                                'end_date': '$$med.end_date', 
                                'days': '$$med.sig_parsed.days', 
                                'dose_per_day': {
                                    '$multiply': [
                                        '$$med.sig_parsed.dose_g', '$$med.sig_parsed.frequency'
                                    ]
                                }
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_lab_date': {
                        '$arrayElemAt': [
                            '$valid_labs.date', 0
                        ]
                    }, 
                    'medications': {
                        '$map': {
                            'input': '$medications', 
                            'as': 'med', 
                            'in': {
                                'date': '$$med.date', 
                                'gpi': '$$med.gpi', 
                                'status': '$$med.status', 
                                'action': '$$med.action', 
                                'end_date': '$$med.end_date', 
                                'days': '$$med.days', 
                                'dose_per_day': '$$med.dose_per_day', 
                                'dosage': {
                                    '$cond': [
                                        {
                                            '$gt': [
                                                {
                                                    '$add': [
                                                        '$$med.date', {
                                                            '$multiply': [
                                                                '$$med.days', 24 * 60 * 60 * 1000
                                                            ]
                                                        }
                                                    ]
                                                }, {
                                                    '$arrayElemAt': [
                                                        '$valid_labs.date', 0
                                                    ]
                                                }
                                            ]
                                        }, {
                                            '$multiply': [
                                                {
                                                    '$divide': [
                                                        {
                                                            '$subtract': [
                                                                {
                                                                    '$arrayElemAt': [
                                                                        '$valid_labs.date', 0
                                                                    ]
                                                                }, '$$med.date'
                                                            ]
                                                        }, 24 * 60 * 60 * 1000
                                                    ]
                                                }, '$$med.dose_per_day'
                                            ]
                                        }, {
                                            '$multiply': [
                                                '$$med.dose_per_day', '$$med.days'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'medications')

class ASTLab(PreprocessedLabs):
    def __init__(self):
        self.base_pipeline = self.get_base_pipeline(
            api_test_name='Aspartate aminotransferase (AST) measurement',
            api_test_names=['Aspartate aminotransferase (AST) measurement'],
            new_api_test_name='aspartate_aminotransferase'
        )

    def run_aggregator_labs(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_after': {
                        'date': '$lab_after.date', 
                        'result': '$lab_after.result', 
                        'unit': '$lab_after.unit', 
                        'range': '$lab_after.range', 
                        'api_test_name': '$lab_after.api_test_name'
                    }, 
                    'lab_before': {
                        'date': '$lab_before.date', 
                        'result': '$lab_before.result', 
                        'unit': '$lab_before.unit', 
                        'range': '$lab_before.range', 
                        'api_test_name': '$lab_before.api_test_name'
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'labs')

    def run_aggregator_diagnosis(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'diagnosis': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': {
                        'lab_after': '$lab_after', 
                        'lab_before': '$lab_before'
                    }, 
                    'diagnosis': {
                        '$filter': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'cond': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$$diag.status', 'Active'
                                        ]
                                    }, {
                                        '$ne': [
                                            '$$diag.date', None
                                        ]
                                    }, {
                                        '$eq': [
                                            {
                                                '$type': '$$diag.date'
                                            }, 'date'
                                        ]
                                    }, {
                                        '$lt': [
                                            '$$diag.date', '$lab_before.date'
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    '_id': 1, 
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'diagnosis': {
                        '$filter': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'cond': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$$diag.status', 'Active'
                                        ]
                                    }, {
                                        '$ne': [
                                            '$$diag.date', None
                                        ]
                                    }, {
                                        '$eq': [
                                            {
                                                '$type': '$$diag.date'
                                            }, 'date'
                                        ]
                                    }, {
                                        '$lt': [
                                            '$$diag.date', '$valid_labs.lab_before.date'
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$diag.date', {
                                                '$dateSubtract': {
                                                    'startDate': '$valid_labs.lab_before.date', 
                                                    'unit': 'year', 
                                                    'amount': 2
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'diagnosis': {
                        '$map': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'in': {
                                'date': '$$diag.date', 
                                'icd_10': '$$diag.icd_10'
                            }
                        }
                    }
                }
            }, {
                '$addFields': {
                    'diagnosis': {
                        '$map': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'in': {
                                'icd_10': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$$diag.icd_10', '.'
                                            ]
                                        }, 0
                                    ]
                                }, 
                                '_id': '$$diag._id'
                            }
                        }
                    }
                }
            }, {
                '$set': {
                    'diagnosis': {
                        '$reduce': {
                            'input': '$diagnosis', 
                            'initialValue': [], 
                            'in': {
                                '$let': {
                                    'vars': {
                                        'existingCodes': '$$value.icd_10'
                                    }, 
                                    'in': {
                                        '$cond': {
                                            'if': {
                                                '$in': [
                                                    '$$this.icd_10', '$$existingCodes'
                                                ]
                                            }, 
                                            'then': '$$value', 
                                            'else': {
                                                '$concatArrays': [
                                                    '$$value', [
                                                        '$$this'
                                                    ]
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'diagnosis')

    def run_aggregator_vitals(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'vitals': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_before': 1, 
                    'vitals': {
                        '$filter': {
                            'input': '$vitals', 
                            'as': 'vital', 
                            'cond': {
                                '$and': [
                                    {
                                        '$lt': [
                                            '$$vital.date', '$lab_before.date'
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$vital.date', {
                                                '$dateSubtract': {
                                                    'startDate': '$lab_before.date', 
                                                    'unit': 'day', 
                                                    'amount': 350
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'vitals')

    def run_aggregator_demo(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'demographics.date_of_birth': 1, 
                    'demographics.gender': 1, 
                    'demographics.race_mapping': 1, 
                    'demographics.ethnicity_mapping': 1
                }
            }, {
                '$addFields': {
                    'age': {
                        '$floor': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        datetime.utcnow(), {
                                            '$getField': {
                                                'field': 'date_of_birth', 
                                                'input': {
                                                    '$arrayElemAt': [
                                                        '$demographics', 0
                                                    ]
                                                }
                                            }
                                        }
                                    ]
                                }, 1000 * 60 * 60 * 24 * 365
                            ]
                        }
                    }
                }
            }, {
                '$set': {
                    'date_of_birth': {
                        '$getField': {
                            'field': 'date_of_birth', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'gender': {
                        '$getField': {
                            'field': 'gender', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'race_mapping': {
                        '$getField': {
                            'field': 'race_mapping', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'ethnicity_mapping': {
                        '$getField': {
                            'field': 'ethnicity_mapping', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }
                }
            }, {
                '$unset': 'demographics'
            }
        ]
        self.run_aggregator(pipeline, 'demo')

    def run_aggregator_medications(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$addFields': {
                    'medications': {
                        '$filter': {
                            'input': '$medications', 
                            'as': 'med', 
                            'cond': {
                                '$and': [
                                    {
                                        '$ne': [
                                            '$$med.date', None
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$med.date', {
                                                '$arrayElemAt': [
                                                    '$valid_labs.date', 1
                                                ]
                                            }
                                        ]
                                    }, {
                                        '$lte': [
                                            '$$med.date', {
                                                '$arrayElemAt': [
                                                    '$valid_labs.date', 0
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'medications': {
                        '$filter': {
                            'input': '$medications', 
                            'as': 'med', 
                            'cond': {
                                '$eq': [
                                    '$$med.status', 'Active'
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'medications': {
                        '$map': {
                            'input': '$medications', 
                            'as': 'med', 
                            'in': {
                                'date': '$$med.date', 
                                'gpi': '$$med.gpi', 
                                'status': '$$med.status', 
                                'action': '$$med.action', 
                                'end_date': '$$med.end_date', 
                                'days': '$$med.sig_parsed.days', 
                                'dose_per_day': {
                                    '$multiply': [
                                        '$$med.sig_parsed.dose_g', '$$med.sig_parsed.frequency'
                                    ]
                                }
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_lab_date': {
                        '$arrayElemAt': [
                            '$valid_labs.date', 0
                        ]
                    }, 
                    'medications': {
                        '$map': {
                            'input': '$medications', 
                            'as': 'med', 
                            'in': {
                                'date': '$$med.date', 
                                'gpi': '$$med.gpi', 
                                'status': '$$med.status', 
                                'action': '$$med.action', 
                                'end_date': '$$med.end_date', 
                                'days': '$$med.days', 
                                'dose_per_day': '$$med.dose_per_day', 
                                'dosage': {
                                    '$cond': [
                                        {
                                            '$gt': [
                                                {
                                                    '$add': [
                                                        '$$med.date', {
                                                            '$multiply': [
                                                                '$$med.days', 24 * 60 * 60 * 1000
                                                            ]
                                                        }
                                                    ]
                                                }, {
                                                    '$arrayElemAt': [
                                                        '$valid_labs.date', 0
                                                    ]
                                                }
                                            ]
                                        }, {
                                            '$multiply': [
                                                {
                                                    '$divide': [
                                                        {
                                                            '$subtract': [
                                                                {
                                                                    '$arrayElemAt': [
                                                                        '$valid_labs.date', 0
                                                                    ]
                                                                }, '$$med.date'
                                                            ]
                                                        }, 24 * 60 * 60 * 1000
                                                    ]
                                                }, '$$med.dose_per_day'
                                            ]
                                        }, {
                                            '$multiply': [
                                                '$$med.dose_per_day', '$$med.days'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'medications')

class AlbuminLab(PreprocessedLabs):
    def __init__(self):
        self.base_pipeline = self.get_base_pipeline(
            api_test_name='Serum or plasma albumin measurement (mass/volume)',
            api_test_names=[
                'Serum or plasma albumin measurement (mass/volume)', 
                'Serum albumin measurement', 
                'Urine albumin measurement', 
                'Albumin measurement for detection of microalbuminuria', 
                'Urine albumin measurement for detection of microalbuminuria', 
                'Urine albumin measurement'
            ],
            new_api_test_name='albumin'
        )

    def run_aggregator_labs(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_after': {
                        'date': '$lab_after.date', 
                        'result': '$lab_after.result', 
                        'unit': '$lab_after.unit', 
                        'range': '$lab_after.range', 
                        'api_test_name': '$lab_after.api_test_name'
                    }, 
                    'lab_before': {
                        'date': '$lab_before.date', 
                        'result': '$lab_before.result', 
                        'unit': '$lab_before.unit', 
                        'range': '$lab_before.range', 
                        'api_test_name': '$lab_before.api_test_name'
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'labs')

    def run_aggregator_diagnosis(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'diagnosis': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': {
                        'lab_after': '$lab_after', 
                        'lab_before': '$lab_before'
                    }, 
                    'diagnosis': {
                        '$filter': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'cond': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$$diag.status', 'Active'
                                        ]
                                    }, {
                                        '$ne': [
                                            '$$diag.date', None
                                        ]
                                    }, {
                                        '$eq': [
                                            {
                                                '$type': '$$diag.date'
                                            }, 'date'
                                        ]
                                    }, {
                                        '$lt': [
                                            '$$diag.date', '$lab_before.date'
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    '_id': 1, 
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'diagnosis': {
                        '$filter': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'cond': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$$diag.status', 'Active'
                                        ]
                                    }, {
                                        '$ne': [
                                            '$$diag.date', None
                                        ]
                                    }, {
                                        '$eq': [
                                            {
                                                '$type': '$$diag.date'
                                            }, 'date'
                                        ]
                                    }, {
                                        '$lt': [
                                            '$$diag.date', '$valid_labs.lab_before.date'
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$diag.date', {
                                                '$dateSubtract': {
                                                    'startDate': '$valid_labs.lab_before.date', 
                                                    'unit': 'year', 
                                                    'amount': 2
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'diagnosis': {
                        '$map': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'in': {
                                'date': '$$diag.date', 
                                'icd_10': '$$diag.icd_10'
                            }
                        }
                    }
                }
            }, {
                '$addFields': {
                    'diagnosis': {
                        '$map': {
                            'input': '$diagnosis', 
                            'as': 'diag', 
                            'in': {
                                'icd_10': {
                                    '$arrayElemAt': [
                                        {
                                            '$split': [
                                                '$$diag.icd_10', '.'
                                            ]
                                        }, 0
                                    ]
                                }, 
                                '_id': '$$diag._id'
                            }
                        }
                    }
                }
            }, {
                '$set': {
                    'diagnosis': {
                        '$reduce': {
                            'input': '$diagnosis', 
                            'initialValue': [], 
                            'in': {
                                '$let': {
                                    'vars': {
                                        'existingCodes': '$$value.icd_10'
                                    }, 
                                    'in': {
                                        '$cond': {
                                            'if': {
                                                '$in': [
                                                    '$$this.icd_10', '$$existingCodes'
                                                ]
                                            }, 
                                            'then': '$$value', 
                                            'else': {
                                                '$concatArrays': [
                                                    '$$value', [
                                                        '$$this'
                                                    ]
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'diagnosis')

    def run_aggregator_vitals(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'vitals': 1, 
                    'lab_after': {
                        '$arrayElemAt': [
                            '$valid_labs', 0
                        ]
                    }, 
                    'lab_before': {
                        '$arrayElemAt': [
                            '$valid_labs', 1
                        ]
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'lab_before': 1, 
                    'vitals': {
                        '$filter': {
                            'input': '$vitals', 
                            'as': 'vital', 
                            'cond': {
                                '$and': [
                                    {
                                        '$lt': [
                                            '$$vital.date', '$lab_before.date'
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$vital.date', {
                                                '$dateSubtract': {
                                                    'startDate': '$lab_before.date', 
                                                    'unit': 'day', 
                                                    'amount': 350
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'vitals')

    def run_aggregator_demo(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'demographics.date_of_birth': 1, 
                    'demographics.gender': 1, 
                    'demographics.race_mapping': 1, 
                    'demographics.ethnicity_mapping': 1
                }
            }, {
                '$addFields': {
                    'age': {
                        '$floor': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        datetime.utcnow(), {
                                            '$getField': {
                                                'field': 'date_of_birth', 
                                                'input': {
                                                    '$arrayElemAt': [
                                                        '$demographics', 0
                                                    ]
                                                }
                                            }
                                        }
                                    ]
                                }, 1000 * 60 * 60 * 24 * 365
                            ]
                        }
                    }
                }
            }, {
                '$set': {
                    'date_of_birth': {
                        '$getField': {
                            'field': 'date_of_birth', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'gender': {
                        '$getField': {
                            'field': 'gender', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'race_mapping': {
                        '$getField': {
                            'field': 'race_mapping', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }, 
                    'ethnicity_mapping': {
                        '$getField': {
                            'field': 'ethnicity_mapping', 
                            'input': {
                                '$arrayElemAt': [
                                    '$demographics', 0
                                ]
                            }
                        }
                    }
                }
            }, {
                '$unset': 'demographics'
            }
        ]
        self.run_aggregator(pipeline, 'demo')

    def run_aggregator_medications(self):
        pipeline = self.base_pipeline.copy() + [
            {
                '$addFields': {
                    'medications': {
                        '$filter': {
                            'input': '$medications', 
                            'as': 'med', 
                            'cond': {
                                '$and': [
                                    {
                                        '$ne': [
                                            '$$med.date', None
                                        ]
                                    }, {
                                        '$gte': [
                                            '$$med.date', {
                                                '$arrayElemAt': [
                                                    '$valid_labs.date', 1
                                                ]
                                            }
                                        ]
                                    }, {
                                        '$lte': [
                                            '$$med.date', {
                                                '$arrayElemAt': [
                                                    '$valid_labs.date', 0
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'medications': {
                        '$filter': {
                            'input': '$medications', 
                            'as': 'med', 
                            'cond': {
                                '$eq': [
                                    '$$med.status', 'Active'
                                ]
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_labs': 1, 
                    'medications': {
                        '$map': {
                            'input': '$medications', 
                            'as': 'med', 
                            'in': {
                                'date': '$$med.date', 
                                'gpi': '$$med.gpi', 
                                'status': '$$med.status', 
                                'action': '$$med.action', 
                                'end_date': '$$med.end_date', 
                                'days': '$$med.sig_parsed.days', 
                                'dose_per_day': {
                                    '$multiply': [
                                        '$$med.sig_parsed.dose_g', '$$med.sig_parsed.frequency'
                                    ]
                                }
                            }
                        }
                    }
                }
            }, {
                '$project': {
                    'PatientID': 1, 
                    'Practice': 1, 
                    'valid_lab_date': {
                        '$arrayElemAt': [
                            '$valid_labs.date', 0
                        ]
                    }, 
                    'medications': {
                        '$map': {
                            'input': '$medications', 
                            'as': 'med', 
                            'in': {
                                'date': '$$med.date', 
                                'gpi': '$$med.gpi', 
                                'status': '$$med.status', 
                                'action': '$$med.action', 
                                'end_date': '$$med.end_date', 
                                'days': '$$med.days', 
                                'dose_per_day': '$$med.dose_per_day', 
                                'dosage': {
                                    '$cond': [
                                        {
                                            '$gt': [
                                                {
                                                    '$add': [
                                                        '$$med.date', {
                                                            '$multiply': [
                                                                '$$med.days', 24 * 60 * 60 * 1000
                                                            ]
                                                        }
                                                    ]
                                                }, {
                                                    '$arrayElemAt': [
                                                        '$valid_labs.date', 0
                                                    ]
                                                }
                                            ]
                                        }, {
                                            '$multiply': [
                                                {
                                                    '$divide': [
                                                        {
                                                            '$subtract': [
                                                                {
                                                                    '$arrayElemAt': [
                                                                        '$valid_labs.date', 0
                                                                    ]
                                                                }, '$$med.date'
                                                            ]
                                                        }, 24 * 60 * 60 * 1000
                                                    ]
                                                }, '$$med.dose_per_day'
                                            ]
                                        }, {
                                            '$multiply': [
                                                '$$med.dose_per_day', '$$med.days'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        ]
        self.run_aggregator(pipeline, 'medications')