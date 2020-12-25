import pymongo

mongo = pymongo.MongoClient('mongodb://localhost:27017/')
constants = mongo.constants
mathematical = constants['mathematical']
mathematical.delete_many({})
# I think here strings are natural rather than ObjectIds
mathematical.insert_many([{'_id': 'E',        'value': 2.718281828459045235360287471352662498},
                          {'_id': 'LOG2E',    'value': 1.442695040888963407359924681001892137},
                          {'_id': 'LOG10E',   'value': 0.434294481903251827651128918916605082},
                          {'_id': 'LN2',      'value': 0.693147180559945309417232121458176568},
                          {'_id': 'LN10',     'value': 2.302585092994045684017991454684364208},
                          {'_id': 'PI',       'value': 3.141592653589793238462643383279502884},
                          {'_id': 'PI_2',     'value': 1.570796326794896619231321691639751442},
                          {'_id': 'PI_4',     'value': 0.785398163397448309615660845819875721},
                          {'_id': '1_PI',     'value': 0.318309886183790671537767526745028724},
                          {'_id': '2_PI',     'value': 0.636619772367581343075535053490057448},
                          {'_id': '2_SQRTPI', 'value': 1.128379167095512573896158903121545172},
                          {'_id': 'SQRT2',    'value': 1.414213562373095048801688724209698079},
                          {'_id': 'SQRT1_2',  'value': 0.707106781186547524400844362104849039},])
physical = constants['physical']
physical.delete_many({})
physical.insert_many([{'_id': 'C', 'value': 299792458.0, 'units': [('m', 1), ('s', -1)]},
                      {'_id': 'G', 'value': 6.67430,     'units': [('m', 3), ('kg', -1), ('s', -2)]},
                      {'_id': 'h', 'value': 6.62607015,  'units': [('J', 1), ('Hz', -1)]}])

