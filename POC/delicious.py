'''
Initial ideas. Not up to date with code below.

User sends request in the form of an _expression_ in a data structure equivalent to code.
E.g. (f, (g,), ((h, (i,), (j,), (const, 2))).
Must be composed entirely of pure functions or functions which rely only on immutable external data.
So results can be cached.

Receiving service translates it into something that is added to the Ready queue.
    Then waits for a result _somehow_.
    Then returns result to the user.
    Might be better to return some key and let the user subscribe to
    notifications of completed computations? Then have a second service
    for retrieving results given the key.

A ready-queue worker picks it up from the Ready queue.
    ? Can we work in affinity here?
    If it is already in the cache:
        The worker returns the value from the cache.
    If it is a call w/ no arguments:
        The worker executes it;
        writes the result to the Results DB;
        adds a Done message to the Done queue;
        removes the item from the Ready queue.
    If the call has arguments which are unevaluated nested function calls:
        The worker adds evaluations of the nested functions to the Ready queue;        
        adds an item to the Waiting queue/map for evaluating the call w/ evaluated nested arge to the Waiting queue;
        removes the item from the Ready queue.
    If the call has arguments and all have been evaluated:
        The worker retrieves all the evaluated value from the Results DB;
        executes the call;
        writes the result to the Results DB;
        adds a Done message to the Done queue;
        if the call was from the receiving service, signals the receiver that it was done;
        removes the item from the Ready queue.

Recursively we will eventually get down to function calls without arguments.
(Constants considered a special case of functions with no arguments.)

A done-queue worker picks up a result from the Done queue.
    The worker examines the waiting queue/map for calculations waiting for this result.
    For each found:
        If this is all the waitee is waiting for:
            The worker adds an item to the Ready queue to evaluate the function with evaluated arguments from the Results DB;
            and removes the item from the Waiting queue.
        If this is one of muliple results the waitee is waiting for:
            The worker adds a new item to the Waiting queue with one less dependency;
            and removes the original item from the Waiting queue.

Results DB is redis, memcached type. Possbly with multiple levels and caches near the workers which would make
affinity matter.
'''

import multiprocessing as mp
import time
import functools
import operator
from collections import namedtuple
import queue
import pickle

import redis


DEBUG = False


#-------------------------------------------------------------------------------
# Mini language we interpret

class Language():
    '''We're onlly going to have built in functions or forms.'''
    
    def __init__( self,
                  num_calc_workers = 4,
                  num_report_workers = 2,
                  num_revival_workers = 2,
                  num_log_workers = 1 # Keep at 1 for now or print statements will get mixed together
    ):
        results_db = redis.Redis(host='localhost', port=6379, db=0)
        self.comms = _Comms(results_db,
                            results_db.pubsub(ignore_subscribe_messages=True),
                            mp.Queue(),
                            mp.Queue(),
                            mp.Queue(),
                            mp.Queue())

        self.calculation_workers = [mp.Process(target=self._calculation_worker_method, args=((i, 'C'), self.comms)) for i in range(num_calc_workers)]
        for w in self.calculation_workers:
            w.start()
            print('started', w )
            
        self.report_workers = [mp.Process(target=self._report_worker_method, args=((i, 'R'), self.comms)) for i in range(num_report_workers)]
        for w in self.report_workers:
            w.start()
            print('started', w )
                
        self.revival_workers = [mp.Process(target=self._revival_worker_method, args=((i, 'V'), self.comms)) for i in range(num_revival_workers)]
        for w in self.revival_workers:
            w.start()
            print('started', w )
                    
        self.log_workers = [mp.Process(target=self._log_worker_method, args=((i, 'L'), self.comms)) for i in range(num_log_workers)]
        for w in self.log_workers:
            w.start()
            print('started', w )

    def join( self ):
        for w in self.calculation_workers + self.report_workers + self.revival_workers + self.log_workers:
            w.join()
            print('joined to', w)

    def request_calc(self, code):
        '''Centralized method for adding to queue. For sanity checks and logs.'''
        if not isinstance(code, tuple):
            raise ValueError()
        req_id = _req_id_from_code(code)
        Language._to_be_calculated_queue_add(self.comms, req_id, code)
        self.comms.toBeLoggedQ.put('[{:s}] user requested {:s}'.format(_time_stamp_str(), str(pickle.loads(req_id))))
        return req_id

    #-------------------------------------------------------------------------------
    # Workers and other innards

    @staticmethod
    def _to_be_calculated_queue_add(comms, req_id, code):
        '''Centralized method for adding to queue. For sanity checks and logs.'''
        if not isinstance(code, tuple):
            raise ValueError()
        comms.toBeCalculatedQ.put((req_id, code))

    @staticmethod
    def _to_be_revived_queue_add(comms, req_id, code):
        '''Centralized method for adding to queue. For sanity checks and logs.'''
        if not isinstance(code, tuple):# or isinstance(code, list)):
            raise ValueError()
        comms.toBeRevivedQ.put((req_id, code))

    @staticmethod
    def _publish_calc_completed(comms, req_id):
        '''Centralized method for adding to queue. For sanity checks and logs.'''
        comms.resultsDB.publish('calc-completed', req_id)

    @staticmethod
    def _to_be_reported_queue_add(comms, req_id):
        '''Centralized method for adding to queue. For sanity checks and logs.'''
        comms.toBeReportedQ.put(req_id)    
    
    @staticmethod
    def _calculation_worker_method(worker_id, comms):
        '''Code for the calculation workers'''
        # NOTE: A request shouldn't be revived unless _all_ inputs are ready. So we won't attempt to handle partial results here.
        
        def process_nested(code):
            '''handles one argument'''
            if not isinstance(code, tuple):
                # Plain old data. Nothing to do.
                return code
            else:
                # Nested function call
                child_req_id = _req_id_from_code(code)
                child_res = comms.resultsDB.get(pickle.loads(child_req_id))
                if child_res:
                    # Result is in the DB
                    return child_res
                else:
                    # TODO: segment functions into ones meant to be distributed and ones meant to run locally then branch here.
                    Language._to_be_calculated_queue_add(comms, child_req_id, code)
                    return _NonExistingResult(child_req_id)
                
        while True:
            req_id, code = comms.toBeCalculatedQ.get()
            if not comms.resultsDB.exists(req_id):
                # If it was in the db there'd be nothing for us to do
                if not any(isinstance(c, tuple) for c in code):
                    # No sub-code, just evaluate.
                    # Fill in any references to the results DB
                    code = tuple(pickle.loads(comms.resultsDB.get(c.req_id)) if isinstance(c, _NonExistingResult) else c for c in code)
                    res = Language.eval(code)
                    comms.resultsDB.set(req_id, pickle.dumps(res))
                    Language._to_be_reported_queue_add(comms, req_id)
                    Language._publish_calc_completed(comms, req_id)
                    if DEBUG:
                        comms.toBeLoggedQ.put('[{:s}] {:s} completed {:s} -> {:s}'.format(_time_stamp_str(), str(worker_id), str(pickle.loads(req_id)), str(res)))
                else:
                    # We have sub-code to evaluate
                    # Fill in any references to the results DB
                    code = tuple(pickle.loads(comms.resultsDB.get(c.req_id)) if isinstance(c, _NonExistingResult) else c for c in code)
                    code = tuple(process_nested(c) for c in code)
                    Language._to_be_revived_queue_add(comms, req_id, tuple(code))
                    if DEBUG:
                        comms.toBeLoggedQ.put('[{:s}] {:s} sending {:s} to toBeRevivedQ.'.format(_time_stamp_str(), str(worker_id), str(pickle.loads(req_id))))
                                    

    @staticmethod
    def _revival_worker_method(worker_id, comms):
        '''Restart calculations which were on hold waiting for inputs'''
        # Questionable method
        tasks = []

        comms.resultsPubSub.subscribe('calc-completed')

        def update_task(task, finished_req_id):
            '''Update one sub-task based on knowledge of new finished_req_id'''
            if finished_req_id not in task.deps:
                return task
            else:
                # Fill in any results we can find in the db, including ones other than finished_req_id. Why wait if they are available?
                code = tuple(pickle.loads(comms.resultsDB.get(c.req_id))
                             if isinstance(c, _NonExistingResult) and comms.resultsDB.exists(c.req_id)
                             else c
                             for c in task.code)
                deps = {c.req_id for c in code if isinstance(c, _NonExistingResult)}
                if deps:
                    # There are still unmet deps
                    return _WaitingRequest(deps, task.req_id, code)
                else:
                    # This guy is ready
                    Language._to_be_calculated_queue_add(comms, task.req_id, task.code)
                    return None

        while True:
            psm = comms.resultsPubSub.get_message()
            if psm and psm['type'] == 'message':
                #print("Got message", pickle.loads(psm['data']) )
                finished_req_id = psm['data']
                if DEBUG:
                    comms.toBeLoggedQ.put("[{:s}] {:s} Noticed {:s}".format(_time_stamp_str(), str(worker_id), str(pickle.loads(finished_req_id))))
                tasks = [update_task(t, finished_req_id) for t in tasks if t is not None]

            if not comms.toBeRevivedQ.empty():
                # Watch for new tasks for us to manage
                # Docs say empty() is unreliable so we still could get an exception
                try:
                    req_id, code = comms.toBeRevivedQ.get_nowait()
                except queue.Empty:
                    pass # Nothing in queue, keep polling
                else:
                    # We got a new one
                    # Some results could have come in since this was added to the queue, so check now
                    code = tuple(pickle.loads(comms.resultsDB.get(c.req_id))
                                 if isinstance(c, _NonExistingResult) and comms.resultsDB.exists(c.req_id)
                                 else c
                                 for c in code)
                    deps = {c.req_id for c in code if isinstance(c, _NonExistingResult)}
                    if deps:
                        tasks.append(_WaitingRequest(deps, req_id, code))
                        if DEBUG:
                            comms.toBeLoggedQ.put("[{:s}] {:s} Took on {:s} with deps {:s}".format(
                                _time_stamp_str(), str(worker_id), str(pickle.loads(req_id)), str(deps)))
                    else:
                        # Everything is actually already ready
                        Language._to_be_calculated_queue_add(comms, req_id, code)
                        if DEBUG:
                            comms.toBeLoggedQ.put("[{:s}] {:s} {:s} is ready already".format(
                                _time_stamp_str(), str(worker_id), str(pickle.loads(req_id))))


    @staticmethod
    def _report_worker_method(worker_id, comms):
        '''Report result to end user'''
        while True:
            req_id = comms.toBeReportedQ.get()
            # Imagine this getting back to the user somehow
            comms.toBeLoggedQ.put("[{:s}] {:s} DONE {:s} -> {:s}".format(
                _time_stamp_str(), str(worker_id), str(pickle.loads(req_id)), str(pickle.loads(comms.resultsDB.get(req_id)))))


    @staticmethod
    def _log_worker_method(worker_id, comms):
        '''Code for the log workers'''
        _ = worker_id # This would matter if we had more than one
        while True:
            msg = comms.toBeLoggedQ.get()
            print(msg)


    #----------------------------------------------------------------------------
    # Mini-language definition
    
    class SpecialForms():
        '''Don't evaluate arguments first'''

        @staticmethod
        def quote(arg):
            '''Protect arg from evaluation.
               Suppose you wanted to use a tuple as data rather than as a function call indicator.'''
            return arg

    class Functions():
        '''Normal functions'''

        @staticmethod
        def sum(*args):
            '''add things'''
            return functools.reduce(operator.add, args, 0.)

        @staticmethod
        def diff(*args):
            '''subtrtact things'''
            return functools.reduce(operator.sub, args, 0.)

        @staticmethod
        def prod(*args):
            '''multiply things'''
            return functools.reduce(operator.mul, args, 1.)

        @staticmethod
        def frac(*args):
            '''divide things'''
            return functools.reduce(operator.truediv, args, 1.)

        @staticmethod
        def evals_to_sum():
            '''Demo to show indirection works'''
            return 'sum'

    @staticmethod
    def eval(code):
        '''Not lisp, just lispish.'''
        if not code:
            return code
        if isinstance(code[0], tuple):
            return Language.eval((Language.eval(code[0]),) + code[1:])
        if hasattr(Language.SpecialForms, code[0]):
            return getattr(Language.SpecialForms, code[0])(*code[1:])
        else:
            code = [Language.eval(x) if isinstance(x, tuple) else x for x in code]
            return getattr(Language.Functions, code[0])(*code[1:])




# NOTE: pickle wasn't happy with these being indside the class
_NonExistingResult = namedtuple('_NonExistingResult', ['req_id'])

_WaitingRequest = namedtuple('_WaitingRequest', ['deps', 'req_id', 'code'])

# The package of connections and such passed to each worker.
_Comms = namedtuple('_Comms', ['resultsDB',        # Db where results are stored
                               'resultsPubSub',    # Publishing and subscribing to messages about calculation completion
                               'toBeCalculatedQ',  # Queue for calculations ready to run
                               'toBeReportedQ',    # Queue for calculations finished which need to be reported to user
                               'toBeRevivedQ',     # Queue for calculations waiting for inputs to be ready
                               'toBeLoggedQ',      # Queue for messages to be logged                                    
                            ])
def _time_stamp_str():
    '''Standardizes time stamp format'''
    return '{:.10f} {:s}'.format(time.monotonic(), time.strftime("%Y%m%d%H%S", time.gmtime()))

def _req_id_from_code(code):
    '''Standardizes code -> request ID generation'''
    return pickle.dumps(hash(code))




#-------------------------------------------------------------------------------
# Example

def main():
    '''Main as a real function'''

    L = Language( num_calc_workers = 4,
                  num_report_workers = 2,
                  num_revival_workers = 2,
                  num_log_workers = 1 )
    

    for code in [('sum', 1, ('prod', 2, 3)),
                 ('frac', 1, 2, 3, 4),
                 ('frac', ('sum', 1., 2.), ('prod', 3., 4.)),
                 ('frac', (('evals_to_sum',), 1., 2.), ('prod', 3., 4.)),
                ]:
        req_id = L.request_calc(code)
    L.join()

if __name__ == '__main__':
    main()
