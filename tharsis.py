'''
User sends request in the form of an _expression_ in code or a data structure equivalent to code.
E.g. f( g(), (h( i(), j(), const(2))).
Must be composed entirely of pure functions or functions which rely only on immutable external data.
So results can be cached.

Receiving service translates it into something that is added to the Ready queue.
    (Said something has an identifier corresponding to the code.)
    Then waits for a result _somehow_.
    Then returns result to the user.
    Might be better to return some key and let the user subscribe to
    notifications of completed computations? Then  have a second service
    for retrieving results given the key.


A ready-queue worker picks it up from the Ready queue.
    ? Can we work in affinity here?
    If it is already in the cache:
        The worker returns the value from the cache.
    If it is a call w/ no arguments:
        The worker executes it;
        writes the result to the Results DB;
        adds a Done message to the Done queue;
        if the call was from the receiving service, signals the receiver that it was done;
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

Recursively we will eventually get down to pure function calls. (Constants considered a special case of pure functions.)

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

DEBUG = False

#-------------------------------------------------------------------------------

def time_stanp_str():
    '''Standardizes time stamp format'''
    return '{:.10f} {:s}'.format(time.monotonic(), time.strftime("%Y%m%d%H%S", time.gmtime()))

def req_id_from_code(code):
    '''Standardizes code -> request ID generation'''
    return hash(code)

NonExistingResult = namedtuple('NonExistingResult', ['req_id'])

WaitingRequest = namedtuple('WaitingRequest', ['deps', 'req_id', 'code'])

Comms = namedtuple('Comms', ['resultsDB',        # Db where results are stored
                             'toBeCalculatedQ',  # Queue for calculations ready to run
                             'toBeAnnouncedQ',   # Queue for calculations finished which need announcing to other workers
                             'toBeReportedQ',    # Queue for calculations finished which need to be reported to user
                             'toBeRevivedQ',     # Queue for calculations waiting for inputs to be ready
                             'toBeLoggedQ',      # Queue for messages to be logged
                             'resultSendrs',     # Pipe ends where completed request IDs are sent
                            ])

def add_to_be_calculated_queue(comms, req_id, code):
    '''Centralized method for adding to queue. For sanity checks and logs.'''
    if not isinstance(code, tuple):# or isinstance(code, list)):
        raise ValueError()
    comms.toBeCalculatedQ.put((req_id, code))

def add_to_be_revived_queue(comms, req_id, code):
    '''Centralized method for adding to queue. For sanity checks and logs.'''
    if not isinstance(code, tuple):# or isinstance(code, list)):
        raise ValueError()
    comms.toBeRevivedQ.put((req_id, code))

def add_to_be_announced_queue(comms, req_id):
    '''Centralized method for adding to queue. For sanity checks and logs.'''
    comms.toBeAnnouncedQ.put(req_id)

def add_to_be_reported_queue(comms, req_id):
    '''Centralized method for adding to queue. For sanity checks and logs.'''
    comms.toBeReportedQ.put(req_id)

#-------------------------------------------------------------------------------
# Mini language we interpret

class Language():
    '''We're onlly going to have built in functions or forms.'''

    class SpecialForms():
        '''Don't evaluate arguments first'''

        @staticmethod
        def quote(arg):
            '''Protect arg from evaluation. Suppose you wanted to use a tuple as data rather than as a function call indicator.'''
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



#-------------------------------------------------------------------------------
# Workers

def calculation_worker_method(worker_id, comms):
    '''Code for the calculation workers'''
    # NOTE: A request shouldn't be revived unless _all_ inputs are ready. So we won't attempt to handle partial results here.

    def process_nested(code):
        '''handles one argument'''
        if not isinstance(code, tuple):
            # Plain old data. Nothing to do.
            return code
        else:
            # Nested function call
            child_req_id = req_id_from_code(code)
            if child_req_id not in comms.resultsDB:
                # TODO: segment functions into ones meant to be distributed and ones meant to run locally then branch here.
                add_to_be_calculated_queue(comms, child_req_id, code)
                return NonExistingResult(child_req_id)
            else:
                return comms.resultsDB[child_req_id]

    while True:
        req_id, code = comms.toBeCalculatedQ.get()
        if req_id not in comms.resultsDB:
            # If it was in the db there'd be nothing for us to do
            if not any(isinstance(c, tuple) for c in code):
                # No sub-code, just evaluate.
                # Fill in any references to the results DB
                code = tuple(comms.resultsDB[c.req_id] if isinstance(c, NonExistingResult) else c for c in code)
                res = Language.eval(code)
                comms.resultsDB[req_id] = res
                add_to_be_reported_queue(comms, req_id)
                add_to_be_announced_queue(comms, req_id)
                if DEBUG:
                    comms.toBeLoggedQ.put('[{:s}] {:s} completed {:d} -> {:s}'.format(time_stanp_str(), str(worker_id), req_id, str(res)))
            else:
                # We have sub-code to evaluate
                # Fill in any references to the results DB
                code = tuple(comms.resultsDB[c.req_id] if isinstance(c, NonExistingResult) else c for c in code)
                code = tuple(process_nested(c) for c in code)
                add_to_be_revived_queue(comms, req_id, tuple(code))
                if DEBUG:
                    comms.toBeLoggedQ.put('[{:s}] {:s} sending {:d} to toBeRevivedQ.'.format(time_stanp_str(), str(worker_id), req_id))


def revival_worker_method(worker_id, comms, pipe):
    '''Restart calculations which were on hold waiting for inputs'''
    # Questionable method
    tasks = []

    def update_task(task, finished_req_id):
        '''Update one sub-task based on knowledge of new finished_req_id'''
        if finished_req_id not in task.deps:
            return task
        else:
            # Fill in any results we can find in the db, including ones other than finished_req_id. Why wait if they are available?
            code = tuple(comms.resultsDB[c.req_id] if isinstance(c, NonExistingResult) else c for c in task.code)
            deps = {c.req_id for c in code if isinstance(c, NonExistingResult)}
            if deps:
                # There are still unmet deps
                return WaitingRequest(deps, task.req_id, code)
            else:
                # This guy is ready
                add_to_be_calculated_queue(comms, task.req_id, task.code)
                return None

    while True:
        if pipe.poll():
            # Listen for announcements that other workers have completed calcualtions and see if any of our tasks depend on them.
            finished_req_id = pipe.recv()
            if DEBUG:
                comms.toBeLoggedQ.put("[{:s}] {:s} Noticed {:d}".format(time_stanp_str(), str(worker_id), finished_req_id))
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
                code = tuple(comms.resultsDB[c.req_id] if isinstance(c, NonExistingResult) and c.req_id in comms.resultsDB else c for c in code)
                deps = {c.req_id for c in code if isinstance(c, NonExistingResult)}
                if deps:
                    tasks.append(WaitingRequest(deps, req_id, code))
                    if DEBUG:
                        comms.toBeLoggedQ.put("[{:s}] {:s} Took on {:d} with deps {:s}".format(time_stanp_str(), str(worker_id), req_id, str(deps)))
                else:
                    # Everything is actually already ready
                    add_to_be_calculated_queue(comms, req_id, code)
                    if DEBUG:
                        comms.toBeLoggedQ.put("[{:s}] {:s} {:d} is ready already".format(time_stanp_str(), str(worker_id), req_id))


def report_worker_method(worker_id, comms):
    '''Report result to end user'''
    while True:
        req_id = comms.toBeReportedQ.get()
        # Imagine this getting back to the user somehow
        comms.toBeLoggedQ.put("[{:s}] {:s} DONE {:d} -> {:s}".format(time_stanp_str(), str(worker_id), req_id, str(comms.resultsDB[req_id])))


def announcement_worker_method(worker_id, comms):
    '''Announce results to other workers'''
    # Exclusive writer to the result announcing pipes. Exactly one of these. Must be quick.
    # Questionable method
    while True:
        req_id = comms.toBeAnnouncedQ.get()
        for p in comms.resultSendrs:
            p.send(req_id) # Hope this doesn't block (buffer full)
            if DEBUG:
                comms.toBeLoggedQ.put("[{:s}] {:s} Announcing {:d}".format(time_stanp_str(), str(worker_id), req_id))
            # A subscription model where only some workers get some messages might be better.


def log_worker_method(worker_id, comms):
    '''Code for the log workers'''
    _ = worker_id # This would matter if we had more than one
    while True:
        msg = comms.toBeLoggedQ.get()
        print(msg)






#-------------------------------------------------------------------------------
# Example

def main():
    '''Main as a real function'''
    num_calc_workers = 4
    num_report_workers = 2
    num_revival_workers = 2
    num_log_workers = 1 # Keep at 1 or print statements will get mixed together
    with mp.Manager() as results_db_mgr:
        result_rcvrs, result_sndrs = zip(*(mp.Pipe() for _ in range(num_revival_workers)))
        comms = Comms(results_db_mgr.dict(),
                      mp.Queue(),
                      mp.Queue(),
                      mp.Queue(),
                      mp.Queue(),
                      mp.Queue(),
                      result_sndrs)

        calculation_workers = [mp.Process(target=calculation_worker_method, args=((i, 'C'), comms)) for i in range(num_calc_workers)]
        for w in calculation_workers:
            w.start()

        # Only one allowed with the way we are using pipes
        announcement_worker = mp.Process(target=announcement_worker_method, args=((0, 'A'), comms))
        announcement_worker.start()

        report_workers = [mp.Process(target=report_worker_method, args=((i, 'R'), comms)) for i in range(num_report_workers)]
        for w in report_workers:
            w.start()

        revival_workers = [mp.Process(target=revival_worker_method, args=((i, 'V'), comms, pipe)) for i, pipe in enumerate(result_rcvrs)]
        for w in revival_workers:
            w.start()

        log_workers = [mp.Process(target=log_worker_method, args=((i, 'L'), comms)) for i in range(num_log_workers)]
        for w in log_workers:
            w.start()

        for code in [('sum', 1, ('prod', 2, 3)),
                     ('frac', 1, 2, 3, 4),
                     ('frac', ('sum', 1., 2.), ('prod', 3., 4.)),
                     ('frac', (('evals_to_sum',), 1., 2.), ('prod', 3., 4.)),
                    ]:
            req_id = req_id_from_code(code)
            comms.toBeLoggedQ.put("User requesting {:d}".format(req_id))
            add_to_be_calculated_queue(comms, req_id, code)

        for w in calculation_workers + [announcement_worker] + report_workers + revival_workers + log_workers:
            w.join()

if __name__ == '__main__':
    main()
