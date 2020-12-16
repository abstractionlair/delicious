import multiprocessing as mp
import time
import functools
import operator
from collections import namedtuple
import queue

# User sends request in the form of an _expression_ in code or a data structure equivalent to code.
# E.g. f( g(), (h( i(), j(), const(2) ) ).
# Must be composed entirely of pure functions or functions which rely only on immutable external data.
# So results can be cached.

# Receiving service translates it into something that is added to the Ready queue.
#     (Said something has an identifier corresponding to the code.)
#     Then waits for a result _somehow_.
#     Then returns result to the user.
#     Might be better to return some key and let the user subscribe to
#     notifications of completed computations? Then  have a second service
#     for retrieving results given the key.
#

# A ready-queue worker picks it up from the Ready queue.
#     ? Can we work in affinity here?
#     If it is already in the cache:
#         The worker returns the value from the cache.
#     If it is a call w/ no arguments:
#         The worker executes it;
#         writes the result to the Results DB;
#         adds a Done message to the Done queue;
#         if the call was from the receiving service, signals the receiver that it was done;
#         removes the item from the Ready queue.
#     If the call has arguments which are unevaluated nested function calls:
#         The worker adds evaluations of the nested functions to the Ready queue;
#         adds an item to the Waiting queue/map for evaluating the call w/ evaluated nested arge to the Waiting queue;
#         removes the item from the Ready queue.
#     If the call has arguments and all have been evaluated:
#         The worker retrieves all the evaluated value from the Results DB;
#         executes the call;
#         writes the result to the Results DB;
#         adds a Done message to the Done queue;
#         if the call was from the receiving service, signals the receiver that it was done;
#         removes the item from the Ready queue.

# Recursively we will eventually get down to pure function calls. (Constants considered a special case of pure functions.)

# A done-queue worker picks up a result from the Done queue.
#     The worker examines the waiting queue/map for calculations waiting for this result.
#     For each found:
#         If this is all the waitee is waiting for:
#             The worker adds an item to the Ready queue to evaluate the function with evaluated arguments from the Results DB;
#             and removes the item from the Waiting queue.
#         If this is one of muliple results the waitee is waiting for:
#             The worker adds a new item to the Waiting queue with one less dependency;
#             and removes the original item from the Waiting queue.

# Results DB is redis, memcached type. Possbly with multiple levels and caches near the workers which would make
# affinity matter.


#-------------------------------------------------------------------------------

def timeStampStr():
    '''Standardizes time stamp format'''
    return '{:.10f} {:s}'.format(time.monotonic(), time.strftime("%Y%m%d%H%S", time.gmtime()))

def reqIdFromCode(code):
    '''Standardizes code -> request ID generation'''
    return hash(code)

def addToCalcQueue(code, toBeCalculatedQ ):
    '''Standardizes format of calculation queue items'''
    reqID = reqIdFromCode(code)
    toBeCalculatedQ.put((reqID, code))
    return reqID

def reAddToCalcQueue(code, toBeCalculatedQ, reqID):
    '''When you already have an ID'''
    toBeCalculatedQ.put((reqID, code))

NonExistingResult = namedtuple('NonExistingResult', ['reqID'] )

ExistingResult = namedtuple('ExistingResult', ['reqID'] )

class WaitingRequest(object):
    # Want mutability for this one
    # Will be owned by a single worker method
    def __init__(self, deps, reqID, code):
        self.deps = deps
        self.reqID = reqID
        self.code = list(code)

Comms = namedtuple( 'Comms', [  'resultsDB',        # Db where results are stored
                                'toBeCalculatedQ',  # Queue for calculations ready to run
                                'toBeAnnouncedQ',   # Queue for calculations finished which need announcing to other workers
                                'toBeReportedQ',    # Queue for calculations finished which need to be reported to user
                                'toBeRevivedQ',     # Queue for calculations waiting for inputs to be ready
                                'toBeLoggedQ',      # Queue for messages to be logged
                                #'resultRecvrs',     # Pipe ends from which completed request IDs are read
                                'resultSendrs',     # Pipe ends where completed request IDs are sent
                             ] )

#-------------------------------------------------------------------------------
# Mini language we interpret

class Language( object ):
    '''We're onlly going to have built in functions or forms.'''

    class SpecialForms( object ):
        @staticmethod
        def quote( arg ):
            return arg

    class Functions( object ):
        @staticmethod
        def sum( *args ):
            return functools.reduce( operator.add, args, 0. )

        @staticmethod
        def diff( *args ):
            return functools.reduce( operator.sub, args, 0. )

        @staticmethod
        def prod( *args ):
            return functools.reduce( operator.mul, args, 1. )

        @staticmethod
        def frac( *args ):
            return functools.reduce( operator.truediv, args, 1. )

        @staticmethod
        def evalsToSum():
            return 'sum'

    @staticmethod
    def eval( code ):
        '''Not lisp, just lispish.'''
        if not code:
            return code
        elif isinstance( code[0], tuple ):
            return Language.eval( ( Language.eval( code[0] ), ) + code[1:] )
        elif hasattr( Language.SpecialForms, code[0] ):
            return getattr( Language.SpecialForms, code[0] )( *code[1:] )
        else:
            code = [ Language.eval( x ) if isinstance( x, tuple ) else x for x in code ]
            return getattr( Language.Functions, code[0] )( *code[1:] )



#-------------------------------------------------------------------------------
# Workers

def calculationWorkerMethod( workerID, comms):
    # NOTE: A request shouldn't be revived unless _all_ inputs are ready. So we won't attempt to handle partial ready results.
    while True:
        reqID, code = comms.toBeCalculatedQ.get()
        if reqID not in comms.resultsDB:
            if not code:
                comms.resultsDB[reqID] = Language.eval(code)
            elif any( isinstance(c, tuple) for c in code ):
                # We have sub-code to evaluate
                children = []
                newCode = []
                for c in code:
                    if isinstance(c, tuple):
                        # Nested function call
                        # FIXME: check if results is already in db
                        childReqID = addToCalcQueue(c, comms.toBeCalculatedQ)
                        children.append(childReqID)
                        newCode.append(NonExistingResult(childReqID))
                    else:
                        # Plain old data
                        newCode.append(c)
                addToCalcQueue(newCode, comms.toBeRevivedQ)
            else:
                comms.resultsDB[ reqID ] = Language.eval([ resultsDB[c.reqID] if isinstance(c, NonExistingResult) else c for c in code ])

        comms.toBeReportedQ.put( reqID )
        comms.toBeLoggedQ.put('[{:s}] {:s} completed {:x}'.format(timeStampStr(), str(workerID), reqID))


def reportWorkerMethod( workerID, comms ):
    '''Report result to end user'''
    while True:
        reqID = comms.toBeReportedQ.get()
        print( "DONE: {:x}".format(reqID) ) # Imagine this getting back to the user somehow

def announcementWorkerMethod(workerID, comms):
    '''Announce results to other workers'''
    # Exclusive writer to the result announcing pipes. Exactly one of these. Must be quick.
    # Questionable method
    while True:
        reqID = comms.toBeAnnouncedQ.get()
        for pe in comms.resultSendrs:
            pe.send(reqID) # Hope this doesn't block (buffer full)
            # A subscription model where only some workers get some messages might be better.

def revivalWorkerMethod(workerID, pipeEnd, comms):
    '''Restart calculations which were on hold waiting for inputs'''
    # Questionable method
    tasks = []

    def updateTask(task, finishedReqID):
        '''Update task based on knowledge of new finishedReqID'''
        if finishedReqID not in task.deps:
            return task
        else:
            task.code = [ExistingResult(c) if isinstance(c, NonExistingResult) else c for c in task.code]
            task.deps.remove(finishedReqID)
            if task.deps:
                # There are still unmet deps
                return task
            else:
                # This guy is ready
                # Do something
                reAddToCalcQueue(task.code, comms.toBeCalculatedQ, task.reqID)
                return None

    while True:
        if pipeEnd.poll():
            finishedReqID = pipeEnd.recv()
            tasks = [updateTask(t, finishedReqID) for t in tasks if t is not None]

        if not comms.toBeRevivedQ.empty():
            # Docs say empty() is unreliable so we still could get an exception
            try:
                reqID, code = comms.toBeRevivedQ.get_nowait()
            except queue.Empty:
                pass # Nothing in queue, keep polling
            else:
                # We got a new one
                tasks.append(WaitingRequest((set(c.reqID for c in code if isinstance(c, NonExistingResult)), reqID, code)))

def logWorkerMethod( workerID, inQ ):
    while True:
        msg =  inQ.get()
        print(msg)






#-------------------------------------------------------------------------------
# Example

if __name__ == '__main__':
    nCalculationWorkers = 4
    nReportWorkers = 1 # One while we are just using print
    nRevivalWorkers = 2
    nLogWorkers = 1
    with mp.Manager() as resultsDBMgr:
        resultsDB = resultsDBMgr.dict()
        toBeCalculatedQ = mp.Queue()
        toBeAnnouncedQ = mp.Queue()
        toBeReportedQ = mp.Queue()
        toBeRevivedQ = mp.Queue()
        toBeLoggedQ = mp.Queue()
        resultRecvrs, resultSendrs = [multiprocessing.Pipe() for _ in range(nRevivalWorkers)]

        comms = comms(resultsDB,
                      toBeCalculatedQ,
                      toBeAnnouncedQ,
                      toBeReportedQ,
                      toBeRevivedQ,
                      toBeLoggedQ,
                      resultSendrs)

        calculationWorkers = [mp.Process(target=calculationWorkerMethod, args=((i, 'C'), comms)) for i in range(nCalculationWorkers)]
        for w in calculationWorkers: w.start()

        announcementWorker = mp.Process(target=announcementWorkerMethod, args=((0,'A'), comms))
        announcementWorker.start()

        reportWorkers = [mp.Process(target=reportWorkerMethod, args=((i, 'R'), comms)) for i in range(nReportWorkers)]
        for w in reportWorkers: w.start()

        revivalWorkers = [mp.Process(target=revivalWorkerMethod, args=((i, 'V'), comms)) for i in range(nRevivalWorkers))]
        for w in revivalWorkers: w.start()

        logWorkers = [ mp.Process(target=logWorkerMethod, args = ( (i 'L'), comms)) for i in range(nLogWorkers)]
        for w in logWorkers: w.start()

        for code in [ ( 'sum', 1, ( 'prod', 2, 3 ) ),98
                      ( 'frac', 1, 2, 3, 4 ),
                      ( 'frac', ( 'sum', 1., 2. ), ( 'prod', 3., 4. ) ),
                      ( 'frac', ( ( 'evalsToSum', ), 1., 2. ), ( 'prod', 3., 4. ) ),
                    ]:
            #reqID = reqIdFromCode( code )
            #toBeCalculatedQ.put( ( reqID, code ) )
            reqID = addToCalcQueue(code, toBeCalculatedQ)

        for w in calculationWorkers + reportWorkers + logWorkers:
            #w.terminate()
            w.join()
